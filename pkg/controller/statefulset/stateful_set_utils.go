/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statefulset

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// Progressing 条件的取值原因。

	// RecreateInProgressReason 用于 Recreate 更新策略下旧版本 Pod 正在删除或仍处于终止中，
	// 或新版本 Pod 正在创建（即重建尚未完成）的场景。
	RecreateInProgressReason = "RecreateInProgress"
	// RecreateCompletedReason 用于所有 Pod 都已用更新版本重建且就绪的场景。
	RecreateCompletedReason = "RecreateCompleted"
)

// patchCodec 负责将 StatefulSet 编码为 JSON，供后续生成 revision patch 使用。
var patchCodec = scheme.Codecs.LegacyCodec(apps.SchemeGroupVersion)

// overlappingStatefulSets 按创建时间对 StatefulSet 列表排序，创建时间相同则以名称作为决胜条件。
// 通常用于在 selector 重叠的多个 StatefulSet 之间确定稳定的处理顺序。
type overlappingStatefulSets []*apps.StatefulSet

// Len 返回待排序的 StatefulSet 数量。
func (o overlappingStatefulSets) Len() int { return len(o) }

// Swap 交换两个 StatefulSet 的位置。
func (o overlappingStatefulSets) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 按创建时间升序比较；创建时间相同时按名称字典序，保证顺序稳定可预测。
func (o overlappingStatefulSets) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// statefulPodRegex 从 Pod 名称中提取所属 StatefulSet 名称与序号（ordinal）。
var statefulPodRegex = regexp.MustCompile("(.*)-([0-9]+)$")

// getParentNameAndOrdinal 从 Pod 名称中解析出所属 StatefulSet 名称与序号。
//   - 若 Pod 名称不符合 "<父名>-<序号>" 格式（如非 StatefulSet 创建），返回空父名与 -1；
//   - 序号解析失败时同样视为无有效序号。
func getParentNameAndOrdinal(pod *v1.Pod) (string, int) {
	// 默认值表示“非 StatefulSet Pod”，解析失败时直接返回。
	parent := ""
	ordinal := -1
	subMatches := statefulPodRegex.FindStringSubmatch(pod.Name)
	// 名称不匹配正则，说明不是 StatefulSet 格式的 Pod。
	if len(subMatches) < 3 {
		return parent, ordinal
	}
	parent = subMatches[1]
	if i, err := strconv.ParseInt(subMatches[2], 10, 32); err == nil {
		ordinal = int(i)
	}
	return parent, ordinal
}

// getOrdinal 返回 Pod 的序号；无有效序号时返回 -1。
func getOrdinal(pod *v1.Pod) int {
	_, ordinal := getParentNameAndOrdinal(pod)
	return ordinal
}

// getStartOrdinal 返回允许的最小序号（含）。
// 未设置 spec.ordinals 时使用默认起点 0。
func getStartOrdinal(set *apps.StatefulSet) int {
	if set.Spec.Ordinals != nil {
		return int(set.Spec.Ordinals.Start)
	}
	return 0
}

// getEndOrdinal 返回允许的最大序号（含），由起点加副本数减一得到。
func getEndOrdinal(set *apps.StatefulSet) int {
	return getStartOrdinal(set) + int(*set.Spec.Replicas) - 1
}

// podInOrdinalRange 判断 Pod 序号是否落在该 StatefulSet 当前允许的范围内。
func podInOrdinalRange(pod *v1.Pod, set *apps.StatefulSet) bool {
	ordinal := getOrdinal(pod)
	return ordinal >= getStartOrdinal(set) && ordinal <= getEndOrdinal(set)
}

// getPodName 按 "<set 名>-<序号>" 格式生成子 Pod 名称。
func getPodName(set *apps.StatefulSet, ordinal int) string {
	return fmt.Sprintf("%s-%d", set.Name, ordinal)
}

// getPersistentVolumeClaimName 生成指定序号 Pod 所使用的 PVC 名称。
// claim 必须是 set 的 VolumeClaimTemplates 中声明的模板。
func getPersistentVolumeClaimName(set *apps.StatefulSet, claim *v1.PersistentVolumeClaim, ordinal int) string {
	// 名称格式被卷可用区打散启发式（ChooseZoneForVolume）依赖，不要随意更改。
	return fmt.Sprintf("%s-%s-%d", claim.Name, set.Name, ordinal)
}

// isMemberOf 判断 Pod 是否属于该 StatefulSet（仅比较父名称，不检查序号范围）。
func isMemberOf(set *apps.StatefulSet, pod *v1.Pod) bool {
	parent, _ := getParentNameAndOrdinal(pod)
	return parent == set.Name
}

// identityMatches 判断 Pod 是否具有作为 set 成员所需的合法身份与网络身份。
func identityMatches(set *apps.StatefulSet, pod *v1.Pod) bool {
	parent, ordinal := getParentNameAndOrdinal(pod)
	return ordinal >= 0 &&
		set.Name == parent &&
		pod.Name == getPodName(set, ordinal) &&
		pod.Namespace == set.Namespace &&
		pod.Labels[apps.StatefulSetPodNameLabel] == pod.Name
}

// storageMatches 判断 Pod 的卷是否覆盖了 set 模板所要求的全部 PVC。
// 无有效序号时直接返回 false。
func storageMatches(set *apps.StatefulSet, pod *v1.Pod) bool {
	ordinal := getOrdinal(pod)
	// 无有效序号时无法推导 PVC 名称，必然不匹配。
	if ordinal < 0 {
		return false
	}
	volumes := make(map[string]v1.Volume, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	for _, claim := range set.Spec.VolumeClaimTemplates {
		volume, found := volumes[claim.Name]
		if !found ||
			volume.VolumeSource.PersistentVolumeClaim == nil ||
			volume.VolumeSource.PersistentVolumeClaim.ClaimName !=
				getPersistentVolumeClaimName(set, &claim, ordinal) {
			return false
		}
	}
	return true
}

// getPersistentVolumeClaimRetentionPolicy 返回 PVC 保留策略；set 未配置时默认全部保留（Retain）。
func getPersistentVolumeClaimRetentionPolicy(set *apps.StatefulSet) apps.StatefulSetPersistentVolumeClaimRetentionPolicy {
	policy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if set.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		policy = *set.Spec.PersistentVolumeClaimRetentionPolicy
	}
	return policy
}

// matchesRef 判断对象是否与 ownerReference 匹配（名称、APIVersion、Kind 均一致）。
func matchesRef(ref *metav1.OwnerReference, obj metav1.Object, gvk schema.GroupVersionKind) bool {
	return gvk.GroupVersion().String() == ref.APIVersion && gvk.Kind == ref.Kind && ref.Name == obj.GetName()
}

// hasUnexpectedController 判断是否存在既非 set 也非 Pod 的第三方控制器占用该 PVC。
//   - Retain/Retain 策略下允许任意控制器，直接返回 false；
//   - set 与 Pod 作为控制器始终合法（策略可能已变更）；UID 不一致则视为异常，
//     以免误操作已被错误孤立的 PVC。
func hasUnexpectedController(claim *v1.PersistentVolumeClaim, set *apps.StatefulSet, pod *v1.Pod) bool {
	policy := getPersistentVolumeClaimRetentionPolicy(set)
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	if policy.WhenScaled == retain && policy.WhenDeleted == retain {
		// Retain 策略下由其他控制器管理 PVC 并无问题。
		return false
	}
	for _, ownerRef := range claim.GetOwnerReferences() {
		if matchesRef(&ownerRef, set, controllerKind) {
			if ownerRef.UID != set.GetUID() {
				// UID 不匹配意味着 Pod 曾被错误孤立；视作异常控制器以避免触碰相关 PVC。
				return true
			}
			// 这是我们自己，无需处理。
			continue
		}

		if matchesRef(&ownerRef, pod, podKind) {
			if ownerRef.UID != pod.GetUID() {
				// 与上面 set UID 不匹配同理，Pod UID 不匹配也视为异常。
				return true
			}
			// 这是我们自己，无需处理。
			continue
		}
		if ownerRef.Controller != nil && *ownerRef.Controller {
			// 这是其他控制器。
			return true
		}
	}
	return false
}

// hasNonControllerOwner 判断 set 或 Pod 是否为 PVC 的 owner 但不是 controller。
func hasNonControllerOwner(claim *v1.PersistentVolumeClaim, set *apps.StatefulSet, pod *v1.Pod) bool {
	for _, ownerRef := range claim.GetOwnerReferences() {
		if ownerRef.UID == set.GetUID() || ownerRef.UID == pod.GetUID() {
			if ownerRef.Controller == nil || !*ownerRef.Controller {
				return true
			}
		}
	}
	return false
}

// removeRefs 移除所有满足 predicate 的 ownerReference，返回过滤后的新列表。
func removeRefs(refs []metav1.OwnerReference, predicate func(ref *metav1.OwnerReference) bool) []metav1.OwnerReference {
	newRefs := []metav1.OwnerReference{}
	for _, ownerRef := range refs {
		if !predicate(&ownerRef) {
			newRefs = append(newRefs, ownerRef)
		}
	}
	return newRefs
}

// isClaimOwnerUpToDate 判断 PVC 的 ownerReference 是否与 StatefulSet 的 PVC 删除策略一致。
//   - 存在过期引用或意外控制器时返回 true，以避免触碰状态未知的 PVC；
//   - 否则按以下四种保留/删除组合校验 set 与 Pod 的引用：
//   - 扩容与 set 删除均保留：无 owner 引用；
//   - 扩容保留、set 删除时删除：仅 set 上有 owner 引用；
//   - 扩容删除、set 删除时保留：仅 Pod 上有 owner 引用；
//   - 扩容与 set 删除均删除：set 与 Pod 上均有 owner 引用。
func isClaimOwnerUpToDate(logger klog.Logger, claim *v1.PersistentVolumeClaim, set *apps.StatefulSet, pod *v1.Pod) bool {
	if hasStaleOwnerRef(claim, set, controllerKind) || hasStaleOwnerRef(claim, pod, podKind) {
		// 该 PVC 由旧版本控制器管理，不应触碰。
		return true
	}

	if hasUnexpectedController(claim, set, pod) {
		if hasOwnerRef(claim, set) || hasOwnerRef(claim, pod) {
			// 需要清理冲突的控制器引用。
			return false
		}
		// 引用本身无问题，但不要在意外控制器之上再添加新的控制器。
		return true
	}

	if hasNonControllerOwner(claim, set, pod) {
		// 存在 owner 但无 controller，需要更新。
		return false
	}

	policy := getPersistentVolumeClaimRetentionPolicy(set)
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	default:
		logger.Error(nil, "Unknown policy, treating as Retain", "policy", set.Spec.PersistentVolumeClaimRetentionPolicy)
		// 未知策略按 Retain 处理，fallthrough 到 Retain/Retain 分支。
		fallthrough
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) ||
			hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		if !hasOwnerRef(claim, set) ||
			hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return false
		}
		podScaledDown := !podInOrdinalRange(pod, set)
		if podScaledDown != hasOwnerRef(claim, pod) {
			return false
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		podScaledDown := !podInOrdinalRange(pod, set)
		// Pod 被缩容时不应保留 set 引用、应保留 Pod 引用；未被缩容时相反。
		if podScaledDown == hasOwnerRef(claim, set) {
			return false
		}
		if podScaledDown != hasOwnerRef(claim, pod) {
			return false
		}
	}
	return true
}

// updateClaimOwnerRefForSetAndPod 根据 StatefulSet 的删除策略更新 PVC 的 ownerReference。
// 调用前应先通过 isClaimOwnerUpToDate 判断，以避免昂贵的更新操作。
func updateClaimOwnerRefForSetAndPod(logger klog.Logger, claim *v1.PersistentVolumeClaim, set *apps.StatefulSet, pod *v1.Pod) {
	refs := claim.GetOwnerReferences()

	unexpectedController := hasUnexpectedController(claim, set, pod)

	// 先清除指向本 set 与 Pod 的旧引用，再根据策略重新添加。
	refs = removeRefs(refs, func(ref *metav1.OwnerReference) bool {
		return matchesRef(ref, set, controllerKind) || matchesRef(ref, pod, podKind)
	})

	if unexpectedController {
		// 存在意外控制器时只清除自身引用，不再创建新的。
		claim.SetOwnerReferences(refs)
		return
	}

	policy := getPersistentVolumeClaimRetentionPolicy(set)
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	default:
		logger.Error(nil, "Unknown policy, treating as Retain", "policy", set.Spec.PersistentVolumeClaimRetentionPolicy)
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		refs = addControllerRef(refs, set, controllerKind)
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		// Pod 被缩容时才添加 Pod 作为 owner。
		podScaledDown := !podInOrdinalRange(pod, set)
		if podScaledDown {
			refs = addControllerRef(refs, pod, podKind)
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		podScaledDown := !podInOrdinalRange(pod, set)
		// Pod 被缩容时添加 Pod 引用。
		if podScaledDown {
			refs = addControllerRef(refs, pod, podKind)
		}
		// Pod 未被缩容时添加 set 引用。
		if !podScaledDown {
			refs = addControllerRef(refs, set, controllerKind)
		}
	}
	claim.SetOwnerReferences(refs)
}

// hasOwnerRef 判断 target 是否含有指向 owner（按 UID）的 ownerReference。
// 不检查 owner 是否为 controller。
func hasOwnerRef(target, owner metav1.Object) bool {
	ownerUID := owner.GetUID()
	for _, ownerRef := range target.GetOwnerReferences() {
		if ownerRef.UID == ownerUID {
			return true
		}
	}
	return false
}

// hasStaleOwnerRef 判断 target 中指向 obj 的引用是否过期，即名称/类型匹配但 UID 不一致。
func hasStaleOwnerRef(target *v1.PersistentVolumeClaim, obj metav1.Object, gvk schema.GroupVersionKind) bool {
	for _, ownerRef := range target.GetOwnerReferences() {
		if matchesRef(&ownerRef, obj, gvk) {
			return ownerRef.UID != obj.GetUID()
		}
	}
	return false
}

// addControllerRef 在必要时把 owner 作为 controller 加入引用列表。
func addControllerRef(refs []metav1.OwnerReference, owner metav1.Object, gvk schema.GroupVersionKind) []metav1.OwnerReference {
	for _, ref := range refs {
		if ref.UID == owner.GetUID() {
			// 引用已存在；由于修改前已清理过自身引用，此时它已是合适的 controller。
			return refs
		}
	}

	return append(refs, *metav1.NewControllerRef(owner, gvk))
}

// getPersistentVolumeClaims 按 set 的模板构造 PVC 映射（模板名 -> PVC），
// 每个 PVC 的名称由 getPersistentVolumeClaimName 结合 Pod 序号生成。
func getPersistentVolumeClaims(set *apps.StatefulSet, pod *v1.Pod) map[string]v1.PersistentVolumeClaim {
	ordinal := getOrdinal(pod)
	templates := set.Spec.VolumeClaimTemplates
	claims := make(map[string]v1.PersistentVolumeClaim, len(templates))
	for i := range templates {
		claim := templates[i].DeepCopy()
		claim.Name = getPersistentVolumeClaimName(set, claim, ordinal)
		claim.Namespace = set.Namespace
		if claim.Labels != nil {
			for key, value := range set.Spec.Selector.MatchLabels {
				claim.Labels[key] = value
			}
		} else {
			claim.Labels = set.Spec.Selector.MatchLabels
		}
		claims[templates[i].Name] = *claim
	}
	return claims
}

// updateStorage 将 Pod 的卷更新为与 set 的 PVC 模板一致；
// 与模板冲突的本地卷会被替换为符合模板的卷。
func updateStorage(set *apps.StatefulSet, pod *v1.Pod) {
	currentVolumes := pod.Spec.Volumes
	claims := getPersistentVolumeClaims(set, pod)
	newVolumes := make([]v1.Volume, 0, len(claims))
	for name, claim := range claims {
		newVolumes = append(newVolumes, v1.Volume{
			Name: name,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim.Name,
					// TODO: 有来源定义时，用该定义设置此值。
					ReadOnly: false,
				},
			},
		})
	}
	for i := range currentVolumes {
		if _, ok := claims[currentVolumes[i].Name]; !ok {
			newVolumes = append(newVolumes, currentVolumes[i])
		}
	}
	pod.Spec.Volumes = newVolumes
}

// initIdentity 初始化 Pod 身份。Hostname 与 Subdomain 属于不可变字段，
// 仅在 Pod 首次创建时设置，更新时不能修改。
func initIdentity(set *apps.StatefulSet, pod *v1.Pod) {
	updateIdentity(set, pod)
	// 不可变字段仅在创建时设置，更新时不修改。
	pod.Spec.Hostname = pod.Name
	pod.Spec.Subdomain = set.Spec.ServiceName
}

// updateIdentity 按 set 名称与 headless service 更新 Pod 的名称、命名空间、
// hostname 相关标签以及序号标签。
func updateIdentity(set *apps.StatefulSet, pod *v1.Pod) {
	ordinal := getOrdinal(pod)
	pod.Name = getPodName(set, ordinal)
	pod.Namespace = set.Namespace
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[apps.StatefulSetPodNameLabel] = pod.Name
	pod.Labels[apps.PodIndexLabel] = strconv.Itoa(ordinal)
}

// isRunningAndReady 判断 Pod 是否处于 Running 且已 Ready。
func isRunningAndReady(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodRunning && podutil.IsPodReady(pod)
}

// isRunningAndAvailable 判断 Pod 是否可用至少 minReadySeconds（用于就绪门槛）。
func isRunningAndAvailable(pod *v1.Pod, minReadySeconds int32, now time.Time) bool {
	return podutil.IsPodAvailable(pod, minReadySeconds, metav1.Time{Time: now})
}

// isCreated 判断 Pod 是否已被 API server 创建并维护（Phase 非空）。
func isCreated(pod *v1.Pod) bool {
	return pod.Status.Phase != ""
}

// isPending 判断 Pod 是否处于 Pending。
func isPending(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodPending
}

// isTerminalPhase 判断 Pod 是否处于 Failed 或 Succeeded 终态。
func isTerminalPhase(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded
}

// isTerminating 判断 Pod 是否处于终止中（DeletionTimestamp 已设置）。
func isTerminating(pod *v1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

// isUnavailable 判断 Pod 不可用或正处于终止中。
func isUnavailable(pod *v1.Pod, minReadySeconds int32, now time.Time) bool {
	return !podutil.IsPodAvailable(pod, minReadySeconds, metav1.Time{Time: now}) || isTerminating(pod)
}

// allowsBurst 判断是否允许并行创建 Pod（alpha burst 注解或 Parallel 策略）。
func allowsBurst(set *apps.StatefulSet) bool {
	return set.Spec.PodManagementPolicy == apps.ParallelPodManagement
}

// setPodRevision 通过 StatefulSetRevisionLabel 标签设置 Pod 的 revision。
func setPodRevision(pod *v1.Pod, revision string) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[apps.StatefulSetRevisionLabel] = revision
}

// getPodRevision 读取 Pod 的 revision 标签；无标签时返回空字符串。
func getPodRevision(pod *v1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[apps.StatefulSetRevisionLabel]
}

// newStatefulSetPod 根据 set 的模板创建一个新 Pod，并按序号生成其身份。
func newStatefulSetPod(set *apps.StatefulSet, ordinal int) *v1.Pod {
	pod, _ := controller.GetPodFromTemplate(&set.Spec.Template, set, metav1.NewControllerRef(set, controllerKind))
	pod.Name = getPodName(set, ordinal)
	initIdentity(set, pod)
	updateStorage(set, pod)
	return pod
}

// newVersionedStatefulSetPod 为指定序号创建 Pod，并按更新策略决定使用当前版本还是更新版本模板：
//   - RollingUpdate 下序号小于 partition（或 currentReplicas）的 Pod 使用 currentSet/currentRevision；
//   - 其余 Pod 使用 updateSet/updateRevision。
func newVersionedStatefulSetPod(currentSet, updateSet *apps.StatefulSet, currentRevision, updateRevision string, ordinal int) *v1.Pod {
	// 位于 partition 之前的 Pod 维持当前版本，保证滚动更新按顺序推进。
	if currentSet.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType &&
		(currentSet.Spec.UpdateStrategy.RollingUpdate == nil && ordinal < (getStartOrdinal(currentSet)+int(currentSet.Status.CurrentReplicas))) ||
		(currentSet.Spec.UpdateStrategy.RollingUpdate != nil && ordinal < (getStartOrdinal(currentSet)+int(*currentSet.Spec.UpdateStrategy.RollingUpdate.Partition))) {
		pod := newStatefulSetPod(currentSet, ordinal)
		setPodRevision(pod, currentRevision)
		return pod
	}
	pod := newStatefulSetPod(updateSet, ordinal)
	setPodRevision(pod, updateRevision)
	return pod
}

// getPatch 生成可将 StatefulSet 恢复到旧版本的 strategic merge patch。
// 当前仅保存 PodSpecTemplate；将 $patch 设为 replace 以确保整个模板被替换。
func getPatch(set *apps.StatefulSet) ([]byte, error) {
	data, err := runtime.Encode(patchCodec, set)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	err = json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})
	spec := raw["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	specCopy["template"] = template
	// 使用 replace 指令确保恢复时整个模板被覆盖，而非按字段合并。
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

// newRevision 创建一个 ControllerRevision，其中保存能重新应用 set 目标状态的 patch。
// set 的注解会被复制到 revision 上，便于追溯。
func newRevision(set *apps.StatefulSet, revision int64, collisionCount *int32) (*apps.ControllerRevision, error) {
	patch, err := getPatch(set)
	if err != nil {
		return nil, err
	}
	cr, err := history.NewControllerRevision(set,
		controllerKind,
		set.Spec.Template.Labels,
		runtime.RawExtension{Raw: patch},
		revision,
		collisionCount)
	if err != nil {
		return nil, err
	}
	// 复制 set 的注解，保证 revision 携带完整元信息。
	if cr.ObjectMeta.Annotations == nil {
		cr.ObjectMeta.Annotations = make(map[string]string)
	}
	for key, value := range set.Annotations {
		cr.ObjectMeta.Annotations[key] = value
	}
	return cr, nil
}

// ApplyRevision 将 revision 中保存的 .Spec 恢复到 set 上，返回一个新的 StatefulSet。
func ApplyRevision(set *apps.StatefulSet, revision *apps.ControllerRevision) (*apps.StatefulSet, error) {
	clone := set.DeepCopy()
	patched, err := strategicpatch.StrategicMergePatch([]byte(runtime.EncodeOrDie(patchCodec, clone)), revision.Data.Raw, clone)
	if err != nil {
		return nil, err
	}
	restoredSet := &apps.StatefulSet{}
	err = json.Unmarshal(patched, restoredSet)
	if err != nil {
		return nil, err
	}
	// 仅恢复 Spec，其余字段沿用原 set。
	clone.Spec = restoredSet.Spec
	return clone, nil
}

// nextRevision 返回下一个可用的 revision 号：无历史 revision 时为 1，
// 否则为最大 revision 加 1。调用方需保证 revisions 已按 Revision 排序。
func nextRevision(revisions []*apps.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

// inconsistentStatus 判断期望状态与当前状态是否不一致，
// 包括 ObservedGeneration 及各副本计数、revision、Conditions 的差异。
func inconsistentStatus(set *apps.StatefulSet, status *apps.StatefulSetStatus) bool {
	return status.ObservedGeneration > set.Status.ObservedGeneration ||
		status.Replicas != set.Status.Replicas ||
		status.CurrentReplicas != set.Status.CurrentReplicas ||
		status.ReadyReplicas != set.Status.ReadyReplicas ||
		status.UpdatedReplicas != set.Status.UpdatedReplicas ||
		status.CurrentRevision != set.Status.CurrentRevision ||
		status.AvailableReplicas != set.Status.AvailableReplicas ||
		status.UpdateRevision != set.Status.UpdateRevision ||
		!reflect.DeepEqual(set.Status.Conditions, status.Conditions)
}

// completeUpdate 在所有副本都已更新到 updateRevision 时完成更新：
// 将 currentRevision/currentReplicas 推进到更新版本；
// Recreate 策略下还会将 Progressing 条件置为 Completed。
func completeUpdate(set *apps.StatefulSet, status *apps.StatefulSetStatus) {
	// 只有副本数、就绪数、总数全部达到期望值时更新才算完成。
	if status.UpdatedReplicas == *set.Spec.Replicas &&
		status.ReadyReplicas == *set.Spec.Replicas &&
		status.Replicas == *set.Spec.Replicas {
		status.CurrentReplicas = status.UpdatedReplicas
		status.CurrentRevision = status.UpdateRevision

		// Recreate 策略更新完成后同步 Progressing 条件。
		if getStatefulSetCondition(*status, apps.StatefulSetProgressing) != nil &&
			set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType &&
			utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetRecreateStrategy) {
			setProgressingCondition(status, v1.ConditionFalse, RecreateCompletedReason, "All pods recreated successfully")
		}
	}
}

// ascendingOrdinal 按序号升序排列 Pod；非 StatefulSet 创建的 Pod 序号为 -1，会被排到最前。
type ascendingOrdinal []*v1.Pod

// Len 返回待排序 Pod 数量。
func (ao ascendingOrdinal) Len() int {
	return len(ao)
}

// Swap 交换两个 Pod 的位置。
func (ao ascendingOrdinal) Swap(i, j int) {
	ao[i], ao[j] = ao[j], ao[i]
}

// Less 按序号升序比较。
func (ao ascendingOrdinal) Less(i, j int) bool {
	return getOrdinal(ao[i]) < getOrdinal(ao[j])
}

// descendingOrdinal 按序号降序排列 Pod；非 StatefulSet 创建的 Pod 序号为 -1，会被排到最后。
type descendingOrdinal []*v1.Pod

// Len 返回待排序 Pod 数量。
func (do descendingOrdinal) Len() int {
	return len(do)
}

// Swap 交换两个 Pod 的位置。
func (do descendingOrdinal) Swap(i, j int) {
	do[i], do[j] = do[j], do[i]
}

// Less 按序号降序比较。
func (do descendingOrdinal) Less(i, j int) bool {
	return getOrdinal(do[i]) > getOrdinal(do[j])
}

// getStatefulSetMaxUnavailable 根据副本数与 maxUnavailable 配置计算实际不可用副本上限。
//   - 未配置时默认 1；百分比向下取整后至少保证 1；
//   - API 校验已保证整数 >1 或百分比在 (0%,100%]，故无需处理其他情况。
func getStatefulSetMaxUnavailable(maxUnavailable *intstr.IntOrString, replicaCount int) (int, error) {
	maxUnavailableNum, err := intstr.GetScaledValueFromIntOrPercent(intstr.ValueOrDefault(maxUnavailable, intstr.FromInt32(1)), replicaCount, false)
	if err != nil {
		return 0, err
	}
	// 小副本数配合百分比可能向下取整为 0，这里强制至少为 1。
	if maxUnavailableNum < 1 {
		maxUnavailableNum = 1
	}
	return maxUnavailableNum, nil
}

// setProgressingCondition 设置 StatefulSet 状态的 Progressing 条件。
func setProgressingCondition(status *apps.StatefulSetStatus, conditionStatus v1.ConditionStatus, reason, message string) {
	condition := newStatefulSetCondition(apps.StatefulSetProgressing, conditionStatus, reason, message)
	setStatefulSetCondition(status, *condition)
}

// newStatefulSetCondition 创建一个新的 StatefulSet 条件。
func newStatefulSetCondition(condType apps.StatefulSetConditionType, status v1.ConditionStatus, reason, message string) *apps.StatefulSetCondition {
	return &apps.StatefulSetCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getStatefulSetCondition 返回指定类型的条件；不存在时返回 nil。
func getStatefulSetCondition(status apps.StatefulSetStatus, condType apps.StatefulSetConditionType) *apps.StatefulSetCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &status.Conditions[i]
		}
	}
	return nil
}

// setStatefulSetCondition 更新状态中的条件；若状态、原因与消息均未变化则不更新，
// 避免无意义的 LastTransitionTime 变更。
func setStatefulSetCondition(status *apps.StatefulSetStatus, condition apps.StatefulSetCondition) {
	currentCond := getStatefulSetCondition(*status, condition.Type)
	if currentCond == nil {
		status.Conditions = append(status.Conditions, condition)
		return
	}

	// 内容完全相同则无需更新，避免触发无意义的状态写入。
	if currentCond.Status == condition.Status &&
		currentCond.Reason == condition.Reason &&
		currentCond.Message == condition.Message {
		return
	}
	// 状态未变化时保留原有 LastTransitionTime，只更新原因/消息。
	if currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	currentCond.LastTransitionTime = condition.LastTransitionTime
	currentCond.Status = condition.Status
	currentCond.Reason = condition.Reason
	currentCond.Message = condition.Message
}
