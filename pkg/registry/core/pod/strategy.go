/*
Copyright 2014 The Kubernetes Authors.

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

package pod

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	netutils "k8s.io/utils/net"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apiv1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper/qos"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/client"
)

// podStrategy 定义 Pod 主资源在 REST 创建、更新、删除时的默认策略：字段重置、校验、告警和优雅删除。
type podStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 Pod 主资源的默认写入策略，所有普通创建/更新请求都会经过它的 Prepare、Validate 和 Generation 处理。
// Strategy 是通过 REST API 创建和更新 Pod 时使用的默认策略。
var Strategy = podStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true：Pod 是命名空间级资源。
// Pod 属于命名空间级资源，所有对象 key 都需要带 namespace。
func (podStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回状态子资源写入时由策略重置、用户不能直接修改的字段集合。
// 主资源写入时由用户提交的 status 必须被重置，避免普通 Pod 更新绕过 pods/status 子资源修改状态。
func (podStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清理创建请求中不允许用户设置的字段，并初始化服务端管理的派生字段。
// PrepareForCreate 在持久化前清除用户无权指定的字段，并补齐依赖创建时机的派生状态。
func (podStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pod := obj.(*api.Pod)
	// 新对象的 Generation 从 1 开始，后续仅当期望状态发生变化时递增。
	pod.Generation = 1

	// 初始状态由 API Server 控制；刚创建的 Pod 只能进入 Pending，调度/运行状态由控制器和 Kubelet 回写。
	pod.Status = api.PodStatus{
		Phase: api.PodPending,
	}

	// 根据当前 Feature Gate 删除尚不可使用的字段，防止新版本客户端在旧能力关闭时写入隐藏字段。
	podutil.DropDisabledPodFields(pod, nil)

	// 依次规范调度门控条件、亲和性和拓扑分布选择器，并处理 AppArmor 版本兼容与 Pod 级资源默认值。
	applySchedulingGatedCondition(pod)
	mutatePodAffinity(pod)
	mutateTopologySpreadConstraints(pod)
	applyAppArmorVersionSkew(ctx, pod)
	podutil.DefaultPodLevelResources(pod)

	// QoS 类别由最终资源 requests/limits 推导，不接受客户端自行设置。
	pod.Status.QOSClass = qos.GetPodQOS(pod)
}

// PrepareForUpdate 恢复服务端管理的字段，只保留 Pod 主资源允许修改的内容。
// PrepareForUpdate 保留服务端管理的状态字段，并只允许本次 REST 操作修改 Pod 主资源允许修改的部分。
func (podStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	// 普通 pods 更新不能修改 status；以旧对象状态覆盖请求内容，状态变更必须走子资源。
	newPod.Status = oldPod.Status

	// 兼容不认识 Pod 级资源字段的旧客户端：它们可能在整体更新时把字段丢掉，服务端需保留旧值。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourcesFixDefaulting) {
		if newPod.Spec.Resources == nil && oldPod.Spec.Resources != nil {
			// 兼容不认识 Pod 级资源（PLR）的旧客户端：请求完全省略该字段时保留旧值。
			newPod.Spec.Resources = oldPod.Spec.Resources.DeepCopy()
		}
	}

	// 最后按 Feature Gate 清理不可写字段，并根据 Spec 是否变化维护 Generation。
	podutil.DropDisabledPodFields(newPod, oldPod)
	updatePodGeneration(newPod, oldPod)
}

// Validate 校验新创建的 Pod。
// Validate 在创建策略准备字段之后执行 Pod 创建规则校验，返回可聚合的字段级错误。
func (podStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	pod := obj.(*api.Pod)
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&pod.Spec, nil, &pod.ObjectMeta, nil)
	opts.ResourceIsPod = true
	return corevalidation.ValidatePodCreate(pod, opts)
}

// WarningsOnCreate 返回创建 Pod 时发现的非阻断性警告。
func (podStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newPod := obj.(*api.Pod)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newPod.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in the Pod's hostname, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, podutil.GetWarningsForPod(ctx, newPod, nil)...)
	return warnings
}

// Canonicalize 在校验后规范化 Pod 对象；当前策略无需额外处理。
func (podStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：Pod 不能通过更新请求隐式创建。
func (podStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 执行普通 Pod 更新校验，并结合新旧对象判断不可变字段和特性兼容性。
// ValidateUpdate 同时传入新旧 Spec/Meta，使校验器能区分不可变字段、已启用特性和历史兼容字段。
func (podStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	// HugePagesDownwardAPI 已启用，或旧 Pod 已经使用该能力时，允许更新请求继续保留相关 Downward API 配置。
	pod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&pod.Spec, &oldPod.Spec, &pod.ObjectMeta, &oldPod.ObjectMeta)
	opts.ResourceIsPod = true
	return corevalidation.ValidatePodUpdate(obj.(*api.Pod), old.(*api.Pod), opts)
}

// WarningsOnUpdate 返回临时容器更新时的非阻断性警告。
func (podStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	// 跳过 Pod 更新告警：用户通常不会直接维护 Pod，而且更新路径可能非常高频，不应承担额外告警计算开销。
	return nil
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的 Pod 无条件更新。
func (podStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// CheckGracefulDelete 判断 Pod 是否需要优雅删除，并将最终确定的宽限期写入 DeleteOptions。
// CheckGracefulDelete 计算最终删除宽限期；是否立即删除取决于请求、Pod 配置、是否已调度和当前终态。
func (podStrategy) CheckGracefulDelete(ctx context.Context, obj runtime.Object, options *metav1.DeleteOptions) bool {
	// 没有 DeleteOptions 时无法向调用方回写宽限期，交由通用删除逻辑处理。
	if options == nil {
		return false
	}
	pod := obj.(*api.Pod)
	period := int64(0)
	// 用户显式指定了删除宽限期。
	if options.GracePeriodSeconds != nil {
		period = *options.GracePeriodSeconds
	} else {
		// 使用 Pod 配置的默认宽限期；若未配置，则立即删除（宽限期为 0）。
		if pod.Spec.TerminationGracePeriodSeconds != nil {
			period = *pod.Spec.TerminationGracePeriodSeconds
		}
	}
	// 尚未绑定 Node 的 Pod 没有需要通知 Kubelet 的本地容器，直接删除即可。
	if len(pod.Spec.NodeName) == 0 {
		period = 0
	}
	// 已经成功或失败的终态 Pod 不再有容器清理工作，避免重复等待终止宽限期。
	if pod.Status.Phase == api.PodFailed || pod.Status.Phase == api.PodSucceeded {
		period = 0
	}

	if period < 0 {
		period = 1
	}

	// 把最终决定写回 DeleteOptions，让后续存储层执行与策略计算一致的删除行为。
	options.GracePeriodSeconds = &period
	return true
}

type podStatusStrategy struct {
	podStrategy
}

// StatusStrategy 将 Pod 状态更新策略导出给 storage 包使用。
var StatusStrategy = podStatusStrategy{Strategy}

// GetResetFields 返回状态子资源写入时由策略重置、用户不能直接修改的字段集合。
func (podStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata", "deletionTimestamp"),
			fieldpath.MakePathOrDie("metadata", "ownerReferences"),
		),
	}
}

// Status 子资源只开放状态更新：Spec、删除时间和 OwnerReferences 均从旧对象恢复，避免借此绕过主资源权限。
func (podStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	// /status 不应更改期望状态；保留旧 Spec 和元数据系统字段。
	newPod.Spec = oldPod.Spec
	newPod.DeletionTimestamp = nil

	// 禁止 pods/status 修改 OwnerReferences：旧版 Kubelet 曾以可能破坏垃圾回收的方式改写这些字段。
	newPod.OwnerReferences = oldPod.OwnerReferences
	// Pod QoS 在创建时由 kube-apiserver 计算，之后不可变。
	// 为兼容旧版 Kubelet 曾在拒绝 Pod 时丢弃该字段的行为，这里需要从旧对象回填。
	if newPod.Status.QOSClass == "" {
		newPod.Status.QOSClass = oldPod.Status.QOSClass
	}

	// 旧客户端可能会丢弃自己不认识的 DRA 状态字段；nil 表示未提交该字段，必须保留旧值。
	// 旧版 Kubelet 在更新 pods/status 时可能剥离其不认识的字段。
	// 对切片字段而言，非 nil 的空切片仍表示客户端明确要求清空字段。
	if newPod.Status.ResourceClaimStatuses == nil && oldPod.Status.ResourceClaimStatuses != nil {
		newPod.Status.ResourceClaimStatuses = oldPod.Status.ResourceClaimStatuses
	}
	if newPod.Status.ExtendedResourceClaimStatus == nil && oldPod.Status.ExtendedResourceClaimStatus != nil {
		newPod.Status.ExtendedResourceClaimStatus = oldPod.Status.ExtendedResourceClaimStatus
	}
	if newPod.Status.NodeAllocatableResourceClaimStatuses == nil && oldPod.Status.NodeAllocatableResourceClaimStatuses != nil {
		newPod.Status.NodeAllocatableResourceClaimStatuses = oldPod.Status.NodeAllocatableResourceClaimStatuses
	}

	preserveOldObservedGeneration(newPod, oldPod)
	podutil.DropDisabledPodFields(newPod, oldPod)
}

// 如果客户端尝试清空 Pod Status 或 Condition 中的 observedGeneration，则保留旧值，避免观测版本信息回退。
// preserveOldObservedGeneration 防止状态客户端通过省略字段把 observedGeneration 回退为 0。
func preserveOldObservedGeneration(newPod, oldPod *api.Pod) {
	if newPod.Status.ObservedGeneration == 0 {
		newPod.Status.ObservedGeneration = oldPod.Status.ObservedGeneration
	}

	// 同一 Condition Type 可能重复出现，因此按类型保存一个队列，并在新条件中按顺序匹配旧值。
	// 按 Condition Type 保存队列，因为校验规则允许同一类型的 Condition 出现多次。
	oldConditionGenerations := map[api.PodConditionType][]int64{}
	for _, oldCondition := range oldPod.Status.Conditions {
		oldConditionGenerations[oldCondition.Type] = append(oldConditionGenerations[oldCondition.Type], oldCondition.ObservedGeneration)
	}

	// 对新 Status 中未设置 observedGeneration 的 Condition，沿用旧 Condition 中对应的值。
	for i, newCondition := range newPod.Status.Conditions {
		oldGeneration := int64(0)
		if oldGenerations, ok := oldConditionGenerations[newCondition.Type]; ok && len(oldGenerations) > 0 {
			oldGeneration = oldGenerations[0]
			oldConditionGenerations[newCondition.Type] = oldGenerations[1:]
		}

		if newCondition.ObservedGeneration == 0 {
			newPod.Status.Conditions[i].ObservedGeneration = oldGeneration
		}
	}
}

// Status 更新使用专用校验器：它只验证允许变更的状态字段，并检查状态转换规则。
func (podStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	pod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&pod.Spec, &oldPod.Spec, &pod.ObjectMeta, &oldPod.ObjectMeta)
	opts.ResourceIsPod = true

	return corevalidation.ValidatePodStatusUpdate(obj.(*api.Pod), old.(*api.Pod), opts)
}

// WarningsOnUpdate 返回对应更新操作的非阻断性警告。
func (podStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	pod := obj.(*api.Pod)
	var warnings []string

	for i, podIP := range pod.Status.PodIPs {
		warnings = append(warnings, utilvalidation.GetWarningsForIP(field.NewPath("status", "podIPs").Index(i).Child("ip"), podIP.IP)...)
	}
	for i, hostIP := range pod.Status.HostIPs {
		warnings = append(warnings, utilvalidation.GetWarningsForIP(field.NewPath("status", "hostIPs").Index(i).Child("ip"), hostIP.IP)...)
	}

	return warnings
}

type podEphemeralContainersStrategy struct {
	podStrategy

	resetFieldsFilter fieldpath.Filter
}

// EphemeralContainersStrategy 将临时容器子资源策略导出给 storage 包使用。
var EphemeralContainersStrategy = podEphemeralContainersStrategy{
	podStrategy: Strategy,
	resetFieldsFilter: fieldpath.NewIncludeMatcherFilter(
		fieldpath.MakePrefixMatcherOrDie("spec", "ephemeralContainers"),
	),
}

// dropNonEphemeralContainerUpdates 丢弃除 Pod.Spec.EphemeralContainers 和指定元数据之外的所有变更。
// dropNonEphemeralContainerUpdates 把请求中除 EphemeralContainers 和允许的元数据之外的变更全部恢复为旧值。
func dropNonEphemeralContainerUpdates(newPod, oldPod *api.Pod) *api.Pod {
	// 先暂存唯一允许新增/修改的字段，再整体恢复旧 Spec、Status 和状态元数据，最后放回临时容器列表。
	newEphemeralContainerSpec := newPod.Spec.EphemeralContainers
	newPod.Spec = oldPod.Spec
	newPod.Status = oldPod.Status
	metav1.ResetObjectMetaForStatus(&newPod.ObjectMeta, &oldPod.ObjectMeta)
	newPod.Spec.EphemeralContainers = newEphemeralContainerSpec
	return newPod
}

// 临时容器子资源先剥离非目标字段，再执行禁用字段清理和 Generation 更新。
func (podEphemeralContainersStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)

	*newPod = *dropNonEphemeralContainerUpdates(newPod, oldPod)
	podutil.DropDisabledPodFields(newPod, oldPod)
	updatePodGeneration(newPod, oldPod)
}

func (podEphemeralContainersStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&newPod.Spec, &oldPod.Spec, &newPod.ObjectMeta, &oldPod.ObjectMeta)
	opts.ResourceIsPod = true
	return corevalidation.ValidatePodEphemeralContainersUpdate(newPod, oldPod, opts)
}

// WarningsOnUpdate 返回本次更新的警告信息。
func (podEphemeralContainersStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetResetFieldsFilter 返回该策略重置的字段过滤集合，用户不能直接修改这些字段。
func (p podEphemeralContainersStrategy) GetResetFieldsFilter() map[fieldpath.APIVersion]fieldpath.Filter {
	return map[fieldpath.APIVersion]fieldpath.Filter{
		"v1": p.resetFieldsFilter,
	}
}

type podResizeStrategy struct {
	podStrategy

	resetFieldsFilter fieldpath.Filter
}

// ResizeStrategy 将 Pod 原地资源扩缩容子资源策略导出给 storage 包使用。
var ResizeStrategy = podResizeStrategy{
	podStrategy: Strategy,
	resetFieldsFilter: fieldpath.NewIncludeMatcherFilter(
		fieldpath.MakePrefixMatcherOrDie("spec", "containers", fieldpath.MatchAnyPathElement(), "resources"),
		fieldpath.MakePrefixMatcherOrDie("spec", "containers", fieldpath.MatchAnyPathElement(), "resizePolicy"),
		fieldpath.MakePrefixMatcherOrDie("spec", "initContainers", fieldpath.MatchAnyPathElement(), "resources"),
		fieldpath.MakePrefixMatcherOrDie("spec", "initContainers", fieldpath.MatchAnyPathElement(), "resizePolicy"),
	),
}

// dropNonResizeUpdates 丢弃除普通容器/InitContainer 资源、ResizePolicy 和指定元数据之外的变更。
// 启用 InPlacePodLevelResourcesVerticalScaling 后，还会保留 Pod 级资源字段。
// dropNonResizeUpdates 只保留容器资源/ResizePolicy，以及 Feature Gate 允许的 Pod 级资源和特定卷更新。
func dropNonResizeUpdates(newPod, oldPod *api.Pod) *api.Pod {
	// 扩缩容请求不允许新增、删除、重排或重命名容器。
	// 一旦检测到这些变化，就保留请求中的 PodSpec 原样返回，由后续校验识别并拒绝该更新。
	if len(newPod.Spec.Containers) != len(oldPod.Spec.Containers) || len(newPod.Spec.InitContainers) != len(oldPod.Spec.InitContainers) {
		return newPod
	}

	// 暂存请求中的 Pod 级资源字段，后续在恢复其他 Spec 字段后再按特性开关决定是否保留。
	newPodResources := newPod.Spec.Resources

	// 先分别提取允许变更的 Volume、普通容器和 InitContainer 字段，再把其余 PodSpec 恢复为旧值。
	newVolumes := dropNonResizeUpdatesForVolumes(newPod.Spec.Volumes, oldPod.Spec.Volumes)

	containers := dropNonResizeUpdatesForContainers(newPod.Spec.Containers, oldPod.Spec.Containers)
	initContainers := dropNonResizeUpdatesForContainers(newPod.Spec.InitContainers, oldPod.Spec.InitContainers)

	newPod.Spec = oldPod.Spec
	// 仅当 PodLevelResources 和 InPlacePodLevelResourcesVerticalScaling 均启用时，
	// 才将暂存的 Pod 级资源请求恢复到新 Pod 的 Spec。
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		newPod.Spec.Resources = newPodResources
	}

	newPod.Status = oldPod.Status
	metav1.ResetObjectMetaForStatus(&newPod.ObjectMeta, &oldPod.ObjectMeta)

	newPod.Spec.Containers = containers
	newPod.Spec.InitContainers = initContainers
	newPod.Spec.Volumes = newVolumes

	return newPod
}

// 对照新旧容器列表，只有名称和顺序保持不变时才合并 Resources 与 ResizePolicy；重排/重命名交给校验器拒绝。
func dropNonResizeUpdatesForContainers(new, old []api.Container) []api.Container {
	if len(new) == 0 {
		return new
	}

	oldCopyWithMergedResources := make([]api.Container, len(old))
	copy(oldCopyWithMergedResources, old)

	for i, ctr := range new {
		if oldCopyWithMergedResources[i].Name != new[i].Name {
			// 检测到容器被重排或重命名，这类变更不允许通过 resize 子资源提交；交由校验器拒绝。
			return new
		}
		oldCopyWithMergedResources[i].Resources = ctr.Resources
		oldCopyWithMergedResources[i].ResizePolicy = ctr.ResizePolicy
	}

	return oldCopyWithMergedResources
}

// 仅启用内存后备卷原地垂直扩缩容时，才允许 resize 请求携带新 Volume 列表。
func dropNonResizeUpdatesForVolumes(new, old []api.Volume) []api.Volume {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		return old
	}
	return new
}

// Resize 子资源仅保留允许的资源字段；随后补齐新资源默认值并更新 Generation。
func (podResizeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)

	*newPod = *dropNonResizeUpdates(newPod, oldPod)

	// 通过 resize 设置或更新 Pod 级资源后执行默认值补全，确保未显式管理的资源（例如只设置 CPU 时的内存）也完整且一致。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourcesFixDefaulting) {
		if newPod.Spec.Resources == nil && oldPod.Spec.Resources != nil {
			// 对于可能不认识 PodLevelResources 字段、从而完全丢弃该字段的客户端，保留其已有的 PLR
			newPod.Spec.Resources = oldPod.Spec.Resources.DeepCopy()
		}

		if newPod.Spec.Resources != nil {
			podutil.DefaultPodLevelResources(newPod)
		}
	}

	podutil.DropDisabledPodFields(newPod, oldPod)
	updatePodGeneration(newPod, oldPod)
}

func (podResizeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPod := obj.(*api.Pod)
	oldPod := old.(*api.Pod)
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&newPod.Spec, &oldPod.Spec, &newPod.ObjectMeta, &oldPod.ObjectMeta)
	opts.ResourceIsPod = true
	return corevalidation.ValidatePodResize(newPod, oldPod, opts)
}

// WarningsOnUpdate 返回本次更新的警告信息。
func (podResizeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetResetFieldsFilter 返回该策略重置的字段过滤集合，用户不能直接修改这些字段。
func (p podResizeStrategy) GetResetFieldsFilter() map[fieldpath.APIVersion]fieldpath.Filter {
	return map[fieldpath.APIVersion]fieldpath.Filter{
		"v1": p.resetFieldsFilter,
	}
}

// GetAttrs 提取 Pod 的 Labels 和可筛选字段，供 LIST/WATCH 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod, ok := obj.(*api.Pod)
	if !ok {
		return nil, nil, fmt.Errorf("not a pod")
	}
	return labels.Set(pod.ObjectMeta.Labels), ToSelectableFields(pod), nil
}

// MatchPod 根据 LabelSelector 和 FieldSelector 创建 Pod 存储层选择谓词。
// MatchPod 把 Label/Field Selector 转成存储层 SelectionPredicate，并声明可通过 spec.nodeName 索引加速过滤。
func MatchPod(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:       label,
		Field:       field,
		GetAttrs:    GetAttrs,
		IndexFields: []string{"spec.nodeName"},
	}
}

// NodeNameTriggerFunc 返回对象的 spec.nodeName，用于判断 NodeName 索引是否需要更新。
func NodeNameTriggerFunc(obj runtime.Object) string {
	return obj.(*api.Pod).Spec.NodeName
}

// NodeNameIndexFunc 返回对象的 spec.nodeName，作为 Pod 按节点查询的索引键。
func NodeNameIndexFunc(obj interface{}) ([]string, error) {
	pod, ok := obj.(*api.Pod)
	if !ok {
		return nil, fmt.Errorf("not a pod")
	}
	return []string{pod.Spec.NodeName}, nil
}

// Indexers 返回 Pod 存储使用的缓存索引器。
// Indexers 注册 Pod 按 spec.nodeName 的缓存索引，支持按节点快速列出 Pod。
func Indexers() *cache.Indexers {
	return &cache.Indexers{
		storage.FieldIndex("spec.nodeName"): NodeNameIndexFunc,
	}
}

// ToSelectableFields 返回 Pod 支持 FieldSelector 查询的字段集合。
// TODO：字段选择器字段并非 Label，不应套用 Label 的校验规则。
// ToSelectableFields 暴露 API Server 支持的 Pod 字段选择器字段，并补充标准 ObjectMeta 字段。
func ToSelectableFields(pod *api.Pod) fields.Set {
	// 预先按预期元素数量分配容量，以减少创建 fields.Set 时的内存分配。
	// 如果在此处新增字段，或 ObjectMeta 相关字段数量发生变化，应同步调整容量。
	podSpecificFieldsSet := make(fields.Set, 10)
	podSpecificFieldsSet["spec.nodeName"] = pod.Spec.NodeName
	podSpecificFieldsSet["spec.restartPolicy"] = string(pod.Spec.RestartPolicy)
	podSpecificFieldsSet["spec.schedulerName"] = string(pod.Spec.SchedulerName)
	podSpecificFieldsSet["spec.serviceAccountName"] = string(pod.Spec.ServiceAccountName)
	podSpecificFieldsSet["spec.hostNetwork"] = strconv.FormatBool(pod.Spec.HostNetwork)
	podSpecificFieldsSet["status.phase"] = string(pod.Status.Phase)
	// TODO：以合适的格式将 podIPs 增加为 Downward API 可读取的字段。
	podIP := ""
	if len(pod.Status.PodIPs) > 0 {
		podIP = string(pod.Status.PodIPs[0].IP)
	}
	podSpecificFieldsSet["status.podIP"] = podIP
	podSpecificFieldsSet["status.nominatedNodeName"] = string(pod.Status.NominatedNodeName)
	return generic.AddObjectMetaFieldsSet(podSpecificFieldsSet, &pod.ObjectMeta, true)
}

// ResourceGetter 定义通过 ResourceLocation 获取资源代理地址的接口。
type ResourceGetter interface {
	Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error)
}

func getPod(ctx context.Context, getter ResourceGetter, name string) (*api.Pod, error) {
	obj, err := getter.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	pod := obj.(*api.Pod)
	if pod == nil {
		return nil, fmt.Errorf("Unexpected object type: %#v", pod)
	}
	return pod, nil
}

// getPodIP 返回 Pod 的主 IP 地址。
func getPodIP(pod *api.Pod) string {
	if pod == nil {
		return ""
	}
	if len(pod.Status.PodIPs) > 0 {
		return pod.Status.PodIPs[0].IP
	}

	return ""
}

// ResourceLocation 将 Pod 名称/端口解析为 Pod IP URL；用于 /proxy 子资源把请求转发到 Pod 网络地址。
func ResourceLocation(ctx context.Context, getter ResourceGetter, rt http.RoundTripper, id string) (*url.URL, http.RoundTripper, error) {
	// 先解析 scheme、Pod 名称和可选端口；缺省端口时从 Pod 容器声明中选择第一个端口。
	// 如果请求未指定端口，则尝试使用 Pod 中声明的第一个容器端口。
	scheme, name, port, valid := utilnet.SplitSchemeNamePort(id)
	if !valid {
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid pod request %q", id))
	}

	pod, err := getPod(ctx, getter, name)
	if err != nil {
		return nil, nil, err
	}

	// 推导代理目标端口。
	if port == "" {
		for i := range pod.Spec.Containers {
			if len(pod.Spec.Containers[i].Ports) > 0 {
				port = fmt.Sprintf("%d", pod.Spec.Containers[i].Ports[0].ContainerPort)
				break
			}
		}
	}
	podIP := getPodIP(pod)
	// 只允许全局单播 Pod IP，拒绝空地址、组播和非全局地址，避免代理到无效目标。
	if ip := netutils.ParseIPSloppy(podIP); ip == nil || !ip.IsGlobalUnicast() {
		return nil, nil, errors.NewBadRequest("address not allowed")
	}

	loc := &url.URL{
		Scheme: scheme,
	}
	if port == "" {
		// IPv6 地址作为 URL 主机名时必须使用方括号包裹；net.JoinHostPort 会自动完成该格式处理。
		if strings.Contains(podIP, ":") {
			loc.Host = "[" + podIP + "]"
		} else {
			loc.Host = podIP
		}
	} else {
		loc.Host = net.JoinHostPort(podIP, port)
	}
	return loc, rt, nil
}

// LogLocation 返回 Pod 容器日志 URL。若 opts.Container 为空且 Pod 只有一个容器，则自动使用该容器。
// LogLocation 根据 Pod 所在 Node、容器名称和日志查询参数构建 Kubelet /containerLogs URL，并选择 TLS Transport。
func LogLocation(
	ctx context.Context, getter ResourceGetter,
	connInfo client.ConnectionInfoGetter,
	name string,
	opts *api.PodLogOptions,
) (*url.URL, http.RoundTripper, error) {
	pod, err := getPod(ctx, getter, name)
	if err != nil {
		return nil, nil, err
	}

	// 确定要执行命令或建立 Attach 会话的容器；显式指定时必须验证容器属于该 Pod。
	container := opts.Container
	container, err = validateContainer(container, pod)
	if err != nil {
		return nil, nil, err
	}
	nodeName := types.NodeName(pod.Spec.NodeName)
	if len(nodeName) == 0 {
		// Pod 尚未绑定 Node 时无法建立端口转发连接，返回空位置。
		return nil, nil, nil
	}
	nodeInfo, err := connInfo.GetConnectionInfo(ctx, nodeName)
	if err != nil {
		return nil, nil, err
	}
	params := url.Values{}
	if opts.Follow {
		params.Add("follow", "true")
	}
	if opts.Previous {
		params.Add("previous", "true")
	}
	if opts.Timestamps {
		params.Add("timestamps", "true")
	}
	if opts.SinceSeconds != nil {
		params.Add("sinceSeconds", strconv.FormatInt(*opts.SinceSeconds, 10))
	}
	if opts.SinceTime != nil {
		params.Add("sinceTime", opts.SinceTime.Format(time.RFC3339))
	}
	if opts.TailLines != nil {
		params.Add("tailLines", strconv.FormatInt(*opts.TailLines, 10))
	}
	if opts.LimitBytes != nil {
		params.Add("limitBytes", strconv.FormatInt(*opts.LimitBytes, 10))
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		// 默认值处理已确保 opts.Stream 不为 nil，因此此处可以安全读取流参数。
		params.Add("stream", string(*opts.Stream))
	}
	loc := &url.URL{
		Scheme:   nodeInfo.Scheme,
		Host:     net.JoinHostPort(nodeInfo.Hostname, nodeInfo.Port),
		Path:     fmt.Sprintf("/containerLogs/%s/%s/%s", pod.Namespace, pod.Name, container),
		RawQuery: params.Encode(),
	}

	if opts.InsecureSkipTLSVerifyBackend {
		return loc, nodeInfo.InsecureSkipTLSVerifyTransport, nil
	}
	return loc, nodeInfo.Transport, nil
}

func podHasContainerWithName(pod *api.Pod, containerName string) bool {
	var hasContainer bool
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *api.Container, containerType podutil.ContainerType) bool {
		if c.Name == containerName {
			hasContainer = true
			return false
		}
		return true
	})
	return hasContainer
}

// streamParams 把 exec/attach/port-forward 的类型化选项编码成 Kubelet 流式接口使用的 URL 查询参数。
func streamParams(params url.Values, opts runtime.Object) error {
	switch opts := opts.(type) {
	case *api.PodExecOptions:
		if opts.Stdin {
			params.Add(api.ExecStdinParam, "1")
		}
		if opts.Stdout {
			params.Add(api.ExecStdoutParam, "1")
		}
		if opts.Stderr {
			params.Add(api.ExecStderrParam, "1")
		}
		if opts.TTY {
			params.Add(api.ExecTTYParam, "1")
		}
		for _, c := range opts.Command {
			params.Add("command", c)
		}
	case *api.PodAttachOptions:
		if opts.Stdin {
			params.Add(api.ExecStdinParam, "1")
		}
		if opts.Stdout {
			params.Add(api.ExecStdoutParam, "1")
		}
		if opts.Stderr {
			params.Add(api.ExecStderrParam, "1")
		}
		if opts.TTY {
			params.Add(api.ExecTTYParam, "1")
		}
	case *api.PodPortForwardOptions:
		if len(opts.Ports) > 0 {
			ports := make([]string, len(opts.Ports))
			for i, p := range opts.Ports {
				ports[i] = strconv.FormatInt(int64(p), 10)
			}
			params.Add(api.PortHeader, strings.Join(ports, ","))
		}
	default:
		return fmt.Errorf("Unknown object for streaming: %v", opts)
	}
	return nil
}

// AttachLocation 返回 Pod 容器的 Attach URL。若 opts.Container 为空且 Pod 只有一个容器，则自动使用该容器。
func AttachLocation(
	ctx context.Context,
	getter ResourceGetter,
	connInfo client.ConnectionInfoGetter,
	name string,
	opts *api.PodAttachOptions,
) (*url.URL, *client.ConnectionInfo, error) {
	return streamLocation(ctx, getter, connInfo, name, opts, opts.Container, "attach")
}

// ExecLocation 返回 Pod 容器的 Exec URL。若 opts.Container 为空且 Pod 只有一个容器，则自动使用该容器。
func ExecLocation(
	ctx context.Context,
	getter ResourceGetter,
	connInfo client.ConnectionInfoGetter,
	name string,
	opts *api.PodExecOptions,
) (*url.URL, *client.ConnectionInfo, error) {
	return streamLocation(ctx, getter, connInfo, name, opts, opts.Container, "exec")
}

// streamLocation 统一处理 exec 和 attach：读取 Pod、确认容器与 Node、获取节点连接信息并构造流式请求 URL。
func streamLocation(
	ctx context.Context,
	getter ResourceGetter,
	connInfo client.ConnectionInfoGetter,
	name string,
	opts runtime.Object,
	container,
	path string,
) (*url.URL, *client.ConnectionInfo, error) {
	pod, err := getPod(ctx, getter, name)
	if err != nil {
		return nil, nil, err
	}

	// 尝试确定要使用哪个容器
	// 如果指定了 container，那它必须是合法的
	container, err = validateContainer(container, pod)
	if err != nil {
		return nil, nil, err
	}

	nodeName := types.NodeName(pod.Spec.NodeName)
	if len(nodeName) == 0 {
		// 如果 Pod 尚未分配到主机，返回空地址
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("pod %s does not have a host assigned", name))
	}
	nodeInfo, err := connInfo.GetConnectionInfo(ctx, nodeName)
	if err != nil {
		return nil, nil, err
	}
	params := url.Values{}
	if err := streamParams(params, opts); err != nil {
		return nil, nil, err
	}
	loc := &url.URL{
		Scheme:   nodeInfo.Scheme,
		Host:     net.JoinHostPort(nodeInfo.Hostname, nodeInfo.Port),
		Path:     fmt.Sprintf("/%s/%s/%s/%s", path, pod.Namespace, pod.Name, container),
		RawQuery: params.Encode(),
	}
	return loc, nodeInfo, nil
}

// PortForwardLocation 获取 Pod 所在节点的连接信息，构造 /portForward/{namespace}/{pod} 请求地址。
func PortForwardLocation(
	ctx context.Context,
	getter ResourceGetter,
	connInfo client.ConnectionInfoGetter,
	name string,
	opts *api.PodPortForwardOptions,
) (*url.URL, *client.ConnectionInfo, error) {
	pod, err := getPod(ctx, getter, name)
	if err != nil {
		return nil, nil, err
	}

	nodeName := types.NodeName(pod.Spec.NodeName)
	if len(nodeName) == 0 {
		// 如果 Pod 尚未分配到主机，返回空地址
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("pod %s does not have a host assigned", name))
	}
	nodeInfo, err := connInfo.GetConnectionInfo(ctx, nodeName)
	if err != nil {
		return nil, nil, err
	}
	params := url.Values{}
	if err := streamParams(params, opts); err != nil {
		return nil, nil, err
	}
	loc := &url.URL{
		Scheme:   nodeInfo.Scheme,
		Host:     net.JoinHostPort(nodeInfo.Hostname, nodeInfo.Port),
		Path:     fmt.Sprintf("/portForward/%s/%s", pod.Namespace, pod.Name),
		RawQuery: params.Encode(),
	}
	return loc, nodeInfo, nil
}

// validateContainer 校验容器是否属于指定 Pod，并返回匹配的容器对象。
// validateContainer 为空容器名选择唯一容器；多容器时要求显式指定，并拒绝不属于该 Pod 的容器。
func validateContainer(container string, pod *api.Pod) (string, error) {
	if len(container) == 0 {
		switch len(pod.Spec.Containers) {
		case 1:
			container = pod.Spec.Containers[0].Name
		case 0:
			return "", errors.NewBadRequest(fmt.Sprintf("a container name must be specified for pod %s", pod.Name))
		default:
			var containerNames []string
			podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *api.Container, containerType podutil.ContainerType) bool {
				containerNames = append(containerNames, c.Name)
				return true
			})
			errStr := fmt.Sprintf("a container name must be specified for pod %s, choose one of: %s", pod.Name, containerNames)
			return "", errors.NewBadRequest(errStr)
		}
	} else {
		if !podHasContainerWithName(pod, container) {
			return "", errors.NewBadRequest(fmt.Sprintf("container %s is not valid for pod %s", container, pod.Name))
		}
	}

	return container, nil
}

// applyLabelKeysToLabelSelector 根据 labelKeys 从给定 Labels 中读取对应的值，并按指定的 operator 合并到 LabelSelector。
func applyLabelKeysToLabelSelector(labelSelector *metav1.LabelSelector, labelKeys []string, operator metav1.LabelSelectorOperator, podLabels map[string]string) {
	for _, key := range labelKeys {
		if value, ok := podLabels[key]; ok {
			labelSelector.MatchExpressions = append(labelSelector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key:      key,
				Operator: operator,
				Values:   []string{value},
			})
		}
	}
}

// applyMatchLabelKeysAndMismatchLabelKeys 用 matchLabelKeys 或 mismatchLabelKeys 中的 key，
// 从 Pod 的 labels 中取出对应的值，并按字段含义合并到 PodAffinityTerm 的 LabelSelector：
// - matchLabelKeys 合并为 key in (value)；
// - mismatchLabelKeys 合并为 key notin (value)。
func applyMatchLabelKeysAndMismatchLabelKeys(term *api.PodAffinityTerm, label map[string]string) {
	if (len(term.MatchLabelKeys) == 0 && len(term.MismatchLabelKeys) == 0) || term.LabelSelector == nil {
		// LabelSelector 为 nil 时无需应用 label keys，因为 nil LabelSelector 匹配不到任何对象。
		return
	}

	applyLabelKeysToLabelSelector(term.LabelSelector, term.MatchLabelKeys, metav1.LabelSelectorOpIn, label)
	applyLabelKeysToLabelSelector(term.LabelSelector, term.MismatchLabelKeys, metav1.LabelSelectorOpNotIn, label)
}

// mutatePodAffinity 在创建/更新时将 MatchLabelKeys、MismatchLabelKeys 根据 Pod 当前标签展开为实际 LabelSelector。
func mutatePodAffinity(pod *api.Pod) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodAffinity) || pod.Spec.Affinity == nil {
		return
	}
	if affinity := pod.Spec.Affinity.PodAffinity; affinity != nil {
		for i := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
			applyMatchLabelKeysAndMismatchLabelKeys(&affinity.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm, pod.Labels)
		}
		for i := range affinity.RequiredDuringSchedulingIgnoredDuringExecution {
			applyMatchLabelKeysAndMismatchLabelKeys(&affinity.RequiredDuringSchedulingIgnoredDuringExecution[i], pod.Labels)
		}
	}
	if affinity := pod.Spec.Affinity.PodAntiAffinity; affinity != nil {
		for i := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
			applyMatchLabelKeysAndMismatchLabelKeys(&affinity.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm, pod.Labels)
		}
		for i := range affinity.RequiredDuringSchedulingIgnoredDuringExecution {
			applyMatchLabelKeysAndMismatchLabelKeys(&affinity.RequiredDuringSchedulingIgnoredDuringExecution[i], pod.Labels)
		}
	}
}

func applyMatchLabelKeys(constraint *api.TopologySpreadConstraint, labels map[string]string) {
	if len(constraint.MatchLabelKeys) == 0 || constraint.LabelSelector == nil {
		// LabelSelector 为 nil 时无需应用 label keys，因为 nil LabelSelector 匹配不到任何对象。
		return
	}

	applyLabelKeysToLabelSelector(constraint.LabelSelector, constraint.MatchLabelKeys, metav1.LabelSelectorOpIn, labels)
}

// mutateTopologySpreadConstraints 按 Feature Gate 将 MatchLabelKeys 展开到拓扑分布约束的 LabelSelector。
func mutateTopologySpreadConstraints(pod *api.Pod) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread) || !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpreadSelectorMerge) || pod.Spec.TopologySpreadConstraints == nil {
		return
	}
	topologySpreadConstraints := pod.Spec.TopologySpreadConstraints
	for i := range topologySpreadConstraints {
		applyMatchLabelKeys(&topologySpreadConstraints[i], pod.Labels)
	}
}

// applySchedulingGatedCondition 必要时为新建的 Pod 添加 {type:PodScheduled, reason:SchedulingGated} 条件，
func applySchedulingGatedCondition(pod *api.Pod) {
	if len(pod.Spec.SchedulingGates) == 0 {
		return
	}

	// 如果已存在类型为 PodScheduled 的条件，直接返回。
	for _, condition := range pod.Status.Conditions {
		if condition.Type == api.PodScheduled {
			return
		}
	}

	podutil.UpdatePodCondition(&pod.Status, &api.PodCondition{
		Type:    api.PodScheduled,
		Status:  api.ConditionFalse,
		Reason:  apiv1.PodReasonSchedulingGated,
		Message: "Scheduling is blocked due to non-empty scheduling gates",
	})
}

// applyAppArmorVersionSkew 实现如下 KEP 中描述的版本偏差（version skew）行为：
// https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/24-apparmor#version-skew-strategy
// 该函数在升级兼容期把旧的 AppArmor Annotation 同步到结构化字段，并对需要迁移的用法发出弃用警告。
func applyAppArmorVersionSkew(ctx context.Context, pod *api.Pod) {
	// Windows 容器不使用 Linux AppArmor 配置，直接跳过这段字段迁移逻辑。
	if pod.Spec.OS != nil && pod.Spec.OS.Name == api.Windows {
		return
	}

	var podProfile *api.AppArmorProfile
	if pod.Spec.SecurityContext != nil {
		podProfile = pod.Spec.SecurityContext.AppArmorProfile
	}

	// 处理 Pod 中的各个容器
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(),
		func(ctr *api.Container, _ podutil.ContainerType) bool {
			// 取出可能存在的 annotation 和字段
			key := api.DeprecatedAppArmorAnnotationKeyPrefix + ctr.Name
			annotation, hasAnnotation := pod.Annotations[key]

			var containerProfile *api.AppArmorProfile
			if ctr.SecurityContext != nil {
				containerProfile = ctr.SecurityContext.AppArmorProfile
			}

			// 把已弃用的 AppArmor annotations 同步到结构化字段
			if hasAnnotation && containerProfile == nil {
				newField := podutil.ApparmorFieldForAnnotation(annotation)
				if errs := corevalidation.ValidateAppArmorProfileField(newField, &field.Path{}); len(errs) > 0 {
					// 跳过非法值，不做拷贝。
					newField = nil
				}

				// 如果某个 annotation 无法推导出合法的字段，则发出警告
				deprecationWarning := newField == nil

				// 仅当 annotation 与 Pod 级别的 profile 不同时，才把它拷贝到字段上。
				if newField != nil && !apiequality.Semantic.DeepEqual(newField, podProfile) {
					if ctr.SecurityContext == nil {
						ctr.SecurityContext = &api.SecurityContext{}
					}
					ctr.SecurityContext.AppArmorProfile = newField
					// 如果存在没有对应字段的 annotation，则发出警告
					deprecationWarning = true
				}

				if deprecationWarning {
					// 注意：annotation 的弃用警告必须加在这里，而不是常见的 WarningsOnCreate 路径，
					// 以便在同步 annotations 与字段之前就发出警告。
					fldPath := field.NewPath("metadata", "annotations").Key(key)
					warning.AddWarning(ctx, "", fmt.Sprintf(`%s: deprecated since v1.30; use the "appArmorProfile" field instead`, fldPath))
				}
			}

			return true
		})
}

// updatePodGeneration 在 PodSpec 发生变化时递增 metadata.generation：
func updatePodGeneration(newPod, oldPod *api.Pod) {
	if !apiequality.Semantic.DeepEqual(newPod.Spec, oldPod.Spec) {
		newPod.Generation++
	}
}
