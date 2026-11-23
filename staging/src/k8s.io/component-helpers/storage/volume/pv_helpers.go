/*
Copyright 2019 The Kubernetes Authors.

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

package volume

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/reference"
	"k8s.io/utils/ptr"
)

const (
	// AnnBindCompleted 注解用于 PVC，表示其生命周期已越过初始设置阶段。
	// 控制器据此区分“尚未处理”和“已处理过”：同一份对象快照在不同阶段要按不同规则解读。
	// 注解的值本身无意义，只看是否存在。
	AnnBindCompleted = "pv.kubernetes.io/bind-completed"

	// AnnBoundByController 注解用于 PV 和 PVC，表示绑定（PV→PVC 或 PVC→PV）由控制器完成。
	// 缺少该注解则代表用户预绑定：预绑定是用户的明确意图，控制器不能随意改写；
	// 外部绑定器必须用同样方式打注解，否则内置 PV 控制器无法正确识别绑定来源。
	// 注解的值本身无意义，只看是否存在。
	AnnBoundByController = "pv.kubernetes.io/bound-by-controller"

	// AnnSelectedNode 注解由调度器添加到被触发动态供应的 PVC 上，值为所选节点的名字。
	// 它是触发延迟绑定的信号：PV 控制器看到后才开始按需创建卷，
	// 创建时可依据节点标签满足拓扑约束。
	AnnSelectedNode = "volume.kubernetes.io/selected-node"

	// NotSupportedProvisioner 是一个特殊的 provisioner 名，写在 StorageClass 中表示该存储不支持动态供应。
	// 用户如果用这类 class 建 PVC，只能等管理员手工创建匹配的静态 PV。
	NotSupportedProvisioner = "kubernetes.io/no-provisioner"

	// AnnDynamicallyProvisioned 注解添加到由 Kubernetes 动态创建的 PV 上，值为创建该卷的卷插件名。
	// 它同时服务于用户（展示 PV 的来源）和 Kubernetes（在决策中识别动态供应的 PV，
	// 并据此走不同的回收/删除逻辑）。
	AnnDynamicallyProvisioned = "pv.kubernetes.io/provisioned-by"

	// AnnMigratedTo 注解添加到需要通过 CSIMigration 特性由对应 CSI 驱动动态创建/删除的
	// PVC 和 PV 上。
	// CSI 迁移开关打开时，标记该对象改由对应 CSI 驱动创建/删除。
	// 一旦设置，Kubernetes 内置组件要让出控制权（“stand-down”），
	// 由外部 provisioner 接管，避免两套逻辑同时操作同一个对象。
	AnnMigratedTo = "pv.kubernetes.io/migrated-to"

	// AnnStorageProvisioner 注解添加到需要动态供应的 PVC 上，值为负责为该 PVC 创建卷的插件名。
	// TODO: 废弃期结束后移除 beta 注解。
	//
	// 保留带 beta 域名的旧版本是为了兼容存量对象。
	AnnStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	AnnBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"

	// PVDeletionProtectionFinalizer 是外部 provisioner 加在 PV 上的 finalizer：
	// 删除前必须让驱动先清理底层存储，防止卷还在使用时对象就消失。
	PVDeletionProtectionFinalizer = "external-provisioner.volume.kubernetes.io/finalizer"

	// PVDeletionInTreeProtectionFinalizer 是保护 in-tree（树内）卷删除的 finalizer，
	// 作用与上一个类似，但由内置 PV 控制器维护，针对尚未迁移到 CSI 的卷。
	PVDeletionInTreeProtectionFinalizer = "kubernetes.io/pv-controller"
)

// IsDelayBindingProvisioning 检查 PVC 是否带有 selected-node 注解，即是否走延迟绑定供应。
//
// 是否走延迟绑定完全由 AnnSelectedNode 是否存在决定：调度器选定节点后打上这个注解，
// PV 控制器据此开始按需创建卷。注解的值不参与判断，空值也代表已发出信号。
func IsDelayBindingProvisioning(claim *v1.PersistentVolumeClaim) bool {
	// 启用 VolumeScheduling 特性时，调度器通过在 PVC 上设置 AnnSelectedNode 注解，
	// 向 PV 控制器发出开始动态供应的信号。
	_, ok := claim.Annotations[AnnSelectedNode]
	return ok
}

// IsDelayBindingMode 检查 PVC 是否处于延迟绑定模式。
//
// 延迟绑定是存储类的策略，不是 PVC 自身字段，必须先由 PVC 解析出类名再查对应 StorageClass。
// 没有类或类已删除时视为非延迟绑定，这样旧对象、孤立 PVC 仍能按默认即时绑定流程处理。
func IsDelayBindingMode(claim *v1.PersistentVolumeClaim, classLister storagelisters.StorageClassLister) (bool, error) {
	className := GetPersistentVolumeClaimClass(claim)
	if className == "" {
		return false, nil
	}

	class, err := classLister.Get(className)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	if class.VolumeBindingMode == nil {
		return false, fmt.Errorf("VolumeBindingMode not set for StorageClass %q", className)
	}

	return *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer, nil
}

// GetBindVolumeToClaim 返回一个绑定到给定 PVC 的新卷对象，
// 并额外返回一个布尔值，表示是否对原卷对象做了修改。
//
// 返回值里的 dirty 标记给调用方决定是否真正发更新请求：没变化时不写 API，
// 可以避免无意义的资源版本号变更和冲突。
func GetBindVolumeToClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) (*v1.PersistentVolume, bool, error) {
	dirty := false

	// 先检查卷是否已绑定（无论是用户还是控制器完成的）。这个判断必须在改动 clone 之前做：
	// 一旦先写入 ClaimRef，IsVolumeBoundToClaim 就会变成真，再也分不清绑定是谁完成的。
	shouldSetBoundByController := false
	if !IsVolumeBoundToClaim(volume, claim) {
		shouldSetBoundByController = true
	}

	// 入参中的卷可能指向 watcher/informer 缓存，这些对象是共享的，原地改写会污染其他读取者，
	// 还会让缓存状态和 API 实际状态不一致，因此所有改动都落在副本上。
	volumeClone := volume.DeepCopy()

	// 若卷尚未绑定，则将其绑定到 PVC。
	if volume.Spec.ClaimRef == nil ||
		volume.Spec.ClaimRef.Name != claim.Name ||
		volume.Spec.ClaimRef.Namespace != claim.Namespace ||
		volume.Spec.ClaimRef.UID != claim.UID {

		claimRef, err := reference.GetReference(scheme.Scheme, claim)
		if err != nil {
			return nil, false, fmt.Errorf("unexpected error getting claim reference: %w", err)
		}
		volumeClone.Spec.ClaimRef = claimRef
		dirty = true
	}

	// 若尚未设置 AnnBoundByController，则补上该注解。注解存在与否本身就是信息：
	// 缺注解代表用户预绑定，控制器不能再覆盖，所以这里只在确实由控制器完成绑定时补上。
	if shouldSetBoundByController && !metav1.HasAnnotation(volumeClone.ObjectMeta, AnnBoundByController) {
		metav1.SetMetaDataAnnotation(&volumeClone.ObjectMeta, AnnBoundByController, "yes")
		dirty = true
	}

	return volumeClone, dirty, nil
}

// IsVolumeBoundToClaim 在给定卷已预绑定或绑定到指定 PVC 时返回 true。
// 要求 claim.Name 和 claim.Namespace 都相等；若 volume.Spec.ClaimRef 中带有 UID，也必须相等。
//
// UID 只在非空时才参与比较，因为预绑定 PV 的 ClaimRef 可能只有名字和命名空间，
// 此时强行要求 UID 相等会误判为未绑定。反过来，已写入 UID 时它就是防误绑的关键：
// 同名 PVC 被删掉重建后 UID 会变，旧 PV 不能继续绑到新对象上。
func IsVolumeBoundToClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) bool {
	if volume.Spec.ClaimRef == nil {
		return false
	}
	if claim.Name != volume.Spec.ClaimRef.Name || claim.Namespace != volume.Spec.ClaimRef.Namespace {
		return false
	}
	if volume.Spec.ClaimRef.UID != "" && claim.UID != volume.Spec.ClaimRef.UID {
		return false
	}
	return true
}

// FindMatchingVolume 遍历卷列表，为 PVC 找到最匹配的卷。
//
// PV 控制器和调度器都会调用本函数。
//
// delayBinding 只在 PV 控制器路径上为 true：此时预绑定卷仍可作为匹配结果返回，但未绑定卷会被跳过。
//
// node 只在调度器路径上设置：设置后会按节点标签检查 PV 的节点亲和性。
//
// excludedVolumes 也只用于调度器路径，用于一次调度中同时评估同一 Pod 的多个未绑定 PVC：
// 每个 PVC 找到匹配卷后，都要把该卷排除，避免后续重复选中。
//
// 两个调用方共用这段匹配逻辑，但关注点不同：PV 控制器在延迟绑定下只看预绑定卷，
// 调度器则按节点亲和性、访问模式挑最小可用卷。用 node 是否为 nil 区分路径，
// 比拆成两个近似的实现少很多重复，也避免两边规则漂移。
func FindMatchingVolume(
	claim *v1.PersistentVolumeClaim,
	volumes []*v1.PersistentVolume,
	node *v1.Node,
	excludedVolumes map[string]*v1.PersistentVolume,
	delayBinding bool,
	vacEnabled bool) (*v1.PersistentVolume, error) {

	if !vacEnabled {
		// 门未开却声明了 VAC，说明用户请求了集群不支持的能力。
		// 这里必须显式报错，否则后续按“没有 VAC”继续匹配，会把卷绑到不满足要求的类上。
		claimVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		if claimVAC != "" {
			return nil, fmt.Errorf("unsupported volumeAttributesClassName is set on claim %s when the feature-gate VolumeAttributesClass is disabled", claimToClaimKey(claim))
		}
	}

	var smallestVolume *v1.PersistentVolume
	var smallestVolumeQty resource.Quantity
	requestedQty := claim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	requestedClass := GetPersistentVolumeClaimClass(claim)

	var selector labels.Selector
	if claim.Spec.Selector != nil {
		internalSelector, err := metav1.LabelSelectorAsSelector(claim.Spec.Selector)
		if err != nil {
			return nil, fmt.Errorf("error creating internal label selector for claim: %v: %v", claimToClaimKey(claim), err)
		}
		selector = internalSelector
	}

	// 遍历所有可用卷，目标有两个：
	// - 找到用户预绑定或为该 PVC 动态创建的卷，因此需要检查全部卷
	// - 若没有预绑定卷，则选出最小的匹配卷
	//
	// 预绑定卷一旦命中就可以立即返回：它是用户明确指定的，不存在“挑更合适的”。
	// 没有预绑定时才退化为选最小满足条件的卷，减少容量浪费。
	for _, volume := range volumes {
		if _, ok := excludedVolumes[volume.Name]; ok {
			// 跳过排除列表中的卷
			continue
		}
		// ClaimRef 指向别的 PVC 就不能再分配；空 ClaimRef 才是可选卷。
		if volume.Spec.ClaimRef != nil && !IsVolumeBoundToClaim(volume, claim) {
			continue
		}

		volumeQty := volume.Spec.Capacity[v1.ResourceStorage]
		if volumeQty.Cmp(requestedQty) < 0 {
			continue
		}
		// 过滤卷模式不匹配的卷
		if CheckVolumeModeMismatches(&claim.Spec, &volume.Spec) {
			continue
		}

		claimVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		volumeVAC := ptr.Deref(volume.Spec.VolumeAttributesClassName, "")

		// 过滤 volumeAttributesClassName 不匹配的卷。
		// VAC 是精确匹配的属性，不相似、不做兼容回退：卷的类必须和用户要求的完全一致，
		// 否则调度器看到的参数与后端实际能力会对不上。
		if vacEnabled && claimVAC != volumeVAC {
			continue
		}
		if !vacEnabled && volumeVAC != "" {
			// 特性门关闭但 PV 对象设置了 VAC 时，不应进行任何绑定。
			// 门关闭时连 PVC 带 VAC 都已提前报错；PV 带 VAC 则可能是状态残留，
			// 此时绑定会悄悄忽略用户诉求，所以直接跳过。
			continue
		}

		// 检查 PV 是否设置了 DeletionTimestamp，若已设置则跳过该卷。
		// 正在删除的卷随时会消失，绑定到它只会让 PVC 卡住，必须排除。
		if volume.ObjectMeta.DeletionTimestamp != nil {
			continue
		}

		nodeAffinityValid := true
		if node != nil {
			// 调度器路径：检查节点是否满足 PV 的节点亲和性。
			// CheckNodeAffinity 是本循环中最昂贵的调用，应先做更廉价的检查，或考虑优化本函数。
			// 亲和性计算放在容量、模式、VAC 等廉价条件之后，让明显不匹配的卷先被过滤掉。
			err := CheckNodeAffinity(volume, node.Labels)
			if err != nil {
				nodeAffinityValid = false
			}
		}

		if IsVolumeBoundToClaim(volume, claim) {
			// 若 PV 节点亲和性不满足，则返回无匹配。
			// 这意味着预绑定卷（以及相应 PVC）不适合该节点。
			// 预绑定卷没有替代选项，节点亲和性不满足时不能退而求其次挑别的卷，
			// 只能返回空让调度判定失败，避免违背用户的预绑定意图。
			if !nodeAffinityValid {
				return nil, nil
			}

			return volume, nil
		}

		if node == nil && delayBinding {
			// PV 控制器不绑定该 PVC，未绑定卷由调度器负责绑定（调度器路径中 node 不为 nil）。
			// 延迟绑定下未绑定卷要等调度器选好节点再处理，控制器提前绑定会绕过拓扑约束。
			continue
		}

		// 过滤以下卷：
		// - 非 Available 状态的卷
		// - 标签不匹配 PVC selector 的卷（如果指定了 selector）
		// - 存储类不是所请求类的卷
		// - 节点亲和性不匹配节点的卷
		if volume.Status.Phase != v1.VolumeAvailable {
			// 忽略非 Available 状态的卷：满足匹配条件的卷会被更新为 Available，
			// 现在绑定它们很可能会因 API 冲突而无谓失败。
			// 处于 Bound/Released 等状态的卷正在或已经被其他流程处理，抢先绑定只会遇到冲突。
			continue
		} else if selector != nil && !selector.Matches(labels.Set(volume.Labels)) {
			continue
		}
		// 存储类也必须一致：不同类的供应商、回收策略、参数都可能不同，不能因为容量合适就绑。
		if GetPersistentVolumeClass(volume) != requestedClass {
			continue
		}
		if !nodeAffinityValid {
			continue
		}

		if node != nil {
			// 调度器路径：检查访问模式是否匹配。
			// 访问模式只在调度路径校验：控制器路径处理的预绑定卷由用户负责，
			// 且调度阶段才有足够信息进行基于节点的完整筛选。
			if !CheckAccessModes(claim, volume) {
				continue
			}
		}

		// 选容量最小的候选：既满足请求又尽量少占用空间，
		// 给后续更大的 PVC 留下可用卷。
		if smallestVolume == nil || smallestVolumeQty.Cmp(volumeQty) > 0 {
			smallestVolume = volume
			smallestVolumeQty = volumeQty
		}
	}

	if smallestVolume != nil {
		// 找到匹配的卷
		return smallestVolume, nil
	}

	return nil, nil
}

// CheckVolumeModeMismatches 是检查 PV 与 PVC 的 volumeMode 是否不匹配的便捷方法。
//
// 缺省值显式当成 Filesystem，是因为滚动升级期间 apiserver 与控制器版本可能不一致，
// 老对象里 VolumeMode 可能是 nil。若按“nil 不等于任何值”处理，会把本该匹配的卷过滤掉。
func CheckVolumeModeMismatches(pvcSpec *v1.PersistentVolumeClaimSpec, pvSpec *v1.PersistentVolumeSpec) bool {
	// HA 升级过程中无法保证 apiserver 版本不低于 controller-manager，
	// 因此将 nil 的 volumeMode 按 filesystem 处理。
	requestedVolumeMode := v1.PersistentVolumeFilesystem
	if pvcSpec.VolumeMode != nil {
		requestedVolumeMode = *pvcSpec.VolumeMode
	}
	pvVolumeMode := v1.PersistentVolumeFilesystem
	if pvSpec.VolumeMode != nil {
		pvVolumeMode = *pvSpec.VolumeMode
	}
	return requestedVolumeMode != pvVolumeMode
}

// CheckAccessModes 在 PV 满足 PVC 请求的所有 AccessMode 时返回 true。
//
// 先建集合再逐个检查请求，是为了避免双重循环，也顺带去掉 PV 里重复的模式。
// 只要求“请求的模式都被覆盖”，PV 多出来的模式无害，不影响卷可用性。
func CheckAccessModes(claim *v1.PersistentVolumeClaim, volume *v1.PersistentVolume) bool {
	pvModesMap := map[v1.PersistentVolumeAccessMode]bool{}
	for _, mode := range volume.Spec.AccessModes {
		pvModesMap[mode] = true
	}

	for _, mode := range claim.Spec.AccessModes {
		_, ok := pvModesMap[mode]
		if !ok {
			return false
		}
	}
	return true
}

// claimToClaimKey 生成 namespace/name 形式的标识，只用于错误信息。
//
// PVC 名字本身可能在多个命名空间重复，报错时只给名字无法定位对象。
func claimToClaimKey(claim *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", claim.Namespace, claim.Name)
}
