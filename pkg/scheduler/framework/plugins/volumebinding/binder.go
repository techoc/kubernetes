/*
Copyright 2017 The Kubernetes Authors.

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

package volumebinding

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/component-helpers/storage/volume"
	csitrans "k8s.io/csi-translation-lib"
	csiplugins "k8s.io/csi-translation-lib/plugins"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding/metrics"
)

// ConflictReason 用特殊字符串说明某个节点为何无法完成卷绑定。
type ConflictReason string

// ConflictReasons 保存某个节点无法完成卷绑定的全部原因。
type ConflictReasons []ConflictReason

// Len 返回原因数量，满足 sort.Interface。
func (reasons ConflictReasons) Len() int { return len(reasons) }

// Less 按字符串顺序比较原因，保证输出顺序稳定。
func (reasons ConflictReasons) Less(i, j int) bool { return reasons[i] < reasons[j] }

// Swap 交换两个原因，满足 sort.Interface。
func (reasons ConflictReasons) Swap(i, j int) { reasons[i], reasons[j] = reasons[j], reasons[i] }

const (
	// ErrReasonBindConflict 用于 VolumeBindingNoMatch 判定错误。
	ErrReasonBindConflict ConflictReason = "node(s) didn't find available persistent volumes to bind"
	// ErrReasonNodeConflict 用于 VolumeNodeAffinityConflict 判定错误。
	ErrReasonNodeConflict ConflictReason = "node(s) didn't match PersistentVolume's node affinity"
	// ErrReasonNotEnoughSpace 用于节点剩余存储空间不足、Pod 无法启动的场景。
	ErrReasonNotEnoughSpace = "node(s) did not have enough free storage"
	// ErrReasonPVNotExist 用于 PVC 绑定到不存在的 PV 的场景。
	ErrReasonPVNotExist = "node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)"
)

// BindingInfo 保存一条 PV 与 PVC 之间的绑定关系。
type BindingInfo struct {
	// pvc 是待绑定的 PVC。
	pvc *v1.PersistentVolumeClaim

	// pv 是提议与该 PVC 绑定的 PV。
	pv *v1.PersistentVolume
}

// StorageClassName 返回 PV 所属存储类的名字。
func (b *BindingInfo) StorageClassName() string {
	return b.pv.Spec.StorageClassName
}

// StorageResource 表示一条卷申请及其容量。
type StorageResource struct {
	// Requested 是 PVC 申请的存储字节数。
	Requested int64
	// Capacity 是 PV 提供的存储字节数。
	Capacity int64
}

// StorageResource 返回该绑定中 PVC 的申请量与 PV 的容量。
func (b *BindingInfo) StorageResource() *StorageResource {
	// 两个字段都是必填项。
	requestedQty := b.pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	capacityQty := b.pv.Spec.Capacity[v1.ResourceName(v1.ResourceStorage)]
	return &StorageResource{
		Requested: requestedQty.Value(),
		Capacity:  capacityQty.Value(),
	}
}

// DynamicProvision 表示需要动态创建的卷。
type DynamicProvision struct {
	// PVC 是需要动态创建卷的声明。
	PVC *v1.PersistentVolumeClaim
	// NodeCapacity 是可供该 PVC 使用的 CSIStorageCapacity，可能为 nil。
	NodeCapacity *storagev1.CSIStorageCapacity
}

// PodVolumes 保存调度阶段使用的 Pod 卷信息。
type PodVolumes struct {
	// StaticBindings 是可以绑定到预置静态 PV 的 PVC 的绑定决策。
	// 不会绑定任何 PV 的 PVC 不在此列。
	StaticBindings []*BindingInfo
	// DynamicProvisions 是需要动态创建的 PVC。
	// 空切片与 nil 均可，nil 表示无需动态创建。
	DynamicProvisions []*DynamicProvision
}

// InTreeToCSITranslator 提供检查 in-tree 插件是否可迁移，以及把 in-tree PV 转换为 CSI PV 所需的方法。
type InTreeToCSITranslator interface {
	// IsPVMigratable 判断该 PV 是否可以转换为 CSI 卷。
	IsPVMigratable(pv *v1.PersistentVolume) bool
	// GetInTreePluginNameFromSpec 从 PV（或卷）规格中取出 in-tree 插件名。
	GetInTreePluginNameFromSpec(pv *v1.PersistentVolume, vol *v1.Volume) (string, error)
	// TranslateInTreePVToCSI 把 in-tree PV 规格转换为等价的 CSI PV 规格，不修改原对象。
	TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
}

// SchedulerVolumeBinder 供调度器的 VolumeBinding 插件处理 PVC/PV 绑定与动态创建。
// 绑定决策被集成进 Pod 的调度流程，从而在考虑 Pod 其他调度条件的同时也考虑 PV 的 NodeAffinity。
//
// 与现有调度流程的集成方式如下：
//  1. 调度器从队列中取出一个 Pod 并串行处理：
//     a. 调用该 Pod 的所有 PreFilter 插件。此处调用 GetPodVolumeClaims()，把 Pod 的卷信息
//     存入当前调度周期的 state，供后续阶段使用。
//     b. 调用所有 Filter 插件，在各节点上并行执行。此处调用 FindPodVolumes()。
//     c. 调用所有 Score 插件。未来/TBD。
//     d. 选出 Pod 的最佳节点。
//     e. 调用所有 Reserve 插件。此处调用 AssumePodVolumes()。
//     i.  若需要绑定 PVC，仅更新内存缓存：
//     * 手动绑定：更新 PV 对象，把其预绑定到对应的 PVC。
//     * 动态创建：给 PVC 对象打上选中的节点注解（来自步骤 d 选出的节点）。
//     * 记录该 Pod 中哪些 PVC/PV 需要调用 API 更新。
//     ii. 之后由调度器主流程把 Pod->Node 的绑定缓存到调度器的 pod cache 中；
//     这部分由调度器处理，不在此处完成。
//     f. 在单独的 goroutine 中异步绑定卷和 Pod。
//     i.  PreBind 阶段先调用 BindPodVolumes()：执行所有必要的 API 更新，并等待
//     PV controller 完成 PVC 的绑定与创建。绑定失败时 Pod 会被送回调度队列重试。
//     ii. BindPodVolumes() 完成后，调度器才做最终的 Pod->Node 绑定。
//  2. 一旦步骤 e 中的所有 assume 操作完成，调度器就处理队列中的下一个 Pod，
//     而真正的绑定操作在后台进行。
type SchedulerVolumeBinder interface {
	// GetPodVolumeClaims 把 Pod 的 PVC 分为：已绑定、延迟绑定（含动态创建）的未绑定 PVC、
	// 立即绑定（含预绑定）的未绑定 PVC，以及属于延迟绑定未绑定 PVC 存储类的 PV。
	GetPodVolumeClaims(logger klog.Logger, pod *v1.Pod) (podVolumeClaims *PodVolumeClaims, err error)

	// FindPodVolumes 检查 Pod 的所有 PVC 能否被该节点满足，并返回 Pod 的卷信息。
	//
	// PVC 已绑定时，检查 PV 的 NodeAffinity 是否匹配该节点；
	// 否则尝试找到一个可用的 PV 绑定到该 PVC。
	//
	// 出错时返回 error；节点当前不可用时返回原因列表。
	//
	// 若启用了 CSIStorageCapacity 特性，还会检查待创建卷的存储空间是否充足。
	//
	// 该函数由调度器的 VolumeBinding 插件调用，可并行调用。
	FindPodVolumes(logger klog.Logger, pod *v1.Pod, podVolumeClaims *PodVolumeClaims, node *v1.Node) (podVolumes *PodVolumes, reasons ConflictReasons, err error)

	// AssumePodVolumes 依次：
	// 1. 取出未绑定 PVC 对应的匹配 PV，假设该 PV 已预绑定到 PVC，更新 PV 缓存。
	// 2. 取出需要动态创建的 PVC，设置相关注解后更新 PVC 缓存。
	//
	// 所有卷均已完全绑定时返回 true。
	//
	// 该函数串行调用。
	AssumePodVolumes(logger klog.Logger, assumedPod *v1.Pod, nodeName string, podVolumes *PodVolumes) (allFullyBound bool, err error)

	// RevertAssumedPodVolumes 回滚 assume 过的 PV 与 PVC 缓存。
	RevertAssumedPodVolumes(podVolumes *PodVolumes)

	// BindPodVolumes 依次：
	// 1. 调用 API 把 PV 预绑定到匹配的 PVC，启动卷绑定。
	// 2. 调用 API 给 PVC 设置相关注解，触发卷创建。
	// 3. 等待 PV controller 完成 PVC 的绑定。
	//
	// 该函数可并行调用。
	BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) error
}

// PodVolumeClaims 按绑定状态和绑定模式对 Pod 的 PVC 与 PV 分类，供卷绑定流程使用。
type PodVolumeClaims struct {
	// boundClaims 是 Pod 中已绑定的 PVC。
	boundClaims []*v1.PersistentVolumeClaim
	// unboundClaimsDelayBinding 是 Pod 中未绑定且为延迟绑定（含动态创建）的 PVC。
	unboundClaimsDelayBinding []*v1.PersistentVolumeClaim
	// unboundClaimsImmediate 是 Pod 中未绑定且为立即绑定的 PVC（按理应当已经绑定）。
	unboundClaimsImmediate []*v1.PersistentVolumeClaim
	// unboundVolumesDelayBinding 是属于 Pod 中延迟绑定未绑定 PVC 的存储类的 PV。
	unboundVolumesDelayBinding map[string][]*v1.PersistentVolume
}

type volumeBinder struct {
	// kubeClient 用于向 API server 发起 PV/PVC 更新。
	kubeClient clientset.Interface
	// enableVolumeAttributesClass 控制匹配时是否考虑 VolumeAttributesClass。
	enableVolumeAttributesClass bool

	// classLister 按名字读取 StorageClass。
	classLister storagelisters.StorageClassLister
	// podLister 按名字读取 Pod，用于检查 Pod 是否已被删除。
	podLister corelisters.PodLister
	// nodeLister 按名字读取 Node。
	nodeLister corelisters.NodeLister
	// csiNodeLister 按名字读取 CSINode，用于判断 CSI 迁移状态。
	csiNodeLister storagelisters.CSINodeLister

	// pvcCache 是 PVC 的 assume 缓存。
	pvcCache PVCAssumeCache
	// pvCache 是 PV 的 assume 缓存。
	pvCache PVAssumeCache

	// bindTimeout 是等待绑定操作成功的最长时间。
	bindTimeout time.Duration

	// translator 用于 in-tree 到 CSI 的 PV 转换。
	translator InTreeToCSITranslator

	// csiDriverLister 按名字读取 CSIDriver。
	csiDriverLister storagelisters.CSIDriverLister
	// csiStorageCapacityLister 读取 CSIStorageCapacity，用于容量检查。
	csiStorageCapacityLister storagelisters.CSIStorageCapacityLister
}

var _ SchedulerVolumeBinder = &volumeBinder{}

// CapacityCheck 保存 NewVolumeBinder 的附加参数，仅当需要
// 按可用存储容量检查卷大小时才用到。
type CapacityCheck struct {
	// CSIDriverInformer 提供 CSIDriver 的 informer。
	CSIDriverInformer storageinformers.CSIDriverInformer
	// CSIStorageCapacityInformer 提供 CSIStorageCapacity 的 informer。
	CSIStorageCapacityInformer storageinformers.CSIStorageCapacityInformer
}

// NewVolumeBinder 建立调度器做卷绑定决策所需的全部缓存。
//
// capacityCheck 决定如何检查存储容量（CSIStorageCapacity 特性）。
func NewVolumeBinder(
	logger klog.Logger,
	kubeClient clientset.Interface,
	fts feature.Features,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	csiNodeInformer storageinformers.CSINodeInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	storageClassInformer storageinformers.StorageClassInformer,
	capacityCheck CapacityCheck,
	bindTimeout time.Duration) (SchedulerVolumeBinder, error) {

	pvcCache, err1 := NewPVCAssumeCache(logger, pvcInformer.Informer())
	pvCache, err2 := NewPVAssumeCache(logger, pvInformer.Informer())
	if err := errors.Join(err1, err2); err != nil {
		return nil, err
	}
	b := &volumeBinder{
		kubeClient:                  kubeClient,
		enableVolumeAttributesClass: fts.EnableVolumeAttributesClass,
		podLister:                   podInformer.Lister(),
		classLister:                 storageClassInformer.Lister(),
		nodeLister:                  nodeInformer.Lister(),
		csiNodeLister:               csiNodeInformer.Lister(),
		pvcCache:                    pvcCache,
		pvCache:                     pvCache,
		bindTimeout:                 bindTimeout,
		translator:                  csitrans.New(),
	}

	b.csiDriverLister = capacityCheck.CSIDriverInformer.Lister()
	b.csiStorageCapacityLister = capacityCheck.CSIStorageCapacityInformer.Lister()

	return b, nil
}

// FindPodVolumes 为给定 Pod 和节点找到 PVC 匹配的 PV，以及需要创建 PV 的节点；
// 节点不合适时返回冲突原因。
func (b *volumeBinder) FindPodVolumes(logger klog.Logger, pod *v1.Pod, podVolumeClaims *PodVolumeClaims, node *v1.Node) (podVolumes *PodVolumes, reasons ConflictReasons, err error) {
	podVolumes = &PodVolumes{}

	// 该日志会被打印多次（#60933），因此使用较高的日志级别。
	logger.V(5).Info("FindPodVolumes", "pod", klog.KObj(pod), "node", klog.KObj(node))

	// 对没有卷的 Pod 初始化为 true。函数正常返回时这些布尔值会被转换成原因字符串。
	unboundVolumesSatisfied := true
	boundVolumesSatisfied := true
	sufficientStorage := true
	boundPVsFound := true
	defer func() {
		// 出错时原因没有意义，交给上层处理。
		if err != nil {
			return
		}
		if !boundVolumesSatisfied {
			reasons = append(reasons, ErrReasonNodeConflict)
		}
		if !unboundVolumesSatisfied {
			reasons = append(reasons, ErrReasonBindConflict)
		}
		if !sufficientStorage {
			reasons = append(reasons, ErrReasonNotEnoughSpace)
		}
		if !boundPVsFound {
			reasons = append(reasons, ErrReasonPVNotExist)
		}
	}()

	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("predicate").Inc()
		}
	}()

	var (
		staticBindings    []*BindingInfo
		dynamicProvisions []*DynamicProvision
	)
	defer func() {
		// 本函数不区分 nil 和空切片，但为了便于测试，把空切片统一规范为 nil。
		if len(staticBindings) == 0 {
			staticBindings = nil
		}
		if len(dynamicProvisions) == 0 {
			dynamicProvisions = nil
		}
		podVolumes.StaticBindings = staticBindings
		podVolumes.DynamicProvisions = dynamicProvisions
	}()

	// 检查已绑定卷的 PV node affinity。
	if len(podVolumeClaims.boundClaims) > 0 {
		boundVolumesSatisfied, boundPVsFound, err = b.checkBoundClaims(logger, podVolumeClaims.boundClaims, node, pod)
		if err != nil {
			return
		}
	}

	// 为未绑定的 PVC 查找匹配的卷或节点。
	if len(podVolumeClaims.unboundClaimsDelayBinding) > 0 {
		var (
			claimsToFindMatching []*v1.PersistentVolumeClaim
			claimsToProvision    []*v1.PersistentVolumeClaim
		)

		// 区分需要动态创建和需要查找匹配 PV 的 PVC。
		for _, claim := range podVolumeClaims.unboundClaimsDelayBinding {
			if selectedNode, ok := claim.Annotations[volume.AnnSelectedNode]; ok {
				if selectedNode != node.Name {
					// 已选中其他节点，快速路径直接判为不匹配。
					unboundVolumesSatisfied = false
					return
				}
				// 注解选中的是当前节点，说明该 PVC 需要动态创建。
				claimsToProvision = append(claimsToProvision, claim)
			} else {
				claimsToFindMatching = append(claimsToFindMatching, claim)
			}
		}

		// 为没有 selectedNode 注解的 PVC 查找可绑定的 PV。
		if len(claimsToFindMatching) > 0 {
			var unboundClaims []*v1.PersistentVolumeClaim
			unboundVolumesSatisfied, staticBindings, unboundClaims, err = b.findMatchingVolumes(logger, pod, claimsToFindMatching, podVolumeClaims.unboundVolumesDelayBinding, node)
			if err != nil {
				return
			}
			// 没找到 PV 的 PVC 转入动态创建流程。
			claimsToProvision = append(claimsToProvision, unboundClaims...)
		}

		// 检查需要动态创建的 PVC。这是首次可能发现节点存储不足的地方。
		if len(claimsToProvision) > 0 {
			unboundVolumesSatisfied, sufficientStorage, dynamicProvisions, err = b.checkVolumeProvisions(logger, pod, claimsToProvision, node)
			if err != nil {
				return
			}
		}
	}

	return
}

// convertDynamicProvisionsToPVCs 把 []*DynamicProvision 转换为 PVC 切片，
// 便于统一处理需要动态创建的声明。
func convertDynamicProvisionsToPVCs(dynamicProvisions []*DynamicProvision) []*v1.PersistentVolumeClaim {
	pvcs := make([]*v1.PersistentVolumeClaim, 0, len(dynamicProvisions))
	for _, dynamicProvision := range dynamicProvisions {
		pvcs = append(pvcs, dynamicProvision.PVC)
	}
	return pvcs
}

// AssumePodVolumes 取出 Pod 卷信息中为选中节点准备的匹配 PV 和待创建 PVC，并执行：
// 1. 假设该 PV 已预绑定到 PVC，更新 pvCache。
// 2. 给待创建的 PVC 设置注解，更新 pvcCache。
// 3. 用缓存中待 API 更新的对象重新填充 PodVolumes。
func (b *volumeBinder) AssumePodVolumes(logger klog.Logger, assumedPod *v1.Pod, nodeName string, podVolumes *PodVolumes) (allFullyBound bool, err error) {
	logger.V(4).Info("AssumePodVolumes", "pod", klog.KObj(assumedPod), "node", klog.KRef("", nodeName))
	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("assume").Inc()
		}
	}()

	if allBound := b.arePodVolumesBound(logger, assumedPod); allBound {
		logger.V(4).Info("AssumePodVolumes: all PVCs bound and nothing to do", "pod", klog.KObj(assumedPod), "node", klog.KRef("", nodeName))
		return true, nil
	}

	// 先 assume PV：只有真正改了绑定关系才需要写缓存，避免无谓的缓存污染。
	newBindings := []*BindingInfo{}
	for _, binding := range podVolumes.StaticBindings {
		newPV, dirty, err := volume.GetBindVolumeToClaim(binding.pv, binding.pvc)
		logger.V(5).Info("AssumePodVolumes: GetBindVolumeToClaim",
			"pod", klog.KObj(assumedPod),
			"PV", klog.KObj(binding.pv),
			"PVC", klog.KObj(binding.pvc),
			"newPV", klog.KObj(newPV),
			"dirty", dirty,
		)
		if err != nil {
			logger.Error(err, "AssumePodVolumes: fail to GetBindVolumeToClaim")
			b.revertAssumedPVs(newBindings)
			return false, err
		}
		// TODO: 能否每次都执行 assume？
		if dirty {
			err = b.pvCache.Assume(newPV)
			if err != nil {
				b.revertAssumedPVs(newBindings)
				return false, err
			}
		}
		newBindings = append(newBindings, &BindingInfo{pv: newPV, pvc: binding.pvc})
	}

	// 再 assume PVC：给待创建的 PVC 打上选中节点的注解。
	newProvisionedPVCs := []*DynamicProvision{}
	for _, dynamicProvision := range podVolumes.DynamicProvisions {
		// 入参中的 claim 可能指向 watcher 缓存，不能直接修改，必须先深拷贝。
		claimClone := dynamicProvision.PVC.DeepCopy()
		metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, volume.AnnSelectedNode, nodeName)
		err = b.pvcCache.Assume(claimClone)
		if err != nil {
			// assume 失败时回滚所有已修改的 PV 和 PVC，避免缓存和 API 不一致。
			pvcs := convertDynamicProvisionsToPVCs(newProvisionedPVCs)
			b.revertAssumedPVs(newBindings)
			b.revertAssumedPVCs(pvcs)
			return
		}

		newProvisionedPVCs = append(newProvisionedPVCs, &DynamicProvision{PVC: claimClone})
	}

	podVolumes.StaticBindings = newBindings
	podVolumes.DynamicProvisions = newProvisionedPVCs
	return
}

// RevertAssumedPodVolumes 回滚 assume 过的 PV 与 PVC 缓存。
func (b *volumeBinder) RevertAssumedPodVolumes(podVolumes *PodVolumes) {
	pvcs := convertDynamicProvisionsToPVCs(podVolumes.DynamicProvisions)
	b.revertAssumedPVs(podVolumes.StaticBindings)
	b.revertAssumedPVCs(pvcs)
}

// BindPodVolumes 从 Pod 的卷信息中取出缓存的绑定与待创建 PVC，发起相应的 API 更新，
// 并等待 PV controller 完成 PVC 的完全绑定。
func (b *volumeBinder) BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("BindPodVolumes", "pod", klog.KObj(assumedPod), "node", klog.KRef("", assumedPod.Spec.NodeName))

	defer func() {
		if err != nil {
			metrics.VolumeSchedulingStageFailed.WithLabelValues("bind").Inc()
		}
	}()

	bindings := podVolumes.StaticBindings
	claimsToProvision := convertDynamicProvisionsToPVCs(podVolumes.DynamicProvisions)

	// 先把 assume 的变更写入 API server，持久化绑定意图。
	err = b.bindAPIUpdate(ctx, assumedPod, bindings, claimsToProvision)
	if err != nil {
		return err
	}

	// 绑定由 controller 异步完成，轮询直到全部绑定、出错或超时。
	err = wait.PollUntilContextTimeout(ctx, time.Second, b.bindTimeout, false, func(ctx context.Context) (bool, error) {
		b, err := b.checkBindings(logger, assumedPod, bindings, claimsToProvision)
		return b, err
	})
	if err != nil {
		return fmt.Errorf("binding volumes: %w", err)
	}
	return nil
}

// getPodName 返回 "namespace/name" 形式的 Pod 标识，用于日志和错误信息。
func getPodName(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

// getPVCName 返回 "namespace/name" 形式的 PVC 键，与缓存中的键格式一致。
func getPVCName(pvc *v1.PersistentVolumeClaim) string {
	return pvc.Namespace + "/" + pvc.Name
}

// bindAPIUpdate 把 assume 过的 PV/PVC 变更写入 API server。
func (b *volumeBinder) bindAPIUpdate(ctx context.Context, pod *v1.Pod, bindings []*BindingInfo, claimsToProvision []*v1.PersistentVolumeClaim) error {
	logger := klog.FromContext(ctx)
	podName := getPodName(pod)
	if bindings == nil {
		return fmt.Errorf("failed to get cached bindings for pod %q", podName)
	}
	if claimsToProvision == nil {
		return fmt.Errorf("failed to get cached claims to provision for pod %q", podName)
	}

	lastProcessedBinding := 0
	lastProcessedProvisioning := 0
	defer func() {
		// 只回滚尚未成功写入 API 的卷，已写入的不能回滚，否则会破坏真实绑定。
		if lastProcessedBinding < len(bindings) {
			b.revertAssumedPVs(bindings[lastProcessedBinding:])
		}
		// 同理，只回滚尚未更新的 PVC。
		if lastProcessedProvisioning < len(claimsToProvision) {
			b.revertAssumedPVCs(claimsToProvision[lastProcessedProvisioning:])
		}
	}()

	var (
		binding *BindingInfo
		i       int
		claim   *v1.PersistentVolumeClaim
	)

	// 执行预绑定，后续绑定由 PV controller 完成。
	// API 层面没有回滚：一旦写入成功，实际绑定失败也无法撤销。
	for _, binding = range bindings {
		// TODO: 如果没有内容需要更新，还发起 API 调用是否有害？
		logger.V(5).Info("Updating PersistentVolume: binding to claim", "pod", klog.KObj(pod), "PV", klog.KObj(binding.pv), "PVC", klog.KObj(binding.pvc))
		newPV, err := b.kubeClient.CoreV1().PersistentVolumes().Update(ctx, binding.pv, metav1.UpdateOptions{})
		if err != nil {
			logger.V(4).Info("Updating PersistentVolume: binding to claim failed", "pod", klog.KObj(pod), "PV", klog.KObj(binding.pv), "PVC", klog.KObj(binding.pvc), "err", err)
			return err
		}

		logger.V(2).Info("Updated PersistentVolume with claim. Waiting for binding to complete", "pod", klog.KObj(pod), "PV", klog.KObj(binding.pv), "PVC", klog.KObj(binding.pvc))
		// 保存 API server 返回的对象，后续用它做 resourceVersion 比较。
		binding.pv = newPV
		lastProcessedBinding++
	}

	// 更新 PVC 以触发卷创建，后续创建由 PV controller 完成。
	// 创建失败时 PV controller 会移除相关注解，向调度器发回重试信号。
	for i, claim = range claimsToProvision {
		logger.V(5).Info("Updating claims objects to trigger volume provisioning", "pod", klog.KObj(pod), "PVC", klog.KObj(claim))
		newClaim, err := b.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			logger.V(4).Info("Updating PersistentVolumeClaim: binding to volume failed", "PVC", klog.KObj(claim), "err", err)
			return err
		}

		// 保存 API server 返回的对象，后续用它做 resourceVersion 比较。
		claimsToProvision[i] = newClaim
		lastProcessedProvisioning++
	}

	return nil
}

var (
	// versioner 用于比较对象的新旧，判断缓存是否已收到 API server 的最新版本。
	versioner = storage.APIObjectVersioner{}
)

// checkBindings 遍历 Pod 的所有 PVC，检查：
// * PVC 是否已完全绑定；
// * 是否存在需要绑定失败并重试的条件。
//
// 所有 PVC 完全绑定时返回 true；绑定（及调度）需要重试时返回 error。
// 注意这里检查的是 API 对象而非 PV/PVC 缓存：主调度循环可能再次 assume 缓存，
// 必须与 PV controller、provisioner 共享的 API server 最新状态为准。
func (b *volumeBinder) checkBindings(logger klog.Logger, pod *v1.Pod, bindings []*BindingInfo, claimsToProvision []*v1.PersistentVolumeClaim) (bool, error) {
	podName := getPodName(pod)
	if bindings == nil {
		return false, fmt.Errorf("failed to get cached bindings for pod %q", podName)
	}
	if claimsToProvision == nil {
		return false, fmt.Errorf("failed to get cached claims to provision for pod %q", podName)
	}

	node, err := b.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return false, fmt.Errorf("failed to get node %q: %w", pod.Spec.NodeName, err)
	}

	csiNode, err := b.csiNodeLister.Get(node.Name)
	if err != nil {
		// TODO: 等 CSINode 默认创建后再把该错误返回。
		logger.V(4).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	// 检查是否存在需要调度重试的条件。

	// Pod 被删除时绑定操作应取消，无需再检查 PV/PVC 绑定状态。
	_, err = b.podLister.Pods(pod.Namespace).Get(pod.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("pod does not exist any more: %w", err)
		}
		logger.Error(err, "Failed to get pod from the lister", "pod", klog.KObj(pod))
	}

	for _, binding := range bindings {
		pv, err := b.pvCache.GetAPIObj(binding.pv.Name)
		if err != nil {
			return false, fmt.Errorf("failed to check binding: %w", err)
		}

		pvc, err := b.pvcCache.GetAPIObj(getPVCName(binding.pvc))
		if err != nil {
			return false, fmt.Errorf("failed to check binding: %w", err)
		}

		// 我们刚更新过 API server 中的 PV，若缓存中的 API 对象更旧，
		// 说明更新还没传播回来，等下一轮再检查。
		if versioner.CompareResourceVersion(binding.pv, pv) > 0 {
			return false, nil
		}

		// CSI 迁移开启时，in-tree PV 的 affinity 检查要按转换后的 CSI 卷进行。
		pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
		if err != nil {
			return false, fmt.Errorf("failed to translate pv to csi: %w", err)
		}

		// 检查 PV 的 node affinity（节点可能没有对应标签）。
		if err := volume.CheckNodeAffinity(pv, node.Labels); err != nil {
			return false, fmt.Errorf("pv %q node affinity doesn't match node %q: %w", pv.Name, node.Name, err)
		}

		// 检查 unbindVolume() 是否把 pv.ClaimRef 清空了。
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID == "" {
			return false, fmt.Errorf("ClaimRef got reset for pv %q", pv.Name)
		}

		// 检查 PVC 是否已完全绑定。
		if !b.isPVCFullyBound(pvc) {
			return false, nil
		}
	}

	for _, claim := range claimsToProvision {
		pvc, err := b.pvcCache.GetAPIObj(getPVCName(claim))
		if err != nil {
			return false, fmt.Errorf("failed to check provisioning pvc: %w", err)
		}

		// 我们刚更新过 API server 中的 PVC，若缓存中的 API 对象更旧，
		// 说明更新还没传播回来，等下一轮再检查。
		if versioner.CompareResourceVersion(claim, pvc) > 0 {
			return false, nil
		}

		// 检查 selectedNode 注解是否仍然存在。
		if pvc.Annotations == nil {
			return false, fmt.Errorf("selectedNode annotation reset for PVC %q", pvc.Name)
		}
		selectedNode := pvc.Annotations[volume.AnnSelectedNode]
		if selectedNode != pod.Spec.NodeName {
			// provisioner 创建卷失败时会移除 selectedNode 注解，
			// 以此通知调度器重试。
			return false, fmt.Errorf("provisioning failed for PVC %q", pvc.Name)
		}

		// PVC 已绑定到 PV 时，检查该 PV 的 node affinity。
		if pvc.Spec.VolumeName != "" {
			pv, err := b.pvCache.GetAPIObj(pvc.Spec.VolumeName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					// 这里的 NotFound 可以容忍：可能是 API 延迟导致暂时查不到，
					// 下一轮再检查；若 PV 真的被删除，PVC 最终会被解绑。
					return false, nil
				}
				return false, fmt.Errorf("failed to get pv %q from cache: %w", pvc.Spec.VolumeName, err)
			}

			pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
			if err != nil {
				return false, err
			}

			if err := volume.CheckNodeAffinity(pv, node.Labels); err != nil {
				return false, fmt.Errorf("pv %q node affinity doesn't match node %q: %w", pv.Name, node.Name, err)
			}
		}

		// 检查 PVC 是否已完全绑定。
		if !b.isPVCFullyBound(pvc) {
			return false, nil
		}
	}

	// 操作过的所有 PV 和 PVC 均已绑定。
	logger.V(2).Info("All PVCs for pod are bound", "pod", klog.KObj(pod))
	return true, nil
}

// isVolumeBound 判断 Pod 中的一个卷是否已绑定，并返回其对应的 PVC。
func (b *volumeBinder) isVolumeBound(logger klog.Logger, pod *v1.Pod, vol *v1.Volume) (bound bool, pvc *v1.PersistentVolumeClaim, err error) {
	pvcName := ""
	isEphemeral := false
	switch {
	case vol.PersistentVolumeClaim != nil:
		pvcName = vol.PersistentVolumeClaim.ClaimName
	case vol.Ephemeral != nil:
		// 通用临时卷同样使用 PVC，只是名字由 Pod 和卷信息推导而来。
		pvcName = ephemeral.VolumeClaimName(pod, vol)
		isEphemeral = true
	default:
		// 非 PVC 卷无需绑定判断。
		return true, nil, nil
	}

	bound, pvc, err = b.isPVCBound(logger, pod.Namespace, pvcName)
	// 临时卷还必须确认其 PVC 归属于该 Pod，避免使用同名但属于别人的 PVC。
	if isEphemeral && err == nil && pvc != nil {
		if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
			return false, nil, err
		}
	}
	return
}

// isPVCBound 从缓存读取 PVC，并判断其是否已完全绑定到 PV。
func (b *volumeBinder) isPVCBound(logger klog.Logger, namespace, pvcName string) (bool, *v1.PersistentVolumeClaim, error) {
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	pvcKey := getPVCName(claim)
	pvc, err := b.pvcCache.Get(pvcKey)
	if err != nil || pvc == nil {
		return false, nil, fmt.Errorf("error getting PVC %q: %v", pvcKey, err)
	}

	fullyBound := b.isPVCFullyBound(pvc)
	if fullyBound {
		logger.V(5).Info("PVC is fully bound to PV", "PVC", klog.KObj(pvc), "PV", klog.KRef("", pvc.Spec.VolumeName))
	} else {
		if pvc.Spec.VolumeName != "" {
			logger.V(5).Info("PVC is not fully bound to PV", "PVC", klog.KObj(pvc), "PV", klog.KRef("", pvc.Spec.VolumeName))
		} else {
			logger.V(5).Info("PVC is not bound", "PVC", klog.KObj(pvc))
		}
	}
	return fullyBound, pvc, nil
}

// isPVCFullyBound 判断 PVC 是否既指定了 VolumeName，又带有绑定完成注解。
// 两者缺一不可：仅设置 VolumeName 只是预绑定，controller 尚未完成绑定。
func (b *volumeBinder) isPVCFullyBound(pvc *v1.PersistentVolumeClaim) bool {
	return pvc.Spec.VolumeName != "" && metav1.HasAnnotation(pvc.ObjectMeta, volume.AnnBindCompleted)
}

// arePodVolumesBound 在 Pod 的所有卷均已完全绑定时返回 true。
func (b *volumeBinder) arePodVolumesBound(logger klog.Logger, pod *v1.Pod) bool {
	for _, vol := range pod.Spec.Volumes {
		if isBound, _, _ := b.isVolumeBound(logger, pod, &vol); !isBound {
			// Pod 至少有一个 PVC 还需要绑定。
			return false
		}
	}
	return true
}

// GetPodVolumeClaims 把 Pod 的 PVC 分为：已绑定、延迟绑定（含动态创建）的未绑定 PVC、
// 立即绑定（含预绑定）的未绑定 PVC，以及属于延迟绑定未绑定 PVC 存储类的 PV。
func (b *volumeBinder) GetPodVolumeClaims(logger klog.Logger, pod *v1.Pod) (podVolumeClaims *PodVolumeClaims, err error) {
	podVolumeClaims = &PodVolumeClaims{
		boundClaims:               []*v1.PersistentVolumeClaim{},
		unboundClaimsImmediate:    []*v1.PersistentVolumeClaim{},
		unboundClaimsDelayBinding: []*v1.PersistentVolumeClaim{},
	}

	for _, vol := range pod.Spec.Volumes {
		volumeBound, pvc, err := b.isVolumeBound(logger, pod, &vol)
		if err != nil {
			return podVolumeClaims, err
		}
		if pvc == nil {
			continue
		}
		if volumeBound {
			podVolumeClaims.boundClaims = append(podVolumeClaims.boundClaims, pvc)
		} else {
			delayBindingMode, err := volume.IsDelayBindingMode(pvc, b.classLister)
			if err != nil {
				return podVolumeClaims, err
			}
			// 已预绑定（有 VolumeName）的 PVC 即使处于延迟绑定存储类，也按立即绑定处理。
			if delayBindingMode && pvc.Spec.VolumeName == "" {
				// 交由调度器决定绑定。
				podVolumeClaims.unboundClaimsDelayBinding = append(podVolumeClaims.unboundClaimsDelayBinding, pvc)
			} else {
				// !delayBindingMode || pvc.Spec.VolumeName != ""
				// 立即绑定本应已经完成，走到这里说明状态异常，记录后交给上层处理。
				podVolumeClaims.unboundClaimsImmediate = append(podVolumeClaims.unboundClaimsImmediate, pvc)
			}
		}
	}

	podVolumeClaims.unboundVolumesDelayBinding = map[string][]*v1.PersistentVolume{}
	for _, pvc := range podVolumeClaims.unboundClaimsDelayBinding {
		// 按 PVC 的存储类分组预取可用 PV，供 FindPodVolumes 并行使用。
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs, err := b.pvCache.ListPVs(storageClassName)
		if err != nil {
			return nil, err
		}
		podVolumeClaims.unboundVolumesDelayBinding[storageClassName] = pvs
	}
	return podVolumeClaims, nil
}

// checkBoundClaims 检查已绑定 PVC 对应的 PV 是否与节点匹配，
// 返回 (nodeAffinity 是否满足, PV 是否找到, error)。
func (b *volumeBinder) checkBoundClaims(logger klog.Logger, claims []*v1.PersistentVolumeClaim, node *v1.Node, pod *v1.Pod) (bool, bool, error) {
	csiNode, err := b.csiNodeLister.Get(node.Name)
	if err != nil {
		// TODO: 等 CSINode 默认创建后再把该错误返回。
		logger.V(5).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	for _, pvc := range claims {
		pvName := pvc.Spec.VolumeName
		pv, err := b.pvCache.Get(pvName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// PV 不存在不算错误，仅把 boundPVsFound 置为 false。
				err = nil
			}
			return true, false, err
		}

		// CSI 迁移场景下按转换后的 PV 检查 affinity，与真正挂载行为保持一致。
		pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
		if err != nil {
			return false, true, err
		}

		err = volume.CheckNodeAffinity(pv, node.Labels)
		if err != nil {
			logger.V(5).Info("PersistentVolume and node mismatch for pod", "PV", klog.KRef("", pvName), "node", klog.KObj(node), "pod", klog.KObj(pod), "err", err)
			return false, true, nil
		}
		logger.V(5).Info("PersistentVolume and node matches for pod", "PV", klog.KRef("", pvName), "node", klog.KObj(node), "pod", klog.KObj(pod))
	}

	logger.V(5).Info("All bound volumes for pod match with node", "pod", klog.KObj(pod), "node", klog.KObj(node))
	return true, true, nil
}

// findMatchingVolumes 尝试为给定 PVC 找到匹配的 PV，并返回无法匹配的 PVC 供后续动态创建。
func (b *volumeBinder) findMatchingVolumes(logger klog.Logger, pod *v1.Pod, claimsToBind []*v1.PersistentVolumeClaim, unboundVolumesDelayBinding map[string][]*v1.PersistentVolume, node *v1.Node) (foundMatches bool, bindings []*BindingInfo, unboundClaims []*v1.PersistentVolumeClaim, err error) {
	// 按申请容量从小到大排序，让小申请优先挑到最合适的 PV。
	sort.Sort(byPVCSize(claimsToBind))

	chosenPVs := map[string]*v1.PersistentVolume{}

	foundMatches = true

	for _, pvc := range claimsToBind {
		// 按 PVC 的存储类取对应的候选 PV。
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs := unboundVolumesDelayBinding[storageClassName]

		// 为 PVC 找一个匹配的 PV，排除已选的 PV，避免多个 PVC 绑定到同一个 PV。
		pv, err := volume.FindMatchingVolume(pvc, pvs, node, chosenPVs, true, b.enableVolumeAttributesClass)
		if err != nil {
			return false, nil, nil, err
		}
		if pv == nil {
			logger.V(5).Info("No matching volumes for pod", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc), "node", klog.KObj(node))
			unboundClaims = append(unboundClaims, pvc)
			foundMatches = false
			continue
		}

		// 记录已匹配的 PV，防止后续 PVC 重复选中。
		chosenPVs[pv.Name] = pv
		bindings = append(bindings, &BindingInfo{pv: pv, pvc: pvc})
		logger.V(5).Info("Found matching PV for PVC for pod", "PV", klog.KObj(pv), "PVC", klog.KObj(pvc), "node", klog.KObj(node), "pod", klog.KObj(pod))
	}

	if foundMatches {
		logger.V(5).Info("Found matching volumes for pod", "pod", klog.KObj(pod), "node", klog.KObj(node))
	}

	return
}

// checkVolumeProvisions 检查给定的未绑定 PVC（已经过 findMatchingVolumes，
// 没有可绑定的 PV），全部可以动态创建时返回 true。
func (b *volumeBinder) checkVolumeProvisions(logger klog.Logger, pod *v1.Pod, claimsToProvision []*v1.PersistentVolumeClaim, node *v1.Node) (provisionSatisfied, sufficientStorage bool, dynamicProvisions []*DynamicProvision, err error) {
	dynamicProvisions = []*DynamicProvision{}

	// 任何检查失败或出错都提前返回，且返回的 dynamicProvisions 为 nil。
	for _, claim := range claimsToProvision {
		pvcName := getPVCName(claim)
		className := volume.GetPersistentVolumeClaimClass(claim)
		if className == "" {
			return false, false, nil, fmt.Errorf("no class for claim %q", pvcName)
		}

		class, err := b.classLister.Get(className)
		if err != nil {
			return false, false, nil, fmt.Errorf("failed to find storage class %q", className)
		}
		provisioner := class.Provisioner
		if provisioner == "" || provisioner == volume.NotSupportedProvisioner {
			logger.V(5).Info("Storage class of claim does not support dynamic provisioning", "storageClassName", className, "PVC", klog.KObj(claim))
			return false, true, nil, nil
		}

		// 检查节点是否满足存储类的拓扑要求。
		if !v1helper.MatchTopologySelectorTerms(class.AllowedTopologies, labels.Set(node.Labels)) {
			logger.V(5).Info("Node cannot satisfy provisioning topology requirements of claim", "node", klog.KObj(node), "PVC", klog.KObj(claim))
			return false, true, nil, nil
		}

		// 检查存储容量。
		sufficient, capacity, err := b.hasEnoughCapacity(logger, provisioner, claim, class, node)
		if err != nil {
			return false, false, nil, err
		}
		if !sufficient {
			// hasEnoughCapacity 已记录了具体原因。
			return true, false, nil, nil
		}

		dynamicProvisions = append(dynamicProvisions, &DynamicProvision{
			PVC:          claim,
			NodeCapacity: capacity,
		})
	}
	logger.V(5).Info("Provisioning for claims of pod that has no matching volumes...", "claimCount", len(claimsToProvision), "pod", klog.KObj(pod), "node", klog.KObj(node))

	return true, true, dynamicProvisions, nil
}

// revertAssumedPVs 逐个恢复 assume 过的 PV 缓存条目。
func (b *volumeBinder) revertAssumedPVs(bindings []*BindingInfo) {
	for _, BindingInfo := range bindings {
		b.pvCache.Restore(BindingInfo.pv)
	}
}

// revertAssumedPVCs 逐个恢复 assume 过的 PVC 缓存条目。
func (b *volumeBinder) revertAssumedPVCs(claims []*v1.PersistentVolumeClaim) {
	for _, claim := range claims {
		b.pvcCache.Restore(claim)
	}
}

// hasEnoughCapacity 检查 provisioner 在该节点可访问的范围内，是否还有足够容量创建指定大小的卷。
// 返回与 PVC 存储类匹配的 CSIStorageCapacity。
func (b *volumeBinder) hasEnoughCapacity(logger klog.Logger, provisioner string, claim *v1.PersistentVolumeClaim, storageClass *storagev1.StorageClass, node *v1.Node) (bool, *storagev1.CSIStorageCapacity, error) {
	quantity, ok := claim.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		// 没有容量请求，无需检查。
		return true, nil, nil
	}

	// 仅对显式启用容量调度的 CSI driver 生效。
	driver, err := b.csiDriverLister.Get(provisioner)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// provisioner 不是 CSI driver，或者 driver 未启用容量调度，
			// 两种情况都跳过容量检查。
			return true, nil, nil
		}
		return false, nil, err
	}
	if driver.Spec.StorageCapacity == nil || !*driver.Spec.StorageCapacity {
		return true, nil, nil
	}

	// 寻找匹配的 CSIStorageCapacity 对象。
	// TODO (for beta): 做一次基准测试，考虑引入某种查找结构 (https://github.com/kubernetes/enhancements/issues/1698#issuecomment-654356718)。
	capacities, err := b.csiStorageCapacityLister.List(labels.Everything())
	if err != nil {
		return false, nil, err
	}

	sizeInBytes := quantity.Value()
	for _, capacity := range capacities {
		if capacity.StorageClassName == storageClass.Name &&
			capacitySufficient(capacity, sizeInBytes) &&
			b.nodeHasAccess(logger, node, capacity) {
			// 找到足够容量。
			return true, capacity, nil
		}
	}

	// TODO (?): 失败信息没有说明考虑了哪些容量池、为何被拒绝。
	// 要在上面打日志吗？但那样日志量可能很大……
	logger.V(5).Info("Node has no accessible CSIStorageCapacity with enough capacity for PVC",
		"node", klog.KObj(node), "PVC", klog.KObj(claim), "size", sizeInBytes, "storageClass", klog.KObj(storageClass))
	return false, nil, nil
}

// capacitySufficient 判断容量对象是否能容纳指定大小的卷；没有容量上限信息时视为不足。
func capacitySufficient(capacity *storagev1.CSIStorageCapacity, sizeInBytes int64) bool {
	limit := volumeLimit(capacity)
	return limit != nil && limit.Value() >= sizeInBytes
}

// volumeLimit 取容量对象的有效上限，优先使用更精确的 MaximumVolumeSize。
func volumeLimit(capacity *storagev1.CSIStorageCapacity) *resource.Quantity {
	if capacity.MaximumVolumeSize != nil {
		// MaximumVolumeSize 更精确，优先使用。
		return capacity.MaximumVolumeSize
	}
	return capacity.Capacity
}

// nodeHasAccess 判断节点是否满足容量对象声明的拓扑约束。
func (b *volumeBinder) nodeHasAccess(logger klog.Logger, node *v1.Node, capacity *storagev1.CSIStorageCapacity) bool {
	if capacity.NodeTopology == nil {
		// 没有拓扑信息，视为不可访问。
		return false
	}
	// 目前只支持按标签匹配。
	selector, err := metav1.LabelSelectorAsSelector(capacity.NodeTopology)
	if err != nil {
		logger.Error(err, "Unexpected error converting to a label selector", "nodeTopology", capacity.NodeTopology)
		return false
	}
	return selector.Matches(labels.Set(node.Labels))
}

// byPVCSize 按申请容量对 PVC 排序，满足 sort.Interface。
type byPVCSize []*v1.PersistentVolumeClaim

// Len 返回 PVC 数量。
func (a byPVCSize) Len() int {
	return len(a)
}

// Swap 交换两个 PVC 的位置。
func (a byPVCSize) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less 按存储申请量升序排列，保证小申请优先匹配。
func (a byPVCSize) Less(i, j int) bool {
	iSize := a[i].Spec.Resources.Requests[v1.ResourceStorage]
	jSize := a[j].Spec.Resources.Requests[v1.ResourceStorage]
	// iSize 小于 jSize 时返回 true。
	return iSize.Cmp(jSize) == -1
}

// isCSIMigrationOnForPlugin 检查给定 in-tree 插件是否启用了 CSI 迁移。
func isCSIMigrationOnForPlugin(pluginName string) bool {
	switch pluginName {
	case csiplugins.AWSEBSInTreePluginName:
		return true
	case csiplugins.GCEPDInTreePluginName:
		return true
	case csiplugins.AzureDiskInTreePluginName:
		return true
	case csiplugins.CinderInTreePluginName:
		return true
	case csiplugins.PortworxVolumePluginName:
		return true
	}
	return false
}

// isPluginMigratedToCSIOnNode 检查 in-tree 插件在该节点上是否已迁移到 CSI driver。
func isPluginMigratedToCSIOnNode(pluginName string, csiNode *storagev1.CSINode) bool {
	if csiNode == nil {
		return false
	}

	csiNodeAnn := csiNode.GetAnnotations()
	if csiNodeAnn == nil {
		return false
	}

	var mpaSet sets.Set[string]
	mpa := csiNodeAnn[v1.MigratedPluginsAnnotationKey]
	if len(mpa) == 0 {
		// 注解存在但为空，按空集合处理。
		mpaSet = sets.New[string]()
	} else {
		tok := strings.Split(mpa, ",")
		mpaSet = sets.New(tok...)
	}

	return mpaSet.Has(pluginName)
}

// tryTranslatePVToCSI 在满足条件时把 in-tree PV 转换为 CSI PV；
// 不满足条件时原样返回该 PV。
func (b *volumeBinder) tryTranslatePVToCSI(logger klog.Logger, pv *v1.PersistentVolume, csiNode *storagev1.CSINode) (*v1.PersistentVolume, error) {
	if !b.translator.IsPVMigratable(pv) {
		return pv, nil
	}

	pluginName, err := b.translator.GetInTreePluginNameFromSpec(pv, nil)
	if err != nil {
		return nil, fmt.Errorf("could not get plugin name from pv: %v", err)
	}

	// 只有全局开启迁移、且目标节点已迁移到 CSI 时才转换，
	// 否则 affinity 检查要按原 in-tree 卷的语义进行。
	if !isCSIMigrationOnForPlugin(pluginName) {
		return pv, nil
	}

	if !isPluginMigratedToCSIOnNode(pluginName, csiNode) {
		return pv, nil
	}

	transPV, err := b.translator.TranslateInTreePVToCSI(logger, pv)
	if err != nil {
		return nil, fmt.Errorf("could not translate pv: %v", err)
	}

	return transPV, nil
}
