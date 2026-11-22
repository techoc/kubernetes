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

package volumerestrictions

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Package volumerestrictions 提供卷使用限制相关的调度插件，
// 处理磁盘冲突与 ReadWriteOncePod 访问模式冲突。

// VolumeRestrictions 是检查卷使用限制的插件。
type VolumeRestrictions struct {
	pvcLister                                          corelisters.PersistentVolumeClaimLister
	sharedLister                                       fwk.SharedLister
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &VolumeRestrictions{}
var _ fwk.FilterPlugin = &VolumeRestrictions{}
var _ fwk.EnqueueExtensions = &VolumeRestrictions{}
var _ fwk.StateData = &preFilterState{}
var _ fwk.SignPlugin = &VolumeRestrictions{}

const (
	// Name 是插件在注册表和配置中使用的名称。
	Name = names.VolumeRestrictions
	// preFilterStateKey 是 CycleState 中存放过滤阶段预计算数据的键。
	// 用插件名作前缀可降低与其他插件的键冲突概率。
	preFilterStateKey = "PreFilter" + Name

	// ErrReasonDiskConflict 是磁盘冲突导致不可调度时的原因字符串。
	ErrReasonDiskConflict = "node(s) had no available disk"
	// ErrReasonReadWriteOncePodConflict 用于 ReadWriteOncePod 模式的 PVC 已被其他 Pod 占用而不可调度的场景。
	ErrReasonReadWriteOncePodConflict = "node(s) unavailable due to PersistentVolumeClaim with ReadWriteOncePod access mode already in-use by another pod"
)

// preFilterState 在 PreFilter 阶段计算，供 Filter 阶段使用。
type preFilterState struct {
	// readWriteOncePodPVCs 是待调度 Pod 中采用 ReadWriteOncePod 访问模式的 PVC 名称集合。
	readWriteOncePodPVCs sets.Set[string]
	// conflictingPVCRefCount 是集群中已调度 Pod 对这些 PVC 的引用总数。
	conflictingPVCRefCount int
}

// updateWithPod 按 multiplier（+1 添加、-1 移除）增量更新冲突引用计数。
func (s *preFilterState) updateWithPod(podInfo fwk.PodInfo, multiplier int) {
	s.conflictingPVCRefCount += multiplier * s.conflictingPVCRefCountForPod(podInfo)
}

// conflictingPVCRefCountForPod 统计单个 Pod 引用了多少个受限制的 PVC。
func (s *preFilterState) conflictingPVCRefCountForPod(podInfo fwk.PodInfo) int {
	conflicts := 0
	for _, volume := range podInfo.GetPod().Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if s.readWriteOncePodPVCs.Has(volume.PersistentVolumeClaim.ClaimName) {
			conflicts += 1
		}
	}
	return conflicts
}

// Clone 返回 preFilterState 的副本，供框架在分支节点时复制状态。
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}
	return &preFilterState{
		readWriteOncePodPVCs:   s.readWriteOncePodPVCs,
		conflictingPVCRefCount: s.conflictingPVCRefCount,
	}
}

// Name 返回插件名称，用于日志等场景。
func (pl *VolumeRestrictions) Name() string {
	return Name
}

// SignPod 基于非合成卷来源生成签名，用于可行性与打分阶段的快速判定。
func (pl *VolumeRestrictions) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)},
	}, nil
}

// isVolumeConflict 判断给定卷是否与 Pod 已挂载的卷冲突。
func isVolumeConflict(volume *v1.Volume, pod *v1.Pod) bool {
	for _, existingVolume := range pod.Spec.Volumes {
		// 同一块 GCE 磁盘被多个 Pod 挂载时，除非全部只读，否则冲突。
		if volume.GCEPersistentDisk != nil && existingVolume.GCEPersistentDisk != nil {
			disk, existingDisk := volume.GCEPersistentDisk, existingVolume.GCEPersistentDisk
			if disk.PDName == existingDisk.PDName && !(disk.ReadOnly && existingDisk.ReadOnly) {
				return true
			}
		}

		if volume.AWSElasticBlockStore != nil && existingVolume.AWSElasticBlockStore != nil {
			if volume.AWSElasticBlockStore.VolumeID == existingVolume.AWSElasticBlockStore.VolumeID {
				return true
			}
		}

		if volume.ISCSI != nil && existingVolume.ISCSI != nil {
			iqn := volume.ISCSI.IQN
			eiqn := existingVolume.ISCSI.IQN
			// 两个 iSCSI 卷 IQN 相同即视为同一个卷。iSCSI 卷为 RWO/ROX 类型，
			// 只允许一个读写挂载；除非全部以只读方式挂载，否则多个 Pod 共享会冲突。
			if iqn == eiqn && !(volume.ISCSI.ReadOnly && existingVolume.ISCSI.ReadOnly) {
				return true
			}
		}

		if volume.RBD != nil && existingVolume.RBD != nil {
			mon, pool, image := volume.RBD.CephMonitors, volume.RBD.RBDPool, volume.RBD.RBDImage
			emon, epool, eimage := existingVolume.RBD.CephMonitors, existingVolume.RBD.RBDPool, existingVolume.RBD.RBDImage
			// 两个 RBD 镜像若共享 Ceph monitor、RADOS 池且镜像名相同，则视为同一个镜像。
			// 同一镜像只允许一个读写挂载；除非所有 Pod 都只读挂载，否则冲突。
			if haveOverlap(mon, emon) && pool == epool && image == eimage && !(volume.RBD.ReadOnly && existingVolume.RBD.ReadOnly) {
				return true
			}
		}
	}

	return false
}

// haveOverlap 判断两个字符串切片是否有交集，
// 用较短切片构建集合以减少内存占用。
func haveOverlap(a1, a2 []string) bool {
	if len(a1) > len(a2) {
		a1, a2 = a2, a1
	}
	m := sets.New(a1...)
	for _, val := range a2 {
		if _, ok := m[val]; ok {
			return true
		}
	}

	return false
}

// needsRestrictionsCheck 在卷属于需要冲突检查的类型时返回 true。
func needsRestrictionsCheck(v v1.Volume) bool {
	return v.GCEPersistentDisk != nil || v.AWSElasticBlockStore != nil || v.RBD != nil || v.ISCSI != nil
}

// PreFilter 计算并存储用于强制执行 ReadWriteOncePod 约束的状态。
// 若节点上不存在需要检查的卷且没有 PVC 冲突，则跳过后续过滤。
func (pl *VolumeRestrictions) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	needsCheck := false
	for i := range pod.Spec.Volumes {
		if needsRestrictionsCheck(pod.Spec.Volumes[i]) {
			needsCheck = true
			break
		}
	}

	pvcs, err := pl.readWriteOncePodPVCsForPod(ctx, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
		}
		return nil, fwk.AsStatus(err)
	}

	s, err := pl.calPreFilterState(ctx, pod, pvcs)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	if !needsCheck && s.conflictingPVCRefCount == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}

// AddPod 在模拟添加 Pod 时增量更新 CycleState 中的预计算状态。
func (pl *VolumeRestrictions) AddPod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToAdd, 1)
	return nil
}

// RemovePod 在模拟移除 Pod 时增量更新 CycleState 中的预计算状态。
func (pl *VolumeRestrictions) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToRemove, -1)
	return nil
}

func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// 读不到状态通常意味着 PreFilter 未被调用。
		return nil, fmt.Errorf("cannot read %q from cycleState", preFilterStateKey)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to volumerestrictions.state error", c)
	}
	return s, nil
}

// calPreFilterState 计算预过滤状态：记录使用 ReadWriteOncePod 的 PVC，
// 以及集群中与之冲突的 Pod 数量。
func (pl *VolumeRestrictions) calPreFilterState(ctx context.Context, pod *v1.Pod, pvcs sets.Set[string]) (*preFilterState, error) {
	conflictingPVCRefCount := 0
	for pvc := range pvcs {
		key := framework.GetNamespacedName(pod.Namespace, pvc)
		if pl.sharedLister.StorageInfos().IsPVCUsedByPods(key) {
			// ReadWriteOncePod PVC 最多只能被一个 Pod 使用，故每个被占用的 PVC 记一次冲突。
			conflictingPVCRefCount += 1
		}
	}
	return &preFilterState{
		readWriteOncePodPVCs:   pvcs,
		conflictingPVCRefCount: conflictingPVCRefCount,
	}, nil
}

// readWriteOncePodPVCsForPod 找出 Pod 引用的所有 ReadWriteOncePod 模式 PVC。
func (pl *VolumeRestrictions) readWriteOncePodPVCsForPod(ctx context.Context, pod *v1.Pod) (sets.Set[string], error) {
	pvcs := sets.New[string]()
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(volume.PersistentVolumeClaim.ClaimName)
		if err != nil {
			return nil, err
		}

		if !v1helper.ContainsAccessMode(pvc.Spec.AccessModes, v1.ReadWriteOncePod) {
			continue
		}
		pvcs.Insert(pvc.Name)
	}
	return pvcs, nil
}

// satisfyVolumeConflicts 检查将 Pod 调度到该节点是否会与已有卷产生冲突。
func satisfyVolumeConflicts(pod *v1.Pod, nodeInfo fwk.NodeInfo) bool {
	for i := range pod.Spec.Volumes {
		v := pod.Spec.Volumes[i]
		if !needsRestrictionsCheck(v) {
			continue
		}
		for _, ev := range nodeInfo.GetPods() {
			if isVolumeConflict(&v, ev.GetPod()) {
				return false
			}
		}
	}
	return true
}

// satisfyReadWriteOncePod 检查 ReadWriteOncePod PVC 是否已被占用而产生冲突。
func satisfyReadWriteOncePod(ctx context.Context, state *preFilterState) *fwk.Status {
	if state == nil {
		return nil
	}
	if state.conflictingPVCRefCount > 0 {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonReadWriteOncePodConflict)
	}
	return nil
}

// PreFilterExtensions 返回 PreFilter 扩展（AddPod/RemovePod）。
func (pl *VolumeRestrictions) PreFilterExtensions() fwk.PreFilterExtensions {
	return pl
}

// Filter 在过滤扩展点执行：判断 Pod 请求的卷与节点上已挂载的卷是否冲突。
// 若同一卷已被挂载，使用它的其他 Pod 不能调度到该节点。
// 目前针对以下卷类型：
//   - GCE PD：允许多个只读挂载
//   - AWS EBS：禁止两个 Pod 挂载同一卷 ID
//   - Ceph RBD：monitor、池、镜像名相同即视为同一镜像，只读例外
//   - iSCSI：IQN 相同即视为同一卷，只读例外
//
// 同时检查 ReadWriteOncePod 模式的 PVC 是否已被占用，以决定是否可抢占。
func (pl *VolumeRestrictions) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonDiskConflict)
	}
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	return satisfyReadWriteOncePod(ctx, state)
}

// EventsToRegister 返回可能让被本插件判为不可调度的 Pod 变为可调度的集群事件。
func (pl *VolumeRestrictions) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// Pod 可能因卷与同节点其他 Pod 冲突而调度失败；
		// 当运行中的 Pod 被删除、卷被释放后即可调度。
		// 由于 `spec.volumes` 是不可变字段，Pod 更新事件无需关注。
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		// 新增节点可能让 Pod 变得可调度；不设置 QueueingHint，因为所有 Node/Add 事件都值得入队。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
		// Pod 可能因依赖的 PVC 尚未创建而调度失败；PVC 存在后才能检查其访问模式。
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add},
			QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimAdded},
	}, nil
}

// isSchedulableAfterPersistentVolumeClaimAdded 在 PVC 新增或变更时调用，
// 判断该变化是否让原先不可调度的 Pod 变得可调度。
func (pl *VolumeRestrictions) isSchedulableAfterPersistentVolumeClaimAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, newPersistentVolumeClaim, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: %w", err)
	}

	if newPersistentVolumeClaim.Namespace != pod.Namespace {
		return fwk.QueueSkip, nil
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if volume.PersistentVolumeClaim.ClaimName == newPersistentVolumeClaim.Name {
			logger.V(5).Info("PVC that is referred from the pod was created, which might make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(newPersistentVolumeClaim))
			return fwk.Queue, nil
		}
	}
	logger.V(5).Info("PVC irrelevant to the Pod was created, which doesn't make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(newPersistentVolumeClaim))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterAssignedPodDeleted 在已分配 Pod 被删除时调用，
// 判断删除该 Pod 是否解除了与目标 Pod 卷的冲突。
func (pl *VolumeRestrictions) isSchedulableAfterAssignedPodDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedPod, _, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterAssignedPodDeleted: %w", err)
	}

	if deletedPod.Namespace != pod.Namespace {
		return fwk.QueueSkip, nil
	}

	nodeInfo := framework.NewNodeInfo(deletedPod)
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		logger.V(5).Info("Pod with the volume that the target pod requires was deleted, which might make this pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
		return fwk.Queue, nil
	}

	// 被删除 Pod 若使用相同 PVC，也应入队：目标 Pod 可能因 ReadWriteOncePod 模式而不可调度。
	//
	// 目前不真正获取 PVC 检查访问模式，因为该操作开销较大。
	// 待 QHint 的可观测性完善后，再视其对延迟的影响决定是否引入该检查。
	// https://github.com/kubernetes/kubernetes/issues/124566
	claims := sets.New[string]()
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			claims.Insert(volume.PersistentVolumeClaim.ClaimName)
		}
	}
	for _, volume := range deletedPod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && claims.Has(volume.PersistentVolumeClaim.ClaimName) {
			logger.V(5).Info("Pod with the same PVC that the target pod requires was deleted, which might make this pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("An irrelevant Pod was deleted, which doesn't make this pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
	return fwk.QueueSkip, nil
}

// New 初始化并返回一个 VolumeRestrictions 插件实例。
func New(_ context.Context, _ runtime.Object, handle fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	informerFactory := handle.SharedInformerFactory()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	sharedLister := handle.SnapshotSharedLister()

	return &VolumeRestrictions{
		pvcLister:    pvcLister,
		sharedLister: sharedLister,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}, nil
}
