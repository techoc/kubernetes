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

// Package volumezone 实现 VolumeZone 调度插件：检查 Pod 所请求的持久卷的拓扑标签（zone/region）
// 是否与候选节点的拓扑标签匹配，避免把 Pod 调度到无法访问其卷的区域。
package volumezone

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	volumehelpers "k8s.io/cloud-provider/volume/helpers"
	"k8s.io/component-helpers/resource"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// VolumeZone 是检查卷区域的插件。
// 它作为 Filter 插件拒绝卷拓扑与节点拓扑不匹配的节点，并在 PreFilter 阶段预先收集
// Pod 所绑定 PV 的拓扑信息供后续阶段复用；同时通过事件注册在相关对象变化时把失败 Pod 重新入队。
type VolumeZone struct {
	pvLister                                           corelisters.PersistentVolumeLister
	pvcLister                                          corelisters.PersistentVolumeClaimLister
	scLister                                           storagelisters.StorageClassLister
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.FilterPlugin = &VolumeZone{}
var _ fwk.PreFilterPlugin = &VolumeZone{}
var _ fwk.EnqueueExtensions = &VolumeZone{}
var _ fwk.SignPlugin = &VolumeZone{}

const (
	// Name 是插件在注册表和配置中使用的名称。
	Name = names.VolumeZone

	preFilterStateKey fwk.StateKey = "PreFilter" + Name

	// ErrReasonConflict 是 NoVolumeZoneConflict 判定失败时使用的状态原因。
	ErrReasonConflict = "node(s) had no available volume zone"
)

// pvTopology 保存一个 PV 的拓扑标签信息：标签键、PV 名称以及该标签允许的取值集合。
type pvTopology struct {
	pvName string
	key    string
	values sets.Set[string]
}

// 该状态在 PreFilter 阶段初始化；由于存入 fwk.CycleState 的是指针，
// 后续阶段无需再次调用 Write 即可看到（共享）最新值。
type stateData struct {
	// podPVTopologies 保存调度所需的 PV 拓扑信息，在 PreFilter 阶段初始化。
	podPVTopologies []pvTopology
}

// Clone 返回状态本身。插件只读取该状态且从不修改其内容，
// 因此直接返回原指针即可，无需深拷贝。
func (d *stateData) Clone() fwk.StateData {
	return d
}

var topologyLabels = []string{
	v1.LabelFailureDomainBetaZone,
	v1.LabelFailureDomainBetaRegion,
	v1.LabelTopologyZone,
	v1.LabelTopologyRegion,
}

// translateToGALabel 把 beta 版本的拓扑标签键转换为稳定（GA）版本的键。
// 兼容旧 PV 使用 beta 标签、而节点仅标注了 GA 标签的场景。
func translateToGALabel(label string) string {
	if label == v1.LabelFailureDomainBetaRegion {
		return v1.LabelTopologyRegion
	}
	if label == v1.LabelFailureDomainBetaZone {
		return v1.LabelTopologyZone
	}
	return label
}

// Name 返回插件名称，用于日志等场景。
func (pl *VolumeZone) Name() string {
	return Name
}

// SignPod 根据非合成（真实）的卷来源为 Pod 生成签名片段，
// 供调度框架判断两个 Pod 的卷约束是否等价。
func (pl *VolumeZone) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)},
	}, nil
}

// PreFilter 在 prefilter 扩展点被调用。
//
// # 它查找 Pod 所请求卷对应 PersistentVolume 的拓扑信息
//
// 目前仅支持 PersistentVolumeClaim，并且只关注已绑定的 PersistentVolume。
// 若 Pod 不需要调整大小或没有相关 PV 拓扑，则返回 Skip 以避免后续无意义计算。
func (pl *VolumeZone) PreFilter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// 扩容被延迟的 Pod 会由原位扩容抢占流程处理，本插件无需参与。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	logger := klog.FromContext(ctx)
	podPVTopologies, status := pl.getPVbyPod(logger, pod)
	if !status.IsSuccess() {
		return nil, status
	}
	// 没有拓扑约束时跳过本插件，避免污染 CycleState。
	if len(podPVTopologies) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cs.Write(preFilterStateKey, &stateData{podPVTopologies: podPVTopologies})
	return nil, nil
}

// getPVbyPod 从 Pod 声明的 PVC 出发，解析出每个已绑定 PV 的拓扑信息。
// PVC 尚未绑定且使用 WaitForFirstConsumer 时跳过该卷，交由卷绑定插件处理。
func (pl *VolumeZone) getPVbyPod(logger klog.Logger, pod *v1.Pod) ([]pvTopology, *fwk.Status) {
	podPVTopologies := make([]pvTopology, 0)

	pvcNames := pl.getPersistentVolumeClaimNameFromPod(pod)
	for _, pvcName := range pvcNames {
		// 空 PVC 名称无法用于查询，直接判定为不可调度且不可通过等待解决。
		if pvcName == "" {
			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "PersistentVolumeClaim had no name")
		}
		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
		if s := getErrorAsStatus(err); !s.IsSuccess() {
			return nil, s
		}

		pvName := pvc.Spec.VolumeName
		if pvName == "" {
			// PVC 未绑定 PV：尝试通过 StorageClass 判断是否为延迟绑定。
			scName := storagehelpers.GetPersistentVolumeClaimClass(pvc)
			if len(scName) == 0 {
				return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "PersistentVolumeClaim had no pv name and storageClass name")
			}

			class, err := pl.scLister.Get(scName)
			if s := getErrorAsStatus(err); !s.IsSuccess() {
				return nil, s
			}
			if class.VolumeBindingMode == nil {
				return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("VolumeBindingMode not set for StorageClass %q", scName))
			}
			if *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer {
				// 延迟绑定的卷在调度时还没有 PV，区域信息尚不存在，跳过。
				continue
			}

			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "PersistentVolume had no name")
		}

		pv, err := pl.pvLister.Get(pvName)
		if s := getErrorAsStatus(err); !s.IsSuccess() {
			return nil, s
		}
		podPVTopologies = append(podPVTopologies, pl.getPVTopologies(logger, pv)...)
	}
	return podPVTopologies, nil
}

// PreFilterExtensions 返回 prefilter 扩展（Pod 增删回调）。
// 本插件无需增量维护状态，返回 nil。
func (pl *VolumeZone) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 在 filter 扩展点被调用。
//
// 它根据 Pod 请求的卷判断节点是否可用：某些卷带有区域调度约束，
// 要求卷的区域标签必须与节点上对应的标签匹配；节点拥有更多区域标签约束是允许的
// （例如假设某个复制卷允许跨整个 region 访问）。
//
// 目前仅支持 PersistentVolumeClaim，并且只检查已绑定 PersistentVolume 上的标签。
//
// 处理 Pod 规范中内联声明的卷（即不使用 PersistentVolume）会更困难，
// 因为需要在调度时确定卷所在区域，而这很可能要调用云厂商接口。
// 无论如何，社区整体上正在远离内联卷声明。
func (pl *VolumeZone) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// 扩容被延迟的 Pod 由原位扩容抢占流程处理，此处放行。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	logger := klog.FromContext(ctx)
	// 没有挂载卷的 Pod 必然满足该断言；走快速路径避免不必要的计算。
	if len(pod.Spec.Volumes) == 0 {
		return nil
	}
	var podPVTopologies []pvTopology
	state, err := getStateData(cs)
	if err != nil {
		// PreFilter 状态缺失时在此重新计算 PV 列表，保证 Filter 独立可用。
		var status *fwk.Status
		podPVTopologies, status = pl.getPVbyPod(logger, pod)
		if !status.IsSuccess() {
			return status
		}
	} else {
		podPVTopologies = state.podPVTopologies
	}

	node := nodeInfo.Node()
	hasAnyNodeConstraint := false
	for _, topologyLabel := range topologyLabels {
		if _, ok := node.Labels[topologyLabel]; ok {
			hasAnyNodeConstraint = true
			break
		}
	}

	// 节点没有任何区域约束即可调度；这覆盖了单区域集群中节点未打拓扑标签的情况。
	if !hasAnyNodeConstraint {
		return nil
	}

	for _, pvTopology := range podPVTopologies {
		v, ok := node.Labels[pvTopology.key]
		if !ok {
			// PV 使用 beta 标签时，节点可能只标注了 GA 版本，做一次键名转换再匹配。
			v, ok = node.Labels[translateToGALabel(pvTopology.key)]
		}
		if !ok || !pvTopology.values.Has(v) {
			logger.V(10).Info("Won't schedule pod onto node due to volume (mismatch on label key)", "pod", klog.KObj(pod), "node", klog.KObj(node), "PV", klog.KRef("", pvTopology.pvName), "PVLabelKey", pvTopology.key)
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict)
		}
	}

	return nil
}

// getStateData 从 CycleState 读取本插件在 PreFilter 阶段写入的状态。
// 类型不匹配时返回错误，调用方据此回退为重新计算。
func getStateData(cs fwk.CycleState) (*stateData, error) {
	state, err := cs.Read(preFilterStateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// getErrorAsStatus 把查询 lister 的错误转换为框架状态。
// NotFound 意味着对象尚不存在，属于不可通过等待解决的不可调度；其余错误按内部错误处理。
func getErrorAsStatus(err error) *fwk.Status {
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
		}
		return fwk.AsStatus(err)
	}
	return nil
}

// EventsToRegister 返回可能让此前被本插件判定为不可调度的 Pod 变为可调度的事件。
func (pl *VolumeZone) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 新增采用 VolumeBindingWaitForFirstConsumer 绑定模式的 StorageClass 可能让 Pod 可调度。
		// storageClass.volumeBindingMode 是不可变字段，因此忽略 StorageClass 的更新事件。
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterStorageClassAdded},
		// 新增节点或修改节点的卷区域标签都可能让 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}},
		// 新增 PVC 可能让 Pod 可调度；PVC 的 VolumeName 被填充（完成绑定）同样如此。
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
		// 新增 PV 或修改 PV 的卷区域标签都可能让 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeChange},
	}, nil
}

// getPersistentVolumeClaimNameFromPod 收集 Pod 规范中所有 PVC 卷引用的名称。
func (pl *VolumeZone) getPersistentVolumeClaimNameFromPod(pod *v1.Pod) []string {
	var pvcNames []string
	for i := range pod.Spec.Volumes {
		volume := pod.Spec.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvcNames = append(pvcNames, pvcName)
	}
	return pvcNames
}

// isSchedulableAfterPersistentVolumeClaimChange 在 PVC 新增或更新时调用，
// 检查该变化是否让此前不可调度的 Pod 变为可调度。
func (pl *VolumeZone) isSchedulableAfterPersistentVolumeClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, modifiedPVC, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: %w", err)
	}
	if pl.isPVCRequestedFromPod(logger, modifiedPVC, pod) {
		logger.V(5).Info("PVC that is referred from the pod was created or updated, which might make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(modifiedPVC))
		return fwk.Queue, nil
	}

	logger.V(5).Info("PVC irrelevant to the Pod was created or updated, which doesn't make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(modifiedPVC))
	return fwk.QueueSkip, nil
}

// isPVCRequestedFromPod 判断给定 PVC 是否被指定 Pod 引用。
// 命名空间必须一致，且 PVC 名称出现在 Pod 的卷列表中。
func (pl *VolumeZone) isPVCRequestedFromPod(logger klog.Logger, pvc *v1.PersistentVolumeClaim, pod *v1.Pod) bool {
	if (pvc == nil) || (pod.Namespace != pvc.Namespace) {
		return false
	}
	pvcNames := pl.getPersistentVolumeClaimNameFromPod(pod)
	for _, pvcName := range pvcNames {
		if pvc.Name == pvcName {
			logger.V(5).Info("PVC is referred from the pod", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc))
			return true
		}
	}
	logger.V(5).Info("PVC is not referred from the pod", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc))
	return false
}

// isSchedulableAfterStorageClassAdded 在 StorageClass 新增时调用，
// 检查其是否让此前不可调度的 Pod 变为可调度。
// 只有绑定模式为 WaitForFirstConsumer 的新 StorageClass 才可能改变调度结果，其余直接跳过。
func (pl *VolumeZone) isSchedulableAfterStorageClassAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedStorageClass, err := util.As[*storage.StorageClass](nil, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterStorageClassAdded: %w", err)
	}
	if (addedStorageClass.VolumeBindingMode == nil) || (*addedStorageClass.VolumeBindingMode != storage.VolumeBindingWaitForFirstConsumer) {
		logger.V(5).Info("StorageClass is created, but its VolumeBindingMode is not waitForFirstConsumer, which doesn't make the pod schedulable", "storageClass", klog.KObj(addedStorageClass), "pod", klog.KObj(pod))
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("StorageClass with waitForFirstConsumer mode was created and it might make this pod schedulable", "pod", klog.KObj(pod), "StorageClass", klog.KObj(addedStorageClass))
	return fwk.Queue, nil
}

// isSchedulableAfterPersistentVolumeChange 在 PV 新增或更新时调用，
// 检查其是否让此前不可调度的 Pod 变为可调度。
// 只有 PV 拓扑标签发生变化才值得重新入队，其余更新直接跳过。
func (pl *VolumeZone) isSchedulableAfterPersistentVolumeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalPV, modifiedPV, err := util.As[*v1.PersistentVolume](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPersistentVolumeChange: %w", err)
	}
	// 新增 PV 可能正是 Pod 等待的卷，无需细看内容即可重新入队试探。
	if originalPV == nil {
		logger.V(5).Info("PV is newly created, which might make the pod schedulable")
		return fwk.Queue, nil
	}
	originalPVTopologies := pl.getPVTopologies(logger, originalPV)
	modifiedPVTopologies := pl.getPVTopologies(logger, modifiedPV)
	if !reflect.DeepEqual(originalPVTopologies, modifiedPVTopologies) {
		logger.V(5).Info("PV's topology was updated, which might make the pod schedulable.", "pod", klog.KObj(pod), "PV", klog.KObj(modifiedPV))
		return fwk.Queue, nil
	}

	logger.V(5).Info("PV was updated, but the topology is unchanged, which it doesn't make the pod schedulable", "pod", klog.KObj(pod), "PV", klog.KObj(modifiedPV))
	return fwk.QueueSkip, nil
}

// getPVTopologies 从给定 PV 提取所有拓扑标签并返回对应的 pvTopology 列表。
// 此处不检查 spec.nodeAffinity：该字段创建后不可变，不会在此处被更新，
// 且节点亲和性由 node affinity 插件负责处理。
func (pl *VolumeZone) getPVTopologies(logger klog.Logger, pv *v1.PersistentVolume) []pvTopology {
	podPVTopologies := make([]pvTopology, 0)
	for _, key := range topologyLabels {
		if value, ok := pv.ObjectMeta.Labels[key]; ok {
			labelZonesSet, err := volumehelpers.LabelZonesToSet(value)
			if err != nil {
				// 非法标签值只忽略该标签，避免因单个脏数据阻断整个 Pod 的调度。
				logger.V(5).Info("failed to parse PV's topology label, ignoring the label", "label", fmt.Sprintf("%s:%s", key, value), "err", err)
				continue
			}
			podPVTopologies = append(podPVTopologies, pvTopology{
				pvName: pv.Name,
				key:    key,
				values: sets.Set[string](labelZonesSet),
			})
		}
	}
	return podPVTopologies
}

// New 从调度器 handle 获取 PV、PVC 和 StorageClass 的 lister，初始化并返回一个 VolumeZone 插件实例。
func New(_ context.Context, _ runtime.Object, handle fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	informerFactory := handle.SharedInformerFactory()
	pvLister := informerFactory.Core().V1().PersistentVolumes().Lister()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	scLister := informerFactory.Storage().V1().StorageClasses().Lister()
	return &VolumeZone{
		pvLister:  pvLister,
		pvcLister: pvcLister,
		scLister:  scLister,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}, nil
}
