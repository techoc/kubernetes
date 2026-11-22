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

package volumebinding

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// stateKey 是卷绑定插件在 fwk.CycleState 中存储状态的键，直接使用插件名。
	stateKey fwk.StateKey = Name

	// maxUtilization 是存储容量利用率的百分比上限，用于评分函数的归一化。
	maxUtilization = 100
)

// stateData 保存卷绑定插件在单个 Pod 调度周期内的共享状态。
// 状态在 PreFilter 阶段初始化，并以指针形式写入 fwk.CycleState；后续阶段直接修改
// 指针指向的内容即可，无需再次调用 Write 覆盖状态值。
type stateData struct {
	// allBound 表示 Pod 的所有卷是否已绑定，用于 PreBind 阶段跳过不必要的绑定操作。
	allBound bool
	// podVolumesByNode 保存 Filter 阶段为每个节点找到的 Pod 卷信息。
	// 在 PreFilter 阶段初始化，键为节点名。
	podVolumesByNode map[string]*PodVolumes
	// podVolumeClaims 保存 Pod 的 PVC 分类结果（已绑定、延迟绑定等）。
	podVolumeClaims *PodVolumeClaims
	// hasStaticBindings 表示 Pod 是否包含静态绑定的卷。
	// 若没有，卷绑定插件会跳过 Score 扩展点，避免无意义的评分计算。
	hasStaticBindings bool
	sync.Mutex
}

// Clone 返回状态自身的指针。
// 这里不进行深拷贝：状态在 PreFilter 中初始化后，后续阶段需要共享同一份数据（如 Filter
// 阶段并发写入 podVolumesByNode），因此各扩展点必须看到同一个对象。
func (d *stateData) Clone() fwk.StateData {
	return d
}

// VolumeBinding 是在调度过程中处理 Pod 卷绑定的插件。
// Filter 阶段为每个 Pod 创建卷绑定缓存，Reserve 和 PreBind 阶段复用该缓存：
// Reserve 先假定绑定（assume），PreBind 再真正执行绑定并等待 PV 控制器完成。
type VolumeBinding struct {
	Binder      SchedulerVolumeBinder
	PVCLister   corelisters.PersistentVolumeClaimLister
	classLister storagelisters.StorageClassLister
	scorer      volumeCapacityScorer
	fts         feature.Features
}

var _ fwk.PreFilterPlugin = &VolumeBinding{}
var _ fwk.FilterPlugin = &VolumeBinding{}
var _ fwk.ReservePlugin = &VolumeBinding{}
var _ fwk.PreBindPlugin = &VolumeBinding{}
var _ fwk.PreScorePlugin = &VolumeBinding{}
var _ fwk.ScorePlugin = &VolumeBinding{}
var _ fwk.EnqueueExtensions = &VolumeBinding{}
var _ fwk.SignPlugin = &VolumeBinding{}

// Name 是插件在注册表和配置中使用的名称。
const Name = names.VolumeBinding

// Name 返回插件名称，用于日志等场景。
func (pl *VolumeBinding) Name() string {
	return Name
}

// SignPod 为 Pod 生成基于非合成卷来源（non-synthetic volume sources）的签名片段，
// 覆盖可行性与评分相关的卷状态。
func (pl *VolumeBinding) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)},
	}, nil
}

// EventsToRegister 返回可能让此前被本插件判定为不可调度的 Pod 变为可调度的集群事件。
func (pl *VolumeBinding) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	events := []fwk.ClusterEventWithHint{
		// Pod 可能因 StorageClass 缺失或配置错误（如 allowedTopologies、volumeBindingMode）
		// 而调度失败，StorageClass 的添加或更新事件可能使其恢复可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterStorageClassChange},

		// 插件负责将 PVC 与 PV 绑定，因此 PVC/PV 的任何变化都可能让 Pod 变为可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add | fwk.Update}},

		// Pod 可能因节点标签不匹配 StorageClass 的 allowedTopologies 或 PV 的节点亲和性
		// 而找不到可用 PV；新增节点或节点标签更新可能使其恢复可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}},

		// 依赖 CSI 节点信息将 in-tree PV 转换为 CSI 卷。
		// TODO: 待所有卷插件完成 CSI 迁移后，kube-schduler 将注销 CSINode 事件。
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSINodeChange},

		// 启用 CSIStorageCapacity 时，CSI 驱动或存储容量的变化可能让 Pod 变为可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.CSIDriver, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIDriverChange},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIStorageCapacityChange},
	}
	return events, nil
}

// isSchedulableAfterCSINodeChange 判断 CSINode 事件是否可能让 Pod 变为可调度。
// 只有当已迁移卷插件注解（MigratedPluginsAnnotationKey）发生变化时才需要重新入队，
// 这关系到 in-tree 卷向 CSI 的转换。
func (pl *VolumeBinding) isSchedulableAfterCSINodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if oldObj == nil {
		logger.V(5).Info("CSINode creation could make the pod schedulable")
		return fwk.Queue, nil
	}
	oldCSINode, modifiedCSINode, err := util.As[*storagev1.CSINode](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSINode", klog.KObj(modifiedCSINode),
	)

	if oldCSINode.ObjectMeta.Annotations[v1.MigratedPluginsAnnotationKey] != modifiedCSINode.ObjectMeta.Annotations[v1.MigratedPluginsAnnotationKey] {
		logger.V(5).Info("CSINode's migrated plugins annotation is updated and that may make the pod schedulable")
		return fwk.Queue, nil
	}

	logger.V(5).Info("CISNode was created or updated but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterPersistentVolumeClaimChange 判断 PVC 事件是否可能让 Pod 变为可调度。
// 仅当 PVC 与 Pod 同命名空间且是 Pod 所引用的 PVC（含通用临时卷）时才需要重新入队。
func (pl *VolumeBinding) isSchedulableAfterPersistentVolumeClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, newPVC, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"PersistentVolumeClaim", klog.KObj(newPVC),
	)

	if pod.Namespace != newPVC.Namespace {
		logger.V(5).Info("PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable because the PVC belongs to a different namespace")
		return fwk.QueueSkip, nil
	}

	for _, vol := range pod.Spec.Volumes {
		var pvcName string
		switch {
		case vol.PersistentVolumeClaim != nil:
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
		default:
			continue
		}

		if pvcName == newPVC.Name {
			// 无论新建还是几乎任何更新，只要涉及 Pod 所引用的 PVC，都可能改变其可调度性，
			// 因此一律重新入队，由后续 Filter 重新评估。
			logger.V(5).Info("PersistentVolumeClaim the pod requires was created or updated, potentially making the target Pod schedulable")
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterStorageClassChange 判断 StorageClass 事件是否可能让 Pod 变为可调度。
// 新建 StorageClass 以及 allowedTopologies 的更新都可能使 Pod 恢复可调度。
// 注意 volumeBindingMode 不允许更新，因此检查更新事件时无需考虑该字段。
func (pl *VolumeBinding) isSchedulableAfterStorageClassChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldSC, newSC, err := util.As[*storagev1.StorageClass](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"StorageClass", klog.KObj(newSC),
	)

	if oldSC == nil {
		// 创建事件没有旧对象可供比较，无法进一步过滤，直接重新入队。
		logger.V(5).Info("A new StorageClass was created, which could make a Pod schedulable")
		return fwk.Queue, nil
	}

	if !apiequality.Semantic.DeepEqual(newSC.AllowedTopologies, oldSC.AllowedTopologies) {
		logger.V(5).Info("StorageClass got an update in AllowedTopologies", "AllowedTopologies", newSC.AllowedTopologies)
		return fwk.Queue, nil
	}

	logger.V(5).Info("StorageClass was updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCSIStorageCapacityChange 判断 CSIStorageCapacity 事件是否可能让 Pod 变为可调度。
// 新建 CSIStorageCapacity，以及卷上限（由 capacity 和 maximumVolumeSize 计算得出）的增加
// 都可能让 Pod 恢复可调度。
// 注意 nodeTopology 和 storageClassName 不允许更新，因此检查更新事件时无需考虑这两个字段。
func (pl *VolumeBinding) isSchedulableAfterCSIStorageCapacityChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldCap, newCap, err := util.As[*storagev1.CSIStorageCapacity](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if oldCap == nil {
		logger.V(5).Info(
			"A new CSIStorageCapacity was created, which could make a Pod schedulable",
			"Pod", klog.KObj(pod),
			"CSIStorageCapacity", klog.KObj(newCap),
		)
		return fwk.Queue, nil
	}

	oldLimit := volumeLimit(oldCap)
	newLimit := volumeLimit(newCap)

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSIStorageCapacity", klog.KObj(newCap),
		"volumeLimit(new)", newLimit,
		"volumeLimit(old)", oldLimit,
	)

	if newLimit != nil && (oldLimit == nil || newLimit.Value() > oldLimit.Value()) {
		logger.V(5).Info("VolumeLimit was increased, which could make a Pod schedulable")
		return fwk.Queue, nil
	}

	logger.V(5).Info("CSIStorageCapacity was updated, but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCSIDriverChange 判断 CSIDriver 事件是否可能让 Pod 变为可调度。
// 当 Pod 使用某 CSI 驱动且其 storageCapacity 特性被关闭时，此前因容量跟踪而失败的
// Pod 可能恢复可调度，需要重新入队。
func (pl *VolumeBinding) isSchedulableAfterCSIDriverChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalCSIDriver, modifiedCSIDriver, err := util.As[*storagev1.CSIDriver](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	logger = klog.LoggerWithValues(
		logger,
		"Pod", klog.KObj(pod),
		"CSIDriver", klog.KObj(modifiedCSIDriver),
	)

	for _, vol := range pod.Spec.Volumes {
		if vol.CSI == nil || vol.CSI.Driver != modifiedCSIDriver.Name {
			continue
		}
		if (originalCSIDriver.Spec.StorageCapacity != nil && *originalCSIDriver.Spec.StorageCapacity) &&
			(modifiedCSIDriver.Spec.StorageCapacity == nil || !*modifiedCSIDriver.Spec.StorageCapacity) {
			logger.V(5).Info("CSIDriver was updated and storage capacity got disabled, which may make the pod schedulable")
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("CSIDriver was created or updated but it doesn't make this pod schedulable")
	return fwk.QueueSkip, nil
}

// podHasPVCs 返回两个值：
//   - 第一个表示给定 Pod 是否定义了 PVC（含通用临时卷）；
//   - 第二个表示所引用的 PVC 是否非法（如丢失、正在删除、临时卷归属不符）。
func (pl *VolumeBinding) podHasPVCs(pod *v1.Pod) (bool, error) {
	hasPVC := false
	for _, vol := range pod.Spec.Volumes {
		var pvcName string
		isEphemeral := false
		switch {
		case vol.PersistentVolumeClaim != nil:
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
			isEphemeral = true
		default:
			// 卷未使用 PVC，跳过。
			continue
		}
		hasPVC = true
		pvc, err := pl.PVCLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
		if err != nil {
			// 普通错误信息（如 "persistentvolumeclaim "myclaim" not found"）通常已足够，
			// 但通用临时内联卷在 Pod 刚创建时 PVC 尚未生成属正常现象，需给出更贴切的提示。
			if isEphemeral && apierrors.IsNotFound(err) {
				err = fmt.Errorf("waiting for ephemeral volume controller to create the persistentvolumeclaim %q", pvcName)
			}
			return hasPVC, err
		}

		if pvc.Status.Phase == v1.ClaimLost {
			return hasPVC, fmt.Errorf("persistentvolumeclaim %q bound to non-existent persistentvolume %q", pvc.Name, pvc.Spec.VolumeName)
		}

		if pvc.DeletionTimestamp != nil {
			return hasPVC, fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}

		if isEphemeral {
			if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
				return hasPVC, err
			}
		}
	}
	return hasPVC, nil
}

// PreFilter 在 prefilter 扩展点调用，检查 Pod 的所有立即绑定（immediate）PVC 是否都已绑定。
// 若有未绑定的立即 PVC，返回 UnschedulableAndUnresolvable，等待 PV 控制器完成绑定后再重试。
func (pl *VolumeBinding) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.fts.EnableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	logger := klog.FromContext(ctx)
	// Pod 不引用任何 PVC 时无需后续卷处理，写入空状态并跳过整个插件。
	if hasPVC, err := pl.podHasPVCs(pod); err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
	} else if !hasPVC {
		state.Write(stateKey, &stateData{})
		return nil, fwk.NewStatus(fwk.Skip)
	}
	podVolumeClaims, err := pl.Binder.GetPodVolumeClaims(logger, pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(podVolumeClaims.unboundClaimsImmediate) > 0 {
		// 立即绑定的 PVC 必须先由 PV 控制器完成绑定，当前节点上的调度对此无能为力。
		// 返回 UnschedulableAndUnresolvable 可让 Pod 在这些 PVC 绑定后经事件触发
		// 重新进入 active/backoff 队列。
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		status.AppendReason("pod has unbound immediate PersistentVolumeClaims")
		return nil, status
	}
	state.Write(stateKey, &stateData{
		podVolumesByNode: make(map[string]*PodVolumes),
		podVolumeClaims: &PodVolumeClaims{
			boundClaims:                podVolumeClaims.boundClaims,
			unboundClaimsDelayBinding:  podVolumeClaims.unboundClaimsDelayBinding,
			unboundVolumesDelayBinding: podVolumeClaims.unboundVolumesDelayBinding,
		},
	})
	return nil, nil
}

// PreFilterExtensions 返回 prefilter 的 pod 添加/移除扩展，本插件无需实现，返回 nil。
func (pl *VolumeBinding) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// getStateData 从 CycleState 中读取并校验 stateData。
// 状态由 PreFilter 写入，若类型断言失败说明插件扩展点被错误地串行调用，需显式报错。
func getStateData(cs fwk.CycleState) (*stateData, error) {
	state, err := cs.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// Filter 在 filter 扩展点调用，评估 Pod 请求的卷是否使当前节点可行，同时检查已绑定和未绑定的 PVC。
//
// 对于已绑定的 PVC，检查对应 PV 的节点亲和性是否满足该节点。
//
// 对于未绑定的 PVC，尝试寻找满足 PVC 要求且 PV 节点亲和性与该节点兼容的可用 PV。
//
// 若启用了存储容量跟踪，还需保证该节点上有足够空间供待创建的卷使用。
//
// 仅当所有已绑定 PVC 的 PV 都与节点兼容，且所有未绑定 PVC 都能匹配到可用且节点兼容的 PV 时，
// 该判定才返回可行（nil Status）。
func (pl *VolumeBinding) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.fts.EnableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}

	podVolumes, reasons, err := pl.Binder.FindPodVolumes(logger, pod, state.podVolumeClaims, node)

	if err != nil {
		return fwk.AsStatus(err)
	}

	if len(reasons) > 0 {
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		for _, reason := range reasons {
			status.AppendReason(string(reason))
		}
		return status
	}

	// Filter 会针对不同节点并发调用，且 CycleState 可能被复制出多个副本共享同一状态指针，
	// 因此必须用 stateData 自带的锁保护写入。
	state.Lock()
	state.podVolumesByNode[node.Name] = podVolumes
	state.hasStaticBindings = state.hasStaticBindings || (podVolumes != nil && len(podVolumes.StaticBindings) > 0)
	state.Unlock()
	return nil
}

// PreScore 在 prescore 扩展点调用，判断卷绑定插件是否可以跳过 Score。
// 既没有静态绑定卷、也未启用存储容量评分时，评分无意义，直接跳过。
func (pl *VolumeBinding) PreScore(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	if pl.scorer == nil {
		return fwk.NewStatus(fwk.Skip)
	}
	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if state.hasStaticBindings || pl.fts.EnableStorageCapacityScoring {
		return nil
	}
	return fwk.NewStatus(fwk.Skip)
}

// Score 在 score 扩展点调用，根据节点的存储资源利用率计算得分。
// 静态绑定卷与动态创建的卷按 StorageClass 聚合后交给评分函数处理。
func (pl *VolumeBinding) Score(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if pl.scorer == nil {
		return 0, nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}
	nodeName := nodeInfo.Node().Name
	podVolumes, ok := state.podVolumesByNode[nodeName]
	if !ok {
		return 0, nil
	}

	classResources := make(classResourceMap)
	if len(podVolumes.StaticBindings) != 0 || !pl.fts.EnableStorageCapacityScoring {
		// 按 StorageClass 聚合静态绑定卷的资源请求与容量。
		for _, staticBinding := range podVolumes.StaticBindings {
			class := staticBinding.StorageClassName()
			storageResource := staticBinding.StorageResource()
			if _, ok := classResources[class]; !ok {
				classResources[class] = &StorageResource{
					Requested: 0,
					Capacity:  0,
				}
			}
			classResources[class].Requested += storageResource.Requested
			classResources[class].Capacity += storageResource.Capacity
		}
	} else {
		// 按 StorageClass 聚合动态创建卷的资源请求与节点容量。
		for _, provision := range podVolumes.DynamicProvisions {
			if provision.NodeCapacity == nil {
				continue
			}
			class := *provision.PVC.Spec.StorageClassName
			if _, ok := classResources[class]; !ok {
				classResources[class] = &StorageResource{
					Requested: 0,
					Capacity:  0,
				}
			}
			// 此处不能用 +=：同一 StorageClass 在节点上的容量是共享的，而非按卷累加。
			// 例如一个 Pod 从容量为 100GB 的 StorageClass 请求两个 50GB 卷时，
			// 该分支会执行两次，用 += 会错误地把容量算成 200GB。
			classResources[class].Capacity = provision.NodeCapacity.Capacity.Value()
			requestedQty := provision.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
			classResources[class].Requested += requestedQty.Value()
		}
	}

	return pl.scorer(classResources), nil
}

// ScoreExtensions 返回 Score 插件的扩展，本插件无需实现，返回 nil。
func (pl *VolumeBinding) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// Reserve 在 reserve 扩展点为 Pod 预留卷，并将绑定状态保存到 CycleState。
// 核心是 AssumePodVolumes：在 assume cache 中记录绑定意图，让同一周期内的后续调度
// 不会把同一卷分配给其他 Pod；真正的 API 绑定留到 PreBind 阶段执行。
func (pl *VolumeBinding) Reserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	state, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	// 一个 Pod 只会预留一个节点，因此这里无需加锁。
	podVolumes, ok := state.podVolumesByNode[nodeName]
	if ok {
		allBound, err := pl.Binder.AssumePodVolumes(klog.FromContext(ctx), pod, nodeName, podVolumes)
		if err != nil {
			return fwk.AsStatus(err)
		}
		state.allBound = allBound
	} else {
		// Pod 不引用任何 PVC 时，podVolumesByNode 中不存在对应条目，此时视为全部已绑定。
		state.allBound = true
	}
	return nil
}

// errNoPodVolumeForNode 表示 CycleState 中缺少指定节点的卷信息。
// 正常流程中 Reserve 已写入该信息；出现此错误通常意味着扩展点调用顺序异常，
// 用包装错误附上节点名便于定位。
var errNoPodVolumeForNode = fmt.Errorf("no pod volume found for node")

// PreBindPreFlight 在 PreBind 之前调用，判断 PreBind 是否需要对当前 Pod 执行操作。
// 它检查 state.podVolumesByNode 中是否存在该节点的卷信息，以决定 PreBind 阶段是否需要处理。
func (pl *VolumeBinding) PreBindPreFlight(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	result := &fwk.PreBindPreFlightResult{AllowParallel: true}
	s, err := getStateData(state)
	if err != nil {
		return result, fwk.AsStatus(err)
	}
	if s.allBound {
		// 所有卷都已绑定，PreBind 阶段无需再执行任何操作。
		return result, fwk.NewStatus(fwk.Skip)
	}

	if _, ok := s.podVolumesByNode[nodeName]; !ok {
		return result, fwk.AsStatus(fmt.Errorf("%w %q", errNoPodVolumeForNode, nodeName))
	}
	return result, nil
}

// PreBind 根据 Reserve 阶段的假定绑定结果向 API server 发起真正的绑定更新，
// 并等待 PV 控制器完成整个绑定操作。
//
// 若绑定出错、超时或被撤销，则返回错误以触发重新调度；Unreserve 会清理 assume cache。
func (pl *VolumeBinding) PreBind(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	s, err := getStateData(cs)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s.allBound {
		// 所有卷都已绑定，无需再次发起绑定。
		return nil
	}
	// 一个 Pod 只会预绑定一个节点，因此这里无需加锁。
	podVolumes, ok := s.podVolumesByNode[nodeName]
	if !ok {
		return fwk.AsStatus(fmt.Errorf("%w %q", errNoPodVolumeForNode, nodeName))
	}
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Trying to bind volumes for pod", "pod", klog.KObj(pod))
	err = pl.Binder.BindPodVolumes(ctx, pod, podVolumes)
	if err != nil {
		logger.V(5).Info("Failed to bind volumes for pod", "pod", klog.KObj(pod), "err", err)
		return fwk.AsStatus(err)
	}
	logger.V(5).Info("Success binding volumes for pod", "pod", klog.KObj(pod))
	return nil
}

// Unreserve 清除 assume cache 中已假定的 PV 和 PVC 状态。
// 该操作是幂等的：若给定 Pod/节点没有对应的假定卷，则直接返回。
// Reserve 之后调度失败或 PreBind 出错时都会调用它，避免残留的假定绑定影响后续调度周期。
func (pl *VolumeBinding) Unreserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) {
	s, err := getStateData(cs)
	if err != nil {
		return
	}
	// 一个 Pod 最多只会撤销一个节点的预留，因此这里无需加锁。
	podVolumes, ok := s.podVolumesByNode[nodeName]
	if !ok {
		return
	}
	pl.Binder.RevertAssumedPodVolumes(podVolumes)
}

// New 根据配置参数初始化并返回一个卷绑定插件实例。
// 它会构建 VolumeBinder（绑定与 assume cache 的核心实现），并在启用存储容量评分时
// 根据 Shape 配置构造评分函数。
func New(ctx context.Context, plArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, ok := plArgs.(*config.VolumeBindingArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type VolumeBindingArgs, got %T", plArgs)
	}
	if err := validation.ValidateVolumeBindingArgsWithOptions(nil, args, validation.VolumeBindingArgsValidationOptions{
		AllowStorageCapacityScoring: fts.EnableStorageCapacityScoring,
	}); err != nil {
		return nil, err
	}
	podInformer := fh.SharedInformerFactory().Core().V1().Pods()
	nodeInformer := fh.SharedInformerFactory().Core().V1().Nodes()
	pvcInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumeClaims()
	pvInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumes()
	storageClassInformer := fh.SharedInformerFactory().Storage().V1().StorageClasses()
	csiNodeInformer := fh.SharedInformerFactory().Storage().V1().CSINodes()
	capacityCheck := CapacityCheck{
		CSIDriverInformer:          fh.SharedInformerFactory().Storage().V1().CSIDrivers(),
		CSIStorageCapacityInformer: fh.SharedInformerFactory().Storage().V1().CSIStorageCapacities(),
	}
	binder, err := NewVolumeBinder(klog.FromContext(ctx), fh.ClientSet(), fts, podInformer, nodeInformer, csiNodeInformer, pvcInformer, pvInformer, storageClassInformer, capacityCheck, time.Duration(args.BindTimeoutSeconds)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to build volume binder: %w", err)
	}

	// 仅在启用存储容量评分时构建评分函数，否则 scorer 保持 nil，PreScore 会跳过 Score。
	var scorer volumeCapacityScorer
	if fts.EnableStorageCapacityScoring {
		shape := make(helper.FunctionShape, 0, len(args.Shape))
		for _, point := range args.Shape {
			shape = append(shape, helper.FunctionShapePoint{
				Utilization: int64(point.Utilization),
				Score:       int64(point.Score) * (fwk.MaxNodeScore / config.MaxCustomPriorityScore),
			})
		}
		scorer = buildScorerFunction(shape)
	}
	return &VolumeBinding{
		Binder:      binder,
		PVCLister:   pvcInformer.Lister(),
		classLister: storageClassInformer.Lister(),
		scorer:      scorer,
		fts:         fts,
	}, nil
}
