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

package nodevolumelimits

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/resource"
	ephemeral "k8s.io/component-helpers/storage/ephemeral"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// ErrReasonMaxVolumeCountExceeded 是超过节点最大卷数量时的过滤原因。
	ErrReasonMaxVolumeCountExceeded = "node(s) exceed max volume count"
)

// InTreeToCSITranslator 提供检查 in-tree 插件是否可迁移、以及将 in-tree PV 转换为 CSI PV 的能力。
type InTreeToCSITranslator interface {
	IsPVMigratable(pv *v1.PersistentVolume) bool
	IsInlineMigratable(vol *v1.Volume) bool
	IsMigratableIntreePluginByName(inTreePluginName string) bool
	GetInTreePluginNameFromSpec(pv *v1.PersistentVolume, vol *v1.Volume) (string, error)
	GetCSINameFromInTreeName(pluginName string) (string, error)
	TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
	TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error)
}

// CSILimits 检查节点的 CSI 卷数量是否达到上限。
type CSILimits struct {
	csiManager      fwk.CSIManager
	pvLister        corelisters.PersistentVolumeLister
	pvcLister       corelisters.PersistentVolumeClaimLister
	scLister        storagelisters.StorageClassLister
	vaLister        storagelisters.VolumeAttachmentLister
	csiDriverLister storagelisters.CSIDriverLister
	vaIndexer       cache.Indexer

	randomVolumeIDPrefix                               string
	enableVolumeLimitScaling                           bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	translator                                         InTreeToCSITranslator
}

var _ fwk.PreFilterPlugin = &CSILimits{}
var _ fwk.FilterPlugin = &CSILimits{}
var _ fwk.EnqueueExtensions = &CSILimits{}
var _ fwk.SignPlugin = &CSILimits{}

// CSIName 是该插件在插件注册表和配置文件中使用的名称。
const CSIName = names.NodeVolumeLimits

// Name 返回插件名称，用于日志等场景。
func (pl *CSILimits) Name() string {
	return CSIName
}

// EventsToRegister 返回可能让此前被本插件判定不可调度的 Pod 变为可调度的事件。
func (pl *CSILimits) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 故意不为 CSINode Add 注册 QueueingHintFn：
		// 任何新增的 CSINode 都可能提供新的卷配额，让此前失败的 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Add}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSINodeUpdated},
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPVCAdded},
		{Event: fwk.ClusterEvent{Resource: fwk.VolumeAttachment, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterVolumeAttachmentDeleted},
	}, nil
}

// isSchedulableAfterAssignedPodDeleted 判断已分配 Pod 的删除是否释放了卷配额，
// 从而让待调度 Pod 变得可调度。
func (pl *CSILimits) isSchedulableAfterAssignedPodDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedPod, _, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterAssignedPodDeleted: %w", err)
	}

	if len(deletedPod.Spec.Volumes) == 0 {
		return fwk.QueueSkip, nil
	}

	if deletedPod.Spec.NodeName == "" && deletedPod.Status.NominatedNodeName == "" {
		return fwk.QueueSkip, nil
	}

	for _, vol := range deletedPod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil || vol.Ephemeral != nil || pl.translator.IsInlineMigratable(&vol) {
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("The deleted pod does not impact the scheduling of the unscheduled pod", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterPVCAdded 判断新增的 PVC 是否是待调度 Pod 所引用的，
// 只有引用的 PVC 创建后才可能让该 Pod 可调度。
func (pl *CSILimits) isSchedulableAfterPVCAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedPvc, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPVCAdded: %w", err)
	}

	if addedPvc.Namespace != pod.Namespace {
		return fwk.QueueSkip, nil
	}

	for _, volumes := range pod.Spec.Volumes {
		var pvcName string
		switch {
		case volumes.PersistentVolumeClaim != nil:
			pvcName = volumes.PersistentVolumeClaim.ClaimName
		case volumes.Ephemeral != nil:
			pvcName = ephemeral.VolumeClaimName(pod, &volumes)
		default:
			// 该卷不使用 PVC，与本次新增的 PVC 无关。
			continue
		}

		if pvcName == addedPvc.Name {
			logger.V(5).Info("PVC that is referred from the pod was created, which might make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(addedPvc))
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("PVC irrelevant to the Pod was created, which doesn't make this pod schedulable", "pod", klog.KObj(pod), "PVC", klog.KObj(addedPvc))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterVolumeAttachmentDeleted 判断 VolumeAttachment 删除是否释放了
// Pod 所使用卷的挂载配额，从而让 Pod 可调度。
func (pl *CSILimits) isSchedulableAfterVolumeAttachmentDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedVolumeAttachment, _, err := util.As[*storagev1.VolumeAttachment](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterVolumeAttachmentDeleted: %w", err)
	}

	for _, vol := range pod.Spec.Volumes {
		// 使用 PVC 的卷会经过 VolumeAttachment，其删除可能释放配额。
		if vol.PersistentVolumeClaim != nil {
			logger.V(5).Info("Pod volume uses PersistentVolumeClaim, which might make this pod schedulable due to VolumeAttachment deletion", "pod", klog.KObj(pod), "volumeAttachment", klog.KObj(deletedVolumeAttachment), "volume", vol.Name)
			return fwk.Queue, nil
		}

		if !pl.translator.IsInlineMigratable(&vol) {
			continue
		}

		translatedPV, err := pl.translator.TranslateInTreeInlineVolumeToCSI(logger, &vol, pod.Namespace)
		if err != nil || translatedPV == nil {
			return fwk.Queue, fmt.Errorf("converting volume(%s) from inline to csi: %w", vol.Name, err)
		}

		if translatedPV.Spec.CSI != nil && deletedVolumeAttachment.Spec.Attacher == translatedPV.Spec.CSI.Driver {
			// 被删除 VolumeAttachment 的 Attacher 与转换后 PV 的 CSI driver 一致
			logger.V(5).Info("Pod volume is an Inline Migratable volume that matches the CSI driver, which might make this pod schedulable due to VolumeAttachment deletion",
				"pod", klog.KObj(pod), "volumeAttachment", klog.KObj(deletedVolumeAttachment),
				"volume", vol.Name, "csiDriver", translatedPV.Spec.CSI.Driver,
			)
			return fwk.Queue, nil
		}
	}

	logger.V(5).Info("the VolumeAttachment deletion wouldn't make this pod schedulable because the pod has no volume related to a deleted VolumeAttachment",
		"pod", klog.KObj(pod), "volumeAttachment", klog.KObj(deletedVolumeAttachment))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCSINodeUpdated 判断 CSINode 更新是否提高了驱动的可挂载卷上限，
// 只有配额增加才值得重新入队。
func (pl *CSILimits) isSchedulableAfterCSINodeUpdated(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldCSINode, newCSINode, err := util.As[*storagev1.CSINode](oldObj, newObj)
	if err != nil {
		return fwk.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterCSINodeUpdated: %w", err)
	}

	oldLimits := make(map[string]int32)
	for _, d := range oldCSINode.Spec.Drivers {
		var count int32
		if d.Allocatable != nil && d.Allocatable.Count != nil {
			count = *d.Allocatable.Count
		}
		oldLimits[d.Name] = count
	}

	// 对比新旧配额：只有上限提高才可能让此前因配额不足被拒的 Pod 可调度。
	for _, d := range newCSINode.Spec.Drivers {
		var oldLimit int32
		if val, exists := oldLimits[d.Name]; exists {
			oldLimit = val
		}
		newLimit := int32(0)
		if d.Allocatable != nil && d.Allocatable.Count != nil {
			newLimit = *d.Allocatable.Count
		}

		if newLimit > oldLimit {
			logger.V(5).Info("CSINode driver limit increased, might make this pod schedulable",
				"pod", klog.KObj(pod),
				"driver", d.Name,
				"oldLimit", oldLimit,
				"newLimit", newLimit,
			)
			return fwk.Queue, nil
		}
	}

	// 没有驱动配额提高，入队无济于事。
	return fwk.QueueSkip, nil
}

// PreFilter 在 PreFilter 扩展点调用。
// 若 Pod 不包含需要计数的卷类型（PVC、临时卷或可迁移的内联卷），则跳过 Filter 阶段。
func (pl *CSILimits) PreFilter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	volumes := pod.Spec.Volumes
	for i := range volumes {
		vol := &volumes[i]
		if vol.PersistentVolumeClaim != nil || vol.Ephemeral != nil || pl.translator.IsInlineMigratable(vol) {
			return nil, nil
		}
	}

	return nil, fwk.NewStatus(fwk.Skip)
}

// PreFilterExtensions 返回预过滤扩展；本插件不需要 Pod 增删钩子。
func (pl *CSILimits) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 在 Filter 扩展点调用，检查节点的 CSI 卷数量是否超限。
func (pl *CSILimits) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	// Pod 没有任何卷，自然不会超出卷数量限制。
	if len(pod.Spec.Volumes) == 0 {
		return nil
	}

	node := nodeInfo.Node()

	logger := klog.FromContext(ctx)

	csiNode, err := pl.csiManager.CSINodes().Get(node.Name)
	if err != nil {
		// TODO: CSINode 默认创建后（再经过两个发布周期）返回错误。
		// 目前 CSINode 可能尚不存在，先容忍并继续，以免误判节点不可调度。
		logger.V(5).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	// 统计新 Pod 需要使用的 CSI 卷。
	newVolumes := make(map[string]string)
	if err := pl.filterAttachableVolumes(logger, pod, csiNode, true /* 新 Pod */, newVolumes); err != nil {
		if apierrors.IsNotFound(err) {
			// PVC 不存在，在它被创建之前该 Pod 永远不可调度，无需再排队。
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
		}
		return fwk.AsStatus(err)
	}

	// 新 Pod 不使用任何 CSI 卷，不受卷数量限制约束。
	if len(newVolumes) == 0 {
		return nil
	}

	if pl.enableVolumeLimitScaling {
		for _, driverName := range newVolumes {
			driverInstalled, err := pl.checkCSIDriverOnNode(driverName, csiNode)
			if err != nil {
				return fwk.AsStatus(err)
			}
			if !driverInstalled {
				driverNotInstalledMsg := fmt.Sprintf("%s CSI driver is not installed on the node", driverName)
				return fwk.NewStatus(fwk.Unschedulable, driverNotInstalledMsg)
			}
		}
	}

	// 节点未声明卷上限，不做数量检查。
	nodeVolumeLimits := getVolumeLimits(csiNode)
	if len(nodeVolumeLimits) == 0 {
		return nil
	}

	// 统计节点上现有 Pod 使用的 CSI 卷。
	attachedVolumes := make(map[string]string)
	for _, existingPod := range nodeInfo.GetPods() {
		if err := pl.filterAttachableVolumes(logger, existingPod.GetPod(), csiNode, false /* 现有 Pod */, attachedVolumes); err != nil {
			return fwk.AsStatus(err)
		}
	}

	attachedVolumeCount := map[string]int{}
	for volumeUniqueName, driverName := range attachedVolumes {
		// 同一卷可能被多个 Pod 引用，只计一次；从新卷集合中移除，避免重复计数。
		delete(newVolumes, volumeUniqueName)
		attachedVolumeCount[driverName]++
	}

	// 再统计 VolumeAttachment 中记录但尚未出现在 Pod 里的卷。
	volumeAttachments, err := pl.getNodeVolumeAttachmentInfo(logger, node.Name)
	if err != nil {
		return fwk.AsStatus(err)
	}

	for volumeUniqueName, driverName := range volumeAttachments {
		// 已在现有 Pod 中统计过的卷不要重复计数。
		if _, exists := attachedVolumes[volumeUniqueName]; !exists {
			attachedVolumeCount[driverName]++
		}
	}

	// 按驱动统计新 Pod 引入的卷数量。
	newVolumeCount := map[string]int{}
	for _, driverName := range newVolumes {
		newVolumeCount[driverName]++
	}

	for driverName, count := range newVolumeCount {
		maxVolumeLimit, ok := nodeVolumeLimits[driverName]
		if ok {
			currentVolumeCount := attachedVolumeCount[driverName]
			logger.V(5).Info("Found plugin volume limits", "node", node.Name, "driverName", driverName,
				"maxLimits", maxVolumeLimit, "currentVolumeCount", currentVolumeCount, "newVolumeCount", count,
				"pod", klog.KObj(pod))
			if currentVolumeCount+count > int(maxVolumeLimit) {
				return fwk.NewStatus(fwk.Unschedulable, ErrReasonMaxVolumeCountExceeded)
			}
		}
	}

	return nil
}

// checkCSIDriverOnNode 检查 CSI 驱动是否已安装到节点上。
// 只有注册为 CSI 的驱动才受此限制约束；找不到对应 CSIDriver 时假定它不是 CSI 驱动，放行调度。
func (pl *CSILimits) checkCSIDriverOnNode(pluginName string, csiNode *storagev1.CSINode) (bool, error) {
	csiDriver, err := pl.csiDriverLister.Get(pluginName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("error getting CSIDriver for provider %s: %w", pluginName, err)
	}

	driverOptin := csiDriver.Spec.PreventPodSchedulingIfMissing

	if driverOptin == nil || !*driverOptin {
		return true, nil
	}

	if csiNode == nil {
		return false, nil
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == pluginName {
			return true, nil
		}
	}
	return false, nil
}

// filterAttachableVolumes 找出 Pod 中可挂载的卷并加入 result。
// result 的键是格式为 "driverName/volumeHandle" 的卷唯一名，值是对应的 CSI 驱动名。
func (pl *CSILimits) filterAttachableVolumes(
	logger klog.Logger, pod *v1.Pod, csiNode *storagev1.CSINode, newPod bool, result map[string]string) error {
	for _, vol := range pod.Spec.Volumes {
		pvcName := ""
		isEphemeral := false
		switch {
		case vol.PersistentVolumeClaim != nil:
			// 普通 CSI 卷只能通过 PVC 使用。
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			// 通用临时内联卷也使用 PVC，只是名称由 Pod 计算得出且归属特定 Pod，
			// 取到 PVC 对象后再校验归属关系。
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
			isEphemeral = true
		default:
			// 内联卷没有 PVC，需检查其 CSI 迁移是否开启：
			// - 可迁移且迁移已开启时，同样计入配额；
			// - 不可迁移的卷由 non_csi 过滤器负责统计。
			if err := pl.checkAttachableInlineVolume(logger, &vol, csiNode, pod, result); err != nil {
				return err
			}

			continue
		}

		if pvcName == "" {
			return fmt.Errorf("PersistentVolumeClaim had no name")
		}

		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)

		if err != nil {
			if newPod {
				// 新 Pod 必须依赖该 PVC 才能运行，查不到就无法继续调度。
				return fmt.Errorf("looking up PVC %s/%s: %w", pod.Namespace, pvcName, err)
			}
			// 现有 Pod 的 PVC 无效时不计数，无法确认它是否真的占用了节点配额。
			logger.V(5).Info("Unable to look up PVC info", "pod", klog.KObj(pod), "PVC", klog.KRef(pod.Namespace, pvcName))
			continue
		}

		// 临时卷的 PVC 必须归属于当前 Pod，防止误计其他 Pod 的卷。
		if isEphemeral {
			if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
				return err
			}
		}

		driverName, volumeHandle := pl.getCSIDriverInfo(logger, csiNode, pvc)
		if driverName == "" || volumeHandle == "" {
			logger.V(5).Info("Could not find a CSI driver name or volume handle, not counting volume")
			continue
		}

		volumeUniqueName := getVolumeUniqueName(driverName, volumeHandle)
		result[volumeUniqueName] = driverName
	}
	return nil
}

// checkAttachableInlineVolume 在内联卷可迁移且对应插件的 CSI 迁移已开启时，
// 将其转换后加入 result 供配额统计。
func (pl *CSILimits) checkAttachableInlineVolume(logger klog.Logger, vol *v1.Volume, csiNode *storagev1.CSINode,
	pod *v1.Pod, result map[string]string) error {
	if !pl.translator.IsInlineMigratable(vol) {
		return nil
	}
	// 先拿到 in-tree provisioner 名，再检查它的 CSI 迁移是否已开启。
	inTreeProvisionerName, err := pl.translator.GetInTreePluginNameFromSpec(nil, vol)
	if err != nil {
		return fmt.Errorf("looking up provisioner name for volume %s: %w", vol.Name, err)
	}
	if !isCSIMigrationOn(csiNode, inTreeProvisionerName) {
		csiNodeName := ""
		if csiNode != nil {
			csiNodeName = csiNode.Name
		}
		logger.V(5).Info("CSI Migration is not enabled for provisioner", "provisioner", inTreeProvisionerName,
			"pod", klog.KObj(pod), "csiNode", csiNodeName)
		return nil
	}
	// 迁移已开启，转换为 CSI 卷以取得驱动名和卷句柄。
	translatedPV, err := pl.translator.TranslateInTreeInlineVolumeToCSI(logger, vol, pod.Namespace)
	if err != nil || translatedPV == nil {
		return fmt.Errorf("converting volume(%s) from inline to csi: %w", vol.Name, err)
	}
	driverName, err := pl.translator.GetCSINameFromInTreeName(inTreeProvisionerName)
	if err != nil {
		return fmt.Errorf("looking up CSI driver name for provisioner %s: %w", inTreeProvisionerName, err)
	}
	// 转换结果没有 CSI 源，说明该卷不支持内联，跳过计数。
	if translatedPV.Spec.PersistentVolumeSource.CSI == nil {
		return nil
	}
	volumeUniqueName := getVolumeUniqueName(driverName, translatedPV.Spec.PersistentVolumeSource.CSI.VolumeHandle)
	result[volumeUniqueName] = driverName
	return nil
}

// getCSIDriverInfo 返回 PVC 对应的 CSI 驱动名和卷 ID。
// 若 PVC 来自已迁移的 in-tree 插件，则返回其迁移到的 CSI 驱动信息。
func (pl *CSILimits) getCSIDriverInfo(logger klog.Logger, csiNode *storagev1.CSINode, pvc *v1.PersistentVolumeClaim) (string, string) {
	pvName := pvc.Spec.VolumeName

	if pvName == "" {
		logger.V(5).Info("Persistent volume had no name for claim", "PVC", klog.KObj(pvc))
		return pl.getCSIDriverInfoFromSC(logger, csiNode, pvc)
	}

	pv, err := pl.pvLister.Get(pvName)
	if err != nil {
		logger.V(5).Info("Unable to look up PV info for PVC and PV", "PVC", klog.KObj(pvc), "PV", klog.KRef("", pvName))
		// PV 可能已被删除，或 PVC 预绑定到尚未创建的 PV；
		// 此时退而使用 StorageClass 推算驱动信息用于计数。
		return pl.getCSIDriverInfoFromSC(logger, csiNode, pvc)
	}

	csiSource := pv.Spec.PersistentVolumeSource.CSI
	if csiSource == nil {
		// 不可迁移的非 CSI 卷直接跳过，无需后续转换。
		if !pl.translator.IsPVMigratable(pv) {
			return "", ""
		}

		pluginName, err := pl.translator.GetInTreePluginNameFromSpec(pv, nil)
		if err != nil {
			logger.V(5).Info("Unable to look up plugin name from PV spec", "err", err)
			return "", ""
		}

		if !isCSIMigrationOn(csiNode, pluginName) {
			logger.V(5).Info("CSI Migration of plugin is not enabled", "plugin", pluginName)
			return "", ""
		}

		csiPV, err := pl.translator.TranslateInTreePVToCSI(logger, pv)
		if err != nil {
			logger.V(5).Info("Unable to translate in-tree volume to CSI", "err", err)
			return "", ""
		}

		if csiPV.Spec.PersistentVolumeSource.CSI == nil {
			logger.V(5).Info("Unable to get a valid volume source for translated PV", "PV", pvName)
			return "", ""
		}

		csiSource = csiPV.Spec.PersistentVolumeSource.CSI
	}

	return csiSource.Driver, csiSource.VolumeHandle
}

// getCSIDriverInfoFromSC 根据 PVC 的 StorageClass 推算 CSI 驱动名，
// 并生成一个随机卷 ID 供去重计数使用。
func (pl *CSILimits) getCSIDriverInfoFromSC(logger klog.Logger, csiNode *storagev1.CSINode, pvc *v1.PersistentVolumeClaim) (string, string) {
	namespace := pvc.Namespace
	pvcName := pvc.Name
	scName := storagehelpers.GetPersistentVolumeClaimClass(pvc)

	// 没有 StorageClass 意味着 PVC 使用立即绑定，调度前必然已绑定 PV，
	// 前面走 PV 路径即可，这里不计数是安全的。
	if scName == "" {
		logger.V(5).Info("PVC has no StorageClass", "PVC", klog.KObj(pvc))
		return "", ""
	}

	storageClass, err := pl.scLister.Get(scName)
	if err != nil {
		logger.V(5).Info("Could not get StorageClass for PVC", "PVC", klog.KObj(pvc), "err", err)
		return "", ""
	}

	// 随机前缀避免与真实卷 ID 冲突：若 PVC 在过滤过程中完成绑定，
	// 且同节点另一个 Pod 也用它，随机 ID 可防止把两个路径统计成同一个卷而漏计。
	volumeHandle := fmt.Sprintf("%s-%s/%s", pl.randomVolumeIDPrefix, namespace, pvcName)

	provisioner := storageClass.Provisioner
	if pl.translator.IsMigratableIntreePluginByName(provisioner) {
		if !isCSIMigrationOn(csiNode, provisioner) {
			logger.V(5).Info("CSI Migration of provisioner is not enabled", "provisioner", provisioner)
			return "", ""
		}

		driverName, err := pl.translator.GetCSINameFromInTreeName(provisioner)
		if err != nil {
			logger.V(5).Info("Unable to look up driver name from provisioner name", "provisioner", provisioner, "err", err)
			return "", ""
		}
		return driverName, volumeHandle
	}

	return provisioner, volumeHandle
}

// volumeAttachmentIndexer 按 VolumeAttachment 所属节点名建立索引键。
func volumeAttachmentIndexer(obj interface{}) ([]string, error) {
	va, ok := obj.(*storagev1.VolumeAttachment)
	if !ok {
		return []string{}, nil
	}
	return []string{va.Spec.NodeName}, nil
}

// NewCSI 初始化并返回一个 CSILimits 插件实例。
func NewCSI(_ context.Context, _ runtime.Object, handle fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	informerFactory := handle.SharedInformerFactory()
	pvLister := informerFactory.Core().V1().PersistentVolumes().Lister()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	scLister := informerFactory.Storage().V1().StorageClasses().Lister()
	vaLister := informerFactory.Storage().V1().VolumeAttachments().Lister()
	csiDriverLister := informerFactory.Storage().V1().CSIDrivers().Lister()
	vaInformer := informerFactory.Storage().V1().VolumeAttachments().Informer()
	if err := vaInformer.AddIndexers(cache.Indexers{vaIndexKey: volumeAttachmentIndexer}); err != nil {
		if vaInformer.GetIndexer().GetIndexers()[vaIndexKey] == nil {
			return nil, fmt.Errorf("failed to add index to VA informer: %w", err)
		}
	}
	csiTranslator := csitrans.New()

	// 长字段前后的空行是为了让各架构下 gofmt 输出保持一致，见 issue #141074。
	return &CSILimits{
		csiManager:               handle.SharedCSIManager(),
		pvLister:                 pvLister,
		pvcLister:                pvcLister,
		scLister:                 scLister,
		vaLister:                 vaLister,
		csiDriverLister:          csiDriverLister,
		enableVolumeLimitScaling: fts.EnableVolumeLimitScaling,

		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,

		randomVolumeIDPrefix: rand.String(32),
		translator:           csiTranslator,
		vaIndexer:            vaInformer.GetIndexer(),
	}, nil
}

// getVolumeLimits 从 CSINode 读取卷上限，返回以驱动名为键、最大可挂载卷数为值的映射。
// 映射中查不到的驱动表示节点对其没有数量限制。
func getVolumeLimits(csiNode *storagev1.CSINode) map[string]int64 {
	nodeVolumeLimits := make(map[string]int64)
	if csiNode == nil {
		return nodeVolumeLimits
	}
	for _, d := range csiNode.Spec.Drivers {
		if d.Allocatable != nil && d.Allocatable.Count != nil {
			nodeVolumeLimits[d.Name] = int64(*d.Allocatable.Count)
		}
	}
	return nodeVolumeLimits
}

// vaIndexKey 是卷附件 informer 按节点名建立索引时使用的键名。
const vaIndexKey = "va.spec.nodename"

// getNodeVolumeAttachmentInfo 返回指定节点上卷 ID 到驱动名的映射。
func (pl *CSILimits) getNodeVolumeAttachmentInfo(logger klog.Logger, nodeName string) (map[string]string, error) {
	volumeAttachments := make(map[string]string)
	vas, err := pl.vaIndexer.ByIndex(vaIndexKey, nodeName)
	if err != nil {
		return nil, err
	}
	for _, vao := range vas {
		va, ok := vao.(*storagev1.VolumeAttachment)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, fmt.Errorf("unexpected object type in volume attachment indexer: %v", vao),
				"volume indexer not available")
			continue
		}
		if va.Spec.NodeName == nodeName {
			if va.Spec.Attacher == "" {
				logger.V(5).Info("VolumeAttachment has no attacher", "VolumeAttachment", klog.KObj(va))
				continue
			}
			if va.Spec.Source.PersistentVolumeName == nil {
				logger.V(5).Info("VolumeAttachment has no PV name", "VolumeAttachment", klog.KObj(va))
				continue
			}
			pv, err := pl.pvLister.Get(*va.Spec.Source.PersistentVolumeName)
			if err != nil {
				logger.V(5).Info("Unable to get PV for VolumeAttachment", "VolumeAttachment", klog.KObj(va), "err", err)
				continue
			}
			if pv.Spec.CSI == nil {
				logger.V(5).Info("PV is not a CSI volume", "PV", klog.KObj(pv))
				continue
			}
			volumeID := getVolumeUniqueName(va.Spec.Attacher, pv.Spec.CSI.VolumeHandle)
			volumeAttachments[volumeID] = va.Spec.Attacher
		}
	}
	return volumeAttachments, nil
}

// getVolumeUniqueName 用驱动名和卷句柄拼出卷的唯一标识。
func getVolumeUniqueName(driverName, volumeHandle string) string {
	return fmt.Sprintf("%s/%s", driverName, volumeHandle)
}

// SignPod 基于非合成卷来源生成签名片段，供过滤和打分使用。
func (pl *CSILimits) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)},
	}, nil
}
