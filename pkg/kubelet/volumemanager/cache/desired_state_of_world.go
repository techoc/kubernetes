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

/*
Package cache 保存 kubelet 卷管理器使用的数据结构，用于跟踪已挂载的卷以及挂载它们的 Pod。
*/
package cache

import (
	"fmt"
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume/csi"

	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// DesiredStateOfWorld 定义 kubelet 卷管理器期望状态缓存的线程安全操作。
// 缓存内容为“卷 -> Pod”：本节点应挂载的所有卷及其引用该卷、需要执行挂载的 Pod。
// 注意：它与 attach/detach 控制器的 DesiredStateOfWorld 不同，二者跟踪的对象并不相同，
// 这里只保存 kubelet 卷管理器的状态。
type DesiredStateOfWorld interface {
	// AddPodToVolume 把指定 Pod 加入卷的挂载集合，表示该 Pod 应挂载该卷。
	// 成功时返回由 volumeSpec 生成的唯一卷名；没有插件或多个插件都能支持该 spec 时返回错误。
	// 若卷不在节点应挂载的卷列表中，会隐式创建；若同名 Pod 已在该卷下，则为空操作。
	AddPodToVolume(logger klog.Logger, podName types.UniquePodName, pod *v1.Pod, volumeSpec *volume.Spec, outerVolumeSpecName string, volumeGIDValue string, seLinuxContainerContexts []*v1.SELinuxOptions) (v1.UniqueVolumeName, error)

	// MarkVolumesReportedInUse 把 reportedVolumes 中卷的 ReportedInUse 置为 true，
	// 其余卷重置为 false（新建卷默认 false）。
	// 该值为 true 表示卷已成功写入节点状态的 VolumesInUse 字段；挂载操作发起前必须检查它。
	// 列表中不存在于期望状态的卷会被跳过，不报错。
	MarkVolumesReportedInUse(reportedVolumes []v1.UniqueVolumeName)

	// DeletePodFromVolume 从卷的挂载集合中移除 Pod，表示该 Pod 不再需要该卷。
	// Pod 或卷不存在时为空操作；删除后卷若无其他 Pod 引用，则一并删除。
	DeletePodFromVolume(podName types.UniquePodName, volumeName v1.UniqueVolumeName)

	// VolumeExists 判断卷是否在节点应挂载的卷列表中。
	// 开启 SELinuxMountReadWriteOncePod 时，SELinux 挂载上下文不同的同名卷被视为不同卷。
	VolumeExists(volumeName v1.UniqueVolumeName, seLinuxMountContext string) bool

	// PodExistsInVolume 判断 Pod 是否在指定卷的 podsToMount 中。
	// Pod 或卷不存在，或 SELinux 挂载选项不匹配时返回 false。
	PodExistsInVolume(podName types.UniquePodName, volumeName v1.UniqueVolumeName, seLinuxMountContext string) bool

	// GetVolumeNamesForPod 返回该 Pod 使用的卷名映射，键为外层 volumeSpec 名。
	GetVolumeNamesForPod(podName types.UniquePodName) map[string]v1.UniqueVolumeName

	// GetVolumesToMount 依据当前期望状态生成应挂载到本节点的卷及其 Pod 列表。
	GetVolumesToMount() []VolumeToMount

	// GetPods 返回期望状态中的所有 Pod，键为 Pod 唯一名；
	// 调用方可据此判断某 Pod 是否仍在期望状态中。
	GetPods() map[types.UniquePodName]bool

	// VolumeExistsWithSpecName 判断指定 Pod 下是否存在给定 volumeSpec 名（内层卷名）的卷。
	VolumeExistsWithSpecName(podName types.UniquePodName, volumeSpecName string) bool

	// AddErrorToPod 为 Pod 记录一条错误，后续由 PopPodErrors 取回；相同错误只保存一次。
	AddErrorToPod(podName types.UniquePodName, err string)

	// PopPodErrors 返回并清除该 Pod 累积的错误。
	PopPodErrors(podName types.UniquePodName) []string

	// GetPodsWithErrors 返回当前记录了错误的 Pod 名称列表。
	GetPodsWithErrors() []types.UniquePodName

	// MarkVolumeAttachability 更新指定卷是否可附加（attachable）。
	MarkVolumeAttachability(volumeName v1.UniqueVolumeName, attachable bool)

	// UpdatePersistentVolumeSize 更新期望的 PV 大小，用于与实际大小比较，必要时触发扩容。
	UpdatePersistentVolumeSize(volumeName v1.UniqueVolumeName, size resource.Quantity)
}

// VolumeToMount 表示已附加到本节点、需要挂载到 PodName 的卷。
type VolumeToMount struct {
	operationexecutor.VolumeToMount
}

// NewDesiredStateOfWorld 创建并返回 DesiredStateOfWorld 实例。
func NewDesiredStateOfWorld(volumePluginMgr *volume.VolumePluginMgr, seLinuxTranslator util.SELinuxLabelTranslator) DesiredStateOfWorld {
	if feature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		registerSELinuxMetrics()
	}
	return &desiredStateOfWorld{
		volumesToMount:    make(map[v1.UniqueVolumeName]volumeToMount),
		volumePluginMgr:   volumePluginMgr,
		podErrors:         make(map[types.UniquePodName]sets.Set[string]),
		seLinuxTranslator: seLinuxTranslator,
	}
}

// desiredStateOfWorld 是 DesiredStateOfWorld 的线程安全实现。
type desiredStateOfWorld struct {
	// volumesToMount 保存应附加到本节点并挂载到引用它的 Pod 的卷集合。
	// 键为卷名，值为包含卷详细信息的对象。
	volumesToMount map[v1.UniqueVolumeName]volumeToMount
	// volumePluginMgr 用于创建卷插件对象。
	volumePluginMgr *volume.VolumePluginMgr
	// podErrors 保存 desiredStateOfWorldPopulator 在处理 Pod 卷时发现的错误。
	podErrors map[types.UniquePodName]sets.Set[string]
	// seLinuxTranslator 把 v1.SELinuxOptions 翻译为文件 SELinux 标签。
	seLinuxTranslator util.SELinuxLabelTranslator

	sync.RWMutex
}

// volumeToMount 表示应附加到本节点并挂载到 podsToMount 中 Pod 的卷。
type volumeToMount struct {
	// volumeName 是该卷的唯一标识。
	volumeName v1.UniqueVolumeName

	// podsToMount 保存引用该卷、待卷附加后需要挂载的 Pod 集合。
	// 键为 Pod 名，值为 Pod 详细信息。
	podsToMount map[types.UniquePodName]podToMount

	// pluginIsAttachable 表示该卷插件实现了 volume.Attacher 接口。
	pluginIsAttachable bool

	// pluginIsDeviceMountable 表示该卷插件实现了 volume.DeviceMounter 接口。
	pluginIsDeviceMountable bool

	// volumeGIDValue 保存 GID 注解的值（若存在）。
	volumeGIDValue string

	// reportedInUse 表示卷已成功写入节点状态的 VolumesInUse 字段。
	reportedInUse bool

	// desiredSizeLimit 表示卷大小的期望上限（若该卷实现支持）。
	desiredSizeLimit *resource.Quantity

	// persistentVolumeSize 记录 PV 的期望大小，通常来自 pv.Spec.Capacity。
	persistentVolumeSize resource.Quantity

	// effectiveSELinuxMountFileLabel 是通过挂载选项应用到卷上的 SELinux 标签。
	// 为空表示以下之一：context+label 未知（由容器运行时随机分配）、插件不支持 -o context、
	// 卷不是 ReadWriteOncePod、或操作系统不支持 SELinux。
	// 这些情况下挂载卷时 SELinux 上下文无关紧要。
	effectiveSELinuxMountFileLabel string

	// originalSELinuxLabel 是在所有访问模式都支持 SELinux 挂载时会使用的标签。
	// 对 RWOP 卷它与 effectiveSELinuxMountFileLabel 相同。
	// 它仅用于上报潜在的 SELinux 不匹配指标；为空的原因与上同。
	originalSELinuxLabel string
}

// podToMount 表示引用底层卷、待卷附加后执行挂载的 Pod。
type podToMount struct {
	// podName 是该 Pod 的唯一名。
	podName types.UniquePodName

	// pod 是待挂载卷的 Pod，用于创建 NewMounter。
	pod *v1.Pod

	// volumeSpec 是该卷的规格，用于生成插件对象并传给插件方法。
	// 非 PVC 卷与 Pod 中定义一致；PVC 卷则来自解引用后的 PV 对象。
	volumeSpec *volume.Spec

	// outerVolumeSpecNames 是 podSpec.Volume[x].Name 中的外层卷名。
	outerVolumeSpecNames []string
	// mountRequestTime 记录发起挂载请求的时间。
	mountRequestTime time.Time
}

const (
	// 每个 Pod 最多保留的错误条数，防止 desiredStateOfWorld.podErrors 无界增长。
	maxPodErrors = 10
)

// AddPodToVolume 将 Pod 加入卷的挂载集合并返回卷名；卷不存在时按需创建。
func (dsw *desiredStateOfWorld) AddPodToVolume(
	logger klog.Logger,
	podName types.UniquePodName,
	pod *v1.Pod,
	volumeSpec *volume.Spec,
	outerVolumeSpecName string,
	volumeGIDValue string,
	seLinuxContainerContexts []*v1.SELinuxOptions) (v1.UniqueVolumeName, error) {
	dsw.Lock()
	defer dsw.Unlock()

	volumePlugin, err := dsw.volumePluginMgr.FindPluginBySpec(volumeSpec)
	if err != nil || volumePlugin == nil {
		return "", fmt.Errorf(
			"failed to get Plugin from volumeSpec for volume %q err=%v",
			volumeSpec.Name(),
			err)
	}
	volumePluginName := getVolumePluginNameWithDriver(logger, volumePlugin, volumeSpec)
	accessMode := getVolumeAccessMode(volumeSpec)

	var volumeName v1.UniqueVolumeName

	// 唯一卷名的生成方式取决于卷是否可附加/可设备挂载。
	attachable := util.IsAttachableVolume(volumeSpec, dsw.volumePluginMgr)
	deviceMountable := util.IsDeviceMountableVolume(volumeSpec, dsw.volumePluginMgr)
	if attachable || deviceMountable {
		// 可附加/可设备挂载的卷直接采用插件给出的唯一卷名。
		volumeName, err =
			util.GetUniqueVolumeNameFromSpec(volumePlugin, volumeSpec)
		if err != nil {
			return "", fmt.Errorf(
				"failed to GetUniqueVolumeNameFromSpec for volumeSpec %q using volume plugin %q err=%v",
				volumeSpec.Name(),
				volumePlugin.GetPluginName(),
				err)
		}
	} else {
		// 不可附加且不可设备挂载的卷，用 Pod 唯一名（podUID）与 Pod 内卷名组合生成唯一卷名。
		volumeName = util.GetUniqueVolumeNameFromSpecWithPod(podName, volumePlugin, volumeSpec)
	}

	seLinuxFileLabel, pluginSupportsSELinuxContextMount, err := dsw.getSELinuxLabel(logger, volumeSpec, seLinuxContainerContexts, pod.Spec.SecurityContext)
	if err != nil {
		return "", err
	}
	logger.V(4).Info("expected volume SELinux label context", "volume", volumeSpec.Name(), "label", seLinuxFileLabel)

	if _, volumeExists := dsw.volumesToMount[volumeName]; !volumeExists {
		var sizeLimit *resource.Quantity
		if volumeSpec.Volume != nil {
			if util.IsLocalEphemeralVolume(*volumeSpec.Volume) {
				podLimits := resourcehelper.PodLimits(pod, resourcehelper.PodResourcesOptions{})
				ephemeralStorageLimit := podLimits[v1.ResourceEphemeralStorage]
				sizeLimit = resource.NewQuantity(ephemeralStorageLimit.Value(), resource.BinarySI)
				if volumeSpec.Volume.EmptyDir != nil &&
					volumeSpec.Volume.EmptyDir.SizeLimit != nil &&
					volumeSpec.Volume.EmptyDir.SizeLimit.Value() > 0 &&
					(sizeLimit.Value() == 0 || volumeSpec.Volume.EmptyDir.SizeLimit.Value() < sizeLimit.Value()) {
					sizeLimit = resource.NewQuantity(volumeSpec.Volume.EmptyDir.SizeLimit.Value(), resource.BinarySI)
				}
			}
		}
		effectiveSELinuxMountLabel := seLinuxFileLabel
		if !util.VolumeSupportsSELinuxMount(volumeSpec) {
			// 访问模式不支持 SELinux 挂载时清空标签，避免误用 context 挂载。
			logger.V(4).Info("volume does not support SELinux context mount, clearing the expected label", "volume", volumeSpec.Name())
			effectiveSELinuxMountLabel = ""
		}
		if seLinuxFileLabel != "" {
			seLinuxVolumesAdmitted.WithLabelValues(volumePluginName, accessMode).Add(1.0)
		}
		vmt := volumeToMount{
			volumeName:                     volumeName,
			podsToMount:                    make(map[types.UniquePodName]podToMount),
			pluginIsAttachable:             attachable,
			pluginIsDeviceMountable:        deviceMountable,
			volumeGIDValue:                 volumeGIDValue,
			reportedInUse:                  false,
			desiredSizeLimit:               sizeLimit,
			effectiveSELinuxMountFileLabel: effectiveSELinuxMountLabel,
			originalSELinuxLabel:           seLinuxFileLabel,
		}
		// 记录卷的期望大小，卷扩容时用于比较。
		if volumeSpec.PersistentVolume != nil {
			pvCap := volumeSpec.PersistentVolume.Spec.Capacity.Storage()
			if pvCap != nil {
				pvCapCopy := pvCap.DeepCopy()
				vmt.persistentVolumeSize = pvCapCopy
			}
		}
		dsw.volumesToMount[volumeName] = vmt
	}

	// 已存在的 Pod 记录只刷新字段，避免对 Downward API 等卷产生无谓的重挂载。
	oldPodMount, ok := dsw.volumesToMount[volumeName].podsToMount[podName]
	mountRequestTime := time.Now()
	var outerVolumeSpecNames []string
	if ok {
		if !volumePlugin.RequiresRemount(volumeSpec) {
			mountRequestTime = oldPodMount.mountRequestTime
		}
		outerVolumeSpecNames = oldPodMount.outerVolumeSpecNames
	}
	// 同一 Pod 可能通过多个 volumeSpec 名引用同一卷，这里去重后保留全部外层卷名。
	if !slices.Contains(outerVolumeSpecNames, outerVolumeSpecName) {
		outerVolumeSpecNames = append(outerVolumeSpecNames, outerVolumeSpecName)
	}

	if !ok {
		// 卷已存在但尚未包含该 Pod：先检查 SELinux 标签冲突并上报指标，再在下方登记 podToMount。
		if pluginSupportsSELinuxContextMount {
			existingVolume := dsw.volumesToMount[volumeName]
			if seLinuxFileLabel != existingVolume.originalSELinuxLabel {
				fullErr := fmt.Errorf("conflicting SELinux labels of volume %s: %q and %q", volumeSpec.Name(), existingVolume.originalSELinuxLabel, seLinuxFileLabel)
				supported := util.VolumeSupportsSELinuxMount(volumeSpec)
				err := handleSELinuxMetricError(
					logger,
					fullErr,
					supported,
					seLinuxVolumeContextMismatchWarnings.WithLabelValues(volumePluginName, accessMode),
					seLinuxVolumeContextMismatchErrors.WithLabelValues(volumePluginName, accessMode))
				if err != nil {
					return "", err
				}
			}
		}
	}

	// 新建 podToMount；若已存在则用新值刷新，以支持 Pod 更新后需要重挂载的卷（如 Downward API）。
	dsw.volumesToMount[volumeName].podsToMount[podName] = podToMount{
		podName:              podName,
		pod:                  pod,
		volumeSpec:           volumeSpec,
		outerVolumeSpecNames: outerVolumeSpecNames,
		mountRequestTime:     mountRequestTime,
	}
	return volumeName, nil
}

// getSELinuxLabel 返回给定卷及 SELinux 标签组合对应的 SELinux 标签，
// 并报告插件是否支持以 SELinux 上下文方式挂载。
// 无法构造标签，或同一卷被多个 SELinux 标签使用时返回错误。
func (dsw *desiredStateOfWorld) getSELinuxLabel(logger klog.Logger, volumeSpec *volume.Spec, seLinuxContainerContexts []*v1.SELinuxOptions, podSecurityContext *v1.PodSecurityContext) (seLinuxFileLabel string, pluginSupportsSELinuxContextMount bool, err error) {
	labelInfo, err := util.GetMountSELinuxLabel(volumeSpec, seLinuxContainerContexts, podSecurityContext, dsw.volumePluginMgr, dsw.seLinuxTranslator)
	if err != nil {
		accessMode := getVolumeAccessMode(volumeSpec)
		seLinuxSupported := util.VolumeSupportsSELinuxMount(volumeSpec)

		if util.IsSELinuxLabelTranslationError(err) {
			err := handleSELinuxMetricError(
				logger,
				err,
				seLinuxSupported,
				seLinuxContainerContextWarnings.WithLabelValues(accessMode),
				seLinuxContainerContextErrors.WithLabelValues(accessMode))
			return "", labelInfo.PluginSupportsSELinuxContextMount, err
		}
		if util.IsMultipleSELinuxLabelsError(err) {
			err := handleSELinuxMetricError(
				logger,
				err,
				seLinuxSupported,
				seLinuxPodContextMismatchWarnings.WithLabelValues(accessMode),
				seLinuxPodContextMismatchErrors.WithLabelValues(accessMode))
			return "", false, err
		}
		return "", labelInfo.PluginSupportsSELinuxContextMount, err
	}

	return labelInfo.SELinuxMountLabel, labelInfo.PluginSupportsSELinuxContextMount, nil
}

// MarkVolumesReportedInUse 以本次上报的卷集合整体覆盖所有卷的 reportedInUse 标记。
func (dsw *desiredStateOfWorld) MarkVolumesReportedInUse(
	reportedVolumes []v1.UniqueVolumeName) {
	dsw.Lock()
	defer dsw.Unlock()

	// 以本次上报集合为准整体覆盖，未上报的卷视为尚未写入节点状态。
	reportedVolumesMap := make(
		map[v1.UniqueVolumeName]bool, len(reportedVolumes) /* capacity */)

	for _, reportedVolume := range reportedVolumes {
		reportedVolumesMap[reportedVolume] = true
	}

	for volumeName, volumeObj := range dsw.volumesToMount {
		_, volumeReported := reportedVolumesMap[volumeName]
		volumeObj.reportedInUse = volumeReported
		dsw.volumesToMount[volumeName] = volumeObj
	}
}

// DeletePodFromVolume 从卷中移除 Pod，并清理该 Pod 的错误记录；卷无 Pod 引用时删除卷。
func (dsw *desiredStateOfWorld) DeletePodFromVolume(
	podName types.UniquePodName, volumeName v1.UniqueVolumeName) {
	dsw.Lock()
	defer dsw.Unlock()

	// 一并丢弃该 Pod 的错误，避免 Pod 重建后误报旧错误。
	delete(dsw.podErrors, podName)

	volumeObj, volumeExists := dsw.volumesToMount[volumeName]
	if !volumeExists {
		return
	}

	if _, podExists := volumeObj.podsToMount[podName]; !podExists {
		return
	}

	// Pod 存在时才删除其挂载记录。
	delete(dsw.volumesToMount[volumeName].podsToMount, podName)

	if len(dsw.volumesToMount[volumeName].podsToMount) == 0 {
		// 没有任何子 Pod 时一并删除卷记录。
		delete(dsw.volumesToMount, volumeName)
	}
}

// UpdatePersistentVolumeSize 更新最近已知的 PV 大小，供卷扩容使用；仅适用于 PV。
func (dsw *desiredStateOfWorld) UpdatePersistentVolumeSize(volumeName v1.UniqueVolumeName, size resource.Quantity) {
	dsw.Lock()
	defer dsw.Unlock()

	vol, volExists := dsw.volumesToMount[volumeName]
	if volExists {
		vol.persistentVolumeSize = size
		dsw.volumesToMount[volumeName] = vol
	}
}

// VolumeExists 判断卷是否存在于期望状态；启用 SELinux 特性时还需上下文匹配。
func (dsw *desiredStateOfWorld) VolumeExists(
	volumeName v1.UniqueVolumeName, seLinuxMountContext string) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	vol, volumeExists := dsw.volumesToMount[volumeName]
	if !volumeExists {
		return false
	}
	if feature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		// 同名但 SELinux 上下文不同的卷按两个不同卷处理：
		// 用旧上下文挂载过的卷必须先卸载，再用新上下文重新挂载。
		// 典型场景：Pod A（alpha_t）正在被 kubelet 终止并拆除卷，
		// 而同一节点上使用相同卷的 Pod B（beta_t）已被调度。
		// 必须完整卸载 A 的卷（含 UnmountDevice），再以 B 的新 SELinux 挂载选项挂载。
		// 未启用 SELinux 时，kubelet 可以（且经常）复用为 A 设备挂载的卷。
		return vol.effectiveSELinuxMountFileLabel == seLinuxMountContext
	}
	return true
}

// PodExistsInVolume 判断 Pod 是否已登记在指定卷下，且 SELinux 挂载选项一致。
func (dsw *desiredStateOfWorld) PodExistsInVolume(
	podName types.UniquePodName, volumeName v1.UniqueVolumeName, seLinuxMountOption string) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	volumeObj, volumeExists := dsw.volumesToMount[volumeName]
	if !volumeExists {
		return false
	}

	if feature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		if volumeObj.effectiveSELinuxMountFileLabel != seLinuxMountOption {
			// 卷在期望状态中，但 SELinux 挂载选项不同。
			// 视为未使用，使其先卸载再以正确选项重新挂载。
			return false
		}
	}

	_, podExists := volumeObj.podsToMount[podName]
	return podExists
}

// VolumeExistsWithSpecName 遍历卷查找该 Pod 下指定内层卷名的卷。
func (dsw *desiredStateOfWorld) VolumeExistsWithSpecName(podName types.UniquePodName, volumeSpecName string) bool {
	dsw.RLock()
	defer dsw.RUnlock()
	for _, volumeObj := range dsw.volumesToMount {
		if podObj, podExists := volumeObj.podsToMount[podName]; podExists {
			if podObj.volumeSpec.Name() == volumeSpecName {
				return true
			}
		}
	}
	return false
}

// GetPods 汇总所有卷下登记的 Pod，返回集合形式的视图。
func (dsw *desiredStateOfWorld) GetPods() map[types.UniquePodName]bool {
	dsw.RLock()
	defer dsw.RUnlock()

	podList := make(map[types.UniquePodName]bool)
	for _, volumeObj := range dsw.volumesToMount {
		for podName := range volumeObj.podsToMount {
			podList[podName] = true
		}
	}
	return podList
}

// GetVolumeNamesForPod 建立该 Pod 外层卷名到唯一卷名的映射。
func (dsw *desiredStateOfWorld) GetVolumeNamesForPod(podName types.UniquePodName) map[string]v1.UniqueVolumeName {
	dsw.RLock()
	defer dsw.RUnlock()

	volumeNames := make(map[string]v1.UniqueVolumeName)
	for volumeName, volumeObj := range dsw.volumesToMount {
		for _, outerVolumeSpecName := range volumeObj.podsToMount[podName].outerVolumeSpecNames {
			volumeNames[outerVolumeSpecName] = volumeName
		}
	}
	return volumeNames
}

// GetVolumesToMount 把内部“卷 -> Pod”结构展开为逐 Pod 的挂载任务列表。
func (dsw *desiredStateOfWorld) GetVolumesToMount() []VolumeToMount {
	dsw.RLock()
	defer dsw.RUnlock()

	volumesToMount := make([]VolumeToMount, 0 /* len */, len(dsw.volumesToMount) /* cap */)
	for volumeName, volumeObj := range dsw.volumesToMount {
		for podName, podObj := range volumeObj.podsToMount {
			vmt := VolumeToMount{
				VolumeToMount: operationexecutor.VolumeToMount{
					VolumeName:              volumeName,
					PodName:                 podName,
					Pod:                     podObj.pod,
					VolumeSpec:              podObj.volumeSpec,
					PluginIsAttachable:      volumeObj.pluginIsAttachable,
					PluginIsDeviceMountable: volumeObj.pluginIsDeviceMountable,
					OuterVolumeSpecNames:    podObj.outerVolumeSpecNames,
					VolumeGIDValue:          volumeObj.volumeGIDValue,
					ReportedInUse:           volumeObj.reportedInUse,
					MountRequestTime:        podObj.mountRequestTime,
					DesiredSizeLimit:        volumeObj.desiredSizeLimit,
					SELinuxLabel:            volumeObj.effectiveSELinuxMountFileLabel,
				},
			}
			// 深拷贝，避免调用方修改缓存中的 Quantity。
			if !volumeObj.persistentVolumeSize.IsZero() {
				vmt.DesiredPersistentVolumeSize = volumeObj.persistentVolumeSize.DeepCopy()
			}
			volumesToMount = append(volumesToMount, vmt)
		}
	}
	return volumesToMount
}

// AddErrorToPod 为 Pod 追加错误，按集合去重并限制条数。
func (dsw *desiredStateOfWorld) AddErrorToPod(podName types.UniquePodName, err string) {
	dsw.Lock()
	defer dsw.Unlock()

	if errs, found := dsw.podErrors[podName]; found {
		// 达到上限后不再追加，防止错误无限增长。
		if errs.Len() <= maxPodErrors {
			errs.Insert(err)
		}
		return
	}
	dsw.podErrors[podName] = sets.New[string](err)
}

// PopPodErrors 取出并清除 Pod 的错误集合。
func (dsw *desiredStateOfWorld) PopPodErrors(podName types.UniquePodName) []string {
	dsw.Lock()
	defer dsw.Unlock()

	if errs, found := dsw.podErrors[podName]; found {
		delete(dsw.podErrors, podName)
		return sets.List(errs)
	}
	return []string{}
}

// GetPodsWithErrors 收集存在错误的 Pod 名。
func (dsw *desiredStateOfWorld) GetPodsWithErrors() []types.UniquePodName {
	dsw.RLock()
	defer dsw.RUnlock()

	pods := make([]types.UniquePodName, 0, len(dsw.podErrors))
	for podName := range dsw.podErrors {
		pods = append(pods, podName)
	}
	return pods
}

// MarkVolumeAttachability 更新卷的可附加标志；卷不存在时忽略。
func (dsw *desiredStateOfWorld) MarkVolumeAttachability(volumeName v1.UniqueVolumeName, attachable bool) {
	dsw.Lock()
	defer dsw.Unlock()
	volumeObj, volumeExists := dsw.volumesToMount[volumeName]
	if !volumeExists {
		return
	}
	volumeObj.pluginIsAttachable = attachable
	dsw.volumesToMount[volumeName] = volumeObj
}

// 根据卷是否支持 SELinux 挂载，递增警告或错误指标，并决定是否把错误返回给调用方。
func handleSELinuxMetricError(logger klog.Logger, err error, seLinuxSupported bool, warningMetric, errorMetric metrics.GaugeMetric) error {
	if seLinuxSupported {
		errorMetric.Add(1.0)
		return err
	}

	// 目前尚未支持其他访问模式，因此不返回错误；支持后会将其视为错误。
	warningMetric.Add(1.0)
	logger.V(4).Info("Please report this error in https://github.com/kubernetes/enhancements/issues/1710, together with full Pod yaml file", "err", err)
	return nil
}

// getVolumePluginNameWithDriver 返回卷插件名；若为 CSI 卷，再附加 CSI 驱动名。
func getVolumePluginNameWithDriver(logger klog.Logger, plugin volume.VolumePlugin, spec *volume.Spec) string {
	pluginName := plugin.GetPluginName()
	if pluginName != csi.CSIPluginName {
		return pluginName
	}

	// CSI 卷：插件名需拼接驱动名以便区分不同驱动。
	driverName, err := csi.GetCSIDriverName(spec)
	if err != nil {
		// 理论上不可达——此类卷无法通过校验。
		logger.V(4).Info("failed to get CSI driver name from volume spec", "err", err)
		driverName = "unknown"
	}
	// 与 util.GetUniqueVolumeName() 一致，用 "/" 分隔插件名与 CSI 驱动名。
	return pluginName + "/" + driverName
}

// getVolumeAccessMode 返回用于指标维度的访问模式标签，内联卷标记为 inline。
func getVolumeAccessMode(spec *volume.Spec) string {
	if spec.PersistentVolume == nil {
		// Pod 内联卷没有具体访问模式，统一标记为 "inline"。
		return "inline"
	}
	// 指标只需一个维度，按优先级取最高的访问模式：RWX > ROX > RWO > RWOP。
	pv := spec.PersistentVolume
	if util.ContainsAccessMode(pv.Spec.AccessModes, v1.ReadWriteMany) {
		return "RWX"
	}
	if util.ContainsAccessMode(pv.Spec.AccessModes, v1.ReadOnlyMany) {
		return "ROX"
	}
	if util.ContainsAccessMode(pv.Spec.AccessModes, v1.ReadWriteOnce) {
		return "RWO"
	}
	if util.ContainsAccessMode(pv.Spec.AccessModes, v1.ReadWriteOncePod) {
		return "RWOP"
	}
	// 不应发生：校验层不允许空的或未知的访问模式。
	return ""
}
