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

// Package cache 实现 kubelet 卷管理器使用的数据结构，用于跟踪已挂载的卷
// 以及挂载这些卷的 Pod。
package cache

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// ActualStateOfWorld 为 kubelet 卷管理器的“实际状态”缓存定义了一组线程安全操作。
// 该缓存保存卷到 Pod 的映射，即本节点上所有已挂载的卷，以及卷管理器认为已成功
// 挂载该卷的 Pod。
// 注意：它与 attach/detach 控制器实现的 ActualStateOfWorld 不是同一个东西，
// 两者跟踪的对象不同；这里只包含 kubelet 卷管理器特有的状态。
type ActualStateOfWorld interface {
	// ActualStateOfWorld 必须实现该接口，operationexecutor 才能与之交互。
	operationexecutor.ActualStateOfWorldMounterUpdater

	// ActualStateOfWorld 必须实现该接口，operationexecutor 才能与之交互。
	operationexecutor.ActualStateOfWorldAttacherUpdater

	// AddPodToVolume 把给定 Pod 加入缓存中给定卷的挂载列表，表示该卷已成功
	// 挂载到该 Pod。
	// 若该卷下已存在同名 Pod，则重置该 Pod 的 remountRequired 值。
	// 若 attachedVolumes 中不存在名为 volumeName 的卷，返回错误。
	AddPodToVolume(operationexecutor.MarkVolumeOpts) error

	// MarkRemountRequired 为指定 Pod 已成功挂载的每个卷标记“需要重新挂载”
	// （前提是卷插件声明在 Pod 更新时需要重新挂载）。依赖原子更新的卷靠此
	// 在 Pod 更新后刷新卷内容。
	MarkRemountRequired(logger klog.Logger, podName volumetypes.UniquePodName)

	// SetDeviceMountState 设置给定卷的设备挂载状态。当状态为 DeviceGloballyMounted
	// 时表示设备已挂载到全局挂载点；为 DeviceMountUncertain 时表示卷“可能”已
	// 挂载到全局挂载点。两种情况下，在 detach 之前都必须先从全局挂载点卸载。
	// 若 attachedVolumes 中不存在名为 volumeName 的卷，返回错误。
	SetDeviceMountState(volumeName v1.UniqueVolumeName, deviceMountState operationexecutor.DeviceMountState, devicePath, deviceMountPath, seLinuxMountContext string) error

	// DeletePodFromVolume 从缓存中给定卷的挂载列表移除给定 Pod，表示该卷已从
	// 该 Pod 成功卸载。
	// 若该卷下不存在同名 Pod，则为空操作。
	// 若 attachedVolumes 中不存在名为 volumeName 的卷，返回错误。
	DeletePodFromVolume(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) error

	// DeleteVolume 从缓存的已挂载卷列表中移除给定卷，表示该卷已从本节点成功卸载（detach）。
	// 若 attachedVolumes 中不存在名为 volumeName 的卷，则为空操作。
	// 若该卷存在且其 mountedPods 列表非空，返回错误。
	DeleteVolume(volumeName v1.UniqueVolumeName) error

	// PodExistsInVolume 在缓存中给定卷的 mountedPods 列表里找到给定 Pod 时返回
	// true，表示卷已挂载到本节点且该 Pod 已成功挂载它。
	// 若该卷下不存在同名 Pod，返回 false。
	// 若 attachedVolumes 中不存在名为 volumeName 的卷，返回 volumeNotAttachedError，
	// 表示该卷尚未挂载。
	// 若 volumeName/podName 组合存在但 remountRequired 为 true，返回
	// remountRequiredError，表示卷虽已挂载到该 Pod，但为反映 Pod 变更应重新挂载。
	// 依赖原子更新的卷靠此刷新卷内容。
	// 所有卷挂载调用都应幂等，因此对无需更新内容的卷再次调用挂载不应失败。
	PodExistsInVolume(logger klog.Logger, podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName, desiredVolumeSize resource.Quantity, seLinuxLabel string) (bool, string, error)

	// PodRemovedFromVolume 在缓存中给定卷的 mountedPods 列表里找不到给定 Pod 时
	// 返回 true，表示该 Pod 已完全卸载该卷或从未挂载过。
	// 若该 Pod 的卷已完全挂载或处于不确定挂载状态，则认为它仍存在于卷管理器的
	// 实际状态中，返回 false。
	PodRemovedFromVolume(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) bool

	// PodHasMountedVolumes 在给定 Pod 挂载了任一处于已挂载状态的卷时返回 true。
	PodHasMountedVolumes(podName volumetypes.UniquePodName) bool

	// VolumeExistsWithSpecName 若以卷 spec 名称（即 InnerVolumeSpecName）标识的卷
	// 存在于应挂载到本节点的卷列表中则返回 true。
	// 若该卷下不存在同名 Pod，返回 false。
	VolumeExistsWithSpecName(podName volumetypes.UniquePodName, volumeSpecName string) bool

	// VolumeExists 若给定卷存在于缓存的已挂载卷列表中则返回 true，表示该卷已
	// 挂载到本节点。
	VolumeExists(volumeName v1.UniqueVolumeName) bool

	// GetMountedVolumes 依据当前实际状态，返回已成功挂载的卷及其 Pod 的列表。
	GetMountedVolumes() []MountedVolume

	// GetAllMountedVolumes 返回所有可能已挂载的卷，包括处于 VolumeMounted 和
	// VolumeMountUncertain 状态的卷。
	GetAllMountedVolumes() []MountedVolume

	// GetMountedVolumesForPod 依据当前实际状态，返回指定 Pod 已成功挂载的卷列表。
	GetMountedVolumesForPod(podName volumetypes.UniquePodName) []MountedVolume

	// GetMountedVolumeForPod 若给定卷已挂载到给定 Pod，则返回该卷及 true。
	GetMountedVolumeForPod(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) (MountedVolume, bool)

	// GetPossiblyMountedVolumesForPod 返回指定 Pod 已挂载或状态“不确定”的卷列表，
	// 即卷插件可能正在挂载该卷。
	GetPossiblyMountedVolumesForPod(podName volumetypes.UniquePodName) []MountedVolume

	// GetGloballyMountedVolumes 返回所有已全局挂载的已挂载卷。该列表可用于确定
	// 哪些卷应在节点的 VolumesInUse 状态字段中上报为 "in use"。这里的“全局挂载”
	// 指可挂载卷的共享插件挂载点，Pod 专属挂载点由它通过 bind mount 创建。
	GetGloballyMountedVolumes() []AttachedVolume

	// GetUnmountedVolumes 返回没有 mountedPods 的已挂载卷列表。该列表可用于确定
	// 哪些卷已无引用、可以全局卸载并 detach。
	GetUnmountedVolumes() []AttachedVolume

	// GetAttachedVolumes 返回已知挂载到本节点的卷列表。该列表可用于确定哪些卷
	// 正在使用中或存在挂载/卸载操作待处理。
	GetAttachedVolumes() []AttachedVolume

	// GetAttachedVolume 返回给定卷名对应的、已知挂载到本节点的卷。若未找到，
	// 第二个返回值为 false。
	GetAttachedVolume(volumeName v1.UniqueVolumeName) (AttachedVolume, bool)

	// 将指定卷以“挂载状态不确定”加入 ASW。
	AddAttachUncertainReconstructedVolume(logger klog.Logger, volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName, devicePath string) error

	// UpdateReconstructedDevicePath 依据 Node.Status.VolumesAttached 更新重建卷的
	// devicePath。仅当卷仍处于不确定状态时才更新 ASW；若期间卷已被挂载，其
	// devicePath 应已由那次更新修正。
	UpdateReconstructedDevicePath(volumeName v1.UniqueVolumeName, devicePath string)

	// UpdateReconstructedVolumeAttachability 依据 API server 更新卷的可挂载性。
	UpdateReconstructedVolumeAttachability(volumeName v1.UniqueVolumeName, volumeAttachable bool)
}

// MountedVolume 表示已成功挂载到某个 Pod 的卷。
type MountedVolume struct {
	operationexecutor.MountedVolume
}

// AttachedVolume 表示已挂载到某个节点的卷。
type AttachedVolume struct {
	operationexecutor.AttachedVolume

	// DeviceMountState 表示设备是否已全局挂载。
	DeviceMountState operationexecutor.DeviceMountState

	// SELinuxMountContext 是卷全局挂载时使用的上下文（通过 -o context=XYZ 挂载选项）。
	// 为空表示卷未以 "-o context=" 方式挂载。
	SELinuxMountContext string
}

// DeviceMayBeMounted 在设备已挂载到全局路径或处于不确定状态时返回 true。
func (av AttachedVolume) DeviceMayBeMounted() bool {
	return av.DeviceMountState == operationexecutor.DeviceGloballyMounted ||
		av.DeviceMountState == operationexecutor.DeviceMountUncertain
}

// NewActualStateOfWorld 返回 ActualStateOfWorld 的新实例。
func NewActualStateOfWorld(
	nodeName types.NodeName,
	volumePluginMgr *volume.VolumePluginMgr) ActualStateOfWorld {
	return &actualStateOfWorld{
		nodeName:                        nodeName,
		attachedVolumes:                 make(map[v1.UniqueVolumeName]attachedVolume),
		foundDuringReconstruction:       make(map[v1.UniqueVolumeName]map[volumetypes.UniquePodName]types.UID),
		attemptedMounts:                 make(map[volumetypes.UniquePodName]sets.Set[v1.UniqueVolumeName]),
		volumePluginMgr:                 volumePluginMgr,
		volumesWithFinalExpansionErrors: sets.New[v1.UniqueVolumeName](),
	}
}

// IsVolumeNotAttachedError 判断指定错误是否为 volumeNotAttachedError。
func IsVolumeNotAttachedError(err error) bool {
	_, ok := err.(volumeNotAttachedError)
	return ok
}

// IsRemountRequiredError 判断指定错误是否为 remountRequiredError。
func IsRemountRequiredError(err error) bool {
	_, ok := err.(remountRequiredError)
	return ok
}

// actualStateOfWorld 是 ActualStateOfWorld 的具体实现，内嵌 RWMutex 保证并发安全。
type actualStateOfWorld struct {
	// nodeName 是本节点名称，会传给 Attach/Detach 操作。
	nodeName types.NodeName

	// attachedVolumes 保存 kubelet 卷管理器认为已成功挂载到本节点的卷集合。
	// 未实现 attacher 接口的卷类型默认就处于该状态。
	// 键为卷名，值为包含该已挂载卷更多信息的对象。
	attachedVolumes map[v1.UniqueVolumeName]attachedVolume
	// foundDuringReconstruction 记录 kubelet 重启时从 kubelet 根目录发现的卷。
	foundDuringReconstruction map[v1.UniqueVolumeName]map[volumetypes.UniquePodName]types.UID

	// attemptedMounts 记录 kubelet 曾尝试 stage/publish 的 Pod/卷对，即使挂载
	// 从未进入 mounted/uncertain 的 ASW 状态。按 Pod 名映射到卷名集合。
	attemptedMounts map[volumetypes.UniquePodName]sets.Set[v1.UniqueVolumeName]

	// volumesWithFinalExpansionErrors 记录扩容遇到不可重试错误的卷。
	volumesWithFinalExpansionErrors sets.Set[v1.UniqueVolumeName]

	// volumePluginMgr 用于创建卷插件对象。
	volumePluginMgr *volume.VolumePluginMgr
	sync.RWMutex
}

// volumeAttachability 表示卷的可挂载性，包括“不确定”这一重建期间的过渡状态。
type volumeAttachability string

// volumeAttachability 的三种取值。
const (
	volumeAttachabilityTrue      volumeAttachability = "True"
	volumeAttachabilityFalse     volumeAttachability = "False"
	volumeAttachabilityUncertain volumeAttachability = "Uncertain"
)

// attachedVolume 表示 kubelet 卷管理器认为已成功挂载到其管理节点上的卷。
// 未实现 attacher 的卷类型默认处于该状态。
type attachedVolume struct {
	// volumeName 是该卷的唯一标识符。
	volumeName v1.UniqueVolumeName

	// mountedPods 保存该卷已成功挂载到的 Pod 集合。键为 Pod 名，值为包含该
	// Pod 更多信息的 mountedPod 对象。
	mountedPods map[volumetypes.UniquePodName]mountedPod

	// spec 是该卷的 spec，用于生成卷插件对象并传给插件方法。
	// 特别是 Unmount 方法会把 spec.Name() 作为挂载路径中的 volumeSpecName：
	// /var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{volumeSpecName}/
	spec *volume.Spec

	// pluginName 是用于挂载和挂载该卷的卷插件的非转义限定名。之所以单独保存，
	// 是因为对需要卸载的卷（例如从磁盘读到挂载路径但没有完整卷 spec 的情况），
	// 可能无法重建完整卷 spec（除名称外的全部内容）。
	pluginName string

	// pluginIsAttachable 表示用于挂载该卷的插件是否实现了 volume.Attacher 接口。
	pluginIsAttachable volumeAttachability

	// deviceMountState 记录设备是否已全局挂载。
	deviceMountState operationexecutor.DeviceMountState

	// devicePath 是可挂载卷在节点上挂载后的设备路径。
	devicePath string

	// deviceMountPath 是设备挂载后应挂载到的节点路径。
	deviceMountPath string

	// volumeInUseErrorForExpansion 表示卷驱动此前已针对该卷返回 volume-in-use
	// 错误，本节点不应再重试扩容。
	volumeInUseErrorForExpansion bool

	// persistentVolumeSize 记录 Pod 启动时的卷大小，或扩容成功后的卷大小。
	persistentVolumeSize resource.Quantity

	// seLinuxMountContext 是卷挂载到全局目录时使用的上下文（通过 -o context=XYZ）。
	// 为 nil 表示卷未挂载；为 "" 表示挂载时未使用 "-o context="。
	seLinuxMountContext *string
}

// mountedPod 表示 kubelet 卷管理器认为底层卷已成功挂载的 Pod。
type mountedPod struct {
	// Pod 名
	podName volumetypes.UniquePodName

	// Pod UID，用于唯一标识该 Pod
	podUID types.UID

	// 用于执行挂载的 mounter
	mounter volume.Mounter

	// 用于支持块卷的 mapper
	blockVolumeMapper volume.BlockVolumeMapper

	// spec 是该卷的 spec，用于生成卷插件对象并传给插件方法。
	// 特别是 Unmount 方法会把 spec.Name() 作为挂载路径中的 volumeSpecName：
	// /var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{volumeSpecName}/
	volumeSpec *volume.Spec

	// remountRequired 表示底层卷虽已成功挂载到该 Pod，但为反映 Pod 变更应重新挂载。
	// 依赖原子更新的卷靠此刷新卷内容。所有卷挂载调用都应幂等，因此对无需更新
	// 内容的卷再次调用挂载不应失败。
	remountRequired bool

	// volumeGIDValue 保存 GID 注解的值（若存在）。
	volumeGIDValue string

	// volumeMountStateForPod 保存该 Pod 的卷挂载状态：
	//   - VolumeMounted：表示该 Pod 的卷已成功挂载
	//   - VolumeMountUncertain：表示该 Pod 的卷可能未挂载，但必须卸载
	volumeMountStateForPod operationexecutor.VolumeMountState

	// seLinuxMountContext 是卷挂载到 Pod 目录时使用的上下文（通过 -o context=XYZ）。
	// 为 nil 表示卷未挂载；为 "" 表示挂载时未使用 "-o context="。
	seLinuxMountContext string
}

// MarkVolumeAsAttached 把卷标记为已挂载。先探测插件是否实现 attacher，
// 以决定该卷是否需要走 attach/detach 流程。
func (asw *actualStateOfWorld) MarkVolumeAsAttached(
	logger klog.Logger,
	volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, _ types.NodeName, devicePath string) error {

	pluginIsAttachable := volumeAttachabilityFalse
	if attachablePlugin, err := asw.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec); err == nil && attachablePlugin != nil {
		pluginIsAttachable = volumeAttachabilityTrue
	}

	return asw.addVolume(logger, volumeName, volumeSpec, devicePath, pluginIsAttachable)
}

// AddAttachUncertainReconstructedVolume 以“挂载状态不确定”把重建出来的卷加入 ASW。
func (asw *actualStateOfWorld) AddAttachUncertainReconstructedVolume(
	logger klog.Logger,
	volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, _ types.NodeName, devicePath string) error {

	return asw.addVolume(logger, volumeName, volumeSpec, devicePath, volumeAttachabilityUncertain)
}

// MarkVolumeAsUncertain 在 kubelet 侧是空操作：kubelet 不负责 attach，只有
// attach/detach 控制器才需要该语义。
func (asw *actualStateOfWorld) MarkVolumeAsUncertain(
	logger klog.Logger, volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, _ types.NodeName) error {
	return nil
}

// MarkVolumeAsDetached 把卷标记为已 detach，即从 ASW 中删除。
func (asw *actualStateOfWorld) MarkVolumeAsDetached(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.DeleteVolume(volumeName)
}

// MarkVolumeExpansionFailedWithFinalError 记录该卷扩容失败且错误不可重试。
func (asw *actualStateOfWorld) MarkVolumeExpansionFailedWithFinalError(volumeName v1.UniqueVolumeName) {
	asw.Lock()
	defer asw.Unlock()

	asw.volumesWithFinalExpansionErrors.Insert(volumeName)
}

// RemoveVolumeFromFailedWithFinalErrors 清除该卷的扩容终态失败标记。
func (asw *actualStateOfWorld) RemoveVolumeFromFailedWithFinalErrors(volumeName v1.UniqueVolumeName) {
	asw.Lock()
	defer asw.Unlock()

	asw.volumesWithFinalExpansionErrors.Delete(volumeName)
}

// CheckVolumeInFailedExpansionWithFinalErrors 判断该卷是否已标记扩容终态失败。
func (asw *actualStateOfWorld) CheckVolumeInFailedExpansionWithFinalErrors(volumeName v1.UniqueVolumeName) bool {
	asw.RLock()
	defer asw.RUnlock()

	return asw.volumesWithFinalExpansionErrors.Has(volumeName)
}

// IsVolumeReconstructed 判断该 Pod/卷对是否来自 kubelet 重启后的重建。
func (asw *actualStateOfWorld) IsVolumeReconstructed(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool {
	volumeState := asw.GetVolumeMountState(volumeName, podName)

	// 只有不确定状态的卷才可能来自重建
	if volumeState != operationexecutor.VolumeMountUncertain {
		return false
	}

	asw.RLock()
	defer asw.RUnlock()
	podMap, ok := asw.foundDuringReconstruction[volumeName]
	if !ok {
		return false
	}
	_, foundPod := podMap[podName]
	return foundPod
}

// IsVolumeDeviceReconstructed 判断该卷的设备挂载信息是否来自重建。
func (asw *actualStateOfWorld) IsVolumeDeviceReconstructed(volumeName v1.UniqueVolumeName) bool {
	asw.RLock()
	defer asw.RUnlock()
	_, ok := asw.foundDuringReconstruction[volumeName]
	return ok
}

// CheckAndMarkVolumeAsUncertainViaReconstruction 在重建阶段把卷/Pod 标记为挂载
// 状态不确定，返回是否实际做了标记。
func (asw *actualStateOfWorld) CheckAndMarkVolumeAsUncertainViaReconstruction(opts operationexecutor.MarkVolumeOpts) (bool, error) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[opts.VolumeName]
	if !volumeExists {
		return false, nil
	}

	podObj, podExists := volumeObj.mountedPods[opts.PodName]
	if podExists {
		// 已处于不确定状态说明仍需继续尝试卸载，无需重复标记
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain {
			return false, nil
		}
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
			return false, nil
		}
	}

	podName := opts.PodName
	podUID := opts.PodUID
	volumeName := opts.VolumeName
	mounter := opts.Mounter
	blockVolumeMapper := opts.BlockVolumeMapper
	volumeGIDValue := opts.VolumeGIDVolume
	volumeSpec := opts.VolumeSpec

	podObj = mountedPod{
		podName:                podName,
		podUID:                 podUID,
		mounter:                mounter,
		blockVolumeMapper:      blockVolumeMapper,
		volumeGIDValue:         volumeGIDValue,
		volumeSpec:             volumeSpec,
		remountRequired:        false,
		volumeMountStateForPod: operationexecutor.VolumeMountUncertain,
	}

	if mounter != nil {
		// 对象里保存的 mounter 可能已过时，使用最新的。
		podObj.mounter = mounter
	}

	asw.attachedVolumes[volumeName].mountedPods[podName] = podObj

	podMap, ok := asw.foundDuringReconstruction[opts.VolumeName]
	if !ok {
		podMap = map[volumetypes.UniquePodName]types.UID{}
	}
	podMap[opts.PodName] = opts.PodUID
	asw.foundDuringReconstruction[opts.VolumeName] = podMap
	// 重建的卷此前已 stage/publish，标记为“已尝试”，使它们仍可参与卷健康探测。
	asw.markVolumeAsAttemptedLocked(opts.PodName, opts.VolumeName)
	return true, nil
}

// CheckAndMarkDeviceUncertainViaReconstruction 在重建阶段把卷的设备挂载状态标记为
// 不确定，返回是否实际做了标记。
func (asw *actualStateOfWorld) CheckAndMarkDeviceUncertainViaReconstruction(volumeName v1.UniqueVolumeName, deviceMountPath string) bool {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	// 该函数要求卷已被标记为 attached，因此卷不存在或不是 DeviceNotMounted 状态时直接返回
	if !volumeExists || volumeObj.deviceMountState != operationexecutor.DeviceNotMounted {
		return false
	}

	volumeObj.deviceMountState = operationexecutor.DeviceMountUncertain
	// 这里只更新 deviceMountPath，因为此阶段 devicePath 由 node 对象决定。
	volumeObj.deviceMountPath = deviceMountPath
	asw.attachedVolumes[volumeName] = volumeObj
	return true

}

// MarkVolumeAsMounted 把卷标记为已挂载到 Pod。
func (asw *actualStateOfWorld) MarkVolumeAsMounted(markVolumeOpts operationexecutor.MarkVolumeOpts) error {
	return asw.AddPodToVolume(markVolumeOpts)
}

// AddVolumeToReportAsAttached 在 kubelet 侧是空操作，节点上报由外部逻辑负责。
func (asw *actualStateOfWorld) AddVolumeToReportAsAttached(logger klog.Logger, volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	// kubelet 侧无操作
}

// RemoveVolumeFromReportAsAttached 在 kubelet 侧是空操作。
func (asw *actualStateOfWorld) RemoveVolumeFromReportAsAttached(volumeName v1.UniqueVolumeName, nodeName types.NodeName) error {
	// kubelet 侧无操作
	return nil
}

// MarkVolumeAsUnmounted 把卷标记为已从 Pod 卸载。
func (asw *actualStateOfWorld) MarkVolumeAsUnmounted(
	podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) error {
	return asw.DeletePodFromVolume(podName, volumeName)
}

// MarkDeviceAsMounted 把设备标记为已全局挂载。
func (asw *actualStateOfWorld) MarkDeviceAsMounted(
	volumeName v1.UniqueVolumeName, devicePath, deviceMountPath, seLinuxMountContext string) error {
	return asw.SetDeviceMountState(volumeName, operationexecutor.DeviceGloballyMounted, devicePath, deviceMountPath, seLinuxMountContext)
}

// MarkDeviceAsUncertain 把设备标记为挂载状态不确定。
func (asw *actualStateOfWorld) MarkDeviceAsUncertain(
	volumeName v1.UniqueVolumeName, devicePath, deviceMountPath, seLinuxMountContext string) error {
	return asw.SetDeviceMountState(volumeName, operationexecutor.DeviceMountUncertain, devicePath, deviceMountPath, seLinuxMountContext)
}

// MarkVolumeMountAsUncertain 把 Pod 的卷挂载标记为不确定，即需要继续尝试卸载。
func (asw *actualStateOfWorld) MarkVolumeMountAsUncertain(markVolumeOpts operationexecutor.MarkVolumeOpts) error {
	markVolumeOpts.VolumeMountState = operationexecutor.VolumeMountUncertain
	return asw.AddPodToVolume(markVolumeOpts)
}

// MarkDeviceAsUnmounted 把设备标记为已全局卸载。
func (asw *actualStateOfWorld) MarkDeviceAsUnmounted(
	volumeName v1.UniqueVolumeName) error {
	return asw.SetDeviceMountState(volumeName, operationexecutor.DeviceNotMounted, "", "", "")
}

// UpdateReconstructedDevicePath 更新重建卷的 devicePath。
func (asw *actualStateOfWorld) UpdateReconstructedDevicePath(volumeName v1.UniqueVolumeName, devicePath string) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return
	}
	if volumeObj.deviceMountState != operationexecutor.DeviceMountUncertain {
		// 说明 Reconciler 已更新过卷状态（例如 Pod 使用该卷并挂载成功），
		// device path 已被那次更新修正。
		return
	}

	volumeObj.devicePath = devicePath
	asw.attachedVolumes[volumeName] = volumeObj
}

// UpdateReconstructedVolumeAttachability 更新重建卷的可挂载性。
func (asw *actualStateOfWorld) UpdateReconstructedVolumeAttachability(volumeName v1.UniqueVolumeName, attachable bool) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return
	}
	if volumeObj.pluginIsAttachable != volumeAttachabilityUncertain {
		// 说明 Reconciler 已更新过卷状态（例如 Pod 使用该卷并挂载成功），
		// device path 已被那次更新修正。
		return
	}

	if attachable {
		volumeObj.pluginIsAttachable = volumeAttachabilityTrue
	} else {
		volumeObj.pluginIsAttachable = volumeAttachabilityFalse
	}
	asw.attachedVolumes[volumeName] = volumeObj
}

// GetDeviceMountState 返回该卷的设备挂载状态，未知卷视为未挂载。
func (asw *actualStateOfWorld) GetDeviceMountState(volumeName v1.UniqueVolumeName) operationexecutor.DeviceMountState {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return operationexecutor.DeviceNotMounted
	}

	return volumeObj.deviceMountState
}

// MarkForInUseExpansionError 标记该卷因“正在使用”而无法扩容，后续不再重试。
func (asw *actualStateOfWorld) MarkForInUseExpansionError(volumeName v1.UniqueVolumeName) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, ok := asw.attachedVolumes[volumeName]
	if ok {
		volumeObj.volumeInUseErrorForExpansion = true
		asw.attachedVolumes[volumeName] = volumeObj
	}
}

// GetVolumeMountState 返回给定 Pod 上该卷的挂载状态，未知时视为未挂载。
func (asw *actualStateOfWorld) GetVolumeMountState(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) operationexecutor.VolumeMountState {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return operationexecutor.VolumeNotMounted
	}

	podObj, podExists := volumeObj.mountedPods[podName]
	if !podExists {
		return operationexecutor.VolumeNotMounted
	}
	return podObj.volumeMountStateForPod
}

// MarkVolumeAsMountAttempted 记录 kubelet 曾为该 Pod 尝试 stage/publish 该卷。
func (asw *actualStateOfWorld) MarkVolumeAsMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) {
	asw.Lock()
	defer asw.Unlock()
	asw.markVolumeAsAttemptedLocked(podName, volumeName)
}

// IsVolumeMountAttempted 判断该 Pod/卷对是否调用过 MarkVolumeAsMountAttempted。
func (asw *actualStateOfWorld) IsVolumeMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) bool {
	asw.RLock()
	defer asw.RUnlock()
	volumes, ok := asw.attemptedMounts[podName]
	if !ok {
		return false
	}
	return volumes.Has(volumeName)
}

// markVolumeAsAttemptedLocked 记录一次挂载尝试。调用方必须持有 asw.Lock。
func (asw *actualStateOfWorld) markVolumeAsAttemptedLocked(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) {
	volumes, ok := asw.attemptedMounts[podName]
	if !ok {
		volumes = sets.New[v1.UniqueVolumeName]()
		asw.attemptedMounts[podName] = volumes
	}
	volumes.Insert(volumeName)
}

// clearVolumeMountAttemptedLocked 清除该 Pod/卷对的“已尝试”标记。
// 调用方必须持有 asw.Lock。
func (asw *actualStateOfWorld) clearVolumeMountAttemptedLocked(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) {
	volumes, ok := asw.attemptedMounts[podName]
	if !ok {
		return
	}
	volumes.Delete(volumeName)
	if volumes.Len() == 0 {
		delete(asw.attemptedMounts, podName)
	}
}

// clearVolumeMountAttemptedForVolumeLocked 清除该卷在所有 Pod 上的“已尝试”标记。
// 调用方必须持有 asw.Lock。
func (asw *actualStateOfWorld) clearVolumeMountAttemptedForVolumeLocked(volumeName v1.UniqueVolumeName) {
	for podName, volumes := range asw.attemptedMounts {
		volumes.Delete(volumeName)
		if volumes.Len() == 0 {
			delete(asw.attemptedMounts, podName)
		}
	}
}

// IsVolumeMountedElsewhere 判断该卷是否还挂载在其他 Pod 上。
func (asw *actualStateOfWorld) IsVolumeMountedElsewhere(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return false
	}

	for _, podObj := range volumeObj.mountedPods {
		if podName != podObj.podName {
			// 在状态确定前，把不确定挂载也当作已挂载处理。
			if podObj.volumeMountStateForPod != operationexecutor.VolumeNotMounted {
				return true
			}
		}
	}
	return false
}

// addVolume 把给定卷加入缓存，表示该卷已挂载到本节点。若未提供卷名，则依据
// volumeSpec 生成唯一卷名。若同名卷已存在，则为空操作。若没有卷插件支持该
// volumeSpec，或有多个插件支持，返回错误。
func (asw *actualStateOfWorld) addVolume(logger klog.Logger,
	volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, devicePath string, attachability volumeAttachability) error {
	asw.Lock()
	defer asw.Unlock()

	volumePlugin, err := asw.volumePluginMgr.FindPluginBySpec(volumeSpec)
	if err != nil || volumePlugin == nil {
		return fmt.Errorf(
			"failed to get Plugin from volumeSpec for volume %q err=%v",
			volumeSpec.Name(),
			err)
	}

	if len(volumeName) == 0 {
		volumeName, err = util.GetUniqueVolumeNameFromSpec(volumePlugin, volumeSpec)
		if err != nil {
			return fmt.Errorf(
				"failed to GetUniqueVolumeNameFromSpec for volumeSpec %q using volume plugin %q err=%v",
				volumeSpec.Name(),
				volumePlugin.GetPluginName(),
				err)
		}
	}

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		volumeObj = attachedVolume{
			volumeName:         volumeName,
			spec:               volumeSpec,
			mountedPods:        make(map[volumetypes.UniquePodName]mountedPod),
			pluginName:         volumePlugin.GetPluginName(),
			pluginIsAttachable: attachability,
			deviceMountState:   operationexecutor.DeviceNotMounted,
			devicePath:         devicePath,
		}
	} else {
		// 卷对象已存在时只更新 device path 等字段
		volumeObj.devicePath = devicePath
		logger.V(2).Info("Volume is already added to attachedVolume list, update device path", "volumeName", volumeName, "path", devicePath)
	}
	asw.attachedVolumes[volumeName] = volumeObj

	return nil
}

// AddPodToVolume 把 Pod 加入卷的已挂载列表，即记录卷已成功挂载到该 Pod。
func (asw *actualStateOfWorld) AddPodToVolume(markVolumeOpts operationexecutor.MarkVolumeOpts) error {
	podName := markVolumeOpts.PodName
	podUID := markVolumeOpts.PodUID
	volumeName := markVolumeOpts.VolumeName
	mounter := markVolumeOpts.Mounter
	blockVolumeMapper := markVolumeOpts.BlockVolumeMapper
	volumeGIDValue := markVolumeOpts.VolumeGIDVolume
	volumeSpec := markVolumeOpts.VolumeSpec
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return fmt.Errorf(
			"no volume with the name %q exists in the list of attached volumes",
			volumeName)
	}

	podObj, podExists := volumeObj.mountedPods[podName]

	updateUncertainVolume := false
	if podExists {
		// 更新不确定状态的卷：新的 markVolumeOpts 可能带有更新后的信息，
		// 尤其是重建时标记为不确定的卷需要这次更新。
		updateUncertainVolume = podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain
	}
	if !podExists || updateUncertainVolume {
		// 新增 mountedPod，或更新已存在的记录。
		podObj = mountedPod{
			podName:                podName,
			podUID:                 podUID,
			mounter:                mounter,
			blockVolumeMapper:      blockVolumeMapper,
			volumeGIDValue:         volumeGIDValue,
			volumeSpec:             volumeSpec,
			volumeMountStateForPod: markVolumeOpts.VolumeMountState,
			seLinuxMountContext:    markVolumeOpts.SELinuxMountContext,
		}
	}

	// Pod 已存在时重置 remountRequired 值
	podObj.remountRequired = false
	podObj.volumeMountStateForPod = markVolumeOpts.VolumeMountState

	// 卷已成功挂载后，应从 foundDuringReconstruction 中移除
	if markVolumeOpts.VolumeMountState == operationexecutor.VolumeMounted {
		delete(asw.foundDuringReconstruction[volumeName], podName)
	}
	if mounter != nil {
		// 对象里保存的 mounter 可能已过时，使用最新的。
		podObj.mounter = mounter
	}
	asw.attachedVolumes[volumeName].mountedPods[podName] = podObj
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		// 同时把挂载上下文存到 AttachedVolume 上，便于 PodExistsInVolume 中快速比较全局上下文。
		if volumeObj.seLinuxMountContext == nil {
			volumeObj.seLinuxMountContext = &markVolumeOpts.SELinuxMountContext
			asw.attachedVolumes[volumeName] = volumeObj
		}
	}

	return nil
}

// MarkVolumeAsResized 更新卷的持久化大小记录，返回卷是否存在。
func (asw *actualStateOfWorld) MarkVolumeAsResized(volumeName v1.UniqueVolumeName, claimSize resource.Quantity) bool {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, ok := asw.attachedVolumes[volumeName]
	if ok {
		volumeObj.persistentVolumeSize = claimSize
		asw.attachedVolumes[volumeName] = volumeObj
		return true
	}
	return false
}

// MarkRemountRequired 为 Pod 挂载的、且插件声明需要重新挂载的卷打上标记。
func (asw *actualStateOfWorld) MarkRemountRequired(
	logger klog.Logger,
	podName volumetypes.UniquePodName) {
	asw.Lock()
	defer asw.Unlock()
	for volumeName, volumeObj := range asw.attachedVolumes {
		if podObj, podExists := volumeObj.mountedPods[podName]; podExists {
			volumePlugin, err :=
				asw.volumePluginMgr.FindPluginBySpec(podObj.volumeSpec)
			if err != nil || volumePlugin == nil {
				// 记录日志后继续处理其余卷，避免一个坏卷阻断整个 Pod 的更新
				logger.Error(nil, "MarkRemountRequired failed to FindPluginBySpec for volume", "uniquePodName", podObj.podName, "podUID", podObj.podUID, "volumeName", volumeName, "volumeSpecName", podObj.volumeSpec.Name())
				continue
			}

			if volumePlugin.RequiresRemount(podObj.volumeSpec) {
				podObj.remountRequired = true
				asw.attachedVolumes[volumeName].mountedPods[podName] = podObj
			}
		}
	}
}

// SetDeviceMountState 设置卷的设备挂载状态及相关路径。
func (asw *actualStateOfWorld) SetDeviceMountState(
	volumeName v1.UniqueVolumeName, deviceMountState operationexecutor.DeviceMountState, devicePath, deviceMountPath, seLinuxMountContext string) error {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return fmt.Errorf(
			"no volume with the name %q exists in the list of attached volumes",
			volumeName)
	}

	volumeObj.deviceMountState = deviceMountState
	volumeObj.deviceMountPath = deviceMountPath
	if devicePath != "" {
		volumeObj.devicePath = devicePath
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		if seLinuxMountContext != "" {
			volumeObj.seLinuxMountContext = &seLinuxMountContext
		}
	}

	asw.attachedVolumes[volumeName] = volumeObj
	return nil
}

// InitializeClaimSize 在卷的持久化大小未知时（通常发生在 kubelet 重启后重建卷时）
// 初始化其大小。
func (asw *actualStateOfWorld) InitializeClaimSize(logger klog.Logger, volumeName v1.UniqueVolumeName, claimSize resource.Quantity) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, ok := asw.attachedVolumes[volumeName]
	// 仅在 persistentVolumeSize 为零时设置，避免覆盖已知的卷大小
	if ok && volumeObj.persistentVolumeSize.IsZero() {
		volumeObj.persistentVolumeSize = claimSize
		asw.attachedVolumes[volumeName] = volumeObj
	}
}

// GetClaimSize 返回卷的持久化大小副本，未知时返回空 Quantity。
func (asw *actualStateOfWorld) GetClaimSize(volumeName v1.UniqueVolumeName) resource.Quantity {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, ok := asw.attachedVolumes[volumeName]
	if ok {
		return volumeObj.persistentVolumeSize.DeepCopy()
	}
	return resource.Quantity{}
}

// DeletePodFromVolume 从卷的已挂载列表中移除 Pod，即记录卷已从该 Pod 卸载。
func (asw *actualStateOfWorld) DeletePodFromVolume(
	podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) error {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return fmt.Errorf(
			"no volume with the name %q exists in the list of attached volumes",
			volumeName)
	}

	_, podExists := volumeObj.mountedPods[podName]
	if podExists {
		delete(asw.attachedVolumes[volumeName].mountedPods, podName)
	}

	// 若存在重建记录，一并清理
	_, podExists = asw.foundDuringReconstruction[volumeName]
	if podExists {
		delete(asw.foundDuringReconstruction[volumeName], podName)
	}

	asw.clearVolumeMountAttemptedLocked(podName, volumeName)

	return nil
}

// DeleteVolume 从已挂载卷列表中移除卷，即记录卷已从本节点 detach。
func (asw *actualStateOfWorld) DeleteVolume(volumeName v1.UniqueVolumeName) error {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return nil
	}

	if len(volumeObj.mountedPods) != 0 {
		return fmt.Errorf(
			"failed to DeleteVolume %q, it still has %v mountedPods",
			volumeName,
			len(volumeObj.mountedPods))
	}

	delete(asw.attachedVolumes, volumeName)
	delete(asw.foundDuringReconstruction, volumeName)
	asw.clearVolumeMountAttemptedForVolumeLocked(volumeName)
	return nil
}

// PodExistsInVolume 判断卷是否已挂载到本节点且该 Pod 已成功挂载它，同时报告需要
// 重新挂载、扩容等特殊情况。
func (asw *actualStateOfWorld) PodExistsInVolume(logger klog.Logger, podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName, desiredVolumeSize resource.Quantity, seLinuxLabel string) (bool, string, error) {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return false, "", newVolumeNotAttachedError(volumeName)
	}

	// 卷存在，检查其 SELinux 上下文挂载选项
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		if volumeObj.seLinuxMountContext != nil && *volumeObj.seLinuxMountContext != seLinuxLabel {
			fullErr := newSELinuxMountMismatchError(volumeName)
			return false, volumeObj.devicePath, fullErr
		}
	}

	podObj, podExists := volumeObj.mountedPods[podName]
	if podExists {
		// 挂载状态不确定时需继续尝试挂载
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain {
			return false, volumeObj.devicePath, nil
		}
		if podObj.remountRequired {
			return true, volumeObj.devicePath, newRemountRequiredError(volumeObj.volumeName, podObj.podName)
		}
		if currentSize, expandVolume := asw.volumeNeedsExpansion(logger, volumeObj, desiredVolumeSize); expandVolume {
			return true, volumeObj.devicePath, newFsResizeRequiredError(volumeObj.volumeName, podObj.podName, currentSize)
		}
	}

	return podExists, volumeObj.devicePath, nil
}

// PodHasMountedVolumes 判断该 Pod 是否有任一处于已挂载状态的卷。
func (asw *actualStateOfWorld) PodHasMountedVolumes(podName volumetypes.UniquePodName) bool {
	asw.RLock()
	defer asw.RUnlock()
	for _, volumeObj := range asw.attachedVolumes {
		if podObj, hasPod := volumeObj.mountedPods[podName]; hasPod {
			if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
				return true
			}
		}
	}

	return false
}

// volumeNeedsExpansion 判断卷是否需要扩容，需要时返回当前大小和 true。
func (asw *actualStateOfWorld) volumeNeedsExpansion(logger klog.Logger, volumeObj attachedVolume, desiredVolumeSize resource.Quantity) (resource.Quantity, bool) {
	currentSize := volumeObj.persistentVolumeSize.DeepCopy()
	if volumeObj.volumeInUseErrorForExpansion {
		return currentSize, false
	}
	if volumeObj.persistentVolumeSize.IsZero() || desiredVolumeSize.IsZero() {
		return currentSize, false
	}

	logger.V(5).Info("NodeExpandVolume checking size", "actualSize", volumeObj.persistentVolumeSize.String(), "desiredSize", desiredVolumeSize.String(), "volume", volumeObj.volumeName)

	if desiredVolumeSize.Cmp(volumeObj.persistentVolumeSize) > 0 {
		volumePlugin, err := asw.volumePluginMgr.FindNodeExpandablePluginBySpec(volumeObj.spec)
		if err != nil || volumePlugin == nil {
			// 记录日志后继续处理
			logger.Info("PodExistsInVolume failed to find expandable plugin",
				"volume", volumeObj.volumeName,
				"volumeSpecName", volumeObj.spec.Name())
			return currentSize, false
		}
		if volumePlugin.RequiresFSResize() {
			return currentSize, true
		}
	}
	return currentSize, false
}

// PodRemovedFromVolume 判断该 Pod 是否已完全从卷上卸载（或从未挂载）。
func (asw *actualStateOfWorld) PodRemovedFromVolume(
	podName volumetypes.UniquePodName,
	volumeName v1.UniqueVolumeName) bool {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return true
	}

	podObj, podExists := volumeObj.mountedPods[podName]
	if podExists {
		// 挂载状态不确定时仍需继续尝试卸载
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain {
			return false
		}
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
			return false
		}
	}
	return true
}

// VolumeExistsWithSpecName 判断该 Pod 下是否存在指定卷 spec 名称的卷。
func (asw *actualStateOfWorld) VolumeExistsWithSpecName(podName volumetypes.UniquePodName, volumeSpecName string) bool {
	asw.RLock()
	defer asw.RUnlock()
	for _, volumeObj := range asw.attachedVolumes {
		if podObj, podExists := volumeObj.mountedPods[podName]; podExists {
			if podObj.volumeSpec.Name() == volumeSpecName {
				return true
			}
		}
	}
	return false
}

// VolumeExists 判断该卷是否存在于已挂载卷列表中。
func (asw *actualStateOfWorld) VolumeExists(
	volumeName v1.UniqueVolumeName) bool {
	asw.RLock()
	defer asw.RUnlock()

	_, volumeExists := asw.attachedVolumes[volumeName]
	return volumeExists
}

// GetMountedVolumes 返回所有已确认挂载的卷。
func (asw *actualStateOfWorld) GetMountedVolumes() []MountedVolume {
	asw.RLock()
	defer asw.RUnlock()
	mountedVolume := make([]MountedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		for _, podObj := range volumeObj.mountedPods {
			if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
				mountedVolume = append(
					mountedVolume,
					getMountedVolume(&podObj, &volumeObj))
			}
		}
	}
	return mountedVolume
}

// GetAllMountedVolumes 返回所有可能已为本 Pod 本地挂载的卷（含不确定状态）。
func (asw *actualStateOfWorld) GetAllMountedVolumes() []MountedVolume {
	asw.RLock()
	defer asw.RUnlock()
	mountedVolume := make([]MountedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		for _, podObj := range volumeObj.mountedPods {
			if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted ||
				podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain {
				mountedVolume = append(
					mountedVolume,
					getMountedVolume(&podObj, &volumeObj))
			}
		}
	}

	return mountedVolume
}

// GetMountedVolumesForPod 返回指定 Pod 已确认挂载的卷。
func (asw *actualStateOfWorld) GetMountedVolumesForPod(
	podName volumetypes.UniquePodName) []MountedVolume {
	asw.RLock()
	defer asw.RUnlock()
	mountedVolume := make([]MountedVolume, 0 /* len */)
	for _, volumeObj := range asw.attachedVolumes {
		for mountedPodName, podObj := range volumeObj.mountedPods {
			if mountedPodName == podName && podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
				mountedVolume = append(
					mountedVolume,
					getMountedVolume(&podObj, &volumeObj))
			}
		}
	}

	return mountedVolume
}

// GetMountedVolumeForPod 返回指定 Pod 上已确认挂载的给定卷。
func (asw *actualStateOfWorld) GetMountedVolumeForPod(
	podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) (MountedVolume, bool) {
	asw.RLock()
	defer asw.RUnlock()
	volumeObj := asw.attachedVolumes[volumeName]
	if podObj, hasPod := volumeObj.mountedPods[podName]; hasPod {
		if podObj.volumeMountStateForPod == operationexecutor.VolumeMounted {
			return getMountedVolume(&podObj, &volumeObj), true
		}
	}

	return MountedVolume{}, false
}

// GetPossiblyMountedVolumesForPod 返回指定 Pod 已挂载或可能已挂载（不确定）的卷。
func (asw *actualStateOfWorld) GetPossiblyMountedVolumesForPod(
	podName volumetypes.UniquePodName) []MountedVolume {
	asw.RLock()
	defer asw.RUnlock()
	mountedVolume := make([]MountedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		for mountedPodName, podObj := range volumeObj.mountedPods {
			if mountedPodName == podName &&
				(podObj.volumeMountStateForPod == operationexecutor.VolumeMounted ||
					podObj.volumeMountStateForPod == operationexecutor.VolumeMountUncertain) {
				mountedVolume = append(
					mountedVolume,
					getMountedVolume(&podObj, &volumeObj))
			}
		}
	}

	return mountedVolume
}

// GetGloballyMountedVolumes 返回所有已全局挂载的卷。
func (asw *actualStateOfWorld) GetGloballyMountedVolumes() []AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()
	globallyMountedVolumes := make(
		[]AttachedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		if volumeObj.deviceMountState == operationexecutor.DeviceGloballyMounted {
			globallyMountedVolumes = append(
				globallyMountedVolumes,
				asw.newAttachedVolume(&volumeObj))
		}
	}

	return globallyMountedVolumes
}

// GetAttachedVolumes 返回所有已挂载到本节点的卷。
func (asw *actualStateOfWorld) GetAttachedVolumes() []AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()
	allAttachedVolumes := make(
		[]AttachedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		allAttachedVolumes = append(
			allAttachedVolumes,
			asw.newAttachedVolume(&volumeObj))
	}

	return allAttachedVolumes
}

// GetAttachedVolume 返回给定卷名对应的已挂载卷。
func (asw *actualStateOfWorld) GetAttachedVolume(volumeName v1.UniqueVolumeName) (AttachedVolume, bool) {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, ok := asw.attachedVolumes[volumeName]
	if !ok {
		return AttachedVolume{}, false
	}

	return asw.newAttachedVolume(&volumeObj), true
}

// GetUnmountedVolumes 返回没有挂载任何 Pod 的已挂载卷。
func (asw *actualStateOfWorld) GetUnmountedVolumes() []AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()
	unmountedVolumes := make([]AttachedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		if len(volumeObj.mountedPods) == 0 {
			unmountedVolumes = append(
				unmountedVolumes,
				asw.newAttachedVolume(&volumeObj))
		}
	}

	return unmountedVolumes
}

// newAttachedVolume 由内部 attachedVolume 构造对外暴露的 AttachedVolume。
func (asw *actualStateOfWorld) newAttachedVolume(
	attachedVolume *attachedVolume) AttachedVolume {
	seLinuxMountContext := ""
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		if attachedVolume.seLinuxMountContext != nil {
			seLinuxMountContext = *attachedVolume.seLinuxMountContext
		}
	}
	return AttachedVolume{
		AttachedVolume: operationexecutor.AttachedVolume{
			VolumeName:          attachedVolume.volumeName,
			VolumeSpec:          attachedVolume.spec,
			NodeName:            asw.nodeName,
			PluginIsAttachable:  attachedVolume.pluginIsAttachable == volumeAttachabilityTrue,
			DevicePath:          attachedVolume.devicePath,
			DeviceMountPath:     attachedVolume.deviceMountPath,
			PluginName:          attachedVolume.pluginName,
			SELinuxMountContext: seLinuxMountContext},
		DeviceMountState:    attachedVolume.deviceMountState,
		SELinuxMountContext: seLinuxMountContext,
	}
}

// 编译期检查：确保 volumeNotAttachedError 实现 error 接口
var _ error = volumeNotAttachedError{}

// volumeNotAttachedError 是 PodExistsInVolume() 在已挂载卷列表中找不到指定卷时
// 返回的错误。
type volumeNotAttachedError struct {
	volumeName v1.UniqueVolumeName
}

// Error 实现 error 接口。
func (err volumeNotAttachedError) Error() string {
	return fmt.Sprintf(
		"volumeName %q does not exist in the list of attached volumes",
		err.volumeName)
}

// newVolumeNotAttachedError 构造 volumeNotAttachedError。
func newVolumeNotAttachedError(volumeName v1.UniqueVolumeName) error {
	return volumeNotAttachedError{
		volumeName: volumeName,
	}
}

// 编译期检查：确保 remountRequiredError 实现 error 接口
var _ error = remountRequiredError{}

// remountRequiredError 是 PodExistsInVolume() 发现卷/Pod 已挂载但 remountRequired
// 为 true 时返回的错误，表示为反映引用 Pod 的变更，该卷应重新挂载到该 Pod。
type remountRequiredError struct {
	volumeName v1.UniqueVolumeName
	podName    volumetypes.UniquePodName
}

// Error 实现 error 接口。
func (err remountRequiredError) Error() string {
	return fmt.Sprintf(
		"volumeName %q is mounted to %q but should be remounted",
		err.volumeName, err.podName)
}

// newRemountRequiredError 构造 remountRequiredError。
func newRemountRequiredError(
	volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) error {
	return remountRequiredError{
		volumeName: volumeName,
		podName:    podName,
	}
}

// FsResizeRequiredError 是 PodExistsInVolume() 发现卷/Pod 已挂载但文件系统需要
// 扩容时返回的错误，表示该卷在挂载后收到了 resize 请求。
type FsResizeRequiredError struct {
	CurrentSize resource.Quantity
	volumeName  v1.UniqueVolumeName
	podName     volumetypes.UniquePodName
}

// Error 实现 error 接口。
func (err FsResizeRequiredError) Error() string {
	return fmt.Sprintf(
		"volumeName %q mounted to %q needs to resize file system",
		err.volumeName, err.podName)
}

// newFsResizeRequiredError 构造 FsResizeRequiredError。
func newFsResizeRequiredError(
	volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName, currentSize resource.Quantity) error {
	return FsResizeRequiredError{
		CurrentSize: currentSize,
		volumeName:  volumeName,
		podName:     podName,
	}
}

// IsFSResizeRequiredError 判断指定错误是否为 FsResizeRequiredError。
func IsFSResizeRequiredError(err error) bool {
	_, ok := err.(FsResizeRequiredError)
	return ok
}

// getMountedVolume 由 mountedPod 和 attachedVolume 构造 MountedVolume。
func getMountedVolume(
	mountedPod *mountedPod, attachedVolume *attachedVolume) MountedVolume {
	seLinuxMountContext := ""
	if attachedVolume.seLinuxMountContext != nil {
		seLinuxMountContext = *attachedVolume.seLinuxMountContext
	}
	return MountedVolume{
		MountedVolume: operationexecutor.MountedVolume{
			PodName:             mountedPod.podName,
			VolumeName:          attachedVolume.volumeName,
			InnerVolumeSpecName: mountedPod.volumeSpec.Name(),
			PluginName:          attachedVolume.pluginName,
			PodUID:              mountedPod.podUID,
			Mounter:             mountedPod.mounter,
			BlockVolumeMapper:   mountedPod.blockVolumeMapper,
			VolumeGIDValue:      mountedPod.volumeGIDValue,
			VolumeSpec:          mountedPod.volumeSpec,
			DeviceMountPath:     attachedVolume.deviceMountPath,
			SELinuxMountContext: seLinuxMountContext}}

}

// seLinuxMountMismatchError 是 PodExistsInVolume() 发现卷的挂载 SELinux 标签与
// 期望不一致时返回的错误。
type seLinuxMountMismatchError struct {
	volumeName v1.UniqueVolumeName
}

// Error 实现 error 接口。
func (err seLinuxMountMismatchError) Error() string {
	return fmt.Sprintf(
		"waiting for unmount of volume %q, because it is already mounted to a different pod with a different SELinux label",
		err.volumeName)
}

// newSELinuxMountMismatchError 构造 seLinuxMountMismatchError。
func newSELinuxMountMismatchError(volumeName v1.UniqueVolumeName) error {
	return seLinuxMountMismatchError{
		volumeName: volumeName,
	}
}

// IsSELinuxMountMismatchError 判断指定错误是否为 seLinuxMountMismatchError。
func IsSELinuxMountMismatchError(err error) bool {
	_, ok := err.(seLinuxMountMismatchError)
	return ok
}
