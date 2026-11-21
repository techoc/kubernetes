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
Package cache 实现 attach/detach 控制器使用的数据结构，
用于跟踪卷、卷挂接到的节点以及引用这些卷的 Pod。
*/
package cache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
)

// ActualStateOfWorld 定义 attach/detach 控制器“实际世界状态”缓存支持的线程安全操作集合。
// 该缓存保存 卷->节点 映射，即控制器认为已成功挂接的所有卷及挂接节点。
// 注意：这与 kubelet 卷管理器实现的 ActualStateOfWorld 不同，两者跟踪的对象不同；
// 这里只保存 attach/detach 控制器特有的状态。
type ActualStateOfWorld interface {
	// 必须实现 operationexecutor 交互所需的方法。
	operationexecutor.ActualStateOfWorldAttacherUpdater

	// AddVolumeNode 将给定卷和节点加入底层存储。
	// attached 为 true 表示卷已确认挂接到该节点；为 false 表示尚未确认挂接。
	// 成功时返回由 volumeSpec 生成的唯一卷名。
	// 若 volumeSpec 不是可挂接卷插件，返回错误。
	// 若存储中不存在该卷则新增；若该卷的已挂接节点列表中不存在该节点则加入。
	AddVolumeNode(logger klog.Logger, uniqueName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName, devicePath string, attached bool) (v1.UniqueVolumeName, error)

	// SetVolumesMountedByNode 设置指定节点挂载的所有卷。
	// 这些卷包括已挂接、尚未挂接的卷，也可能包括不可挂接的卷。
	// 出现在 volumeNames 中表示该节点已挂载该卷，因而卸载可能不安全；否则表示未挂载。
	SetVolumesMountedByNode(logger klog.Logger, volumeNames []v1.UniqueVolumeName, nodeName types.NodeName)

	// SetNodeStatusUpdateNeeded 将指定节点的 statusUpdateNeeded 置为 true，
	// 表示节点状态对象中的 AttachedVolume 字段需要由节点状态更新器再次更新。
	// 若指定节点不在 nodesToUpdateStatusFor 列表中，记录错误并返回。
	SetNodeStatusUpdateNeeded(logger klog.Logger, nodeName types.NodeName)

	// ResetDetachRequestTime 将 detachRequestTime 重置为 0，表示该卷已无卸载请求。
	ResetDetachRequestTime(logger klog.Logger, volumeName v1.UniqueVolumeName, nodeName types.NodeName)

	// SetDetachRequestTime 在尚无卸载请求（上次请求时间为零）时，
	// 将 detachRequestedTime 设为当前时间，并返回距上次请求已过去的时间。
	SetDetachRequestTime(logger klog.Logger, volumeName v1.UniqueVolumeName, nodeName types.NodeName) (time.Duration, error)

	// DeleteVolumeNode 从底层存储移除给定卷和节点，表示该卷不再挂接到该节点。
	// 若该卷/节点组合不存在，则为空操作。
	// 若删除该节点后卷不再挂接任何节点，则同时删除该卷。
	DeleteVolumeNode(volumeName v1.UniqueVolumeName, nodeName types.NodeName)

	// GetAttachState 返回给定卷/节点组合的挂接状态。
	// 组合存在且已确认挂接时返回 AttachStateAttached；不存在时返回 AttachStateDetached；
	// 挂接状态不确定时返回 AttachStateUncertain。
	GetAttachState(volumeName v1.UniqueVolumeName, nodeName types.NodeName) AttachState

	// GetAttachedVolumes 生成并返回卷/节点对列表，反映当前实际状态中哪些卷可能挂接到了哪些节点。
	// 列表包含挂接成功的卷，也包含挂接过程中报错的卷。
	GetAttachedVolumes() []AttachedVolume

	// GetAttachedVolumesForNode 生成并返回加入指定节点的卷列表，
	// 反映当前实际状态中哪些卷已挂接或可能挂接到该节点。
	// 当前由 attach_detach_controller 用于处理 VolumeInUse。
	GetAttachedVolumesForNode(nodeName types.NodeName) []AttachedVolume

	// GetAttachedVolumesPerNode 生成并返回节点到卷的映射，
	// 反映当前实际状态中哪些卷已挂接到对应节点。
	// 当前由 reconciler 用于校验卷是否仍挂接在节点上。
	GetAttachedVolumesPerNode() map[types.NodeName][]operationexecutor.AttachedVolume

	// GetNodesForAttachedVolume 返回卷已挂接到的节点列表，供 reconciler 做多重挂接检查。
	GetNodesForAttachedVolume(volumeName v1.UniqueVolumeName) []types.NodeName

	// GetVolumesToReportAttached 返回一个映射，包含需要更新 Node API 对象
	// VolumesAttached 状态字段的节点集合。键为节点名，值为应上报为已挂接的卷列表。
	// 注意：由于一旦决定卸载就会（在触发卸载前）把卷从列表中移除，
	// 该列表可能与节点上实际挂接的卷列表不同。
	GetVolumesToReportAttached(logger klog.Logger) map[types.NodeName][]v1.AttachedVolume

	// GetVolumesToReportAttachedForNode 返回指定节点应上报为已挂接的卷列表，
	// 并给出该节点是否有更新以及相应的 attachedVolumes 列表。
	GetVolumesToReportAttachedForNode(logger klog.Logger, name types.NodeName) (bool, []v1.AttachedVolume)

	// GetNodesToUpdateStatusFor 返回节点名到 nodeToUpdateStatusFor 的映射。
	GetNodesToUpdateStatusFor() map[types.NodeName]nodeToUpdateStatusFor
}

// AttachedVolume 表示已挂接到某节点的卷。
type AttachedVolume struct {
	operationexecutor.AttachedVolume

	// MountedByNode 表示该卷已被节点挂载，因此卸载不安全。
	// 由 SetVolumesMountedByNode(...) 设置和清除。
	MountedByNode bool

	// DetachRequestedTime 记录卸载该卷的诉求。
	// 卷新建时该值为零；若此前为零，SetDetachRequestTime(...) 会将其设为当前时间
	// （否则保持不变）；ResetDetachRequestTime(...) 会将其重置为零。
	DetachRequestedTime time.Time
}

// AttachState 表示“实际世界状态”所知的卷到节点的挂接状态。
// 该类型仅作为挂接状态的外部表示（具体用作 GetAttachState 的返回值）；
// 内部缓存实现中状态的表示方式不同。
type AttachState int

const (
	// AttachStateAttached 表示卷已挂接到节点。
	AttachStateAttached AttachState = iota

	// AttachStateUncertain 表示“实际世界状态”无法确定卷是否已挂接到节点。
	AttachStateUncertain

	// AttachStateDetached 表示卷未挂接到节点。
	AttachStateDetached
)

// String 返回挂接状态的可读名称。
func (s AttachState) String() string {
	return []string{"Attached", "Uncertain", "Detached"}[s]
}

// NewActualStateOfWorld 创建并返回一个 ActualStateOfWorld 实例。
func NewActualStateOfWorld(volumePluginMgr *volume.VolumePluginMgr) ActualStateOfWorld {
	return &actualStateOfWorld{
		attachedVolumes:        make(map[v1.UniqueVolumeName]attachedVolume),
		nodesToUpdateStatusFor: make(map[types.NodeName]nodeToUpdateStatusFor),
		inUseVolumes:           make(map[types.NodeName]sets.Set[v1.UniqueVolumeName]),
		volumePluginMgr:        volumePluginMgr,
	}
}

// actualStateOfWorld 是 ActualStateOfWorld 接口的具体实现。
type actualStateOfWorld struct {
	// attachedVolumes 保存控制器认为已成功挂接到其管理节点的卷集合。
	// 键为卷名，值为包含已挂接卷详细信息的对象。
	attachedVolumes map[v1.UniqueVolumeName]attachedVolume

	// nodesToUpdateStatusFor 保存需要更新 VolumesAttached 状态字段的节点集合。
	// 键为节点名，值为节点详细信息（包括应上报为已挂接的卷列表）。
	nodesToUpdateStatusFor map[types.NodeName]nodeToUpdateStatusFor

	// inUseVolumes 保存 kubelet 上报为正在使用的卷集合。
	inUseVolumes map[types.NodeName]sets.Set[v1.UniqueVolumeName]

	// volumePluginMgr 用于创建卷插件对象。
	volumePluginMgr *volume.VolumePluginMgr

	sync.RWMutex
}

// attachedVolume 表示控制器认为已成功挂接到其所管理节点的卷。
type attachedVolume struct {
	// volumeName 是该卷的唯一标识。
	volumeName v1.UniqueVolumeName

	// spec 是卷的规格说明，用于生成卷插件对象并传给挂接/卸载方法。
	spec *volume.Spec

	// nodesAttachedTo 保存该卷已挂接到的节点集合。
	// 键为节点名，值为包含节点详细信息的对象。
	nodesAttachedTo map[types.NodeName]nodeAttachedTo

	// devicePath 是卷在节点上的挂接路径。
	devicePath string
}

// nodeAttachedTo 表示已挂接卷或正在尝试挂接卷的节点。
type nodeAttachedTo struct {
	// nodeName 是节点名。
	nodeName types.NodeName

	// attachedConfirmed 表示存储系统已确认卷挂接到该节点。
	// 挂接操作失败但卷可能已挂接时，该值为 false。
	attachedConfirmed bool

	// detachRequestedTime 记录卸载该卷的诉求。
	detachRequestedTime time.Time
}

// nodeToUpdateStatusFor 反映有一个或多个卷挂接的节点，
// 记录需要在 Node Status API 对象中上报为已挂接的卷。
type nodeToUpdateStatusFor struct {
	// nodeName 是节点名。
	nodeName types.NodeName

	// statusUpdateNeeded 表示 Node Status API 对象中的 VolumesAttached 字段需要更新。
	// 每当 volumesToReportAsAttached 增删卷时应置为 true，状态更新后重置。
	statusUpdateNeeded bool

	// volumesToReportAsAttached 是应在节点状态中上报为已挂接的卷列表。
	// 注意：由于一旦决定卸载就会（在触发卸载前）把卷从列表中移除，
	// 该列表可能与节点上实际挂接的卷列表不同。
	volumesToReportAsAttached map[v1.UniqueVolumeName]v1.UniqueVolumeName
}

// MarkVolumeAsUncertain 将卷标记为不确定挂接：加入缓存但不确认挂接成功，等待后续核实。
func (asw *actualStateOfWorld) MarkVolumeAsUncertain(
	logger klog.Logger,
	uniqueName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName) error {

	_, err := asw.AddVolumeNode(logger, uniqueName, volumeSpec, nodeName, "", false /* isAttached */)
	return err
}

// MarkVolumeAsAttached 将卷标记为已确认挂接到指定节点。
func (asw *actualStateOfWorld) MarkVolumeAsAttached(
	logger klog.Logger,
	uniqueName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName, devicePath string) error {
	_, err := asw.AddVolumeNode(logger, uniqueName, volumeSpec, nodeName, devicePath, true)
	return err
}

// MarkVolumeAsDetached 将卷标记为已从指定节点卸载。
func (asw *actualStateOfWorld) MarkVolumeAsDetached(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.DeleteVolumeNode(volumeName, nodeName)
}

// RemoveVolumeFromReportAsAttached 将卷从需上报为已挂接的列表中移除。
func (asw *actualStateOfWorld) RemoveVolumeFromReportAsAttached(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) error {
	asw.Lock()
	defer asw.Unlock()
	return asw.removeVolumeFromReportAsAttached(volumeName, nodeName)
}

// AddVolumeToReportAsAttached 将卷加入需上报为已挂接的列表。
func (asw *actualStateOfWorld) AddVolumeToReportAsAttached(
	logger klog.Logger,
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.Lock()
	defer asw.Unlock()
	asw.addVolumeToReportAsAttached(logger, volumeName, nodeName)
}

// AddVolumeNode 将卷/节点加入“实际世界状态”，必要时由 volumeSpec 生成唯一卷名。
func (asw *actualStateOfWorld) AddVolumeNode(
	logger klog.Logger,
	uniqueName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName, devicePath string, isAttached bool) (v1.UniqueVolumeName, error) {
	volumeName := uniqueName
	// 调用方未提供唯一卷名时，从卷规格推导，避免同名卷在不同插件下冲突。
	if volumeName == "" {
		if volumeSpec == nil {
			return volumeName, fmt.Errorf("volumeSpec cannot be nil if volumeName is empty")
		}
		attachableVolumePlugin, err := asw.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
		if err != nil || attachableVolumePlugin == nil {
			if attachableVolumePlugin == nil {
				err = fmt.Errorf("plugin do not support attachment")
			}
			return "", fmt.Errorf(
				"failed to get AttachablePlugin from volumeSpec for volume %q err=%v",
				volumeSpec.Name(),
				err)
		}

		volumeName, err = util.GetUniqueVolumeNameFromSpec(
			attachableVolumePlugin, volumeSpec)
		if err != nil {
			return "", fmt.Errorf(
				"failed to GetUniqueVolumeNameFromSpec for volumeSpec %q err=%v",
				volumeSpec.Name(),
				err)
		}
	}

	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		volumeObj = attachedVolume{
			volumeName:      volumeName,
			spec:            volumeSpec,
			nodesAttachedTo: make(map[types.NodeName]nodeAttachedTo),
			devicePath:      devicePath,
		}
	} else {
		// 卷对象已存在说明其信息可能过期；只刷新设备路径和规格，
		// 避免覆盖已有的挂接节点状态。
		volumeObj.devicePath = devicePath
		volumeObj.spec = volumeSpec
		logger.V(2).Info("Volume is already added to attachedVolume list to node, update device path",
			"volumeName", volumeName,
			"node", klog.KRef("", string(nodeName)),
			"devicePath", devicePath)
	}
	node, nodeExists := volumeObj.nodesAttachedTo[nodeName]
	if !nodeExists {
		// 节点记录不存在时新建。
		node = nodeAttachedTo{
			nodeName:            nodeName,
			attachedConfirmed:   isAttached,
			detachRequestedTime: time.Time{},
		}
		// 默认假定卷已被节点挂载，直到收到明确的反证；
		// 这样可避免在状态未明确前误判为可安全卸载。
		if asw.inUseVolumes[nodeName] == nil {
			asw.inUseVolumes[nodeName] = sets.New(volumeName)
		} else {
			asw.inUseVolumes[nodeName].Insert(volumeName)
		}
	} else {
		node.attachedConfirmed = isAttached
		logger.V(5).Info("Volume is already added to attachedVolume list to the node",
			"volumeName", volumeName,
			"node", klog.KRef("", string(nodeName)),
			"currentAttachState", isAttached)
	}

	volumeObj.nodesAttachedTo[nodeName] = node
	asw.attachedVolumes[volumeName] = volumeObj

	// 只有确认挂接后才需上报到节点状态，避免把不确定状态的卷误报为已挂接。
	if isAttached {
		asw.addVolumeToReportAsAttached(logger, volumeName, nodeName)
	}
	return volumeName, nil
}

// SetVolumesMountedByNode 用给定列表覆盖节点已挂载卷的集合，
// 以便判断哪些卷当前不可安全卸载。
func (asw *actualStateOfWorld) SetVolumesMountedByNode(
	logger klog.Logger, volumeNames []v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.Lock()
	defer asw.Unlock()

	asw.inUseVolumes[nodeName] = sets.New(volumeNames...)
	logger.V(5).Info("SetVolumesMountedByNode volume to the node",
		"node", klog.KRef("", string(nodeName)),
		"volumeNames", volumeNames)
}

// ResetDetachRequestTime 清除卷在该节点上的卸载请求时间。
func (asw *actualStateOfWorld) ResetDetachRequestTime(
	logger klog.Logger,
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, nodeObj, err := asw.getNodeAndVolume(volumeName, nodeName)
	if err != nil {
		logger.Error(err, "Failed to ResetDetachRequestTime with error")
		return
	}
	nodeObj.detachRequestedTime = time.Time{}
	volumeObj.nodesAttachedTo[nodeName] = nodeObj
}

// SetDetachRequestTime 记录首次卸载请求的时间，并返回自该请求以来的时长；
// 已有请求时不重置，以免反复请求导致等待时间被无限推迟。
func (asw *actualStateOfWorld) SetDetachRequestTime(
	logger klog.Logger,
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) (time.Duration, error) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, nodeObj, err := asw.getNodeAndVolume(volumeName, nodeName)
	if err != nil {
		return 0, fmt.Errorf("failed to set detach request time with error: %v", err)
	}
	// 首次卸载请求才记录时间；后续重复请求沿用原时间，保证限速计时不被重置。
	if nodeObj.detachRequestedTime.IsZero() {
		nodeObj.detachRequestedTime = time.Now()
		volumeObj.nodesAttachedTo[nodeName] = nodeObj
		logger.V(4).Info("Set detach request time to current time for volume on node",
			"node", klog.KRef("", string(nodeName)),
			"volumeName", volumeName)
	}
	return time.Since(nodeObj.detachRequestedTime), nil
}

// getNodeAndVolume 从“实际世界状态”中取出卷和节点对象；
// 内部函数，调用方需自行加锁和解锁。
//
// 注意：返回的是脱离原 map 的副本，修改卷对象后必须写回
// `asw.attachedVolumes[volumeName]=volumeObj`。
//
// 修改节点对象时必须写回 `volumeObj.nodesAttachedTo[nodeName] = nodeObj`。
// 这样做是正确的：卷对象为空时本函数会返回错误，且 nodesAttachedTo 是引用类型，
// 修改副本中的 map 即修改原 map。
func (asw *actualStateOfWorld) getNodeAndVolume(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) (attachedVolume, nodeAttachedTo, error) {

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if volumeExists {
		nodeObj, nodeExists := volumeObj.nodesAttachedTo[nodeName]
		if nodeExists {
			return volumeObj, nodeObj, nil
		}
	}

	return attachedVolume{}, nodeAttachedTo{}, fmt.Errorf("volume %v is no longer attached to the node %q",
		volumeName,
		nodeName)
}

// removeVolumeFromReportAsAttached 将卷从节点的 volumesToReportAsAttached 列表移除；
// 内部函数，调用方需自行加锁和解锁。
func (asw *actualStateOfWorld) removeVolumeFromReportAsAttached(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) error {

	nodeToUpdate, nodeToUpdateExists := asw.nodesToUpdateStatusFor[nodeName]
	if nodeToUpdateExists {
		_, nodeToUpdateVolumeExists :=
			nodeToUpdate.volumesToReportAsAttached[volumeName]
		if nodeToUpdateVolumeExists {
			// 列表变化后必须标记待更新，节点状态更新器才会推送新状态。
			nodeToUpdate.statusUpdateNeeded = true
			delete(nodeToUpdate.volumesToReportAsAttached, volumeName)
			asw.nodesToUpdateStatusFor[nodeName] = nodeToUpdate
			return nil
		}
	}
	return fmt.Errorf("volume %q does not exist in volumesToReportAsAttached list or node %q does not exist in nodesToUpdateStatusFor list",
		volumeName,
		nodeName)

}

// addVolumeToReportAsAttached 将卷加入节点的 volumesToReportAsAttached 列表；
// 内部函数，调用方需自行加锁和解锁。
func (asw *actualStateOfWorld) addVolumeToReportAsAttached(
	logger klog.Logger, volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	// 卷/节点条目可能已被并发移除；此时无需再上报，直接跳过。
	if _, _, err := asw.getNodeAndVolume(volumeName, nodeName); err != nil {
		logger.V(4).Info("Volume is no longer attached to node", "node", klog.KRef("", string(nodeName)), "volumeName", volumeName)
		return
	}
	nodeToUpdate, nodeToUpdateExists := asw.nodesToUpdateStatusFor[nodeName]
	if !nodeToUpdateExists {
		// 节点首次需要上报状态时创建对应记录。
		nodeToUpdate = nodeToUpdateStatusFor{
			nodeName:                  nodeName,
			statusUpdateNeeded:        true,
			volumesToReportAsAttached: make(map[v1.UniqueVolumeName]v1.UniqueVolumeName),
		}
		asw.nodesToUpdateStatusFor[nodeName] = nodeToUpdate
		logger.V(4).Info("Add new node to nodesToUpdateStatusFor", "node", klog.KRef("", string(nodeName)))
	}
	_, nodeToUpdateVolumeExists :=
		nodeToUpdate.volumesToReportAsAttached[volumeName]
	if !nodeToUpdateVolumeExists {
		// 仅在新加入卷时标记待更新，避免重复写相同卷触发无谓的状态更新。
		nodeToUpdate.statusUpdateNeeded = true
		nodeToUpdate.volumesToReportAsAttached[volumeName] = volumeName
		asw.nodesToUpdateStatusFor[nodeName] = nodeToUpdate
		logger.V(4).Info("Report volume as attached to node", "node", klog.KRef("", string(nodeName)), "volumeName", volumeName)
	}
}

// updateNodeStatusUpdateNeeded 更新 statusUpdateNeeded，
// 标记节点状态是否已更新或仍需由节点状态更新器再次更新；
// 内部函数，调用方需自行加锁和解锁。
// 若指定节点不在 nodesToUpdateStatusFor 列表中，返回错误。
func (asw *actualStateOfWorld) updateNodeStatusUpdateNeeded(nodeName types.NodeName, needed bool) error {
	nodeToUpdate, nodeToUpdateExists := asw.nodesToUpdateStatusFor[nodeName]
	if !nodeToUpdateExists {
		// 理论上不应发生：只有需要上报状态的节点才会进入该 map。
		errMsg := fmt.Sprintf("Failed to set statusUpdateNeeded to needed %t, because nodeName=%q does not exist",
			needed, nodeName)
		return errors.New(errMsg)
	}

	nodeToUpdate.statusUpdateNeeded = needed
	asw.nodesToUpdateStatusFor[nodeName] = nodeToUpdate

	return nil
}

// SetNodeStatusUpdateNeeded 标记指定节点的状态需要再次更新。
func (asw *actualStateOfWorld) SetNodeStatusUpdateNeeded(logger klog.Logger, nodeName types.NodeName) {
	asw.Lock()
	defer asw.Unlock()
	if err := asw.updateNodeStatusUpdateNeeded(nodeName, true); err != nil {
		logger.Info("Failed to update statusUpdateNeeded field in actual state of world", "err", err)
	}
}

// DeleteVolumeNode 删除卷与节点的挂接关系；卷不再挂接任何节点时一并删除卷记录。
func (asw *actualStateOfWorld) DeleteVolumeNode(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
	asw.Lock()
	defer asw.Unlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists {
		return
	}

	_, nodeExists := volumeObj.nodesAttachedTo[nodeName]
	if nodeExists {
		delete(asw.attachedVolumes[volumeName].nodesAttachedTo, nodeName)
	}

	// 卷已不挂接任何节点，保留空壳记录没有意义，直接删除。
	if len(volumeObj.nodesAttachedTo) == 0 {
		delete(asw.attachedVolumes, volumeName)
	}

	// 卸载已开始时就不再上报该卷为已挂接，避免状态滞后。
	asw.removeVolumeFromReportAsAttached(volumeName, nodeName)
}

// GetAttachState 返回卷/节点组合的挂接状态；未挂接返回 Detached，
// 存在但未确认返回 Uncertain，已确认返回 Attached。
func (asw *actualStateOfWorld) GetAttachState(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) AttachState {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if volumeExists {
		if node, nodeExists := volumeObj.nodesAttachedTo[nodeName]; nodeExists {
			if node.attachedConfirmed {
				return AttachStateAttached
			}
			return AttachStateUncertain
		}
	}

	return AttachStateDetached
}

// InitializeClaimSize 是空实现：attach/detach 控制器不设置卷声明大小，
// 仅为满足 operationexecutor 接口要求。
func (asw *actualStateOfWorld) InitializeClaimSize(logger klog.Logger, volumeName v1.UniqueVolumeName, claimSize resource.Quantity) {
	logger.V(5).Info("no-op InitializeClaimSize call in attach-detach controller")
}

// GetClaimSize 返回零值：attach/detach 控制器不关心卷声明大小。
func (asw *actualStateOfWorld) GetClaimSize(volumeName v1.UniqueVolumeName) resource.Quantity {
	// attach/detach 控制器不需要该信息。
	return resource.Quantity{}
}

// GetAttachedVolumes 返回当前所有卷/节点挂接关系，包括未确认挂接的卷。
func (asw *actualStateOfWorld) GetAttachedVolumes() []AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()

	attachedVolumes := make([]AttachedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		for _, nodeObj := range volumeObj.nodesAttachedTo {
			attachedVolumes = append(
				attachedVolumes,
				asw.getAttachedVolume(&volumeObj, &nodeObj))
		}
	}

	return attachedVolumes
}

// GetAttachedVolumesForNode 返回指定节点上已挂接或可能挂接的所有卷。
func (asw *actualStateOfWorld) GetAttachedVolumesForNode(
	nodeName types.NodeName) []AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()

	attachedVolumes := make(
		[]AttachedVolume, 0 /* len */, len(asw.attachedVolumes) /* cap */)
	for _, volumeObj := range asw.attachedVolumes {
		if nodeObj, nodeExists := volumeObj.nodesAttachedTo[nodeName]; nodeExists {
			attachedVolumes = append(
				attachedVolumes,
				asw.getAttachedVolume(&volumeObj, &nodeObj))
		}
	}

	return attachedVolumes
}

// GetAttachedVolumesPerNode 按节点分组返回已确认挂接的卷，
// 供 reconciler 校验挂接关系是否仍然有效。
func (asw *actualStateOfWorld) GetAttachedVolumesPerNode() map[types.NodeName][]operationexecutor.AttachedVolume {
	asw.RLock()
	defer asw.RUnlock()

	attachedVolumesPerNode := make(map[types.NodeName][]operationexecutor.AttachedVolume)
	for _, volumeObj := range asw.attachedVolumes {
		for nodeName, nodeObj := range volumeObj.nodesAttachedTo {
			// 只返回存储已确认的挂接，不确定状态不参与校验。
			if nodeObj.attachedConfirmed {
				volumes := attachedVolumesPerNode[nodeName]
				volumes = append(volumes, asw.getAttachedVolume(&volumeObj, &nodeObj).AttachedVolume)
				attachedVolumesPerNode[nodeName] = volumes
			}
		}
	}

	return attachedVolumesPerNode
}

// GetNodesForAttachedVolume 返回卷已确认挂接到的所有节点，供多重挂接检查使用。
func (asw *actualStateOfWorld) GetNodesForAttachedVolume(volumeName v1.UniqueVolumeName) []types.NodeName {
	asw.RLock()
	defer asw.RUnlock()

	volumeObj, volumeExists := asw.attachedVolumes[volumeName]
	if !volumeExists || len(volumeObj.nodesAttachedTo) == 0 {
		return []types.NodeName{}
	}

	nodes := []types.NodeName{}
	for nodeName, nodesAttached := range volumeObj.nodesAttachedTo {
		// 只统计已确认挂接的节点，避免误判多重挂接。
		if nodesAttached.attachedConfirmed {
			nodes = append(nodes, nodeName)
		}
	}
	return nodes
}

// GetVolumesToReportAttached 返回各节点需上报的已挂接卷列表，
// 并在读取后清除待更新标记，表示状态已交给更新器处理。
func (asw *actualStateOfWorld) GetVolumesToReportAttached(logger klog.Logger) map[types.NodeName][]v1.AttachedVolume {
	asw.Lock()
	defer asw.Unlock()

	volumesToReportAttached := make(map[types.NodeName][]v1.AttachedVolume)
	for nodeName, nodeToUpdateObj := range asw.nodesToUpdateStatusFor {
		if nodeToUpdateObj.statusUpdateNeeded {
			volumesToReportAttached[nodeToUpdateObj.nodeName] = asw.getAttachedVolumeFromUpdateObject(nodeToUpdateObj.volumesToReportAsAttached)
		}
		// 节点状态更新器读取后立即清除待更新标记，
		// 避免同一状态被重复推送；期间的新变化会重新置位。
		if err := asw.updateNodeStatusUpdateNeeded(nodeName, false); err != nil {
			logger.Error(err, "Failed to update statusUpdateNeeded field when getting volumes")
		}
	}

	return volumesToReportAttached
}

// GetVolumesToReportAttachedForNode 返回单个节点需上报的已挂接卷列表，
// 布尔值表示是否确有状态更新，读取后清除待更新标记。
func (asw *actualStateOfWorld) GetVolumesToReportAttachedForNode(logger klog.Logger, nodeName types.NodeName) (bool, []v1.AttachedVolume) {
	asw.Lock()
	defer asw.Unlock()

	nodeToUpdateObj, ok := asw.nodesToUpdateStatusFor[nodeName]
	if !ok {
		return false, nil
	}
	if !nodeToUpdateObj.statusUpdateNeeded {
		return false, nil
	}

	volumesToReportAttached := asw.getAttachedVolumeFromUpdateObject(nodeToUpdateObj.volumesToReportAsAttached)
	// 状态已被读取并即将更新，清除标记避免重复推送。
	if err := asw.updateNodeStatusUpdateNeeded(nodeName, false); err != nil {
		logger.Error(err, "Failed to update statusUpdateNeeded field when getting volumes")
	}

	return true, volumesToReportAttached
}

// GetNodesToUpdateStatusFor 返回所有需要更新状态的节点记录。
func (asw *actualStateOfWorld) GetNodesToUpdateStatusFor() map[types.NodeName]nodeToUpdateStatusFor {
	return asw.nodesToUpdateStatusFor
}

// getAttachedVolumeFromUpdateObject 将待上报卷名列表转换为带设备路径的 API 卷对象。
func (asw *actualStateOfWorld) getAttachedVolumeFromUpdateObject(volumesToReportAsAttached map[v1.UniqueVolumeName]v1.UniqueVolumeName) []v1.AttachedVolume {
	var attachedVolumes = make(
		[]v1.AttachedVolume,
		0,
		len(volumesToReportAsAttached) /* len */)
	for _, volume := range volumesToReportAsAttached {
		attachedVolumes = append(attachedVolumes,
			v1.AttachedVolume{
				Name:       volume,
				DevicePath: asw.attachedVolumes[volume].devicePath,
			})
	}
	return attachedVolumes
}

// getAttachedVolume 根据内部缓存记录组装对外暴露的 AttachedVolume，
// 挂载状态和卸载请求时间也一并带上。
func (asw *actualStateOfWorld) getAttachedVolume(
	attachedVolume *attachedVolume,
	nodeAttachedTo *nodeAttachedTo) AttachedVolume {
	return AttachedVolume{
		AttachedVolume: operationexecutor.AttachedVolume{
			VolumeName:         attachedVolume.volumeName,
			VolumeSpec:         attachedVolume.spec,
			NodeName:           nodeAttachedTo.nodeName,
			DevicePath:         attachedVolume.devicePath,
			PluginIsAttachable: true,
		},
		MountedByNode:       asw.inUseVolumes[nodeAttachedTo.nodeName].Has(attachedVolume.volumeName),
		DetachRequestedTime: nodeAttachedTo.detachRequestedTime}
}
