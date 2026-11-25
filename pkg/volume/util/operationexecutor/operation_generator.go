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

package operationexecutor

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	volerr "k8s.io/cloud-provider/volume/errors"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	kevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

const (
	unknownVolumePlugin           string = "UnknownVolumePlugin"
	unknownAttachableVolumePlugin string = "UnknownAttachableVolumePlugin"

	// DetachOperationName 是卷分离操作的名称，用于指标标签和日志关联。
	DetachOperationName string = "volume_detach"

	// VerifyControllerAttachedVolumeOpName 是校验控制器附加状态的操作名。
	VerifyControllerAttachedVolumeOpName string = "verify_controller_attached_volume"
)

var _ OperationGenerator = &operationGenerator{}

// operationGenerator 生成 kubelet 的卷操作函数。
type operationGenerator struct {
	// kubeClient 用于从 API 服务器获取对象，例如 VerifyControllerAttachedVolume 中的 Node。
	kubeClient clientset.Interface

	// volumePluginMgr 用于创建卷插件对象。
	volumePluginMgr *volume.VolumePluginMgr

	// recorder 向 API 服务器记录事件。
	recorder record.EventRecorder

	// blkUtil 提供块设备卷的路径相关操作。
	blkUtil volumepathhandler.BlockVolumePathHandler
}

// inTreeResizeResponse 记录树内插件扩容的结果。
type inTreeResizeResponse struct {
	pvc *v1.PersistentVolumeClaim
	pv  *v1.PersistentVolume

	err error
	// 是否调用了底层卷驱动的扩容操作，主要用于测试。
	resizeCalled bool
}

// NewOperationGenerator 返回 operationGenerator 实例。
func NewOperationGenerator(kubeClient clientset.Interface,
	volumePluginMgr *volume.VolumePluginMgr,
	recorder record.EventRecorder,
	blkUtil volumepathhandler.BlockVolumePathHandler) OperationGenerator {

	util.RegisterMetrics()
	return &operationGenerator{
		kubeClient:      kubeClient,
		volumePluginMgr: volumePluginMgr,
		recorder:        recorder,
		blkUtil:         blkUtil,
	}
}

// OperationGenerator 把 operation_executor 中的函数抽象为接口，便于依赖注入。
type OperationGenerator interface {
	// GenerateMountVolumeFunc 生成执行卷插件挂载所需的 MountVolume 函数。
	GenerateMountVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater, isRemount bool) volumetypes.GeneratedOperations

	// GenerateUnmountVolumeFunc 生成卸载卷插件所需的 UnmountVolume 函数。
	GenerateUnmountVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, podsDir string) (volumetypes.GeneratedOperations, error)

	// GenerateAttachVolumeFunc 生成挂载卷插件到节点所需的 AttachVolume 函数。
	GenerateAttachVolumeFunc(logger klog.Logger, volumeToAttach VolumeToAttach, actualStateOfWorld ActualStateOfWorldAttacherUpdater) volumetypes.GeneratedOperations

	// GenerateDetachVolumeFunc 生成从节点分离卷插件所需的 DetachVolume 函数。
	GenerateDetachVolumeFunc(logger klog.Logger, volumeToDetach AttachedVolume, verifySafeToDetach bool, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error)

	// GenerateVolumesAreAttachedFunc 生成校验卷插件是否仍挂载在节点上所需的函数。
	GenerateVolumesAreAttachedFunc(attachedVolumes []AttachedVolume, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error)

	// GenerateUnmountDeviceFunc 生成卸载设备所需的 UnMountDevice 函数。
	GenerateUnmountDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, mounter hostutil.HostUtils) (volumetypes.GeneratedOperations, error)

	// GenerateVerifyControllerAttachedVolumeFunc 生成检查 attach_detach 控制器是否已挂载卷所需的函数。
	GenerateVerifyControllerAttachedVolumeFunc(logger klog.Logger, volumeToMount VolumeToMount, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error)

	// GenerateMapVolumeFunc 生成映射块设备卷所需的 MapVolume 函数。
	GenerateMapVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error)

	// GenerateUnmapVolumeFunc 生成解除块设备卷映射所需的 UnmapVolume 函数。
	GenerateUnmapVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error)

	// GenerateUnmapDeviceFunc 生成解除设备映射所需的 UnmapDevice 函数。
	GenerateUnmapDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, mounter hostutil.HostUtils) (volumetypes.GeneratedOperations, error)

	// GetVolumePluginMgr 返回卷插件管理器。
	GetVolumePluginMgr() *volume.VolumePluginMgr

	// GenerateExpandVolumeFunc 生成卷扩容函数。
	GenerateExpandVolumeFunc(*v1.PersistentVolumeClaim, *v1.PersistentVolume) (volumetypes.GeneratedOperations, error)

	// GenerateExpandAndRecoverVolumeFunc 生成带恢复流程的卷扩容函数。
	GenerateExpandAndRecoverVolumeFunc(*v1.PersistentVolumeClaim, *v1.PersistentVolume, string) (volumetypes.GeneratedOperations, error)

	// GenerateExpandInUseVolumeFunc 生成在线文件系统扩容函数，无需卸载卷即可扩容到目标大小。
	// 除 volumeToMount 与 actualStateOfWorld 外，还需传入节点上卷的当前大小；
	// 该大小取自 actualStateOfWorld 中记录的 pvc.Status.Capacity。
	GenerateExpandInUseVolumeFunc(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) (volumetypes.GeneratedOperations, error)
}

// inTreeResizeOpts 封装树内卷扩容所需的参数。
type inTreeResizeOpts struct {
	resizerName  string
	pvc          *v1.PersistentVolumeClaim
	pv           *v1.PersistentVolume
	volumeSpec   *volume.Spec
	volumePlugin volume.ExpandableVolumePlugin
}

// nodeResizeOperationOpts 封装节点侧卷扩容所需的参数。
type nodeResizeOperationOpts struct {
	vmt                VolumeToMount
	pvc                *v1.PersistentVolumeClaim
	pv                 *v1.PersistentVolume
	pluginResizeOpts   volume.NodeResizeOptions
	volumePlugin       volume.NodeExpandableVolumePlugin
	actualStateOfWorld ActualStateOfWorldMounterUpdater
}

// GenerateVolumesAreAttachedFunc 生成按插件批量核对节点附加状态的操作。
// 规格或插件不可用、核对失败时仅记录日志并跳过；仅确认未附加的卷会被标记为已分离。
func (og *operationGenerator) GenerateVolumesAreAttachedFunc(
	attachedVolumes []AttachedVolume,
	nodeName types.NodeName,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	// volumesPerPlugin 按插件名聚合卷规格，便于按插件批量检查。
	volumesPerPlugin := make(map[string][]*volume.Spec)
	// volumeSpecMap 保存卷规格到唯一卷名的映射，供 MarkVolumeAsDetached 使用。
	volumeSpecMap := make(map[*volume.Spec]v1.UniqueVolumeName)

	// 遍历卷规格，按插件名归类。
	for _, volumeAttached := range attachedVolumes {
		if volumeAttached.VolumeSpec == nil {
			klog.Errorf("VerifyVolumesAreAttached.GenerateVolumesAreAttachedFunc: nil spec for volume %s", volumeAttached.VolumeName)
			continue
		}
		volumePlugin, err :=
			og.volumePluginMgr.FindPluginBySpec(volumeAttached.VolumeSpec)
		if err != nil || volumePlugin == nil {
			klog.Error(volumeAttached.GenerateErrorDetailed("VolumesAreAttached.FindPluginBySpec failed", err).Error())
			continue
		}
		volumeSpecList, pluginExists := volumesPerPlugin[volumePlugin.GetPluginName()]
		if !pluginExists {
			volumeSpecList = []*volume.Spec{}
		}
		volumeSpecList = append(volumeSpecList, volumeAttached.VolumeSpec)
		volumesPerPlugin[volumePlugin.GetPluginName()] = volumeSpecList
		// 迁移场景：保留原始卷名，供 ActualStateOfWorld 使用。
		volumeSpecMap[volumeAttached.VolumeSpec] = volumeAttached.VolumeName
	}

	volumesAreAttachedFunc := func() volumetypes.OperationContext {

		// 对每个插件批量调用 VolumesAreAttached，检查卷是否仍挂载在节点上。
		for pluginName, volumesSpecs := range volumesPerPlugin {
			attachableVolumePlugin, err :=
				og.volumePluginMgr.FindAttachablePluginByName(pluginName)
			if err != nil || attachableVolumePlugin == nil {
				klog.Errorf(
					"VolumeAreAttached.FindAttachablePluginBySpec failed for plugin %q with: %v",
					pluginName,
					err)
				continue
			}

			volumeAttacher, newAttacherErr := attachableVolumePlugin.NewAttacher()
			if newAttacherErr != nil {
				klog.Errorf(
					"VolumesAreAttached.NewAttacher failed for getting plugin %q with: %v",
					pluginName,
					newAttacherErr)
				continue
			}

			attached, areAttachedErr := volumeAttacher.VolumesAreAttached(volumesSpecs, nodeName)
			if areAttachedErr != nil {
				klog.Errorf(
					"VolumesAreAttached failed for checking on node %q with: %v",
					nodeName,
					areAttachedErr)
				continue
			}

			for spec, check := range attached {
				if !check {
					actualStateOfWorld.MarkVolumeAsDetached(volumeSpecMap[spec], nodeName)
					klog.V(1).Infof("VerifyVolumesAreAttached determined volume %q (spec.Name: %q) is no longer attached to node %q, therefore it was marked as detached.",
						volumeSpecMap[spec], spec.Name(), nodeName)
				}
			}
		}

		// verify_volumes_are_attached_per_node 难以逐个区分卷的迁移状态，统一按非迁移处理。
		return volumetypes.NewOperationContext(nil, nil, false)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "verify_volumes_are_attached_per_node",
		OperationFunc:     volumesAreAttachedFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume("<n/a>", nil), "verify_volumes_are_attached_per_node"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateAttachVolumeFunc 生成将卷挂载到节点的 AttachVolume 操作。
func (og *operationGenerator) GenerateAttachVolumeFunc(
	logger klog.Logger,
	volumeToAttach VolumeToAttach,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) volumetypes.GeneratedOperations {

	attachVolumeFunc := func() volumetypes.OperationContext {
		attachableVolumePlugin, err :=
			og.volumePluginMgr.FindAttachablePluginBySpec(volumeToAttach.VolumeSpec)

		migrated := getMigratedStatusBySpec(volumeToAttach.VolumeSpec)

		if err != nil || attachableVolumePlugin == nil {
			eventErr, detailedErr := volumeToAttach.GenerateError("AttachVolume.FindAttachablePluginBySpec failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		volumeAttacher, newAttacherErr := attachableVolumePlugin.NewAttacher()
		if newAttacherErr != nil {
			eventErr, detailedErr := volumeToAttach.GenerateError("AttachVolume.NewAttacher failed", newAttacherErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 执行挂载。
		devicePath, attachErr := volumeAttacher.Attach(
			volumeToAttach.VolumeSpec, volumeToAttach.NodeName)

		if attachErr != nil {
			uncertainNode := volumeToAttach.NodeName
			if derr, ok := attachErr.(*volerr.DanglingAttachError); ok {
				uncertainNode = derr.CurrentNode
			}
			addErr := actualStateOfWorld.MarkVolumeAsUncertain(
				logger,
				volumeToAttach.VolumeName,
				volumeToAttach.VolumeSpec,
				uncertainNode)
			if addErr != nil {
				klog.Errorf("AttachVolume.MarkVolumeAsUncertain fail to add the volume %q to actual state with %s", volumeToAttach.VolumeName, addErr)
			}

			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToAttach.GenerateError("AttachVolume.Attach failed", attachErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 更新实际状态。
		addVolumeNodeErr := actualStateOfWorld.MarkVolumeAsAttached(
			logger, volumeToAttach.VolumeName, volumeToAttach.VolumeSpec, volumeToAttach.NodeName, devicePath)
		if addVolumeNodeErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToAttach.GenerateError("AttachVolume.MarkVolumeAsAttached failed", addVolumeNodeErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 挂载成功事件便于用户排查问题。
		simpleMsg, _ := volumeToAttach.GenerateMsg("AttachVolume.Attach succeeded", "")
		for _, pod := range volumeToAttach.ScheduledPods {
			og.recorder.Eventf(pod, v1.EventTypeNormal, kevents.SuccessfulAttachVolume, "%s", simpleMsg)
		}
		klog.Info(volumeToAttach.GenerateMsgDetailed("AttachVolume.Attach succeeded", ""))

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			for _, pod := range volumeToAttach.ScheduledPods {
				og.recorder.Eventf(pod, v1.EventTypeWarning, kevents.FailedAttachVolume, "%s", (*err).Error())
			}
		}
	}

	attachableVolumePluginName := unknownAttachableVolumePlugin

	// 获取可挂载插件。
	attachableVolumePlugin, err :=
		og.volumePluginMgr.FindAttachablePluginBySpec(volumeToAttach.VolumeSpec)
	// 此处可忽略错误：生成阶段不会返回错误。生成时跳过的错误在执行生成函数时同样会触发，
	// 并在执行阶段通过退避策略处理。
	if err == nil && attachableVolumePlugin != nil {
		attachableVolumePluginName = attachableVolumePlugin.GetPluginName()
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "volume_attach",
		OperationFunc:     attachVolumeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(attachableVolumePluginName, volumeToAttach.VolumeSpec), "volume_attach"),
	}
}

// GetVolumePluginMgr 返回卷插件管理器。
func (og *operationGenerator) GetVolumePluginMgr() *volume.VolumePluginMgr {
	return og.volumePluginMgr
}

// GenerateDetachVolumeFunc 生成从节点分离卷的 DetachVolume 操作。
func (og *operationGenerator) GenerateDetachVolumeFunc(
	logger klog.Logger,
	volumeToDetach AttachedVolume,
	verifySafeToDetach bool,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	var volumeName string
	var attachableVolumePlugin volume.AttachableVolumePlugin
	var pluginName string
	var err error

	if volumeToDetach.VolumeSpec != nil {
		attachableVolumePlugin, err = findDetachablePluginBySpec(volumeToDetach.VolumeSpec, og.volumePluginMgr)
		if err != nil || attachableVolumePlugin == nil {
			return volumetypes.GeneratedOperations{}, volumeToDetach.GenerateErrorDetailed("DetachVolume.findDetachablePluginBySpec failed", err)
		}

		volumeName, err =
			attachableVolumePlugin.GetVolumeName(volumeToDetach.VolumeSpec)
		if err != nil {
			return volumetypes.GeneratedOperations{}, volumeToDetach.GenerateErrorDetailed("DetachVolume.GetVolumeName failed", err)
		}
	} else {
		// 没有 VolumeSpec 时只能拆分唯一卷名来找到插件：这种情况仅见于 attach/detach
		// 控制器崩溃恢复——控制器停机期间 Pod 已被删除。
		pluginName, volumeName, err = util.SplitUniqueName(volumeToDetach.VolumeName)
		if err != nil {
			return volumetypes.GeneratedOperations{}, volumeToDetach.GenerateErrorDetailed("DetachVolume.SplitUniqueName failed", err)
		}

		attachableVolumePlugin, err = og.volumePluginMgr.FindAttachablePluginByName(pluginName)
		if err != nil || attachableVolumePlugin == nil {
			return volumetypes.GeneratedOperations{}, volumeToDetach.GenerateErrorDetailed("DetachVolume.FindAttachablePluginByName failed", err)
		}

	}

	if pluginName == "" {
		pluginName = attachableVolumePlugin.GetPluginName()
	}

	volumeDetacher, err := attachableVolumePlugin.NewDetacher()
	if err != nil {
		return volumetypes.GeneratedOperations{}, volumeToDetach.GenerateErrorDetailed("DetachVolume.NewDetacher failed", err)
	}

	detachVolumeFunc := func() volumetypes.OperationContext {
		var err error
		if verifySafeToDetach {
			err = og.verifyVolumeIsSafeToDetach(volumeToDetach)
		}
		if err == nil {
			err = volumeDetacher.Detach(volumeName, volumeToDetach.NodeName)
		}

		migrated := getMigratedStatusBySpec(volumeToDetach.VolumeSpec)

		if err != nil {
			// 失败时将卷标记为状态不确定；必须等 Attach() 成功才能重新记为已挂载。
			uncertainError := actualStateOfWorld.MarkVolumeAsUncertain(
				logger, volumeToDetach.VolumeName, volumeToDetach.VolumeSpec, volumeToDetach.NodeName)
			if uncertainError != nil {
				klog.Errorf("DetachVolume.MarkVolumeAsUncertain failed to add the volume %q to actual state after detach error: %s", volumeToDetach.VolumeName, uncertainError)
			}
			eventErr, detailedErr := volumeToDetach.GenerateError("DetachVolume.Detach failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		klog.Info(volumeToDetach.GenerateMsgDetailed("DetachVolume.Detach succeeded", ""))

		// 更新实际状态。
		actualStateOfWorld.MarkVolumeAsDetached(
			volumeToDetach.VolumeName, volumeToDetach.NodeName)

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     DetachOperationName,
		OperationFunc:     detachVolumeFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(pluginName, volumeToDetach.VolumeSpec), DetachOperationName),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateMountVolumeFunc 生成挂载卷到 Pod 的 MountVolume 操作。
func (og *operationGenerator) GenerateMountVolumeFunc(
	waitForAttachTimeout time.Duration,
	volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	isRemount bool) volumetypes.GeneratedOperations {

	volumePluginName := unknownVolumePlugin
	volumePlugin, err :=
		og.volumePluginMgr.FindPluginBySpec(volumeToMount.VolumeSpec)
	if err == nil && volumePlugin != nil {
		volumePluginName = volumePlugin.GetPluginName()
	}

	mountVolumeFunc := func() volumetypes.OperationContext {
		// 获取挂载插件。
		volumePlugin, err := og.volumePluginMgr.FindPluginBySpec(volumeToMount.VolumeSpec)

		migrated := getMigratedStatusBySpec(volumeToMount.VolumeSpec)

		if err != nil || volumePlugin == nil {
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.FindPluginBySpec failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		affinityErr := checkNodeAffinity(og, volumeToMount)
		if affinityErr != nil {
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.NodeAffinity check failed", affinityErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		volumeMounter, newMounterErr := volumePlugin.NewMounter(
			volumeToMount.VolumeSpec,
			volumeToMount.Pod)
		if newMounterErr != nil {
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.NewMounter initialization failed", newMounterErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		mountCheckError := checkMountOptionSupport(og, volumeToMount, volumePlugin)
		if mountCheckError != nil {
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.MountOptionSupport check failed", mountCheckError)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 若访问模式仅有 ReadWriteOncePod，则在此强制校验；调度阶段也会校验。
		if actualStateOfWorld.IsVolumeMountedElsewhere(volumeToMount.VolumeName, volumeToMount.PodName) &&
			// 多个访问模式时无法判断 Pod 的意图，故只处理单一模式的情形。
			len(volumeToMount.VolumeSpec.PersistentVolume.Spec.AccessModes) == 1 &&
			v1helper.ContainsAccessMode(volumeToMount.VolumeSpec.PersistentVolume.Spec.AccessModes, v1.ReadWriteOncePod) {

			err = goerrors.New("volume uses the ReadWriteOncePod access mode and is already in use by another pod")
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.SetUp failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 尽量获取挂载器；并非所有卷都需要 Attach。
		attachableVolumePlugin, _ :=
			og.volumePluginMgr.FindAttachablePluginBySpec(volumeToMount.VolumeSpec)
		var volumeAttacher volume.Attacher
		if attachableVolumePlugin != nil {
			volumeAttacher, _ = attachableVolumePlugin.NewAttacher()
		}

		// 尽量获取设备挂载器；并非所有卷都需要在全局路径挂载设备。
		deviceMountableVolumePlugin, _ := og.volumePluginMgr.FindDeviceMountablePluginBySpec(volumeToMount.VolumeSpec)
		var volumeDeviceMounter volume.DeviceMounter
		if deviceMountableVolumePlugin != nil {
			volumeDeviceMounter, _ = deviceMountableVolumePlugin.NewDeviceMounter()
		}

		var fsGroup *int64
		var fsGroupChangePolicy *v1.PodFSGroupChangePolicy
		if podSc := volumeToMount.Pod.Spec.SecurityContext; podSc != nil {
			if podSc.FSGroup != nil {
				fsGroup = podSc.FSGroup
			}
			if podSc.FSGroupChangePolicy != nil {
				fsGroupChangePolicy = podSc.FSGroupChangePolicy
			}
		}

		devicePath := volumeToMount.DevicePath
		if volumeAttacher != nil {
			// 等待可挂载卷完成 Attach。
			klog.InfoS(volumeToMount.GenerateMsgDetailed("MountVolume.WaitForAttach entering", fmt.Sprintf("DevicePath %q", volumeToMount.DevicePath)), "pod", klog.KObj(volumeToMount.Pod))

			devicePath, err = volumeAttacher.WaitForAttach(
				volumeToMount.VolumeSpec, devicePath, volumeToMount.Pod, waitForAttachTimeout)
			if err != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.WaitForAttach failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			klog.InfoS(volumeToMount.GenerateMsgDetailed("MountVolume.WaitForAttach succeeded", fmt.Sprintf("DevicePath %q", devicePath)), "pod", klog.KObj(volumeToMount.Pod))
		}

		var resizeError error
		resizeOptions := volume.NodeResizeOptions{
			DevicePath: devicePath,
		}

		if volumeDeviceMounter != nil && actualStateOfWorld.GetDeviceMountState(volumeToMount.VolumeName) != DeviceGloballyMounted {
			deviceMountPath, err :=
				volumeDeviceMounter.GetDeviceMountPath(volumeToMount.VolumeSpec)
			if err != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.GetDeviceMountPath failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			// 将设备挂载到全局挂载路径。
			og.markVolumeMountAsAttempted(actualStateOfWorld, volumeToMount.PodName, volumeToMount.VolumeName)
			err = volumeDeviceMounter.MountDevice(
				volumeToMount.VolumeSpec,
				devicePath,
				deviceMountPath,
				volume.DeviceMounterArgs{FsGroup: fsGroup, SELinuxLabel: volumeToMount.SELinuxLabel},
			)
			if err != nil {
				og.checkForFailedMount(volumeToMount, err)
				og.markDeviceErrorState(volumeToMount, devicePath, deviceMountPath, err, actualStateOfWorld)
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.MountDevice failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			klog.InfoS(volumeToMount.GenerateMsgDetailed("MountVolume.MountDevice succeeded", fmt.Sprintf("device mount path %q", deviceMountPath)), "pod", klog.KObj(volumeToMount.Pod))

			// 更新实际状态，记录卷已全局挂载。
			markDeviceMountedErr := actualStateOfWorld.MarkDeviceAsMounted(
				volumeToMount.VolumeName, devicePath, deviceMountPath, volumeToMount.SELinuxLabel)
			if markDeviceMountedErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.MarkDeviceAsMounted failed", markDeviceMountedErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
			// 记录 staging 路径，供后续卷扩容使用。
			resizeOptions.DeviceStagePath = deviceMountPath
		}

		if volumeDeviceMounter != nil && resizeOptions.DeviceStagePath == "" {
			deviceStagePath, err := volumeDeviceMounter.GetDeviceMountPath(volumeToMount.VolumeSpec)
			if err != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.GetDeviceMountPath failed for expansion", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
			resizeOptions.DeviceStagePath = deviceStagePath
		}

		// 执行挂载。
		og.markVolumeMountAsAttempted(actualStateOfWorld, volumeToMount.PodName, volumeToMount.VolumeName)
		mountErr := volumeMounter.SetUp(volume.MounterArgs{
			FsUser:              util.FsUserFrom(volumeToMount.Pod),
			FsGroup:             fsGroup,
			DesiredSize:         volumeToMount.DesiredSizeLimit,
			FSGroupChangePolicy: fsGroupChangePolicy,
			Recorder:            og.recorder,
			SELinuxLabel:        volumeToMount.SELinuxLabel,
			ReconstructedVolume: actualStateOfWorld.IsVolumeReconstructed(volumeToMount.VolumeName, volumeToMount.PodName),
			IsRemount:           isRemount,
		})
		// 更新实际状态。
		markOpts := MarkVolumeOpts{
			PodName:             volumeToMount.PodName,
			PodUID:              volumeToMount.Pod.UID,
			VolumeName:          volumeToMount.VolumeName,
			Mounter:             volumeMounter,
			VolumeGIDVolume:     volumeToMount.VolumeGIDValue,
			VolumeSpec:          volumeToMount.VolumeSpec,
			VolumeMountState:    VolumeMounted,
			SELinuxMountContext: volumeToMount.SELinuxLabel,
		}
		if mountErr != nil {
			og.checkForFailedMount(volumeToMount, mountErr)
			og.markVolumeErrorState(volumeToMount, markOpts, mountErr, actualStateOfWorld)
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.SetUp failed", mountErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		detailedMsg := volumeToMount.GenerateMsgDetailed("MountVolume.SetUp succeeded", "")
		verbosity := klog.Level(1)
		if isRemount {
			verbosity = klog.Level(4)
		}
		klog.V(verbosity).InfoS(detailedMsg, "pod", klog.KObj(volumeToMount.Pod))
		resizeOptions.DeviceMountPath = volumeMounter.GetPath()

		_, resizeError = og.expandVolumeDuringMount(volumeToMount, actualStateOfWorld, resizeOptions)
		if resizeError != nil {
			klog.Errorf("MountVolume.NodeExpandVolume failed with %v", resizeError)
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.Setup failed while expanding volume", resizeError)
			// SetUp 已成功，需让协调器能在必要时清理卷；但扩容失败又不能标记为已挂载，
			// 否则 Pod 会开始使用它。故标记为不确定：Pod 删除时触发清理，同时阻止其启动使用。
			if err := actualStateOfWorld.MarkVolumeMountAsUncertain(markOpts); err != nil {
				klog.Error(volumeToMount.GenerateErrorDetailed("MountVolume.MarkVolumeMountAsUncertain failed", err).Error())
			}
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 记录挂载耗时，含等待 Attach、节点状态更新及插件调用成功的端到端时间。
		mountRequestTime := volumeToMount.MountRequestTime
		totalTimeTaken := time.Since(mountRequestTime).Seconds()
		util.RecordOperationLatencyMetric(util.GetFullQualifiedPluginNameForVolume(volumePluginName, volumeToMount.VolumeSpec), "overall_volume_mount", totalTimeTaken)

		markVolMountedErr := actualStateOfWorld.MarkVolumeAsMounted(markOpts)
		if markVolMountedErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MountVolume.MarkVolumeAsMounted failed", markVolMountedErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FailedMountVolume, "%s", (*err).Error())
		}
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "volume_mount",
		OperationFunc:     mountVolumeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePluginName, volumeToMount.VolumeSpec), "volume_mount"),
	}
}

// checkForFailedMount 针对文件系统类型不匹配的挂载失败记录告警事件。
func (og *operationGenerator) checkForFailedMount(volumeToMount VolumeToMount, mountError error) {
	pv := volumeToMount.VolumeSpec.PersistentVolume
	if pv == nil {
		return
	}

	if volumetypes.IsFilesystemMismatchError(mountError) {
		simpleMsg, _ := volumeToMount.GenerateMsg("MountVolume failed", mountError.Error())
		og.recorder.Eventf(pv, v1.EventTypeWarning, kevents.FailedMountOnFilesystemMismatch, "%s", simpleMsg)
	}
}

// markVolumeMountAsAttempted 在 CSIVolumeHealth 开启时记录卷挂载尝试，供健康监控使用。
func (og *operationGenerator) markVolumeMountAsAttempted(actualStateOfWorld ActualStateOfWorldMounterUpdater, podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) {
	if utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		actualStateOfWorld.MarkVolumeAsMountAttempted(podName, volumeName)
	}
}

// markDeviceErrorState 根据错误类型把设备状态调整为已卸载或不确定。
func (og *operationGenerator) markDeviceErrorState(volumeToMount VolumeToMount, devicePath, deviceMountPath string, mountError error, actualStateOfWorld ActualStateOfWorldMounterUpdater) {
	if volumetypes.IsOperationFinishedError(mountError) &&
		actualStateOfWorld.GetDeviceMountState(volumeToMount.VolumeName) == DeviceMountUncertain {

		if actualStateOfWorld.IsVolumeDeviceReconstructed(volumeToMount.VolumeName) {
			klog.V(2).InfoS("MountVolume.markDeviceErrorState leaving volume uncertain", "volumeName", volumeToMount.VolumeName)
			return
		}

		// 只有状态不确定的设备才能标记为已卸载。
		markDeviceUnmountError := actualStateOfWorld.MarkDeviceAsUnmounted(volumeToMount.VolumeName)
		if markDeviceUnmountError != nil {
			klog.Error(volumeToMount.GenerateErrorDetailed("MountDevice.MarkDeviceAsUnmounted failed", markDeviceUnmountError).Error())
		}
		return
	}

	if volumetypes.IsUncertainProgressError(mountError) &&
		actualStateOfWorld.GetDeviceMountState(volumeToMount.VolumeName) == DeviceNotMounted {
		// 只有未挂载的设备才能标记为不确定；已标记为已挂载的设备不能在此降级。
		markDeviceUncertainError := actualStateOfWorld.MarkDeviceAsUncertain(volumeToMount.VolumeName, devicePath, deviceMountPath, volumeToMount.SELinuxLabel)
		if markDeviceUncertainError != nil {
			klog.Error(volumeToMount.GenerateErrorDetailed("MountDevice.MarkDeviceAsUncertain failed", markDeviceUncertainError).Error())
		}
	}

}

// markVolumeErrorState 根据错误类型把卷挂载状态调整为已卸载或不确定。
func (og *operationGenerator) markVolumeErrorState(volumeToMount VolumeToMount, markOpts MarkVolumeOpts, mountError error, actualStateOfWorld ActualStateOfWorldMounterUpdater) {
	if volumetypes.IsOperationFinishedError(mountError) &&
		actualStateOfWorld.GetVolumeMountState(volumeToMount.VolumeName, markOpts.PodName) == VolumeMountUncertain {
		// 重建恢复的卷即使挂载失败也不改为已卸载，保持其不确定状态。
		if actualStateOfWorld.IsVolumeReconstructed(volumeToMount.VolumeName, volumeToMount.PodName) {
			klog.V(3).InfoS("MountVolume.markVolumeErrorState leaving volume uncertain", "volumeName", volumeToMount.VolumeName)
			return
		}

		t := actualStateOfWorld.MarkVolumeAsUnmounted(volumeToMount.PodName, volumeToMount.VolumeName)
		if t != nil {
			klog.Error(volumeToMount.GenerateErrorDetailed("MountVolume.MarkVolumeAsUnmounted failed", t).Error())
		}
		return

	}

	if volumetypes.IsUncertainProgressError(mountError) &&
		actualStateOfWorld.GetVolumeMountState(volumeToMount.VolumeName, markOpts.PodName) == VolumeNotMounted {
		t := actualStateOfWorld.MarkVolumeMountAsUncertain(markOpts)
		if t != nil {
			klog.Error(volumeToMount.GenerateErrorDetailed("MountVolume.MarkVolumeMountAsUncertain failed", t).Error())
		}
	}
}

// GenerateUnmountVolumeFunc 生成从 Pod 卸载卷的 UnmountVolume 操作。
func (og *operationGenerator) GenerateUnmountVolumeFunc(
	volumeToUnmount MountedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	podsDir string) (volumetypes.GeneratedOperations, error) {
	// 获取可卸载插件。
	volumePlugin, err := og.volumePluginMgr.FindPluginByName(volumeToUnmount.PluginName)
	if err != nil || volumePlugin == nil {
		return volumetypes.GeneratedOperations{}, volumeToUnmount.GenerateErrorDetailed("UnmountVolume.FindPluginByName failed", err)
	}
	volumeUnmounter, newUnmounterErr := volumePlugin.NewUnmounter(
		volumeToUnmount.InnerVolumeSpecName, volumeToUnmount.PodUID)
	if newUnmounterErr != nil {
		return volumetypes.GeneratedOperations{}, volumeToUnmount.GenerateErrorDetailed("UnmountVolume.NewUnmounter failed", newUnmounterErr)
	}

	unmountVolumeFunc := func() volumetypes.OperationContext {
		subpather := og.volumePluginMgr.Host.GetSubpather()

		migrated := getMigratedStatusBySpec(volumeToUnmount.VolumeSpec)

		// 清理 subPath 产生的所有 bind mount。
		podDir := filepath.Join(podsDir, string(volumeToUnmount.PodUID))
		if err := subpather.CleanSubPaths(podDir, volumeToUnmount.InnerVolumeSpecName); err != nil {
			eventErr, detailedErr := volumeToUnmount.GenerateError("error cleaning subPath mounts", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 执行卸载。
		unmountErr := volumeUnmounter.TearDown()
		if unmountErr != nil {
			// 标记为不确定，使新 Pod 仍会调用 SetUp；卸载可能正在进行中。
			opts := MarkVolumeOpts{
				PodName:          volumeToUnmount.PodName,
				PodUID:           volumeToUnmount.PodUID,
				VolumeName:       volumeToUnmount.VolumeName,
				VolumeGIDVolume:  volumeToUnmount.VolumeGIDValue,
				VolumeSpec:       volumeToUnmount.VolumeSpec,
				VolumeMountState: VolumeMountUncertain,
			}
			markMountUncertainErr := actualStateOfWorld.MarkVolumeMountAsUncertain(opts)
			if markMountUncertainErr != nil {
				// 别无他法，只能等待 UnmountVolume 很快重试。
				klog.Error(volumeToUnmount.GenerateErrorDetailed("UnmountVolume.MarkVolumeMountAsUncertain failed", markMountUncertainErr).Error())
			}

			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToUnmount.GenerateError("UnmountVolume.TearDown failed", unmountErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		klog.Infof(
			"UnmountVolume.TearDown succeeded for volume %q pod %q (UID: %q). InnerVolumeSpecName %q. PluginName %q, VolumeGIDValue %q",
			volumeToUnmount.VolumeName,
			volumeToUnmount.PodName,
			volumeToUnmount.PodUID,
			volumeToUnmount.InnerVolumeSpecName,
			volumeToUnmount.PluginName,
			volumeToUnmount.VolumeGIDValue)

		// 更新实际状态。
		markVolMountedErr := actualStateOfWorld.MarkVolumeAsUnmounted(
			volumeToUnmount.PodName, volumeToUnmount.VolumeName)
		if markVolMountedErr != nil {
			// 此处失败只记录日志，不再重试。
			klog.Error(volumeToUnmount.GenerateErrorDetailed("UnmountVolume.MarkVolumeAsUnmounted failed", markVolMountedErr).Error())
		}

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "volume_unmount",
		OperationFunc:     unmountVolumeFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeToUnmount.VolumeSpec), "volume_unmount"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateUnmountDeviceFunc 生成卸载设备全局挂载路径的 UnmountDevice 操作。
func (og *operationGenerator) GenerateUnmountDeviceFunc(
	deviceToDetach AttachedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	hostutil hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {
	// 获取设备挂载插件。
	deviceMountableVolumePlugin, err :=
		og.volumePluginMgr.FindDeviceMountablePluginByName(deviceToDetach.PluginName)
	if err != nil || deviceMountableVolumePlugin == nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmountDevice.FindDeviceMountablePluginByName failed", err)
	}

	volumeDeviceUnmounter, err := deviceMountableVolumePlugin.NewDeviceUnmounter()
	if err != nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmountDevice.NewDeviceUnmounter failed", err)
	}

	volumeDeviceMounter, err := deviceMountableVolumePlugin.NewDeviceMounter()
	if err != nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmountDevice.NewDeviceMounter failed", err)
	}

	unmountDeviceFunc := func() volumetypes.OperationContext {

		migrated := getMigratedStatusBySpec(deviceToDetach.VolumeSpec)

		//deviceMountPath := deviceToDetach.DeviceMountPath
		deviceMountPath, err :=
			volumeDeviceMounter.GetDeviceMountPath(deviceToDetach.VolumeSpec)
		if err != nil {
			// 除“路径不存在”外的错误均直接返回，由调用方记录并重试。
			if !strings.Contains(err.Error(), "does not exist") {
				eventErr, detailedErr := deviceToDetach.GenerateError("GetDeviceMountPath failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
			// 找不到挂载路径时不让卸载失败：回退到 deviceToDetach.DeviceMountPath 并告警，
			// 这样才能把设备记为已卸载。
			deviceMountPath = deviceToDetach.DeviceMountPath
			klog.Warning(deviceToDetach.GenerateMsgDetailed(fmt.Sprintf(
				"GetDeviceMountPath failed, but unmount operation will proceed using deviceMountPath=%s: %v", deviceMountPath, err), ""))
		}
		refs, err := deviceMountableVolumePlugin.GetDeviceMountRefs(deviceMountPath)

		if err != nil || util.HasMountRefs(deviceMountPath, refs) {
			if err == nil {
				err = fmt.Errorf("the device mount path %q is still mounted by other references %v", deviceMountPath, refs)
			}
			eventErr, detailedErr := deviceToDetach.GenerateError("GetDeviceMountRefs check failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		// 执行设备卸载。
		unmountDeviceErr := volumeDeviceUnmounter.UnmountDevice(deviceMountPath)
		if unmountDeviceErr != nil {
			// 标记为不确定，使新 Pod 仍会调用 MountDevice；卸载可能正在进行中。
			markDeviceUncertainErr := actualStateOfWorld.MarkDeviceAsUncertain(deviceToDetach.VolumeName, deviceToDetach.DevicePath, deviceMountPath, deviceToDetach.SELinuxMountContext)
			if markDeviceUncertainErr != nil {
				// 别无他法，只能等待 UnmountDevice 很快重试。
				klog.Error(deviceToDetach.GenerateErrorDetailed("UnmountDevice.MarkDeviceAsUncertain failed", markDeviceUncertainErr).Error())
			}

			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := deviceToDetach.GenerateError("UnmountDevice failed", unmountDeviceErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		// 宣告卸载成功前，先用 hostutil.PathIsDevice 确认路径是否为设备，
		// 再用 hostutil.DeviceOpened 检查系统其他位置是否仍在使用；若仍被占用则需重试。
		deviceOpened, deviceOpenedErr := isDeviceOpened(deviceToDetach, hostutil)
		if deviceOpenedErr != nil {
			return volumetypes.NewOperationContext(nil, deviceOpenedErr, migrated)
		}
		// 设备仍被别处使用，由调用方记录并重试。
		if deviceOpened {
			// 标记为不确定，使新 Pod 仍会调用 MountDevice。
			markDeviceUncertainErr := actualStateOfWorld.MarkDeviceAsUncertain(deviceToDetach.VolumeName, deviceToDetach.DevicePath, deviceMountPath, deviceToDetach.SELinuxMountContext)
			if markDeviceUncertainErr != nil {
				// 别无他法，只能等待 UnmountDevice 很快重试。
				klog.Error(deviceToDetach.GenerateErrorDetailed("UnmountDevice.MarkDeviceAsUncertain failed", markDeviceUncertainErr).Error())
			}
			eventErr, detailedErr := deviceToDetach.GenerateError(
				"UnmountDevice failed",
				goerrors.New("the device is in use when it was no longer expected to be in use"))
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		klog.Info(deviceToDetach.GenerateMsgDetailed("UnmountDevice succeeded", ""))

		// 更新实际状态。
		markDeviceUnmountedErr := actualStateOfWorld.MarkDeviceAsUnmounted(
			deviceToDetach.VolumeName)
		if markDeviceUnmountedErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := deviceToDetach.GenerateError("MarkDeviceAsUnmounted failed", markDeviceUnmountedErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "unmount_device",
		OperationFunc:     unmountDeviceFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(deviceMountableVolumePlugin.GetPluginName(), deviceToDetach.VolumeSpec), "unmount_device"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateMapVolumeFunc 按如下步骤把块卷标记为已挂载：
// 可挂载插件先调用 WaitForAttach() 并把设备标记为已挂载；
// 随后不区分插件类型调用 SetUpDevice（主要针对不可挂载插件）。
// 设置完成后在全局映射路径和 Pod 设备映射路径创建符号链接，
// 再通过 loopback 持有设备 fd 锁，避免卷被静默替换；
// 无人使用该设备时锁自动释放。所有步骤完成后卷才被标记为已挂载。
func (og *operationGenerator) GenerateMapVolumeFunc(
	waitForAttachTimeout time.Duration,
	volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {

	// 获取块卷映射插件。
	blockVolumePlugin, err :=
		og.volumePluginMgr.FindMapperPluginBySpec(volumeToMount.VolumeSpec)
	if err != nil {
		return volumetypes.GeneratedOperations{}, volumeToMount.GenerateErrorDetailed("MapVolume.FindMapperPluginBySpec failed", err)
	}

	if blockVolumePlugin == nil {
		return volumetypes.GeneratedOperations{}, volumeToMount.GenerateErrorDetailed("MapVolume.FindMapperPluginBySpec failed to find BlockVolumeMapper plugin. Volume plugin is nil.", nil)
	}

	affinityErr := checkNodeAffinity(og, volumeToMount)
	if affinityErr != nil {
		eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.NodeAffinity check failed", affinityErr)
		og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FailedMountVolume, "%s", eventErr.Error())
		return volumetypes.GeneratedOperations{}, detailedErr
	}
	blockVolumeMapper, newMapperErr := blockVolumePlugin.NewBlockVolumeMapper(
		volumeToMount.VolumeSpec,
		volumeToMount.Pod)
	if newMapperErr != nil {
		eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.NewBlockVolumeMapper initialization failed", newMapperErr)
		og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FailedMapVolume, "%s", eventErr.Error())
		return volumetypes.GeneratedOperations{}, detailedErr
	}

	// 尽量获取挂载器；并非所有卷都需要 Attach。
	attachableVolumePlugin, _ :=
		og.volumePluginMgr.FindAttachablePluginBySpec(volumeToMount.VolumeSpec)
	var volumeAttacher volume.Attacher
	if attachableVolumePlugin != nil {
		volumeAttacher, _ = attachableVolumePlugin.NewAttacher()
	}

	mapVolumeFunc := func() (operationContext volumetypes.OperationContext) {
		var devicePath string
		var stagingPath string

		migrated := getMigratedStatusBySpec(volumeToMount.VolumeSpec)

		// 强制校验 ReadWriteOncePod 访问模式；调度阶段同样会校验。
		if actualStateOfWorld.IsVolumeMountedElsewhere(volumeToMount.VolumeName, volumeToMount.PodName) &&
			// 多个访问模式时无法判断 Pod 的意图，故只处理单一模式的情形。
			len(volumeToMount.VolumeSpec.PersistentVolume.Spec.AccessModes) == 1 &&
			v1helper.ContainsAccessMode(volumeToMount.VolumeSpec.PersistentVolume.Spec.AccessModes, v1.ReadWriteOncePod) {

			err = goerrors.New("volume uses the ReadWriteOncePod access mode and is already in use by another pod")
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.SetUpDevice failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 通过符号链接在插件目录下建立全局映射路径。
		globalMapPath, err :=
			blockVolumeMapper.GetGlobalMapPath(volumeToMount.VolumeSpec)
		if err != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.GetGlobalMapPath failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		if volumeAttacher != nil {
			// 等待可挂载卷完成 Attach。
			klog.InfoS(volumeToMount.GenerateMsgDetailed("MapVolume.WaitForAttach entering", fmt.Sprintf("DevicePath %q", volumeToMount.DevicePath)), "pod", klog.KObj(volumeToMount.Pod))

			devicePath, err = volumeAttacher.WaitForAttach(
				volumeToMount.VolumeSpec, volumeToMount.DevicePath, volumeToMount.Pod, waitForAttachTimeout)
			if err != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.WaitForAttach failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			klog.InfoS(volumeToMount.GenerateMsgDetailed("MapVolume.WaitForAttach succeeded", fmt.Sprintf("DevicePath %q", devicePath)), "pod", klog.KObj(volumeToMount.Pod))

		}
		// 仅自定义块映射插件才需要 SetUpDevice。
		if customBlockVolumeMapper, ok := blockVolumeMapper.(volume.CustomBlockVolumeMapper); ok && actualStateOfWorld.GetDeviceMountState(volumeToMount.VolumeName) != DeviceGloballyMounted {
			var mapErr error
			og.markVolumeMountAsAttempted(actualStateOfWorld, volumeToMount.PodName, volumeToMount.VolumeName)
			stagingPath, mapErr = customBlockVolumeMapper.SetUpDevice()
			if mapErr != nil {
				og.markDeviceErrorState(volumeToMount, devicePath, globalMapPath, mapErr, actualStateOfWorld)
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.SetUpDevice failed", mapErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}

		// 更新实际状态，记录卷已全局挂载。
		markedDevicePath := devicePath
		markDeviceMappedErr := actualStateOfWorld.MarkDeviceAsMounted(
			volumeToMount.VolumeName, markedDevicePath, globalMapPath, "")
		if markDeviceMappedErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MarkDeviceAsMounted failed", markDeviceMappedErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		markVolumeOpts := MarkVolumeOpts{
			PodName:           volumeToMount.PodName,
			PodUID:            volumeToMount.Pod.UID,
			VolumeName:        volumeToMount.VolumeName,
			BlockVolumeMapper: blockVolumeMapper,
			VolumeGIDVolume:   volumeToMount.VolumeGIDValue,
			VolumeSpec:        volumeToMount.VolumeSpec,
			VolumeMountState:  VolumeMounted,
		}

		// 自定义块映射插件可自行完成 Pod 设备映射。
		if customBlockVolumeMapper, ok := blockVolumeMapper.(volume.CustomBlockVolumeMapper); ok {
			// 执行驱动特定的映射。
			pluginDevicePath, mapErr := customBlockVolumeMapper.MapPodDevice()
			if mapErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				og.markVolumeErrorState(volumeToMount, markVolumeOpts, mapErr, actualStateOfWorld)
				eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MapPodDevice failed", mapErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			// 卷至此已映射完成；后续出错时标记为不确定，
			// 以便对应 Pod 删除时执行解除映射。
			defer func() {
				if operationContext.EventErr != nil {
					errText := operationContext.EventErr.Error()
					og.markVolumeErrorState(volumeToMount, markVolumeOpts, volumetypes.NewUncertainProgressError(errText), actualStateOfWorld)
				}
			}()

			// 插件给出了设备路径时，说明挂载器可能未提供设备，或附加流程通过 SetupDevice 获取路径。
			if len(pluginDevicePath) != 0 {
				devicePath = pluginDevicePath
			}
			if len(devicePath) == 0 {
				eventErr, detailedErr := volumeToMount.GenerateError("MapVolume failed", goerrors.New("device path of the volume is empty"))
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}

		// 容器化部署的 kubelet 可能无法访问 devicePath 中的符号链接，
		// 故在宿主机上解析，期望其指向 /dev 下容器可见的设备；若仍不存在，AttachFileDevice 会失败。
		// 非容器化 kubelet 也统一做解析。
		kvh, ok := og.GetVolumePluginMgr().Host.(volume.KubeletVolumeHost)
		if !ok {
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume type assertion error", fmt.Errorf("volume host does not implement KubeletVolumeHost interface"))
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		hu := kvh.GetHostUtil()
		devicePath, err = hu.EvalHostSymlinks(devicePath)
		if err != nil {
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.EvalHostSymlinks failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 解析后设备路径可能变化，需重新更新实际状态。
		// TODO: 等 #82492 合并且 ASW 保存相关状态后，可以改进此处处理。
		if markedDevicePath != devicePath {
			markDeviceMappedErr := actualStateOfWorld.MarkDeviceAsMounted(
				volumeToMount.VolumeName, devicePath, globalMapPath, "")
			if markDeviceMappedErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MarkDeviceAsMounted failed", markDeviceMappedErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}

		// 执行通用映射。
		volumeMapPath, volName := blockVolumeMapper.GetPodDeviceMapPath()
		mapErr := util.MapBlockVolume(og.blkUtil, devicePath, globalMapPath, volumeMapPath, volName, volumeToMount.Pod.UID)
		if mapErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MapBlockVolume failed", mapErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 全局映射路径的设备映射成功。
		simpleMsg, detailedMsg := volumeToMount.GenerateMsg("MapVolume.MapPodDevice succeeded", fmt.Sprintf("globalMapPath %q", globalMapPath))
		verbosity := klog.Level(4)
		og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeNormal, kevents.SuccessfulMountVolume, "%s", simpleMsg)
		klog.V(verbosity).InfoS(detailedMsg, "pod", klog.KObj(volumeToMount.Pod))

		// Pod 设备映射路径的设备映射成功。
		simpleMsg, detailedMsg = volumeToMount.GenerateMsg("MapVolume.MapPodDevice succeeded", fmt.Sprintf("volumeMapPath %q", volumeMapPath))
		verbosity = klog.Level(1)
		og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeNormal, kevents.SuccessfulMountVolume, "%s", simpleMsg)
		klog.V(verbosity).InfoS(detailedMsg, "pod", klog.KObj(volumeToMount.Pod))

		resizeOptions := volume.NodeResizeOptions{
			DevicePath:      devicePath,
			DeviceStagePath: stagingPath,
		}
		_, resizeError := og.expandVolumeDuringMount(volumeToMount, actualStateOfWorld, resizeOptions)
		if resizeError != nil {
			klog.Errorf("MapVolume.NodeExpandVolume failed with %v", resizeError)
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MarkVolumeAsMounted failed while expanding volume", resizeError)
			// 此时 SetUp 已成功，需让协调器能在必要时清理卷；但扩容失败又不能标记为已挂载，
			// 否则 Pod 会开始使用它。故标记为不确定：Pod 删除时触发清理，同时阻止其启动使用。
			if err := actualStateOfWorld.MarkVolumeMountAsUncertain(markVolumeOpts); err != nil {
				klog.Error(volumeToMount.GenerateErrorDetailed("MountVolume.MarkVolumeMountAsUncertain failed", err).Error())
			}
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		markVolMountedErr := actualStateOfWorld.MarkVolumeAsMounted(markVolumeOpts)
		if markVolMountedErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("MapVolume.MarkVolumeAsMounted failed", markVolMountedErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FailedMapVolume, "%s", (*err).Error())
		}
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "map_volume",
		OperationFunc:     mapVolumeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(blockVolumePlugin.GetPluginName(), volumeToMount.VolumeSpec), "map_volume"),
	}, nil
}

// GenerateUnmapVolumeFunc 按如下步骤把卷标记为未挂载：
// 删除 Pod 设备映射路径与全局映射路径下的符号链接，再删除 Pod 设备映射目录。
// 所有步骤完成后卷才被标记为未挂载。
func (og *operationGenerator) GenerateUnmapVolumeFunc(
	volumeToUnmount MountedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {

	// 获取块卷解除映射插件。
	blockVolumePlugin, err :=
		og.volumePluginMgr.FindMapperPluginByName(volumeToUnmount.PluginName)
	if err != nil {
		return volumetypes.GeneratedOperations{}, volumeToUnmount.GenerateErrorDetailed("UnmapVolume.FindMapperPluginByName failed", err)
	}
	if blockVolumePlugin == nil {
		return volumetypes.GeneratedOperations{}, volumeToUnmount.GenerateErrorDetailed("UnmapVolume.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil.", nil)
	}
	blockVolumeUnmapper, newUnmapperErr := blockVolumePlugin.NewBlockVolumeUnmapper(
		volumeToUnmount.InnerVolumeSpecName, volumeToUnmount.PodUID)
	if newUnmapperErr != nil {
		return volumetypes.GeneratedOperations{}, volumeToUnmount.GenerateErrorDetailed("UnmapVolume.NewUnmapper failed", newUnmapperErr)
	}

	unmapVolumeFunc := func() volumetypes.OperationContext {

		migrated := getMigratedStatusBySpec(volumeToUnmount.VolumeSpec)

		// pods/{podUid}/volumeDevices/{escapeQualifiedPluginName}/{volumeName}
		podDeviceUnmapPath, volName := blockVolumeUnmapper.GetPodDeviceMapPath()
		// plugins/kubernetes.io/{PluginName}/volumeDevices/{volumePluginDependentPath}/{podUID}
		globalUnmapPath, err := blockVolumeUnmapper.GetGlobalMapPath(volumeToUnmount.VolumeSpec)
		if err != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToUnmount.GenerateError("UnmapVolume.GetGlobalMapPath failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 先把设备标记为不确定，保证下面任何 "return err" 分支都会让 kubelet 再次调用 UnmapDevice。
		// 全部成功后才在末尾标记为完全解除映射。
		markVolumeOpts := MarkVolumeOpts{
			PodName:          volumeToUnmount.PodName,
			PodUID:           volumeToUnmount.PodUID,
			VolumeName:       volumeToUnmount.VolumeName,
			VolumeGIDVolume:  volumeToUnmount.VolumeGIDValue,
			VolumeSpec:       volumeToUnmount.VolumeSpec,
			VolumeMountState: VolumeMountUncertain,
		}
		markVolumeUncertainErr := actualStateOfWorld.MarkVolumeMountAsUncertain(markVolumeOpts)
		if markVolumeUncertainErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToUnmount.GenerateError("UnmapVolume.MarkDeviceAsUncertain failed", markVolumeUncertainErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 执行通用解除映射。
		unmapErr := util.UnmapBlockVolume(og.blkUtil, globalUnmapPath, podDeviceUnmapPath, volName, volumeToUnmount.PodUID)
		if unmapErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToUnmount.GenerateError("UnmapVolume.UnmapBlockVolume failed", unmapErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 自定义块映射插件自行解除 Pod 设备映射。
		if customBlockVolumeUnmapper, ok := blockVolumeUnmapper.(volume.CustomBlockVolumeUnmapper); ok {
			// 执行插件特定的解除映射。
			unmapErr = customBlockVolumeUnmapper.UnmapPodDevice()
			if unmapErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToUnmount.GenerateError("UnmapVolume.UnmapPodDevice failed", unmapErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}

		klog.Infof(
			"UnmapVolume succeeded for volume %q pod %q (UID: %q). InnerVolumeSpecName %q. PluginName %q, VolumeGIDValue %q",
			volumeToUnmount.VolumeName,
			volumeToUnmount.PodName,
			volumeToUnmount.PodUID,
			volumeToUnmount.InnerVolumeSpecName,
			volumeToUnmount.PluginName,
			volumeToUnmount.VolumeGIDValue)

		// 更新实际状态。
		markVolUnmountedErr := actualStateOfWorld.MarkVolumeAsUnmounted(
			volumeToUnmount.PodName, volumeToUnmount.VolumeName)
		if markVolUnmountedErr != nil {
			// 此处失败只记录日志，不再重试。
			klog.Error(volumeToUnmount.GenerateErrorDetailed("UnmapVolume.MarkVolumeAsUnmounted failed", markVolUnmountedErr).Error())
		}

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "unmap_volume",
		OperationFunc:     unmapVolumeFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(blockVolumePlugin.GetPluginName(), volumeToUnmount.VolumeSpec), "unmap_volume"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateUnmapDeviceFunc 按如下步骤把设备标记为未挂载：
// 检查 globalMapPath 下是否还有 Pod 的符号链接；没有则说明设备已不被任何 Pod 引用。
// 调用插件的 TearDownDevice 清理设备连接及 globalMapPath 下的数据（具体行为由插件决定），
// 完成后删除 globalMapPath 目录，此时方可安全释放 loopback 持有的设备 fd 锁。
// 最后再检查一次设备占用状态。所有步骤完成后设备才被标记为未挂载。
func (og *operationGenerator) GenerateUnmapDeviceFunc(
	deviceToDetach AttachedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	hostutil hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {

	blockVolumePlugin, err :=
		og.volumePluginMgr.FindMapperPluginByName(deviceToDetach.PluginName)
	if err != nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmapDevice.FindMapperPluginByName failed", err)
	}

	if blockVolumePlugin == nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmapDevice.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil.", nil)
	}

	blockVolumeUnmapper, newUnmapperErr := blockVolumePlugin.NewBlockVolumeUnmapper(
		deviceToDetach.VolumeSpec.Name(),
		"" /* podUID */)
	if newUnmapperErr != nil {
		return volumetypes.GeneratedOperations{}, deviceToDetach.GenerateErrorDetailed("UnmapDevice.NewUnmapper failed", newUnmapperErr)
	}

	unmapDeviceFunc := func() volumetypes.OperationContext {
		migrated := getMigratedStatusBySpec(deviceToDetach.VolumeSpec)
		// 检查 globalMapPath 下所有来自 Pod 的符号链接是否已删除；
		// 仍有链接说明 Pod 可能还在引用该卷。
		globalMapPath := deviceToDetach.DeviceMountPath
		refs, err := og.blkUtil.GetDeviceBindMountRefs(deviceToDetach.DevicePath, globalMapPath)
		if err != nil {
			if os.IsNotExist(err) {
				// 看起来 SetupDevice 未完成；继续走 TearDownDevice 并把设备标记为未挂载。
				refs = nil
			} else {
				eventErr, detailedErr := deviceToDetach.GenerateError("UnmapDevice.GetDeviceBindMountRefs check failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}
		if len(refs) > 0 {
			err = fmt.Errorf("the device %q is still referenced from other Pods %v", globalMapPath, refs)
			eventErr, detailedErr := deviceToDetach.GenerateError("UnmapDevice failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 先把设备标记为不确定，保证下面任何 "return err" 分支都会让 kubelet 再次调用 UnmapDevice。
		// 全部成功后才在末尾标记为完全解除映射。
		markDeviceUncertainErr := actualStateOfWorld.MarkDeviceAsUncertain(
			deviceToDetach.VolumeName, deviceToDetach.DevicePath, globalMapPath, "" /* seLinuxMountContext */)
		if markDeviceUncertainErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := deviceToDetach.GenerateError("UnmapDevice.MarkDeviceAsUncertain failed", markDeviceUncertainErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 自定义块映射插件自行清理设备。
		if customBlockVolumeUnmapper, ok := blockVolumeUnmapper.(volume.CustomBlockVolumeUnmapper); ok {
			// 执行插件特定的设备清理。
			unmapErr := customBlockVolumeUnmapper.TearDownDevice(globalMapPath, deviceToDetach.DevicePath)
			if unmapErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := deviceToDetach.GenerateError("UnmapDevice.TearDownDevice failed", unmapErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
		}

		// 插件已清理完设备，globalMapPath 目录及其中的数据不再需要，直接删除。
		removeMapPathErr := og.blkUtil.RemoveMapPath(globalMapPath)
		if removeMapPathErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := deviceToDetach.GenerateError("UnmapDevice.RemoveMapPath failed", removeMapPathErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 宣告解除映射成功前，先用 hostutil.PathIsDevice 确认路径是否为设备，
		// 再用 hostutil.DeviceOpened 检查系统其他位置是否仍在使用；若仍被占用则需重试。
		deviceOpened, deviceOpenedErr := isDeviceOpened(deviceToDetach, hostutil)
		if deviceOpenedErr != nil {
			return volumetypes.NewOperationContext(nil, deviceOpenedErr, migrated)
		}
		// 设备仍被别处使用，由调用方记录并重试。
		if deviceOpened {
			eventErr, detailedErr := deviceToDetach.GenerateError(
				"UnmapDevice failed",
				fmt.Errorf("the device is in use when it was no longer expected to be in use"))
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		klog.Info(deviceToDetach.GenerateMsgDetailed("UnmapDevice succeeded", ""))

		// 更新实际状态。
		markDeviceUnmountedErr := actualStateOfWorld.MarkDeviceAsUnmounted(
			deviceToDetach.VolumeName)
		if markDeviceUnmountedErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := deviceToDetach.GenerateError("MarkDeviceAsUnmounted failed", markDeviceUnmountedErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "unmap_device",
		OperationFunc:     unmapDeviceFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(blockVolumePlugin.GetPluginName(), deviceToDetach.VolumeSpec), "unmap_device"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil
}

// GenerateVerifyControllerAttachedVolumeFunc 生成校验控制器是否已完成卷挂载的操作。
func (og *operationGenerator) GenerateVerifyControllerAttachedVolumeFunc(
	logger klog.Logger,
	volumeToMount VolumeToMount,
	nodeName types.NodeName,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	volumePlugin, err :=
		og.volumePluginMgr.FindPluginBySpec(volumeToMount.VolumeSpec)
	if err != nil || volumePlugin == nil {
		return volumetypes.GeneratedOperations{}, volumeToMount.GenerateErrorDetailed("VerifyControllerAttachedVolume.FindPluginBySpec failed", err)
	}

	// 对可挂载卷先从 node lister 缓存检查挂载状态，避免不必要的指数退避与 goroutine。
	// 之后仍会直接读 API 服务器复核，以防 informer 缓存状态带来竞态。
	if volumeToMount.PluginIsAttachable {
		cachedAttachedVolumes, _ := og.volumePluginMgr.Host.GetAttachedVolumesFromNodeStatus()
		if cachedAttachedVolumes != nil {
			_, volumeFound := cachedAttachedVolumes[volumeToMount.VolumeName]
			if !volumeFound {
				return volumetypes.GeneratedOperations{}, NewMountPreConditionFailedError(fmt.Sprintf("volume %s is not yet in node's status", volumeToMount.VolumeName))
			}
		}
	}

	verifyControllerAttachedVolumeFunc := func() volumetypes.OperationContext {
		migrated := getMigratedStatusBySpec(volumeToMount.VolumeSpec)
		claimSize := actualStateOfWorld.GetClaimSize(volumeToMount.VolumeName)

		// 仅当之前未记录 claimSize 时才去获取。
		if volumeToMount.VolumeSpec.PersistentVolume != nil && claimSize.IsZero() && !volumeToMount.VolumeSpec.InlineVolumeSpecForCSIMigration {
			pv := volumeToMount.VolumeSpec.PersistentVolume
			pvc, err := og.kubeClient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(context.TODO(), pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				eventErr, detailedErr := volumeToMount.GenerateError("VerifyControllerAttachedVolume fetching pvc failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
			pvcStatusSize := pvc.Status.Capacity.Storage()
			if pvcStatusSize != nil {
				claimSize = pvcStatusSize.DeepCopy()
			}
		}

		if !volumeToMount.PluginIsAttachable {
			// 卷未实现 attacher 接口时视为已挂载，并相应更新实际状态。

			addVolumeNodeErr := actualStateOfWorld.MarkVolumeAsAttached(
				logger, volumeToMount.VolumeName, volumeToMount.VolumeSpec, nodeName, "" /* devicePath */)
			if addVolumeNodeErr != nil {
				// 失败时返回错误，由调用方记录并重试。
				eventErr, detailedErr := volumeToMount.GenerateError("VerifyControllerAttachedVolume.MarkVolumeAsAttachedByUniqueVolumeName failed", addVolumeNodeErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}
			actualStateOfWorld.InitializeClaimSize(logger, volumeToMount.VolumeName, claimSize)
			return volumetypes.NewOperationContext(nil, nil, migrated)
		}

		if !volumeToMount.ReportedInUse {
			// 卷尚未出现在节点状态的 VolumesInUse 列表中时直接报错，由调用方记录并重试。
			// 节点状态由 kubelet 周期性更新，最多可能需 10 秒才会生效。
			// 按需更新节点状态的方案见 Issue #28141。
			eventErr, detailedErr := volumeToMount.GenerateError("Volume has not been added to the list of VolumesInUse in the node's volume status", nil)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		// 获取最新的 Node 对象。
		node, fetchErr := og.kubeClient.CoreV1().Nodes().Get(context.TODO(), string(nodeName), metav1.GetOptions{})
		if fetchErr != nil {
			// 失败时返回错误，由调用方记录并重试。
			eventErr, detailedErr := volumeToMount.GenerateError("VerifyControllerAttachedVolume failed fetching node from API server", fetchErr)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		for _, attachedVolume := range node.Status.VolumesAttached {
			if attachedVolume.Name == volumeToMount.VolumeName {
				addVolumeNodeErr := actualStateOfWorld.MarkVolumeAsAttached(
					logger, v1.UniqueVolumeName(""), volumeToMount.VolumeSpec, nodeName, attachedVolume.DevicePath)
				klog.InfoS(volumeToMount.GenerateMsgDetailed("Controller attach succeeded", fmt.Sprintf("device path: %q", attachedVolume.DevicePath)), "pod", klog.KObj(volumeToMount.Pod))
				if addVolumeNodeErr != nil {
					// 失败时返回错误，由调用方记录并重试。
					eventErr, detailedErr := volumeToMount.GenerateError("VerifyControllerAttachedVolume.MarkVolumeAsAttached failed", addVolumeNodeErr)
					return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
				}
				actualStateOfWorld.InitializeClaimSize(logger, volumeToMount.VolumeName, claimSize)
				return volumetypes.NewOperationContext(nil, nil, migrated)
			}
		}

		// 节点状态显示卷未挂载，返回错误由调用方记录并重试。
		eventErr, detailedErr := volumeToMount.GenerateError("Volume not attached according to node status", nil)
		return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
	}

	return volumetypes.GeneratedOperations{
		OperationName:     VerifyControllerAttachedVolumeOpName,
		OperationFunc:     verifyControllerAttachedVolumeFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeToMount.VolumeSpec), "verify_controller_attached_volume"),
		EventRecorderFunc: nil, // 不希望在该操作出错时产生事件。
	}, nil

}

// verifyVolumeIsSafeToDetach 检查节点状态以确认卷可安全分离。
func (og *operationGenerator) verifyVolumeIsSafeToDetach(
	volumeToDetach AttachedVolume) error {
	// 获取最新的 Node 对象。
	node, fetchErr := og.kubeClient.CoreV1().Nodes().Get(context.TODO(), string(volumeToDetach.NodeName), metav1.GetOptions{})
	if fetchErr != nil {
		if errors.IsNotFound(fetchErr) {
			klog.Warning(volumeToDetach.GenerateMsgDetailed("Node not found on API server. DetachVolume will skip safe to detach check", ""))
			return nil
		}

		// 失败时返回错误，由调用方记录并重试。
		return volumeToDetach.GenerateErrorDetailed("DetachVolume failed fetching node from API server", fetchErr)
	}

	if slices.Contains(node.Status.VolumesInUse, volumeToDetach.VolumeName) {
		return volumeToDetach.GenerateErrorDetailed(
			"DetachVolume failed",
			fmt.Errorf("volume is still in use by node, according to Node status"))
	}

	// 节点未将卷标记为使用中，可安全分离。
	klog.Info(volumeToDetach.GenerateMsgDetailed("Verified volume is safe to detach", ""))
	return nil
}

// GenerateExpandVolumeFunc 生成控制器侧卷扩容操作。
func (og *operationGenerator) GenerateExpandVolumeFunc(
	pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume) (volumetypes.GeneratedOperations, error) {

	volumeSpec := volume.NewSpecFromPersistentVolume(pv, false)

	volumePlugin, err := og.volumePluginMgr.FindExpandablePluginBySpec(volumeSpec)
	if err != nil {
		return volumetypes.GeneratedOperations{}, fmt.Errorf("error finding plugin for expanding volume: %q with error %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
	}

	if volumePlugin == nil {
		return volumetypes.GeneratedOperations{}, fmt.Errorf("can not find plugin for expanding volume: %q", util.GetPersistentVolumeClaimQualifiedName(pvc))
	}

	expandVolumeFunc := func() volumetypes.OperationContext {
		migrated := false

		newSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		statusSize := pvc.Status.Capacity[v1.ResourceStorage]
		pvSize := pv.Spec.Capacity[v1.ResourceStorage]
		if pvSize.Cmp(newSize) < 0 {
			updatedSize, expandErr := volumePlugin.ExpandVolumeDevice(
				volumeSpec,
				newSize,
				statusSize)
			if expandErr != nil {
				detailedErr := fmt.Errorf("error expanding volume %q of plugin %q: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), volumePlugin.GetPluginName(), expandErr)
				return volumetypes.NewOperationContext(detailedErr, detailedErr, migrated)
			}

			klog.Infof("ExpandVolume succeeded for volume %s", util.GetPersistentVolumeClaimQualifiedName(pvc))

			newSize = updatedSize
			// k8s 没有事务，无法保证更新 PV 后 PVC 一定更新成功；
			// 因此所有 pvc.Spec.Size > pvc.Status.Size 的 PVC 都需反复处理，
			// 直到 pvc.Status.Size 反映用户请求的大小。
			_, updateErr := util.UpdatePVSize(pv, newSize, og.kubeClient)
			if updateErr != nil {
				detailedErr := fmt.Errorf("error updating PV spec capacity for volume %q with : %v", util.GetPersistentVolumeClaimQualifiedName(pvc), updateErr)
				return volumetypes.NewOperationContext(detailedErr, detailedErr, migrated)
			}

			klog.Infof("ExpandVolume.UpdatePV succeeded for volume %s", util.GetPersistentVolumeClaimQualifiedName(pvc))
		}

		fsVolume, _ := util.CheckVolumeModeFilesystem(volumeSpec)
		// 无需云厂商扩容时直接标记完成；其余扩容控制器逻辑在 pvc.Status.Size
		// 反映用户请求大小之前，会始终视 PVC 为未扩容。
		if !volumePlugin.RequiresFSResize() || !fsVolume {
			klog.V(4).Infof("Controller resizing done for PVC %s", util.GetPersistentVolumeClaimQualifiedName(pvc))
			_, err := util.MarkResizeFinished(pvc, newSize, og.kubeClient)
			if err != nil {
				detailedErr := fmt.Errorf("error marking pvc %s as resized: %w", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
				return volumetypes.NewOperationContext(detailedErr, detailedErr, migrated)
			}
			successMsg := fmt.Sprintf("ExpandVolume succeeded for volume %s", util.GetPersistentVolumeClaimQualifiedName(pvc))
			og.recorder.Eventf(pvc, v1.EventTypeNormal, kevents.VolumeResizeSuccess, "%s", successMsg)
		} else {
			_, err := util.MarkForFSResize(pvc, og.kubeClient)
			if err != nil {
				detailedErr := fmt.Errorf("error updating pvc %s condition for fs resize: %w", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
				klog.Warning(detailedErr)
				return volumetypes.NewOperationContext(nil, nil, migrated)
			}
			oldCapacity := pvc.Status.Capacity[v1.ResourceStorage]
			err = util.AddAnnPreResizeCapacity(pv, oldCapacity, og.kubeClient)
			if err != nil {
				detailedErr := fmt.Errorf("error updating pv %s annotation (%s) with pre-resize capacity %s: %v", pv.ObjectMeta.Name, util.AnnPreResizeCapacity, oldCapacity.String(), err)
				klog.Warning(detailedErr)
				return volumetypes.NewOperationContext(nil, nil, migrated)
			}

		}
		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			og.recorder.Eventf(pvc, v1.EventTypeWarning, kevents.VolumeResizeFailed, "%s", (*err).Error())
		}
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "expand_volume",
		OperationFunc:     expandVolumeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeSpec), "expand_volume"),
	}, nil
}

// GenerateExpandAndRecoverVolumeFunc 生成支持从扩容失败中恢复的卷扩容操作。
func (og *operationGenerator) GenerateExpandAndRecoverVolumeFunc(
	pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume, resizerName string) (volumetypes.GeneratedOperations, error) {

	volumeSpec := volume.NewSpecFromPersistentVolume(pv, false)

	volumePlugin, err := og.volumePluginMgr.FindExpandablePluginBySpec(volumeSpec)
	if err != nil {
		return volumetypes.GeneratedOperations{}, fmt.Errorf("error finding plugin for expanding volume: %q with error %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
	}

	if volumePlugin == nil {
		return volumetypes.GeneratedOperations{}, fmt.Errorf("can not find plugin for expanding volume: %q", util.GetPersistentVolumeClaimQualifiedName(pvc))
	}

	expandVolumeFunc := func() volumetypes.OperationContext {
		resizeOpts := inTreeResizeOpts{
			pvc:          pvc,
			pv:           pv,
			resizerName:  resizerName,
			volumePlugin: volumePlugin,
			volumeSpec:   volumeSpec,
		}
		migrated := false
		resp := og.expandAndRecoverFunction(resizeOpts)
		if resp.err != nil {
			return volumetypes.NewOperationContext(resp.err, resp.err, migrated)
		}
		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			og.recorder.Eventf(pvc, v1.EventTypeWarning, kevents.VolumeResizeFailed, "%s", (*err).Error())
		}
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "expand_volume",
		OperationFunc:     expandVolumeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeSpec), "expand_volume"),
	}, nil
}

// Deprecated: 未来不应再被任何控制器调用，并应从 Kubernetes 代码中移除。
// expandAndRecoverFunction 执行扩容并从失败中恢复。
func (og *operationGenerator) expandAndRecoverFunction(resizeOpts inTreeResizeOpts) inTreeResizeResponse {
	pvc := resizeOpts.pvc
	pv := resizeOpts.pv
	resizerName := resizeOpts.resizerName
	volumePlugin := resizeOpts.volumePlugin
	volumeSpec := resizeOpts.volumeSpec

	pvcSpecSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcStatusSize := pvc.Status.Capacity[v1.ResourceStorage]
	pvSize := pv.Spec.Capacity[v1.ResourceStorage]

	resizeResponse := inTreeResizeResponse{
		pvc:          pvc,
		pv:           pv,
		resizeCalled: false,
	}

	// 默认扩容到 pvc.Spec.Resources 中请求的大小。
	newSize := pvcSpecSize

	var resizeStatus v1.ClaimResourceStatus
	if status, ok := pvc.Status.AllocatedResourceStatuses[v1.ResourceStorage]; ok {
		resizeStatus = status
	}

	var allocatedSize *resource.Quantity
	t, ok := pvc.Status.AllocatedResources[v1.ResourceStorage]
	if ok {
		allocatedSize = &t
	}
	var err error

	if pvSize.Cmp(pvcSpecSize) < 0 {
		// PV 尚未达到请求大小，需要扩容。

		switch resizeStatus {
		case v1.PersistentVolumeClaimControllerResizeInProgress,
			v1.PersistentVolumeClaimNodeResizePending,
			v1.PersistentVolumeClaimNodeResizeInProgress,
			v1.PersistentVolumeClaimNodeResizeInfeasible:
			if allocatedSize != nil {
				newSize = *allocatedSize
			}
		default:
			newSize = pvcSpecSize
		}
	} else {
		// PV 已扩容仍走到这里，可能原因有四：
		//   1. 节点扩容待处理，且这只是一次多余的更新事件——无需操作，交给 kubelet。
		//   2. 卷已成功扩容但未写回 API 对象——应恢复扩容并更新 API 对象。
		//   3. 控制器已扩容，但节点扩容失败——kubelet 重试前控制器需确认是否安全。
		//   4. 节点扩容待处理期间用户缩小了 PVC。
		switch resizeStatus {
		case v1.PersistentVolumeClaimNodeResizeInProgress,
			v1.PersistentVolumeClaimNodeResizePending:
			// 无需操作，多由多余的更新事件触发（情形 1）。
			return resizeResponse
		case v1.PersistentVolumeClaimNodeResizeInfeasible:
			// 情形 3。
			pvc, err = og.markForPendingNodeExpansion(pvc, pv)
			resizeResponse.pvc = pvc
			resizeResponse.err = err
			return resizeResponse
		case v1.PersistentVolumeClaimControllerResizeInProgress,
			v1.PersistentVolumeClaimControllerResizeInfeasible:
			// 情形 2；也可能是情形 4：用户扩容后又手动缩小了 PVC。
			if allocatedSize != nil {
				newSize = *allocatedSize
			}
		default:
			// resizeStatus 为空而 allocatedSize 非空本不应出现；若真遇到，
			// 按 allocatedSize 中记录的上次大小恢复扩容最稳妥。
			if resizeStatus == "" && allocatedSize != nil {
				newSize = *allocatedSize
			} else {
				newSize = pvcSpecSize
			}
		}
	}

	pvc, err = util.MarkControllerReisizeInProgress(pvc, resizerName, newSize, og.kubeClient)
	if err != nil {
		msg := fmt.Errorf("error updating pvc %s with resize in progress: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
		resizeResponse.err = msg
		resizeResponse.pvc = pvc
		return resizeResponse
	}

	updatedSize, err := volumePlugin.ExpandVolumeDevice(volumeSpec, newSize, pvcStatusSize)
	resizeResponse.resizeCalled = true

	if err != nil {
		msg := fmt.Errorf("error expanding pvc %s: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
		resizeResponse.err = msg
		resizeResponse.pvc = pvc
		return resizeResponse
	}

	// 更新 PV 大小。
	var updateErr error
	pv, updateErr = util.UpdatePVSize(pv, updatedSize, og.kubeClient)
	// PV 更新失败时保持 PVC 处于 ControllerExpansionInProgress，便于按之前记录的 allocatedSize 重试。
	if updateErr != nil {
		msg := fmt.Errorf("error updating pv for pvc %s: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), updateErr)
		resizeResponse.err = msg
		return resizeResponse
	}
	resizeResponse.pv = pv

	fsVolume, _ := util.CheckVolumeModeFilesystem(volumeSpec)

	if !volumePlugin.RequiresFSResize() || !fsVolume {
		pvc, err = util.MarkResizeFinished(pvc, updatedSize, og.kubeClient)
		if err != nil {
			msg := fmt.Errorf("error marking pvc %s as resized: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
			resizeResponse.err = msg
			return resizeResponse
		}
		resizeResponse.pvc = pvc
		successMsg := fmt.Sprintf("ExpandVolume succeeded for volume %s", util.GetPersistentVolumeClaimQualifiedName(pvc))
		og.recorder.Eventf(pvc, v1.EventTypeNormal, kevents.VolumeResizeSuccess, "%s", successMsg)
	} else {
		pvc, err = og.markForPendingNodeExpansion(pvc, pv)
		resizeResponse.pvc = pvc
		if err != nil {
			msg := fmt.Errorf("error marking pvc %s for node expansion: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
			resizeResponse.err = msg
			return resizeResponse
		}
	}
	return resizeResponse
}

// markForPendingNodeExpansion 将 PVC 标记为等待节点扩容，并保存扩容前容量。
func (og *operationGenerator) markForPendingNodeExpansion(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) (*v1.PersistentVolumeClaim, error) {
	var err error
	pvc, err = util.MarkForFSResize(pvc, og.kubeClient)
	if err != nil {
		msg := fmt.Errorf("error marking pvc %s for node expansion: %v", util.GetPersistentVolumeClaimQualifiedName(pvc), err)
		return pvc, msg
	}
	// 把扩容前容量存入 PV：若节点扩容待处理期间 PVC 被删除，可从 PV 注解恢复大小继续扩容。
	oldCapacity := pvc.Status.Capacity[v1.ResourceStorage]
	err = util.AddAnnPreResizeCapacity(pv, oldCapacity, og.kubeClient)
	if err != nil {
		detailedErr := fmt.Errorf("error updating pv %s annotation (%s) with pre-resize capacity %s: %v", pv.ObjectMeta.Name, util.AnnPreResizeCapacity, oldCapacity.String(), err)
		klog.Warning(detailedErr)
		return pvc, detailedErr
	}
	return pvc, nil
}

// GenerateExpandInUseVolumeFunc 生成在线文件系统扩容操作，无需卸载卷。
func (og *operationGenerator) GenerateExpandInUseVolumeFunc(
	volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) (volumetypes.GeneratedOperations, error) {

	volumePlugin, err :=
		og.volumePluginMgr.FindPluginBySpec(volumeToMount.VolumeSpec)
	if err != nil || volumePlugin == nil {
		return volumetypes.GeneratedOperations{}, volumeToMount.GenerateErrorDetailed("NodeExpandVolume.FindPluginBySpec failed", err)
	}

	fsResizeFunc := func() volumetypes.OperationContext {
		var resizeDone bool
		var eventErr, detailedErr error
		migrated := false

		if currentSize.IsZero() || volumeToMount.DesiredPersistentVolumeSize.IsZero() {
			err := fmt.Errorf("current or new size of the volume is not set")
			eventErr, detailedErr = volumeToMount.GenerateError("NodeExpandvolume.expansion failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		resizeOptions := volume.NodeResizeOptions{
			VolumeSpec: volumeToMount.VolumeSpec,
			DevicePath: volumeToMount.DevicePath,
			OldSize:    currentSize,
			NewSize:    volumeToMount.DesiredPersistentVolumeSize,
		}
		fsVolume, err := util.CheckVolumeModeFilesystem(volumeToMount.VolumeSpec)
		if err != nil {
			eventErr, detailedErr = volumeToMount.GenerateError("NodeExpandvolume.CheckVolumeModeFilesystem failed", err)
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}

		if fsVolume {
			volumeMounter, newMounterErr := volumePlugin.NewMounter(
				volumeToMount.VolumeSpec,
				volumeToMount.Pod)
			if newMounterErr != nil {
				eventErr, detailedErr = volumeToMount.GenerateError("NodeExpandVolume.NewMounter initialization failed", newMounterErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			resizeOptions.DeviceMountPath = volumeMounter.GetPath()

			deviceMountableVolumePlugin, _ := og.volumePluginMgr.FindDeviceMountablePluginBySpec(volumeToMount.VolumeSpec)
			var volumeDeviceMounter volume.DeviceMounter
			if deviceMountableVolumePlugin != nil {
				volumeDeviceMounter, _ = deviceMountableVolumePlugin.NewDeviceMounter()
			}

			if volumeDeviceMounter != nil {
				deviceStagePath, err := volumeDeviceMounter.GetDeviceMountPath(volumeToMount.VolumeSpec)
				if err != nil {
					eventErr, detailedErr = volumeToMount.GenerateError("NodeExpandVolume.GetDeviceMountPath failed", err)
					return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
				}
				resizeOptions.DeviceStagePath = deviceStagePath
			}
		} else {
			// 获取块卷映射插件。
			blockVolumePlugin, err :=
				og.volumePluginMgr.FindMapperPluginBySpec(volumeToMount.VolumeSpec)
			if err != nil {
				eventErr, detailedErr = volumeToMount.GenerateError("MapVolume.FindMapperPluginBySpec failed", err)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			if blockVolumePlugin == nil {
				eventErr, detailedErr = volumeToMount.GenerateError("MapVolume.FindMapperPluginBySpec failed to find BlockVolumeMapper plugin. Volume plugin is nil.", nil)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			blockVolumeMapper, newMapperErr := blockVolumePlugin.NewBlockVolumeMapper(
				volumeToMount.VolumeSpec,
				volumeToMount.Pod)
			if newMapperErr != nil {
				eventErr, detailedErr = volumeToMount.GenerateError("MapVolume.NewBlockVolumeMapper initialization failed", newMapperErr)
				return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
			}

			// 自定义映射插件可自行提供 staging 路径。
			if customBlockVolumeMapper, ok := blockVolumeMapper.(volume.CustomBlockVolumeMapper); ok {
				resizeOptions.DeviceStagePath = customBlockVolumeMapper.GetStagingPath()
			}
		}

		// 在线扩容时卷已发布，直接执行扩容。
		resizeDone, eventErr, detailedErr = og.doOnlineExpansion(volumeToMount, actualStateOfWorld, resizeOptions)
		if eventErr != nil || detailedErr != nil {
			return volumetypes.NewOperationContext(eventErr, detailedErr, migrated)
		}
		if resizeDone {
			return volumetypes.NewOperationContext(nil, nil, migrated)
		}
		klog.InfoS("Waiting for volume to be expandable on the node", "volumeName", volumeToMount.VolumeName)
		return volumetypes.NewOperationContext(nil, nil, migrated)
	}

	eventRecorderFunc := func(err *error) {
		if *err != nil {
			og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.VolumeResizeFailed, "%s", (*err).Error())
		}
	}

	return volumetypes.GeneratedOperations{
		OperationName:     "volume_fs_resize",
		OperationFunc:     fsResizeFunc,
		EventRecorderFunc: eventRecorderFunc,
		CompleteFunc:      util.OperationCompleteHook(util.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeToMount.VolumeSpec), "volume_fs_resize"),
	}, nil
}

// doOnlineExpansion 执行在线扩容并在成功后更新实际状态。
func (og *operationGenerator) doOnlineExpansion(volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	resizeOptions volume.NodeResizeOptions) (bool, error, error) {

	resizeDone, newSize, err := og.nodeExpandVolume(volumeToMount, actualStateOfWorld, resizeOptions)
	if err != nil {
		e1, e2 := volumeToMount.GenerateError("NodeExpandVolume.NodeExpandVolume failed", err)
		klog.Error(e2.Error())
		return false, e1, e2
	}
	if resizeDone {
		markingDone := actualStateOfWorld.MarkVolumeAsResized(volumeToMount.VolumeName, newSize)
		if !markingDone {
			// 失败时返回错误，由调用方记录并重试。
			genericFailureError := fmt.Errorf("unable to mark volume as resized")
			e1, e2 := volumeToMount.GenerateError("NodeExpandVolume.MarkVolumeAsResized failed", genericFailureError)
			return false, e1, e2
		}
		return true, nil, nil
	}
	return false, nil, nil
}

// expandVolumeDuringMount 在挂载流程中按需执行节点扩容。
func (og *operationGenerator) expandVolumeDuringMount(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, rsOpts volume.NodeResizeOptions) (bool, error) {
	supportsExpansion, expandablePlugin := og.checkIfSupportsNodeExpansion(volumeToMount)
	if supportsExpansion {
		pv := volumeToMount.VolumeSpec.PersistentVolume
		pvc, err := og.kubeClient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(context.TODO(), pv.Spec.ClaimRef.Name, metav1.GetOptions{})
		if err != nil {
			// 直接报错而非放任文件系统未扩容；调用方会记录并重试。
			return false, fmt.Errorf("mountVolume.NodeExpandVolume get PVC failed : %v", err)
		}

		pvcStatusCap := pvc.Status.Capacity[v1.ResourceStorage]
		pvSpecCap := pv.Spec.Capacity[v1.ResourceStorage]
		if pvcStatusCap.Cmp(pvSpecCap) < 0 {
			if volumeToMount.VolumeSpec.ReadOnly {
				simpleMsg, detailedMsg := volumeToMount.GenerateMsg("MountVolume.NodeExpandVolume failed", "requested read-only file system")
				klog.Warning(detailedMsg)
				og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FileSystemResizeFailed, "%s", simpleMsg)
				og.recorder.Eventf(pvc, v1.EventTypeWarning, kevents.FileSystemResizeFailed, "%s", simpleMsg)
				return true, nil
			}

			rsOpts.NewSize = pvSpecCap
			rsOpts.OldSize = pvcStatusCap
			rsOpts.VolumeSpec = volumeToMount.VolumeSpec
			resizeOp := nodeResizeOperationOpts{
				vmt:                volumeToMount,
				pvc:                pvc,
				pv:                 pv,
				pluginResizeOpts:   rsOpts,
				volumePlugin:       expandablePlugin,
				actualStateOfWorld: actualStateOfWorld,
			}
			if og.checkForRecoveryFromExpansion(pvc, volumeToMount) {
				// 恢复特性开启时，以 PVC 状态中的 allocated 大小作为新大小。
				rsOpts.NewSize = pvc.Status.AllocatedResources[v1.ResourceStorage]
				resizeOp.pluginResizeOpts = rsOpts
				nodeExpander := newNodeExpander(resizeOp, og.kubeClient, og.recorder)
				resizeFinished, _, err := nodeExpander.expandOnPlugin()
				return resizeFinished, err
			} else {
				return og.legacyCallNodeExpandOnPlugin(resizeOp)
			}
		}
	}
	return true, nil
}

// checkIfSupportsNodeExpansion 判断卷是否支持节点扩容并返回相应插件。
func (og *operationGenerator) checkIfSupportsNodeExpansion(volumeToMount VolumeToMount) (bool, volume.NodeExpandableVolumePlugin) {
	if volumeToMount.VolumeSpec != nil &&
		volumeToMount.VolumeSpec.InlineVolumeSpecForCSIMigration {
		klog.V(4).Infof("This volume %s is a migrated inline volume and is not resizable", volumeToMount.VolumeName)
		return false, nil
	}

	// 尽量获取节点扩容器；并非所有卷都支持节点扩容。
	expandableVolumePlugin, _ :=
		og.volumePluginMgr.FindNodeExpandablePluginBySpec(volumeToMount.VolumeSpec)
	if expandableVolumePlugin != nil &&
		expandableVolumePlugin.RequiresFSResize() &&
		volumeToMount.VolumeSpec.PersistentVolume != nil {
		return true, expandableVolumePlugin
	}
	return false, nil
}

// nodeExpandVolume 在节点侧执行扩容，返回是否完成、当前大小及错误。
func (og *operationGenerator) nodeExpandVolume(
	volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	rsOpts volume.NodeResizeOptions) (bool, resource.Quantity, error) {

	supportsExpansion, expandableVolumePlugin := og.checkIfSupportsNodeExpansion(volumeToMount)

	if supportsExpansion {
		// 使用调用方传入的大小做比较。
		if rsOpts.NewSize.Cmp(rsOpts.OldSize) > 0 {
			pv := volumeToMount.VolumeSpec.PersistentVolume
			pvc, err := og.kubeClient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(context.TODO(), pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			currentSize := pvc.Status.Capacity.Storage()
			if err != nil {
				// 直接报错而非放任文件系统未扩容；调用方会记录并重试。
				return false, *currentSize, fmt.Errorf("mountVolume.NodeExpandVolume get PVC failed : %w", err)
			}

			if volumeToMount.VolumeSpec.ReadOnly {
				simpleMsg, detailedMsg := volumeToMount.GenerateMsg("MountVolume.NodeExpandVolume failed", "requested read-only file system")
				klog.Warning(detailedMsg)
				og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeWarning, kevents.FileSystemResizeFailed, "%s", simpleMsg)
				og.recorder.Eventf(pvc, v1.EventTypeWarning, kevents.FileSystemResizeFailed, "%s", simpleMsg)
				return true, *currentSize, nil
			}
			resizeOp := nodeResizeOperationOpts{
				vmt:                volumeToMount,
				pvc:                pvc,
				pv:                 pv,
				pluginResizeOpts:   rsOpts,
				volumePlugin:       expandableVolumePlugin,
				actualStateOfWorld: actualStateOfWorld,
			}

			if og.checkForRecoveryFromExpansion(pvc, volumeToMount) {
				// 恢复特性开启时，以 PVC 状态中的 allocated 大小作为新大小。
				newSize := pvc.Status.AllocatedResources[v1.ResourceStorage]
				rsOpts.NewSize = newSize
				resizeOp.pluginResizeOpts.NewSize = newSize
				nodeExpander := newNodeExpander(resizeOp, og.kubeClient, og.recorder)
				resizeFinished, newSize, err := nodeExpander.expandOnPlugin()
				return resizeFinished, newSize, err
			} else {
				resizeFinished, err := og.legacyCallNodeExpandOnPlugin(resizeOp)
				return resizeFinished, rsOpts.NewSize, err
			}
		}
	}
	return true, rsOpts.OldSize, nil
}

// checkForRecoveryFromExpansion 判断本次扩容是否应按失败恢复路径处理。
func (og *operationGenerator) checkForRecoveryFromExpansion(pvc *v1.PersistentVolumeClaim, volumeToMount VolumeToMount) bool {
	resizeStatus := pvc.Status.AllocatedResourceStatuses[v1.ResourceStorage]
	allocatedResource := pvc.Status.AllocatedResources
	featureGateStatus := utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure)

	if !featureGateStatus {
		// 即使 RecoverVolumeExpansionFailure 特性门控关闭，只要 resizeStatus 或
		// allocatedResources 已设置，也应视作恢复路径生效。
		if resizeStatus != "" || allocatedResource != nil {
			return true
		}
		return false
	}

	// 特性门控虽已开启，但 resizeStatus 与 allocatedResources 均为空，说明运行的是旧版扩容控制器，
	// 不会填充这些字段。这是版本偏差导致的，故继续使用旧逻辑扩容。
	if resizeStatus == "" && allocatedResource == nil {
		_, detailedMsg := volumeToMount.GenerateMsg("MountVolume.NodeExpandVolume running with", "older external resize controller")
		klog.Warning(detailedMsg)
		return false
	}
	return true
}

// legacyCallNodeExpandOnPlugin 是旧版节点扩容调用方式，不支持从扩容失败中恢复。
// TODO: RecoverVolumeExpansionFailure 特性 GA 后移除此代码。
func (og *operationGenerator) legacyCallNodeExpandOnPlugin(resizeOp nodeResizeOperationOpts) (bool, error) {
	pvc := resizeOp.pvc
	volumeToMount := resizeOp.vmt
	rsOpts := resizeOp.pluginResizeOpts
	actualStateOfWorld := resizeOp.actualStateOfWorld
	expandableVolumePlugin := resizeOp.volumePlugin

	pvcStatusCap := pvc.Status.Capacity[v1.ResourceStorage]

	nodeName := volumeToMount.Pod.Spec.NodeName

	var err error

	// 已请求文件系统扩容，继续执行。
	klog.V(4).InfoS(volumeToMount.GenerateMsgDetailed("MountVolume.NodeExpandVolume entering", fmt.Sprintf("DevicePath %q", volumeToMount.DevicePath)), "pod", klog.KObj(volumeToMount.Pod))

	rsOpts.VolumeSpec = volumeToMount.VolumeSpec

	_, resizeErr := expandableVolumePlugin.NodeExpand(rsOpts)
	if resizeErr != nil {
		// 这是临时方案，待 RecoverFromVolumeExpansionFailure 特性 GA 后移除。
		// 该特性开启时不会走到这里，因为 kubelet 会先等待 VolumeExpansionPendingOnNode 再扩容。
		if volumetypes.IsOperationNotSupportedError(resizeErr) {
			klog.V(4).InfoS(volumeToMount.GenerateMsgDetailed("MountVolume.NodeExpandVolume failed", "NodeExpandVolume not supported"), "pod", klog.KObj(volumeToMount.Pod))
			return true, nil
		}

		// 驱动返回 FailedPrecondition 表示本节点不应重试扩容，但扩容操作不应阻塞挂载。
		if volumetypes.IsFailedPreconditionError(resizeErr) {
			actualStateOfWorld.MarkForInUseExpansionError(volumeToMount.VolumeName)
			klog.Error(volumeToMount.GenerateErrorDetailed("MountVolume.NodeExpandVolume failed", resizeErr).Error())
			return true, nil
		}
		return false, resizeErr
	}

	simpleMsg, detailedMsg := volumeToMount.GenerateMsg("MountVolume.NodeExpandVolume succeeded", nodeName)
	og.recorder.Eventf(volumeToMount.Pod, v1.EventTypeNormal, kevents.FileSystemResizeSuccess, "%s", simpleMsg)
	og.recorder.Eventf(pvc, v1.EventTypeNormal, kevents.FileSystemResizeSuccess, "%s", simpleMsg)
	klog.InfoS(detailedMsg, "pod", klog.KObj(volumeToMount.Pod))

	// PVC 已有新大小则无需更新。
	if pvcStatusCap.Cmp(rsOpts.NewSize) >= 0 {
		return true, nil
	}

	// 文件系统扩容成功，更新 PVC 容量使其与 PV 一致。
	_, err = util.MarkFSResizeFinished(pvc, rsOpts.NewSize, og.kubeClient)
	if err != nil {
		// 重试时 NodeExpandVolume 会被再次调用，但不会重复操作。
		return false, fmt.Errorf("mountVolume.NodeExpandVolume update PVC status failed : %v", err)
	}
	return true, nil
}

// checkMountOptionSupport 在卷规格指定了挂载选项时检查插件是否支持。
func checkMountOptionSupport(og *operationGenerator, volumeToMount VolumeToMount, plugin volume.VolumePlugin) error {
	mountOptions := util.MountOptionFromSpec(volumeToMount.VolumeSpec)

	if len(mountOptions) > 0 && !plugin.SupportsMountOption() {
		return fmt.Errorf("mount options are not supported for this volume type")
	}
	return nil
}

// checkNodeAffinity 根据 PV 的节点亲和性检查当前节点标签是否匹配，
// 确保不会挂载不属于本节点的卷。
func checkNodeAffinity(og *operationGenerator, volumeToMount VolumeToMount) error {
	pv := volumeToMount.VolumeSpec.PersistentVolume
	if pv != nil {
		nodeLabels, err := og.volumePluginMgr.Host.GetNodeLabels()
		if err != nil {
			return err
		}
		err = storagehelpers.CheckNodeAffinity(pv, nodeLabels)
		if err != nil {
			return err
		}
	}
	return nil
}

// isDeviceOpened 检查设备路径是否仍被系统其他位置占用。
func isDeviceOpened(deviceToDetach AttachedVolume, hostUtil hostutil.HostUtils) (bool, error) {
	isDevicePath, devicePathErr := hostUtil.PathIsDevice(deviceToDetach.DevicePath)
	var deviceOpened bool
	var deviceOpenedErr error
	if !isDevicePath && devicePathErr == nil ||
		(devicePathErr != nil && strings.Contains(devicePathErr.Error(), "does not exist")) {
		// 不是设备路径或路径不存在。
		// TODO: 参见 #36092。
		klog.V(3).Infof("The path isn't device path or doesn't exist. Skip checking device path: %s", deviceToDetach.DevicePath)
		deviceOpened = false
	} else if devicePathErr != nil {
		return false, deviceToDetach.GenerateErrorDetailed("PathIsDevice failed", devicePathErr)
	} else {
		deviceOpened, deviceOpenedErr = hostUtil.DeviceOpened(deviceToDetach.DevicePath)
		if deviceOpenedErr != nil {
			return false, deviceToDetach.GenerateErrorDetailed("DeviceOpened failed", deviceOpenedErr)
		}
	}
	return deviceOpened, nil
}

// findDetachablePluginBySpec 是 VolumePluginMgr.FindAttachablePluginByName() 的变体：
// 对 CSI 插件绕过 CanAttach() 检查，假定所有 CSI 插件都支持分离。
// 这样 CSI 卷即使处于不确定状态，也能通过分离操作完成清理，无论其是否真的支持 attach/detach。
func findDetachablePluginBySpec(spec *volume.Spec, pm *volume.VolumePluginMgr) (volume.AttachableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if attachableVolumePlugin, ok := volumePlugin.(volume.AttachableVolumePlugin); ok {
		if attachableVolumePlugin.GetPluginName() == "kubernetes.io/csi" {
			return attachableVolumePlugin, nil
		}
		if canAttach, err := attachableVolumePlugin.CanAttach(spec); err != nil {
			return nil, err
		} else if canAttach {
			return attachableVolumePlugin, nil
		}
	}
	return nil, nil
}

// getMigratedStatusBySpec 返回卷规格的迁移标记；nil 规格按非迁移处理。
func getMigratedStatusBySpec(spec *volume.Spec) bool {
	migrated := false
	if spec != nil {
		migrated = spec.Migrated
	}
	return migrated
}
