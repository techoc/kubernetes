/*
Copyright 2022 The Kubernetes Authors.

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

package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	volumepkg "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/nestedpendingoperations"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/mount-utils"
)

// Reconciler 周期性对比期望状态与实际状态，触发 attach、detach、mount、unmount 操作。
// 注意：它与 attach/detach 控制器中的 Reconciler 不同——
// 这里同步的是 kubelet volume manager 的状态。
type Reconciler interface {
	// Run 启动周期性的同步循环：检查应挂载/卸载的卷是否已挂载/卸载，
	// 不一致时触发 mount/unmount 操作。
	// 启用 attach/detach 管理时，还会检查卷的挂接状态并触发相应操作。
	Run(ctx context.Context, stopCh <-chan struct{})

	// StatesHasBeenSynced 仅在 kubelet 启动后至少完成一次状态同步时返回 true。
	StatesHasBeenSynced() bool
}

// NewReconciler 创建 Reconciler 实例。
// controllerAttachDetachEnabled 为 true 表示由 attach/detach 控制器负责本节点的挂接操作。
// loopSleepDuration 是同步循环的休眠间隔；waitForAttachTimeout 是 Mount 等待卷挂接的超时。
// nodeName 用于 Attach/Detach；desiredStateOfWorld/actualStateOfWorld 是两侧状态缓存。
// populatorHasAddedPods 用于判断 Pod 是否已全部加入期望状态。
// operationExecutor 安全地触发 attach/detach/mount/unmount，避免同一卷并发操作。
// mounter、hostutil、volumePluginMgr 均由 kubelet 传入。
func NewReconciler(
	kubeClient clientset.Interface,
	controllerAttachDetachEnabled bool,
	loopSleepDuration time.Duration,
	waitForAttachTimeout time.Duration,
	nodeName types.NodeName,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	populatorHasAddedPods func() bool,
	operationExecutor operationexecutor.OperationExecutor,
	mounter mount.Interface,
	hostutil hostutil.HostUtils,
	volumePluginMgr *volumepkg.VolumePluginMgr,
	kubeletPodsDir string) Reconciler {
	return &reconciler{
		kubeClient:                      kubeClient,
		controllerAttachDetachEnabled:   controllerAttachDetachEnabled,
		loopSleepDuration:               loopSleepDuration,
		waitForAttachTimeout:            waitForAttachTimeout,
		nodeName:                        nodeName,
		desiredStateOfWorld:             desiredStateOfWorld,
		actualStateOfWorld:              actualStateOfWorld,
		populatorHasAddedPods:           populatorHasAddedPods,
		operationExecutor:               operationExecutor,
		mounter:                         mounter,
		hostutil:                        hostutil,
		skippedDuringReconstruction:     map[v1.UniqueVolumeName]*globalVolumeInfo{},
		volumePluginMgr:                 volumePluginMgr,
		kubeletPodsDir:                  kubeletPodsDir,
		timeOfLastSync:                  time.Time{},
		volumesFailedReconstruction:     make([]podVolume, 0),
		volumesNeedUpdateFromNodeStatus: make([]v1.UniqueVolumeName, 0),
	}
}

// reconciler 是 Reconciler 的具体实现。
type reconciler struct {
	kubeClient                    clientset.Interface
	controllerAttachDetachEnabled bool
	loopSleepDuration             time.Duration
	waitForAttachTimeout          time.Duration
	nodeName                      types.NodeName
	desiredStateOfWorld           cache.DesiredStateOfWorld
	actualStateOfWorld            cache.ActualStateOfWorld
	populatorHasAddedPods         func() bool
	operationExecutor             operationexecutor.OperationExecutor
	mounter                       mount.Interface
	hostutil                      hostutil.HostUtils
	volumePluginMgr               *volumepkg.VolumePluginMgr
	skippedDuringReconstruction   map[v1.UniqueVolumeName]*globalVolumeInfo
	kubeletPodsDir                string
	// lock 保护 timeOfLastSync 的读写。
	timeOfLastSyncLock              sync.Mutex
	timeOfLastSync                  time.Time
	volumesFailedReconstruction     []podVolume
	volumesNeedUpdateFromNodeStatus []v1.UniqueVolumeName
}

// unmountVolumes 卸载那些已不在期望状态中的卷。
func (rc *reconciler) unmountVolumes(logger klog.Logger) {
	// 确保应卸载的卷都被卸载。
	for _, mountedVolume := range rc.actualStateOfWorld.GetAllMountedVolumes() {
		if rc.operationExecutor.IsOperationPending(mountedVolume.VolumeName, mountedVolume.PodName, nestedpendingoperations.EmptyNodeName) {
			continue
		}
		if !rc.desiredStateOfWorld.PodExistsInVolume(mountedVolume.PodName, mountedVolume.VolumeName, mountedVolume.SELinuxMountContext) {
			// 卷仍挂载但期望状态中已不存在，触发卸载。
			logger.V(5).Info(mountedVolume.GenerateMsgDetailed("Starting operationExecutor.UnmountVolume", ""))
			err := rc.operationExecutor.UnmountVolume(
				mountedVolume.MountedVolume, rc.actualStateOfWorld, rc.kubeletPodsDir)
			if err != nil && !isExpectedError(err) {
				logger.Error(err, mountedVolume.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.UnmountVolume failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error())
			}
			if err == nil {
				logger.Info(mountedVolume.GenerateMsgDetailed("operationExecutor.UnmountVolume started", ""))
			}
		}
	}
}

// mountOrAttachVolumes 处理需要挂载或挂接的卷，并按错误类型分流。
func (rc *reconciler) mountOrAttachVolumes(logger klog.Logger) {
	// 确保应挂接/挂载的卷都已就位。
	for _, volumeToMount := range rc.desiredStateOfWorld.GetVolumesToMount() {
		if rc.operationExecutor.IsOperationPending(volumeToMount.VolumeName, nestedpendingoperations.EmptyUniquePodName, nestedpendingoperations.EmptyNodeName) {
			continue
		}
		volMounted, devicePath, err := rc.actualStateOfWorld.PodExistsInVolume(logger, volumeToMount.PodName, volumeToMount.VolumeName, volumeToMount.DesiredPersistentVolumeSize, volumeToMount.SELinuxLabel)
		volumeToMount.DevicePath = devicePath
		if cache.IsSELinuxMountMismatchError(err) {
			// 卷已挂载但 SELinux 上下文不符；它会在 unmountVolumes / unmountDetachDevices 中
			// 被卸载，随后从 actualStateOfWorld 移除。
			rc.desiredStateOfWorld.AddErrorToPod(volumeToMount.PodName, err.Error())
			continue
		} else if cache.IsVolumeNotAttachedError(err) {
			rc.waitForVolumeAttach(logger, volumeToMount)
		} else if !volMounted || cache.IsRemountRequiredError(err) {
			rc.mountAttachedVolumes(logger, volumeToMount, err)
		} else if cache.IsFSResizeRequiredError(err) {
			fsResizeRequiredErr, _ := err.(cache.FsResizeRequiredError)
			rc.expandVolume(logger, volumeToMount, fsResizeRequiredErr.CurrentSize)
		}
	}
}

// expandVolume 对使用中的卷执行在线扩容。
func (rc *reconciler) expandVolume(logger klog.Logger, volumeToMount cache.VolumeToMount, currentSize resource.Quantity) {
	logger.V(4).Info(volumeToMount.GenerateMsgDetailed("Starting operationExecutor.ExpandInUseVolume", ""), "pod", klog.KObj(volumeToMount.Pod))
	err := rc.operationExecutor.ExpandInUseVolume(volumeToMount.VolumeToMount, rc.actualStateOfWorld, currentSize)

	if err != nil && !isExpectedError(err) {
		logger.Error(err, volumeToMount.GenerateErrorDetailed("operationExecutor.ExpandInUseVolume failed", err).Error(), "pod", klog.KObj(volumeToMount.Pod))
	}

	if err == nil {
		logger.V(4).Info(volumeToMount.GenerateMsgDetailed("operationExecutor.ExpandInUseVolume started", ""), "pod", klog.KObj(volumeToMount.Pod))
	}
}

// mountAttachedVolumes 挂载已挂接到节点的卷；需要时执行重新挂载。
func (rc *reconciler) mountAttachedVolumes(logger klog.Logger, volumeToMount cache.VolumeToMount, podExistError error) {
	// 卷未挂载，或已挂载但需要重新挂载。
	remountingLogStr := ""
	isRemount := cache.IsRemountRequiredError(podExistError)
	if isRemount {
		remountingLogStr = "Volume is already mounted to pod, but remount was requested."
	}
	logger.V(4).Info(volumeToMount.GenerateMsgDetailed("Starting operationExecutor.MountVolume", remountingLogStr), "pod", klog.KObj(volumeToMount.Pod))
	err := rc.operationExecutor.MountVolume(
		rc.waitForAttachTimeout,
		volumeToMount.VolumeToMount,
		rc.actualStateOfWorld,
		isRemount)
	if err != nil && !isExpectedError(err) {
		logger.Error(err, volumeToMount.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.MountVolume failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error(), "pod", klog.KObj(volumeToMount.Pod))
	}
	if err == nil {
		if remountingLogStr == "" {
			logger.V(1).Info(volumeToMount.GenerateMsgDetailed("operationExecutor.MountVolume started", remountingLogStr), "pod", klog.KObj(volumeToMount.Pod))
		} else {
			logger.V(5).Info(volumeToMount.GenerateMsgDetailed("operationExecutor.MountVolume started", remountingLogStr), "pod", klog.KObj(volumeToMount.Pod))
		}
	}
}

// waitForVolumeAttach 根据是否启用 kubelet attach 决定等待控制器挂接还是主动挂接。
func (rc *reconciler) waitForVolumeAttach(logger klog.Logger, volumeToMount cache.VolumeToMount) {
	if rc.controllerAttachDetachEnabled || !volumeToMount.PluginIsAttachable {
		// 这种情况不要启动 goroutine，也不要触发无谓的指数退避。
		if volumeToMount.PluginIsAttachable && !volumeToMount.ReportedInUse {
			logger.V(5).Info(volumeToMount.GenerateMsgDetailed("operationExecutor.VerifyControllerAttachedVolume failed", " volume not marked in-use"), "pod", klog.KObj(volumeToMount.Pod))
			return
		}
		// 卷未挂接（或插件不支持挂接）且 kubelet attach 已禁用时，
		// 等待控制器完成挂接。
		logger.V(5).Info(volumeToMount.GenerateMsgDetailed("Starting operationExecutor.VerifyControllerAttachedVolume", ""), "pod", klog.KObj(volumeToMount.Pod))
		err := rc.operationExecutor.VerifyControllerAttachedVolume(
			logger,
			volumeToMount.VolumeToMount,
			rc.nodeName,
			rc.actualStateOfWorld)
		if err != nil && !isExpectedError(err) {
			logger.Error(err, volumeToMount.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.VerifyControllerAttachedVolume failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error(), "pod", klog.KObj(volumeToMount.Pod))
		}
		if err == nil {
			logger.Info(volumeToMount.GenerateMsgDetailed("operationExecutor.VerifyControllerAttachedVolume started", ""), "pod", klog.KObj(volumeToMount.Pod))
		}
	} else {
		// 卷未挂接到节点、kubelet attach 已启用且插件支持挂接，直接执行挂接。
		volumeToAttach := operationexecutor.VolumeToAttach{
			VolumeName:    volumeToMount.VolumeName,
			VolumeSpec:    volumeToMount.VolumeSpec,
			NodeName:      rc.nodeName,
			ScheduledPods: []*v1.Pod{volumeToMount.Pod},
		}
		logger.V(5).Info(volumeToAttach.GenerateMsgDetailed("Starting operationExecutor.AttachVolume", ""), "pod", klog.KObj(volumeToMount.Pod))
		err := rc.operationExecutor.AttachVolume(logger, volumeToAttach, rc.actualStateOfWorld)
		if err != nil && !isExpectedError(err) {
			logger.Error(err, volumeToMount.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.AttachVolume failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error(), "pod", klog.KObj(volumeToMount.Pod))
		}
		if err == nil {
			logger.Info(volumeToMount.GenerateMsgDetailed("operationExecutor.AttachVolume started", ""), "pod", klog.KObj(volumeToMount.Pod))
		}
	}
}

// unmountDetachDevices 卸载设备并解除挂接，处理不再被任何 Pod 使用的卷。
func (rc *reconciler) unmountDetachDevices(logger klog.Logger) {
	for _, attachedVolume := range rc.actualStateOfWorld.GetUnmountedVolumes() {
		// 先检查是否有挂起操作，避免把正在挂载的卷误标为已解除挂接。
		if !rc.desiredStateOfWorld.VolumeExists(attachedVolume.VolumeName, attachedVolume.SELinuxMountContext) &&
			!rc.operationExecutor.IsOperationPending(attachedVolume.VolumeName, nestedpendingoperations.EmptyUniquePodName, nestedpendingoperations.EmptyNodeName) {

			// 重新读取实际状态，确认期间没有新的挂载发生。
			// 这是安全的：上面已确认没有挂起操作，且只有本 reconciler 会新增操作。
			attachedVolume, _ = rc.actualStateOfWorld.GetAttachedVolume(attachedVolume.VolumeName)
			if attachedVolume.DeviceMayBeMounted() {
				// 卷仍全局挂载在设备上，先卸载设备。
				logger.V(5).Info(attachedVolume.GenerateMsgDetailed("Starting operationExecutor.UnmountDevice", ""))
				err := rc.operationExecutor.UnmountDevice(
					attachedVolume.AttachedVolume, rc.actualStateOfWorld, rc.hostutil)
				if err != nil && !isExpectedError(err) {
					logger.Error(err, attachedVolume.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.UnmountDevice failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error())
				}
				if err == nil {
					logger.Info(attachedVolume.GenerateMsgDetailed("operationExecutor.UnmountDevice started", ""))
				}
			} else {
				// 卷仍挂接在节点上：kubelet 不负责解除挂接，
				// 或该卷插件不支持挂接，因此直接标记为已解除挂接。
				if rc.controllerAttachDetachEnabled || !attachedVolume.PluginIsAttachable {
					rc.actualStateOfWorld.MarkVolumeAsDetached(attachedVolume.VolumeName, attachedVolume.NodeName)
					logger.Info(attachedVolume.GenerateMsgDetailed("Volume detached", fmt.Sprintf("DevicePath %q", attachedVolume.DevicePath)))
				} else {
					// 仅在 kubelet detach 启用时执行解除挂接。
					logger.V(5).Info(attachedVolume.GenerateMsgDetailed("Starting operationExecutor.DetachVolume", ""))
					err := rc.operationExecutor.DetachVolume(
						logger, attachedVolume.AttachedVolume, false /* verifySafeToDetach */, rc.actualStateOfWorld)
					if err != nil && !isExpectedError(err) {
						logger.Error(err, attachedVolume.GenerateErrorDetailed(fmt.Sprintf("operationExecutor.DetachVolume failed (controllerAttachDetachEnabled %v)", rc.controllerAttachDetachEnabled), err).Error())
					}
					if err == nil {
						logger.Info(attachedVolume.GenerateMsgDetailed("operationExecutor.DetachVolume started", ""))
					}
				}
			}
		}
	}
}

// isExpectedError 忽略 IsAlreadyExists 和 IsExponentialBackoff：
// 这两类错误在并发同步中是预期内的。
func isExpectedError(err error) bool {
	return nestedpendingoperations.IsAlreadyExists(err) || exponentialbackoff.IsExponentialBackoff(err) || operationexecutor.IsMountFailedPreconditionError(err)
}
