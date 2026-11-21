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

package volumemanager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	k8stypes "k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/metrics"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/populator"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/reconciler"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/volumehealth"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

const (
	// reconciler 循环两次执行之间的等待时间。
	reconcilerLoopSleepPeriod = 100 * time.Millisecond

	// DesiredStateOfWorldPopulator 循环两次执行之间的等待时间。
	desiredStateOfWorldPopulatorLoopSleepPeriod = 100 * time.Millisecond

	// podAttachAndMountTimeout 是 WaitForAttachAndMount 等待指定 Pod 全部卷附加并挂载的上限。
	// 云操作可能耗时数分钟，但这里只等 2 分钟，因为 kubelet 会在下一轮同步时重试，
	// 从而释放该 Pod 的 goroutine 去处理更新的请求（例如删除 Pod）。
	// 取值刻意比 2 分钟多一点，便于从超时时间识别出是本常量触发的超时。
	podAttachAndMountTimeout = 2*time.Minute + 3*time.Second

	// GetVolumesForPod 重试前的等待间隔。
	podAttachAndMountRetryInterval = 300 * time.Millisecond

	// waitForAttachTimeout 是 operationexecutor.Mount 等待卷附加的上限。
	// 某些插件的附加操作可能耗时数分钟，因此设为 10 分钟；等待期间只阻塞同一设备上的操作，
	// 不影响其他设备。
	waitForAttachTimeout = 10 * time.Minute

	// VolumeAttachmentLimitExceededReason 是节点卷附加数量达到上限时拒绝 Pod 的原因值。
	VolumeAttachmentLimitExceededReason = "VolumeAttachmentLimitExceeded"
)

// VolumeManager 运行一组异步循环，依据调度到本节点的 Pod 计算需要
// 附加/挂载/卸载/解附的卷，并执行相应操作。
type VolumeManager interface {
	// Run 启动卷管理器及其控制的所有异步循环。
	Run(ctx context.Context, sourcesReady config.SourcesReady)

	// WaitForAttachAndMount 处理 Pod 引用的卷，阻塞直到它们全部附加并挂载
	// （即反映到实际状态中）；超过 podAttachAndMountTimeout 仍未完成则返回错误。
	WaitForAttachAndMount(ctx context.Context, pod *v1.Pod) error

	// WaitForUnmount 处理 Pod 引用的卷，阻塞直到它们全部卸载；超时返回错误。
	WaitForUnmount(ctx context.Context, pod *v1.Pod) error

	// WaitForAllPodsUnmount 是 WaitForUnmount 的批量版本，等到所有 Pod 的卷都卸载完成；
	// 只要有一个 Pod 超时未卸载即返回错误。
	WaitForAllPodsUnmount(ctx context.Context, pods []*v1.Pod) error

	// GetMountedVolumesForPod 返回 Pod 引用、且处于期望状态并已实际附加挂载的卷。
	// 键为外层卷名（pod.Spec.Volumes[x].Name）；Pod 没有卷时返回空映射。
	GetMountedVolumesForPod(podName types.UniquePodName) container.VolumeMap

	// HasPossiblyMountedVolumesForPod 判断 Pod 是否有已成功挂载或状态不确定
	// （插件可能正在挂载）的卷。
	HasPossiblyMountedVolumesForPod(podName types.UniquePodName) bool

	// GetExtraSupplementalGroupsForPod 返回来自 PV 注解的额外补充组。
	GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64

	// GetVolumesInUse 返回实现 volume.Attacher 接口、且根据期望/实际状态仍在使用中的卷。
	// 卷一旦进入期望状态即视为“使用中”（表示它*应*附加到本节点），
	// 直到同时从期望状态和实际状态移除，或实际状态显示已卸载。
	GetVolumesInUse() []v1.UniqueVolumeName

	// ReconcilerStatesHasBeenSynced 仅在 kubelet 启动后 reconciler 的实际状态至少同步过一次后返回 true，
	// 此时才可安全地用实际状态更新已挂载卷列表。
	ReconcilerStatesHasBeenSynced() bool

	// VolumeIsAttached 判断指定卷是否已附加到本节点。
	VolumeIsAttached(volumeName v1.UniqueVolumeName) bool

	// MarkVolumesAsReportedInUse 标记指定卷已成功上报为节点状态中的“使用中”。
	MarkVolumesAsReportedInUse(volumesReportedAsInUse []v1.UniqueVolumeName)

	// ResizeEphemeralVolume 直接触发指定卷的扩容，
	// 适用于支持在线扩容且需要与容器运行时操作严格协调的卷。
	ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *resource.Quantity) error
}

// PodStateProvider 用于判断 Pod 是否即将被终止。
type PodStateProvider interface {
	// ShouldPodContainersBeTerminating 判断该 Pod 的容器是否应进入终止流程。
	ShouldPodContainersBeTerminating(k8stypes.UID) bool
	// ShouldPodRuntimeBeRemoved 判断该 Pod 的运行时资源是否应被清理。
	ShouldPodRuntimeBeRemoved(k8stypes.UID) bool
}

// PodManager 是卷管理器观察 kubelet 实际状态所需的方法子集。
// 方法语义见 pkg/k8s.io/kubernetes/pkg/kubelet/pod.Manager。
type PodManager interface {
	// GetPodByUID 按 UID 查询 Pod。
	GetPodByUID(k8stypes.UID) (*v1.Pod, bool)
	// GetPods 返回当前已知的全部 Pod。
	GetPods() []*v1.Pod
}

// NewVolumeManager 返回 VolumeManager 接口的具体实现。
//
// kubeClient：DesiredStateOfWorldPopulator 用来访问 API server 获取 PV/PVC 的客户端。
// volumePluginMgr：访问卷插件的管理器，必须已初始化。
func NewVolumeManager(
	controllerAttachDetachEnabled bool,
	nodeName k8stypes.NodeName,
	podManager PodManager,
	podStateProvider PodStateProvider,
	kubeClient clientset.Interface,
	volumePluginMgr *volume.VolumePluginMgr,
	mounter mount.Interface,
	hostutil hostutil.HostUtils,
	kubeletPodsDir string,
	recorder record.EventRecorder,
	blockVolumePathHandler volumepathhandler.BlockVolumePathHandler,
	volumeHealthStatusUpdater volumehealth.StatusUpdater,
	volumeHealthProbeInterval time.Duration) VolumeManager {

	seLinuxTranslator := util.NewSELinuxLabelTranslator()
	vm := &volumeManager{
		kubeClient:          kubeClient,
		volumePluginMgr:     volumePluginMgr,
		desiredStateOfWorld: cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator),
		actualStateOfWorld:  cache.NewActualStateOfWorld(nodeName, volumePluginMgr),
		operationExecutor: operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
			kubeClient,
			volumePluginMgr,
			recorder,
			blockVolumePathHandler)),
	}

	intreeToCSITranslator := csitrans.New()
	csiMigratedPluginManager := csimigration.NewPluginManager(intreeToCSITranslator)

	vm.intreeToCSITranslator = intreeToCSITranslator
	vm.csiMigratedPluginManager = csiMigratedPluginManager
	vm.desiredStateOfWorldPopulator = populator.NewDesiredStateOfWorldPopulator(
		kubeClient,
		desiredStateOfWorldPopulatorLoopSleepPeriod,
		podManager,
		podStateProvider,
		vm.desiredStateOfWorld,
		vm.actualStateOfWorld,
		csiMigratedPluginManager,
		intreeToCSITranslator,
		volumePluginMgr)
	vm.reconciler = reconciler.NewReconciler(
		kubeClient,
		controllerAttachDetachEnabled,
		reconcilerLoopSleepPeriod,
		waitForAttachTimeout,
		nodeName,
		vm.desiredStateOfWorld,
		vm.actualStateOfWorld,
		vm.desiredStateOfWorldPopulator.HasAddedPods,
		vm.operationExecutor,
		mounter,
		hostutil,
		volumePluginMgr,
		kubeletPodsDir)

	if utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		vm.volumeHealthManager = volumehealth.NewManager(
			vm.desiredStateOfWorld,
			vm.actualStateOfWorld,
			volumeHealthStatusUpdater,
			volumeHealthProbeInterval)
	}
	return vm
}

// volumeManager 实现 VolumeManager 接口。
type volumeManager struct {
	// kubeClient 是 DesiredStateOfWorldPopulator 访问 API server 获取 PV/PVC 的客户端。
	kubeClient clientset.Interface

	// volumePluginMgr 是访问卷插件的管理器，必须已初始化。
	volumePluginMgr *volume.VolumePluginMgr

	// desiredStateOfWorld 保存卷管理器的期望状态：哪些卷应被附加、哪些 Pod 引用了它们。
	// 该结构由 desired state of world populator 借助 kubelet PodManager 填充。
	desiredStateOfWorld cache.DesiredStateOfWorld

	// actualStateOfWorld 保存卷管理器的实际状态：本节点已附加哪些卷、卷已挂载给哪些 Pod。
	// reconciler 触发的附加/解附/挂载/卸载操作成功后更新该结构。
	actualStateOfWorld cache.ActualStateOfWorld

	// operationExecutor 用于发起异步的附加、解附、挂载和卸载操作。
	operationExecutor operationexecutor.OperationExecutor

	// reconciler 运行异步周期循环，通过 operationExecutor 触发附加/解附/挂载/卸载，
	// 使实际状态逐步逼近期望状态。
	reconciler reconciler.Reconciler

	// desiredStateOfWorldPopulator 运行异步周期循环，利用 kubelet PodManager 填充期望状态。
	desiredStateOfWorldPopulator populator.DesiredStateOfWorldPopulator

	// volumeHealthManager 周期探测 CSI 卷/存储健康，并写入 PodStatus.VolumeHealth /
	// CSINode.Status.StorageHealth。
	volumeHealthManager volumehealth.Manager

	// csiMigratedPluginManager 跟踪各插件的 CSI 迁移状态。
	csiMigratedPluginManager csimigration.PluginManager

	// intreeToCSITranslator 把 in-tree 卷 spec 转换为 CSI spec。
	intreeToCSITranslator csimigration.InTreeToCSITranslator
}

// VolumeAttachLimitExceededError 表示节点卷附加数量已达上限而拒绝 Pod，
// 并携带未挂载、未附加及未进入期望状态的卷，便于定位原因。
type VolumeAttachLimitExceededError struct {
	UnmountedVolumes  []string
	UnattachedVolumes []string
	VolumesNotInDSW   []string
	OriginalError     error
}

// Error 实现 error 接口，输出各类未完成卷与底层错误。
func (e *VolumeAttachLimitExceededError) Error() string {
	return fmt.Sprintf("Node has reached its volume attachment limit, rejecting pod. unmounted volumes=%v, unattached volumes=%v, failed to process volumes=%v: %v",
		e.UnmountedVolumes, e.UnattachedVolumes, e.VolumesNotInDSW, e.OriginalError)
}

// Run 启动 populator、reconciler 及可选的卷健康管理器，并注册指标，直到 ctx 取消。
func (vm *volumeManager) Run(ctx context.Context, sourcesReady config.SourcesReady) {
	logger := klog.FromContext(ctx)
	defer runtime.HandleCrashWithContext(ctx)

	if vm.kubeClient != nil {
		// kubeClient 可用时启动 CSIDriver informer，供卷插件感知驱动信息。
		go vm.volumePluginMgr.Run(ctx.Done())
	}

	go vm.desiredStateOfWorldPopulator.Run(ctx, sourcesReady)
	logger.V(2).Info("The desired_state_of_world populator starts")

	logger.Info("Starting Kubelet Volume Manager")
	go vm.reconciler.Run(ctx, ctx.Done())

	if vm.volumeHealthManager != nil {
		go vm.volumeHealthManager.Run(ctx)
	}

	metrics.Register(vm.actualStateOfWorld, vm.desiredStateOfWorld, vm.volumePluginMgr)

	<-ctx.Done()
	logger.Info("Shutting down Kubelet Volume Manager")
}

// GetMountedVolumesForPod 把内部挂载卷转换为容器运行时使用的 VolumeMap。
func (vm *volumeManager) GetMountedVolumesForPod(podName types.UniquePodName) container.VolumeMap {
	podVolumes := make(container.VolumeMap)
	for name, mountedVolume := range vm.getMountedVolumes(podName) {
		podVolumes[name] = container.VolumeInfo{
			Mounter:             mountedVolume.Mounter,
			BlockVolumeMapper:   mountedVolume.BlockVolumeMapper,
			ReadOnly:            mountedVolume.VolumeSpec.ReadOnly,
			InnerVolumeSpecName: mountedVolume.InnerVolumeSpecName,
		}
	}
	return podVolumes
}

// HasPossiblyMountedVolumesForPod 判断实际状态中是否存在该 Pod 可能已挂载的卷。
func (vm *volumeManager) HasPossiblyMountedVolumesForPod(podName types.UniquePodName) bool {
	return len(vm.actualStateOfWorld.GetPossiblyMountedVolumesForPod(podName)) > 0
}

// GetExtraSupplementalGroupsForPod 汇总卷 GID 注解带来的额外补充组，并剔除 Pod 已声明的组。
func (vm *volumeManager) GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64 {
	podName := util.GetUniquePodName(pod)
	supplementalGroups := sets.New[string]()

	// 卷 GID 注解可能带来 Pod 未显式声明的补充组。
	for _, mountedVolume := range vm.actualStateOfWorld.GetMountedVolumesForPod(podName) {
		if mountedVolume.VolumeGIDValue != "" {
			supplementalGroups.Insert(mountedVolume.VolumeGIDValue)
		}
	}

	result := make([]int64, 0, supplementalGroups.Len())
	for _, group := range supplementalGroups.UnsortedList() {
		iGroup, extra := getExtraSupplementalGID(group, pod)
		if !extra {
			continue
		}

		result = append(result, int64(iGroup))
	}

	return result
}

// GetVolumesInUse 合并期望与实际状态中可附加的卷，排序去重后返回。
func (vm *volumeManager) GetVolumesInUse() []v1.UniqueVolumeName {
	// 同时上报期望与实际状态中的卷：一旦决定卷*应*附加到本节点即标记为使用中，
	// 直到其被安全卸载。
	desiredVolumes := vm.desiredStateOfWorld.GetVolumesToMount()
	allAttachedVolumes := vm.actualStateOfWorld.GetAttachedVolumes()
	volumesToReportInUse := make([]v1.UniqueVolumeName, 0, len(desiredVolumes)+len(allAttachedVolumes))

	for _, volume := range desiredVolumes {
		if volume.PluginIsAttachable {
			volumesToReportInUse = append(volumesToReportInUse, volume.VolumeName)
		}
	}

	for _, volume := range allAttachedVolumes {
		if volume.PluginIsAttachable {
			volumesToReportInUse = append(volumesToReportInUse, volume.VolumeName)
		}
	}

	slices.Sort(volumesToReportInUse)
	return slices.Compact(volumesToReportInUse)
}

// ReconcilerStatesHasBeenSynced 转发 reconciler 的同步状态。
func (vm *volumeManager) ReconcilerStatesHasBeenSynced() bool {
	return vm.reconciler.StatesHasBeenSynced()
}

// VolumeIsAttached 查询实际状态中是否存在该卷。
func (vm *volumeManager) VolumeIsAttached(
	volumeName v1.UniqueVolumeName) bool {
	return vm.actualStateOfWorld.VolumeExists(volumeName)
}

// MarkVolumesAsReportedInUse 把已上报的卷集合同步到期望状态。
func (vm *volumeManager) MarkVolumesAsReportedInUse(
	volumesReportedAsInUse []v1.UniqueVolumeName) {
	vm.desiredStateOfWorld.MarkVolumesReportedInUse(volumesReportedAsInUse)
}

// WaitForAttachAndMount 轮询等待 Pod 的期望卷全部挂载，超时后汇总未完成项并返回错误。
func (vm *volumeManager) WaitForAttachAndMount(ctx context.Context, pod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	if pod == nil {
		return nil
	}

	expectedVolumes := getExpectedVolumes(pod)
	if len(expectedVolumes) == 0 {
		// Pod 没有卷，无需等待。
		return nil
	}

	logger.V(3).Info("Waiting for volumes to attach and mount for pod", "pod", klog.KObj(pod))
	uniquePodName := util.GetUniquePodName(pod)

	// 某些 Pod 需要反复调用 Setup 才能更新内容：为这类插件重新处理 Pod
	// （例如 Downward API 等原子更新卷依赖此机制刷新内容）。
	vm.desiredStateOfWorldPopulator.ReprocessPod(uniquePodName)

	// 轮询直到所有期望卷都完成挂载；超时后再分类汇总失败原因。
	err := wait.PollUntilContextTimeout(
		ctx,
		podAttachAndMountRetryInterval,
		podAttachAndMountTimeout,
		true,
		vm.verifyVolumesMountedFunc(uniquePodName, expectedVolumes))

	if err != nil {
		unmountedVolumes :=
			vm.getUnmountedVolumes(uniquePodName, expectedVolumes)
		// 同时收集未附加的卷和未进入期望状态的卷，便于在错误信息中区分原因。
		unattachedVolumeMounts :=
			vm.getUnattachedVolumes(uniquePodName)
		volumesNotInDSW :=
			vm.getVolumesNotInDSW(uniquePodName, expectedVolumes)

		if len(unmountedVolumes) == 0 {
			return nil
		}

		unattachedVolumes := []string{}
		for _, volumeToMount := range unattachedVolumeMounts {
			unattachedVolumes = append(unattachedVolumes, volumeToMount.OuterVolumeSpecNames...)
		}
		// 排序保证错误信息稳定，便于测试与排查。
		slices.Sort(unattachedVolumes)

		if utilfeature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) {
			for _, volumeToMount := range unattachedVolumeMounts {
				attachablePlugin, findErr := vm.volumePluginMgr.FindAttachablePluginBySpec(volumeToMount.VolumeSpec)
				if findErr != nil || attachablePlugin == nil {
					// 该卷类型不支持 attachable 接口，跳过附加资源检查。
					continue
				}
				if attachablePlugin.VerifyExhaustedResource(volumeToMount.VolumeSpec) {
					// 返回该错误，让 kubelet 触发 Pod 终止逻辑。
					return &VolumeAttachLimitExceededError{
						UnmountedVolumes:  unmountedVolumes,
						UnattachedVolumes: unattachedVolumes,
						VolumesNotInDSW:   volumesNotInDSW,
						OriginalError:     err,
					}
				}
			}
		}

		return fmt.Errorf(
			"unmounted volumes=%v, unattached volumes=%v, failed to process volumes=%v: %w",
			unmountedVolumes,
			unattachedVolumes,
			volumesNotInDSW,
			err)
	}

	logger.V(3).Info("All volumes are attached and mounted for pod", "pod", klog.KObj(pod))
	return nil
}

// WaitForUnmount 轮询等待 Pod 的卷全部卸载，超时后列出仍挂载的卷并返回错误。
func (vm *volumeManager) WaitForUnmount(ctx context.Context, pod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	if pod == nil {
		return nil
	}

	logger.V(3).Info("Waiting for volumes to unmount for pod", "pod", klog.KObj(pod))
	uniquePodName := util.GetUniquePodName(pod)

	vm.desiredStateOfWorldPopulator.ReprocessPod(uniquePodName)

	err := wait.PollUntilContextTimeout(
		ctx,
		podAttachAndMountRetryInterval,
		podAttachAndMountTimeout,
		true,
		vm.verifyVolumesUnmountedFunc(uniquePodName))

	if err != nil {
		var mountedVolumes []v1.UniqueVolumeName
		for _, v := range vm.actualStateOfWorld.GetMountedVolumesForPod(uniquePodName) {
			mountedVolumes = append(mountedVolumes, v.VolumeName)
		}
		if len(mountedVolumes) == 0 {
			return nil
		}

		slices.Sort(mountedVolumes)
		return fmt.Errorf(
			"mounted volumes=%v: %w",
			mountedVolumes,
			err)
	}

	logger.V(3).Info("All volumes are unmounted for pod", "pod", klog.KObj(pod))
	return nil
}

// WaitForAllPodsUnmount 并发等待所有 Pod 的卷卸载完成。
func (vm *volumeManager) WaitForAllPodsUnmount(ctx context.Context, pods []*v1.Pod) error {
	if len(pods) == 0 {
		return nil
	}

	// 为每个 Pod 生成一个等待函数，由 AggregateGoroutines 并发执行。
	funcs := make([]func() error, 0, len(pods))
	for _, pod := range pods {
		funcs = append(funcs, func() error {
			return vm.WaitForUnmount(ctx, pod)
		})
	}

	return utilerrors.AggregateGoroutines(funcs...)
}

// getVolumesNotInDSW 返回 Pod 期望但尚未进入期望状态缓存的卷（外层卷名）。
func (vm *volumeManager) getVolumesNotInDSW(uniquePodName types.UniquePodName, expectedVolumes []string) []string {
	volumesNotInDSW := sets.New(expectedVolumes...)

	for _, volumeToMount := range vm.desiredStateOfWorld.GetVolumesToMount() {
		if volumeToMount.PodName == uniquePodName {
			volumesNotInDSW.Delete(volumeToMount.OuterVolumeSpecNames...)
		}
	}

	return sets.List(volumesNotInDSW)
}

// getUnattachedVolumes 返回 Pod 期望附加、但实际尚未附加到本节点的卷。
func (vm *volumeManager) getUnattachedVolumes(uniquePodName types.UniquePodName) []cache.VolumeToMount {
	unattachedVolumes := []cache.VolumeToMount{}
	for _, volumeToMount := range vm.desiredStateOfWorld.GetVolumesToMount() {
		if volumeToMount.PodName == uniquePodName &&
			volumeToMount.PluginIsAttachable &&
			!vm.actualStateOfWorld.VolumeExists(volumeToMount.VolumeName) {
			unattachedVolumes = append(unattachedVolumes, volumeToMount)
		}
	}
	return unattachedVolumes
}

// verifyVolumesMountedFunc 返回一个条件函数：所有期望卷都挂载时返回 true。
func (vm *volumeManager) verifyVolumesMountedFunc(podName types.UniquePodName, expectedVolumes []string) wait.ConditionWithContextFunc {
	return func(_ context.Context) (done bool, err error) {
		if errs := vm.desiredStateOfWorld.PopPodErrors(podName); len(errs) > 0 {
			return true, errors.New(strings.Join(errs, "; "))
		}
		return len(vm.getUnmountedVolumes(podName, expectedVolumes)) == 0, nil
	}
}

// verifyVolumesUnmountedFunc 返回一个条件函数：该 Pod 没有已挂载卷时返回 true。
func (vm *volumeManager) verifyVolumesUnmountedFunc(podName types.UniquePodName) wait.ConditionWithContextFunc {
	return func(_ context.Context) (done bool, err error) {
		if errs := vm.desiredStateOfWorld.PopPodErrors(podName); len(errs) > 0 {
			return true, errors.New(strings.Join(errs, "; "))
		}
		return !vm.actualStateOfWorld.PodHasMountedVolumes(podName), nil
	}
}

// getMountedVolumes 返回既在期望状态、又已实际挂载的卷，按外层卷名索引。
func (vm *volumeManager) getMountedVolumes(podName types.UniquePodName) map[string]*cache.MountedVolume {
	volumes := vm.actualStateOfWorld.GetMountedVolumesForPod(podName)
	// 先按唯一卷名建索引，再通过期望状态的外层卷名映射回 Pod 视角。
	volumesByName := make(map[v1.UniqueVolumeName]*cache.MountedVolume, len(volumes))
	for i, mountedVolume := range volumes {
		volumesByName[mountedVolume.VolumeName] = &volumes[i]
	}

	volumeNames := vm.desiredStateOfWorld.GetVolumeNamesForPod(podName)
	volumesByOuterName := make(map[string]*cache.MountedVolume, len(volumeNames))
	for outerName, volumeName := range volumeNames {
		mountedVolume, ok := volumesByName[volumeName]
		if ok {
			volumesByOuterName[outerName] = mountedVolume
		}
	}

	return volumesByOuterName
}

// getUnmountedVolumes 返回未挂载的卷，包括不在期望/实际状态中的期望卷，
// 以及可能处于不确定挂载状态的卷。
func (vm *volumeManager) getUnmountedVolumes(podName types.UniquePodName, expectedVolumes []string) []string {
	unmountedVolumes := []string{}
	volumeNames := vm.desiredStateOfWorld.GetVolumeNamesForPod(podName)
	for _, outerName := range expectedVolumes {
		volumeName, ok := volumeNames[outerName]
		if !ok {
			unmountedVolumes = append(unmountedVolumes, outerName)
			continue
		}
		_, ok = vm.actualStateOfWorld.GetMountedVolumeForPod(podName, volumeName)
		if !ok {
			unmountedVolumes = append(unmountedVolumes, outerName)
		}
	}
	slices.Sort(unmountedVolumes)

	return unmountedVolumes
}

// getExpectedVolumes 返回只有全部挂载后才算完成 Pod 卷准备工作的卷列表。
func getExpectedVolumes(pod *v1.Pod) []string {
	mounts, devices, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	return mounts.Union(devices).UnsortedList()
}

// getExtraSupplementalGID 返回卷注解中额外补充组的 GID，并报告该 GID
// 是否未被 Pod 的补充组列表包含。
func getExtraSupplementalGID(volumeGIDValue string, pod *v1.Pod) (int64, bool) {
	if volumeGIDValue == "" {
		return 0, false
	}

	gid, err := strconv.ParseInt(volumeGIDValue, 10, 64)
	if err != nil {
		return 0, false
	}

	if pod.Spec.SecurityContext != nil {
		for _, existingGID := range pod.Spec.SecurityContext.SupplementalGroups {
			if gid == int64(existingGID) {
				return 0, false
			}
		}
	}

	return gid, true
}

// ResizeEphemeralVolume 同步触发指定卷的扩容；卷不支持直接扩容时返回错误。
func (vm *volumeManager) ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *resource.Quantity) error {
	resizablePlugin, volumeSpec, err := vm.volumePluginSupportsDirectResize(pod, volumeName)
	if err != nil {
		return err
	}
	return resizablePlugin.ResizeEphemeralVolume(volumeSpec, pod, newSize)
}

// volumePluginSupportsDirectResize 找到 Pod 中指定卷并判断其插件是否支持直接扩容。
func (vm *volumeManager) volumePluginSupportsDirectResize(pod *v1.Pod, volumeName string) (volume.ResizableEphemeralVolumePlugin, *volume.Spec, error) {
	vol := findVolumeInPodSpec(pod, volumeName)
	if vol == nil {
		return nil, nil, fmt.Errorf("volume %s not found in pod %s", volumeName, pod.Name)
	}

	// 该机制只支持 emptyDir 卷；其他卷类型直接返回不支持。
	if vol.EmptyDir != nil {
		plugin, err := vm.volumePluginMgr.FindPluginByName(emptydir.EmptyDirPluginName)
		if err != nil {
			return nil, nil, fmt.Errorf("plugin %s not found: %w", emptydir.EmptyDirPluginName, err)
		}

		if resizablePlugin, ok := plugin.(volume.ResizableEphemeralVolumePlugin); ok {
			volumeSpec := volume.NewSpecFromVolume(vol)
			return resizablePlugin, volumeSpec, nil
		}
	}

	return nil, nil, fmt.Errorf("volume %s does not support direct resizing", volumeName)
}

// findVolumeInPodSpec 按外层卷名在 Pod spec 中查找卷定义。
func findVolumeInPodSpec(pod *v1.Pod, volumeName string) *v1.Volume {
	for _, v := range pod.Spec.Volumes {
		if v.Name == volumeName {
			return &v
		}
	}
	return nil
}
