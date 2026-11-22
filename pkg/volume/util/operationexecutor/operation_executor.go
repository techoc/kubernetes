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

// Package operationexecutor 提供通过 nestedpendingoperations 执行附加、分离、挂载和卸载操作的接口，
// 确保同一卷不会为同一 pod 触发多个操作。
package operationexecutor

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/nestedpendingoperations"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// OperationExecutor 定义卷的附加、分离、挂载和卸载操作，由 NewNestedPendingOperations 执行，
// 确保同一卷上不会同时触发多个操作。
//
// 这些操作应当是幂等的（例如卷已附加到节点时 AttachVolume 仍应成功），
// 但这依赖卷插件实现该行为。
//
// 操作成功后会更新 actualStateOfWorld，反映卷的附加/分离/挂载/卸载状态。
//
// 若因同 UniqueVolumeName 的操作正在等待而无法启动操作，返回非 nil 错误。
//
// 操作启动后异步执行：错误只记录日志并终止 goroutine，不更新 actualStateOfWorld
// （调用方负责按需重试）。
//
// 部分操作会调用 API server，调用方需自行对错误限流。
type OperationExecutor interface {
	// AttachVolume 将卷附加到 volumeToAttach 指定的节点，并更新实际状态以反映结果。
	AttachVolume(logger klog.Logger, volumeToAttach VolumeToAttach, actualStateOfWorld ActualStateOfWorldAttacherUpdater) error

	// VerifyVolumesAreAttachedPerNode 校验给定卷列表是否仍附加在节点上。
	// 若某个卷当前未附加，则更新实际状态。
	// 该操作可能与其他附加/分离操作并发执行。
	// 理论上（实践中极少发生）并发竞争可能把仍附加的卷标为已分离，
	// 但 reconciler 会在短时间内纠正。
	VerifyVolumesAreAttachedPerNode(AttachedVolumes []AttachedVolume, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) error

	// VerifyVolumesAreAttached 校验整个集群中正在使用的卷是否仍附加在节点上。
	// 若某个卷当前未附加，则更新实际状态。
	VerifyVolumesAreAttached(volumesToVerify map[types.NodeName][]AttachedVolume, actualStateOfWorld ActualStateOfWorldAttacherUpdater)

	// DetachVolume 从 volumeToDetach 指定的节点分离卷，并更新实际状态。
	// verifySafeToDetach 为 true 时会获取节点对象，校验卷不在 Node 的
	// Status.VolumesInUse 列表中（否则操作失败并返回错误）。
	DetachVolume(logger klog.Logger, volumeToDetach AttachedVolume, verifySafeToDetach bool, actualStateOfWorld ActualStateOfWorldAttacherUpdater) error

	// 卷的 volumeMode 为 'Filesystem' 时，MountVolume 将卷挂载到 volumeToMount 指定的 pod。
	// 具体流程：
	// * 等待设备附加完成（仅可附加卷）。
	// * 将设备挂载到全局挂载路径（仅可附加卷）。
	// * 更新实际状态，反映卷已全局挂载（仅可附加卷）。
	// * 将卷挂载到 pod 专属路径。
	// * 更新实际状态，反映卷已挂载到 pod 路径。
	// 参数 "isRemount" 仅用于提示，以调整日志详细程度：
	// 首次挂载比重挂载更值得记录。
	//
	// volumeMode 为 'Block' 时，该方法为卷在 volumeToMount 指定的 pod 与全局映射路径
	// 两处创建符号链接。具体流程：
	// * 等待设备附加完成（仅可附加卷）。
	// * 更新实际状态，反映卷已全局挂载/映射。
	// * 用符号链接将卷映射到全局映射路径。
	// * 用符号链接将卷映射到 pod 设备映射路径。
	// * 更新实际状态，反映卷已挂载/映射到 pod 路径。
	MountVolume(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, isRemount bool) error

	// 卷的 volumeMode 为 'Filesystem' 时，UnmountVolume 从 volumeToUnmount 指定的 pod
	// 卸载卷，并更新实际状态。
	//
	// volumeMode 为 'Block' 时，该方法解除 volumeToUnmount 中 pod 设备映射路径
	// 与全局映射路径上的卷符号链接，然后更新实际状态。
	UnmountVolume(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, podsDir string) error

	// 卷的 volumeMode 为 'Filesystem' 时，UnmountDevice 从设备卸载卷的全局挂载路径
	// （仅可附加卷），使其可被分离，然后更新实际状态。
	//
	// volumeMode 为 'Block' 时，该方法检查全局映射路径下的符号链接数量；
	// 引用数为零时删除全局映射目录，使卷可被分离，然后更新实际状态。
	UnmountDevice(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, hostutil hostutil.HostUtils) error

	// VerifyControllerAttachedVolume 检查指定卷是否出现在指定节点的
	// AttachedVolumes 状态字段中，通过 kubeClient 获取节点对象。
	// 找到卷时更新实际状态，标记卷为已附加。
	// 卷未实现 attacher 接口时，假定其已附加并相应更新实际状态。
	// 未找到卷或发生错误（例如获取节点对象失败）时返回错误，触发指数退避重试。
	VerifyControllerAttachedVolume(logger klog.Logger, volumeToMount VolumeToMount, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) error

	// IsOperationPending 判断给定 volumeName 与 podName 或 nodeName 对应的操作是否正在等待。
	IsOperationPending(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName, nodeName types.NodeName) bool
	// IsOperationSafeToRetry 判断给定 volumeName 与 podName 或 nodeName 对应的操作
	// 是否既不等待也不处于指数退避中。
	IsOperationSafeToRetry(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName, nodeName types.NodeName, operationName string) bool
	// ExpandInUseVolume 在不卸载卷的情况下将文件系统扩容到期望大小。
	ExpandInUseVolume(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) error
	// ReconstructVolumeOperation 由插件构造新的 volumeSpec 并返回。
	ReconstructVolumeOperation(volumeMode v1.PersistentVolumeMode, plugin volume.VolumePlugin, mapperPlugin volume.BlockVolumePlugin, uid types.UID, podName volumetypes.UniquePodName, volumeSpecName string, volumePath string, pluginName string) (volume.ReconstructedVolume, error)
}

// NewOperationExecutor 创建 OperationExecutor 实例。
func NewOperationExecutor(
	operationGenerator OperationGenerator) OperationExecutor {

	return &operationExecutor{
		pendingOperations: nestedpendingoperations.NewNestedPendingOperations(
			true /* exponentialBackOffOnError */),
		operationGenerator: operationGenerator,
	}
}

// MarkVolumeOpts 封装传给卷挂载状态更新方法的参数。
type MarkVolumeOpts struct {
	// PodName 是挂载目标 pod 的唯一标识。
	PodName volumetypes.UniquePodName
	// PodUID 是挂载目标 pod 的 UID。
	PodUID types.UID
	// VolumeName 是卷的唯一标识。
	VolumeName v1.UniqueVolumeName
	// Mounter 用于挂载文件系统卷。
	Mounter volume.Mounter
	// BlockVolumeMapper 用于映射块卷。
	BlockVolumeMapper volume.BlockVolumeMapper
	// VolumeGIDVolume 是卷的 GID 值。
	VolumeGIDVolume string
	// VolumeSpec 是卷的规格。
	VolumeSpec *volume.Spec
	// VolumeMountState 是卷当前的挂载状态。
	VolumeMountState VolumeMountState
	// SELinuxMountContext 是挂载时使用的 SELinux 上下文。
	SELinuxMountContext string
}

// ActualStateOfWorldMounterUpdater 定义在挂载/卸载成功后更新实际状态缓存的操作。
type ActualStateOfWorldMounterUpdater interface {
	// MarkVolumeAsMounted 标记指定卷已挂载到指定 pod。
	MarkVolumeAsMounted(markVolumeOpts MarkVolumeOpts) error

	// MarkVolumeAsUnmounted 标记指定卷已从指定 pod 卸载。
	MarkVolumeAsUnmounted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) error

	// MarkVolumeMountAsUncertain 将 pod 的卷挂载状态标记为不确定。
	MarkVolumeMountAsUncertain(markVolumeOpts MarkVolumeOpts) error

	// MarkDeviceAsMounted 标记指定卷的设备已全局挂载。
	MarkDeviceAsMounted(volumeName v1.UniqueVolumeName, devicePath, deviceMountPath, seLinuxMountContext string) error

	// MarkDeviceAsUncertain 将全局挂载路径中的设备状态标记为不确定。
	MarkDeviceAsUncertain(volumeName v1.UniqueVolumeName, devicePath, deviceMountPath, seLinuxMountContext string) error

	// MarkDeviceAsUnmounted 标记指定卷的全局挂载已卸载。
	MarkDeviceAsUnmounted(volumeName v1.UniqueVolumeName) error

	// MarkVolumeAsResized 标记指定卷的文件系统扩容请求已完成。
	MarkVolumeAsResized(volumeName v1.UniqueVolumeName, claimSize resource.Quantity) bool

	// GetDeviceMountState 返回设备在全局路径的挂载状态。
	GetDeviceMountState(volumeName v1.UniqueVolumeName) DeviceMountState

	// GetVolumeMountState 返回卷在指定 Pod 中的挂载状态。
	GetVolumeMountState(volumName v1.UniqueVolumeName, podName volumetypes.UniquePodName) VolumeMountState

	// IsVolumeMountedElsewhere 判断给定卷是否挂载在指定 pod 之外的其他 pod 上。
	IsVolumeMountedElsewhere(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool

	// MarkForInUseExpansionError 标记卷在扩容时出现 in-use 错误。
	// 此后不得再对该卷重试扩容。
	MarkForInUseExpansionError(volumeName v1.UniqueVolumeName)

	// CheckAndMarkVolumeAsUncertainViaReconstruction 仅在卷尚未进入实际状态时将其加入，
	// 以免覆盖已有状态。加入 ASOW 失败时返回错误；若本次确实加入了卷则返回 true，否则返回 false。
	CheckAndMarkVolumeAsUncertainViaReconstruction(opts MarkVolumeOpts) (bool, error)

	// CheckAndMarkDeviceUncertainViaReconstruction 仅在设备尚未进入实际状态时将其加入，
	// 以免覆盖已有状态。只需传入 deviceMountPath，因为 devicePath 已由
	// VerifyControllerAttachedVolume 确定。
	CheckAndMarkDeviceUncertainViaReconstruction(volumeName v1.UniqueVolumeName, deviceMountPath string) bool

	// IsVolumeReconstructed 判断当前实际状态中的卷是否由重建流程发现。
	IsVolumeReconstructed(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool

	// IsVolumeDeviceReconstructed 判断 volumeName 对应的卷设备是否由重建流程发现。
	IsVolumeDeviceReconstructed(volumeName v1.UniqueVolumeName) bool

	// MarkVolumeExpansionFailedWithFinalError 标记卷扩容因最终错误失败，
	// 该状态无需记录到 API server。
	MarkVolumeExpansionFailedWithFinalError(volumeName v1.UniqueVolumeName)

	// RemoveVolumeFromFailedWithFinalErrors 从最终错误失败列表中移除卷。
	RemoveVolumeFromFailedWithFinalErrors(volumeName v1.UniqueVolumeName)

	// CheckVolumeInFailedExpansionWithFinalErrors 判断卷扩容是否因最终错误而失败。
	CheckVolumeInFailedExpansionWithFinalErrors(volumeName v1.UniqueVolumeName) bool

	// MarkVolumeAsMountAttempted 记录 kubelet 已尝试为该 pod stage/publish 卷。
	// 供卷健康探测使用，覆盖那些从未进入 mounted/uncertain ASW 状态的卷。
	MarkVolumeAsMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName)
	// IsVolumeMountAttempted 判断该 pod 与卷的组合是否调用过 MarkVolumeAsMountAttempted。
	IsVolumeMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) bool
}

// ActualStateOfWorldAttacherUpdater 定义在附加/分离/挂载/卸载成功后更新实际状态缓存的操作。
type ActualStateOfWorldAttacherUpdater interface {
	// MarkVolumeAsAttached 标记指定卷已附加到指定节点。提供了卷名时使用传入的卷名，
	// 否则根据插件查询结果计算卷名。
	//
	// TODO: 将来应能移除此方法的 volumeName 参数，因为它仅用于可附加卷。
	// 参见 issue 29695。
	MarkVolumeAsAttached(logger klog.Logger, volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName, devicePath string) error

	// MarkVolumeAsUncertain 标记指定卷*可能*已附加到指定节点。
	// 附加操作失败时，附加/分离控制器无法确定卷是否已附加。
	// 提供了卷名时使用传入的卷名，否则根据插件查询结果计算卷名。
	MarkVolumeAsUncertain(logger klog.Logger, volumeName v1.UniqueVolumeName, volumeSpec *volume.Spec, nodeName types.NodeName) error

	// MarkVolumeAsDetached 标记指定卷已从指定节点分离。
	MarkVolumeAsDetached(volumeName v1.UniqueVolumeName, nodeName types.NodeName)

	// RemoveVolumeFromReportAsAttached 表达分离指定卷的意图（从节点的
	// volumesToReportAsAttached 列表中移除该卷）。
	RemoveVolumeFromReportAsAttached(volumeName v1.UniqueVolumeName, nodeName types.NodeName) error

	// AddVolumeToReportAsAttached 撤销分离指定卷的意图（将卷加回节点的
	// volumesToReportAsAttached 列表）。
	AddVolumeToReportAsAttached(logger klog.Logger, volumeName v1.UniqueVolumeName, nodeName types.NodeName)

	// InitializeClaimSize 读取 pvc.Status.Capacity 设置 PVC 声明大小。
	InitializeClaimSize(logger klog.Logger, volumeName v1.UniqueVolumeName, claimSize resource.Quantity)

	// GetClaimSize 返回卷对应的 PVC 声明大小。
	GetClaimSize(volumeName v1.UniqueVolumeName) resource.Quantity
}

// VolumeLogger 定义生成卷相关日志与错误消息的操作。
type VolumeLogger interface {
	// GenerateMsgDetailed 生成可用于日志的详细消息。
	// 格式为 "<prefixMsg> <卷详情> <suffixMsg>"，卷详情由各实现提供。
	GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string)

	// GenerateErrorDetailed 生成可用于日志的详细错误。
	// 消息格式为 "<prefixMsg> <卷详情>: <err> "。
	GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error)

	// GenerateMsg 生成面向用户的简单消息和可用于日志的详细消息。
	// 格式为 "<prefixMsg> <卷详情> <suffixMsg>"，卷详情由各实现提供。
	GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string)

	// GenerateError 生成面向用户的简单错误和可用于日志的详细错误。
	// 消息格式为 "<prefixMsg> <卷详情>: <err> "。
	GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error)
}

// errSuffix 在 err 非空时生成格式为 ": <err>" 的错误字符串。
func errSuffix(err error) string {
	errStr := ""
	if err != nil {
		errStr = fmt.Sprintf(": %v", err)
	}
	return errStr
}

// generateVolumeMsgDetailed 生成供日志使用的详细消息。
func generateVolumeMsgDetailed(prefixMsg, suffixMsg, volumeName, details string) (detailedMsg string) {
	return fmt.Sprintf("%v for volume %q %v %v", prefixMsg, volumeName, details, suffixMsg)
}

// generateVolumeMsg 生成供事件使用的简化消息和供日志使用的详细消息。
func generateVolumeMsg(prefixMsg, suffixMsg, volumeName, details string) (simpleMsg, detailedMsg string) {
	simpleMsg = fmt.Sprintf("%v for volume %q %v", prefixMsg, volumeName, suffixMsg)
	return simpleMsg, generateVolumeMsgDetailed(prefixMsg, suffixMsg, volumeName, details)
}

// VolumeToAttach 表示应附加到节点的卷。
type VolumeToAttach struct {
	// MultiAttachErrorReported 表示该卷的多重附加错误是否已上报。
	// 用于避免同一卷重复上报该错误。
	MultiAttachErrorReported bool

	// VolumeName 是待附加卷的唯一标识。
	VolumeName v1.UniqueVolumeName

	// VolumeSpec 是待附加卷的规格。
	VolumeSpec *volume.Spec

	// NodeName 是卷应附加到的节点标识。
	NodeName types.NodeName

	// ScheduledPods 是引用该卷且已调度到该节点的一组 pod。
	ScheduledPods []*v1.Pod
}

// GenerateMsgDetailed 返回待附加卷的详细消息。
func (volume *VolumeToAttach) GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) from node %q", volume.VolumeName, volume.NodeName)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsgDetailed(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateMsg 返回待附加卷的简单消息和详细消息。
func (volume *VolumeToAttach) GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) from node %q", volume.VolumeName, volume.NodeName)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsg(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateErrorDetailed 返回待附加卷的详细错误。
func (volume *VolumeToAttach) GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error) {
	return errors.New(volume.GenerateMsgDetailed(prefixMsg, errSuffix(err)))
}

// GenerateError 返回待附加卷的简单错误和详细错误。
func (volume *VolumeToAttach) GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error) {
	simpleMsg, detailedMsg := volume.GenerateMsg(prefixMsg, errSuffix(err))
	return errors.New(simpleMsg), errors.New(detailedMsg)
}

// String 以文本格式组合卷的关键字段用于日志。
func (volume *VolumeToAttach) String() string {
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return fmt.Sprintf("%s (UniqueName: %s) from node %s", volumeSpecName, volume.VolumeName, volume.NodeName)
}

// MarshalLog 以结构化格式组合卷的关键字段用于日志。
func (volume *VolumeToAttach) MarshalLog() interface{} {
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return struct {
		VolumeName, UniqueName, NodeName string
	}{
		VolumeName: volumeSpecName,
		UniqueName: string(volume.VolumeName),
		NodeName:   string(volume.NodeName),
	}
}

// 编译期断言：VolumeToAttach 实现 fmt.Stringer 与 logr.Marshaler。
var _ fmt.Stringer = &VolumeToAttach{}
var _ logr.Marshaler = &VolumeToAttach{}

// VolumeToMount 表示应附加到本节点并挂载到指定 Pod 的卷。
type VolumeToMount struct {
	// VolumeName 是待挂载卷的唯一标识。
	VolumeName v1.UniqueVolumeName

	// PodName 是卷附加后要挂载到的 pod 的唯一标识。
	PodName volumetypes.UniquePodName

	// VolumeSpec 是待挂载卷的规格，用于创建 Mounter 并生成 InnerVolumeSpecName。
	VolumeSpec *volume.Spec

	// OuterVolumeSpecNames 是卷在 podSpec.Volumes[x].Name 中的名称。
	OuterVolumeSpecNames []string

	// Pod 是卷的挂载目标，用于创建 Mounter。
	Pod *v1.Pod

	// PluginIsAttachable 表示该卷的插件实现了 volume.Attacher 接口。
	PluginIsAttachable bool

	// PluginIsDeviceMountable 表示该卷的插件实现了 volume.DeviceMounter 接口。
	PluginIsDeviceMountable bool

	// VolumeGIDValue 是 GID 注解的值（若存在）。
	VolumeGIDValue string

	// DevicePath 是卷在节点上的附加路径；不可附加卷为空。
	DevicePath string

	// ReportedInUse 表示卷已成功加入节点状态的 VolumesInUse 字段。
	ReportedInUse bool

	// DesiredSizeLimit 是卷大小的期望上限（若插件实现）。
	DesiredSizeLimit *resource.Quantity

	// MountRequestTime 是请求挂载卷的时间。
	MountRequestTime time.Time

	// DesiredPersistentVolumeSize 存储卷的期望大小，通常取自 pv.Spec.Capacity。
	DesiredPersistentVolumeSize resource.Quantity

	// SELinuxLabel 是挂载时要使用的 SELinux 标签。
	// 满足以下任一条件时设置：
	// * 启用 SELinuxMountReadWriteOncePod 特性门控、卷为 RWOP 且 kubelet 知道 SELinux 标签。
	// * 或启用 SELinuxMount 特性门控且 kubelet 知道 SELinux 标签。
	SELinuxLabel string
}

// DeviceMountState 表示设备在全局路径中的挂载状态。
type DeviceMountState string

const (
	// DeviceGloballyMounted 表示设备已成功全局挂载。
	DeviceGloballyMounted DeviceMountState = "DeviceGloballyMounted"

	// DeviceMountUncertain 表示设备可能尚未挂载，但可能有挂载操作正在进行并使其最终成功。
	DeviceMountUncertain DeviceMountState = "DeviceMountUncertain"

	// DeviceNotMounted 表示设备尚未全局挂载。
	DeviceNotMounted DeviceMountState = "DeviceNotMounted"
)

// VolumeMountState 表示卷在 pod 本地路径中的挂载状态。
type VolumeMountState string

const (
	// VolumeMounted 表示卷已挂载到 pod 的本地路径。
	VolumeMounted VolumeMountState = "VolumeMounted"

	// VolumeMountUncertain 表示卷可能已挂载、也可能未挂载到 pod 的本地路径。
	VolumeMountUncertain VolumeMountState = "VolumeMountUncertain"

	// VolumeNotMounted 表示卷尚未挂载到 pod 的本地路径。
	VolumeNotMounted VolumeMountState = "VolumeNotMounted"
)

// MountPreConditionFailed 表示挂载前置条件不满足的错误。
type MountPreConditionFailed struct {
	msg string
}

// Error 返回错误消息。
func (err *MountPreConditionFailed) Error() string {
	return err.msg
}

// NewMountPreConditionFailedError 创建挂载前置条件失败错误。
func NewMountPreConditionFailedError(msg string) *MountPreConditionFailed {
	return &MountPreConditionFailed{msg: msg}
}

// IsMountFailedPreconditionError 判断 err 是否为挂载前置条件失败错误。
func IsMountFailedPreconditionError(err error) bool {
	var failedPreconditionError *MountPreConditionFailed
	return errors.As(err, &failedPreconditionError)
}

// GenerateMsgDetailed 返回待挂载卷的详细消息。
func (volume *VolumeToMount) GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) pod %q (UID: %q)", volume.VolumeName, volume.Pod.Name, volume.Pod.UID)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsgDetailed(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateMsg 返回待挂载卷的简单消息和详细消息。
func (volume *VolumeToMount) GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) pod %q (UID: %q)", volume.VolumeName, volume.Pod.Name, volume.Pod.UID)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsg(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateErrorDetailed 返回待挂载卷的详细错误。
func (volume *VolumeToMount) GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error) {
	return errors.New(volume.GenerateMsgDetailed(prefixMsg, errSuffix(err)))
}

// GenerateError 返回待挂载卷的简单错误和详细错误。
func (volume *VolumeToMount) GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error) {
	simpleMsg, detailedMsg := volume.GenerateMsg(prefixMsg, errSuffix(err))
	return errors.New(simpleMsg), errors.New(detailedMsg)
}

// AttachedVolume 表示已附加到节点的卷。
type AttachedVolume struct {
	// VolumeName 是已附加卷的唯一标识。
	VolumeName v1.UniqueVolumeName

	// VolumeSpec 是已附加卷的规格。
	VolumeSpec *volume.Spec

	// NodeName 是卷所附加到的节点标识。
	NodeName types.NodeName

	// PluginIsAttachable 表示该卷的插件实现了 volume.Attacher 接口。
	PluginIsAttachable bool

	// DevicePath 是卷在节点上的附加路径；不可附加卷为空。
	DevicePath string

	// DeviceMountPath 是设备附加后应挂载到节点上的路径。
	DeviceMountPath string

	// PluginName 是用于附加和挂载该卷的插件的未转义限定名称。
	PluginName string

	// SELinuxMountContext 是挂载时使用的 SELinux 上下文。
	SELinuxMountContext string
}

// GenerateMsgDetailed 返回已附加卷的详细消息。
func (volume *AttachedVolume) GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) on node %q", volume.VolumeName, volume.NodeName)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsgDetailed(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateMsg 返回已附加卷的简单消息和详细消息。
func (volume *AttachedVolume) GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) on node %q", volume.VolumeName, volume.NodeName)
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return generateVolumeMsg(prefixMsg, suffixMsg, volumeSpecName, detailedStr)
}

// GenerateErrorDetailed 返回已附加卷的详细错误。
func (volume *AttachedVolume) GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error) {
	return errors.New(volume.GenerateMsgDetailed(prefixMsg, errSuffix(err)))
}

// GenerateError 返回已附加卷的简单错误和详细错误。
func (volume *AttachedVolume) GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error) {
	simpleMsg, detailedMsg := volume.GenerateMsg(prefixMsg, errSuffix(err))
	return errors.New(simpleMsg), errors.New(detailedMsg)
}

// String 以文本格式组合卷的关键字段用于日志。
func (volume *AttachedVolume) String() string {
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return fmt.Sprintf("%s (UniqueName: %s) from node %s", volumeSpecName, volume.VolumeName, volume.NodeName)
}

// MarshalLog 以结构化格式组合卷的关键字段用于日志。
func (volume *AttachedVolume) MarshalLog() interface{} {
	volumeSpecName := "nil"
	if volume.VolumeSpec != nil {
		volumeSpecName = volume.VolumeSpec.Name()
	}
	return struct {
		VolumeName, UniqueName, NodeName string
	}{
		VolumeName: volumeSpecName,
		UniqueName: string(volume.VolumeName),
		NodeName:   string(volume.NodeName),
	}
}

// 编译期断言：AttachedVolume 实现 fmt.Stringer 与 logr.Marshaler。
var _ fmt.Stringer = &AttachedVolume{}
var _ logr.Marshaler = &AttachedVolume{}

// MountedVolume 表示已成功挂载到 pod 的卷。
type MountedVolume struct {
	// PodName 是挂载目标 pod 的唯一标识。
	PodName volumetypes.UniquePodName

	// VolumeName 是挂载到 pod 的卷的唯一标识。
	VolumeName v1.UniqueVolumeName

	// InnerVolumeSpecName 是卷的 volume.Spec.Name()。若卷通过 PVC 引用，
	// 这里存放绑定的 PV 对象名称。
	// 插件在 pod 挂载路径中使用该名称，例如：
	// /var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{innerVolumeSpecName}/
	// PVC 示例：
	//   apiVersion: v1
	//   kind: PersistentVolume
	//   metadata:
	//     name: pv0003				<- InnerVolumeSpecName
	//   spec:
	//     capacity:
	//       storage: 5Gi
	//     accessModes:
	//       - ReadWriteOnce
	//     persistentVolumeReclaimPolicy: Recycle
	//     nfs:
	//       path: /tmp
	//       server: 172.17.0.2
	// 非 PVC 示例：
	//   apiVersion: v1
	//   kind: Pod
	//   metadata:
	//     name: test-pd
	//   spec:
	//     containers:
	//     - image: registry.k8s.io/test-webserver
	//     	 name: test-container
	//     	 volumeMounts:
	//     	 - mountPath: /test-pd
	//     	   name: test-volume
	//     volumes:
	//     - name: test-volume			<- InnerVolumeSpecName
	//     	 gcePersistentDisk:
	//     	   pdName: my-data-disk
	//     	   fsType: ext4
	InnerVolumeSpecName string

	// PluginName 是用于挂载和卸载该卷的插件的未转义限定名称。
	// 可按需据此获取插件进行卸载。插件在 pod 挂载路径中使用其转义形式，例如：
	// /var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{outerVolumeSpecName}/
	PluginName string

	// PodUID 是挂载目标 pod 的 UID，也是插件在 pod 挂载路径中使用的字符串，例如：
	// /var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{outerVolumeSpecName}/
	PodUID types.UID

	// Mounter 是挂载该卷的卷 mounter，kubelet 需要它来构建 container.VolumeMap。
	// 仅文件系统卷需要 Mounter，块卷不需要。
	Mounter volume.Mounter

	// BlockVolumeMapper 是映射该卷的卷 mapper，kubelet 需要它来构建 container.VolumeMap。
	// 仅块卷需要 BlockVolumeMapper，文件系统卷不需要。
	BlockVolumeMapper volume.BlockVolumeMapper

	// VolumeGIDValue 是 GID 注解的值（若存在）。
	VolumeGIDValue string

	// VolumeSpec 是待挂载卷的规格。
	VolumeSpec *volume.Spec

	// DeviceMountPath 是设备附加后应挂载到节点上的路径。
	DeviceMountPath string

	// SELinuxMountContext 是挂载选项 'mount -o context=XYZ' 的值；为空表示未使用该挂载选项。
	SELinuxMountContext string
}

// GenerateMsgDetailed 返回已挂载卷的详细消息。
func (volume *MountedVolume) GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) pod %q (UID: %q)", volume.VolumeName, volume.PodName, volume.PodUID)
	return generateVolumeMsgDetailed(prefixMsg, suffixMsg, string(volume.VolumeName), detailedStr)
}

// GenerateMsg 返回已挂载卷的简单消息和详细消息。
func (volume *MountedVolume) GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string) {
	detailedStr := fmt.Sprintf("(UniqueName: %q) pod %q (UID: %q)", volume.VolumeName, volume.PodName, volume.PodUID)
	return generateVolumeMsg(prefixMsg, suffixMsg, string(volume.VolumeName), detailedStr)
}

// GenerateErrorDetailed 返回已挂载卷的详细错误。
func (volume *MountedVolume) GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error) {
	return errors.New(volume.GenerateMsgDetailed(prefixMsg, errSuffix(err)))
}

// GenerateError 返回已挂载卷的简单错误和详细错误。
func (volume *MountedVolume) GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error) {
	simpleMsg, detailedMsg := volume.GenerateMsg(prefixMsg, errSuffix(err))
	return errors.New(simpleMsg), errors.New(detailedMsg)
}

// operationExecutor 是 OperationExecutor 的实现。
type operationExecutor struct {
	// pendingOperations 跟踪正在等待的附加/分离操作，避免同卷启动多个操作。
	pendingOperations nestedpendingoperations.NestedPendingOperations

	// operationGenerator 提供生成卷操作函数的实现。
	operationGenerator OperationGenerator
}

// IsOperationPending 判断卷与 pod/节点对应的操作是否正在等待。
func (oe *operationExecutor) IsOperationPending(
	volumeName v1.UniqueVolumeName,
	podName volumetypes.UniquePodName,
	nodeName types.NodeName) bool {
	return oe.pendingOperations.IsOperationPending(volumeName, podName, nodeName)
}

// IsOperationSafeToRetry 判断卷与 pod/节点对应的操作是否可安全重试。
func (oe *operationExecutor) IsOperationSafeToRetry(
	volumeName v1.UniqueVolumeName,
	podName volumetypes.UniquePodName,
	nodeName types.NodeName,
	operationName string) bool {
	return oe.pendingOperations.IsOperationSafeToRetry(volumeName, podName, nodeName, operationName)
}

// AttachVolume 将卷附加到指定节点，并更新实际状态。
func (oe *operationExecutor) AttachVolume(
	logger klog.Logger,
	volumeToAttach VolumeToAttach,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) error {
	generatedOperations :=
		oe.operationGenerator.GenerateAttachVolumeFunc(logger, volumeToAttach, actualStateOfWorld)

	// 可多重附加的卷按节点区分并发键：不同节点可并行附加，同节点仍需串行。
	if util.IsMultiAttachAllowed(volumeToAttach.VolumeSpec) {
		return oe.pendingOperations.Run(
			volumeToAttach.VolumeName, "" /* podName */, volumeToAttach.NodeName, generatedOperations)
	}

	// 不可多重附加的卷按卷名全局串行，防止多个节点同时附加。
	return oe.pendingOperations.Run(
		volumeToAttach.VolumeName, "" /* podName */, "" /* nodeName */, generatedOperations)
}

// DetachVolume 从指定节点分离卷，并更新实际状态。
func (oe *operationExecutor) DetachVolume(
	logger klog.Logger,
	volumeToDetach AttachedVolume,
	verifySafeToDetach bool,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) error {
	generatedOperations, err :=
		oe.operationGenerator.GenerateDetachVolumeFunc(logger, volumeToDetach, verifySafeToDetach, actualStateOfWorld)
	if err != nil {
		return err
	}

	// 与附加对称：可多重附加的卷按节点区分，其他卷全局串行。
	if util.IsMultiAttachAllowed(volumeToDetach.VolumeSpec) {
		return oe.pendingOperations.Run(
			volumeToDetach.VolumeName, "" /* podName */, volumeToDetach.NodeName, generatedOperations)
	}
	return oe.pendingOperations.Run(
		volumeToDetach.VolumeName, "" /* podName */, "" /* nodeName */, generatedOperations)

}

// VerifyVolumesAreAttached 逐节点校验卷的附加状态；单节点失败只记日志，不影响其他节点。
func (oe *operationExecutor) VerifyVolumesAreAttached(
	attachedVolumes map[types.NodeName][]AttachedVolume,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) {

	for node, nodeAttachedVolumes := range attachedVolumes {
		nodeError := oe.VerifyVolumesAreAttachedPerNode(nodeAttachedVolumes, node, actualStateOfWorld)
		if nodeError != nil {
			klog.Errorf("VerifyVolumesAreAttached failed for volumes %v, node %q with error %v", nodeAttachedVolumes, node, nodeError)
		}
	}
}

// VerifyVolumesAreAttachedPerNode 校验指定节点上卷的附加状态。
func (oe *operationExecutor) VerifyVolumesAreAttachedPerNode(
	attachedVolumes []AttachedVolume,
	nodeName types.NodeName,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) error {
	generatedOperations, err :=
		oe.operationGenerator.GenerateVolumesAreAttachedFunc(attachedVolumes, nodeName, actualStateOfWorld)
	if err != nil {
		return err
	}

	// 使用空 UniqueVolumeName，使不同节点的校验操作可以并发执行。
	return oe.pendingOperations.Run("" /* volumeName */, "" /* podName */, "" /* nodeName */, generatedOperations)
}

// MountVolume 按卷模式生成挂载或映射操作并执行。
func (oe *operationExecutor) MountVolume(
	waitForAttachTimeout time.Duration,
	volumeToMount VolumeToMount,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	isRemount bool) error {
	fsVolume, err := util.CheckVolumeModeFilesystem(volumeToMount.VolumeSpec)
	if err != nil {
		return err
	}
	var generatedOperations volumetypes.GeneratedOperations
	if fsVolume {
		// 文件系统卷：附加完成后挂载或重挂载。
		generatedOperations = oe.operationGenerator.GenerateMountVolumeFunc(
			waitForAttachTimeout, volumeToMount, actualStateOfWorld, isRemount)

	} else {
		// 块卷：附加完成后创建到设备的映射。
		generatedOperations, err = oe.operationGenerator.GenerateMapVolumeFunc(
			waitForAttachTimeout, volumeToMount, actualStateOfWorld)
	}
	if err != nil {
		return err
	}
	// 默认按卷名串行，避免引用同卷的多个 pod 并行挂载/映射。
	podName := nestedpendingoperations.EmptyUniquePodName

	// TODO: remove this -- not necessary
	if !volumeToMount.PluginIsAttachable && !volumeToMount.PluginIsDeviceMountable {
		// 不可附加且不可设备挂载的插件可安全地为多个 pod 并行挂载同一卷。
		podName = util.GetUniquePodName(volumeToMount.Pod)
	}

	// TODO mount_device
	return oe.pendingOperations.Run(
		volumeToMount.VolumeName, podName, "" /* nodeName */, generatedOperations)
}

// UnmountVolume 按卷模式生成卸载或解除映射操作并执行。
func (oe *operationExecutor) UnmountVolume(
	volumeToUnmount MountedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	podsDir string) error {
	fsVolume, err := util.CheckVolumeModeFilesystem(volumeToUnmount.VolumeSpec)
	if err != nil {
		return err
	}
	var generatedOperations volumetypes.GeneratedOperations
	if fsVolume {
		// 文件系统卷：已挂载时执行卸载。
		generatedOperations, err = oe.operationGenerator.GenerateUnmountVolumeFunc(
			volumeToUnmount, actualStateOfWorld, podsDir)
	} else {
		// 块卷：已映射时解除映射。
		generatedOperations, err = oe.operationGenerator.GenerateUnmapVolumeFunc(
			volumeToUnmount, actualStateOfWorld)
	}
	if err != nil {
		return err
	}
	// 所有插件都允许引用同卷的多个 pod 并行卸载/解除映射，因此用 pod UID 作为键。
	podName := volumetypes.UniquePodName(volumeToUnmount.PodUID)

	return oe.pendingOperations.Run(
		volumeToUnmount.VolumeName, podName, "" /* nodeName */, generatedOperations)
}

// UnmountDevice 按卷模式生成设备卸载或解除映射操作并执行。
func (oe *operationExecutor) UnmountDevice(
	deviceToDetach AttachedVolume,
	actualStateOfWorld ActualStateOfWorldMounterUpdater,
	hostutil hostutil.HostUtils) error {
	fsVolume, err := util.CheckVolumeModeFilesystem(deviceToDetach.VolumeSpec)
	if err != nil {
		return err
	}
	var generatedOperations volumetypes.GeneratedOperations
	if fsVolume {
		// 文件系统卷：卷不再被引用时卸载设备。
		generatedOperations, err = oe.operationGenerator.GenerateUnmountDeviceFunc(
			deviceToDetach, actualStateOfWorld, hostutil)
	} else {
		// 块卷：卷不再被引用时分离设备并移除回环设备。
		generatedOperations, err = oe.operationGenerator.GenerateUnmapDeviceFunc(
			deviceToDetach, actualStateOfWorld, hostutil)
	}
	if err != nil {
		return err
	}
	// 多个 pod 共享同一设备，按卷名串行避免并发卸载同一设备。
	podName := nestedpendingoperations.EmptyUniquePodName

	return oe.pendingOperations.Run(
		deviceToDetach.VolumeName, podName, "" /* nodeName */, generatedOperations)
}

// ExpandInUseVolume 生成并执行在线扩容操作。
func (oe *operationExecutor) ExpandInUseVolume(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) error {
	generatedOperations, err := oe.operationGenerator.GenerateExpandInUseVolumeFunc(volumeToMount, actualStateOfWorld, currentSize)
	if err != nil {
		return err
	}
	return oe.pendingOperations.Run(volumeToMount.VolumeName, "", "" /* nodeName */, generatedOperations)
}

// VerifyControllerAttachedVolume 生成并运行控制器附加状态校验操作。
func (oe *operationExecutor) VerifyControllerAttachedVolume(
	logger klog.Logger,
	volumeToMount VolumeToMount,
	nodeName types.NodeName,
	actualStateOfWorld ActualStateOfWorldAttacherUpdater) error {
	generatedOperations, err :=
		oe.operationGenerator.GenerateVerifyControllerAttachedVolumeFunc(logger, volumeToMount, nodeName, actualStateOfWorld)
	if err != nil {
		return err
	}

	return oe.pendingOperations.Run(
		volumeToMount.VolumeName, "" /* podName */, "" /* nodeName */, generatedOperations)
}

// ReconstructVolumeOperation 由插件根据挂载路径重建 volumeSpec。
func (oe *operationExecutor) ReconstructVolumeOperation(
	volumeMode v1.PersistentVolumeMode,
	plugin volume.VolumePlugin,
	mapperPlugin volume.BlockVolumePlugin,
	uid types.UID,
	podName volumetypes.UniquePodName,
	volumeSpecName string,
	volumePath string,
	pluginName string) (volume.ReconstructedVolume, error) {

	// 文件系统卷：从挂载路径重建 volumeSpec。
	if volumeMode == v1.PersistentVolumeFilesystem {
		klog.V(5).Infof("Starting operationExecutor.ReconstructVolume for file volume on pod %q", podName)
		reconstructed, err := plugin.ConstructVolumeSpec(volumeSpecName, volumePath)
		if err != nil {
			return volume.ReconstructedVolume{}, err
		}
		return reconstructed, nil
	}

	// 块卷：从挂载路径重建 volumeSpec。
	klog.V(5).Infof("Starting operationExecutor.ReconstructVolume for block volume on pod %q", podName)

	// volumePath 的路径中包含卷名；对块卷而言 {volumeName} 是指向原始块设备的符号链接。
	// 例如 volumePath: pods/{podUid}}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/{volumeName}
	volumeSpec, err := mapperPlugin.ConstructBlockVolumeSpec(uid, volumeSpecName, volumePath)
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	return volume.ReconstructedVolume{
		Spec: volumeSpec,
	}, nil
}
