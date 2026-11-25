/*
Copyright 2014 The Kubernetes Authors.

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

package volume

import (
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// Volume 表示节点上供 Pod 或宿主机使用的目录。
// 该接口的所有方法实现都必须是幂等的。
type Volume interface {
	// GetPath 返回该卷在 Pod 中应挂载到的路径。
	GetPath() string

	// MetricsProvider 内嵌用于暴露指标（如已用、可用空间）的方法。
	MetricsProvider
}

// BlockVolume 接口提供生成全局映射路径和 Pod 设备映射路径的方法。
type BlockVolume interface {
	// GetGlobalMapPath 返回包含块设备 bind mount 的全局映射路径。
	// 例如 plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}
	GetGlobalMapPath(spec *Spec) (string, error)
	// GetPodDeviceMapPath 返回 Pod 设备映射路径及与块设备关联的符号链接名。
	// 例如 pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/, {volumeName}
	GetPodDeviceMapPath() (string, string)

	// 若 MetricsProvider 已初始化，SupportsMetrics 应返回 true。
	SupportsMetrics() bool

	// 内嵌 MetricsProvider 以暴露卷指标（如已用、可用空间）。
	MetricsProvider
}

// MetricsProvider 暴露与卷相关的指标（如已用、可用空间）。
type MetricsProvider interface {
	// GetMetrics 返回卷的指标；某些实现中该调用开销较大。
	GetMetrics() (*Metrics, error)
}

// Metrics 表示卷的已用和可用字节数。
type Metrics struct {
	// 这些统计数据的更新时间。
	Time metav1.Time

	// Used 是卷已使用的总字节数。
	// 注意：块设备的该值可能大于其中文件的总大小。
	Used *resource.Quantity

	// Capacity 是卷底层存储的总容量（字节）。对于与宿主机共享文件系统的卷
	// （如 emptydir、hostpath），这是底层存储的大小；由于文件系统共享，
	// 该值不等于 Used + Available。
	Capacity *resource.Quantity

	// Available 是卷可用的存储空间（字节）。对于与宿主机共享文件系统的卷
	// （如 emptydir、hostpath），这是底层存储的可用空间，并与宿主机进程及
	// 其他卷共享。
	Available *resource.Quantity

	// InodesUsed 是卷已使用的 inode 总数。
	InodesUsed *resource.Quantity

	// Inodes 是卷中可用 inode 的总数。对于与宿主机共享文件系统的卷
	// （如 emptydir、hostpath），这是底层存储中的可用 inode 数，
	// 由于文件系统共享，该值不等于 InodesUsed + InodesFree。
	Inodes *resource.Quantity

	// InodesFree 是卷可用的 inode 数。对于与宿主机共享文件系统的卷
	// （如 emptydir、hostpath），这是底层存储中的空闲 inode 数，并与宿主机
	// 进程及其他卷共享。
	InodesFree *resource.Quantity
}

// Attributes 表示该 mounter 的属性。
type Attributes struct {
	ReadOnly       bool
	Managed        bool
	SELinuxRelabel bool
}

// MounterArgs 为 Mounter 提供易于扩展的参数。
type MounterArgs struct {
	// 设置 FsUser 时，卷的属主会被改为 FsUser 并赋予写权限；否则不产生副作用。
	// 目前仅 projection 服务账号令牌支持。
	FsUser              *int64
	FsGroup             *int64
	FSGroupChangePolicy *v1.PodFSGroupChangePolicy
	DesiredSize         *resource.Quantity
	SELinuxLabel        string
	Recorder            record.EventRecorder

	// 可选的卷属主变更接口，主要用于单元测试。
	VolumeOwnershipApplicator VolumeOwnershipChanger
	ReconstructedVolume       bool

	// 当 reconciler 认为卷已挂载到 Pod 时（例如由
	// CSIDriver.spec.requiresRepublish=true 触发的周期性重发布），IsRemount
	// 为 true。卷插件应据此避免破坏 Pod 正通过现有 bind mount 观察的状态
	// （如挂载目录、卷元数据文件）：失败重挂载的清理无法由后续成功的重挂载
	// 修复，只会导致 Pod 看到过期内容。
	IsRemount bool
}

// VolumeOwnershipChanger 用于修改卷属主和权限。
type VolumeOwnershipChanger interface {
	// AddProgressNotifier 设置变更进度事件的记录器。
	AddProgressNotifier(pod *v1.Pod, recorder record.EventRecorder) VolumeOwnershipChanger
	// ChangePermissions 执行权限变更。
	ChangePermissions() error
}

// VolumeOwnership 实现卷的属主和权限变更，并记录进度。
type VolumeOwnership struct {
	mounter             Mounter
	dir                 string
	fsGroup             *int64
	fsGroupChangePolicy *v1.PodFSGroupChangePolicy
	completionCallback  func(volumetypes.CompleteFuncParam)

	// 用于监控权限变更操作的进度。
	pod         *v1.Pod
	fileCounter atomic.Int64
	recorder    record.EventRecorder
}

// Mounter 接口提供设置/挂载卷的方法。
type Mounter interface {
	// 内嵌 Volume 以提供供 Docker bind 使用的路径。
	Volume

	// SetUp 准备卷并将其挂载/解包到插件自行决定的目录。挂载点及其内容应
	// 归属于 fsUser 或 fsGroup，以便 Pod 访问。该方法可能被多次调用，实现
	// 必须幂等。
	// 可返回以下错误类型：
	//   - TransientOperationFailure
	//   - UncertainProgressError
	//   - 其他类型的错误均视为最终错误
	SetUp(mounterArgs MounterArgs) error

	// SetUpAt 准备卷并将其挂载/解包到指定目录（该目录可以尚不存在）。挂载点
	// 及其内容应归属于 fsUser 或 fsGroup，以便 Pod 访问。该方法可能被多次
	// 调用，实现必须幂等。
	SetUpAt(dir string, mounterArgs MounterArgs) error
	// GetAttributes 返回 mounter 的属性，在 SetUp()/SetUpAt() 之后调用。
	GetAttributes() Attributes
}

// Unmounter 接口提供清理/卸载卷的方法。
type Unmounter interface {
	Volume
	// TearDown 从插件自行决定的目录卸载卷并清除 SetUp 留下的痕迹。
	TearDown() error
	// TearDownAt 从指定目录卸载卷并清除 SetUp 留下的痕迹。
	TearDownAt(dir string) error
}

// BlockVolumeMapper 是块卷的映射接口。
type BlockVolumeMapper interface {
	BlockVolume
}

// CustomBlockVolumeMapper 提供自定义的设置/映射卷方法。
type CustomBlockVolumeMapper interface {
	BlockVolumeMapper
	// SetUpDevice 以插件特定的方式在节点上准备卷。对大多数树内插件，
	// attacher.Attach() 和 attacher.WaitForAttach() 会完成必要工作。
	// 该方法可能被多次调用，实现必须幂等。
	// 准备成功时 SetUpDevice 返回 stagingPath。
	SetUpDevice() (stagingPath string, err error)

	// MapPodDevice 将块设备映射到某个路径并返回该路径。
	// 路径在 kubelet 节点重启前后必须保持唯一，否则可能导致块卷被意外销毁。
	// 返回空字符串时，使用 attacher.Attach() 和 attacher.WaitForAttach()
	// 返回的路径。
	MapPodDevice() (publishPath string, err error)

	// GetStagingPath 返回卷 staging 时使用的路径，主要供 CSI 插件使用。
	GetStagingPath() string
}

// BlockVolumeUnmapper 是块卷的取消映射接口。
type BlockVolumeUnmapper interface {
	BlockVolume
}

// CustomBlockVolumeUnmapper 提供自定义的清理/取消映射卷方法。
type CustomBlockVolumeUnmapper interface {
	BlockVolumeUnmapper
	// TearDownDevice 清除 SetUpDevice 留下的痕迹。若插件不可附加，
	// 该方法同时从节点上卸载卷。
	TearDownDevice(mapPath string, devicePath string) error

	// UnmapPodDevice 清除 MapPodDevice 留下的痕迹。
	UnmapPodDevice() error
}

// Provisioner 接口创建 PersistentVolume 模板，并可在基础设施提供方
// 中将卷创建为新资源。
type Provisioner interface {
	// Provision 在存储系统中分配底层卷来创建资源。该方法应阻塞至完成，
	// 并返回代表所创建存储资源的 PersistentVolume。
	Provision(selectedNode *v1.Node, allowedTopologies []v1.TopologySelectorTerm) (*v1.PersistentVolume, error)
}

// Deleter 从底层存储提供方删除资源。调用应阻塞至删除完成。
// 返回错误表示卷回收失败，返回 nil 表示成功。
type Deleter interface {
	Volume
	// 该方法应阻塞至完成。返回的 deletedVolumeInUseError 不会作为错误上报，
	// 而是以 "Info" 事件发送给被删除的 PV；卷控制器会在下次周期性同步时
	// 重试删除。这可用于推迟删除正在从节点上分离的卷：此类卷的删除本来就
	// 会失败，上报此类错误只会让用户困惑。
	Delete() error
}

// Attacher 可将卷附加到节点。
type Attacher interface {
	DeviceMounter

	// Attach 将给定 spec 指定的卷附加到指定名称的节点，成功时返回设备上
	// 附加到节点后的设备路径。
	Attach(spec *Spec, nodeName types.NodeName) (string, error)

	// VolumesAreAttached 检查卷列表是否仍附加在指定节点上，返回从卷 spec
	// 到检查结果的映射。检查过程中出错时返回该错误。
	VolumesAreAttached(specs []*Spec, nodeName types.NodeName) (map[*Spec]bool, error)

	// WaitForAttach 阻塞直到设备附加到本节点，成功时返回设备路径；
	// 若在给定超时时间内未能附加，则返回错误。
	WaitForAttach(spec *Spec, devicePath string, pod *v1.Pod, timeout time.Duration) (string, error)
}

// DeviceMounterArgs 为 DeviceMounter 提供可选辅助参数。
type DeviceMounterArgs struct {
	FsGroup      *int64
	SELinuxLabel string
}

// DeviceMounter 可将块卷挂载到全局路径。
type DeviceMounter interface {
	// GetDeviceMountPath 返回设备附加后应挂载到的路径，这是供各个卷
	// bind mount 使用的全局挂载点。
	GetDeviceMountPath(spec *Spec) (string, error)

	// MountDevice 将磁盘挂载到全局路径，供各 Pod 随后 bind mount。
	// 若卷插件未实现 Attach 或 WaitForAttach，devicePath 可能为空。
	// 可返回以下错误类型：
	//   - TransientOperationFailure
	//   - UncertainProgressError
	//   - 其他类型的错误均视为最终错误
	MountDevice(spec *Spec, devicePath string, deviceMountPath string, deviceMounterArgs DeviceMounterArgs) error
}

// Detacher 可从节点上分离卷。
type Detacher interface {
	DeviceUnmounter
	// Detach 从指定名称的节点分离给定卷。
	// volumeName 是插件 GetVolumeName() 返回的卷名。
	Detach(volumeName string, nodeName types.NodeName) error
}

// DeviceUnmounter 可从全局路径卸载块卷。
type DeviceUnmounter interface {
	// UnmountDevice 卸载磁盘的全局挂载。仅应在所有 bind mount
	// 都已卸载后调用。
	UnmountDevice(deviceMountPath string) error
}
