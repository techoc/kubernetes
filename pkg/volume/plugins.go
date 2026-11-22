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

// Package volume 定义卷插件的抽象、管理器以及卷规格等核心类型。
package volume

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

// ProbeOperation 表示探测卷插件时的操作类型。
type ProbeOperation uint32

// ProbeEvent 表示卷插件探测过程中触发的事件。
type ProbeEvent struct {
	Plugin     VolumePlugin // 被添加/更新/移除的 VolumePlugin；若 ProbeEvent.Op 为 ProbeRemove，Plugin 应为 nil
	PluginName string
	Op         ProbeOperation // 对该插件执行的操作
}

const (
	// VolumeParameterFSType 是可在 StorageClass 中指定的通用参数，用于指定期望的文件系统类型。
	// 基于块设备的 Provisioner 应当实现对该参数的支持。
	// 取值必须是宿主操作系统支持的文件系统类型，
	// 例如 "ext4"、"xfs"、"ntfs"；默认值由具体 Provisioner 决定。
	VolumeParameterFSType = "fstype"

	// ProbeAddOrUpdate 表示添加或更新一个探测结果的操作。
	ProbeAddOrUpdate ProbeOperation = 1 << iota

	// ProbeRemove 表示移除一个探测结果的操作。
	// 该操作用于表明此前添加的探测结果应当被移除。
	ProbeRemove
)

// ErrNoPluginMatched 在没有卷插件匹配请求的类型时返回。
var ErrNoPluginMatched = errors.New("no volume plugin matched")

// VolumeOptions 包含与卷相关的选项信息。
type VolumeOptions struct {
	// 以下属性是 volume.Provisioner 所必需的。
	// TODO: 当管理员可以配置多种 Provisioner 时，将这些内容重构到卷之外。

	// 持久卷的回收策略
	PersistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy
	// 持久卷的挂载选项
	MountOptions []string
	// 建议用于待创建 PersistentVolume 的 PV.Name。
	// 这是一个生成的名称，在 Kubernetes 集群中保证唯一。
	// 如果不将其用作卷名称，请通过与你自己的值组合或自行生成唯一值来保证唯一性。
	PVName string
	// PVC 指向触发创建新 PV 的声明。
	// Provisioner *必须* 创建能被该 PVC 匹配的 PV，
	// 即具备所需的容量、accessMode、与 PVC.Selector 匹配的标签等。
	PVC *v1.PersistentVolumeClaim
	// 来自 StorageClass 的卷制备参数
	Parameters map[string]string
}

// NodeResizeOptions 包含节点扩容时传入的选项。
type NodeResizeOptions struct {
	VolumeSpec *Spec

	// DevicePath 是节点上实际设备的位置；对于 CSI，这可能只是 volumeID。
	DevicePath string

	// DeviceMountPath 是设备上节点上的挂载位置。若卷类型可附着，
	// 该路径为全局挂载路径；否则为卷为 Pod 挂载的位置。
	DeviceMountPath string

	// DeviceStagePath 保存卷被暂存（stage）的位置。
	DeviceStagePath string

	NewSize resource.Quantity
	OldSize resource.Quantity
}

// DynamicPluginProber 定义探测动态卷插件的方法。
type DynamicPluginProber interface {
	Init() error

	// 聚合成功驱动的事件以及失败驱动的错误
	Probe() (events []ProbeEvent, err error)
}

// VolumePlugin 是可在 Kubernetes 节点上使用（例如被 kubelet 使用）的卷插件接口，
// 用于实例化和管理卷。
type VolumePlugin interface {
	// Init 初始化插件。在所有 New* 调用之前只会调用一次，插件实现可以依赖这一点。
	Init(host VolumeHost) error

	// GetPluginName 返回插件名称。插件必须使用带命名空间的名称，
	// 例如 "example.com/volume"，且恰好包含一个 '/' 字符。
	// "kubernetes.io" 命名空间保留给随 Kubernetes 一起发布的插件。
	GetPluginName() string

	// GetVolumeName 返回用于唯一标识指定卷 spec 所引用的实际后端设备、
	// 目录、路径等的名称/ID。
	// 对于可附着卷，该值必须能够传回卷的 Detach 方法，以标识要操作的设备。
	// 若插件不支持给定的 spec，则返回错误。
	GetVolumeName(spec *Spec) (string, error)

	// CanSupport 检查插件是否支持来自 API 的给定卷规格。
	// spec 指针应视为常量。
	CanSupport(spec *Spec) bool

	// RequiresRemount 在插件要求重新执行挂载调用时返回 true。
	// Downward API 等会自动更新的卷依赖此机制来更新卷内容。
	RequiresRemount(spec *Spec) bool

	// NewMounter 根据 API 规格创建新的 volume.Mounter。
	// spec 指针的所有权*不*转移。
	// - spec: v1.Volume 规格
	// - pod: 所属的 Pod
	NewMounter(spec *Spec, podRef *v1.Pod) (Mounter, error)

	// NewUnmounter 根据可恢复的状态创建新的 volume.Unmounter。
	// - name: v1.Volume 规格中的卷名
	// - podUID: 所属 Pod 的 UID
	NewUnmounter(name string, podUID types.UID) (Unmounter, error)

	// ConstructVolumeSpec 根据给定的卷名和 volumePath 构造卷规格。
	// 由于输入信息有限，构造出的 spec 可能不完整。卷管理器通过读取磁盘上的
	// 卷目录，使用该函数重建卷规格。
	ConstructVolumeSpec(volumeName, volumePath string) (ReconstructedVolume, error)

	// SupportsMountOption 在卷插件支持挂载选项时返回 true。
	SupportsMountOption() bool

	// SupportsSELinuxContextMount 在卷插件支持对给定卷执行
	// mount -o context=XYZ 时返回 true。
	SupportsSELinuxContextMount(spec *Spec) (bool, error)
}

// PersistentVolumePlugin 是 VolumePlugin 的扩展接口，供希望提供长期数据持久化的卷使用。
type PersistentVolumePlugin interface {
	VolumePlugin
	// GetAccessModes 描述给定卷可被访问/挂载的方式。
	GetAccessModes() []v1.PersistentVolumeAccessMode
}

// RecyclableVolumePlugin 是 VolumePlugin 的扩展接口，供那些在重新提供给新声明之前
// 需要先回收的持久卷使用。
type RecyclableVolumePlugin interface {
	VolumePlugin

	// Recycle 知道如何在卷从 PersistentVolumeClaim 释放后回收该资源。
	// Recycle 会使用传入的 recorder 记录用户可能感兴趣的事件；
	// 调用方应将这些事件传递给正在被回收的 PV。
	Recycle(pvName string, spec *Spec, eventRecorder recyclerclient.RecycleEventRecorder) error
}

// DeletableVolumePlugin 是 VolumePlugin 的扩展接口，供那些在从
// PersistentVolumeClaim 释放后需要从集群中删除的持久卷使用。
type DeletableVolumePlugin interface {
	VolumePlugin
	// NewDeleter 创建一个新的 volume.Deleter，它知道如何在卷从声明释放后，
	// 按照底层存储提供商的规则删除该资源。
	NewDeleter(logger klog.Logger, spec *Spec) (Deleter, error)
}

// ProvisionableVolumePlugin 是 VolumePlugin 的扩展接口，用于为集群创建卷。
type ProvisionableVolumePlugin interface {
	VolumePlugin
	// NewProvisioner 创建一个新的 volume.Provisioner，它知道如何按照插件底层
	// 存储提供商的规则创建 PersistentVolume。
	NewProvisioner(logger klog.Logger, options VolumeOptions) (Provisioner, error)
}

// AttachableVolumePlugin 是 VolumePlugin 的扩展接口，供挂载前需要先附着到节点的卷使用。
type AttachableVolumePlugin interface {
	DeviceMountableVolumePlugin
	NewAttacher() (Attacher, error)
	NewDetacher() (Detacher, error)
	// CanAttach 检查给定的卷规格是否可附着。
	CanAttach(spec *Spec) (bool, error)
	VerifyExhaustedResource(spec *Spec) bool
}

// DeviceMountableVolumePlugin 是 VolumePlugin 的扩展接口，供那些在绑定到 Pod 之前
// 需要先将设备挂载到节点的卷使用。
type DeviceMountableVolumePlugin interface {
	VolumePlugin
	NewDeviceMounter() (DeviceMounter, error)
	NewDeviceUnmounter() (DeviceUnmounter, error)
	GetDeviceMountRefs(deviceMountPath string) ([]string, error)
	// CanDeviceMount 判断 volume.Spec 中的设备是否可挂载。
	CanDeviceMount(spec *Spec) (bool, error)
}

// ExpandableVolumePlugin 是 VolumePlugin 的扩展接口，供可通过控制面
// ExpandVolumeDevice 调用进行扩容的卷使用。
type ExpandableVolumePlugin interface {
	VolumePlugin
	ExpandVolumeDevice(spec *Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error)
	RequiresFSResize() bool
}

// NodeExpandableVolumePlugin 是 VolumePlugin 的扩展接口，供需要通过 NodeExpand
// 调用在节点上扩容的卷使用。
type NodeExpandableVolumePlugin interface {
	VolumePlugin
	RequiresFSResize() bool
	// NodeExpand 在给定的 deviceMountPath 上扩容卷，调整成功时返回 true。
	NodeExpand(resizeOptions NodeResizeOptions) (bool, error)
}

// ResizableEphemeralVolumePlugin 是 VolumePlugin 的扩展接口，供那些无需经过标准
// 异步调谐流程、可直接在节点上调整的卷使用。
type ResizableEphemeralVolumePlugin interface {
	VolumePlugin
	// ResizeEphemeralVolume 在节点上调整卷大小。
	ResizeEphemeralVolume(spec *Spec, pod *v1.Pod, newSize *resource.Quantity) error
}

// BlockVolumePlugin 是 VolumePlugin 的扩展接口，用于支持块设备卷。
type BlockVolumePlugin interface {
	VolumePlugin
	// NewBlockVolumeMapper 根据 API 规格创建新的 volume.BlockVolumeMapper。
	// spec 指针的所有权*不*转移。
	// - spec: v1.Volume 规格
	// - pod: 所属的 Pod
	NewBlockVolumeMapper(spec *Spec, podRef *v1.Pod) (BlockVolumeMapper, error)
	// NewBlockVolumeUnmapper 根据可恢复的状态创建新的 volume.BlockVolumeUnmapper。
	// - name: v1.Volume 规格中的卷名
	// - podUID: 所属 Pod 的 UID
	NewBlockVolumeUnmapper(name string, podUID types.UID) (BlockVolumeUnmapper, error)
	// ConstructBlockVolumeSpec 根据给定的 podUID、卷名和 Pod 设备映射路径构造卷规格。
	// 由于输入信息有限，构造出的 spec 可能不完整。卷管理器通过读取磁盘上的
	// 卷目录，使用该函数重建卷规格。
	ConstructBlockVolumeSpec(podUID types.UID, volumeName, volumePath string) (*Spec, error)
}

// TODO(#14217)
// 作为 Volume Host 重构的一部分，我们开始为特定宿主创建专用的 Volume Host。
// 各特定宿主的新方法可以添加在此处。
// 目前调用方通过类型断言获取特定类型的 Volume Host；但最终目标应是
// 将特定的 Volume Host 直接传给需要它的函数（而不是使用一个包罗万象的
// VolumeHost 接口）。

// KubeletVolumeHost 是 Kubelet 专用的接口，插件可用它访问 kubelet。
type KubeletVolumeHost interface {
	// SetKubeletError 让插件在 Kubelet 运行时状态上设置错误，
	// 使 Kubelet 以所提供错误信息发布 NotReady 状态。
	SetKubeletError(err error)

	// GetInformerFactory 返回 CSIDriverLister 使用的 informer 工厂。
	GetInformerFactory() informers.SharedInformerFactory
	// CSIDriverLister 返回 CSIDriver API 对象的 informer lister。
	CSIDriverLister() storagelistersv1.CSIDriverLister
	// CSIDriversSynced 返回 CSIDriver API 对象的 informer synced 函数。
	CSIDriversSynced() cache.InformerSynced
	// WaitForCacheSync 是等待 CSIDriverLister 缓存同步的辅助函数。
	WaitForCacheSync() error
	// 返回 hostutil.HostUtils。
	GetHostUtil() hostutil.HostUtils

	// 从指定名称的 ClusterTrustBundle 返回信任锚。
	GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error)

	// 从按签名者名称和标签选择器选出的 ClusterTrustBundle 返回信任锚。
	GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error)

	// 返回指定 podCertificate 投射卷来源的凭据包。
	GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error)
}

// CSIDriverVolumeHost 是可访问 CSIDriverLister 的卷宿主。
type CSIDriverVolumeHost interface {
	// CSIDriverLister 返回 CSIDriver API 对象的 informer lister。
	CSIDriverLister() storagelistersv1.CSIDriverLister
}

// AttachDetachVolumeHost 是 AttachDetach Controller 专用的接口，插件可用它访问
// Attach Detach Controller 上的方法。
type AttachDetachVolumeHost interface {
	CSIDriverVolumeHost
	// CSINodeLister 返回 CSINode API 对象的 informer lister。
	CSINodeLister() storagelistersv1.CSINodeLister
	// VolumeAttachmentLister 返回 VolumeAttachment API 对象的 informer lister。
	VolumeAttachmentLister() storagelistersv1.VolumeAttachmentLister
	// IsAttachDetachController 是接口标记，用于将 AttachDetachVolumeHost
	// 严格绑定到 attachDetachController。
	IsAttachDetachController() bool
}

// VolumeHost 是插件可用来访问 kubelet 的接口。
type VolumeHost interface {
	// GetPluginDir 返回某个插件可存放数据的目录的绝对路径。
	// 该目录目前可能还不存在于磁盘上。对于按 Pod 划分的插件数据，
	// 请参见 GetPodPluginDir()。
	GetPluginDir(pluginName string) string

	// GetVolumeDevicePluginDir 返回某个插件可存放数据的目录的绝对路径。
	// 例如 plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/
	GetVolumeDevicePluginDir(pluginName string) string

	// GetPodsDir 返回存放所有 Pod 信息的目录的绝对路径。
	GetPodsDir() string

	// GetPodVolumeDir 返回表示给定 Pod 在指定插件下的指定卷的目录的绝对路径。
	// 如果指定的 Pod 不存在，该调用的返回值也可能不存在。
	GetPodVolumeDir(podUID types.UID, pluginName string, volumeName string) string

	// GetPodPluginDir 返回某个插件可为给定 Pod 存放数据的目录的绝对路径。
	// 如果指定的 Pod 不存在，该调用的返回值也可能不存在；该目录当前也可能尚未落在磁盘上。
	GetPodPluginDir(podUID types.UID, pluginName string) string

	// GetPodVolumeDeviceDir 返回表示给定 Pod 的指定插件的目录的绝对路径。
	// 如果指定的 Pod 不存在，该调用的返回值也可能不存在。
	// 例如 pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/
	GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string

	// GetKubeClient 返回客户端接口。
	GetKubeClient() clientset.Interface

	// NewWrapperMounter 找到合适的插件来处理给定的 spec。
	// 这用于实现“包装”其他插件的卷插件，例如 "secret" 卷就是基于
	// "emptyDir" 卷实现的。
	NewWrapperMounter(volName string, spec Spec, pod *v1.Pod) (Mounter, error)

	// NewWrapperUnmounter 找到合适的插件来处理给定的 spec。
	// 更多背景信息见 NewWrapperMounter 的注释。
	NewWrapperUnmounter(volName string, spec Spec, podUID types.UID) (Unmounter, error)

	// 获取 mounter 接口。
	GetMounter() mount.Interface

	// 返回节点可分配资源。
	GetNodeAllocatable() (v1.ResourceList, error)

	// 返回一个用于获取 secret 的函数。
	GetSecretFunc() func(namespace, name string) (*v1.Secret, error)

	// 返回一个用于获取 configmap 的函数。
	GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error)

	// 返回一个用于获取 ServiceAccount token 的函数。
	GetServiceAccountTokenFunc() func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)

	// 返回一个用于删除 ServiceAccount token 的函数。
	DeleteServiceAccountTokenFunc() func(podUID types.UID)

	// 返回节点上的标签。
	GetNodeLabels() (map[string]string, error)

	// 返回节点名称。
	GetNodeName() types.NodeName

	// 从节点状态返回已附着的卷。
	GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error)

	// 返回 kubelet 的事件记录器。
	GetEventRecorder() record.EventRecorder

	// 返回应用于执行 subpath 操作的接口。
	GetSubpather() subpath.Interface
}

// VolumePluginMgr 跟踪已注册的插件。
type VolumePluginMgr struct {
	mutex         sync.RWMutex
	plugins       map[string]VolumePlugin
	prober        DynamicPluginProber
	probedPlugins map[string]VolumePlugin
	Host          VolumeHost
}

// Spec 是卷的内部表示；所有 API 卷类型都会转换为 Spec。
type Spec struct {
	Volume                          *v1.Volume
	PersistentVolume                *v1.PersistentVolume
	ReadOnly                        bool
	InlineVolumeSpecForCSIMigration bool
	Migrated                        bool
}

// Name 返回 Volume 或 PersistentVolume 的名称，二者必须有一个不为 nil。
func (spec *Spec) Name() string {
	switch {
	case spec.Volume != nil:
		return spec.Volume.Name
	case spec.PersistentVolume != nil:
		return spec.PersistentVolume.Name
	default:
		return ""
	}
}

// IsKubeletExpandable 对只能由节点（而非控制器）扩容的卷类型返回 true。
// 目前只有 Flex 卷属于这一类，因为它通常不会安装在控制器上。
func (spec *Spec) IsKubeletExpandable() bool {
	switch {
	case spec.Volume != nil:
		return spec.Volume.FlexVolume != nil
	case spec.PersistentVolume != nil:
		return spec.PersistentVolume.Spec.FlexVolume != nil
	default:
		return false
	}
}

// KubeletExpandablePluginName 创建并返回插件名称。
// 控制器不支持卷扩容，因此在那里查找插件会失败；但控制器仍需要一个插件名，
// 该方法即用于这种场景。
func (spec *Spec) KubeletExpandablePluginName() string {
	switch {
	case spec.Volume != nil && spec.Volume.FlexVolume != nil:
		return spec.Volume.FlexVolume.Driver
	case spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil:
		return spec.PersistentVolume.Spec.FlexVolume.Driver
	default:
		return ""
	}
}

// VolumeConfig 是卷插件接收配置的方式。与插件对应的实例会传给插件的
// ProbeVolumePlugins(config) 函数。托管插件的二进制会提供合理的默认值，
// 同时允许覆盖这些默认值；之后这些配置值被设置到 VolumeConfig 实例中并传给插件。
//
// VolumeConfig 中的值应当与多个插件相关，但不必与所有插件相关。这里倾向于
// 使用强类型。所有配置项都必须有一个描述性但不特定的名称
// （例如 RecyclerMinimumTimeout 可以，RecyclerMinimumTimeoutForNFS 不行）。
// 配置实例会直接交给插件，因此不需要针对特定插件的配置名称；否则还会在
// VolumeConfig 结构体中不当地暴露插件。
//
// OtherAttributes 是一个字符串映射，用于插件的一次性配置或仅与单个插件
// 相关的配置。所有值都以字符串传递，需要插件自行解释。用字符串传递配置是
// 最不理想的方式，但可用于真正一次性的配置。二进制仍应在把 CLI 值作为字符串
// 写入 OtherAttributes 之前，使用强类型绑定这些值。
type VolumeConfig struct {
	// RecyclerPodTemplate 是能够清理持久卷的 Pod 模板。插件会在该模板基础上
	// 按自身需求覆盖 Pod 的特定属性。需要覆盖的属性见
	// NewPersistentVolumeRecyclerPodTemplate。
	RecyclerPodTemplate *v1.Pod

	// RecyclerMinimumTimeout 是回收 Pod 的 ActiveDeadlineSeconds 属性的最小秒数。
	// 在该最小超时之上还会叠加按容量每 Gi 增加的时长。
	RecyclerMinimumTimeout int

	// RecyclerTimeoutIncrement 是持久容量每增加 1 Gi，为回收 Pod 的
	// ActiveDeadlineSeconds 增加的秒数。例如：5Gi 卷 × 30s 增量 = 150s，
	// 加上 30s 最小值 = 回收 Pod 的 ActiveDeadlineSeconds 为 180s。
	RecyclerTimeoutIncrement int

	// PVName 是正在被回收的 PersistentVolume 实例的名称，
	// 用于生成唯一的回收 Pod 名称。
	PVName string

	// OtherAttributes 以字符串形式存放配置。这些字符串对系统是不透明的，
	// 只有托管插件的二进制和插件自身能理解。
	OtherAttributes map[string]string

	// ProvisioningEnabled 配置是否启用该插件的制备功能，目前仅用于 host_path 插件。
	ProvisioningEnabled bool
}

// ReconstructedVolume 包含由 ConstructVolumeSpec() 重建的卷的相关信息。
type ReconstructedVolume struct {
	// Spec 是已挂载卷的卷规格。
	Spec *Spec
	// SELinuxMountContext 是 -o context=XYZ 挂载选项的值；为空表示未使用该挂载选项。
	SELinuxMountContext string
}

// NewSpecFromVolume 从 v1.Volume 创建 Spec。
func NewSpecFromVolume(vs *v1.Volume) *Spec {
	return &Spec{
		Volume: vs,
	}
}

// NewSpecFromPersistentVolume 从 v1.PersistentVolume 创建 Spec。
func NewSpecFromPersistentVolume(pv *v1.PersistentVolume, readOnly bool) *Spec {
	return &Spec{
		PersistentVolume: pv,
		ReadOnly:         readOnly,
	}
}

// InitPlugins 初始化每个插件；所有插件的名称必须唯一。
// 在任意插件上调用 New* 方法之前，该方法必须且只能调用一次。
func (pm *VolumePluginMgr) InitPlugins(plugins []VolumePlugin, prober DynamicPluginProber, host VolumeHost) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.Host = host

	if prober == nil {
		// 使用空探测器避免 nil 解引用。
		pm.prober = &dummyPluginProber{}
	} else {
		pm.prober = prober
	}
	if err := pm.prober.Init(); err != nil {
		// 探测器初始化失败不应影响其他插件的初始化。
		klog.ErrorS(err, "Error initializing dynamic plugin prober")
		pm.prober = &dummyPluginProber{}
	}

	if pm.plugins == nil {
		pm.plugins = map[string]VolumePlugin{}
	}
	if pm.probedPlugins == nil {
		pm.probedPlugins = map[string]VolumePlugin{}
	}

	allErrs := []error{}
	for _, plugin := range plugins {
		name := plugin.GetPluginName()
		if errs := validation.IsQualifiedName(name); len(errs) != 0 {
			allErrs = append(allErrs, fmt.Errorf("volume plugin has invalid name: %q: %s", name, strings.Join(errs, ";")))
			continue
		}

		if _, found := pm.plugins[name]; found {
			allErrs = append(allErrs, fmt.Errorf("volume plugin %q was registered more than once", name))
			continue
		}
		err := plugin.Init(host)
		if err != nil {
			klog.ErrorS(err, "Failed to load volume plugin", "pluginName", name)
			allErrs = append(allErrs, err)
			continue
		}
		pm.plugins[name] = plugin
		klog.V(1).InfoS("Loaded volume plugin", "pluginName", name)
	}
	pm.refreshProbedPlugins()
	return utilerrors.NewAggregate(allErrs)
}

// initProbedPlugin 初始化一个动态探测到的插件并校验其名称。
func (pm *VolumePluginMgr) initProbedPlugin(probedPlugin VolumePlugin) error {
	name := probedPlugin.GetPluginName()
	if errs := validation.IsQualifiedName(name); len(errs) != 0 {
		return fmt.Errorf("volume plugin has invalid name: %q: %s", name, strings.Join(errs, ";"))
	}

	err := probedPlugin.Init(pm.Host)
	if err != nil {
		return fmt.Errorf("failed to load volume plugin %s, error: %s", name, err.Error())
	}

	klog.V(1).InfoS("Loaded volume plugin", "pluginName", name)
	return nil
}

// FindPluginBySpec 查找能支持给定卷规格的插件。没有插件支持或有多个插件
// 支持时返回错误。
func (pm *VolumePluginMgr) FindPluginBySpec(spec *Spec) (VolumePlugin, error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	if spec == nil {
		return nil, fmt.Errorf("could not find plugin because volume spec is nil")
	}

	var match VolumePlugin
	matchedPluginNames := []string{}
	for _, v := range pm.plugins {
		if v.CanSupport(spec) {
			match = v
			matchedPluginNames = append(matchedPluginNames, v.GetPluginName())
		}
	}
	pm.refreshProbedPlugins()

	for _, plugin := range pm.probedPlugins {
		if plugin.CanSupport(spec) {
			match = plugin
			matchedPluginNames = append(matchedPluginNames, plugin.GetPluginName())
		}
	}

	if len(matchedPluginNames) == 0 {
		return nil, ErrNoPluginMatched
	}
	if len(matchedPluginNames) > 1 {
		return nil, fmt.Errorf("multiple volume plugins matched: %s", strings.Join(matchedPluginNames, ","))
	}

	return match, nil
}

// FindPluginByName 按名称获取插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindPluginByName(name string) (VolumePlugin, error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	var match VolumePlugin
	if v, found := pm.plugins[name]; found {
		match = v
	}
	pm.refreshProbedPlugins()
	if plugin, found := pm.probedPlugins[name]; found {
		if match != nil {
			return nil, fmt.Errorf("multiple volume plugins matched: %s and %s", match.GetPluginName(), plugin.GetPluginName())
		}
		match = plugin
	}

	if match == nil {
		return nil, fmt.Errorf("no volume plugin matched name: %s", name)
	}
	return match, nil
}

// refreshProbedPlugins 处理探测事件：按需初始化新探测到的插件，并据此更新
// probedPlugins 缓存。
func (pm *VolumePluginMgr) refreshProbedPlugins() {
	events, err := pm.prober.Probe()

	if err != nil {
		klog.ErrorS(err, "Error dynamically probing plugins")
	}

	// 即使探测函数返回了错误，它也可能同时返回有效插件列表；
	// 仍须处理这些插件，否则它们会因为事件只触发一次而被永久跳过。

	for _, event := range events {
		if event.Op == ProbeAddOrUpdate {
			if err := pm.initProbedPlugin(event.Plugin); err != nil {
				klog.ErrorS(err, "Error initializing dynamically probed plugin",
					"pluginName", event.Plugin.GetPluginName())
				continue
			}
			pm.probedPlugins[event.Plugin.GetPluginName()] = event.Plugin
		} else if event.Op == ProbeRemove {
			// ProbeRemove 事件只带 PluginName，不提供 Plugin 本身。
			delete(pm.probedPlugins, event.PluginName)
		} else {
			klog.ErrorS(nil, "Unknown Operation on PluginName.",
				"pluginName", event.Plugin.GetPluginName())
		}
	}
}

// FindPersistentPluginBySpec 查找能支持给定卷规格的持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindPersistentPluginBySpec(spec *Spec) (PersistentVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, fmt.Errorf("could not find volume plugin for spec: %#v", spec)
	}
	if persistentVolumePlugin, ok := volumePlugin.(PersistentVolumePlugin); ok {
		return persistentVolumePlugin, nil
	}
	return nil, fmt.Errorf("no persistent volume plugin matched")
}

// FindPersistentPluginByName 按名称获取持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindPersistentPluginByName(name string) (PersistentVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}
	if persistentVolumePlugin, ok := volumePlugin.(PersistentVolumePlugin); ok {
		return persistentVolumePlugin, nil
	}
	return nil, fmt.Errorf("no persistent volume plugin matched")
}

// FindRecyclablePluginBySpec 按规格获取可回收的持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindRecyclablePluginBySpec(spec *Spec) (RecyclableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if recyclableVolumePlugin, ok := volumePlugin.(RecyclableVolumePlugin); ok {
		return recyclableVolumePlugin, nil
	}
	return nil, fmt.Errorf("no recyclable volume plugin matched")
}

// FindProvisionablePluginByName 按名称获取可制备的持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindProvisionablePluginByName(name string) (ProvisionableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}
	if provisionableVolumePlugin, ok := volumePlugin.(ProvisionableVolumePlugin); ok {
		return provisionableVolumePlugin, nil
	}
	return nil, fmt.Errorf("no provisionable volume plugin matched")
}

// FindDeletablePluginBySpec 按规格获取可删除的持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindDeletablePluginBySpec(spec *Spec) (DeletableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if deletableVolumePlugin, ok := volumePlugin.(DeletableVolumePlugin); ok {
		return deletableVolumePlugin, nil
	}
	return nil, fmt.Errorf("no deletable volume plugin matched")
}

// FindDeletablePluginByName 按名称获取可删除的持久卷插件；找不到时返回错误。
func (pm *VolumePluginMgr) FindDeletablePluginByName(name string) (DeletableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}
	if deletableVolumePlugin, ok := volumePlugin.(DeletableVolumePlugin); ok {
		return deletableVolumePlugin, nil
	}
	return nil, fmt.Errorf("no deletable volume plugin matched")
}

// FindAttachablePluginBySpec 按规格获取可附着的持久卷插件。
// 与其他 "FindPlugin" 方法不同，找不到插件时不会返回错误：所有卷都需要
// mounter 和 unmounter，但并非每个卷都有 attacher/detacher。
func (pm *VolumePluginMgr) FindAttachablePluginBySpec(spec *Spec) (AttachableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if attachableVolumePlugin, ok := volumePlugin.(AttachableVolumePlugin); ok {
		if canAttach, err := attachableVolumePlugin.CanAttach(spec); err != nil {
			return nil, err
		} else if canAttach {
			return attachableVolumePlugin, nil
		}
	}
	return nil, nil
}

// FindAttachablePluginByName 按名称获取可附着的卷插件。
// 与其他 "FindPlugin" 方法不同，找不到插件时不会返回错误：所有卷都需要
// mounter 和 unmounter，但并非每个卷都有 attacher/detacher。
func (pm *VolumePluginMgr) FindAttachablePluginByName(name string) (AttachableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}
	if attachablePlugin, ok := volumePlugin.(AttachableVolumePlugin); ok {
		return attachablePlugin, nil
	}
	return nil, nil
}

// FindDeviceMountablePluginBySpec 按规格获取支持设备挂载的持久卷插件。
func (pm *VolumePluginMgr) FindDeviceMountablePluginBySpec(spec *Spec) (DeviceMountableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if deviceMountableVolumePlugin, ok := volumePlugin.(DeviceMountableVolumePlugin); ok {
		if canMount, err := deviceMountableVolumePlugin.CanDeviceMount(spec); err != nil {
			return nil, err
		} else if canMount {
			return deviceMountableVolumePlugin, nil
		}
	}
	return nil, nil
}

// FindDeviceMountablePluginByName 按名称获取支持设备挂载的持久卷插件。
func (pm *VolumePluginMgr) FindDeviceMountablePluginByName(name string) (DeviceMountableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}
	if deviceMountableVolumePlugin, ok := volumePlugin.(DeviceMountableVolumePlugin); ok {
		return deviceMountableVolumePlugin, nil
	}
	return nil, nil
}

// FindExpandablePluginBySpec 按规格获取可扩容的持久卷插件。
func (pm *VolumePluginMgr) FindExpandablePluginBySpec(spec *Spec) (ExpandableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		if spec.IsKubeletExpandable() {
			// 对于只能由 kubelet 扩容的卷，返回一个空插件，
			// 让控制器上的扩容流程直接成功。
			klog.V(4).InfoS("FindExpandablePluginBySpec -> returning noopExpandableVolumePluginInstance", "specName", spec.Name())
			return &noopExpandableVolumePluginInstance{spec}, nil
		}
		if errors.Is(err, ErrNoPluginMatched) {
			return nil, nil
		}
		klog.V(4).InfoS("FindExpandablePluginBySpec -> err", "specName", spec.Name(), "err", err)
		return nil, err
	}

	if expandableVolumePlugin, ok := volumePlugin.(ExpandableVolumePlugin); ok {
		return expandableVolumePlugin, nil
	}
	return nil, nil
}

// FindExpandablePluginByName 按名称获取可扩容的持久卷插件。
func (pm *VolumePluginMgr) FindExpandablePluginByName(name string) (ExpandableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}

	if expandableVolumePlugin, ok := volumePlugin.(ExpandableVolumePlugin); ok {
		return expandableVolumePlugin, nil
	}
	return nil, nil
}

// FindMapperPluginBySpec 按规格获取块卷插件。
func (pm *VolumePluginMgr) FindMapperPluginBySpec(spec *Spec) (BlockVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}

	if blockVolumePlugin, ok := volumePlugin.(BlockVolumePlugin); ok {
		return blockVolumePlugin, nil
	}
	return nil, nil
}

// FindMapperPluginByName 按名称获取块卷插件。
func (pm *VolumePluginMgr) FindMapperPluginByName(name string) (BlockVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}

	if blockVolumePlugin, ok := volumePlugin.(BlockVolumePlugin); ok {
		return blockVolumePlugin, nil
	}
	return nil, nil
}

// FindNodeExpandablePluginBySpec 按规格获取可在节点上扩容的持久卷插件。
func (pm *VolumePluginMgr) FindNodeExpandablePluginBySpec(spec *Spec) (NodeExpandableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginBySpec(spec)
	if err != nil {
		return nil, err
	}
	if fsResizablePlugin, ok := volumePlugin.(NodeExpandableVolumePlugin); ok {
		return fsResizablePlugin, nil
	}
	return nil, nil
}

// FindNodeExpandablePluginByName 按名称获取可在节点上扩容的持久卷插件。
func (pm *VolumePluginMgr) FindNodeExpandablePluginByName(name string) (NodeExpandableVolumePlugin, error) {
	volumePlugin, err := pm.FindPluginByName(name)
	if err != nil {
		return nil, err
	}

	if fsResizablePlugin, ok := volumePlugin.(NodeExpandableVolumePlugin); ok {
		return fsResizablePlugin, nil
	}

	return nil, nil
}

// Run 启动卷插件管理器，初始化并运行管理卷插件所需的任务；
// 通常在插件管理生命周期开始时调用。
func (pm *VolumePluginMgr) Run(stopCh <-chan struct{}) {
	kletHost, ok := pm.Host.(KubeletVolumeHost)
	if ok {
		// 启动 CSIDriver 的 informer。
		informerFactory := kletHost.GetInformerFactory()
		informerFactory.Start(stopCh)
		informerFactory.WaitForCacheSync(stopCh)
	}
}

// NewPersistentVolumeRecyclerPodTemplate 创建回收 Pod 的模板。
// 默认情况下，回收 Pod 只需对卷执行 "rm -rf" 并检查其是否为空。
// 模板中的大多数属性对多数插件实现都是合适的，以下属性可通过配置按插件覆盖：
//
//  1. pod.Spec.Volumes[0].VolumeSource 必须覆盖。没有有效 VolumeSource 的
//     回收实现会失败。
//  2. pod.GenerateName 有助于按名称区分回收 Pod，建议设置，默认为 "pv-recycler-"。
//  3. pod.Spec.ActiveDeadlineSeconds 为回收 Pod 设置失败前的最大超时，
//     建议设置，默认为 60 秒。
//
// 可参考 HostPath 和 NFS 的回收实现。
func NewPersistentVolumeRecyclerPodTemplate() *v1.Pod {
	timeout := int64(60)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pv-recycler-",
			Namespace:    metav1.NamespaceDefault,
		},
		Spec: v1.PodSpec{
			ActiveDeadlineSeconds: &timeout,
			RestartPolicy:         v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name: "vol",
					// 重要！所有使用该模板的插件都必须覆盖
					// pod.Spec.Volumes[0].VolumeSource；没有有效 VolumeSource 的
					// 回收实现会失败。
					VolumeSource: v1.VolumeSource{},
				},
			},
			Containers: []v1.Container{
				{
					Name:    "pv-recycler",
					Image:   "registry.k8s.io/build-image/debian-base:bookworm-v1.0.6",
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", "test -e /scrub && find /scrub -mindepth 1 -delete && test -z \"$(ls -A /scrub)\" || exit 1"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "vol",
							MountPath: "/scrub",
						},
					},
				},
			},
		},
	}
	return pod
}

// ValidateRecyclerPodTemplate 检查回收 Pod 模板的有效性。
// 检查项包括：回收 Pod 模板中至少定义了一个卷。
// 通过检查时返回 nil，否则返回错误。
func ValidateRecyclerPodTemplate(pod *v1.Pod) error {
	if len(pod.Spec.Volumes) < 1 {
		return fmt.Errorf("does not contain any volume(s)")
	}
	return nil
}

// dummyPluginProber 是探测器的空实现，在未配置探测器或探测器初始化失败时使用。
type dummyPluginProber struct{}

// Init 和 Probe 均为空实现：不执行初始化，也不返回任何探测事件。
func (*dummyPluginProber) Init() error                  { return nil }
func (*dummyPluginProber) Probe() ([]ProbeEvent, error) { return nil, nil }
