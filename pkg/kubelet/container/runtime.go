/*
Copyright 2015 The Kubernetes Authors.

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

//go:generate mockery
package container

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/flowcontrol"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/volume"
)

// Version 接口用于消费运行时版本信息：支持比较与字符串格式化。
type Version interface {
	// Compare 比较两个运行时版本：小于对方返回 -1，
	// 大于对方返回 1，
	// 相等返回 0。
	Compare(other string) (int, error)
	// String 返回版本的字符串表示。
	String() string
}

// ImageSpec 是镜像的内部表示。目前它包装容器 Image 字段的值，
// 未来还会包含不同镜像类型的更多详细信息。
type ImageSpec struct {
	// 镜像 ID。
	Image string
	// 拉取该镜像所使用的 runtime handler。
	RuntimeHandler string
	// 镜像的注解。
	// 拉取镜像时传给 CRI，列举镜像时由 CRI 返回。
	Annotations []Annotation
}

// ImageStats 包含当前可用镜像的统计信息。
type ImageStats struct {
	// 现有镜像占用的存储总量。
	TotalStorageBytes uint64
}

// Runtime 接口定义容器运行时需要实现的能力。
// 该接口的实现必须保证线程安全。
type Runtime interface {
	// Type 返回容器运行时类型。
	Type() string

	// Version 返回容器运行时的版本信息。
	Version(ctx context.Context) (Version, error)

	// APIVersion 返回缓存的容器运行时 API 版本信息；实现应定期刷新缓存。
	// 该版本可能与运行时引擎自身的版本不同。
	// TODO(random-liu): We should fold this into Version()
	APIVersion() (Version, error)
	// Status 返回运行时状态；仅当状态查询本身失败时才返回 error。
	Status(ctx context.Context) (*RuntimeStatus, error)
	// GetPods 返回按 Pod 分组的容器列表；all 为 true 时包含已退出、已死亡的容器
	// （供垃圾回收使用）。
	GetPods(ctx context.Context, all bool) ([]*Pod, error)
	// GetPod 获取指定 Pod 当前的 sandbox 与容器。
	// 找不到任何 sandbox 时返回 ErrPodNotFound。
	GetPod(ctx context.Context, podUID types.UID) (*Pod, error)
	// GarbageCollect 按给定 GC 策略清理已死亡的容器。
	// allSourcesReady 为 false 表示 kubelet 尚未拿到所有来源（apiserver、http、
	// 文件等）的完整 Pod 列表；此时 GC 应避免激进策略，
	// 例如不要删除无法识别 Pod 的全部容器。
	// evictNonDeletedPods 为 true 时，终止但尚未删除的 Pod 的容器与 sandbox
	// 也会被清理；否则只清理已删除的 Pod。
	// TODO: Revisit this method and make it cleaner.
	GarbageCollect(ctx context.Context, gcPolicy GCPolicy, allSourcesReady bool, evictNonDeletedPods bool) error
	// SyncPod 把运行中的 Pod 同步到期望状态。
	SyncPod(ctx context.Context, pod *v1.Pod, podStatus *PodStatus, pullSecrets []v1.Secret, backOff *flowcontrol.Backoff, restartAllContainers bool) PodSyncResult
	// KillPod 终止 Pod 的所有容器；pod 可以为 nil，runningPod 不可以。
	// TODO(random-liu): Return PodSyncResult in KillPod.
	// gracePeriodOverride 非空时允许调用方覆盖 Pod 默认的优雅终止期。
	// kubelet 中只有硬杀路径可以传入 gracePeriodOverride，避免破坏用户数据：
	// 例如硬驱逐时发 SIGKILL，或软驱逐时使用最大宽限期。
	KillPod(ctx context.Context, pod *v1.Pod, runningPod Pod, gracePeriodOverride *int64) error
	// GetPodStatus 获取 Pod 状态，包括运行时可见的全部容器信息。
	GetPodStatus(ctx context.Context, pod *Pod) (*PodStatus, error)
	// TODO(vmarmol): Unify pod and containerID args.
	// GetContainerLogs 返回指定容器的日志。默认返回日志快照；
	// follow 为 true 时流式读取；为 false 时按 tailLines 返回末尾若干行
	// （如 "100" 或 "all"）。
	GetContainerLogs(ctx context.Context, pod *v1.Pod, containerID ContainerID, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) (err error)
	// DeleteContainer 删除容器；容器仍在运行时返回错误。
	DeleteContainer(ctx context.Context, containerID ContainerID) error
	// ImageService 提供与镜像相关的方法。
	ImageService
	// UpdatePodCIDR 把新的 podCIDR 下发给运行时。
	// 该方法只是把更新后的 runtimeConfig 透传给运行时 shim。
	UpdatePodCIDR(ctx context.Context, podCIDR string) error
	// CheckpointContainer 通知运行时对容器做 checkpoint，
	// 并把归档写入 checkpoint 目录。
	CheckpointContainer(ctx context.Context, options *runtimeapi.CheckpointContainerRequest) error
	// GeneratePodStatus 根据 CRI 容器事件生成 Pod 状态。
	GeneratePodStatus(event *runtimeapi.ContainerEventResponse) *PodStatus
	// ListMetricDescriptors 获取 ListPodSandboxMetrics 将要返回的指标描述符。
	// 该列表在启动后应保持静态：增删指标描述符时必须让客户端与服务端一起重启，
	// 否则不要改动它。
	// 换言之，若 ListPodSandboxMetrics 返回了初始 ListMetricDescriptors 中
	// 未描述的指标名，该指标将不会被广播。
	ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error)
	// ListPodSandboxMetrics 获取所有 Pod sandbox 的指标。
	ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error)
	// GetContainerStatus 返回指定容器的状态。
	GetContainerStatus(ctx context.Context, podUID types.UID, id ContainerID) (*Status, error)
	// GetContainerSwapBehavior 报告容器是否可被换出，
	// 用于决定是否为容器处理 InPlacePodVerticalScaling。
	GetContainerSwapBehavior(pod *v1.Pod, container *v1.Container) kubelettypes.SwapBehavior
	// IsPodResizeInProgress 判断 Pod 是否正在调整资源
	// （已分配资源与实际生效资源不一致）。
	IsPodResizeInProgress(allocatedPod *v1.Pod, podStatus *PodStatus) bool
	// UpdateActuatedPodLevelResources 更新 actuatedState 中的 Pod 级资源。
	UpdateActuatedPodLevelResources(logger klog.Logger, actuatedPod *v1.Pod) error
	// InitializeActuatedPod 为给定 Pod 初始化实际生效的容器资源与 emptyDir 大小限制，
	// 已设置的值保持不变。
	InitializeActuatedPod(logger klog.Logger, allocatedPod *v1.Pod)
}

var (
	// ErrPodNotFound 在 GetPod 找不到任何 sandbox 时返回。
	ErrPodNotFound = errors.New("pod sandboxes not found")
)

// StreamingRuntime 由自行处理流式调用（exec/attach/port-forward）的运行时实现；
// 此时 kubelet 应把请求重定向到该运行时服务端。
type StreamingRuntime interface {
	// GetExec 返回用于 exec 的流式 URL。
	GetExec(ctx context.Context, id ContainerID, cmd []string, stdin, stdout, stderr, tty bool) (*url.URL, error)
	// GetAttach 返回用于 attach 的流式 URL。
	GetAttach(ctx context.Context, id ContainerID, stdin, stdout, stderr, tty bool) (*url.URL, error)
	// GetPortForward 返回用于 port-forward 的流式 URL。
	GetPortForward(ctx context.Context, podName, podNamespace string, podUID types.UID, ports []int32) (*url.URL, error)
}

// ImageService 是与镜像服务交互的接口。
type ImageService interface {
	// PullImage 使用给定凭证把镜像从网络拉取到本地存储；
	// 返回已拉取镜像的引用（digest 或 ID）以及实际使用的凭证；
	// 凭证为 nil 表示匿名拉取。
	PullImage(ctx context.Context, image ImageSpec, credentials []credentialprovider.TrackedAuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, *credentialprovider.TrackedAuthConfig, error)
	// GetImageRef 获取本地已有镜像的引用（digest 或 ID）；
	// 镜像不在本地时返回 ("", nil)。
	GetImageRef(ctx context.Context, image ImageSpec) (string, error)
	// ListImages 获取本机当前的所有镜像。
	ListImages(ctx context.Context) ([]Image, error)
	// RemoveImage 删除指定镜像。
	RemoveImage(ctx context.Context, image ImageSpec) error
	// ImageStats 返回镜像统计信息。
	ImageStats(ctx context.Context) (*ImageStats, error)
	// ImageFsInfo 返回容器/镜像使用的文件系统列表。
	ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error)
	// GetImageSize 返回镜像大小。
	GetImageSize(ctx context.Context, image ImageSpec) (uint64, error)
}

// Attacher 接口用于 attach 到容器。
type Attacher interface {
	// AttachContainer 把 stdin/stdout/stderr 连接到指定容器，并处理终端尺寸变化。
	AttachContainer(ctx context.Context, id ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) (err error)
}

// CommandRunner 接口用于在容器内执行命令。
type CommandRunner interface {
	// RunInContainer 在容器内同步执行命令并返回输出；
	// 退出码非 0 时返回 k8s.io/utils/exec.ExitError。
	RunInContainer(ctx context.Context, id ContainerID, cmd []string, timeout time.Duration) ([]byte, error)
}

// Pod 是一组容器。
type Pod struct {
	// Pod 的 ID，可用于从 GetPods() 返回的列表中定位具体 Pod。
	ID types.UID
	// Pod 的名称与命名空间，供人阅读。
	Name      string
	Namespace string
	// Pod 的创建时间，单位纳秒。
	CreatedAt uint64
	// 属于该 Pod 的容器列表：可能只含运行中的容器，
	// 也可能混有已死亡的容器（GetPods(true)）。
	Containers []*Container
	// 该 Pod 关联的 sandbox 列表。sandbox 被临时转换为 Container，
	// 以避免对其他组件做大幅改动；该字段仅由 kuberuntime 填充，
	// 并按创建时间从新到旧排序。
	// TODO: use the runtimeApi.PodSandbox type directly.
	Sandboxes []*Container
	// Timestamp 是该 Pod 对象从运行时读取的时间。
	Timestamp time.Time
}

// PodPair 同时包含运行时侧 Pod 与 API 侧 v1.Pod。
type PodPair struct {
	// APIPod 是 API 侧的 v1.Pod。
	APIPod *v1.Pod
	// RunningPod 是运行时侧的 Pod（pkg/kubelet/container/runtime#Pod）。
	RunningPod *Pod
}

// ContainerID 标识一个容器，由运行时类型与实例 ID 组成。
type ContainerID struct {
	// 容器运行时类型，例如 'docker'。
	Type string
	// 容器实例 ID，底层运行时可直接消费；
	// 注意接口仍以整个结构体作为输入。
	ID string
}

// BuildContainerID 根据类型与 ID 构造 ContainerID。
func BuildContainerID(typ, ID string) ContainerID {
	return ContainerID{Type: typ, ID: ID}
}

// ParseContainerID 从 "type://id" 字符串解析 ContainerID，解析失败仅记录日志。
func ParseContainerID(logger klog.Logger, containerID string) ContainerID {
	var id ContainerID
	if err := id.ParseString(containerID); err != nil {
		// 解析失败时保留零值 ContainerID，调用方可用 IsEmpty 判定。
		logger.Error(err, "Parsing containerID failed")
	}
	return id
}

// ParseString 把 "type://id" 字符串解析到 ContainerID。
func (c *ContainerID) ParseString(data string) error {
	// 去掉外层引号后按 "://" 拆分类型与 ID。
	parts := strings.Split(strings.Trim(data, "\""), "://")
	if len(parts) != 2 {
		return fmt.Errorf("invalid container ID: %q", data)
	}
	c.Type, c.ID = parts[0], parts[1]
	return nil
}

// String 返回 "type://id" 形式的字符串。
func (c *ContainerID) String() string {
	return fmt.Sprintf("%s://%s", c.Type, c.ID)
}

// IsEmpty 判断 ContainerID 是否为零值。
func (c *ContainerID) IsEmpty() bool {
	return *c == ContainerID{}
}

// MarshalJSON 把 ContainerID 序列化为 JSON 字符串。
func (c *ContainerID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", c.String())), nil
}

// UnmarshalJSON 从 JSON 字节数组解析 ContainerID。
func (c *ContainerID) UnmarshalJSON(data []byte) error {
	return c.ParseString(string(data))
}

// State 表示容器状态。
type State string

const (
	// ContainerStateCreated 表示容器已创建（如 docker create）但尚未启动。
	ContainerStateCreated State = "created"
	// ContainerStateRunning 表示容器正在运行。
	ContainerStateRunning State = "running"
	// ContainerStateExited 表示容器已运行结束（其他语境称 stopped）。
	ContainerStateExited State = "exited"
	// ContainerStateUnknown 覆盖当前不关心的状态（restarting、paused、dead 等）。
	ContainerStateUnknown State = "unknown"
)

// ContainerReasonStatusUnknown 表示无法确定容器状态。
const ContainerReasonStatusUnknown string = "ContainerStatusUnknown"

// Container 提供容器的运行时信息，如 ID、hash、状态等。
type Container struct {
	// 容器 ID，容器运行时用它定位容器。
	ID ContainerID
	// 容器名称，应与 v1.Container 中的名称一致。
	Name string
	// 容器镜像名（含 tag），期望格式为 "NAME:TAG"。
	Image string
	// 容器使用的镜像 ID。
	ImageID string
	// 容器所用镜像的 digest 引用。
	ImageRef string
	// 拉取该镜像时使用的 runtime handler（如有）。
	ImageRuntimeHandler string
	// 容器 hash，用于比较；非 kubelet 管理的容器可为空。
	Hash uint64
	// 容器状态。
	State State
	// 容器所属 sandbox 的 ID。
	PodSandboxID string
	// 容器创建时间，单位纳秒。
	CreatedAt int64
}

// PodStatus 表示 Pod 及其容器的状态，
// v1.PodStatus 可结合 PodStatus 与 v1.Pod 推导得到。
type PodStatus struct {
	// Pod ID。
	ID types.UID
	// Pod 名称。
	Name string
	// Pod 命名空间。
	Namespace string
	// 分配给该 Pod 的全部 IP。
	IPs []string
	// Pod 中容器的状态。
	ContainerStatuses []*Status
	// Pod 中活跃 sandbox 的容器状态。
	ActiveContainerStatuses []*Status
	// Pod sandbox 的状态。
	// 目前只有 kuberuntime 填充，其他运行时可能保持为 nil。
	SandboxStatuses []*runtimeapi.PodSandboxStatus
	// 记录容器与 Pod 状态的时间戳。
	TimeStamp time.Time
}

// ContainerResources 表示分配给运行中容器的资源。
type ContainerResources struct {
	// 为容器预留的 CPU 容量。
	CPURequest *resource.Quantity
	// 施加在容器上的 CPU 上限。
	CPULimit *resource.Quantity
	// 为容器预留的内存容量。
	MemoryRequest *resource.Quantity
	// 施加在容器上的内存上限。
	MemoryLimit *resource.Quantity
}

// Status 表示容器状态。
//
// Status 不包含 VolumeMap，因为 CRI API 不知道卷名称。
type Status struct {
	// 容器 ID。
	ID ContainerID
	// 容器名称。
	Name string
	// 容器状态。
	State State
	// 容器创建时间。
	CreatedAt time.Time
	// 容器启动时间。
	StartedAt time.Time
	// 容器结束时间。
	FinishedAt time.Time
	// 容器退出码。
	ExitCode int
	// 镜像名（含 tag），期望格式为 "NAME:TAG"。
	Image string
	// 镜像 ID。
	ImageID string
	// 容器所用镜像的 digest 引用。
	ImageRef string
	// 拉取该镜像时使用的 runtime handler（如有）。
	ImageRuntimeHandler string
	// 容器 hash，用于比较。
	Hash uint64
	// 容器重启次数。
	RestartCount int
	// 解释容器处于该状态的原因。
	Reason string
	// 容器退出前写入的消息（存放在 TerminationMessagePath）。
	Message string
	// 该容器的 CPU 与内存资源。
	Resources *ContainerResources
	// 容器首个进程的用户身份信息。
	User *ContainerUser
	// 容器的卷挂载列表。
	Mounts []Mount
	// StopSignal 表示容器实际生效的停止信号。
	StopSignal *v1.Signal
}

// ContainerUser 表示用户身份信息。
type ContainerUser struct {
	// Linux 保存 Linux 容器首个进程的用户身份信息；
	// spec.os.name 为 windows 时不能设置该字段。
	Linux *LinuxContainerUser

	// Windows 保存 Windows 容器首个进程的用户身份信息，
	// 目前仅为未来预留。
	// Windows *WindowsContainerUser
}

// LinuxContainerUser 表示 Linux 容器的用户身份信息。
type LinuxContainerUser struct {
	// UID 是容器首个进程的主 uid。
	UID int64
	// GID 是容器首个进程的主 gid。
	GID int64
	// SupplementalGroups 是附加到容器首个进程的附加组。
	SupplementalGroups []int64
}

// FindContainerStatusByName 返回 Pod 状态中指定名称的容器状态。
// 存在同名状态时返回第一个匹配项。
func (podStatus *PodStatus) FindContainerStatusByName(containerName string) *Status {
	for _, containerStatus := range podStatus.ContainerStatuses {
		if containerStatus.Name == containerName {
			return containerStatus
		}
	}
	return nil
}

// FindActiveContainerStatusByName 返回活跃容器状态中指定名称的项。
// 存在同名状态时返回第一个匹配项。
func (podStatus *PodStatus) FindActiveContainerStatusByName(containerName string) *Status {
	for _, containerStatus := range podStatus.ActiveContainerStatuses {
		if containerStatus.Name == containerName {
			return containerStatus
		}
	}
	return nil
}

// GetRunningContainerStatuses 返回 Pod 中所有运行中容器的状态。
func (podStatus *PodStatus) GetRunningContainerStatuses() []*Status {
	runningContainerStatuses := []*Status{}
	for _, containerStatus := range podStatus.ContainerStatuses {
		if containerStatus.State == ContainerStateRunning {
			runningContainerStatuses = append(runningContainerStatuses, containerStatus)
		}
	}
	return runningContainerStatuses
}

// Image 包含容器镜像的基本信息。
type Image struct {
	// 镜像 ID。
	ID string
	// 该镜像的其他已知名称。
	RepoTags []string
	// 该镜像的 digest 列表。
	RepoDigests []string
	// 镜像大小（字节）。
	Size int64
	// 镜像的 ImageSpec，包含注解。
	Spec ImageSpec
	// Pinned 为 true 时阻止该镜像被垃圾回收。
	Pinned bool
}

// EnvVar 表示环境变量。
type EnvVar struct {
	Name  string
	Value string // TODO: switch to []byte
}

// Annotation 表示一条注解。
type Annotation struct {
	Name  string
	Value string
}

// Mount 表示一次卷挂载。
type Mount struct {
	// 卷挂载名称。
	// TODO(yifan): Remove this field, as this is not representing the unique name of the mount,
	// but the volume name only.
	Name string
	// 容器内的挂载路径。
	ContainerPath string
	// 宿主机上的挂载路径。
	HostPath string
	// 是否只读挂载。
	ReadOnly bool
	// 是否递归只读挂载；
	// ReadOnly 为 false 时该字段不得为 true。
	RecursiveReadOnly bool
	// 是否需要 SELinux 重标记。
	SELinuxRelabel bool
	// 请求的挂载传播模式。
	Propagation runtimeapi.MountPropagation
	// 需要挂载 OCI 镜像卷（按 image ID/digest）时设置该字段（特殊场景）。
	Image *runtimeapi.ImageSpec
	// 需要挂载镜像卷子路径时设置 ImageSubPath，
	// 仅在 Image 已设置时有效。
	ImageSubPath string
	// BindMountOptions 是挂载时附加的 bind 选项（noexec、nodev、nosuid），
	// 用于限制卷在容器内的行为。
	BindMountOptions []string
}

// ImageVolumes 按卷名索引镜像规格。
type ImageVolumes = map[string]*runtimeapi.ImageSpec

// PortMapping 包含端口映射信息。
type PortMapping struct {
	// 端口映射的协议。
	Protocol v1.Protocol
	// 容器内端口号。
	ContainerPort int
	// 宿主机端口号。
	HostPort int
	// 宿主机 IP。
	HostIP string
}

// DeviceInfo 包含设备信息。
type DeviceInfo struct {
	// 宿主机上用于映射的设备路径。
	PathOnHost string
	// 映射到容器内的设备路径。
	PathInContainer string
	// cgroup 权限。
	Permissions string
}

// CDIDevice 包含 CDI 设备信息。
type CDIDevice struct {
	// Name 是按 CDI 规范完全限定的设备名，规范见下方链接：
	// https://github.com/cncf-tags/container-device-interface/blob/e66544063aa7760c4ea6330ce9e6c757f8e61df2/README.md?plain=1#L9-L15
	Name string
}

// RunContainerOptions 指定运行容器所需的选项。
type RunContainerOptions struct {
	// 环境变量列表。
	Envs []EnvVar
	// 容器的挂载列表。
	Mounts []Mount
	// 映射进容器的宿主机设备。
	Devices []DeviceInfo
	// 容器的 CDI 设备。
	CDIDevices []CDIDevice
	// 容器的注解。
	// 这些注解由其他组件（而非用户）生成；
	// 目前只有设备插件会填充它们。
	Annotations []Annotation
	// 容器指定了 TerminationMessagePath 时，用该目录创建并挂载日志文件到
	// container.TerminationMessagePath。
	PodContainerDir string
	// 容器 rootfs 是否只读。
	ReadOnly bool
}

// VolumeInfo 包含卷信息。
type VolumeInfo struct {
	// Mounter 是该卷的挂载器。
	Mounter volume.Mounter
	// BlockVolumeMapper 是块设备的映射器。
	BlockVolumeMapper volume.BlockVolumeMapper
	// SELinuxLabeled 表示该卷是否已应用 Pod 的 SELinux 标签。
	SELinuxLabeled bool
	// 卷是否以只读权限挂载；
	// 该值来自 volume.spec。
	ReadOnly bool
	// 内层卷规格名：使用 PV 时为 PV 名，
	// 否则与外层卷规格名相同。
	InnerVolumeSpecName string
}

// VolumeMap 是卷名到卷信息的映射。
type VolumeMap map[string]VolumeInfo

// RuntimeConditionType 表示运行时必需条件的类型。
type RuntimeConditionType string

const (
	// RuntimeReady 表示运行时已就绪，可以接受普通容器。
	RuntimeReady RuntimeConditionType = "RuntimeReady"
	// NetworkReady 表示运行时网络已就绪，可以接受需要网络的容器。
	NetworkReady RuntimeConditionType = "NetworkReady"
)

// RuntimeStatus 包含运行时状态。
type RuntimeStatus struct {
	// 当前观测到的运行时条件。
	Conditions []RuntimeCondition
	// 当前可用的 runtime handler 列表。
	Handlers []RuntimeHandler
	// 运行时实现的功能集合。
	Features *RuntimeFeatures
}

// GetRuntimeCondition 从运行时状态中获取指定类型的条件。
func (r *RuntimeStatus) GetRuntimeCondition(t RuntimeConditionType) *RuntimeCondition {
	for i := range r.Conditions {
		c := &r.Conditions[i]
		if c.Type == t {
			return c
		}
	}
	return nil
}

// String 把运行时状态格式化为人类可读字符串。
func (r *RuntimeStatus) String() string {
	var ss []string
	var sh []string
	for _, c := range r.Conditions {
		ss = append(ss, c.String())
	}
	for _, h := range r.Handlers {
		sh = append(sh, h.String())
	}
	return fmt.Sprintf("Runtime Conditions: %s; Handlers: %s, Features: %s", strings.Join(ss, ", "), strings.Join(sh, ", "), r.Features.String())
}

// RuntimeHandler 包含 runtime handler 的能力信息。
type RuntimeHandler struct {
	// handler 名称。
	Name string
	// SupportsRecursiveReadOnlyMounts 表示该 handler 是否支持
	// 递归只读挂载。
	SupportsRecursiveReadOnlyMounts bool
	// SupportsUserNamespaces 表示该 handler 是否支持
	// 用户命名空间。
	SupportsUserNamespaces bool
}

// String 把 runtime handler 格式化为人类可读字符串。
func (h *RuntimeHandler) String() string {
	return fmt.Sprintf("Name=%s SupportsRecursiveReadOnlyMounts: %v SupportsUserNamespaces: %v",
		h.Name, h.SupportsRecursiveReadOnlyMounts, h.SupportsUserNamespaces)
}

// RuntimeCondition 包含运行时条件信息。
type RuntimeCondition struct {
	// 运行时条件类型。
	Type RuntimeConditionType
	// 条件状态，true 或 false。
	Status bool
	// 条件最近一次翻转的简要原因。
	Reason string
	// 描述最近一次翻转细节的人类可读消息。
	Message string
}

// String 把运行时条件格式化为人类可读字符串。
func (c *RuntimeCondition) String() string {
	return fmt.Sprintf("%s=%t reason:%s message:%s", c.Type, c.Status, c.Reason, c.Message)
}

// RuntimeFeatures 包含运行时实现的功能集合。
type RuntimeFeatures struct {
	SupplementalGroupsPolicy  bool
	UserNamespacesHostNetwork bool
	MountOptions              bool
}

// String 把运行时特性格式化为人类可读字符串；f 为 nil 时返回 "nil"。
func (f *RuntimeFeatures) String() string {
	// 运行时可能尚未上报特性集合；nil 时返回固定字符串，避免调用方空指针。
	if f == nil {
		return "nil"
	}
	return fmt.Sprintf("SupplementalGroupsPolicy: %v UserNamespacesHostNetwork: %v MountOptions: %v", f.SupplementalGroupsPolicy, f.UserNamespacesHostNetwork, f.MountOptions)
}

// Pods 是 Pod 列表。
type Pods []*Pod

// FindPodByID 按 UID 查找 Pod；
// 找不到时返回空 Pod。
func (p Pods) FindPodByID(podUID types.UID) Pod {
	for i := range p {
		if p[i].ID == podUID {
			return *p[i]
		}
	}
	return Pod{}
}

// FindPodByFullName 按全名查找 Pod；
// 找不到时返回空 Pod。
func (p Pods) FindPodByFullName(podFullName string) Pod {
	for i := range p {
		if BuildPodFullName(p[i].Name, p[i].Namespace) == podFullName {
			return *p[i]
		}
	}
	return Pod{}
}

// FindPod 在 FindPodByID 与 FindPodByFullName 之间做选择：
// 全名非空时按全名查找，否则按 UID 查找；
// 找不到时返回空 Pod。
func (p Pods) FindPod(podFullName string, podUID types.UID) Pod {
	// 全名包含命名空间，优先使用；仅在全名为空时退回 UID 查找。
	if len(podFullName) > 0 {
		return p.FindPodByFullName(podFullName)
	}
	return p.FindPodByID(podUID)
}

// FindContainerByName 返回 Pod 中指定名称的容器；
// 存在同名容器时返回第一个匹配项。
func (p *Pod) FindContainerByName(containerName string) *Container {
	for _, c := range p.Containers {
		if c.Name == containerName {
			return c
		}
	}
	return nil
}

// FindContainerByID 返回 Pod 中指定 ContainerID 的容器。
func (p *Pod) FindContainerByID(id ContainerID) *Container {
	for _, c := range p.Containers {
		if c.ID == id {
			return c
		}
	}
	return nil
}

// FindSandboxByID 返回 Pod 中指定 ContainerID 的 sandbox。
func (p *Pod) FindSandboxByID(id ContainerID) *Container {
	for _, c := range p.Sandboxes {
		if c.ID == id {
			return c
		}
	}
	return nil
}

// ToAPIPod 把 Pod 转换为 v1.Pod；
// v1.Pod 中没有对应来源的字段不会被填充。
func (p *Pod) ToAPIPod() *v1.Pod {
	var pod v1.Pod
	pod.UID = p.ID
	pod.Name = p.Name
	pod.Namespace = p.Namespace

	// 只填充名称、镜像等最小子集，其余字段保持零值。
	for _, c := range p.Containers {
		var container v1.Container
		container.Name = c.Name
		container.Image = c.Image
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}
	return &pod
}

// IsEmpty 判断 Pod 是否为空。
func (p *Pod) IsEmpty() bool {
	return reflect.DeepEqual(p, &Pod{})
}

// GetPodFullName 返回能唯一标识 Pod 的名称。
func GetPodFullName(pod *v1.Pod) string {
	// 用下划线做分隔符：Pod 名（DNS 子域格式）不允许下划线，
	// 而容器名格式允许。
	return pod.Name + "_" + pod.Namespace
}

// BuildPodFullName 由名称与命名空间构造 Pod 全名。
func BuildPodFullName(name, namespace string) string {
	return name + "_" + namespace
}

// ParsePodFullName 解析 Pod 全名，返回名称与命名空间。
func ParsePodFullName(podFullName string) (string, string, error) {
	parts := strings.Split(podFullName, "_")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("failed to parse the pod full name %q", podFullName)
	}
	return parts[0], parts[1], nil
}

// Option 是 Runtime 的函数式选项类型，
// 用于完全可选的配置项。
type Option func(Runtime)

// SortContainerStatusesByCreationTime 按创建时间排序容器状态。
type SortContainerStatusesByCreationTime []*Status

// Len 实现 sort.Interface，返回元素个数。
func (s SortContainerStatusesByCreationTime) Len() int { return len(s) }

// Swap 实现 sort.Interface，交换两个元素。
func (s SortContainerStatusesByCreationTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 实现 sort.Interface，按创建时间升序比较。
func (s SortContainerStatusesByCreationTime) Less(i, j int) bool {
	return s[i].CreatedAt.Before(s[j].CreatedAt)
}

const (
	// MaxPodTerminationMessageLogLength 是单个 Pod 所有容器可写入终止消息的总字节上限，
	// 超出时各容器的输出会被均摊截断。
	MaxPodTerminationMessageLogLength = 1024 * 12
	// MaxContainerTerminationMessageLength 是单个容器可写入终止消息路径的字节上限，
	// 超出部分会被截断。
	MaxContainerTerminationMessageLength = 1024 * 4
	// MaxContainerTerminationMessageLogLength 是从日志读取终止消息时，
	// 单个容器允许的最大字节数。
	MaxContainerTerminationMessageLogLength = 1024 * 2
	// MaxContainerTerminationMessageLogLines 是终止消息可包含的
	// 日志末尾行数上限。
	MaxContainerTerminationMessageLogLines = 80
)
