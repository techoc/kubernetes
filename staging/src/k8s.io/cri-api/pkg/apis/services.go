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

package cri

import (
	"context"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// RuntimeVersioner 定义了获取容器运行时名称、版本以及 API 版本的方法。
// 该接口通常作为 RuntimeService 的一部分被嵌入使用。
type RuntimeVersioner interface {
	// Version 返回运行时的名称、版本以及其实现的 CRI API 版本。
	// apiVersion 是调用方期望的 API 版本；若运行时不支持该版本，应返回错误。
	Version(ctx context.Context, apiVersion string) (*runtimeapi.VersionResponse, error)
}

// ContainerManager 定义了操作容器运行时所管理容器的方法，所有方法都是线程安全的。
// 覆盖容器从创建、启动、停止、删除，到状态查询、资源更新、命令执行和日志处理的完整生命周期。
type ContainerManager interface {
	// CreateContainer 在指定的 Pod 沙箱（podSandboxID）中创建一个新容器。
	// config 描述容器本身的配置，sandboxConfig 描述所属沙箱的配置；成功时返回容器 ID。
	CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error)
	// StartContainer 启动指定 ID 的容器，使容器进程真正运行起来。
	StartContainer(ctx context.Context, containerID string) error
	// StopContainer 停止一个正在运行的容器，timeout 表示优雅停止的超时时间（秒）。
	// 超时后运行时应强制终止容器。
	StopContainer(ctx context.Context, containerID string, timeout int64) error
	// RemoveContainer 删除指定 ID 的容器，回收其资源。
	RemoveContainer(ctx context.Context, containerID string) error
	// ListContainers 按 filter 中指定的条件（如沙箱 ID、状态、标签）列出容器。
	// filter 为 nil 时不做过滤，返回全部容器。
	ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error)
	// ContainerStatus 返回指定容器的详细状态。
	// verbose 为 true 时，运行时可返回额外信息（如镜像、PID、退出原因等）。
	ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error)
	// UpdateContainerResources 同步更新容器的资源配置（如 CPU、内存限额）。
	// 该更新必须具有事务性：要么全部生效，要么完全不生效；部分成功时应返回错误。
	UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error
	// ExecSync 在容器内同步执行一条命令并等待其结束，返回标准输出和标准错误输出。
	// timeout 表示等待命令完成的超时时间；若命令以非零退出码结束，也视为错误返回。
	ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error)
	// Exec 为"在容器内执行命令"准备一个流式服务端点（streaming endpoint），并返回其访问地址。
	// 与 ExecSync 不同，它不阻塞等待命令结束，而是交给流式服务器处理（如 kubectl exec）。
	Exec(ctx context.Context, request *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error)
	// Attach 为"连接到正在运行的容器"准备流式服务端点并返回地址，常用于 kubectl attach 场景。
	Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error)
	// ReopenContainerLog 要求运行时重新打开容器的 stdout/stderr 日志文件，
	// 典型场景是日志轮转后需要切换到新的日志文件。
	// 注意约束：如果本方法返回错误，运行时绝不能创建新的日志文件，
	// 否则 kubelet 会误以为切换成功而丢失日志。
	ReopenContainerLog(ctx context.Context, ContainerID string) error
	// CheckpointContainer 为指定容器创建检查点（checkpoint），保存其运行状态以便后续恢复。
	CheckpointContainer(ctx context.Context, options *runtimeapi.CheckpointContainerRequest) error
	// GetContainerEvents 从 CRI 运行时持续接收容器事件（如 OOM、容器退出等）。
	// 这是一个流式 RPC：事件写入 containerEventsCh 通道；
	// connectionEstablishedCallback 会在流连接建立后被调用，传入底层流客户端，
	// 以便调用方在连接断开后能及时感知并重新建立连接。
	GetContainerEvents(ctx context.Context, containerEventsCh chan *runtimeapi.ContainerEventResponse, connectionEstablishedCallback func(runtimeapi.RuntimeService_GetContainerEventsClient)) error
}

// PodSandboxManager 定义了操作 Pod 沙箱（PodSandbox）的方法，所有方法都是线程安全的。
// PodSandbox 是 Pod 级别的资源隔离单元，为其中的容器提供共享的 network namespace 等环境。
type PodSandboxManager interface {
	// RunPodSandbox 创建并启动一个 Pod 级别的沙箱，成功时返回沙箱 ID。
	// runtimeHandler 用于选择运行时处理器（如不同的 OCI 运行时类）。
	// 运行时必须保证方法返回时沙箱已处于 ready（就绪）状态。
	RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error)
	// StopPodSandbox 停止指定的沙箱。若沙箱中仍有运行中的容器，运行时应先强制终止这些容器。
	// 该操作应释放沙箱占用的网络等资源，但保留沙箱对象本身以便后续删除或查询状态。
	StopPodSandbox(ctx context.Context, podSandboxID string) error
	// RemovePodSandbox 删除指定的沙箱。若沙箱中仍有运行中的容器，应一并强制删除。
	// 与 StopPodSandbox 的区别是：Stop 只停止，Remove 会彻底清理沙箱及其资源。
	RemovePodSandbox(ctx context.Context, podSandboxID string) error
	// PodSandboxStatus 返回指定沙箱的状态信息。
	// verbose 为 true 时，可返回额外的详细信息（如网络命名空间路径、标签等）。
	PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error)
	// ListPodSandbox 按 filter 条件（如 ID、状态、标签）列出沙箱。
	// filter 为 nil 时返回全部沙箱。
	ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error)
	// PortForward 为"把宿主机端口转发到 Pod 沙箱"准备流式服务端点，并返回其地址，
	// 典型场景是 kubectl port-forward。
	PortForward(ctx context.Context, request *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error)
	// UpdatePodSandboxResources 同步更新沙箱的 Pod 级别资源配置。
	// 调用时序很重要：kubelet 会先重新配置 Pod 级别的 cgroup，然后再调用本方法。
	// 该请求按"尽力而为"（best effort）处理：即使失败也不会阻塞 kubelet 继续完成 resize 流程。
	UpdatePodSandboxResources(ctx context.Context, request *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error)
	// CheckpointPod 为指定的 Pod 沙箱创建检查点，保存其运行状态。
	CheckpointPod(ctx context.Context, options *runtimeapi.CheckpointPodRequest) error
	// RestorePod 从之前保存的检查点恢复一个 Pod 沙箱，返回恢复结果。
	RestorePod(ctx context.Context, options *runtimeapi.RestorePodRequest) (*runtimeapi.RestorePodResponse, error)
}

// ContainerStatsManager 定义了获取容器和 Pod 沙箱统计信息（如 CPU、内存、网络使用率）的方法。
type ContainerStatsManager interface {
	// ContainerStats 返回单个容器的统计信息。若容器不存在，调用应返回错误。
	ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error)
	// ListContainerStats 按 filter 条件列出所有运行中容器的统计信息。
	ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error)
	// PodSandboxStats 返回单个 Pod 沙箱的统计信息。若沙箱不存在，调用应返回错误。
	PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error)
	// ListPodSandboxStats 按 filter 条件列出所有运行中 Pod 沙箱的统计信息。
	ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error)
	// ListMetricDescriptors 返回指标描述符（名称、帮助信息、类型等），
	// 用于解释 ListPodSandboxMetrics 返回的指标值。
	ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error)
	// ListPodSandboxMetrics 返回所有运行中 Pod 沙箱的自定义指标数据。
	ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error)
}

// RuntimeService 是容器运行时必须实现的顶层接口，由以下四个子接口组合而成：
// RuntimeVersioner（版本）、ContainerManager（容器）、PodSandboxManager（沙箱）、ContainerStatsManager（统计）。
// 所有方法都应当是线程安全的，因为 kubelet 会从多个 goroutine 并发调用。
type RuntimeService interface {
	RuntimeVersioner
	ContainerManager
	PodSandboxManager
	ContainerStatsManager

	// UpdateRuntimeConfig 更新运行时的全局配置，例如网络配置。
	// runtimeConfig 中为 nil 的字段表示不做修改。
	UpdateRuntimeConfig(ctx context.Context, runtimeConfig *runtimeapi.RuntimeConfig) error
	// Status 返回运行时的整体状态，包括各项运行条件（如 RuntimeReady、NetworkReady）。
	// kubelet 用它来判断节点是否就绪。
	Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error)
	// RuntimeConfig 返回当前运行时的配置信息，供 kubelet 查询实际生效的配置。
	RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error)
	// Close 关闭客户端内部持有的 gRPC 连接，释放相关资源。
	Close(ctx context.Context) error
}

// ImageManagerService 是容器镜像管理器必须实现的接口，负责镜像的查询、拉取、删除以及镜像文件系统信息上报。
// 所有方法都应当是线程安全的。
type ImageManagerService interface {
	// ListImages 按 filter 条件列出本地已有的镜像；filter 为 nil 时返回全部镜像。
	ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error)
	// ImageStatus 返回指定镜像的状态信息（如是否存在、大小、标签等）。
	// verbose 为 true 时可返回更详细的信息。
	ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error)
	// PullImage 根据镜像规格 image 拉取镜像，auth 提供仓库认证信息，
	// podSandboxConfig 提供将要使用该镜像的 Pod 沙箱上下文（如用于选择拉取凭据）。
	// 成功时返回拉取到的镜像 ID。
	PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error)
	// RemoveImage 从本地删除指定的镜像，释放其占用的磁盘空间。
	RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) error
	// ImageFsInfo 返回用于存储镜像只读层（read-only layers）和容器可写层（writeable layer）的文件系统信息，
	// 例如容量、已用空间、inodes 等，供 kubelet 做磁盘压力判断。
	ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error)
	// Close 关闭客户端内部持有的 gRPC 连接，释放相关资源。
	Close(ctx context.Context) error
}
