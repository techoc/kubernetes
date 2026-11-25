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

// Package kubeletplugin 提供 DRA（动态资源分配）kubelet 插件的辅助实现：
// 它封装 kubelet 插件注册服务与节点 DRA gRPC 服务，并把具体的资源准备/清理
// 工作委托给 DRAPlugin 实现，同时负责 ResourceSlice 发布、文件锁序列化等横切逻辑。
package kubeletplugin

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	metadatav1alpha1 "k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
	metadatav1beta1 "k8s.io/dynamic-resource-allocation/api/metadata/v1beta1"
	draclient "k8s.io/dynamic-resource-allocation/client"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	drapbv1beta1 "k8s.io/kubelet/pkg/apis/dra/v1beta1"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const (
	// KubeletPluginsDir 是插件数据目录（PluginDataDirectoryPath）的默认路径。
	KubeletPluginsDir = "/var/lib/kubelet/plugins"
	// KubeletRegistryDir 是 kubelet 监听插件注册 socket 的目录（RegistrarDirectoryPath）的默认值。
	KubeletRegistryDir = "/var/lib/kubelet/plugins_registry"

	// unixPathMax 是 Linux 上 AF_UNIX socket 路径允许的最大字节长度。
	unixPathMax = 108

	// rollingUpdateUIDHashBytes 是完整 Pod UID 无法放入注册 socket 路径时使用的摘要长度：
	// 对 UID 做 SHA-256，取前 8 字节（64 位）并以 base64 编码，保证同一节点上的滚动更新实例名称唯一。
	rollingUpdateUIDHashBytes = 8

	// rollingUpdateRegistrarSocketHashBytes 是最短候选名称的摘要长度：
	// 对“驱动名 + NUL + Pod UID”做 SHA-256，取前 16 字节（128 位）并以 base64 编码。
	rollingUpdateRegistrarSocketHashBytes = 16
)

// RollingUpdateRegistrarSocketFile 为滚动更新生成注册 socket 的文件名，按可读性优先依次尝试：
//  1. <driver name>-<pod UID>-reg.sock
//  2. <driver name>-<base64(SHA-256(pod UID)[:8])>-reg.sock
//  3. dra-<base64(SHA-256(driver name, NUL, pod UID)[:16])>-reg.sock
//
// 返回第一个拼接后完整路径不超过 unixPathMax 的 basename；若都超长，则返回最短（哈希形式）的候选。
func RollingUpdateRegistrarSocketFile(registryDir, driverName string, podUID types.UID) string {
	uid := string(podUID)
	// 先算出两种摘要：仅 UID 的摘要，以及驱动名与 UID 组合的摘要。
	uidHash := sha256Sum(uid)
	driverUIDHash := sha256Sum(driverName + "\x00" + uid)
	// 候选名称由可读到不可读依次排列，尽量保留驱动名以便排查。
	candidates := []string{
		driverName + "-" + uid + "-reg.sock",
		driverName + "-" + base64.RawURLEncoding.EncodeToString(uidHash[:rollingUpdateUIDHashBytes]) + "-reg.sock",
		"dra-" + base64.RawURLEncoding.EncodeToString(driverUIDHash[:rollingUpdateRegistrarSocketHashBytes]) + "-reg.sock",
	}
	for _, basename := range candidates {
		// 用 path.Join 后完整路径的长度来判断是否超出 AF_UNIX 限制。
		if len(path.Join(registryDir, basename)) <= unixPathMax {
			return basename
		}
	}
	return candidates[len(candidates)-1]
}

// sha256Sum 返回字符串数据的 SHA-256 摘要（32 字节）。
func sha256Sum(data string) [32]byte {
	return sha256.Sum256([]byte(data))
}

// DRAPlugin 是 DRA 驱动需要实现的接口；辅助包通过包装该实现来对外提供
// kubelet 所期望的 gRPC 接口。
type DRAPlugin interface {
	// PrepareResourceClaims 准备该驱动负责且 Pod 需要的全部 Claim 中已分配的资源，对应 NodePrepareResources。
	// 与原始 gRPC 不同，入参是辅助包已取回的完整 Claim 对象，且辅助包已验证所有 Claim 均已分配。
	// 辅助包递增数字计数，把 requestID 与 method 加入上下文 logger，并按 GRPCVerbosity 记录请求/响应；
	// 默认序列化调用，除非驱动通过 Serialize(false) 关闭。返回值按 Claim UID 索引，每个 Claim 恰好一条结果。
	// 总错误非 nil 时全部结果被忽略；否则单个 Claim 的失败放入 PrepareResult.Err，由 kubelet 重试。
	// 驱动必须验证已分配设备未被其他无关 Claim 使用，这是防止重复分配的最后一道防线。
	// 实现必须幂等，因为 kubelet 重启后可能重复调用。CDI spec 可按需创建并在 unprepare 时删除；
	// 运行时也可以缓存 spec，但缓存未命中时必须重新加载。CDI 唯一名称不得复用，Claim UID 适合作为名称来源。
	PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (result map[types.UID]PrepareResult, err error)

	// UnprepareResourceClaims 撤销 PrepareResourceClaims 所做的工作，对应 NodeUnprepareResources。
	// 驱动重启或滚动更新后，prepare 与 unprepare 可能由不同实例处理，因此不能依赖进程内状态。
	// 此时原始 Claim 可能已被删除，只传入 UID/namespace/name；辅助包负责日志与调用序列化。
	// 总错误非 nil 时结果被忽略；否则每个 Claim 都必须有结果条目，值可以为 nil。
	// 实现必须幂等，对未准备的 Claim 调用不应报错。
	UnprepareResourceClaims(ctx context.Context, claims []NamespacedObject) (result map[types.UID]error, err error)

	// HandleError 处理后台任务（例如发布 ResourceSlice）中遇到的错误。推荐调用
	// k8s.io/apimachinery/pkg/util/runtime.HandleErrorWithContext(ctx, err, msg) 记录日志，
	// 并对致命错误退出进程（最好取消主 context 以优雅关闭）；可用 errors.Is(err, ErrRecoverable) 区分。
	// 重试无法修复的错误必须 fatal，例如 resourceslice.DroppedFieldsError 或 apierrors.IsInvalid(err)；
	// 驱动应退出或修正准备发布的切片。该方法是强制的。
	HandleError(ctx context.Context, err error, msg string)

	// WatchHealthStatus 向 kubelet 上报驱动设备的健康状况。先发送一条覆盖所有设备的初始报告，
	// 随后在设备健康变化时发送新报告；即使未变化，也必须在 DeviceHealth.HealthCheckTimeout
	// （默认 30 秒）内重发，否则 kubelet 会把健康状态视为 unknown。报告可以只覆盖部分设备。
	// 发送不得在 ctx 取消后无限阻塞，例如：
	//	select {
	//	case <-ctx.Done():
	//	    return nil
	//	case reports <- report:
	//	}
	// ctx 取消后可被再次调用。不支持健康上报时应尽快返回 ErrHealthNotSupported，
	// 或用 HealthService 选项关闭服务（此时该方法不会被调用）。
	WatchHealthStatus(ctx context.Context, reports chan<- DeviceHealthReport) error
}

// ErrRecoverable 用于把可恢复错误与应当导致进程退出的致命错误区分开，
// 配合 errors.Is(err, ErrRecoverable) 使用。
var ErrRecoverable = errors.New("recoverable error")

// recoverableError 包装一个原始错误，并把它标记为可恢复错误。
type recoverableError struct {
	error
}

// 编译期断言：recoverableError 满足 error 接口。
var _ error = recoverableError{}

// Is 让 errors.Is(err, ErrRecoverable) 对该包装错误返回 true。
// Unwrap 返回被包装的原始错误。
func (err recoverableError) Is(other error) bool { return other == ErrRecoverable }
func (err recoverableError) Unwrap() error       { return err.error }

// PrepareResult 保存单个 ResourceClaim 的准备结果，包含错误信息与要注入的 CDI 设备。
type PrepareResult struct {
	// Err 非 nil 时表示准备该 Claim 出错，此时 Devices 会被忽略，kubelet 后续会重试。
	Err error

	// Devices 保存与 Claim 中特定 request 关联的 CDI 设备 ID，kubelet 会把它们传给
	// 容器运行时；空切片也是合法的。
	Devices []Device
}

// Device 描述 ResourceClaim 中一个 request 对应的 CDI 设备信息。
type Device struct {
	// Requests 列出该设备关联的 request（或子 request）名称；可以只返回 request 名。
	// 值可取自 DeviceRequestAllocationResult.Request。为空表示关联所有 request。
	Requests []string

	// PoolName 标识包含该设备的驱动资源池，不能为空。
	PoolName string

	// DeviceName 标识资源池内的具体设备，不能为空。
	DeviceName string

	// CDIDeviceIDs 列出与该 DRA 设备关联的所有 CDI 设备 ID，格式形如
	// "<vendor ID>/<class>=<unique name>"，可以为空。唯一名称不得在后续 Claim 中重复使用。
	CDIDeviceIDs []string

	// ShareID 标识设备的共享份额，可以为空。
	ShareID *types.UID

	// Metadata 保存要通过 CDI bind-mount 的 JSON 文件暴露给工作负载的设备属性与网络数据。
	// 只有启用 EnableDeviceMetadata 时该字段才生效；设置后立即写入，为 nil 时驱动可在
	// Pod 启动前通过 UpdateRequestMetadata 填充。
	Metadata *DeviceMetadata
}

// DeviceMetadata 保存要通过元数据文件暴露给工作负载的设备属性和网络数据。
// 最佳实践是包含 ResourceSlice 中为该设备发布的全部属性，避免工作负载看到不一致的运行时信息；
// 例如 PCI 总线 ID、mdev UUID、硬件型号与固件版本。也可以补充仅在运行时可知的属性。
// 网络设备应提供 CNI 配置完成后分配的接口名、IP 与硬件地址。
type DeviceMetadata struct {
	// Attributes 保存与 ResourceSlice 中相同格式的设备属性，键遵循 QualifiedName 约定；
	// 每个属性值只能设置 StringValue/IntValue/BoolValue/VersionValue 中的一个。
	// 不带域名的 "model" 等名称属于该驱动域；第三方或通用名称应加前缀，如 "resource.kubernetes.io/pciBusID"。
	Attributes map[string]resourceapi.DeviceAttribute

	// NetworkData 保存接口名、IP 地址、硬件地址等网络信息，通常由网络类 DRA 插件
	// 在 CNI 配置完成后通过 NRI hook 填充。
	NetworkData *resourceapi.NetworkDeviceData
}

// Option 是 Start 的函数式选项类型，每个选项负责修改启动配置 options。
type Option func(o *options) error

// DriverName 设置 DRA 驱动名称，为必填项；必须是 DNS 子域、仅使用小写字符，并应以驱动厂商拥有的 DNS 域名结尾。
func DriverName(driverName string) Option {
	return func(o *options) error {
		o.driverName = driverName
		return nil
	}
}

// GRPCVerbosity 设置 gRPC 调用的日志详细级别，默认 6（含请求与响应），负数关闭日志。
func GRPCVerbosity(level int) Option {
	return func(o *options) error {
		o.grpcVerbosity = level
		return nil
	}
}

// RegistrarDirectoryPath 设置 kubelet 查找插件注册 socket 的目录，默认
// /var/lib/kubelet/plugins_registry；容器内外路径必须一致且目录必须已存在。
func RegistrarDirectoryPath(path string) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.dir = path
		return nil
	}
}

// RegistrarSocketFilename 设置注册 socket 的文件名，默认 <驱动名>-reg.sock。
// 该选项主要用于兼容旧版本安装，普通驱动无需设置，且与 RollingUpdate 互斥。
// 滚动更新时辅助包会自行选择符合 AF_UNIX 长度限制、并尽量保留可读驱动名的 basename。
func RegistrarSocketFilename(name string) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.file = name
		return nil
	}
}

// RegistrarListener 自定义注册 socket 的创建方式，默认先删除已存在的文件再创建 socket。
// 主要用于端到端测试。
func RegistrarListener(listen func(ctx context.Context, path string) (net.Listener, error)) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.listenFunc = listen
		return nil
	}
}

// PluginDataDirectoryPath 设置 DRA gRPC socket（dra.sock）所在目录，同时用于滚动更新时多 Pod 间的协调。
// 默认 /var/lib/kubelet/plugins/<驱动名>，不能与其他插件共享；目录不必位于 kubelet 数据目录下，但 kubelet 必须能访问。
// 容器内外路径必须一致，且目录必须已存在。
func PluginDataDirectoryPath(path string) Option {
	return func(o *options) error {
		o.pluginDataDirectoryPath = path
		return nil
	}
}

// PluginSocket 设置 DRA gRPC socket 的文件名，主要供测试使用，普通驱动依赖默认命名即可。
func PluginSocket(name string) Option {
	return func(o *options) error {
		o.pluginSocket = name
		return nil
	}
}

// PluginListener 自定义 DRA 服务 socket 的创建方式，默认先删除已存在的文件再创建 socket。
// 主要用于端到端测试。
func PluginListener(listen func(ctx context.Context, path string) (net.Listener, error)) Option {
	return func(o *options) error {
		o.draEndpointListen = listen
		return nil
	}
}

// RollingUpdate 用 Pod UID 启用滚动更新：允许新旧两个插件实例并行运行，二者共享
// 插件数据目录但使用不同的 socket，由 kubelet 同时连接。不保证 kubelet 每次使用哪个实例，
// 因此 prepare 与 unprepare 可能切换到不同实例，每个实例跨调用都必须无状态。
// 文件锁跨实例序列化调用；共享状态可在每次调用开始时加载、结束时保存。
// DaemonSet 可通过 downward API 传入 Pod UID：
// https://kubernetes.io/docs/concepts/workloads/pods/downward-api/#downwardapi-fieldRef
// 新实例无法删除旧实例遗留的 socket，因此 Pod 必须处理 SIGINT/TERM 并干净停止辅助进程。
// 传入非空 UID 表示启用，空 UID 表示关闭；与 RegistrarSocketFilename 互斥，且依赖 kubelet 1.33+ 的支持。
func RollingUpdate(uid types.UID) Option {
	return func(o *options) error {
		o.rollingUpdateUID = uid

		// TODO：向 kubelet 查询该 Pod 是否仍在运行，并清理可能遗留的 socket。
		return nil
	}
}

// GRPCInterceptor 注册一元 gRPC 调用拦截器，可多次调用，所有拦截器都会生效。
func GRPCInterceptor(interceptor grpc.UnaryServerInterceptor) Option {
	return func(o *options) error {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptor)
		return nil
	}
}

// GRPCStreamInterceptor 注册流式 gRPC 调用拦截器，可多次调用，所有拦截器都会生效。
func GRPCStreamInterceptor(interceptor grpc.StreamServerInterceptor) Option {
	return func(o *options) error {
		o.streamInterceptors = append(o.streamInterceptors, interceptor)
		return nil
	}
}

// NodeV1beta1 控制是否启用 DRA gRPC API v1beta1，默认启用，主要供端到端测试使用。
func NodeV1beta1(enabled bool) Option {
	return func(o *options) error {
		o.nodeV1beta1 = enabled
		return nil
	}
}

// NodeV1 控制是否启用 DRA gRPC API v1，默认启用，主要供端到端测试使用。
func NodeV1(enabled bool) Option {
	return func(o *options) error {
		o.nodeV1 = enabled
		return nil
	}
}

// KubeClient 提供访问 API Server 的客户端，用于同步 ResourceSlice。调用方需保证
// 该客户端具备对 ResourceSlice 的读写/补丁/list 权限以及读取 Node 的权限，
// 最好用校验准入策略限制其只写本节点的 ResourceSlice。
func KubeClient(kubeClient kubernetes.Interface) Option {
	return func(o *options) error {
		o.kubeClient = kubeClient
		return nil
	}
}

// NodeName 告诉插件所在节点名，同步 ResourceSlice 时必需。
func NodeName(nodeName string) Option {
	return func(o *options) error {
		o.nodeName = nodeName
		return nil
	}
}

// NodeUID 传入 v1.Node 对象的 UID，用于同步 ResourceSlice；非必填，
// 不提供时控制器会自行查询一次。
func NodeUID(nodeUID types.UID) Option {
	return func(o *options) error {
		o.nodeUID = nodeUID
		return nil
	}
}

// Serialize 控制是否序列化 prepare/unprepare 调用，默认开启。驱动可关闭以提升
// 并行度，但需自行处理并发安全。
func Serialize(enabled bool) Option {
	return func(o *options) error {
		o.serialize = enabled
		return nil
	}
}

// FlockDirectoryPath 设置文件锁目录，启用滚动更新并序列化 gRPC 调用时需要；
// 目录必须存在且专供该驱动使用，默认是插件数据目录。
func FlockDirectoryPath(path string) Option {
	return func(o *options) error {
		o.flockDirectoryPath = path
		return nil
	}
}

// RegistrationService 控制是否启动插件注册 gRPC 服务，默认开启，主要供测试使用。
func RegistrationService(enabled bool) Option {
	return func(o *options) error {
		o.registrationService = enabled
		return nil
	}
}

// DRAService 控制是否启动 DRA gRPC 服务，默认开启，主要供测试使用。
func DRAService(enabled bool) Option {
	return func(o *options) error {
		o.draService = enabled
		return nil
	}
}

// HealthService 控制是否向 kubelet 声明并提供可选的 DRAResourceHealth gRPC 服务，
// 默认开启。关闭后 kubelet 不会订阅健康更新，WatchHealthStatus 也不会被调用。
func HealthService(enabled bool) Option {
	return func(o *options) error {
		o.healthService = enabled
		return nil
	}
}

// HealthV1 控制是否启用 DRAResourceHealth gRPC API v1，默认启用，仅在健康服务
// 本身开启时生效，主要供端到端测试使用。
func HealthV1(enabled bool) Option {
	return func(o *options) error {
		o.healthV1 = enabled
		return nil
	}
}

// TODO(harche)：在 HealthV1alpha1 中移除该选项及 v1alpha1 服务支持；
// 预计在 1.40 前后移除，届时 kubelet 1.36 已超出支持的版本偏差范围。
// HealthV1alpha1 控制是否在 v1 之外额外提供 DRAResourceHealth API v1alpha1，
// 默认开启以兼容 1.36 及更早的 kubelet；仅在健康服务开启时生效。
func HealthV1alpha1(enabled bool) Option {
	return func(o *options) error {
		o.healthV1alpha1 = enabled
		return nil
	}
}

// ReconcilePoolWithName 把 ResourceSlice 的同步限定在 Spec.Pool.Name 等于 name 的切片。
// 即使 owner 是 Node，也不会设置 Spec.NodeName；切片通过 NodeSelector/AllNodes 对全集群可见，
// 每个节点都会收到该驱动的全部切片，带来集群级性能开销。空字符串表示使用默认行为。
func ReconcilePoolWithName(name string) Option {
	return func(o *options) error {
		o.reconcilePoolWithName = name
		return nil
	}
}

// EnableDeviceMetadata 启用设备元数据特性，并为元数据文件指定编码的 API 版本列表。
// 启用后框架会为每个 request 写入一个元数据文件和一个 CDI spec，把元数据只读 bind-mount 进容器。
// 文件是 JSON 流：同一份数据按 versions 顺序编码，最新版本在前；消费者使用第一个能理解的版本。
// 因此必须显式包含最新版本，也可携带较旧版本供旧消费者使用。
// 当前最新版本是 []schema.GroupVersion{metadatav1beta1.SchemeGroupVersion}，
// 来自 k8s.io/dynamic-resource-allocation/api/metadata/v1beta1。GA 时驱动必须加入 v1，否则启动校验会在运行时失败。
// 辅助包故意不导出“最新版本”变量，以免变量升级时仅暴露最新版而悄悄破坏旧消费者。
func EnableDeviceMetadata(enabled bool, versions []schema.GroupVersion) Option {
	return func(o *options) error {
		o.enableDeviceMetadata = enabled
		o.metadataVersions = versions
		return nil
	}
}

// CDIDirectory 设置元数据特性写入 CDI spec 文件的目录，默认 /var/run/cdi；
// 只有同时启用 EnableDeviceMetadata 才生效，容器运行时需配置从该目录发现 CDI spec。
func CDIDirectory(path string) Option {
	return func(o *options) error {
		o.cdiDir = path
		return nil
	}
}

// MetadataFileOperations 允许调用方覆盖元数据写入器使用的文件操作，主要用于需要把
// 文件 I/O 代理到远端节点的 E2E 测试。任何 nil 字段都会被填成对应的 os/filepath 函数。
type MetadataFileOperations struct {
	// WriteFile 是写文件函数，默认为 writeFileAtomically（原子写）。
	// ReadFile 是读文件函数，默认为 os.ReadFile。
	// MkdirAll 是递归创建目录函数，默认为 os.MkdirAll。
	// RemoveAll 是递归删除函数，默认为 os.RemoveAll。
	// Remove 是删除单个文件函数，默认为 os.Remove。
	// Glob 是模式匹配函数，默认为 filepath.Glob。
	WriteFile func(name string, data []byte, perm os.FileMode) error
	ReadFile  func(name string) ([]byte, error)
	MkdirAll  func(path string, perm os.FileMode) error
	RemoveAll func(path string) error
	Remove    func(path string) error
	Glob      func(pattern string) ([]string, error)
}

// defaultMetadataFileOperations 把所有未设置的字段填成默认实现，
// 保证后续使用不必再判断 nil。
func defaultMetadataFileOperations(ops MetadataFileOperations) MetadataFileOperations {
	if ops.WriteFile == nil {
		ops.WriteFile = writeFileAtomically
	}
	if ops.ReadFile == nil {
		ops.ReadFile = os.ReadFile
	}
	if ops.MkdirAll == nil {
		ops.MkdirAll = os.MkdirAll
	}
	if ops.RemoveAll == nil {
		ops.RemoveAll = os.RemoveAll
	}
	if ops.Remove == nil {
		ops.Remove = os.Remove
	}
	if ops.Glob == nil {
		ops.Glob = filepath.Glob
	}
	return ops
}

// writeFileAtomically 以"临时文件 + rename"的方式原子写入文件：
// 先在同目录创建临时文件，写完、改权限并 Sync 后关闭，最后 rename 到目标路径并同步目录。
// 任何一步出错都会清理临时文件。
func writeFileAtomically(name string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(name)
	// 在目标同目录创建带随机后缀的临时文件，保证 rename 是同一文件系统内的原子操作。
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(name)+".*.tmp")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	cleanup := true
	// 只要尚未成功 rename，退出时就删除临时文件。
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	// Sync 保证数据真正落盘后再发布。
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// rename 覆盖目标文件，这一步是原子发布点。
	if err := os.Rename(tmpName, name); err != nil {
		return err
	}
	// 同步目录，确保 rename 本身持久化。
	if err := syncDir(dir); err != nil {
		return err
	}
	cleanup = false
	return nil
}

// syncDir 打开目录并调用 Sync，把目录项的变更（如 rename）持久化到磁盘。
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

// MetadataFileOps 覆盖元数据写入器使用的文件系统操作，主要供 E2E 测试把文件 I/O
// 代理到远端节点使用；只有同时启用 EnableDeviceMetadata 才生效。
func MetadataFileOps(ops MetadataFileOperations) Option {
	return func(o *options) error {
		o.metadataFileOps = ops
		return nil
	}
}

// options 保存 Start 的所有启动配置，由函数式选项 Option 逐项填充。
type options struct {
	// logger 用于记录启动与运行日志。
	// grpcVerbosity 是 gRPC 调用日志的详细级别，负数表示关闭。
	// driverName 是 DRA 驱动名称，必填。
	// nodeName 是插件所在节点名，发布 ResourceSlice 时必填。
	// nodeUID 是节点对象 UID，可选。
	// pluginRegistrationEndpoint 描述注册 socket 的目录、文件名及监听函数。
	// pluginDataDirectoryPath 是插件创建 socket 的目录。
	// pluginSocket 是 DRA gRPC 服务使用的 socket 名。
	// rollingUpdateUID 是滚动更新使用的 Pod UID，为空表示不启用。
	// draEndpointListen 是自定义的 DRA socket 监听函数。
	// unaryInterceptors 是一元 gRPC 拦截器列表。
	// streamInterceptors 是流式 gRPC 拦截器列表。
	// kubeClient 是访问 API Server 的客户端，必填。
	// serialize 控制是否序列化 prepare/unprepare 调用。
	// flockDirectoryPath 是文件锁所在目录，默认插件数据目录。
	// nodeV1beta1 控制是否启用 DRA gRPC API v1beta1。
	// nodeV1 控制是否启用 DRA gRPC API v1。
	// registrationService 控制是否启动插件注册服务。
	// draService 控制是否启动 DRA 节点 gRPC 服务。
	// healthService 控制是否提供设备健康 gRPC 服务。
	// healthV1alpha1 控制是否提供健康 API v1alpha1（兼容旧 kubelet）。
	// healthV1 控制是否提供健康 API v1。
	// reconcilePoolWithName 是按 Pool.Name 过滤 ResourceSlice 的条件，空表示不过滤。
	// enableDeviceMetadata 控制是否启用设备元数据特性。
	// metadataVersions 是元数据文件使用的 API 版本列表。
	// cdiDir 是元数据 CDI spec 文件所在目录。
	// metadataFileOps 是可覆盖的文件系统操作。
	logger                     klog.Logger
	grpcVerbosity              int
	driverName                 string
	nodeName                   string
	nodeUID                    types.UID
	pluginRegistrationEndpoint endpoint
	pluginDataDirectoryPath    string // 创建插件 socket 的目录。
	pluginSocket               string // DRA gRPC 服务的 socket 名称。
	rollingUpdateUID           types.UID
	draEndpointListen          func(ctx context.Context, path string) (net.Listener, error)
	unaryInterceptors          []grpc.UnaryServerInterceptor
	streamInterceptors         []grpc.StreamServerInterceptor
	kubeClient                 kubernetes.Interface
	serialize                  bool
	flockDirectoryPath         string
	nodeV1beta1                bool
	nodeV1                     bool
	registrationService        bool
	draService                 bool
	healthService              bool
	healthV1alpha1             bool
	healthV1                   bool
	reconcilePoolWithName      string
	enableDeviceMetadata       bool
	metadataVersions           []schema.GroupVersion
	cdiDir                     string
	metadataFileOps            MetadataFileOperations
}

// Helper 把 kubelet 插件注册服务和 DRA 节点插件服务组合在一起，
// 并通过调用 DRAPlugin 实现来对外提供这两个服务。
type Helper struct {
	// wg 等待 Start 启动的所有后台 goroutine 退出。
	// registrar 是插件注册 gRPC 服务端。
	// pluginServer 是 DRA 节点插件 gRPC 服务端。
	// plugin 是驱动提供的业务实现。
	// driverName 是驱动名称。
	// nodeName 是节点名称。
	// nodeUID 是节点对象 UID。
	// kubeClient 是 API Server 客户端。
	// resourceClient 是资源 API 客户端，用于读取 ResourceClaim。
	// serialize 控制是否序列化 gRPC 调用。
	// grpcMutex 是进程内序列化 gRPC 调用的互斥锁。
	// grpcLockFilePath 非空时改用该文件锁跨实例序列化（滚动更新场景）。
	// reconcilePoolWithName 是 ResourceSlice 同步的 Pool.Name 过滤条件。
	// metadataWriter 是设备元数据写入器，未启用时为 nil。
	// backgroundCtx 用于后续启动的后台活动（如 ResourceSlice 控制器），与插件生命周期绑定。
	backgroundCtx context.Context
	// cancel 取消 backgroundCtx，Stop 也通过它触发所有后台 goroutine 退出。
	cancel                func(cause error)
	wg                    sync.WaitGroup
	registrar             *nodeRegistrar
	pluginServer          *grpcServer
	plugin                DRAPlugin
	driverName            string
	nodeName              string
	nodeUID               types.UID
	kubeClient            kubernetes.Interface
	resourceClient        cgoresource.ResourceV1Interface
	serialize             bool
	grpcMutex             sync.Mutex
	grpcLockFilePath      string
	reconcilePoolWithName string
	metadataWriter        *metadataWriter

	// 资源发布信息会被并发修改，必须用 mutex 保护；控制器只在首次发布时才启动。
	mutex                   sync.Mutex
	resourceSliceController *resourceslice.Controller
}

// Start 启动所有已启用的 gRPC 服务（默认包括注册服务和 DRA 节点插件服务），并把它们委托给 DRAPlugin。
// 健康服务是附加服务：未启用任何 DRA gRPC API 时 kubelet 无法使用驱动，因此在声明服务前快速失败。
// 除非 HealthService(false)，否则声明 DRAResourceHealth；驱动实现版本无关的 WatchHealthStatus，
// 辅助包通过转换同时提供 v1 与 v1alpha1，让 kubelet 选择支持的最新版本，从而无需改驱动源码即可迁移 API。
// ErrHealthNotSupported 会转换为 Unimplemented，告知 kubelet 停止监听。
// 健康服务的声明独立于 draService，便于注册与 DRA 服务拆分部署时共享同一 endpoint。
// 取消传入的 context 或调用 Helper.Stop 可停止所有后台活动，Stop 会阻塞等待。KubeClient 与 DriverName 为必填，
// 需要发布资源时还必须设置 NodeName。
func Start(ctx context.Context, plugin DRAPlugin, opts ...Option) (result *Helper, finalErr error) {
	logger := klog.FromContext(ctx)
	// 先填入所有默认值，再用函数式选项覆盖。
	o := options{
		logger:        klog.Background(),
		grpcVerbosity: 6, // 记录请求与响应，它们可能很大。
		serialize:     true,
		nodeV1beta1:   true,
		nodeV1:        true,
		pluginRegistrationEndpoint: endpoint{
			dir: KubeletRegistryDir,
		},
		draService:          true,
		registrationService: true,
		healthService:       true,
		healthV1:            true,
		healthV1alpha1:      true,
	}
	// 依次应用调用方传入的选项，任一失败立即返回。
	for _, option := range opts {
		if err := option(&o); err != nil {
			return nil, err
		}
	}

	// 校验必填项与互斥选项。
	if o.driverName == "" {
		return nil, errors.New("driver name must be set")
	}
	if o.kubeClient == nil {
		return nil, errors.New("Kubernetes client must be set")
	}
	if o.rollingUpdateUID != "" && o.pluginRegistrationEndpoint.file != "" {
		return nil, errors.New("rolling updates and explicit registration socket filename are mutually exclusive")
	}
	uidPart := ""
	if o.rollingUpdateUID != "" {
		uidPart = "-" + string(o.rollingUpdateUID)
	}
	// 推导各类 socket 名称：滚动更新时使用带 UID 或哈希的默认名，普通模式用固定名。
	if o.pluginRegistrationEndpoint.file == "" {
		if o.rollingUpdateUID != "" {
			o.pluginRegistrationEndpoint.file = RollingUpdateRegistrarSocketFile(o.pluginRegistrationEndpoint.dir, o.driverName, o.rollingUpdateUID)
		} else {
			o.pluginRegistrationEndpoint.file = o.driverName + "-reg.sock"
		}
	}
	if o.pluginDataDirectoryPath == "" {
		o.pluginDataDirectoryPath = path.Join(KubeletPluginsDir, o.driverName)
	}
	if o.pluginSocket == "" {
		o.pluginSocket = "dra" + uidPart + ".sock" // “dra” 为硬编码前缀；目录本身唯一，因此即使不带 UID，完整路径也唯一。
	}

	// 构造 Helper，集中保存驱动实现与各项配置。
	d := &Helper{
		driverName:            o.driverName,
		nodeName:              o.nodeName,
		nodeUID:               o.nodeUID,
		kubeClient:            o.kubeClient,
		resourceClient:        draclient.New(o.kubeClient),
		serialize:             o.serialize,
		plugin:                plugin,
		reconcilePoolWithName: o.reconcilePoolWithName,
	}
	if o.rollingUpdateUID != "" {
		dir := o.pluginDataDirectoryPath
		if o.flockDirectoryPath != "" {
			dir = o.flockDirectoryPath
		}
		// 滚动更新下多个 Pod 并行运行，进程内互斥锁不够，需改用文件锁。
		d.grpcLockFilePath = path.Join(dir, "serialize.lock")
	}
	// 启用设备元数据时，校验版本列表并初始化元数据写入器。
	if o.enableDeviceMetadata {
		cdiDir := o.cdiDir
		if cdiDir == "" {
			cdiDir = DefaultCDIDir
		}
		versions := o.metadataVersions
		// 版本必须显式指定，不能为空。
		if len(versions) == 0 {
			return nil, errors.New("metadata versions must be explicitly selected in EnableDeviceMetadata, none chosen")
		}
		haveLatest := false
		for _, version := range versions {
			// 只接受已知版本，并确认包含了最新版本。
			switch version {
			case metadatav1beta1.SchemeGroupVersion:
				haveLatest = true
			case metadatav1alpha1.SchemeGroupVersion:
			// TODO：移除该版本时启用此错误：
			default:
				return nil, fmt.Errorf("metadata version %q is not supported", version)
			}
		}
		if !haveLatest {
			return nil, fmt.Errorf("the latest metadata version is %s (= metadatav1beta1.SchemeGroupVersion) and must be enabled explicitly", metadatav1beta1.SchemeGroupVersion)
		}
		mw, err := newMetadataWriter(o.driverName, o.pluginDataDirectoryPath, cdiDir, versions, defaultMetadataFileOperations(o.metadataFileOps))
		if err != nil {
			return nil, fmt.Errorf("initialize device metadata: %w", err)
		}
		d.metadataWriter = mw
	}

	// 派生带取消原因的后台 context，外部取消与 Stop 都走同一条关闭路径。
	ctx, cancel := context.WithCancelCause(ctx)
	d.backgroundCtx, d.cancel = ctx, cancel
	logger.V(3).Info("Starting")
	d.wg.Add(1)
	// 该 goroutine 仅用于等待取消信号并计入 WaitGroup。
	go func() {
		defer d.wg.Done()
		defer logger.V(3).Info("Stopping")
		<-ctx.Done()
	}()

	// 若启动过程中发生 panic 或返回错误，调用 Stop 回收已启动的资源。
	defer func() {
		if r := recover(); r != nil {
			d.Stop()
			panic(r)
		}
		if finalErr != nil {
			d.Stop()
		}
	}()

	// 汇总要向 kubelet 声明的 gRPC 服务名。
	var supportedServices []string
	if o.nodeV1 {
		supportedServices = append(supportedServices, drapbv1.DRAPluginService)
	}
	if o.nodeV1beta1 {
		supportedServices = append(supportedServices, drapbv1beta1.DRAPluginService)
	}
	if len(supportedServices) == 0 {
		return nil, errors.New("no supported DRA gRPC API is implemented and enabled")
	}
	if o.healthService {
		if o.healthV1 {
			logger.V(5).Info("advertising v1.DRAResourceHealth gRPC service")
			supportedServices = append(supportedServices, drahealthv1.DRAResourceHealthService)
		}
		if o.healthV1alpha1 {
			logger.V(5).Info("advertising v1alpha1.DRAResourceHealth gRPC service")
			supportedServices = append(supportedServices, drahealthv1alpha1.DRAResourceHealthService)
		}
	}
	draEndpoint := endpoint{
		dir:        o.pluginDataDirectoryPath,
		file:       o.pluginSocket,
		listenFunc: o.draEndpointListen,
	}

	if o.draService {
		pluginServer, err := startGRPCServer(
			klog.LoggerWithName(logger, "dra"),
			o.grpcVerbosity,
			o.unaryInterceptors,
			o.streamInterceptors,
			draEndpoint,
			func(ctx context.Context, err error) { // 该错误处理是必需的
				plugin.HandleError(ctx, err, "DRA gRPC server failed")
			},
			func(grpcServer *grpc.Server) {
				if o.nodeV1 {
					logger.V(5).Info("registering v1.DRAPlugin gRPC service")
					drapbv1.RegisterDRAPluginServer(grpcServer, &nodePluginImplementation{Helper: d})
				}

				if o.nodeV1beta1 {
					logger.V(5).Info("registering v1beta1.DRAPlugin gRPC service")
					drapbv1beta1.RegisterDRAPluginServer(grpcServer, drapbv1beta1.V1ServerWrapper{DRAPluginServer: &nodePluginImplementation{Helper: d}})
				}

				if o.healthService {
					healthServer := &healthServerBridge{plugin: d.plugin}
					if o.healthV1 {
						logger.V(5).Info("registering v1.DRAResourceHealth gRPC service")
						drahealthv1.RegisterDRAResourceHealthServer(grpcServer, healthServer)
					}
					if o.healthV1alpha1 {
						logger.V(5).Info("registering v1alpha1.DRAResourceHealth gRPC service")
						drahealthv1alpha1.RegisterDRAResourceHealthServer(grpcServer, drahealthv1.V1ServerWrapper{Server: healthServer})
					}
				}
			},
		)
		if err != nil {
			return nil, fmt.Errorf("start DRA service: %w", err)
		}
		d.pluginServer = pluginServer
	}

	if o.registrationService {
		// DRA 服务就绪后再启动注册服务，把本驱动声明的服务列表交给 kubelet。
		registrar, err := startRegistrar(
			klog.LoggerWithName(logger, "registrar"),
			o.grpcVerbosity,
			o.unaryInterceptors,
			o.streamInterceptors,
			o.driverName,
			supportedServices,
			draEndpoint.path(),
			o.pluginRegistrationEndpoint,
			func(ctx context.Context, err error) {
				plugin.HandleError(ctx, err, "registrar gRPC server failed")
			},
		)
		if err != nil {
			return nil, fmt.Errorf("start registrar: %w", err)
		}
		d.registrar = registrar
	}

	// 两个 gRPC 服务端自身不处理取消，这里统一在 context 取消时停止它们
	// 以及可能已启动的 ResourceSlice 控制器。
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		<-ctx.Done()

		// 关闭 DRA 服务与注册服务。
		d.pluginServer.stop()
		d.registrar.stop()

		// 控制器指针由 PublishResources 并发设置，需在锁内读取并停止。
		d.mutex.Lock()
		d.resourceSliceController.Stop()
		d.mutex.Unlock()
	}()

	return d, nil
}

// Stop 取消后台 context 并阻塞等待 Start 启动的所有 goroutine 退出，从而释放资源；
// 对 nil Helper 调用是安全的空操作。
func (d *Helper) Stop() {
	if d == nil {
		return
	}
	d.cancel(errors.New("DRA plugin was stopped"))
	d.wg.Wait()
}

// PublishResources 发布或更新驱动要写入 ResourceSlice 的资源信息，可被多次调用；若从未调用则不管理任何切片。
// slice 顺序有意义：名称以索引前缀开头，分配器按名称字典序排序并采用 first-fit，因此优先的切片应排在前面。
// 从随机命名升级会删除并重建已有切片。命名为 [base16 索引]-[驱动名]-[owner 名（非 nil 时)]-。
// 带 binding conditions 的设备所在 pool 最后尝试，其余按 pool 名排序；pool 名可表达驱动偏好。
// 调用方必须保证 pool/slice 满足 API 限制。调用不阻塞，API 写入异步进行；非法切片被异步拒绝，
// 不会返回同步错误。同步错误只表示发布配置错误（如缺少 KubeClient 或 NodeName）。返回后调用方可修改 resources。
func (d *Helper) PublishResources(_ context.Context, resources resourceslice.DriverResources) error {
	// 发布资源必须知道所属节点。
	if d.nodeName == "" {
		return errors.New("no NodeName was set to publish resources")
	}

	// 控制器指针被并发访问，全程持锁。
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// 以 Node 对象作为 ResourceSlice 的 owner。
	owner := resourceslice.Owner{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       d.nodeName,
		UID:        d.nodeUID, // 可选，为空时由控制器决定。
	}
	driverResources := &resourceslice.DriverResources{
		Pools: resources.Pools,
	}

	if d.resourceSliceController == nil {
		// 首次发布时启动控制器，使用后台 context，使其生命周期与插件一致。
		controllerCtx := d.backgroundCtx
		controllerLogger := klog.FromContext(controllerCtx)
		controllerLogger = klog.LoggerWithName(controllerLogger, "ResourceSlice controller")
		controllerCtx = klog.NewContext(controllerCtx, controllerLogger)
		var err error
		if d.resourceSliceController, err = resourceslice.StartController(controllerCtx,
			resourceslice.Options{
				DriverName: d.driverName,
				KubeClient: d.kubeClient,
				Owner:      &owner,
				Resources:  driverResources,
				ErrorHandler: func(ctx context.Context, err error, msg string) {
					// 发布错误（字段丢失、spec 非法等）重试或重启都无法解决，
					// 但也不应导致插件退出，因此统一包装为可恢复错误上报。
					d.plugin.HandleError(ctx, recoverableError{error: err}, msg)
				},
				ReconcilePoolWithName: d.reconcilePoolWithName,
			}); err != nil {
			return fmt.Errorf("start ResourceSlice controller: %w", err)
		}
	} else {
		// 控制器已在运行，直接把最新资源信息推送给它。
		d.resourceSliceController.Update(driverResources)
	}

	return nil
}

// RegistrationStatus 返回 kubelet 注册结果；尚未收到注册回调（或未启动注册服务）时返回 nil。
func (d *Helper) RegistrationStatus() *registerapi.RegistrationStatus {
	if d.registrar == nil {
		return nil
	}
	// TODO：防止并发问题。
	return d.registrar.status
}

// SetGetInfoError 让注册服务的 GetInfo 调用返回指定错误，传 nil 恢复正常行为；
// 该方法主要用于测试。
func (d *Helper) SetGetInfoError(err error) {
	d.registrar.setGetInfoError(err)
}

// SetNotifyRegistrationStatusError 让注册服务的 NotifyRegistrationStatus 调用返回
// 指定错误，传 nil 恢复正常行为；该方法主要用于测试。
func (d *Helper) SetNotifyRegistrationStatusError(err error) {
	d.registrar.setNotifyRegistrationStatusError(err)
}

// serializeGRPCIfEnabled 在启用序列化时获取锁，返回一个必须由调用方 defer 执行的解锁函数；
// 未启用时返回空操作函数。滚动更新场景下使用文件锁跨实例互斥，否则使用进程内互斥锁。
func (d *Helper) serializeGRPCIfEnabled() (func(), error) {
	if !d.serialize {
		return func() {}, nil
	}

	// 滚动更新时多个进程并存，必须借助文件锁，仅靠内存锁无法互斥。
	if d.grpcLockFilePath != "" {
		file, err := fileutil.LockFile(d.grpcLockFilePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("lock file: %w", err)
		}
		return func() {
			_ = file.Close()
		}, nil
	}

	// 单实例场景使用进程内互斥锁。
	d.grpcMutex.Lock()
	return d.grpcMutex.Unlock, nil
}

// nodePluginImplementation 是 Helper 的薄包装，实现 gRPC 服务接口，
// 避免把这些实现细节暴露到公开 API 中。
type nodePluginImplementation struct {
	*Helper
	drapbv1.UnsafeDRAPluginServer
}

// NodePrepareResources 实现 NodePrepareResources gRPC：先从 API Server 取回并校验
// 所有 Claim，再在（可选的）锁保护下调用驱动准备资源，最后把结果转换为 gRPC 响应，
// 并在启用元数据特性时写入元数据文件、注入对应的 CDI 设备 ID。
func (d *nodePluginImplementation) NodePrepareResources(ctx context.Context, req *drapbv1.NodePrepareResourcesRequest) (*drapbv1.NodePrepareResourcesResponse, error) {
	// 把较慢的 API 查询放在加锁之前，缩短临界区。
	claims, err := d.getResourceClaims(ctx, req.Claims)
	if err != nil {
		return nil, fmt.Errorf("get resource claims: %w", err)
	}

	// 按配置获取互斥锁，保证 prepare 调用被序列化。
	unlock, err := d.serializeGRPCIfEnabled()
	if err != nil {
		return nil, fmt.Errorf("serialize gRPC: %w", err)
	}
	defer unlock()

	// 委托驱动执行实际的资源准备工作。
	result, err := d.plugin.PrepareResourceClaims(ctx, claims)
	if err != nil {
		return nil, fmt.Errorf("prepare resource claims: %w", err)
	}

	// 启用元数据特性时，为准备成功的 Claim 写元数据文件并记录要注入的 CDI ID。
	var metadataCDIIDs map[types.UID]map[string]string
	if d.metadataWriter != nil {
		metadataCDIIDs = make(map[types.UID]map[string]string)
		claimsByUID := make(map[types.UID]*resourceapi.ResourceClaim, len(claims))
		for _, c := range claims {
			claimsByUID[c.UID] = c
		}
		for uid, claimResult := range result {
			// 准备失败的 Claim 不写元数据。
			if claimResult.Err != nil {
				continue
			}
			claim := claimsByUID[uid]
			if claim == nil {
				continue
			}
			// 写入失败只记日志、不影响准备结果。
			ids, err := d.metadataWriter.processPreparedClaim(claim, claimResult.Devices)
			if err != nil {
				klog.FromContext(ctx).Error(err, "Failed to write device metadata", "claim", klog.KObj(claim))
				continue
			}
			metadataCDIIDs[uid] = ids
		}
	}

	// 把内部准备结果转换为 gRPC 响应，按 Claim UID 索引。
	resp := &drapbv1.NodePrepareResourcesResponse{Claims: map[string]*drapbv1.NodePrepareResourceResponse{}}
	for uid, claimResult := range result {
		var devices []*drapbv1.Device
		// 记录已注入元数据的 request，避免重复注入。
		injectedRequests := make(map[string]bool)
		for _, result := range claimResult.Devices {
			device := &drapbv1.Device{
				RequestNames: stripSubrequestNames(result.Requests),
				PoolName:     result.PoolName,
				DeviceName:   result.DeviceName,
				CdiDeviceIds: result.CDIDeviceIDs,
				ShareId:      (*string)(result.ShareID),
			}
			// 每个 request 只在第一个设备上追加元数据 CDI ID，
			// 让容器运行时把元数据文件 bind-mount 进容器。
			if ids, ok := metadataCDIIDs[uid]; ok {
				for _, reqName := range device.RequestNames {
					if cdiID, exists := ids[reqName]; exists && !injectedRequests[reqName] {
						device.CdiDeviceIds = append(device.CdiDeviceIds, cdiID)
						injectedRequests[reqName] = true
					}
				}
			}
			devices = append(devices, device)
		}
		resp.Claims[string(uid)] = &drapbv1.NodePrepareResourceResponse{
			Error:   errorString(claimResult.Err),
			Devices: devices,
		}
	}
	return resp, nil
}

// errorString 把错误转换为 gRPC 响应使用的字符串：nil 错误转为空串。
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// stripSubrequestNames 去掉子 request 名称后缀，只保留基础 request 名称。
func stripSubrequestNames(names []string) []string {
	stripped := make([]string, len(names))
	for i, name := range names {
		stripped[i] = resourceclaim.BaseRequestRef(name)
	}
	return stripped
}

// getResourceClaims 根据 gRPC 请求中的 namespace/name 从 API Server 读取完整 Claim，
// 并校验它已分配、UID 未被替换；任一项不满足就返回错误。
func (d *nodePluginImplementation) getResourceClaims(ctx context.Context, claims []*drapbv1.Claim) ([]*resourceapi.ResourceClaim, error) {
	var resourceClaims []*resourceapi.ResourceClaim
	for _, claimReq := range claims {
		claim, err := d.resourceClient.ResourceClaims(claimReq.Namespace).Get(ctx, claimReq.Name, metav1.GetOptions{})
		if err != nil {
			return resourceClaims, fmt.Errorf("retrieve claim %s/%s: %w", claimReq.Namespace, claimReq.Name, err)
		}
		// 只允许准备已经分配资源的 Claim。
		if claim.Status.Allocation == nil {
			return resourceClaims, fmt.Errorf("claim %s/%s not allocated", claimReq.Namespace, claimReq.Name)
		}
		// 校验 UID 一致，避免使用被同名新对象替换后的 Claim。
		if claim.UID != types.UID(claimReq.Uid) {
			return resourceClaims, fmt.Errorf("claim %s/%s got replaced", claimReq.Namespace, claimReq.Name)
		}
		resourceClaims = append(resourceClaims, claim)
	}
	return resourceClaims, nil
}

// NodeUnprepareResources 实现 NodeUnprepareResources gRPC：在（可选的）锁保护下
// 委托驱动撤销资源准备，为清理成功的 Claim 删除元数据文件，并把每个 Claim 的
// 错误结果转换成 gRPC 响应。
func (d *nodePluginImplementation) NodeUnprepareResources(ctx context.Context, req *drapbv1.NodeUnprepareResourcesRequest) (*drapbv1.NodeUnprepareResourcesResponse, error) {
	// 按配置获取互斥锁，保证 unprepare 调用被序列化。
	unlock, err := d.serializeGRPCIfEnabled()
	if err != nil {
		return nil, fmt.Errorf("serialize gRPC: %w", err)
	}
	defer unlock()

	// 清理时 Claim 可能已被删除，只传 UID/namespace/name。
	claims := make([]NamespacedObject, 0, len(req.Claims))
	for _, claim := range req.Claims {
		claims = append(claims, NamespacedObject{UID: types.UID(claim.Uid), NamespacedName: types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}})
	}
	result, err := d.plugin.UnprepareResourceClaims(ctx, claims)
	if err != nil {
		return nil, fmt.Errorf("unprepare resource claims: %w", err)
	}

	// 仅为清理成功的 Claim 删除对应元数据文件；删除失败转为该 Claim 的错误。
	if d.metadataWriter != nil {
		claimsByUID := make(map[types.UID]NamespacedObject, len(claims))
		for _, c := range claims {
			claimsByUID[c.UID] = c
		}
		for uid, claimErr := range result {
			if claimErr != nil {
				continue
			}
			c := claimsByUID[uid]
			if err := d.metadataWriter.cleanupClaim(c.Namespace, c.Name, uid); err != nil {
				result[uid] = fmt.Errorf("clean up device metadata: %w", err)
			}
		}
	}

	// 把每个 Claim 的结果（含错误字符串）填入 gRPC 响应。
	resp := &drapbv1.NodeUnprepareResourcesResponse{Claims: map[string]*drapbv1.NodeUnprepareResourceResponse{}}
	for uid, err := range result {
		resp.Claims[string(uid)] = &drapbv1.NodeUnprepareResourceResponse{
			Error: errorString(err),
		}
	}
	return resp, nil
}
