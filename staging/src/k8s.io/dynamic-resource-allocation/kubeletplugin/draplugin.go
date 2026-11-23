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
	// KubeletPluginsDir is the default directory for [PluginDataDirectoryPath].
	// KubeletPluginsDir 是插件数据目录（PluginDataDirectoryPath）的默认路径。
	KubeletPluginsDir = "/var/lib/kubelet/plugins"
	// KubeletRegistryDir is the default for [RegistrarDirectoryPath]
	// KubeletRegistryDir 是 kubelet 监听插件注册 socket 的目录（RegistrarDirectoryPath）的默认值。
	KubeletRegistryDir = "/var/lib/kubelet/plugins_registry"

	// unixPathMax is the maximum length of an AF_UNIX socket path on Linux.
	// unixPathMax 是 Linux 上 AF_UNIX socket 路径允许的最大字节长度。
	unixPathMax = 108

	// rollingUpdateUIDHashBytes is how much of the SHA-256 digest of a pod UID
	// is base64-encoded when the full UID does not fit in the registration socket
	// path. 8 bytes (64 bits) is ample for node-local uniqueness during rolling
	// updates.
	// rollingUpdateUIDHashBytes 是当完整 Pod UID 无法放进注册 socket 路径时，
	// 用于拼接名称的 UID 摘要字节数。
	rollingUpdateUIDHashBytes = 8

	// rollingUpdateRegistrarSocketHashBytes is how much of the SHA-256 digest
	// of (driver name, pod UID) is base64-encoded into the shortest rolling-update
	// registration socket basename. 16 bytes (128 bits) is ample for
	// node-local uniqueness while keeping AF_UNIX paths under typical limits.
	// rollingUpdateRegistrarSocketHashBytes 是最短滚动更新注册 socket 名称中
	// 使用的 (驱动名, Pod UID) 摘要字节数。
	rollingUpdateRegistrarSocketHashBytes = 16
)

// RollingUpdateRegistrarSocketFile returns a kubelet plugin registration socket
// basename for rolling updates. The full path path.Join(registryDir, basename)
// must fit within unixPathMax bytes.
//
// The basename is chosen in order of preference:
//  1. <driver name>-<pod UID>-reg.sock
//  2. <driver name>-<base64(SHA-256(pod UID)[:8])>-reg.sock
//  3. dra-<base64(SHA-256(driver name, NUL, pod UID)[:16])>-reg.sock
//
// RollingUpdateRegistrarSocketFile 为滚动更新生成注册 socket 的文件名。它按优先级
// 依次尝试三种候选名称，返回第一个拼接后完整路径不超过 unixPathMax 的 basename；
// 若都超长，则返回最短（哈希形式）的候选。
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

// DRAPlugin is the interface that needs to be implemented by a DRA driver to
// use this helper package. The helper package then implements the gRPC
// interface expected by the kubelet by wrapping the DRAPlugin implementation.
//
// DRAPlugin 是 DRA 驱动需要实现的接口；辅助包通过包装该实现来对外提供
// kubelet 所期望的 gRPC 接口。
type DRAPlugin interface {
	// PrepareResourceClaims is called to prepare all resources allocated
	// for the given ResourceClaims. This is used to implement
	// the gRPC NodePrepareResources call.
	//
	// It gets called with the complete list of claims handled by this DRA driver
	// that are needed by some pod. In contrast to the gRPC call, the helper has
	// already retrieved the actual ResourceClaim objects.
	//
	// In addition to that, the helper also:
	// - verifies that all claims are really allocated
	// - increments a numeric counter for each call and
	//   adds its value to a per-context logger with "requestID" as key
	// - adds the method name with "method" as key to that logger
	// - logs the gRPC call and response (configurable with GRPCVerbosity)
	// - serializes all gRPC calls unless the driver explicitly opted out of that
	//
	// This call must be idempotent because the kubelet might have to ask
	// for preparation multiple times, for example if it gets restarted.
	//
	// A DRA driver should verify that all devices listed in a
	// [resourceapi.DeviceRequestAllocationResult] are not already in use
	// for some other ResourceClaim. Kubernetes tries very hard to ensure
	// that, but if something went wrong, then the DRA driver is the last
	// line of defense against using the same device for two different
	// unrelated workloads.
	//
	// If an error is returned, the result is ignored. Otherwise the result
	// must have exactly one entry for each claim, identified by the UID of
	// the corresponding ResourceClaim. For each claim, preparation
	// can be either successful (no error set in the per-ResourceClaim PrepareResult)
	// or can be reported as failed.
	//
	// It is possible to create the CDI spec files which define the CDI devices
	// on-the-fly in PrepareResourceClaims. UnprepareResourceClaims then can
	// remove them. Container runtimes may cache CDI specs but must reload
	// files in case of a cache miss. To avoid false cache hits, the unique
	// name in the CDI device ID should not be reused. A DRA driver can use
	// the claim UID for it.
	//
	// PrepareResourceClaims 准备给定 ResourceClaims 的所有已分配资源，对应 NodePrepareResources
	// gRPC 调用。入参是辅助包已取回的完整 Claim 对象；返回值按 Claim UID 索引，
	// 每个 Claim 必须恰好有一条结果。实现必须是幂等的，因为 kubelet 重启后可能重复调用。
	PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (result map[types.UID]PrepareResult, err error)

	// UnprepareResourceClaims must undo whatever work PrepareResourceClaims did.
	//
	// At the time when this gets called, the original ResourceClaims may have
	// been deleted already. They also don't get cached by the kubelet. Therefore
	// parameters for each ResourcClaim are only the UID, namespace and name.
	// It is the responsibility of the DRA driver to cache whatever additional
	// information it might need about prepared resources.
	//
	// The DRA driver cannot assume that the matching PrepareResourceClaims
	// call was handled by the same process:
	// - The driver might have been restarted in the meantime.
	// - [RollingUpdate], if enabled, can lead to PrepareResourceClaims being
	//   called in one driver instance and UnprepareResourceClaims in another.
	//
	// This call must be idempotent because the kubelet might have to ask
	// for un-preparation multiple times, for example if it gets restarted.
	// Therefore it is not an error if this gets called for a ResourceClaim
	// which is not currently prepared.
	//
	// As with PrepareResourceClaims, the helper takes care of logging
	// and serialization.
	//
	// The conventions for returning one overall error and several per-ResourceClaim
	// errors are the same as in PrepareResourceClaims. In particular, all claims
	// must have an entry in the response, even if that entry is nil.
	//
	// UnprepareResourceClaims 撤销 PrepareResourceClaims 所做的工作，对应 NodeUnprepareResources
	// gRPC 调用。此时原始 Claim 可能已被删除，因此只传入 UID/namespace/name；驱动需自行
	// 缓存所需信息。实现必须幂等，对未准备的 Claim 调用不应报错，且每个 Claim 都要有结果条目。
	UnprepareResourceClaims(ctx context.Context, claims []NamespacedObject) (result map[types.UID]error, err error)

	// HandleError gets called for errors encountered in the background,
	// for example while publishing ResourceSlices.
	// See [resourceslice.Options.ErrorHandler] for details on that.
	//
	// The recommended implementation is to log with
	// runtime.HandleErrorWithContext(ctx, err, msg) (from
	// k8s.io/apimachinery/pkg/util/runtime.HandleErrorWithContext)
	// and then to exit the process if the error is fatal.
	// Ideally the process should shut down gracefully, which can be
	// achieved by canceling the main context of the DRA driver.
	//
	// Fatal errors can be distinguished from recoverable errors via
	//    errors.Is(err, kubeletplugin.ErrRecoverable)
	//
	// This is a mandatory method because drivers should check for errors
	// which won't get resolved by retrying and then fail or change the
	// slices that they are trying to publish:
	// - dropped fields (see [resourceslice.DroppedFieldsError])
	// - validation errors (see [apierrors.IsInvalid])
	//
	// HandleError 处理后台任务（例如发布 ResourceSlice）中遇到的错误。推荐的
	// 实现是用 runtime.HandleErrorWithContext 记录日志，并对致命错误退出进程
	// （最好通过取消主 context 优雅关闭）。可用 errors.Is(err, ErrRecoverable)
	// 区分可恢复错误与致命错误。该方法是强制的。
	HandleError(ctx context.Context, err error, msg string)

	// WatchHealthStatus reports the health of the driver's devices to the
	// kubelet. Send an initial [DeviceHealthReport] covering all devices,
	// then a new report whenever the health of a device changes, until ctx
	// is canceled; then return nil. The method may be called again after a
	// previous call returned, for example when the kubelet reconnects.
	//
	// The kubelet reports a device's health as unknown when it was not
	// refreshed within the device's [DeviceHealth.HealthCheckTimeout]
	// (30 seconds by default), so re-send reports within that window even
	// when nothing changed. Reports may also cover a subset of devices,
	// see [DeviceHealthReport].
	//
	// Sending must not block indefinitely when ctx is canceled:
	//
	//	select {
	//	case <-ctx.Done():
	//		return nil
	//	case reports <- report:
	//	}
	//
	// A driver which does not support health reporting must return
	// [ErrHealthNotSupported] promptly without sending anything, or turn
	// the service off entirely with the [HealthService] option, in which
	// case WatchHealthStatus is never called.
	//
	// WatchHealthStatus 向 kubelet 上报驱动设备的健康状况。先发送一条覆盖所有设备的
	// 初始报告，随后在设备健康变化时发送新报告，直到 ctx 被取消后返回 nil；ctx 取消后
	// 可被再次调用。若不支持健康上报，应尽快返回 ErrHealthNotSupported，或用
	// HealthService 选项彻底关闭该服务（此时该方法不会被调用）。
	WatchHealthStatus(ctx context.Context, reports chan<- DeviceHealthReport) error
}

// ErrRecoverable distinguishes recoverable errors from those errors which are fatal
// and should cause the process to exit. Use with:
//
//	errors.Is(err, ErrRecoverable)
//
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

// PrepareResult contains the result of preparing one particular ResourceClaim.
//
// PrepareResult 保存单个 ResourceClaim 的准备结果，包含错误信息与要注入的 CDI 设备。
type PrepareResult struct {
	// Err, if non-nil, describes a problem that occurred while preparing
	// the ResourceClaim. The devices are then ignored and the kubelet will
	// try to prepare the ResourceClaim again later.
	// Err 非 nil 时表示准备该 Claim 出错，此时 Devices 会被忽略，kubelet 后续会重试。
	Err error

	// Devices contains the IDs of CDI devices associated with specific requests
	// in a ResourceClaim. Those IDs will be passed on to the container runtime
	// by the kubelet.
	//
	// The empty slice is also valid.
	// Devices 保存与 Claim 中特定 request 关联的 CDI 设备 ID，kubelet 会把它们传给
	// 容器运行时；空切片也是合法的。
	Devices []Device
}

// Device provides the CDI device IDs for one request in a ResourceClaim.
//
// Device 描述 ResourceClaim 中一个 request 对应的 CDI 设备信息。
type Device struct {
	// Requests lists the names of requests or subrequests in the
	// ResourceClaim that this device is associated with. The subrequest
	// name may be included here, but it is also okay to just return
	// the request name.
	//
	// A DRA driver can get this string from the Request field in
	// [resourceapi.DeviceRequestAllocationResult], which includes the
	// subrequest name if there is one.
	//
	// If empty, the device is associated with all requests.
	// Requests 列出该设备关联的 request（或子 request）名称；为空表示关联所有 request。
	Requests []string

	// PoolName identifies the DRA driver's pool which contains the device.
	// Must not be empty.
	// PoolName 标识包含该设备的驱动资源池，不能为空。
	PoolName string

	// DeviceName identifies the device inside that pool.
	// Must not be empty.
	// DeviceName 标识资源池内的具体设备，不能为空。
	DeviceName string

	// CDIDeviceIDs lists all CDI devices associated with this DRA device.
	// Each ID must be of the form "<vendor ID>/<class>=<unique name>".
	// May be empty.
	// CDIDeviceIDs 列出与该 DRA 设备关联的所有 CDI 设备 ID，格式形如
	// "<vendor ID>/<class>=<unique name>"，可以为空。
	CDIDeviceIDs []string

	// ShareID identifes the device share.
	// May be empty.
	// ShareID 标识设备的共享份额，可以为空。
	ShareID *types.UID

	// Metadata contains driver-specific device attributes and network data
	// to expose to workloads via CDI bind-mounted JSON files.
	//
	// This field is ignored unless [EnableDeviceMetadata] enables support.
	// If that support is enabled and this field is set, then the attributes
	// and network data are included immediately. If nil, the driver can
	// populate them later via [Helper.UpdateRequestMetadata] before the
	// pod starts (e.g., from an NRI hook after CNI).
	// Metadata 保存要通过 CDI bind-mount 的 JSON 文件暴露给工作负载的设备属性与网络数据。
	// 只有启用 EnableDeviceMetadata 时该字段才生效；设置后立即写入，为 nil 时驱动可在
	// Pod 启动前通过 UpdateRequestMetadata 填充。
	Metadata *DeviceMetadata
}

// DeviceMetadata contains device attributes and network data to expose to
// workloads via the metadata file (see [EnableDeviceMetadata]).
//
// As a best practice, drivers should include all attributes that are published
// in the ResourceSlice for the device, so that the same information is
// available to workloads at runtime. This includes but is not limited to PCI
// bus IDs, mdev UUIDs, hardware model names, and firmware versions. Drivers
// may also include additional attributes that are only relevant at runtime and
// not published in the ResourceSlice. For network devices, populate
// NetworkData with interface names, IP addresses, and hardware addresses
// assigned after CNI runs.
//
// DeviceMetadata 保存要通过元数据文件暴露给工作负载的设备属性和网络数据。
type DeviceMetadata struct {
	// Attributes contains device attributes in the same format as
	// [resourceapi.Device.Attributes] in a ResourceSlice. Keys follow the
	// [resourceapi.QualifiedName] conventions: unqualified names (e.g.,
	// "model") are assumed to belong to the driver's domain, while
	// third-party or well-known attributes use a domain prefix (e.g.,
	// "resource.kubernetes.io/pciBusID"). Each value must have exactly one
	// field set (StringValue, IntValue, BoolValue, or VersionValue).
	// Attributes 保存与 ResourceSlice 中相同格式的设备属性，键遵循 QualifiedName 约定；
	// 每个属性值只能设置 StringValue/IntValue/BoolValue/VersionValue 中的一个。
	Attributes map[string]resourceapi.DeviceAttribute

	// NetworkData contains network-specific device information such as
	// interface name, IP addresses, and hardware address. Typically
	// populated by network DRA plugins (e.g., SR-IOV, DPDK) from an NRI
	// hook after CNI assigns the network configuration.
	// NetworkData 保存接口名、IP 地址、硬件地址等网络信息，通常由网络类 DRA 插件
	// 在 CNI 配置完成后通过 NRI hook 填充。
	NetworkData *resourceapi.NetworkDeviceData
}

// Option implements the functional options pattern for Start.
//
// Option 是 Start 的函数式选项类型，每个选项负责修改启动配置 options。
type Option func(o *options) error

// DriverName defines the driver name for the dynamic resource allocation driver.
// Must be set. Must be a DNS subdomain and should end with a DNS domain
// owned by the vendor of the driver. It should use only lower case characters.
//
// DriverName 设置 DRA 驱动名称，为必填项；必须是 DNS 子域，通常使用小写字符。
func DriverName(driverName string) Option {
	return func(o *options) error {
		o.driverName = driverName
		return nil
	}
}

// GRPCVerbosity sets the verbosity for logging gRPC calls.
// Default is 6, which includes gRPC calls and their responses.
// A negative value disables logging.
//
// GRPCVerbosity 设置 gRPC 调用的日志详细级别，默认 6（含请求与响应），负数关闭日志。
func GRPCVerbosity(level int) Option {
	return func(o *options) error {
		o.grpcVerbosity = level
		return nil
	}
}

// RegistrarDirectoryPath sets the path to the directory where the kubelet
// expects to find registration sockets of plugins. Typically this is
// /var/lib/kubelet/plugins_registry with /var/lib/kubelet being the kubelet's
// data directory.
//
// This is also the default. Some Kubernetes clusters may use a different data directory.
// This path must be the same inside and outside of the driver's container.
// The directory must exist.
//
// RegistrarDirectoryPath 设置 kubelet 查找插件注册 socket 的目录，默认
// /var/lib/kubelet/plugins_registry；容器内外路径必须一致且目录必须已存在。
func RegistrarDirectoryPath(path string) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.dir = path
		return nil
	}
}

// RegistrarSocketFilename sets the name of the socket inside the directory where
// the kubelet watches for registration sockets (see RegistrarDirectoryPath).
//
// Usually DRA drivers should not need this option. It is provided to
// support updates from an installation which used an older release of
// of the helper code.
//
// The default is <driver name>-reg.sock. When rolling updates are enabled,
// the basename is chosen so the full path under [RegistrarDirectoryPath]
// fits within AF_UNIX length limits, preferring names that keep the driver
// name visible (see [RollingUpdateRegistrarSocketFile]).
//
// This option and [RollingUpdate] are mutually exclusive.
//
// RegistrarSocketFilename 设置注册 socket 的文件名，默认 <驱动名>-reg.sock。
// 该选项主要用于兼容旧版本安装，普通驱动无需设置，且与 RollingUpdate 互斥。
func RegistrarSocketFilename(name string) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.file = name
		return nil
	}
}

// RegistrarListener configures how to create the registrar socket.
// The default is to remove the file if it exists and to then
// create a socket.
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// RegistrarListener 自定义注册 socket 的创建方式，默认先删除已存在的文件再创建 socket。
// 主要用于端到端测试。
func RegistrarListener(listen func(ctx context.Context, path string) (net.Listener, error)) Option {
	return func(o *options) error {
		o.pluginRegistrationEndpoint.listenFunc = listen
		return nil
	}
}

// PluginDataDirectoryPath sets the path where the DRA driver creates the
// "dra.sock" socket that the kubelet connects to for the DRA-specific gRPC calls.
// It is also used to coordinate between different Pods when using rolling
// updates. It must not be shared with other kubelet plugins.
//
// The default is /var/lib/kubelet/plugins/<driver name>. This directory
// does not need to be inside the kubelet data directory, as long as
// the kubelet can access it.
//
// This path must be the same inside and outside of the driver's container.
// The directory must exist.
//
// PluginDataDirectoryPath 设置 DRA gRPC socket（dra.sock）所在目录，同时用于滚动
// 更新时多 Pod 间的协调。默认 /var/lib/kubelet/plugins/<驱动名>，不能与其他插件共享。
func PluginDataDirectoryPath(path string) Option {
	return func(o *options) error {
		o.pluginDataDirectoryPath = path
		return nil
	}
}

// PluginSocket sets the name of the socket inside the directory where
// the DRA driver creates the socket for the DRA gRPC calls. This is used
// by the kubelet to connect to the DRA plugin.
//
// This is meant for testing. Normal DRA drivers should not use this and
// instead rely on the automatic handling of the name.
//
// PluginSocket 设置 DRA gRPC socket 的文件名，主要供测试使用，普通驱动依赖默认命名即可。
func PluginSocket(name string) Option {
	return func(o *options) error {
		o.pluginSocket = name
		return nil
	}
}

// PluginListener configures how to create the DRA service socket.
// The default is to remove the file if it exists and to then
// create a socket.
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// PluginListener 自定义 DRA 服务 socket 的创建方式，默认先删除已存在的文件再创建 socket。
// 主要用于端到端测试。
func PluginListener(listen func(ctx context.Context, path string) (net.Listener, error)) Option {
	return func(o *options) error {
		o.draEndpointListen = listen
		return nil
	}
}

// RollingUpdate can be used to enable support for running two plugin instances
// in parallel while a newer instance replaces the older. When enabled, both
// instances must share the same plugin data directory and driver name.
// They create different registration sockets (and DRA gRPC sockets) so the
// kubelet can connect to both at the same time. The default registration socket
// basename is chosen to fit within AF_UNIX path limits (see
// [RollingUpdateRegistrarSocketFile]).
//
// There is no guarantee which of the two instances are used by kubelet.
// For example, it can happen that a claim gets prepared by one instance
// and then needs to be unprepared by the other. Kubelet then may fall back
// to the first one again for some other operation. In practice this means
// that each instance must be entirely stateless across method calls.
// Serialization (on by default, see [Serialize]) ensures that methods
// are serialized across all instances through file locking. The plugin
// implementation can load shared state from a file at the start
// of a call, execute and then store the updated shared state again.
//
// Passing a non-empty uid enables rolling updates, an empty uid disables it.
// The uid must be the pod UID. A DaemonSet can pass that into the driver container
// via the downward API (https://kubernetes.io/docs/concepts/workloads/pods/downward-api/#downwardapi-fieldRef).
//
// Because new instances cannot remove stale sockets of older instances,
// it is important that each pod shuts down cleanly: it must catch SIGINT/TERM
// and stop the helper instead of quitting immediately.
//
// This depends on support in the kubelet which was added in Kubernetes 1.33.
// Don't use this if it is not certain that the kubelet has that support!
//
// This option and [RegistrarSocketFilename] are mutually exclusive.
//
// RollingUpdate 用 Pod UID 启用滚动更新：允许新旧两个插件实例并行运行，二者共享
// 插件数据目录但使用不同的 socket，由 kubelet 同时连接。要求每个实例跨调用无状态，
// 默认通过文件锁跨实例序列化。传入非空 UID 表示启用，空 UID 表示关闭；与
// RegistrarSocketFilename 互斥，且依赖 kubelet 1.33+ 的支持。
func RollingUpdate(uid types.UID) Option {
	return func(o *options) error {
		o.rollingUpdateUID = uid

		// TODO: ask the kubelet whether that pod is still running and
		// clean up leftover sockets?
		return nil
	}
}

// GRPCInterceptor is called for each incoming gRPC method call. This option
// may be used more than once and each interceptor will get called.
//
// GRPCInterceptor 注册一元 gRPC 调用拦截器，可多次调用，所有拦截器都会生效。
func GRPCInterceptor(interceptor grpc.UnaryServerInterceptor) Option {
	return func(o *options) error {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptor)
		return nil
	}
}

// GRPCStreamInterceptor is called for each gRPC streaming method call. This option
// may be used more than once and each interceptor will get called.
//
// GRPCStreamInterceptor 注册流式 gRPC 调用拦截器，可多次调用，所有拦截器都会生效。
func GRPCStreamInterceptor(interceptor grpc.StreamServerInterceptor) Option {
	return func(o *options) error {
		o.streamInterceptors = append(o.streamInterceptors, interceptor)
		return nil
	}
}

// NodeV1beta1 explicitly chooses whether the DRA gRPC API v1beta1
// gets enabled. True by default.
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// NodeV1beta1 控制是否启用 DRA gRPC API v1beta1，默认启用，主要供端到端测试使用。
func NodeV1beta1(enabled bool) Option {
	return func(o *options) error {
		o.nodeV1beta1 = enabled
		return nil
	}
}

// NodeV1 explicitly chooses whether the DRA gRPC API v1
// gets enabled. True by default.
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// NodeV1 控制是否启用 DRA gRPC API v1，默认启用，主要供端到端测试使用。
func NodeV1(enabled bool) Option {
	return func(o *options) error {
		o.nodeV1 = enabled
		return nil
	}
}

// KubeClient grants the plugin access to the API server. This is needed
// for syncing ResourceSlice objects. It's the responsibility of the DRA driver
// developer to ensure that this client has permission to read, write,
// patch and list such objects. It also needs permission to read node objects.
// Ideally, a validating admission policy should be used to limit write
// access to ResourceSlices which belong to the node.
//
// KubeClient 提供访问 API Server 的客户端，用于同步 ResourceSlice。调用方需保证
// 该客户端具备对 ResourceSlice 的读写/补丁/list 权限以及读取 Node 的权限，
// 最好用校验准入策略限制其只写本节点的 ResourceSlice。
func KubeClient(kubeClient kubernetes.Interface) Option {
	return func(o *options) error {
		o.kubeClient = kubeClient
		return nil
	}
}

// NodeName tells the plugin on which node it is running. This is needed for
// syncing ResourceSlice objects.
//
// NodeName 告诉插件所在节点名，同步 ResourceSlice 时必需。
func NodeName(nodeName string) Option {
	return func(o *options) error {
		o.nodeName = nodeName
		return nil
	}
}

// NodeUID tells the plugin the UID of the v1.Node object. This is used
// when syncing ResourceSlice objects, but doesn't have to be used. If
// not supplied, the controller will look up the object once.
//
// NodeUID 传入 v1.Node 对象的 UID，用于同步 ResourceSlice；非必填，
// 不提供时控制器会自行查询一次。
func NodeUID(nodeUID types.UID) Option {
	return func(o *options) error {
		o.nodeUID = nodeUID
		return nil
	}
}

// Serialize overrides whether the helper serializes the prepare and unprepare
// calls. The default is to serialize.
//
// A DRA driver can opt out of that to speed up parallel processing, but then
// must handle concurrency itself.
//
// Serialize 控制是否序列化 prepare/unprepare 调用，默认开启。驱动可关闭以提升
// 并行度，但需自行处理并发安全。
func Serialize(enabled bool) Option {
	return func(o *options) error {
		o.serialize = enabled
		return nil
	}
}

// FlockDir changes where lock files are created and locked. A lock file
// is needed when serializing gRPC calls and rolling updates are enabled.
// The directory must exist and be reserved for exclusive use by the
// driver. The default is the plugin data directory.
//
// FlockDirectoryPath 设置文件锁目录，启用滚动更新并序列化 gRPC 调用时需要；
// 目录必须存在且专供该驱动使用，默认是插件数据目录。
func FlockDirectoryPath(path string) Option {
	return func(o *options) error {
		o.flockDirectoryPath = path
		return nil
	}
}

// RegistrationService controls whether the kubelet plugin gRPC service
// is started. It's on by default. This is meant for testing, normal
// DRA drivers should use the default.
//
// RegistrationService 控制是否启动插件注册 gRPC 服务，默认开启，主要供测试使用。
func RegistrationService(enabled bool) Option {
	return func(o *options) error {
		o.registrationService = enabled
		return nil
	}
}

// DRAService controls whether the DRA gRPC service
// is started. It's on by default. This is meant for testing, normal
// DRA drivers should use the default.
//
// DRAService 控制是否启动 DRA gRPC 服务，默认开启，主要供测试使用。
func DRAService(enabled bool) Option {
	return func(o *options) error {
		o.draService = enabled
		return nil
	}
}

// HealthService controls whether the optional DRAResourceHealth gRPC service
// is advertised to the kubelet and served. It's on by default.
//
// Disabling it allows a driver to implement [DRAPlugin.WatchHealthStatus] but
// turn off device health reporting, for example behind its own configuration
// flag. When disabled, the kubelet does not subscribe to health updates and
// [DRAPlugin.WatchHealthStatus] is never called.
//
// HealthService 控制是否向 kubelet 声明并提供可选的 DRAResourceHealth gRPC 服务，
// 默认开启。关闭后 kubelet 不会订阅健康更新，WatchHealthStatus 也不会被调用。
func HealthService(enabled bool) Option {
	return func(o *options) error {
		o.healthService = enabled
		return nil
	}
}

// HealthV1 explicitly chooses whether the DRAResourceHealth gRPC API
// v1 gets enabled. True by default. Only has an effect when the health
// service itself is enabled (see [HealthService]).
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// HealthV1 控制是否启用 DRAResourceHealth gRPC API v1，默认启用，仅在健康服务
// 本身开启时生效，主要供端到端测试使用。
func HealthV1(enabled bool) Option {
	return func(o *options) error {
		o.healthV1 = enabled
		return nil
	}
}

// HealthV1alpha1 chooses whether the DRAResourceHealth gRPC API v1alpha1
// gets served in addition to v1. True by default, so that kubelets from
// releases where only v1alpha1 existed (1.36 and older) can consume device
// health. Only has an effect when the health service itself is enabled
// (see [HealthService]).
//
// This is used in Kubernetes for end-to-end testing. The default should
// be fine for DRA drivers.
//
// TODO(harche): remove this option and the v1alpha1 serving support in the
// 1.40 era, when kubelet 1.36 is no longer within the supported version skew.
//
// HealthV1alpha1 控制是否在 v1 之外额外提供 DRAResourceHealth API v1alpha1，
// 默认开启以兼容 1.36 及更早的 kubelet；仅在健康服务开启时生效。
func HealthV1alpha1(enabled bool) Option {
	return func(o *options) error {
		o.healthV1alpha1 = enabled
		return nil
	}
}

// ReconcilePoolWithName limits reconciliation to slices with Spec.Pool.Name
// equal to name.
//
// If set, the controller enqueues only ResourceSlices with a matching
// Spec.Pool.Name and does not set Spec.NodeName (even for Node owners).
// This enables node-owned slices that remain cluster-visible via
// NodeSelector or AllNodes.
//
// Beware that this has a performance impact on the cluster
// because all nodes have to receive all ResourceSlices of
// the driver. Without this option, each node only receives
// its own ResourceSlices.
//
// Empty means the default behavior.
//
// ReconcilePoolWithName 把 ResourceSlice 的同步限定在 Spec.Pool.Name 等于 name 的切片，
// 此时不设置 Spec.NodeName，便于节点自有的切片仍通过 NodeSelector/AllNodes 对全集群可见。
// 该选项会增加性能开销；空字符串表示使用默认行为（每个节点只同步自己的切片）。
func ReconcilePoolWithName(name string) Option {
	return func(o *options) error {
		o.reconcilePoolWithName = name
		return nil
	}
}

// EnableDeviceMetadata enables the device metadata feature. When enabled,
// the framework writes a metadata file per request under the plugin data
// directory and a CDI spec per request under the CDI directory (see
// [CDIDirectory]) that bind-mounts the metadata file read-only into containers
// at /var/run/kubernetes.io/dra-device-attributes/{claimName}/{requestName}/metadata.json.
//
// Each metadata file is a JSON stream: the same data encoded once per
// configured API version (newest first). A consumer reads through the stream
// and uses the first object whose apiVersion it understands, similar to how
// clients negotiate API versions with the API server.
//
// When enabled, the version(s) used for encoding the metadata must be
// specified explicitly:
//   - At least the latest version must be used.
//   - Older versions may be used. This can be useful to support
//     consumers that haven't been updated yet to support the latest
//     version.
//   - Versions are written in the order listed here.
//
// At the moment, []schema.GroupVersion{metadatav1beta1.SchemeGroupVersion}
// is the latest version, from k8s.io/dynamic-resource-allocation/api/metadata/v1beta1.
// When the feature graduates to GA, DRA drivers have to be updated to include
// the v1 version in their EnableDeviceMetadata call or they will encounter a runtime
// error when the versions are validated during startup.
//
// The latest version is intentionally not exported as a variable in this
// package because then a DRA driver using only it would become incompatible to
// older consumers when that variable gets bumped to the next version and the
// DRA driver updates the package. This would only show up when explicitly
// testing with an older consumer.
//
// EnableDeviceMetadata 启用设备元数据特性，并为元数据文件指定编码的 API 版本列表
// （必须包含最新版本，按优先顺序排列）。启用后框架会为每个 request 写入元数据文件
// 和对应的 CDI spec，把元数据只读 bind-mount 进容器。
func EnableDeviceMetadata(enabled bool, versions []schema.GroupVersion) Option {
	return func(o *options) error {
		o.enableDeviceMetadata = enabled
		o.metadataVersions = versions
		return nil
	}
}

// CDIDirectory sets the directory where the framework writes CDI spec files
// for the device metadata feature. Each file is named
// {driverName}_metadata_{claimUID}_{requestName}.json and contains a CDI
// spec that bind-mounts the corresponding metadata JSON file into containers.
// The container runtime must be configured to discover CDI specs from this
// directory.
//
// The default is /var/run/cdi. This option has no effect unless
// [EnableDeviceMetadata] is also set to true.
//
// CDIDirectory 设置元数据特性写入 CDI spec 文件的目录，默认 /var/run/cdi；
// 只有同时启用 EnableDeviceMetadata 才生效，容器运行时需配置从该目录发现 CDI spec。
func CDIDirectory(path string) Option {
	return func(o *options) error {
		o.cdiDir = path
		return nil
	}
}

// MetadataFileOperations allows callers to override how the metadata writer
// performs filesystem operations. This is used by E2E tests that need to
// proxy file I/O into a remote node (e.g. via PodDirIO on kind clusters).
//
// Any nil field is defaulted to the corresponding os / filepath function.
//
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

// MetadataFileOps overrides the filesystem operations used by the metadata
// writer. This is intended for testing environments where the process running
// the kubelet plugin helper does not share a filesystem with the node
// (e.g. E2E tests on kind clusters).
//
// This option has no effect unless [EnableDeviceMetadata] is also set to true.
//
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
	pluginDataDirectoryPath    string // The directory where the plugin socket is created.
	pluginSocket               string // The socket name for the DRA gRPC service.
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

// Helper combines the kubelet registration service and the DRA node plugin
// service and implements them by calling a [DRAPlugin] implementation.
//
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
	// backgroundCtx is for activities that are started later.
	// backgroundCtx 用于后续启动的后台活动（如 ResourceSlice 控制器），与插件生命周期绑定。
	backgroundCtx context.Context
	// cancel cancels the backgroundCtx.
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

	// Information about resource publishing changes concurrently and thus
	// must be protected by the mutex. The controller gets started only
	// if needed.
	// 资源发布信息会被并发修改，必须用 mutex 保护；控制器只在首次发布时才启动。
	mutex                   sync.Mutex
	resourceSliceController *resourceslice.Controller
}

// Start sets up all enabled gRPC servers (by default, one for registration,
// one for the DRA node client) and implements them by calling a [DRAPlugin]
// implementation.
//
// The context and/or DRAPlugin.Stop can be used to stop all background activity.
// Stop also blocks. A logger can be stored in the context to add values or
// a name to all log entries.
//
// [KubeClient] and [DriverName] options are mandatory.
// If the plugin will be used to publish resources, [NodeName]
// is also mandatory.
//
// Start 启动所有已启用的 gRPC 服务（默认包括注册服务和 DRA 节点插件服务），
// 并把它们委托给给定的 DRAPlugin 实现。取消传入的 context 或调用 Helper.Stop
// 可停止所有后台活动，Stop 会阻塞等待。KubeClient 与 DriverName 为必填，
// 需要发布资源时还必须设置 NodeName。
func Start(ctx context.Context, plugin DRAPlugin, opts ...Option) (result *Helper, finalErr error) {
	logger := klog.FromContext(ctx)
	// 先填入所有默认值，再用函数式选项覆盖。
	o := options{
		logger:        klog.Background(),
		grpcVerbosity: 6, // Logs requests and responses, which can be large.
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
		o.pluginSocket = "dra" + uidPart + ".sock" // "dra" is hard-coded. The directory is unique, so we get a unique full path also without the UID.
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
		// Enable file locking, required for concurrently running pods.
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
			// TODO: when removing this version, enable this error:
			// return nil, fmt.Errorf(`metadata version "metadata.resource.k8s.io/v1alpha1" is no longer supported`)
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

	// Stop calls cancel and therefore both cancellation
	// and Stop cause goroutines to stop.
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

	// Clean up if we don't finish succcessfully.
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
	// The health service is an add-on: without at least one DRA gRPC API
	// version the kubelet cannot use the driver, so fail fast before
	// advertising anything else.
	if len(supportedServices) == 0 {
		return nil, errors.New("no supported DRA gRPC API is implemented and enabled")
	}
	// Advertise the DRAResourceHealth service unless turned off with
	// HealthService(false). Drivers implement the version-neutral
	// [DRAPlugin.WatchHealthStatus] method; the helper serves both the newest
	// (v1) gRPC API and the older v1alpha1 API (via a conversion wrapper)
	// so the kubelet can pick the most recent version it supports. This enables
	// graceful migration of DRA drivers across health API versions without
	// requiring source changes in the driver.
	//
	// A driver which does not support health reporting returns
	// ErrHealthNotSupported from WatchHealthStatus; the helper then fails the
	// kubelet's stream with Unimplemented, which tells the kubelet to stop
	// watching.
	//
	// Advertisement is intentionally independent of o.draService: in split
	// deployments the registrar instance (DRAService(false)) advertises
	// services which a separate service instance provides on the shared
	// endpoint.
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
		// Run the node plugin gRPC server first to ensure that it is ready.
		pluginServer, err := startGRPCServer(
			klog.LoggerWithName(logger, "dra"),
			o.grpcVerbosity,
			o.unaryInterceptors,
			o.streamInterceptors,
			draEndpoint,
			func(ctx context.Context, err error) { // This error handler is REQUIRED
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
					// The helper implements the versioned gRPC servers itself
					// by calling the driver's version-neutral WatchHealthStatus.
					healthServer := &healthServerBridge{plugin: d.plugin}
					if o.healthV1 {
						logger.V(5).Info("registering v1.DRAResourceHealth gRPC service")
						drahealthv1.RegisterDRAResourceHealthServer(grpcServer, healthServer)
					}
					if o.healthV1alpha1 {
						// Also serve the older v1alpha1 API by converting
						// the v1 responses on the fly.
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
		// Now make it available to kubelet.
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

	// startGRPCServer and startRegistrar don't implement cancellation
	// themselves, we add that for both here.
	// 两个 gRPC 服务端自身不处理取消，这里统一在 context 取消时停止它们
	// 以及可能已启动的 ResourceSlice 控制器。
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		<-ctx.Done()

		// Time to stop.
		// 关闭 DRA 服务与注册服务。
		d.pluginServer.stop()
		d.registrar.stop()

		// d.resourceSliceController is set concurrently.
		// 控制器指针由 PublishResources 并发设置，需在锁内读取并停止。
		d.mutex.Lock()
		d.resourceSliceController.Stop()
		d.mutex.Unlock()
	}()

	return d, nil
}

// Stop ensures that all spawned goroutines are stopped and frees resources.
//
// Stop 取消后台 context 并阻塞等待 Start 启动的所有 goroutine 退出，从而释放资源；
// 对 nil Helper 调用是安全的空操作。
func (d *Helper) Stop() {
	if d == nil {
		return
	}
	d.cancel(errors.New("DRA plugin was stopped"))
	// Wait for goroutines in Start to clean up and exit.
	d.wg.Wait()
}

// PublishResources may be called one or more times to publish
// resource information in ResourceSlice objects. If it never gets
// called, then the kubelet plugin does not manage any ResourceSlice
// objects.
//
// Note that the order of slices in the provided resources
// is significant: ResourceSlices start with a name prefix based on
// their index, and the allocator uses this order to prioritize allocation
// (first-fit). Also, upgrading to this naming from an older
// version using random names without the index at the beginning will
// cause existing slices to be recreated.
// The name prefix follows the scheme:
// [index encoded as base16 string]-[driver name]-[owner name (if not nil)]-
// See [resourceslice.Pool.Slices] for more details.
//
// Pools are sorted first so that pools with devices which have binding
// conditions are tried last, then by name. So the name can also be used to
// indicate preference when a driver publishes more than one pool.
//
// PublishResources does not block, so it might still take a while
// after it returns before all information is actually written
// to the API server.
//
// It is the responsibility of the caller to ensure that the pools and
// slices described in the driver resources parameters are valid
// according to the restrictions defined in the resource.k8s.io API.
//
// Invalid ResourceSlices will be rejected by the apiserver during
// publishing, which happens asynchronously and thus does not
// get returned as error here. The only error returned here is
// when publishing was not set up properly, for example missing
// [KubeClient] or [NodeName] options.
//
// The caller may modify the resources after this call returns.
//
// PublishResources 发布或更新驱动要写入 ResourceSlice 的资源信息，可被多次调用；
// 若从未调用，则插件不管理任何 ResourceSlice。该调用不阻塞，实际写入 API Server
// 是异步的；这里只同步地检查发布配置是否正确（如缺少 NodeName）。
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
		UID:        d.nodeUID, // Optional, will be determined by controller if empty.
	}
	driverResources := &resourceslice.DriverResources{
		Pools: resources.Pools,
	}

	if d.resourceSliceController == nil {
		// Start publishing the information. The controller is using
		// our background context, not the one passed into this
		// function, and thus is connected to the lifecycle of the
		// plugin.
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
					// ResourceSlice publishing errors like dropped fields or
					// invalid spec are not going to get resolved by retrying,
					// but neither is restarting the process going to help
					// -> all errors are recoverable.
					// 发布错误（字段丢失、spec 非法等）重试或重启都无法解决，
					// 但也不应导致插件退出，因此统一包装为可恢复错误上报。
					d.plugin.HandleError(ctx, recoverableError{error: err}, msg)
				},
				ReconcilePoolWithName: d.reconcilePoolWithName,
			}); err != nil {
			return fmt.Errorf("start ResourceSlice controller: %w", err)
		}
	} else {
		// Inform running controller about new information.
		// 控制器已在运行，直接把最新资源信息推送给它。
		d.resourceSliceController.Update(driverResources)
	}

	return nil
}

// RegistrationStatus returns the result of registration, nil if none received yet.
//
// RegistrationStatus 返回 kubelet 注册结果；尚未收到注册回调（或未启动注册服务）时返回 nil。
func (d *Helper) RegistrationStatus() *registerapi.RegistrationStatus {
	if d.registrar == nil {
		return nil
	}
	// TODO: protect against concurrency issues.
	return d.registrar.status
}

// SetGetInfoError configures the registration server to make
// GetInfo calls return the specified error.
// To restore normal behavior, call SetGetInfoError(nil).
//
// SetGetInfoError 让注册服务的 GetInfo 调用返回指定错误，传 nil 恢复正常行为；
// 该方法主要用于测试。
func (d *Helper) SetGetInfoError(err error) {
	d.registrar.setGetInfoError(err)
}

// SetNotifyRegistrationStatusError configures the registration server
// to make NotifyRegistrationStatus calls return the specified error.
// To restore normal behavior, call SetNotifyRegistrationStatusError(nil).
//
// SetNotifyRegistrationStatusError 让注册服务的 NotifyRegistrationStatus 调用返回
// 指定错误，传 nil 恢复正常行为；该方法主要用于测试。
func (d *Helper) SetNotifyRegistrationStatusError(err error) {
	d.registrar.setNotifyRegistrationStatusError(err)
}

// serializeGRPCIfEnabled locks a mutex if serialization is enabled.
// Either way it returns a method that the caller must invoke
// via defer.
//
// serializeGRPCIfEnabled 在启用序列化时获取锁，返回一个必须由调用方 defer 执行的解锁函数；
// 未启用时返回空操作函数。滚动更新场景下使用文件锁跨实例互斥，否则使用进程内互斥锁。
func (d *Helper) serializeGRPCIfEnabled() (func(), error) {
	if !d.serialize {
		return func() {}, nil
	}

	// If rolling updates are enabled, we cannot do only in-memory locking.
	// We must use file locking.
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

// nodePluginImplementation is a thin wrapper around the helper instance.
// It prevents polluting the public API with these implementation details.
//
// nodePluginImplementation 是 Helper 的薄包装，实现 gRPC 服务接口，
// 避免把这些实现细节暴露到公开 API 中。
type nodePluginImplementation struct {
	*Helper
	drapbv1.UnsafeDRAPluginServer
}

// NodePrepareResources implements [drapbv1.NodePrepareResources].
//
// NodePrepareResources 实现 NodePrepareResources gRPC：先从 API Server 取回并校验
// 所有 Claim，再在（可选的）锁保护下调用驱动准备资源，最后把结果转换为 gRPC 响应，
// 并在启用元数据特性时写入元数据文件、注入对应的 CDI 设备 ID。
func (d *nodePluginImplementation) NodePrepareResources(ctx context.Context, req *drapbv1.NodePrepareResourcesRequest) (*drapbv1.NodePrepareResourcesResponse, error) {
	// Do slow API calls before serializing.
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

	// Write device metadata files and collect CDI device IDs to inject.
	// metadataCDIIDs[claimUID][requestName] = cdiDeviceID
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
			// Inject metadata CDI device IDs into the first device
			// per request so the container runtime bind-mounts the
			// metadata file.
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

// NodeUnprepareResources implements [draapi.NodeUnprepareResources].
//
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

	// Clean up metadata files for successfully unprepared claims.
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
