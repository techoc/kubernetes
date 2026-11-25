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

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	gpath "path"
	"strings"
	"sync"
	"time"

	systemd "github.com/coreos/go-systemd/v22/daemon"

	"golang.org/x/time/rate"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/util/managedfields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilwaitgroup "k8s.io/apimachinery/pkg/util/waitgroup"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapi "k8s.io/apiserver/pkg/endpoints"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/statusz"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	restclient "k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	openapibuilder3 "k8s.io/kube-openapi/pkg/builder3"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/handler3"
	openapiutil "k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// APIGroupInfo 描述一个 API 组的安装信息：版本优先级、各版本的资源存储、
// 编解码器与 OpenAPI 定义。它是 InstallAPIGroup* 的唯一输入。
type APIGroupInfo struct {
	// PrioritizedVersions 按优先级从高到低排列；发现文档与默认版本选择都依赖该顺序。
	PrioritizedVersions []schema.GroupVersion
	// 该组内各资源的信息：从版本到资源再到存储的映射。
	VersionedResourcesStorageMap map[string]map[string]rest.Storage
	// OptionsExternalVersion 控制 schema 中通用对象（如 api.Status、
	// api.DeleteOptions、metav1.ListOptions）所使用的 APIVersion。
	// 其他实现者可能定义了 "v1beta1" 版本，却想使用 Kubernetes 的 "v1"
	// 内部对象。若为 nil，默认取 groupMeta.GroupVersion。
	// TODO: 待 https://github.com/kubernetes/kubernetes/issues/19018 修复后移除本字段。
	OptionsExternalVersion *schema.GroupVersion
	// MetaGroupVersion 默认为 "meta.k8s.io/v1"，是用于解码 ListOptions 等
	// 通用 API 实现的 scheme 组版本。未来的改动将允许它按组版本而异
	// （当不可避免的 meta/v2 组出现时）。
	MetaGroupVersion *schema.GroupVersion

	// Scheme 包含此组使用的所有类型以及它们之间的转换方式
	// （或把本组之外、但被本 API 接受的对象转换进来的方式）。
	// TODO: 替换为接口
	Scheme *runtime.Scheme
	// NegotiatedSerializer 控制此组如何编码和解码数据
	NegotiatedSerializer runtime.NegotiatedSerializer
	// ParameterCodec 对传给 API 调用的查询参数执行转换
	ParameterCodec runtime.ParameterCodec

	// StaticOpenAPISpec 是由一起安装的所有资源的定义派生出的 spec。
	// 它在 InstallAPIGroups、InstallAPIGroup 和 InstallLegacyAPIGroup 期间被设置。
	StaticOpenAPISpec map[string]*spec.Schema
}

// destroyStorage 关闭该组所有版本的存储；只在服务器销毁时调用一次。
func (a *APIGroupInfo) destroyStorage() {
	for _, stores := range a.VersionedResourcesStorageMap {
		for _, store := range stores {
			store.Destroy()
		}
	}
}

// GenericAPIServer 持有 Kubernetes 集群 API 服务器的状态。
//
// 该结构只承载“通用”能力；具体 API 组由 kube-apiserver/aggregator 通过
// InstallAPIGroup* 注入。生命周期钩子（post-start/pre-shutdown）在
// PrepareRun 之后才会真正生效。
type GenericAPIServer struct {
	// discoveryAddresses 用于为发现服务构造 cluster IP。
	discoveryAddresses discovery.Addresses

	// LoopbackClientConfig 是到 API 服务器的特权回环（loopback）连接的配置
	LoopbackClientConfig *restclient.Config

	// Flagz 用于搭建 flagz 端点。
	Flagz flagz.Reader

	// minRequestTimeout 是请求超时下限的最小值。用于构造 RESTHandler
	minRequestTimeout time.Duration

	// ShutdownTimeout 是服务器关闭使用的超时。它指定服务器
	// 优雅关闭返回前的超时时间。
	ShutdownTimeout time.Duration

	// legacyAPIGroupPrefixes 用于设置授权时的 URL 解析，以及校验发往
	// InstallLegacyAPIGroup 的请求
	legacyAPIGroupPrefixes sets.String

	// admissionControl 用于构建支撑某个 API Group 的 RESTStorage。
	admissionControl admission.Interface

	// SecureServingInfo 保存 TLS 服务器的配置。
	SecureServingInfo *SecureServingInfo

	// ExternalAddress 是本 GenericAPIServer 在外部（公网）URL 中
	// 应当使用的地址（主机名或 IP 加端口）。
	ExternalAddress string

	// Serializer 控制本服务器如何序列化不带组/版本前缀的通用 API 对象。
	// 各个 APIGroup 可以定义自己的序列化器。
	Serializer runtime.NegotiatedSerializer

	// “输出”
	// Handler 持有本 API 服务器正在使用的处理器链
	Handler *APIServerHandler

	// UnprotectedDebugSocket 用于在 unix 域 socket 上提供 pprof 信息。
	// 该 socket 不受认证/授权保护。
	UnprotectedDebugSocket *routes.DebugSocket

	// listedPathProvider 是一个 lister，提供 / 路径下展示的路径集合
	listedPathProvider routes.ListedPathProvider

	// DiscoveryGroupManager 以非聚合形式服务 /apis。
	DiscoveryGroupManager discovery.GroupManager

	// AggregatedDiscoveryGroupManager 以聚合形式服务 /apis。
	AggregatedDiscoveryGroupManager discoveryendpoint.ResourceManager

	// PeerAggregatedDiscoveryManager 服务从所有对等 apiserver 聚合而来的 /apis。
	PeerAggregatedDiscoveryManager discoveryendpoint.PeerAggregatedResourceManager

	// AggregatedLegacyDiscoveryGroupManager 以聚合形式服务 /api。
	AggregatedLegacyDiscoveryGroupManager discoveryendpoint.ResourceManager

	// 若这些配置非 nil，则启用 swagger 和/或 OpenAPI。
	openAPIConfig *openapicommon.Config

	// 若这些配置非 nil，则启用 swagger 和/或 OpenAPI V3。
	openAPIV3Config *openapicommon.OpenAPIV3Config

	// skipOpenAPIInstallation 表示在 PrepareRun 期间不安装 OpenAPI 处理器。
	// 当具体的 API Server 有自己的 OpenAPI 处理器时
	// （如 kube-aggregator），将其设为 true
	skipOpenAPIInstallation bool

	// OpenAPIVersionedService 控制 /openapi/v2 端点，可用于更新对外提供的 spec。
	// 若 `openAPIConfig` 非 nil 且 `skipOpenAPIInstallation` 不为 true，
	// 它会在 PrepareRun 期间被设置。
	OpenAPIVersionedService *handler.OpenAPIService

	// OpenAPIV3VersionedService 控制 /openapi/v3 端点，可用于更新对外提供的 spec。
	// 若 `openAPIConfig` 非 nil 且 `skipOpenAPIInstallation` 不为 true，
	// 它会在 PrepareRun 期间被设置。
	OpenAPIV3VersionedService *handler3.OpenAPIService

	// StaticOpenAPISpec 是从 restful container 端点派生出的 spec。
	// 它在 PrepareRun 期间被设置。
	StaticOpenAPISpec *spec.Swagger

	// 每个 PostStartHooks 都在服务器开始监听之后、各自独立的 go func 中调用，
	// 彼此之间没有顺序保证。映射的键是用于错误报告的名字。
	// 钩子若愿意，可以通过返回错误让进程以 panic 终止。
	postStartHookLock      sync.Mutex
	postStartHooks         map[string]postStartHookEntry
	postStartHooksCalled   bool
	disabledPostStartHooks sets.String

	preShutdownHookLock    sync.Mutex
	preShutdownHooks       map[string]preShutdownHookEntry
	preShutdownHooksCalled bool

	// healthz 检查项注册表
	healthzRegistry healthCheckRegistry
	readyzRegistry  healthCheckRegistry
	livezRegistry   healthCheckRegistry

	livezGracePeriod time.Duration

	// 审计。后端在服务器开始监听之前启动。
	AuditBackend audit.Backend

	// Authorizer 决定用户是否被允许发起某个请求。Handler 会基于请求 URI
	// 做初步授权检查，但可能有必要做额外检查，
	// 例如 create-on-update 的场景
	Authorizer authorizer.UnconditionalAuthorizer

	// EquivalentResourceRegistry 提供与给定资源等价的资源信息，
	// 以及与给定资源关联的 kind。随着资源被安装，它们会注册到这里。
	EquivalentResourceRegistry runtime.EquivalentResourceRegistry

	// delegationTarget 是委托链中的下一环。它永远不为 nil。
	delegationTarget DelegationTarget

	// NonLongRunningRequestWaitGroup 允许你在服务器关闭期间，
	// 等待所有与非长时运行请求
	// 关联的链式处理器完成。
	NonLongRunningRequestWaitGroup *utilwaitgroup.SafeWaitGroup
	// WatchRequestWaitGroup 允许我们在服务器关闭期间，
	// 等待所有与活跃 watch 请求关联的
	// 链式处理器完成。
	WatchRequestWaitGroup *utilwaitgroup.RateLimitedSafeWaitGroup

	// ShutdownDelayDuration 允许在一段时间内阻塞关闭流程，例如直到指向本 API 服务器的
	// endpoints 在所有节点上收敛为止。在此期间，API 服务器持续服务，/healthz 返回 200，
	// 但 /readyz 返回失败。
	ShutdownDelayDuration time.Duration

	// 写请求中会被接收并解码的请求体大小上限。
	// 0 表示无限制。
	maxRequestBodyBytes int64

	// APIServerID 是本 API 服务器的 ID
	APIServerID string

	// StorageReadinessHook 实现了用于检查已注册资源的
	// 底层存储就绪性的 post-start-hook 功能。
	StorageReadinessHook *StorageReadinessHook

	// StorageVersionManager 持有本服务器安装的 API 资源的存储版本。
	StorageVersionManager storageversion.Manager

	// EffectiveVersion 基于 API/特性的生命周期，
	// 决定哪些 API 和特性可用。
	EffectiveVersion basecompatibility.EffectiveVersion
	// EmulationForwardCompatible 是一个选项，隐式启用所有在模拟版本之后引入、
	// 且优先级高于模拟版本下已启用的同组资源 API 的 API。
	// 若为 true，所有优先级高于模拟版本下已启用的同组资源 API（beta+）的 API 都会被安装。
	// 当控制器实现（就其二进制版本而言）迁移到更新的 API 版本、并且在设置了模拟版本时
	// 仍使用这些更新版本时，需要该选项。
	// 不适用于 alpha API。
	EmulationForwardCompatible bool
	// RuntimeConfigEmulationForwardCompatible 是一个通过 runtime-config 显式启用
	// 模拟版本之后引入的特定 API 的选项。
	// 若为 true，由 group/version 标识、在 --runtime-config flag 中启用的 API 即使是在模拟版本之后引入的也会被安装。--runtime-config flag 中标识多个 API 的取值（如 api/all、api/ga、api/beta）不受本 flag 影响，只会启用当前模拟版本下可用的 API。
	// 若为 false，当 --runtime-config flag 中显式启用的任何 GroupVersion 或 GroupVersionResource 是在模拟版本之后引入时会抛出错误。
	RuntimeConfigEmulationForwardCompatible bool

	// FeatureGate 是如果你有 feature gate，将其贯穿传递下去的方式。
	FeatureGate featuregate.FeatureGate

	// lifecycleSignals 提供对 API 服务器生命周期中发生的各种信号的访问。
	lifecycleSignals lifecycleSignals

	// destroyFns 包含一组在关闭时应调用以清理资源的函数。
	destroyFns []func()

	// muxAndDiscoveryCompleteSignals 持有指示所有已知 HTTP 路径均已注册完成的信号。
	// 它的存在主要是为了避免在资源实际存在、但我们尚未安装其处理路径时
	// 返回 404 响应。
	// 对外暴露该字段是为了便于各个服务器的组合。
	// 本字段的主要使用者是 WithMuxCompleteProtection 过滤器和 NotFoundHandler
	muxAndDiscoveryCompleteSignals map[string]<-chan struct{}

	// ShutdownSendRetryAfter 决定在 API 服务器优雅终止期间何时启动
	// HTTP 服务器的关闭。若为 true，我们会等待
	// 在途的非长时运行请求排空后再启动
	// HTTP 服务器的关闭。若为 false，一旦 ShutdownDelayDuration
	// 到期，我们就启动 HTTP 服务器的关闭。
	// 若启用，在 ShutdownDelayDuration 到期后，任何 incoming 请求都会
	// 被以 429 状态码和 'Retry-After' 响应拒绝。
	ShutdownSendRetryAfter bool

	// ShutdownWatchTerminationGracePeriod，若设为正值，
	// 是 API 服务器等待所有活跃
	// watch 请求排空的最长时间。
	// 该宽限期一旦到期，API 服务器将不再
	// 等待在途的活跃 watch 请求排空，而是进入
	// 优雅关闭流程的下一步。
	// 若设为正值，API 服务器将跟踪
	// 在途活跃 watch 请求的数量，并在关闭期间
	// 至多等待指定的时长，允许这些活跃
	// watch 请求在一定限流下排空。
	// 默认值为零，表示 API 服务器不跟踪
	// 在途活跃 watch 请求，也不会等待
	// 它们排空，以保持向后兼容。
	// 该宽限期与其他宽限期相互正交，
	// 且不会被任何其他宽限期覆盖。
	ShutdownWatchTerminationGracePeriod time.Duration
}

// DelegationTarget 是一个接口，允许组合 API 服务器并让顶层处理
// 按预期工作。
//
// 组合模式：kube-apiserver 在最外层，聚合层与各 API 组按顺序委托。每个
// 实现的 NextDelegate 指向下一环，直到 emptyDelegate 返回 404。
type DelegationTarget interface {
	// UnprotectedHandler 返回一个未经标准保护链保护的 handler
	UnprotectedHandler() http.Handler

	// PostStartHooks 返回需要被组合的 post-start 钩子
	PostStartHooks() map[string]postStartHookEntry

	// PreShutdownHooks 返回需要被组合的 pre-stop 钩子
	PreShutdownHooks() map[string]preShutdownHookEntry

	// HealthzChecks 返回需要被组合的 healthz 检查项
	HealthzChecks() []healthz.HealthChecker

	// ListedPaths 返回用于支持索引页的路径
	ListedPaths() []string

	// NextDelegate 返回委托链中的下一个 delegationTarget
	NextDelegate() DelegationTarget

	// PrepareRun 执行 API 安装完成后的准备步骤。它会递归调用各委托对象的同名函数。
	PrepareRun() preparedGenericAPIServer

	// MuxAndDiscoveryCompleteSignals 暴露已注册的信号，用于指示所有已知 HTTP 路径是否已安装完成。
	MuxAndDiscoveryCompleteSignals() map[string]<-chan struct{}

	// Destroy 在关闭时清理自身资源。
	// Destroy 必须以线程安全的方式实现，并且要能接受
	// 被多次调用。
	Destroy()
}

// UnprotectedHandler 返回未经认证/授权链保护的 handler；仅供内部组合使用，
// 直接对外暴露会绕过所有安全过滤。
func (s *GenericAPIServer) UnprotectedHandler() http.Handler {
	// 委托时，需要由被委托到的服务器决定是否使用 gorestful
	return s.Handler.Director
}

// PostStartHooks 返回可被组合的 post-start 钩子集合。
func (s *GenericAPIServer) PostStartHooks() map[string]postStartHookEntry {
	return s.postStartHooks
}

// PreShutdownHooks 返回可被组合的 pre-shutdown 钩子集合。
func (s *GenericAPIServer) PreShutdownHooks() map[string]preShutdownHookEntry {
	return s.preShutdownHooks
}

// HealthzChecks 返回 /healthz 汇总使用的检查项。
func (s *GenericAPIServer) HealthzChecks() []healthz.HealthChecker {
	return s.healthzRegistry.checks
}

// ListedPaths 返回根路径索引页展示的路径列表。
func (s *GenericAPIServer) ListedPaths() []string {
	return s.listedPathProvider.ListedPaths()
}

// NextDelegate 返回委托链中的下一环。
func (s *GenericAPIServer) NextDelegate() DelegationTarget {
	return s.delegationTarget
}

// RegisterMuxAndDiscoveryCompleteSignal 注册给定信号，用于判断所有已知
// HTTP 路径是否已注册完成。可以在实例化通用服务器之后、运行之前调用本方法。
func (s *GenericAPIServer) RegisterMuxAndDiscoveryCompleteSignal(signalName string, signal <-chan struct{}) error {
	if _, exists := s.muxAndDiscoveryCompleteSignals[signalName]; exists {
		return fmt.Errorf("%s already registered", signalName)
	}
	s.muxAndDiscoveryCompleteSignals[signalName] = signal
	return nil
}

func (s *GenericAPIServer) MuxAndDiscoveryCompleteSignals() map[string]<-chan struct{} {
	return s.muxAndDiscoveryCompleteSignals
}

// RegisterDestroyFunc 注册一个将在 Destroy() 期间调用的函数。
// 该函数必须是幂等的，并且要能接受被多次调用。
func (s *GenericAPIServer) RegisterDestroyFunc(destroyFn func()) {
	s.destroyFns = append(s.destroyFns, destroyFn)
}

// Destroy 在关闭时清理其自身及其委托目标的全部资源。
// 它先销毁自身资源，随后继续处理
// 其委托目标。
func (s *GenericAPIServer) Destroy() {
	for _, destroyFn := range s.destroyFns {
		destroyFn()
	}
	if s.delegationTarget != nil {
		s.delegationTarget.Destroy()
	}
}

// emptyDelegate 是委托链的终点：未匹配的请求最终落到这里的 handler。
type emptyDelegate struct {
	// handler 在委托链的末尾被调用：
	// 当请求命中未注册的 HTTP 路径时，各个服务器会直接将其透传，直到到达该 handler。
	handler http.Handler
}

// NewEmptyDelegate 返回不带自定义 handler 的链尾（默认 404）。
func NewEmptyDelegate() DelegationTarget {
	return emptyDelegate{}
}

// NewEmptyDelegateWithCustomHandler 允许注册一个自定义 handler，通常用于对 404 请求做特殊处理
func NewEmptyDelegateWithCustomHandler(handler http.Handler) DelegationTarget {
	return emptyDelegate{handler}
}

func (s emptyDelegate) UnprotectedHandler() http.Handler {
	return s.handler
}
func (s emptyDelegate) PostStartHooks() map[string]postStartHookEntry {
	return map[string]postStartHookEntry{}
}
func (s emptyDelegate) PreShutdownHooks() map[string]preShutdownHookEntry {
	return map[string]preShutdownHookEntry{}
}
func (s emptyDelegate) HealthzChecks() []healthz.HealthChecker {
	return []healthz.HealthChecker{}
}
func (s emptyDelegate) ListedPaths() []string {
	return []string{}
}
func (s emptyDelegate) NextDelegate() DelegationTarget {
	return nil
}
func (s emptyDelegate) PrepareRun() preparedGenericAPIServer {
	return preparedGenericAPIServer{nil}
}
func (s emptyDelegate) MuxAndDiscoveryCompleteSignals() map[string]<-chan struct{} {
	return map[string]<-chan struct{}{}
}
func (s emptyDelegate) Destroy() {
}

// preparedGenericAPIServer 是私有包装类型，强制在调用 Run 之前先调用 PrepareRun()；
// 未导出的类型把“必须先 PrepareRun”编码进类型系统，避免漏装 healthz/OpenAPI。
type preparedGenericAPIServer struct {
	*GenericAPIServer
}

// PrepareRun 执行 API 安装完成后的准备步骤。它会递归调用各委托对象的同名函数。
//
// 顺序很重要：先递归准备委托链，再安装本层的 OpenAPI 与健康检查，
// 这样根路径索引才能看到所有已注册端点。
func (s *GenericAPIServer) PrepareRun() preparedGenericAPIServer {
	s.delegationTarget.PrepareRun()

	// OpenAPI 安装可能较重，skipOpenAPIInstallation 允许 kube-aggregator 等自行处理。
	if s.openAPIConfig != nil && !s.skipOpenAPIInstallation {
		s.OpenAPIVersionedService, s.StaticOpenAPISpec = routes.OpenAPI{
			Config: s.openAPIConfig,
		}.InstallV2(s.Handler.GoRestfulContainer, s.Handler.NonGoRestfulMux)
	}

	if s.openAPIV3Config != nil && !s.skipOpenAPIInstallation {
		s.OpenAPIV3VersionedService = routes.OpenAPI{
			V3Config: s.openAPIV3Config,
		}.InstallV3(s.Handler.GoRestfulContainer, s.Handler.NonGoRestfulMux)
	}

	s.installHealthz()
	s.installLivez()

	// 一旦开始关闭流程，readiness 就应开始失败，
	// 让负载均衡在 ShutdownDelayDuration 窗口内先停止发送流量。
	readinessStopCh := s.lifecycleSignals.ShutdownInitiated.Signaled()
	err := s.addReadyzShutdownCheck(readinessStopCh)
	if err != nil {
		klog.Errorf("Failed to install readyz shutdown check %s", err)
	}
	s.installReadyz()

	componentName := "apiserver"
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if s.Flagz != nil {
			flagz.Install(s.Handler.NonGoRestfulMux, componentName, s.Flagz)
		}
	}
	// statusz 最后安装，这样它就能列出所有已注册的路径
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		statusz.Install(s.Handler.NonGoRestfulMux, componentName, statusz.NewRegistry(s.EffectiveVersion, statusz.WithListedPaths(s.ListedPaths())))
	}

	return preparedGenericAPIServer{s}
}

// Run 启动安全的 http 服务器。只有当 stopCh 关闭
// 或安全端口最初无法监听时才会返回。
//
// Deprecated: 请改用 RunWithContext。为避免破坏现有使用方，
// Run 不会被移除，但新代码不应使用。
func (s preparedGenericAPIServer) Run(stopCh <-chan struct{}) error {
	// 兼容旧调用方：把 channel 适配为 context 后复用新实现。
	ctx := wait.ContextForChannel(stopCh)
	return s.RunWithContext(ctx)
}

// RunWithContext 启动安全的 http 服务器。只有当 ctx 被取消
// 或安全端口最初无法监听时才会返回。
// 以下是各 context/channel/signal 之间依赖关系的示意图：
//
// |                                   ctx
// |                                    |
// |           ---------------------------------------------------------
// |           |                                                       |
// |    ShutdownInitiated (shutdownInitiatedCh)                        |
// |           |                                                       |
// | (ShutdownDelayDuration)                                    (PreShutdownHooks)
// |           |                                                       |
// |  AfterShutdownDelayDuration (delayedStopCh)   PreShutdownHooksStopped (preShutdownHooksHasStoppedCh)
// |           |                                                       |
// |           |-------------------------------------------------------|
// |                                    |
// |                                    |
// |               NotAcceptingNewRequest (notAcceptingNewRequestCh)
// |                                    |
// |                                    |
// |           |----------------------------------------------------------------------------------|
// |           |                        |              |                                          |
// |        [without                 [with             |                                          |
// | ShutdownSendRetryAfter]  ShutdownSendRetryAfter]  |                                          |
// |           |                        |              |                                          |
// |           |                        ---------------|                                          |
// |           |                                       |                                          |
// |           |                      |----------------|-----------------------|                  |
// |           |                      |                                        |                  |
// |           |         (NonLongRunningRequestWaitGroup::Wait)   (WatchRequestWaitGroup::Wait)   |
// |           |                      |                                        |                  |
// |           |                      |------------------|---------------------|                  |
// |           |                                         |                                        |
// |           |                         InFlightRequestsDrained (drainedCh)                      |
// |           |                                         |                                        |
// |           |-------------------|---------------------|----------------------------------------|
// |                               |                     |
// |                       stopHttpServerCtx     (AuditBackend::Shutdown())
// |                               |
// |                       listenerStoppedCh
// |                               |
// |      HTTPServerStoppedListening (httpServerStoppedListeningCh)
//
// 核心不变量：先停止接收新请求，再等待在途请求排空，最后才关闭监听，
// 从而避免优雅终止期间出现 connection refused。
func (s preparedGenericAPIServer) RunWithContext(ctx context.Context) error {
	stopCh := ctx.Done()
	delayedStopCh := s.lifecycleSignals.AfterShutdownDelayDuration
	shutdownInitiatedCh := s.lifecycleSignals.ShutdownInitiated

	// 关闭时清理资源。
	defer s.Destroy()

	// 若启用了 UDS profiling，启动一个监听该 socket 的本地 http 服务器
	if s.UnprotectedDebugSocket != nil {
		go func() {
			defer utilruntime.HandleCrashWithContext(ctx)
			klog.Error(s.UnprotectedDebugSocket.RunWithContext(ctx))
		}()
	}

	// 为关闭 MuxAndDiscoveryComplete 信号派生一个新的 goroutine：
	// 信号的注册发生在通用 api server 的构造阶段，
	// 委托链中最后一个服务器会聚合前面各个实例的信号
	go func() {
		for _, muxAndDiscoveryCompletedSignal := range s.GenericAPIServer.MuxAndDiscoveryCompleteSignals() {
			select {
			case <-muxAndDiscoveryCompletedSignal:
				continue
			case <-stopCh:
				klog.V(1).Infof("haven't completed %s, stop requested", s.lifecycleSignals.MuxAndDiscoveryComplete.Name())
				return
			}
		}
		s.lifecycleSignals.MuxAndDiscoveryComplete.Signal()
		klog.V(1).Infof("%s has all endpoints registered and discovery information is complete", s.lifecycleSignals.MuxAndDiscoveryComplete.Name())
	}()

	go func() {
		defer delayedStopCh.Signal()
		defer klog.V(1).InfoS("[graceful-termination] shutdown event", "name", delayedStopCh.Name())

		<-stopCh

		// 一旦开始关闭流程，/readyz 就应当开始返回失败。
		// 这给了负载均衡器一个由 ShutdownDelayDuration 定义的窗口，
		// 用于发现 /readyz 变红并停止向本服务器发送流量。
		shutdownInitiatedCh.Signal()
		klog.V(1).InfoS("[graceful-termination] shutdown event", "name", shutdownInitiatedCh.Name())

		time.Sleep(s.ShutdownDelayDuration)
	}()

	// 在延迟的 stopCh 之后关闭 socket
	shutdownTimeout := s.ShutdownTimeout
	if s.ShutdownSendRetryAfter {
		// 启用该模式时，我们会执行以下操作：
		// - 服务器持续监听，直到所有在途的既有请求
		//   （不含活跃的长时运行请求）全部排空。
		// - 排空后，以 2 秒超时调用 http Server Shutdown，
		//   因为 net/http 会等待 1 秒让对端响应 GO_AWAY 帧，
		//   所以我们至少应等待 2 秒
		shutdownTimeout = 2 * time.Second
		klog.V(1).InfoS("[graceful-termination] using HTTP Server shutdown timeout", "shutdownTimeout", shutdownTimeout)
	}

	notAcceptingNewRequestCh := s.lifecycleSignals.NotAcceptingNewRequest
	drainedCh := s.lifecycleSignals.InFlightRequestsDrained
	// 取消父 context 并不会立即取消 HTTP 服务器。
	// 这里只继承 context 的值，取消由我们自己处理。
	stopHTTPServerCtx, stopHTTPServer := context.WithCancelCause(context.WithoutCancel(ctx))
	go func() {
		defer stopHTTPServer(errors.New("time to stop HTTP server"))

		timeToStopHttpServerCh := notAcceptingNewRequestCh.Signaled()
		if s.ShutdownSendRetryAfter {
			timeToStopHttpServerCh = drainedCh.Signaled()
		}

		<-timeToStopHttpServerCh
	}()

	// 在任何请求进入之前启动审计后端。这意味着必须在 http 服务器开始服务之前
	// 调用 Backend.Run，否则 Backend.ProcessEvents 调用可能会阻塞。
	// AuditBackend.Run 会在所有在途请求排空后立即停止。
	if s.AuditBackend != nil {
		if err := s.AuditBackend.Run(drainedCh.Signaled()); err != nil {
			return fmt.Errorf("failed to run the audit backend: %v", err)
		}
	}

	stoppedCh, listenerStoppedCh, err := s.NonBlockingRunWithContext(stopHTTPServerCtx, shutdownTimeout)
	if err != nil {
		return err
	}

	httpServerStoppedListeningCh := s.lifecycleSignals.HTTPServerStoppedListening
	go func() {
		<-listenerStoppedCh
		httpServerStoppedListeningCh.Signal()
		klog.V(1).InfoS("[graceful-termination] shutdown event", "name", httpServerStoppedListeningCh.Name())
	}()

	// 一旦 ShutdownDelayDuration 到期且 pre-shutdown 钩子执行完毕，
	// 我们就不再接受新请求。
	preShutdownHooksHasStoppedCh := s.lifecycleSignals.PreShutdownHooksStopped
	go func() {
		defer klog.V(1).InfoS("[graceful-termination] shutdown event", "name", notAcceptingNewRequestCh.Name())
		defer notAcceptingNewRequestCh.Signal()

		// 等待延迟的 stopCh 之后再关闭处理器链
		<-delayedStopCh.Signaled()

		// 此外还要等待 pre-shutdown 钩子全部完成，因为其中一些钩子需要
		// 发起 API 调用来清理自身（例如 lease reconciler 要把自己
		// 从活跃服务器列表中移除）。
		<-preShutdownHooksHasStoppedCh.Signaled()
	}()

	// 等待所有在途的非长时运行请求完成
	nonLongRunningRequestDrainedCh := make(chan struct{})
	go func() {
		defer close(nonLongRunningRequestDrainedCh)
		defer klog.V(1).Info("[graceful-termination] in-flight non long-running request(s) have drained")

		// 在延迟的 stopCh 之后再关闭处理器链（调用 Wait 之后它会拒绝一切请求）。
		<-notAcceptingNewRequestCh.Signaled()

		// 等待所有请求完成，其数量受 RequestTimeout 变量约束。
		// 一旦调用 Wait，apiserver 就应当
		// 通过 WithWaitGroup 过滤器以 {503, Retry-After} 响应拒绝任何
		// incoming 请求。但相反，我们观察到 incoming 请求收到的是
		// 'connection refused' 错误，原因是：此时我们已调用 'Server.Shutdown'
		// 且 net/http 服务器已停止监听，这会使 incoming 请求
		// 得到 'connection refused' 错误。
		// 另一方面，若启用了 'ShutdownSendRetryAfter'，incoming 请求
		// 会被以 {429, Retry-After} 拒绝，因为只有在在途请求
		// 排空之后才会调用 'Server.Shutdown'。
		// TODO: 能否将这两种优雅终止模式合并？
		s.NonLongRunningRequestWaitGroup.Wait()
	}()

	// 等待所有在途 watch 完成
	activeWatchesDrainedCh := make(chan struct{})
	go func() {
		defer close(activeWatchesDrainedCh)

		<-notAcceptingNewRequestCh.Signaled()
		if s.ShutdownWatchTerminationGracePeriod <= time.Duration(0) {
			klog.V(1).InfoS("[graceful-termination] not going to wait for active watch request(s) to drain")
			return
		}

		// 等待所有活跃 watch 完成
		grace := s.ShutdownWatchTerminationGracePeriod
		activeBefore, activeAfter, err := s.WatchRequestWaitGroup.Wait(func(count int) (utilwaitgroup.RateLimiter, context.Context, context.CancelFunc) {
			qps := float64(count) / grace.Seconds()
			// TODO: 我们不希望 QPS（每秒排空的最大请求数）
			//  跌破某个下限，因为我们希望服务器
			//  尽快排空活跃的 watch 请求。
			//  目前硬编码为 200，该值可能根据
			//  规模测试的结果调整。
			if qps < 200 {
				qps = 200
			}

			ctx, cancel := context.WithTimeout(context.Background(), grace)
			// 我们不预期单次 Wait 调用会消耗超过一个令牌，
			// 因此 burst 设为 1。
			return rate.NewLimiter(rate.Limit(qps), 1), ctx, cancel
		})
		klog.V(1).InfoS("[graceful-termination] active watch request(s) have drained",
			"duration", grace, "activeWatchesBefore", activeBefore, "activeWatchesAfter", activeAfter, "error", err)
	}()

	go func() {
		defer klog.V(1).InfoS("[graceful-termination] shutdown event", "name", drainedCh.Name())
		defer drainedCh.Signal()

		<-nonLongRunningRequestDrainedCh
		<-activeWatchesDrainedCh
	}()

	klog.V(1).Info("[graceful-termination] waiting for shutdown to be initiated")
	<-stopCh

	// 直接运行 shutdown 钩子。这包括从
	// kubernetes endpoint 中注销（针对 kube-apiserver 的情形）。
	func() {
		defer func() {
			preShutdownHooksHasStoppedCh.Signal()
			klog.V(1).InfoS("[graceful-termination] pre-shutdown hooks completed", "name", preShutdownHooksHasStoppedCh.Name())
		}()
		err = s.RunPreShutdownHooks()
	}()
	if err != nil {
		return err
	}

	// 等待所有在途请求排空，其数量受 RequestTimeout 变量约束。
	<-drainedCh.Signaled()

	if s.AuditBackend != nil {
		s.AuditBackend.Shutdown()
		klog.V(1).InfoS("[graceful-termination] audit backend shutdown completed")
	}

	// 等待 stoppedCh，它在优雅终止（server.Shutdown）完成时被关闭。
	<-listenerStoppedCh
	<-stoppedCh

	klog.V(1).Info("[graceful-termination] apiserver is exiting")
	return nil
}

// NonBlockingRun 启动安全的 http 服务器。若安全端口无法监听，
// 则返回错误。
// 返回的通道在（异步）终止完成时被关闭。
//
// Deprecated: 请改用 RunWithContext。为避免破坏现有使用方，
// Run 不会被移除，但新代码不应使用。
func (s preparedGenericAPIServer) NonBlockingRun(stopCh <-chan struct{}, shutdownTimeout time.Duration) (<-chan struct{}, <-chan struct{}, error) {
	ctx := wait.ContextForChannel(stopCh)
	return s.NonBlockingRunWithContext(ctx, shutdownTimeout)
}

// NonBlockingRunWithContext 启动安全的 http 服务器。若安全端口无法监听，
// 则返回错误。
// 返回的通道在（异步）终止完成时被关闭。
func (s preparedGenericAPIServer) NonBlockingRunWithContext(ctx context.Context, shutdownTimeout time.Duration) (<-chan struct{}, <-chan struct{}, error) {
	// 使用内部停止通道，以便出错时能清理监听器。
	internalStopCh := make(chan struct{})
	var stoppedCh <-chan struct{}
	var listenerStoppedCh <-chan struct{}
	if s.SecureServingInfo != nil && s.Handler != nil {
		var err error
		stoppedCh, listenerStoppedCh, err = s.SecureServingInfo.Serve(s.Handler, shutdownTimeout, internalStopCh)
		if err != nil {
			close(internalStopCh)
			return nil, nil, err
		}
	}

	// 监听器已成功绑定之后，关闭所提供的通道以保证清理完成
	// 就是调用方的责任了。
	go func() {
		<-ctx.Done()
		close(internalStopCh)
	}()

	// post-start 钩子只在监听成功后运行，避免对外宣告一个无法服务的实例。
	s.RunPostStartHooks(ctx)

	// 通知 systemd 启动完成（非 systemd 环境下为空操作）。
	if _, err := systemd.SdNotify(true, "READY=1\n"); err != nil {
		klog.Errorf("Unable to send systemd daemon successful start message: %v\n", err)
	}

	return stoppedCh, listenerStoppedCh, nil
}

// installAPIResources 是一个私有方法，用于安装支撑每个 api groupversionresource
// 的 REST 存储
func (s *GenericAPIServer) installAPIResources(apiPrefix string, apiGroupInfo *APIGroupInfo, typeConverter managedfields.TypeConverter) error {
	var resourceInfos []*storageversion.ResourceInfo
	for _, groupVersion := range apiGroupInfo.PrioritizedVersions {
		// 没有资源的版本不注册，否则会在发现文档中出现空版本。
		if len(apiGroupInfo.VersionedResourcesStorageMap[groupVersion.Version]) == 0 {
			klog.Warningf("Skipping API %v because it has no resources.", groupVersion)
			continue
		}

		apiGroupVersion, err := s.getAPIGroupVersion(apiGroupInfo, groupVersion, apiPrefix)
		if err != nil {
			return err
		}
		if apiGroupInfo.OptionsExternalVersion != nil {
			apiGroupVersion.OptionsExternalVersion = apiGroupInfo.OptionsExternalVersion
		}
		apiGroupVersion.TypeConverter = typeConverter
		apiGroupVersion.MaxRequestBodyBytes = s.maxRequestBodyBytes

		discoveryAPIResources, r, err := apiGroupVersion.InstallREST(s.Handler.GoRestfulContainer)

		if err != nil {
			return fmt.Errorf("unable to setup API %v: %v", apiGroupInfo, err)
		}
		resourceInfos = append(resourceInfos, r...)

		// 聚合发现只聚合 /apis 前缀下的资源；
		// 旧版 /api 使用单独的 manager，避免与 /apis 的组名冲突。
		if apiPrefix == APIGroupPrefix {
			s.AggregatedDiscoveryGroupManager.AddGroupVersion(
				groupVersion.Group,
				apidiscoveryv2.APIVersionDiscovery{
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
					Version:   groupVersion.Version,
					Resources: discoveryAPIResources,
				},
			)
		} else {
			// legacy 资源只有一个组版本，优先级可以默认为 0。
			s.AggregatedLegacyDiscoveryGroupManager.AddGroupVersion(
				groupVersion.Group,
				apidiscoveryv2.APIVersionDiscovery{
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
					Version:   groupVersion.Version,
					Resources: discoveryAPIResources,
				},
			)
		}

	}

	// 存储随服务器关闭一起销毁；注册为 destroy 函数而非立即执行。
	s.RegisterDestroyFunc(apiGroupInfo.destroyStorage)

	if s.FeatureGate.Enabled(features.StorageVersionAPI) &&
		s.FeatureGate.Enabled(features.APIServerIdentity) {
		// API 安装发生在我们开始监听处理器之前，
		// 因此在这里注册 ResourceInfos 是安全的。在目标资源的存储版本
		// 被更新之前，处理器会阻塞写请求。
		s.StorageVersionManager.AddResourceInfo(resourceInfos...)
	}

	return nil
}

// InstallLegacyAPIGroup 在 API 中暴露给定的 legacy api 组。
// 传入本函数的 <apiGroupInfo> 不应再在其他地方使用，
// 因为其底层存储会在本服务器关闭时被销毁。
func (s *GenericAPIServer) InstallLegacyAPIGroup(apiPrefix string, apiGroupInfo *APIGroupInfo) error {
	// 只允许显式登记的旧前缀（通常是 /api），防止任意路径被当成核心组。
	if !s.legacyAPIGroupPrefixes.Has(apiPrefix) {
		return fmt.Errorf("%q is not in the allowed legacy API prefixes: %v", apiPrefix, s.legacyAPIGroupPrefixes.List())
	}

	openAPIModels, err := s.getOpenAPIModels(apiPrefix, apiGroupInfo)
	if err != nil {
		return fmt.Errorf("unable to get openapi models: %v", err)
	}

	if err := s.installAPIResources(apiPrefix, apiGroupInfo, openAPIModels); err != nil {
		return err
	}

	// 安装版本处理器。
	// 在 /<apiPrefix> 添加一个处理器以列举受支持的 api 版本。
	legacyRootAPIHandler := discovery.NewLegacyRootAPIHandler(s.discoveryAddresses, s.Serializer, apiPrefix)
	// legacy API 组不支持对等发现（peer-to-peer discovery）。
	wrapped := discoveryendpoint.WrapAggregatedDiscoveryToHandler(legacyRootAPIHandler, s.AggregatedLegacyDiscoveryGroupManager, s.AggregatedLegacyDiscoveryGroupManager)
	s.Handler.GoRestfulContainer.Add(wrapped.GenerateWebService("/api", metav1.APIVersions{}))
	s.registerStorageReadinessCheck("", apiGroupInfo)

	return nil
}

// InstallAPIGroups 在 API 中暴露给定的 api 组。
// 传入本函数的 <apiGroupInfos> 不应再在其他地方使用，
// 因为其底层存储会在本服务器关闭时被销毁。
func (s *GenericAPIServer) InstallAPIGroups(apiGroupInfos ...*APIGroupInfo) error {
	for _, apiGroupInfo := range apiGroupInfos {
		if len(apiGroupInfo.PrioritizedVersions) == 0 {
			return fmt.Errorf("no version priority set for %#v", *apiGroupInfo)
		}
		// 不要注册空的 group 或空的 version。那样做会把 /apis/ 错误地
		// 声明为某个错误实体返回的内容。
		// 在这里拦截这些问题，能让错误离它的来源更近
		if len(apiGroupInfo.PrioritizedVersions[0].Group) == 0 {
			return fmt.Errorf("cannot register handler with an empty group for %#v", *apiGroupInfo)
		}
		if len(apiGroupInfo.PrioritizedVersions[0].Version) == 0 {
			return fmt.Errorf("cannot register handler with an empty version for %#v", *apiGroupInfo)
		}
	}

	openAPIModels, err := s.getOpenAPIModels(APIGroupPrefix, apiGroupInfos...)
	if err != nil {
		return fmt.Errorf("unable to get openapi models: %v", err)
	}

	for _, apiGroupInfo := range apiGroupInfos {
		if err := s.installAPIResources(APIGroupPrefix, apiGroupInfo, openAPIModels); err != nil {
			return fmt.Errorf("unable to install api resources: %v", err)
		}

		// 设置发现信息
		// 安装版本处理器。
		// 在 /apis/<groupName> 添加处理器，以列举该组支持的所有版本。
		apiVersionsForDiscovery := []metav1.GroupVersionForDiscovery{}
		for _, groupVersion := range apiGroupInfo.PrioritizedVersions {
			// 检查配置，确保省略掉没有任何资源的版本
			if len(apiGroupInfo.VersionedResourcesStorageMap[groupVersion.Version]) == 0 {
				continue
			}
			apiVersionsForDiscovery = append(apiVersionsForDiscovery, metav1.GroupVersionForDiscovery{
				GroupVersion: groupVersion.String(),
				Version:      groupVersion.Version,
			})
		}
		// 首选版本始终是 PrioritizedVersions[0]，即使它没有资源（保持 API 契约稳定）。
		preferredVersionForDiscovery := metav1.GroupVersionForDiscovery{
			GroupVersion: apiGroupInfo.PrioritizedVersions[0].String(),
			Version:      apiGroupInfo.PrioritizedVersions[0].Version,
		}
		apiGroup := metav1.APIGroup{
			Name:             apiGroupInfo.PrioritizedVersions[0].Group,
			Versions:         apiVersionsForDiscovery,
			PreferredVersion: preferredVersionForDiscovery,
		}

		s.DiscoveryGroupManager.AddGroup(apiGroup)
		s.Handler.GoRestfulContainer.Add(discovery.NewAPIGroupHandler(s.Serializer, apiGroup).WebService())
		s.registerStorageReadinessCheck(apiGroupInfo.PrioritizedVersions[0].Group, apiGroupInfo)
	}
	return nil
}

// registerStorageReadinessCheck 为指定 APIGroup 下所有底层 storage 注册就绪检查；
// 仅注册实现了 rest.StorageWithReadiness 的存储，其他存储视为始终就绪。
func (s *GenericAPIServer) registerStorageReadinessCheck(groupName string, apiGroupInfo *APIGroupInfo) {
	for version, storageMap := range apiGroupInfo.VersionedResourcesStorageMap {
		for resource, storage := range storageMap {
			if withReadiness, ok := storage.(rest.StorageWithReadiness); ok {
				gvr := metav1.GroupVersionResource{
					Group:    groupName,
					Version:  version,
					Resource: resource,
				}
				s.StorageReadinessHook.RegisterStorage(gvr, withReadiness)
			}
		}
	}
}

// InstallAPIGroup 在 API 中暴露给定的 api 组。
// 传入本函数的 <apiGroupInfo> 不应再在其他地方使用，
// 因为其底层存储会在本服务器关闭时被销毁。
func (s *GenericAPIServer) InstallAPIGroup(apiGroupInfo *APIGroupInfo) error {
	return s.InstallAPIGroups(apiGroupInfo)
}

// getAPIGroupVersion 校验资源名并组装单版本的安装描述。
func (s *GenericAPIServer) getAPIGroupVersion(apiGroupInfo *APIGroupInfo, groupVersion schema.GroupVersion, apiPrefix string) (*genericapi.APIGroupVersion, error) {
	storage := make(map[string]rest.Storage)
	for k, v := range apiGroupInfo.VersionedResourcesStorageMap[groupVersion.Version] {
		// 资源名参与 URL 与 RBAC 规则，必须小写，避免大小写绕过授权。
		if strings.ToLower(k) != k {
			return nil, fmt.Errorf("resource names must be lowercase only, not %q", k)
		}
		storage[k] = v
	}
	version := s.newAPIGroupVersion(apiGroupInfo, groupVersion)
	version.Root = apiPrefix
	version.Storage = storage
	return version, nil
}

// newAPIGroupVersion 构造 APIGroupVersion，并把所有版本共享的 scheme/授权器等注入。
func (s *GenericAPIServer) newAPIGroupVersion(apiGroupInfo *APIGroupInfo, groupVersion schema.GroupVersion) *genericapi.APIGroupVersion {

	// 记录每个资源在该组内可用的全部版本，用于转换与发现。
	allServedVersionsByResource := map[string][]string{}
	for version, resourcesInVersion := range apiGroupInfo.VersionedResourcesStorageMap {
		for resource := range resourcesInVersion {
			if len(groupVersion.Group) == 0 {
				allServedVersionsByResource[resource] = append(allServedVersionsByResource[resource], version)
			} else {
				allServedVersionsByResource[resource] = append(allServedVersionsByResource[resource], fmt.Sprintf("%s/%s", groupVersion.Group, version))
			}
		}
	}

	return &genericapi.APIGroupVersion{
		GroupVersion:                groupVersion,
		AllServedVersionsByResource: allServedVersionsByResource,
		MetaGroupVersion:            apiGroupInfo.MetaGroupVersion,

		ParameterCodec:        apiGroupInfo.ParameterCodec,
		Serializer:            apiGroupInfo.NegotiatedSerializer,
		Creater:               apiGroupInfo.Scheme,
		Convertor:             apiGroupInfo.Scheme,
		ConvertabilityChecker: apiGroupInfo.Scheme,
		UnsafeConvertor:       runtime.UnsafeObjectConvertor(apiGroupInfo.Scheme),
		Defaulter:             apiGroupInfo.Scheme,
		Typer:                 apiGroupInfo.Scheme,
		Namer:                 runtime.Namer(meta.NewAccessor()),

		EquivalentResourceRegistry: s.EquivalentResourceRegistry,

		Admit:             s.admissionControl,
		MinRequestTimeout: s.minRequestTimeout,
		Authorizer:        s.Authorizer,
	}
}

// NewDefaultAPIGroupInfo 返回一个用“常规”值预填的 APIGroupInfo，
// 对外暴露以便其他包更容易地组合使用
func NewDefaultAPIGroupInfo(group string, scheme *runtime.Scheme, parameterCodec runtime.ParameterCodec, codecs serializer.CodecFactory) APIGroupInfo {
	// 默认启用流式集合编码，降低大列表的峰值内存。
	opts := []serializer.CodecFactoryOptionsMutator{
		serializer.WithStreamingCollectionEncodingToJSON(),
		serializer.WithStreamingCollectionEncodingToProtobuf(),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		opts = append(opts, serializer.WithSerializer(cbor.NewSerializerInfo))
	}
	// 仅在需要附加选项时重建 codec factory，否则复用调用方传入的实例。
	if len(opts) != 0 {
		codecs = serializer.NewCodecFactory(scheme, opts...)
	}
	return APIGroupInfo{
		PrioritizedVersions:          scheme.PrioritizedVersionsForGroup(group),
		VersionedResourcesStorageMap: map[string]map[string]rest.Storage{},
		// TODO 去掉这里的硬编码。此前是硬编码的，但我们需要重新评估
		OptionsExternalVersion: &schema.GroupVersion{Version: "v1"},
		Scheme:                 scheme,
		ParameterCodec:         parameterCodec,
		NegotiatedSerializer:   codecs,
	}
}

// getOpenAPIModels 是一个私有方法，用于获取 OpenAPI 模型
func (s *GenericAPIServer) getOpenAPIModels(apiPrefix string, apiGroupInfos ...*APIGroupInfo) (managedfields.TypeConverter, error) {
	if s.openAPIV3Config == nil {
		// SSA 已 GA，创建模型必须设置 OpenAPI 配置。
		return nil, errors.New("OpenAPIV3 config must not be nil")
	}
	pathsToIgnore := openapiutil.NewTrie(s.openAPIV3Config.IgnorePrefixes)
	resourceNames := make([]string, 0)
	for _, apiGroupInfo := range apiGroupInfos {
		groupResources, err := getResourceNamesForGroup(apiPrefix, apiGroupInfo, pathsToIgnore)
		if err != nil {
			return nil, err
		}
		resourceNames = append(resourceNames, groupResources...)
	}

	// 为这些资源构建 openapi 定义，并将其转换为 proto 模型
	openAPISpec, err := openapibuilder3.BuildOpenAPIDefinitionsForResources(s.openAPIV3Config, resourceNames...)
	if err != nil {
		return nil, err
	}
	// 同一批安装的组共享一份 OpenAPI spec，保证跨组类型引用一致。
	for _, apiGroupInfo := range apiGroupInfos {
		apiGroupInfo.StaticOpenAPISpec = openAPISpec
	}

	typeConverter, err := managedfields.NewTypeConverter(openAPISpec, false)
	if err != nil {
		return nil, err
	}

	return typeConverter, nil
}

// getResourceNamesForGroup 是一个私有方法，用于获取 api 组中要构建的每个资源的规范名称
func getResourceNamesForGroup(apiPrefix string, apiGroupInfo *APIGroupInfo, pathsToIgnore openapiutil.Trie) ([]string, error) {
	// 获取本 api 组中需要构建的每个资源的规范名称
	resourceNames := make([]string, 0)
	for _, groupVersion := range apiGroupInfo.PrioritizedVersions {
		for resource, storage := range apiGroupInfo.VersionedResourcesStorageMap[groupVersion.Version] {
			path := gpath.Join(apiPrefix, groupVersion.Group, groupVersion.Version, resource)
			if !pathsToIgnore.HasPrefix(path) {
				kind, err := genericapi.GetResourceKind(groupVersion, storage, apiGroupInfo.Scheme)
				if err != nil {
					return nil, err
				}
				sampleObject, err := apiGroupInfo.Scheme.New(kind)
				if err != nil {
					return nil, err
				}
				name := openapiutil.GetCanonicalTypeName(sampleObject)
				resourceNames = append(resourceNames, name)
			}
		}
	}

	return resourceNames, nil
}
