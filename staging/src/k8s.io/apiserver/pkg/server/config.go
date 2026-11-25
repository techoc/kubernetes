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

package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base32"
	"fmt"
	"net"
	"net/http"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/cryptobyte"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilwaitgroup "k8s.io/apimachinery/pkg/util/waitgroup"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	authenticatorunion "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/endpoints/filterlatency"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/filters/impersonation"
	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/routine"
	serverstore "k8s.io/apiserver/pkg/server/storage"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/informers"
	restclient "k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/utils/clock"
	utilsnet "k8s.io/utils/net"

	// 安装 apis
	_ "k8s.io/apiserver/pkg/apis/apiserver/install"
)

// hostnameFunc 用于设置本 apiserver 的 hostnameFunc。
// 仅供测试使用，用来模拟存在多个 apiserver 的场景。
// 这种情况下我们需要保证 apiserver ID 唯一，而它是 hostnameFunc 的哈希值。
var (
	hostnameFunc = os.Hostname
)

const (
	// DefaultLegacyAPIPrefix 是 legacy API 所在的路径前缀。
	DefaultLegacyAPIPrefix = "/api"

	// APIGroupPrefix 是非 legacy API group 所在的路径前缀。
	APIGroupPrefix = "/apis"
)

// Config 是用于配置 GenericAPIServer 的结构体。
// 其成员大致按照对组装者（composers）的重要程度排序。
//
// Config 由各聚合 apiserver 在启动时填充；Complete 会补齐可推导字段，之后应视为只读。
type Config struct {
	// SecureServing 是提供 https 服务所必需的
	SecureServing *SecureServingInfo

	// Authentication 是认证（authentication）相关的配置
	Authentication AuthenticationInfo

	// Authorization 是授权（authorization）相关的配置
	Authorization AuthorizationInfo

	// LoopbackClientConfig 是到 API 服务器的特权回环连接的客户端配置，GenericAPIServer 上的 PostStartHooks 必须依赖它才能正常工作。
	// 回环客户端使用特权令牌，仅用于服务器内部组件（如 post-start hook）调用自身。
	// TODO: 一旦不安全服务（insecure serving）被移除，就把这里移入 SecureServing(WithLoopback)
	LoopbackClientConfig *restclient.Config

	// EgressSelector 提供用于拨出（outbound）连接的查找机制。
	// 它依据启动时读取的 EgressSelectorConfiguration 来决定如何拨号。
	EgressSelector *egressselector.EgressSelector

	// RuleResolver 是获取“在给定命名空间下作用于给定用户”的规则列表所必需的
	RuleResolver authorizer.RuleResolver
	// AdmissionControl 对给定请求（包括请求内容）做深入检查，
	// 以设置取值并判断其是否被允许
	AdmissionControl      admission.Interface
	CorsAllowedOriginList []string
	HSTSDirectives        []string
	// FlowControl 非 nil 时为请求处理提供优先级与公平性（APF）；配置后启用 APF，否则退化为固定上限的 max-in-flight 限流（见 DefaultBuildHandlerChain）。
	FlowControl utilflowcontrol.Interface

	EnableIndex     bool
	EnableProfiling bool
	DebugSocketPath string
	EnableDiscovery bool

	// 需要先启用通用 profiling（EnableProfiling）
	EnableContentionProfiling bool
	EnableMetrics             bool

	DisabledPostStartHooks sets.String
	// 该 map 中值的 done 字段会被忽略。
	PostStartHooks map[string]PostStartHookConfigEntry

	// EffectiveVersion 依据 API/特性的生命周期（lifecycle）
	// 决定哪些 apis 和 features 可用。
	EffectiveVersion basecompatibility.EffectiveVersion
	// EmulationForwardCompatible 是一个隐式启用所有在模拟版本之后引入、且优先级高于模拟版本下已启用的同组同资源 API 的选项。
	// 为 true 时，所有优先级高于模拟版本下已启用的同组同资源 API（beta+）的 API 都会被安装。
	// 当控制器实现已在二进制版本中迁移到更新的 API 版本、即使设置了模拟版本也要继续使用新版本时，需要开启该选项。
	// 仅对 GA/Beta API 生效；alpha API 不受模拟版本兼容逻辑影响。
	EmulationForwardCompatible bool
	// RuntimeConfigEmulationForwardCompatible 是通过 runtime-config 显式启用特定在模拟版本之后引入的 API 的选项。
	// 为 true 时，--runtime-config 中按 group/version 启用的 API 即使晚于模拟版本引入也会被安装；为 false 时，若 --runtime-config 显式启用了此类晚于模拟版本的 API 则会报错。
	// 聚合型取值（如 api/all、api/ga、api/beta）不受该开关影响，只启用当前模拟版本已有的 API。
	RuntimeConfigEmulationForwardCompatible bool
	// FeatureGate 是把你持有的 feature gate 传递进来的方式。
	FeatureGate featuregate.FeatureGate
	// AuditBackend 是审计事件发送的目标。
	AuditBackend audit.Backend
	// AuditPolicyRuleEvaluator 决定是否审计记录一次请求、以及如何记录。
	AuditPolicyRuleEvaluator audit.PolicyRuleEvaluator
	// ExternalAddress 是对外（公网）URL（例如 Swagger）所使用的宿主名。
	// 缺省时会取一个基于 secure serving 信息和可用 IPv4 地址的值。
	ExternalAddress string

	// TracerProvider 可以提供一个 tracer，用于为分布式追踪记录 span。
	TracerProvider tracing.TracerProvider

	//===========================================================================
	// 这些字段你大概不关心改不改
	//===========================================================================

	// BuildHandlerChainFunc 允许你通过装饰 apiHandler 来构建自定义的 handler 链。
	BuildHandlerChainFunc func(apiHandler http.Handler, c *Config) (secure http.Handler)
	// NonLongRunningRequestWaitGroup 让你在服务器关闭期间等待
	// 所有与非长时运行（non long-running）请求
	// 相关的链上 handler 执行完毕。
	NonLongRunningRequestWaitGroup *utilwaitgroup.SafeWaitGroup
	// WatchRequestWaitGroup 让我们在服务器关闭期间等待
	// 所有与处于活动状态的 watch 请求相关的
	// 链上 handler 执行完毕。
	WatchRequestWaitGroup *utilwaitgroup.RateLimitedSafeWaitGroup
	// DiscoveryAddresses 用于构造传给 discovery 的 IP。若为 nil，则
	// 始终上报 ExternalAddress
	DiscoveryAddresses discovery.Addresses
	// 默认的 healthz 检查集合。后续还可以通过 AddHealthChecks 动态添加更多。
	HealthzChecks []healthz.HealthChecker
	// 默认的 livez 检查集合。后续还可以通过 AddHealthChecks 动态添加更多。
	LivezChecks []healthz.HealthChecker
	// 默认仅用于 readyz 的检查集合。后续还可以通过 AddReadyzChecks 动态添加更多。
	ReadyzChecks []healthz.HealthChecker
	Flagz        flagz.Reader
	// LegacyAPIGroupPrefixes 用于设置授权所用的 URL 解析，以及校验发往
	// InstallLegacyAPIGroup 的请求。新的 API 服务器一般根本没有 legacy group。
	LegacyAPIGroupPrefixes sets.String
	// RequestInfoResolver 用于依据请求 URL 赋值那些属性（被 admission 和 authorization 使用）。
	// 类似 kubelet 的使用场景可能需要自定义它。
	RequestInfoResolver apirequest.RequestInfoResolver
	// Serializer 是必需的，它提供把对象序列化/反序列化并与线上格式互转的接口。
	// 默认值（api.Codecs）通常就够用了。
	Serializer runtime.NegotiatedSerializer
	// OpenAPIConfig 将用于生成 OpenAPI spec。默认为 nil。想要“可用”的默认值请使用 DefaultOpenAPIConfig。
	OpenAPIConfig *openapicommon.Config
	// OpenAPIV3Config 将用于生成 OpenAPI V3 spec。默认为 nil。想要“可用”的默认值请使用 DefaultOpenAPIV3Config。
	OpenAPIV3Config *openapicommon.OpenAPIV3Config
	// SkipOpenAPIInstallation 为 true 时，不会安装 OpenAPI handler。
	SkipOpenAPIInstallation bool

	// ResourceTransformers 用于在资源与 etcd 之间来回转换，例如做加密。
	ResourceTransformers storagevalue.ResourceTransformers
	// RESTOptionsGetter 用于通过通用 registry 构造 RESTStorage 类型。
	RESTOptionsGetter genericregistry.RESTOptionsGetter

	// RequestTimeout 指定后，除匹配 LongRunningFunc 谓词的请求外，所有请求超过该时长即超时。
	// 超时通过上下文截止实现，handler 可继续在后台运行，但客户端已收到超时响应。
	RequestTimeout time.Duration
	// MinRequestTimeout 指定后，watch 等长时请求会被分配一个介于该值与其两倍之间的随机超时（单位：秒）；是否遵循该超时由请求处理器自行决定。
	// 随机化可避免大量 watch 同时超时重连，形成惊群。
	MinRequestTimeout int

	// StorageInitializationTimeout 定义在宣告 apiserver 就绪之前，
	// 等待存储初始化的最长时间。
	StorageInitializationTimeout time.Duration

	// 它表示 apiserver 完成启动流程并变为健康状态所应当花费的最长时间。
	// 从 apiserver 启动时刻起，到该时长耗尽之前，/livez 会认为
	// 尚未完成的 post-start hook 会成功完成，
	// 因而返回 true。
	LivezGracePeriod time.Duration
	// ShutdownDelayDuration 允许推迟关闭一段时间，例如直到指向本 API 服务器的 endpoints 在所有节点上收敛。在此期间 API 服务器持续服务，/healthz 返回 200，但 /readyz 返回失败。
	// 该延迟让负载均衡器有时间把流量切走，减少滚动更新时的连接错误。
	ShutdownDelayDuration time.Duration

	// 限制一次 json patch 中所有 "copy" 操作可能造成的
	// 总体积增长上限。
	// 它作用于该二进制中所有应用 json patch 的地方。
	JSONPatchMaxCopyBytes int64
	// MaxRequestBodyBytes 限制写请求可接受并解码的请求体大小；0 表示不限制。
	// 默认 3MB，用于防止超大请求耗尽内存；etcd 侧推荐的请求上限为 1.5MB。
	MaxRequestBodyBytes int64
	// MaxRequestsInFlight 是并行处理的非长时运行请求的最大数量，超出的
	// 请求必须等待。它只适用于非变更（non-mutating）请求。
	MaxRequestsInFlight int
	// MaxMutatingRequestsInFlight 是并行处理的变更（mutating）请求的最大数量，
	// 超出的请求必须等待。
	MaxMutatingRequestsInFlight int
	// 对于长时运行的 http 请求路径，该谓词返回 true
	LongRunningFunc apirequest.LongRunningRequestCheck

	// GoawayChance 是向 HTTP/2 客户端发送 GOAWAY 的概率：收到 GOAWAY 后客户端在途请求不受影响，新请求会新建 TCP 连接，从而触发负载均衡到后端另一台服务器。
	// 默认为 0，表示从不发送 GOAWAY；上限为 0.02，以免压垮 apiserver。概率性 GOAWAY 用于推动客户端重新建连，从而在负载均衡后端之间再平衡。
	GoawayChance float64

	// MergedResourceConfig 指明哪些 groupVersion 被启用，以及其中的资源是启用还是禁用。
	// 它由 genericapiserver 的 defaultAPIResourceConfig 与从命令行 flag 解析出的配置合并而成。
	// 如果 flag 中没有指定任何配置，genericapiserver 只会启用 defaultAPIResourceConfig。
	MergedResourceConfig *serverstore.ResourceConfig

	// lifecycleSignals 提供对 API 服务器生命周期中各个信号的访问。
	// 该字段有意声明为私有，绝不应被外部覆盖；生命周期信号驱动优雅关闭的各个阶段。
	lifecycleSignals lifecycleSignals

	// StorageObjectCountTracker 用于统计存储中每种资源的对象总数，
	// 这样我们就能估算进入请求的宽度（width）。
	StorageObjectCountTracker flowcontrolrequest.StorageObjectCountTracker

	// ShutdownSendRetryAfter 决定 API 服务器优雅终止期间何时启动 HTTP Server 关闭：为 true 时，先排空在途的非长时请求再发起关闭；为 false 时，ShutdownDelayDuration 一到就立即发起关闭。
	// 若开启，ShutdownDelayDuration 到期后新到的请求将被拒绝，并返回 429 状态码和 'Retry-After' 响应。
	// 开启后客户端能明确感知"正在关闭"，并按照 Retry-After 重试到其他实例。
	ShutdownSendRetryAfter bool

	//===========================================================================
	// 以下这些值是计划移除的对象
	//===========================================================================

	// PublicAddress 是集群成员（kubelet、kube-proxy、Service 等）
	// 访问 GenericAPIServer 时使用的 IP 地址。
	// 若为 nil 或 0.0.0.0，则使用主机的默认网卡。
	PublicAddress net.IP

	// EquivalentResourceRegistry 提供“与给定资源等价的资源”以及“给定资源对应的 kind”
	// 相关信息。随着资源被安装，它们会在此处注册。
	EquivalentResourceRegistry runtime.EquivalentResourceRegistry

	// APIServerID 是本 API 服务器的 ID
	APIServerID string

	// StorageVersionManager 保存本服务器安装的 API 资源的存储版本。
	StorageVersionManager storageversion.Manager

	// AggregatedDiscoveryGroupManager 以聚合形式提供 /apis 服务。
	AggregatedDiscoveryGroupManager discoveryendpoint.ResourceManager
	// ShutdownWatchTerminationGracePeriod 为正值时，表示 API 服务器等待所有活跃 watch 请求排空的最长时长；宽限期一到，服务器便不再等待在途的活跃 watch，直接继续优雅关闭流程的下一步。
	// 设置为正值后，API 服务器会跟踪在途活跃 watch 请求的数量，并在关闭时最多等待指定时长，让这些活跃 watch 请求在限速生效的情况下排空。
	// 默认值为 0，表示 API 服务器既不跟踪在途活跃 watch 请求，也不等待它们排空，以保持向后兼容。
	// 该宽限期与其他宽限期相互独立，不会被任何其他宽限期覆盖。
	// 为 0 时保持旧行为：不跟踪、不等待活跃 watch；正值会启用限速排空。
	ShutdownWatchTerminationGracePeriod time.Duration
}

// RecommendedConfig 在 Config 之上附带推荐选项创建的共享 informer 工厂与客户端配置。
type RecommendedConfig struct {
	Config

	// SharedInformerFactory 为 Kubernetes 资源提供共享 informer。该值由
	// RecommendedOptions.ApplyTo 调用的 RecommendedOptions.CoreAPI.ApplyTo 设置。
	// 默认使用集群内（in-cluster）客户端配置，或使用 kubeconfig 命令行 flag 指定的 kubeconfig。
	SharedInformerFactory informers.SharedInformerFactory

	// ClientConfig 保存 Kubernetes 客户端配置。
	// 该值由 RecommendedOptions.ApplyTo 调用的 RecommendedOptions.CoreAPI.ApplyTo 设置。
	// 默认使用集群内（in-cluster）客户端配置。
	ClientConfig *restclient.Config
}

// SecureServingInfo 描述安全（HTTPS）监听的网络与 TLS 参数。
type SecureServingInfo struct {
	// Listener 是安全服务的网络监听器。
	Listener net.Listener

	// Cert 是主服务器证书，当 SNI 不匹配时使用。Cert 必须非 nil，
	// 并且允许同时出现在 SNICerts 中。
	Cert dynamiccertificates.CertKeyContentProvider

	// SNICerts 是用于 SNI 的 TLS 证书。
	SNICerts []dynamiccertificates.SNICertKeyContentProvider

	// ClientCA 是证书集合，包含你会认可的所有签发者，用于校验进来的客户端证书
	ClientCA dynamiccertificates.CAContentProvider

	// MinTLSVersion 可以选择性地覆盖所支持的最小 TLS 版本。
	// 取值来自 tls 包的常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	MinTLSVersion uint16

	// CipherSuites 可以选择性地覆盖服务器允许的加密套件列表。
	// 取值来自 tls 包的常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	CipherSuites []uint16

	// CurvePreferences 可以选择性地指定服务器允许的密钥交换机制集合。
	// 列表顺序会被忽略，Go 会按照内部偏好顺序
	// 从该列表中挑选密钥交换机制。
	// 若为空，则使用 Go 的默认曲线。
	// 取值来自 Go crypto/tls 的 CurveID 常量（https://golang.org/pkg/crypto/tls/#CurveID）。
	CurvePreferences []tls.CurveID

	// HTTP2MaxStreamsPerConnection 是 api server 对每个客户端施加的上限。
	// 取值为零表示使用 Go 的 HTTP/2 支持所提供的默认值。
	HTTP2MaxStreamsPerConnection int

	// DisableHTTP2 表示不应启用 http2。
	DisableHTTP2 bool
}

// AuthenticationInfo 汇总认证所需的认证器、受众标识与请求头配置。
type AuthenticationInfo struct {
	// APIAudiences 是 API 对外声明自身身份的标识列表，某些
	// 认证器用它校验绑定了受众（audience）的凭据。
	APIAudiences authenticator.Audiences
	// Authenticator 判定发起请求的是哪个主体
	Authenticator authenticator.Request

	RequestHeaderConfig *authenticatorfactory.RequestHeaderConfig
}

// AuthorizationInfo 汇总授权所需的授权器。
type AuthorizationInfo struct {
	// Authorizer 仅依据 RequestURI 判断该主体
	// 是否被允许发起本次请求
	Authorizer authorizer.Authorizer
}

func init() {
	utilruntime.Must(features.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewConfig 返回一个带有默认取值的 Config 结构体
func NewConfig(codecs serializer.CodecFactory) *Config {
	defaultHealthChecks := []healthz.HealthChecker{healthz.PingHealthz, healthz.LogHealthz}
	var id string
	// 启用 APIServerIdentity 时生成稳定的实例 ID，用于 lease 等机制区分不同 apiserver 实例。
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		hostname, err := hostnameFunc()
		if err != nil {
			klog.Fatalf("error getting hostname for apiserver identity: %v", err)
		}

		// 由于哈希需要在每个 kube-apiserver 和各聚合 apiserver 之间保持唯一，
		// 因此身份所用的哈希应当同时包含 hostname 和 identity 取值。
		// TODO: 一旦 generic apiserver 接管了 apiserver identity lease 控制器的
		// post start hook，就把 identity 取值作为参数接收进来。
		// 长度前缀编码避免 hostname 与固定字符串拼接产生歧义。
		b := cryptobyte.NewBuilder(nil)
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte(hostname))
		})
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte("kube-apiserver"))
		})
		hashData, err := b.Bytes()
		if err != nil {
			klog.Fatalf("error building hash data for apiserver identity: %v", err)
		}

		hash := sha256.Sum256(hashData)
		id = "apiserver-" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash[:16]))
	}
	lifecycleSignals := newLifecycleSignals()

	return &Config{
		Serializer:                     codecs,
		BuildHandlerChainFunc:          DefaultBuildHandlerChain,
		NonLongRunningRequestWaitGroup: new(utilwaitgroup.SafeWaitGroup),
		WatchRequestWaitGroup:          &utilwaitgroup.RateLimitedSafeWaitGroup{},
		LegacyAPIGroupPrefixes:         sets.NewString(DefaultLegacyAPIPrefix),
		DisabledPostStartHooks:         sets.NewString(),
		PostStartHooks:                 map[string]PostStartHookConfigEntry{},
		HealthzChecks:                  append([]healthz.HealthChecker{}, defaultHealthChecks...),
		ReadyzChecks:                   append([]healthz.HealthChecker{}, defaultHealthChecks...),
		LivezChecks:                    append([]healthz.HealthChecker{}, defaultHealthChecks...),
		EnableIndex:                    true,
		EnableDiscovery:                true,
		EnableProfiling:                true,
		DebugSocketPath:                "",
		EnableMetrics:                  true,
		MaxRequestsInFlight:            400,
		MaxMutatingRequestsInFlight:    200,
		RequestTimeout:                 time.Duration(60) * time.Second,
		MinRequestTimeout:              1800,
		StorageInitializationTimeout:   time.Minute,
		LivezGracePeriod:               time.Duration(0),
		ShutdownDelayDuration:          time.Duration(0),
		// 1.5MB 是 etcd 服务端应当接受的客户端请求大小默认字节数，
		// 参见
		// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56。
		// 请求体可能以 json 编码，持久化到 etcd 时会被转换为 proto，
		// 因此我们把 json patch 中所有 "copy" 操作可能造成的最大
		// 体积增长放宽到 2 倍。
		JSONPatchMaxCopyBytes: int64(3 * 1024 * 1024),
		// 1.5MB 是推荐 etcd 服务端接受的客户端请求大小字节数，
		// 参见
		// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56。
		// 请求体可能以 json 编码，持久化到 etcd 时会被转换为 proto，
		// 因此我们把写请求可接受并解码的最大请求体大小放宽到 2 倍。
		// 如果修改了该常量，k8s.io/apiserver/pkg/cel/limits.go 中的
		// DefaultMaxRequestSizeBytes 也应当同步修改以反映新取值，
		// 除非两者已经以某种方式关联在一起。
		MaxRequestBodyBytes: int64(3 * 1024 * 1024),

		// 默认把 watch 视为长时运行操作
		// 通用 API 服务器没有固有的长时运行子资源
		LongRunningFunc:                     genericfilters.BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString()),
		lifecycleSignals:                    lifecycleSignals,
		StorageObjectCountTracker:           flowcontrolrequest.NewStorageObjectCountTracker(),
		ShutdownWatchTerminationGracePeriod: time.Duration(0),

		APIServerID:           id,
		StorageVersionManager: storageversion.NewDefaultManager(),
		TracerProvider:        tracing.NewNoopTracerProvider(),
	}
}

// NewRecommendedConfig 返回一个带有默认取值的 RecommendedConfig 结构体
func NewRecommendedConfig(codecs serializer.CodecFactory) *RecommendedConfig {
	return &RecommendedConfig{
		Config: *NewConfig(codecs),
	}
}

// DefaultOpenAPIConfig 提供用于构建 OpenAPI V2 spec 的默认 OpenAPIConfig
func DefaultOpenAPIConfig(getDefinitions openapicommon.GetOpenAPIDefinitions, defNamer *apiopenapi.DefinitionNamer) *openapicommon.Config {
	return &openapicommon.Config{
		ProtocolList:   []string{"https"},
		IgnorePrefixes: []string{},
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title: "Generic API Server",
			},
		},
		DefaultResponse: &spec.Response{
			ResponseProps: spec.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetOperationIDAndTags: apiopenapi.GetOperationIDAndTags,
		GetDefinitionName:     defNamer.GetDefinitionName,
		GetDefinitions:        getDefinitions,
	}
}

// DefaultOpenAPIV3Config 提供用于构建 OpenAPI V3 spec 的默认 OpenAPIV3Config
func DefaultOpenAPIV3Config(getDefinitions openapicommon.GetOpenAPIDefinitions, defNamer *apiopenapi.DefinitionNamer) *openapicommon.OpenAPIV3Config {
	defaultConfig := &openapicommon.OpenAPIV3Config{
		IgnorePrefixes: []string{},
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title: "Generic API Server",
			},
		},
		DefaultResponse: &spec3.Response{
			ResponseProps: spec3.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetOperationIDAndTags: apiopenapi.GetOperationIDAndTags,
		GetDefinitionName:     defNamer.GetDefinitionName,
		GetDefinitions:        getDefinitions,
	}
	// V3 的定义引用需要生成 #/components/schemas/ 形式，定义名中的特殊字符必须转义。
	defaultConfig.Definitions = getDefinitions(func(name string) spec.Ref {
		defName, _ := defaultConfig.GetDefinitionName(name)
		return spec.MustCreateRef("#/components/schemas/" + openapicommon.EscapeJsonPointer(defName))
	})

	return defaultConfig
}

// ApplyClientCert 把客户端 CA 追加到安全监听配置中；
// 已有 CA 时使用联合 provider，保留双方签发的证书都可信。
func (c *AuthenticationInfo) ApplyClientCert(clientCA dynamiccertificates.CAContentProvider, servingInfo *SecureServingInfo) error {
	if servingInfo == nil {
		return nil
	}
	if clientCA == nil {
		return nil
	}
	if servingInfo.ClientCA == nil {
		servingInfo.ClientCA = clientCA
		return nil
	}

	servingInfo.ClientCA = dynamiccertificates.NewUnionCAContentProvider(servingInfo.ClientCA, clientCA)
	return nil
}

// completedConfig 由 Complete 生成，补充了完成阶段才能确定的字段。
type completedConfig struct {
	*Config

	//===========================================================================
	// 以下这些值在完成（completion）阶段被填充
	//===========================================================================

	// SharedInformerFactory 为资源提供共享 informer
	SharedInformerFactory informers.SharedInformerFactory
}

// CompletedConfig 把 completedConfig 藏在私有指针后，保证只能由本包生成，
// 调用方无法手工构造一个未完成初始化的配置。
type CompletedConfig struct {
	// 嵌入一个私有指针，该指针无法在本包之外实例化。
	*completedConfig
}

// AddHealthChecks 将健康检查同时注册到 healthz/livez/readyz 三个端点。
// 应优先使用本方法添加健康检查，除非明确只想把某个检查加到特定的健康端点，才直接向 config 添加 healthChecks。
func (c *Config) AddHealthChecks(healthChecks ...healthz.HealthChecker) {
	c.HealthzChecks = append(c.HealthzChecks, healthChecks...)
	c.LivezChecks = append(c.LivezChecks, healthChecks...)
	c.ReadyzChecks = append(c.ReadyzChecks, healthChecks...)
}

// AddHealthzChecks 把给定的健康检查加入我们的配置，
// 由所配置的 apiserver 的 healthz 端点暴露。
func (c *Config) AddHealthzChecks(healthChecks ...healthz.HealthChecker) {
	c.HealthzChecks = append(c.HealthzChecks, healthChecks...)
}

// AddLivezChecks 把给定的健康检查加入我们的配置，
// 由所配置的 apiserver 的 livez 端点暴露。
func (c *Config) AddLivezChecks(healthChecks ...healthz.HealthChecker) {
	c.LivezChecks = append(c.LivezChecks, healthChecks...)
}

// AddReadyzChecks 把健康检查加入我们的配置，由所配置的
// apiserver 的 readyz 端点暴露。
func (c *Config) AddReadyzChecks(healthChecks ...healthz.HealthChecker) {
	c.ReadyzChecks = append(c.ReadyzChecks, healthChecks...)
}

// AddPostStartHook 允许你添加一个 PostStartHook，它稍后会在 New 调用中被加入服务器本身。
// 名称冲突会导致错误。
func (c *Config) AddPostStartHook(name string, hook PostStartHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	if hook == nil {
		return fmt.Errorf("hook func may not be nil: %q", name)
	}
	// 显式禁用的 hook 静默跳过，便于下游通过配置裁剪默认行为。
	if c.DisabledPostStartHooks.Has(name) {
		klog.V(1).Infof("skipping %q because it was explicitly disabled", name)
		return nil
	}

	if postStartHook, exists := c.PostStartHooks[name]; exists {
		// 这属于编程错误，但可能难以排查：记录首次注册的调用栈，重名时直接指出来源，省去二分排查。
		return fmt.Errorf("unable to add %q because it was already registered by: %s", name, postStartHook.originatingStack)
	}
	c.PostStartHooks[name] = PostStartHookConfigEntry{hook: hook, originatingStack: string(debug.Stack())}

	return nil
}

// AddPostStartHookOrDie 允许你添加一个 PostStartHook，失败时直接退出进程。
func (c *Config) AddPostStartHookOrDie(name string, hook PostStartHookFunc) {
	if err := c.AddPostStartHook(name, hook); err != nil {
		klog.Fatalf("Error registering PostStartHook %q: %v", name, err)
	}
}

// completeOpenAPI 填充 OpenAPI V2 的默认安全定义、公共响应与版本信息。
func completeOpenAPI(config *openapicommon.Config, version *version.Version) {
	if config == nil {
		return
	}
	if config.SecurityDefinitions != nil {
		// 设置 OpenAPI security：目前所有 API 使用相同的认证方式；排序后生成 DefaultSecurity，保证输出稳定、便于 diff。
		config.DefaultSecurity = []map[string][]string{}
		keys := []string{}
		for k := range *config.SecurityDefinitions {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			config.DefaultSecurity = append(config.DefaultSecurity, map[string][]string{k: {}})
		}
		if config.CommonResponses == nil {
			config.CommonResponses = map[int]spec.Response{}
		}
		if _, exists := config.CommonResponses[http.StatusUnauthorized]; !exists {
			config.CommonResponses[http.StatusUnauthorized] = spec.Response{
				ResponseProps: spec.ResponseProps{
					Description: "Unauthorized",
				},
			}
		}
	}
	// 如果不是手工设置的，就要保证把 info 以及 info.version 填上
	if config.Info == nil {
		config.Info = &spec.Info{}
	}
	if config.Info.Version == "" {
		if version != nil {
			// 去掉版本号中的 "-alpha.1" 等后缀，只保留 major.minor。
			config.Info.Version = strings.Split(version.String(), "-")[0]
		} else {
			config.Info.Version = "unversioned"
		}
	}
}

// completeOpenAPIV3 与 completeOpenAPI 对应，但使用 OpenAPI V3 的 schema 类型。
func completeOpenAPIV3(config *openapicommon.OpenAPIV3Config, version *version.Version) {
	if config == nil {
		return
	}
	if config.SecuritySchemes != nil {
		// 设置 OpenAPI security：目前所有 API 使用相同的认证方式。
		config.DefaultSecurity = []map[string][]string{}
		keys := []string{}
		for k := range config.SecuritySchemes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			config.DefaultSecurity = append(config.DefaultSecurity, map[string][]string{k: {}})
		}
		if config.CommonResponses == nil {
			config.CommonResponses = map[int]*spec3.Response{}
		}
		if _, exists := config.CommonResponses[http.StatusUnauthorized]; !exists {
			config.CommonResponses[http.StatusUnauthorized] = &spec3.Response{
				ResponseProps: spec3.ResponseProps{
					Description: "Unauthorized",
				},
			}
		}
	}
	// 如果不是手工设置的，就要保证把 info 以及 info.version 填上
	if config.Info == nil {
		config.Info = &spec.Info{}
	}
	if config.Info.Version == "" {
		if version != nil {
			config.Info.Version = strings.Split(version.String(), "-")[0]
		} else {
			config.Info.Version = "unversioned"
		}
	}
}

// DrainedNotify 返回表示 genericapiserver 在关闭过程中已排空在途请求的生命周期信号，
// 用于等待所有在途请求处理完成后再关闭外部依赖（如 etcd 客户端）。
func (c *Config) DrainedNotify() <-chan struct{} {
	return c.lifecycleSignals.InFlightRequestsDrained.Signaled()
}

// ShutdownInitiated 返回一个生命周期信号，表示 apiserver 已经发起关闭流程。
func (c *Config) ShutdownInitiatedNotify() <-chan struct{} {
	return c.lifecycleSignals.ShutdownInitiated.Signaled()
}

// Complete 会把那些未设置、但必须拥有有效数据且可以从其他字段推导出来的
// 字段补齐。如果你打算调用 `ApplyOptions`，请先调用它。该方法会修改接收者本身。
func (c *Config) Complete(informers informers.SharedInformerFactory) CompletedConfig {
	if c.FeatureGate == nil {
		c.FeatureGate = utilfeature.DefaultFeatureGate
	}
	if len(c.ExternalAddress) == 0 && c.PublicAddress != nil {
		c.ExternalAddress = c.PublicAddress.String()
	}

	// 外部地址缺少端口时从安全监听推断，否则生成的发现地址不可用。
	if _, _, err := net.SplitHostPort(c.ExternalAddress); err != nil {
		if c.SecureServing == nil {
			klog.Fatalf("cannot derive external address port without listening on a secure port.")
		}
		_, port, err := c.SecureServing.HostPort()
		if err != nil {
			klog.Fatalf("cannot derive external address from the secure port: %v", err)
		}
		c.ExternalAddress = net.JoinHostPort(c.ExternalAddress, strconv.Itoa(port))
	}
	completeOpenAPI(c.OpenAPIConfig, c.EffectiveVersion.EmulationVersion())
	completeOpenAPIV3(c.OpenAPIV3Config, c.EffectiveVersion.EmulationVersion())

	if c.DiscoveryAddresses == nil {
		c.DiscoveryAddresses = discovery.DefaultAddresses{DefaultAddress: c.ExternalAddress}
	}

	AuthorizeClientBearerToken(c.LoopbackClientConfig, &c.Authentication, &c.Authorization)

	if c.RequestInfoResolver == nil {
		c.RequestInfoResolver = NewRequestInfoResolver(c)
	}

	if c.EquivalentResourceRegistry == nil {
		if c.RESTOptionsGetter == nil {
			c.EquivalentResourceRegistry = runtime.NewEquivalentResourceRegistry()
		} else {
			// 有存储时用存储前缀作为等价资源标识，避免不同组下同名的资源互相混淆。
			c.EquivalentResourceRegistry = runtime.NewEquivalentResourceRegistryWithIdentity(func(groupResource schema.GroupResource) string {
				// 尽可能使用存储前缀作为键
				if opts, err := c.RESTOptionsGetter.GetRESTOptions(groupResource, nil); err == nil {
					return opts.ResourcePrefix
				}
				// 否则返回 ""，以使用默认的键（父 group/version 名）
				return ""
			})
		}
	}

	return CompletedConfig{&completedConfig{c, informers}}
}

// Complete 会把那些未设置、但必须拥有有效数据且可以从其他字段推导出来的
// 字段补齐。如果你打算调用 `ApplyOptions`，请先调用它。该方法会修改接收者本身。
//
// RecommendedConfig 复用内嵌 Config 的完成逻辑，并带上推荐选项创建的 informer 工厂。
func (c *RecommendedConfig) Complete() CompletedConfig {
	return c.Config.Complete(c.SharedInformerFactory)
}

var defaultAllowedMediaTypes = []string{
	runtime.ContentTypeJSON,
	runtime.ContentTypeYAML,
	runtime.ContentTypeProtobuf,
}

// New 创建一个新服务器，它在逻辑上把处理链与传入的服务器组合在一起。
// name 用于在日志中区分实例。handler 链尤其难以把握，因为它会开始委托（delegating）。
// delegationTarget 不得为 nil。
func (c completedConfig) New(name string, delegationTarget DelegationTarget) (*GenericAPIServer, error) {
	if c.Serializer == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.Serializer == nil")
	}
	// 只允许显式批准的媒体类型，防止序列化器意外暴露未审查的格式
	// （例如在特性门控关闭时仍接受 CBOR）。
	allowedMediaTypes := defaultAllowedMediaTypes
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		allowedMediaTypes = append(allowedMediaTypes, runtime.ContentTypeCBOR)
	}
	for _, info := range c.Serializer.SupportedMediaTypes() {
		var ok bool
		for _, mt := range allowedMediaTypes {
			if info.MediaType == mt {
				ok = true
				break
			}
		}
		if !ok {
			return nil, fmt.Errorf("refusing to create new apiserver %q with support for media type %q (allowed media types are: %s)", name, info.MediaType, strings.Join(allowedMediaTypes, ", "))
		}
	}
	if c.LoopbackClientConfig == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.LoopbackClientConfig == nil")
	}
	if c.EquivalentResourceRegistry == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.EquivalentResourceRegistry == nil")
	}

	handlerChainBuilder := func(handler http.Handler) http.Handler {
		return c.BuildHandlerChainFunc(handler, c.Config)
	}

	var debugSocket *routes.DebugSocket
	if c.DebugSocketPath != "" {
		debugSocket = routes.NewDebugSocket(c.DebugSocketPath)
	}

	apiServerHandler := NewAPIServerHandler(name, c.Serializer, handlerChainBuilder, delegationTarget.UnprotectedHandler())

	s := &GenericAPIServer{
		discoveryAddresses:             c.DiscoveryAddresses,
		LoopbackClientConfig:           c.LoopbackClientConfig,
		legacyAPIGroupPrefixes:         c.LegacyAPIGroupPrefixes,
		admissionControl:               c.AdmissionControl,
		Serializer:                     c.Serializer,
		AuditBackend:                   c.AuditBackend,
		Authorizer:                     c.Authorization.Authorizer,
		delegationTarget:               delegationTarget,
		EquivalentResourceRegistry:     c.EquivalentResourceRegistry,
		NonLongRunningRequestWaitGroup: c.NonLongRunningRequestWaitGroup,
		WatchRequestWaitGroup:          c.WatchRequestWaitGroup,
		Handler:                        apiServerHandler,
		UnprotectedDebugSocket:         debugSocket,

		listedPathProvider: apiServerHandler,
		Flagz:              c.Flagz,

		minRequestTimeout:                   time.Duration(c.MinRequestTimeout) * time.Second,
		ShutdownTimeout:                     c.RequestTimeout,
		ShutdownDelayDuration:               c.ShutdownDelayDuration,
		ShutdownWatchTerminationGracePeriod: c.ShutdownWatchTerminationGracePeriod,
		SecureServingInfo:                   c.SecureServing,
		ExternalAddress:                     c.ExternalAddress,

		openAPIConfig:           c.OpenAPIConfig,
		openAPIV3Config:         c.OpenAPIV3Config,
		skipOpenAPIInstallation: c.SkipOpenAPIInstallation,

		postStartHooks:         map[string]postStartHookEntry{},
		preShutdownHooks:       map[string]preShutdownHookEntry{},
		disabledPostStartHooks: c.DisabledPostStartHooks,

		healthzRegistry:  healthCheckRegistry{path: "/healthz", checks: c.HealthzChecks},
		livezRegistry:    healthCheckRegistry{path: "/livez", checks: c.LivezChecks, clock: clock.RealClock{}},
		readyzRegistry:   healthCheckRegistry{path: "/readyz", checks: c.ReadyzChecks},
		livezGracePeriod: c.LivezGracePeriod,

		DiscoveryGroupManager: discovery.NewRootAPIsHandler(c.DiscoveryAddresses, c.Serializer),

		maxRequestBodyBytes: c.MaxRequestBodyBytes,

		lifecycleSignals:       c.lifecycleSignals,
		ShutdownSendRetryAfter: c.ShutdownSendRetryAfter,

		APIServerID:           c.APIServerID,
		StorageReadinessHook:  NewStorageReadinessHook(c.StorageInitializationTimeout),
		StorageVersionManager: c.StorageVersionManager,

		EffectiveVersion:                        c.EffectiveVersion,
		EmulationForwardCompatible:              c.EmulationForwardCompatible,
		RuntimeConfigEmulationForwardCompatible: c.RuntimeConfigEmulationForwardCompatible,
		FeatureGate:                             c.FeatureGate,

		muxAndDiscoveryCompleteSignals: map[string]<-chan struct{}{},
	}

	manager := c.AggregatedDiscoveryGroupManager
	if manager == nil {
		manager = discoveryendpoint.NewResourceManager("apis")
	}
	s.AggregatedDiscoveryGroupManager = manager
	s.AggregatedLegacyDiscoveryGroupManager = discoveryendpoint.NewResourceManager("api")
	for {
		if c.JSONPatchMaxCopyBytes <= 0 {
			break
		}
		existing := atomic.LoadInt64(&jsonpatch.AccumulatedCopySizeLimit)
		if existing > 0 && existing < c.JSONPatchMaxCopyBytes {
			break
		}
		if atomic.CompareAndSwapInt64(&jsonpatch.AccumulatedCopySizeLimit, existing, c.JSONPatchMaxCopyBytes) {
			break
		}
	}

	// 先注册被委托服务器的 hook，再注册本地配置的 hook，保证重名检测覆盖两者。
	for k, v := range delegationTarget.PostStartHooks() {
		s.postStartHooks[k] = v
	}

	for k, v := range delegationTarget.PreShutdownHooks() {
		s.preShutdownHooks[k] = v
	}

	// 添加预先配置好的 poststarthooks。使用 add 方法可以在同名 hook 已被注册时得到错误。
	for name, preconfiguredPostStartHook := range c.PostStartHooks {
		if err := s.AddPostStartHook(name, preconfiguredPostStartHook.hook); err != nil {
			return nil, err
		}
	}

	// mux/discovery 完成信号参与 503 兜底逻辑，必须汇总到同一个服务器实例上。
	for k, v := range delegationTarget.MuxAndDiscoveryCompleteSignals() {
		if err := s.RegisterMuxAndDiscoveryCompleteSignal(k, v); err != nil {
			return nil, err
		}
	}

	genericApiServerHookName := "generic-apiserver-start-informers"
	if c.SharedInformerFactory != nil {
		// 只在未注册时启动 informer，避免与委托目标重复启动同一工厂。
		if !s.isPostStartHookRegistered(genericApiServerHookName) {
			err := s.AddPostStartHook(genericApiServerHookName, func(hookContext PostStartHookContext) error {
				c.SharedInformerFactory.Start(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		// TODO: 等我们去掉 /healthz 之后，可以考虑把它改成 post-start-hook。
		err := s.AddReadyzChecks(healthz.NewInformerSyncHealthz(c.SharedInformerFactory))
		if err != nil {
			return nil, err
		}
	}

	const priorityAndFairnessConfigConsumerHookName = "priority-and-fairness-config-consumer"
	if s.isPostStartHookRegistered(priorityAndFairnessConfigConsumerHookName) {
	} else if c.FlowControl != nil {
		err := s.AddPostStartHook(priorityAndFairnessConfigConsumerHookName, func(hookContext PostStartHookContext) error {
			go c.FlowControl.Run(hookContext.Done())
			return nil
		})
		if err != nil {
			return nil, err
		}
		// TODO(yue9944882): 为请求管理系统（request-management system）接上 pre-shutdown-hook？
	} else {
		klog.V(3).Infof("Not requested to run hook %s", priorityAndFairnessConfigConsumerHookName)
	}

	// 添加 PostStartHooks 以维护 Priority-and-Fairness 与 Max-in-Flight 两类过滤器的水位；
	// 二者互斥：启用 APF 时由 APF 的排队/并发指标驱动，否则由 max-in-flight 采样驱动。
	if c.FlowControl != nil {
		const priorityAndFairnessFilterHookName = "priority-and-fairness-filter"
		if !s.isPostStartHookRegistered(priorityAndFairnessFilterHookName) {
			err := s.AddPostStartHook(priorityAndFairnessFilterHookName, func(hookContext PostStartHookContext) error {
				genericfilters.StartPriorityAndFairnessWatermarkMaintenance(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	} else {
		const maxInFlightFilterHookName = "max-in-flight-filter"
		if !s.isPostStartHookRegistered(maxInFlightFilterHookName) {
			err := s.AddPostStartHook(maxInFlightFilterHookName, func(hookContext PostStartHookContext) error {
				genericfilters.StartMaxInFlightWatermarkMaintenance(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	// 添加用于维护对象计数跟踪器（object count tracker）的 PostStartHook。
	if c.StorageObjectCountTracker != nil {
		const storageObjectCountTrackerHookName = "storage-object-count-tracker-hook"
		if !s.isPostStartHookRegistered(storageObjectCountTrackerHookName) {
			if err := s.AddPostStartHook(storageObjectCountTrackerHookName, func(hookContext PostStartHookContext) error {
				go c.StorageObjectCountTracker.RunUntil(hookContext.Done())
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}

	// 合并委托目标的健康检查；同名检查以委托目标为准，避免重复执行。
	for _, delegateCheck := range delegationTarget.HealthzChecks() {
		skip := false
		for _, existingCheck := range c.HealthzChecks {
			if existingCheck.Name() == delegateCheck.Name() {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		s.AddHealthChecks(delegateCheck)
	}
	s.RegisterDestroyFunc(func() {
		if err := c.Config.TracerProvider.Shutdown(context.Background()); err != nil {
			klog.Errorf("failed to shut down tracer provider: %v", err)
		}
	})

	s.listedPathProvider = routes.ListedPathProviders{s.listedPathProvider, delegationTarget}

	installAPI(name, s, c.Config)

	// 使用委托目标的 UnprotectedHandler，以保证在委托场景下我们不会
	// 重复执行认证、授权或过滤器链中的其他环节。
	if delegationTarget.UnprotectedHandler() == nil && c.EnableIndex {
		s.Handler.NonGoRestfulMux.NotFoundHandler(routes.IndexLister{
			StatusCode:   http.StatusNotFound,
			PathProvider: s.listedPathProvider,
		})
	}

	return s, nil
}

// BuildHandlerChainWithStorageVersionPrecondition 在默认链之外增加存储版本前置校验，
// 防止客户端在存储版本迁移期间读到不支持的版本。
func BuildHandlerChainWithStorageVersionPrecondition(apiHandler http.Handler, c *Config) http.Handler {
	// WithStorageVersionPrecondition 需要先让 WithRequestInfo 跑起来
	handler := genericapifilters.WithStorageVersionPrecondition(apiHandler, c.StorageVersionManager, c.Serializer)
	return DefaultBuildHandlerChain(handler, c)
}

// DefaultBuildHandlerChain 组装默认的过滤器链。
//
// 链的顺序即执行顺序（后包装的先执行）：审计初始化 -> panic 恢复 -> 请求信息解析 ->
// 超时/限流 -> 认证 -> 授权 -> ...。顺序敏感，调整前务必确认各过滤器对上下文与响应的依赖。
func DefaultBuildHandlerChain(apiHandler http.Handler, c *Config) http.Handler {
	handler := apiHandler

	// 各阶段用 TrackStarted/TrackCompleted 成对打点；据此可从总延迟中扣除固定开销，
	// 并单独统计 webhook 与 APF 排队耗时。
	handler = filterlatency.TrackCompleted(handler)
	handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, c.Serializer)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authorization")

	// APF 与固定上限限流二选一：APF 支持按优先级/公平性分配并发。
	if c.FlowControl != nil {
		workEstimatorCfg := flowcontrolrequest.DefaultWorkEstimatorConfig()
		requestWorkEstimator := flowcontrolrequest.NewWorkEstimator(
			c.StorageObjectCountTracker.Get, c.FlowControl.GetInterestedWatchCount, workEstimatorCfg, c.FlowControl.GetMaxSeats)
		handler = filterlatency.TrackCompleted(handler)
		handler = genericfilters.WithPriorityAndFairness(handler, c.LongRunningFunc, c.FlowControl, requestWorkEstimator, c.RequestTimeout/4)
		handler = filterlatency.TrackStarted(handler, c.TracerProvider, "priorityandfairness")
	} else {
		handler = genericfilters.WithMaxInFlightLimit(handler, c.MaxRequestsInFlight, c.MaxMutatingRequestsInFlight, c.LongRunningFunc)
	}

	handler = filterlatency.TrackCompleted(handler)
	// 受限模拟会额外校验被模拟者的权限边界，特性门控关闭时退回传统模拟。
	if c.FeatureGate.Enabled(genericfeatures.ConstrainedImpersonation) {
		handler = impersonation.WithConstrainedImpersonation(handler, c.Authorization.Authorizer, c.Serializer)
		handler = filterlatency.TrackStarted(handler, c.TracerProvider, "constrainedimpersonation")
	} else {
		handler = impersonation.WithImpersonation(handler, c.Authorization.Authorizer, c.Serializer)
		handler = filterlatency.TrackStarted(handler, c.TracerProvider, "impersonation")
	}

	handler = filterlatency.TrackCompleted(handler)
	handler = genericapifilters.WithAudit(handler, c.AuditBackend, c.AuditPolicyRuleEvaluator, c.LongRunningFunc)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "audit")

	failedHandler := genericapifilters.Unauthorized(c.Serializer)
	failedHandler = genericapifilters.WithFailedAuthenticationAudit(failedHandler, c.AuditBackend, c.AuditPolicyRuleEvaluator)

	// WithTracing 放在认证之后，这样我们允许已通过认证的
	// 客户端影响采样决策。
	handler = genericapifilters.WithTracing(handler, c.TracerProvider)
	failedHandler = filterlatency.TrackCompleted(failedHandler)
	handler = filterlatency.TrackCompleted(handler)
	handler = genericapifilters.WithAuthentication(handler, c.Authentication.Authenticator, failedHandler, c.Authentication.APIAudiences, c.Authentication.RequestHeaderConfig)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authentication")

	handler = genericfilters.WithCORS(handler, c.CorsAllowedOriginList, nil, nil, nil, "true")

	// WithWarningRecorder 必须被 timeout handler 包裹，
	// 这样才能让添加 warning 响应头的操作是线程安全的。
	handler = genericapifilters.WithWarningRecorder(handler)

	// WithTimeoutForNonLongRunningRequests 会在一个 goroutine 中、使用带截止时间（deadline）的
	// context 调用其余的请求处理逻辑。该 goroutine 可以继续运行，而超时逻辑会向客户端返回超时。
	handler = genericfilters.WithTimeoutForNonLongRunningRequests(handler, c.LongRunningFunc)

	handler = genericapifilters.WithRequestDeadline(handler, c.AuditBackend, c.AuditPolicyRuleEvaluator,
		c.LongRunningFunc, c.Serializer, c.RequestTimeout)
	handler = genericfilters.WithWaitGroup(handler, c.LongRunningFunc, c.NonLongRunningRequestWaitGroup)
	if c.ShutdownWatchTerminationGracePeriod > 0 {
		handler = genericfilters.WithWatchTerminationDuringShutdown(handler, c.lifecycleSignals, c.WatchRequestWaitGroup)
	}
	if c.SecureServing != nil && !c.SecureServing.DisableHTTP2 && c.GoawayChance > 0 {
		handler = genericfilters.WithProbabilisticGoaway(handler, c.GoawayChance)
	}
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHSTS(handler, c.HSTSDirectives)
	if c.ShutdownSendRetryAfter {
		handler = genericfilters.WithRetryAfter(handler, c.lifecycleSignals.NotAcceptingNewRequest.Signaled())
	}
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericapifilters.WithLatencyTrackers(handler)
	// WithRoutine 会在一个独立的 goroutine 中执行后续的 handler，而在当前
	// goroutine 中执行 serving handler，从而把栈内存用量降到最低。它必须位于
	// WithPanicRecover() 之后，才能受到 panic 保护。
	if c.FeatureGate.Enabled(genericfeatures.APIServingWithRoutine) {
		handler = routine.WithRoutine(handler, c.LongRunningFunc)
	}
	handler = genericapifilters.WithRequestInfo(handler, c.RequestInfoResolver)
	handler = genericapifilters.WithRequestReceivedTimestamp(handler)
	handler = genericapifilters.WithMuxAndDiscoveryComplete(handler, c.lifecycleSignals.MuxAndDiscoveryComplete.Signaled())
	handler = genericfilters.WithPanicRecovery(handler, c.RequestInfoResolver)
	handler = genericapifilters.WithAuditInit(handler)
	return handler
}

// installAPI 安装可选的通用端点（索引、pprof、metrics、version、发现等）。
func installAPI(name string, s *GenericAPIServer, c *Config) {
	if c.EnableIndex {
		routes.Index{}.Install(s.listedPathProvider, s.Handler.NonGoRestfulMux)
	}
	if c.EnableProfiling {
		routes.Profiling{}.Install(s.Handler.NonGoRestfulMux)
		if c.EnableContentionProfiling {
			// 1 表示以采样方式记录所有阻塞事件，开销显著，仅用于排障。
			goruntime.SetBlockProfileRate(1)
		}
		// 到目前为止，这些调试开关只认为适合添加到与日志相关的端点上。
		routes.DebugFlags{}.Install(s.Handler.NonGoRestfulMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	if s.UnprotectedDebugSocket != nil {
		// debug socket 不经认证，只应在受控环境（如本地回环）中启用。
		s.UnprotectedDebugSocket.InstallProfiling()
		s.UnprotectedDebugSocket.InstallDebugFlag("v", routes.StringFlagPutHandler(logs.GlogSetter))
		if c.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
	}

	if c.EnableMetrics {
		if c.EnableProfiling {
			// 同时暴露重置接口，便于本地调试；生产环境应通过鉴权限制访问。
			routes.MetricsWithReset{}.Install(s.Handler.NonGoRestfulMux)
			slis.SLIMetricsWithReset{}.Install(s.Handler.NonGoRestfulMux)
		} else {
			routes.DefaultMetrics{}.Install(s.Handler.NonGoRestfulMux)
			slis.SLIMetrics{}.Install(s.Handler.NonGoRestfulMux)
		}
	}

	routes.Version{Version: c.EffectiveVersion.Info()}.Install(s.Handler.GoRestfulContainer)

	if c.EnableDiscovery {
		wrapped := discoveryendpoint.WrapAggregatedDiscoveryToHandler(s.DiscoveryGroupManager, s.AggregatedDiscoveryGroupManager, s.PeerAggregatedDiscoveryManager)
		s.Handler.GoRestfulContainer.Add(wrapped.GenerateWebService("/apis", metav1.APIGroupList{}))
	}
	if c.FlowControl != nil {
		c.FlowControl.Install(s.Handler.NonGoRestfulMux)
	}
}

// NewRequestInfoResolver 根据配置中的 API 前缀构造请求信息解析器。
// GrouplessAPIPrefixes 中的前缀（默认 /api）走无组路径，其余（/apis）必须带组。
func NewRequestInfoResolver(c *Config) *apirequest.RequestInfoFactory {
	apiPrefixes := sets.NewString(strings.Trim(APIGroupPrefix, "/")) // 所有可能的 API 前缀
	legacyAPIPrefixes := sets.String{}                               // 不带 group 的 APIPrefixes（legacy）
	for legacyAPIPrefix := range c.LegacyAPIGroupPrefixes {
		apiPrefixes.Insert(strings.Trim(legacyAPIPrefix, "/"))
		legacyAPIPrefixes.Insert(strings.Trim(legacyAPIPrefix, "/"))
	}

	return &apirequest.RequestInfoFactory{
		APIPrefixes:          apiPrefixes,
		GrouplessAPIPrefixes: legacyAPIPrefixes,
	}
}

// HostPort 返回安全监听的实际地址与端口；listener 未创建时返回错误。
func (s *SecureServingInfo) HostPort() (string, int, error) {
	if s == nil || s.Listener == nil {
		return "", 0, fmt.Errorf("no listener found")
	}
	addr := s.Listener.Addr().String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get port from listener address %q: %v", addr, err)
	}
	port, err := utilsnet.ParsePort(portStr, true)
	if err != nil {
		return "", 0, fmt.Errorf("invalid non-numeric port %q", portStr)
	}
	return host, port, nil
}

// AuthorizeClientBearerToken 在指定了回环（loopback）客户端配置、并且它带有 bearer token 时，
// 把认证器和授权器包装上回环认证逻辑。注意：只要 authn 或 authz 为 nil，
// 本函数就不会添加 token 认证器或授权器。
//
// 回环令牌被映射为 system:masters，仅用于服务器内部调用；它只加入认证链前端，
// 仍需经过授权链（特权组会放行）。
func AuthorizeClientBearerToken(loopback *restclient.Config, authn *AuthenticationInfo, authz *AuthorizationInfo) {
	if loopback == nil || len(loopback.BearerToken) == 0 {
		return
	}
	if authn == nil || authz == nil {
		// 避免空指针 panic
		return
	}
	if authn.Authenticator == nil || authz.Authorizer == nil {
		// 当我们希望绕过 authz/authn 时，authenticator 或 authorizer 可能为 nil，
		// 这种情况下我们同样什么都不做。
		return
	}

	privilegedLoopbackToken := loopback.BearerToken
	// 随机 UID 使每次启动的内部身份可区分，便于审计追踪。
	var uid = uuid.New().String()
	tokens := make(map[string]*user.DefaultInfo)
	tokens[privilegedLoopbackToken] = &user.DefaultInfo{
		Name:   user.APIServerUser,
		UID:    uid,
		Groups: []string{user.AllAuthenticated, user.SystemPrivilegedGroup},
	}

	tokenAuthenticator := authenticatorfactory.NewFromTokens(tokens, authn.APIAudiences)
	authn.Authenticator = authenticatorunion.New(tokenAuthenticator, authn.Authenticator)
}

// 仅供测试使用
func SetHostnameFuncForTests(name string) {
	hostnameFunc = func() (host string, err error) {
		host = name
		err = nil
		return
	}
}
