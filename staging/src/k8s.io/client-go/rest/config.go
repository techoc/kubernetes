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

package rest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	gruntime "runtime"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/client-go/features"
	"k8s.io/client-go/pkg/version"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/transport"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

const (
	// DefaultQPS 是未设置 QPS 时的默认客户端限速（每秒请求数）。
	DefaultQPS float32 = 5.0
	// DefaultBurst 是未设置 Burst 时的默认令牌桶容量。
	DefaultBurst int = 10
)

// ErrNotInCluster 在未定义 KUBERNETES_SERVICE_HOST/PORT 时由 InClusterConfig 返回。
var ErrNotInCluster = errors.New("unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined")

// Config 是构造所有客户端的基础配置：连接地址、认证、TLS、序列化与限流。
// 它同时被 kubeconfig 加载、controller 与 generated clientset 使用，
// 因此字段语义变化需考虑向后兼容（见各字段注释）。
type Config struct {
	// Host 是主机字符串、host:port 或指向 apiserver 根路径的 URL。URL 中的可选 Path 会作为前缀
	// 加到所有访问 apiserver 的请求 URI 上，便于前端代理搬迁 apiserver 端点。
	Host string
	// APIPath 是指向 API 根的子路径。
	APIPath string

	// ContentConfig 包含影响对象发送到服务端时如何序列化的设置。
	ContentConfig

	// 服务端要求 Basic 认证。
	Username string
	Password string `datapolicy:"password"`

	// 服务端要求 Bearer 认证；本客户端不会为 OAuth2 流程使用 refresh token。
	// TODO：给出兼容 OAuth2 的客户端示例。
	BearerToken string `datapolicy:"token"`

	// 存放 BearerToken 的文件路径。设置后会定期读取，最后一次成功读取的值优先于 BearerToken。
	BearerTokenFile string

	// Impersonate 是 RESTClient 用于身份伪装的配置。
	Impersonate ImpersonationConfig

	// 服务端要求由插件指定的认证方式。
	AuthProvider *clientcmdapi.AuthProviderConfig

	// 用于持久化 AuthProvider 配置的回调。
	AuthConfigPersister AuthProviderConfigPersister

	// 基于 exec 插件的认证提供者。
	ExecProvider *clientcmdapi.ExecConfig

	// TLSClientConfig 包含启用传输层安全的设置。
	TLSClientConfig

	// UserAgent 可选，用于标识请求调用方。
	UserAgent string

	// DisableCompression 关闭自动向服务端请求 GZip 压缩。
	DisableCompression bool

	// Transport 用于自定义 HTTP 行为，不能与 TLS 客户端证书选项同时指定；需要按服务端添加中间件时使用 WrapTransport。
	Transport http.RoundTripper
	// WrapTransport 在底层 transport（由 TLSClientConfig、Transport 创建，或 http.DefaultTransport）
	// 初始化后调用，用于自定义 HTTP 行为。配置可在返回的 RoundTripper 上继续叠加中间件。
	//
	// 未来该字段会改为数组，请改用 config.Wrap() 而不是直接赋值。
	WrapTransport transport.WrapperFunc

	// QPS 是该客户端向 master 发起请求的最大每秒次数；为 0 时创建的 RESTClient 使用 DefaultQPS: 5。
	// 设为负数会关闭客户端限流，除非同时设置了 `Ratelimiter`。
	QPS float32

	// 限流的最大突发量；为 0 时创建的 RESTClient 使用 DefaultBurst: 10。
	Burst int

	// 限制该客户端到 master 连接速率的限流器，设置后会覆盖 QPS/Burst。
	RateLimiter flowcontrol.RateLimiter

	// WarningHandler 处理服务端响应中的 warning。两者都未设置时使用默认处理器；
	// 同时设置时优先 WarningHandlerWithContext。
	//
	// 详见 [SetDefaultWarningHandler] 的文档。
	//
	//logcheck:context // WarningHandlerWithContext should be used instead of WarningHandler in code which supports contextual logging.
	WarningHandler WarningHandler

	// WarningHandlerWithContext 处理服务端响应中的 warning。两者都未设置时使用默认处理器；
	// 同时设置时优先 WarningHandlerWithContext。
	//
	// 详见 [SetDefaultWarningHandler] 的文档。
	WarningHandlerWithContext WarningHandlerWithContext

	// 放弃服务端请求前的最长等待时间；0 表示不超时。
	Timeout time.Duration

	// Dial 指定创建明文 TCP 连接的拨号函数。
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	// Proxy 是该 transport 所有请求使用的代理函数；nil 时用 http.ProxyFromEnvironment，
	// 返回 nil *URL 表示不使用代理。socks5 目前不支持 SPDY 流式端点。
	Proxy func(*http.Request) (*url.URL, error)

	// Version 强制使用指定版本（前提是已注册）。是否仍需要该字段？
	// Version 字符串。
}

var _ fmt.Stringer = new(Config)
var _ fmt.GoStringer = new(Config)

// sanitizedConfig 是 Config 的类型别名，仅用于 String/GoString：
// 通过重新格式化别名类型，避免直接打印原始结构体时泄露敏感字段。
type sanitizedConfig *Config

// sanitizedAuthConfigPersister 隐藏持久化回调的内部状态，只保留占位说明。
type sanitizedAuthConfigPersister struct{ AuthProviderConfigPersister }

func (sanitizedAuthConfigPersister) GoString() string {
	return "rest.AuthProviderConfigPersister(--- REDACTED ---)"
}
func (sanitizedAuthConfigPersister) String() string {
	return "rest.AuthProviderConfigPersister(--- REDACTED ---)"
}

// sanitizedObject 隐藏 exec 插件配置中的任意对象内容。
type sanitizedObject struct{ runtime.Object }

func (sanitizedObject) GoString() string {
	return "runtime.Object(--- REDACTED ---)"
}
func (sanitizedObject) String() string {
	return "runtime.Object(--- REDACTED ---)"
}

// GoString 实现 fmt.GoStringer，并对 Config 的敏感字段脱敏，防止日志意外泄露。
func (c *Config) GoString() string {
	return c.String()
}

// 在副本上替换密码、Bearer token、auth persister 与 exec 配置，
// 使 %v/%#v 等日志输出不会带出凭证。
func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	cc := sanitizedConfig(CopyConfig(c))
	// 显式把非空的凭证字段标记为已脱敏。
	if cc.Password != "" {
		cc.Password = "--- REDACTED ---"
	}
	if cc.BearerToken != "" {
		cc.BearerToken = "--- REDACTED ---"
	}
	if cc.AuthConfigPersister != nil {
		cc.AuthConfigPersister = sanitizedAuthConfigPersister{cc.AuthConfigPersister}
	}
	if cc.ExecProvider != nil && cc.ExecProvider.Config != nil {
		cc.ExecProvider.Config = sanitizedObject{Object: cc.ExecProvider.Config}
	}
	return fmt.Sprintf("%#v", cc)
}

// ImpersonationConfig 对应 kubeconfig 的 impersonate 配置，通过 Imprsonate-*
// 请求头让 apiserver 以其他身份完成鉴权；调用方自身的 RBAC 仍需允许 impersonate。
type ImpersonationConfig struct {
	// UserName 是每次请求要伪装成的用户名。
	UserName string
	// UID 是标识用户的唯一值。
	UID string
	// Groups 是每次请求要伪装成的用户组。
	Groups []string
	// Extra 是自由格式的字段，用于把认证信息关联到授权信息，也可被伪装。
	Extra map[string][]string
}

// +k8s:deepcopy-gen=true
// TLSClientConfig 描述客户端 TLS 行为。*Data 字段优先于对应的 *File 字段，
// 便于直接嵌入 kubeconfig；若同时提供，文件路径会被忽略。
type TLSClientConfig struct {
	// 访问服务端时不校验 TLS 证书，仅限测试使用。
	Insecure bool
	// ServerName 作为 SNI 传给服务端，并用于客户端校验证书；为空时使用连接服务端的主机名。
	ServerName string

	// 服务端要求 TLS 客户端证书认证。
	CertFile string
	// 服务端要求 TLS 客户端证书认证。
	KeyFile string
	// 服务端的可信根证书。
	CAFile string

	// CertData 保存 PEM 编码的字节（通常来自客户端证书文件），优先于 CertFile。
	CertData []byte
	// KeyData 保存 PEM 编码的字节（通常来自客户端证书私钥文件），优先于 KeyFile。
	KeyData []byte `datapolicy:"security-key"`
	// CAData 保存 PEM 编码的字节（通常来自根证书包），优先于 CAFile。
	CAData []byte

	// NextProtos 是按偏好排序的应用层协议列表，用于填充 tls.Config.NextProtos。
	// 要告知服务端偏好 http/1.1 而非 http/2，设为 ["http/1.1", "h2"]（服务端可忽略该偏好）；
	// 只用 http/1.1 则设为 ["http/1.1"]。
	NextProtos []string
}

var _ fmt.Stringer = TLSClientConfig{}
var _ fmt.GoStringer = TLSClientConfig{}

type sanitizedTLSClientConfig TLSClientConfig

// GoString 实现 fmt.GoStringer，并对 TLSClientConfig 的敏感字段脱敏，防止日志意外泄露。
func (c TLSClientConfig) GoString() string {
	return c.String()
}

// String 实现 fmt.Stringer，并对 TLSClientConfig 的敏感字段脱敏，防止日志意外泄露。
// 证书数据可公开，但私钥必须脱敏，因此两者使用不同的占位文本。
func (c TLSClientConfig) String() string {
	cc := sanitizedTLSClientConfig{
		Insecure:   c.Insecure,
		ServerName: c.ServerName,
		CertFile:   c.CertFile,
		KeyFile:    c.KeyFile,
		CAFile:     c.CAFile,
		CertData:   c.CertData,
		KeyData:    c.KeyData,
		CAData:     c.CAData,
		NextProtos: c.NextProtos,
	}
	// 显式把非空的凭证字段标记为已脱敏。
	if len(cc.CertData) != 0 {
		cc.CertData = []byte("--- TRUNCATED ---")
	}
	if len(cc.KeyData) != 0 {
		cc.KeyData = []byte("--- REDACTED ---")
	}
	return fmt.Sprintf("%#v", cc)
}

// ContentConfig 定义请求/响应的媒体类型与序列化器，与连接和认证无关，
// 因此常被整体复用或复制到派生配置中。
type ContentConfig struct {
	// AcceptContentTypes 指定客户端可接受的类型，可选；未设置时用 ContentType 填充 Accept 头。
	AcceptContentTypes string
	// ContentType 指定与服务端通信的线上格式，作为 Accept 头及发送给服务端的对象的默认类型；未设置时取 "application/json"。
	ContentType string
	// GroupVersion 是通信所用的 API 版本。直接初始化 RESTClient 时必须提供，通过 Client 初始化时取默认代码版本。
	GroupVersion *schema.GroupVersion
	// NegotiatedSerializer 用于为多种媒体类型获取编解码器。
	//
	// TODO：随着 Kubernetes 移除内部客户端，NegotiatedSerializer 将被淘汰。
	NegotiatedSerializer runtime.NegotiatedSerializer
}

// RESTClientFor 按 Config 中要求的属性返回 RESTClient。注意 RESTClient 可能需要初始化 Client 时可选项的字段。
// 该方法创建的 RESTClient 是通用的：期望 API 遵循 Kubernetes 约定，但不一定是 Kubernetes API。
// 等价于 RESTClientForConfigAndClient(config, httpClient)，其中 httpClient 由 HTTPClientFor(config) 生成。
func RESTClientFor(config *Config) (*RESTClient, error) {
	if config.GroupVersion == nil {
		return nil, fmt.Errorf("GroupVersion is required when initializing a RESTClient")
	}
	if config.NegotiatedSerializer == nil {
		return nil, fmt.Errorf("NegotiatedSerializer is required when initializing a RESTClient")
	}

	// 在构造 transport/client 之前提前校验 config.Host，尽快失败；ServerURL 稍后会由
	// RESTClientForConfigAndClient() 获取，避免在建立昂贵连接后才暴露 URL 配置错误。
	_, _, err := DefaultServerUrlFor(config)
	if err != nil {
		return nil, err
	}

	httpClient, err := HTTPClientFor(config)
	if err != nil {
		return nil, err
	}

	return RESTClientForConfigAndClient(config, httpClient)
}

// RESTClientForConfigAndClient 按 Config 中要求的属性返回 RESTClient。
// 与 RESTClientFor 不同，它接受一个可在所有 API 组与版本间共享的 http.Client；传入的 client
// 优先于配置中的 transport 设置，nil 时默认使用 `http.DefaultClient`。
func RESTClientForConfigAndClient(config *Config, httpClient *http.Client) (*RESTClient, error) {
	metrics.EnsureRegistered()

	if config.GroupVersion == nil {
		return nil, fmt.Errorf("GroupVersion is required when initializing a RESTClient")
	}
	if config.NegotiatedSerializer == nil {
		return nil, fmt.Errorf("NegotiatedSerializer is required when initializing a RESTClient")
	}

	baseURL, versionedAPIPath, err := DefaultServerUrlFor(config)
	if err != nil {
		return nil, err
	}

	// QPS/Burst 仅在未提供自定义 RateLimiter 时生效；QPS<=0 表示禁用客户端限流。
	rateLimiter := config.RateLimiter
	if rateLimiter == nil {
		qps := config.QPS
		if config.QPS == 0.0 {
			qps = DefaultQPS
		}
		burst := config.Burst
		if config.Burst == 0 {
			burst = DefaultBurst
		}
		if qps > 0 {
			rateLimiter = flowcontrol.NewTokenBucketRateLimiter(qps, burst)
		}
	}

	var gv schema.GroupVersion
	if config.GroupVersion != nil {
		gv = *config.GroupVersion
	}
	clientContent := ClientContentConfig{
		AcceptContentTypes: config.AcceptContentTypes,
		ContentType:        config.ContentType,
		GroupVersion:       gv,
		Negotiator:         runtime.NewClientNegotiator(config.NegotiatedSerializer, gv),
	}

	restClient, err := NewRESTClient(baseURL, versionedAPIPath, clientContent, rateLimiter, httpClient)
	maybeSetWarningHandler(restClient, config.WarningHandler, config.WarningHandlerWithContext)
	return restClient, err
}

// maybeSetWarningHandler 若 handlerWithContext 非 nil 则设置它，否则在 handler 非 nil 时
// 用其包装后设置，两者都为 nil 时什么都不做；可能被用于 nil client。
// 两者同时设置时 WithContext 优先，以支持上下文日志。
func maybeSetWarningHandler(c *RESTClient, handler WarningHandler, handlerWithContext WarningHandlerWithContext) {
	if c == nil {
		return
	}
	switch {
	case handlerWithContext != nil:
		c.warningHandler = handlerWithContext
	case handler != nil:
		c.warningHandler = warningLoggerNopContext{l: handler}
	}
}

// UnversionedRESTClientFor 与 RESTClientFor 相同，但允许 config.Version 为空。
//
// 未版本化客户端用于 discovery、/version、/healthz 等不属于某个 GroupVersion 的端点。
func UnversionedRESTClientFor(config *Config) (*RESTClient, error) {
	if config.NegotiatedSerializer == nil {
		return nil, fmt.Errorf("NegotiatedSerializer is required when initializing a RESTClient")
	}

	// 构造 transport/client 前先校验 config.Host，尽快失败；ServerURL 稍后由 UnversionedRESTClientForConfigAndClient() 获取。
	_, _, err := DefaultServerUrlFor(config)
	if err != nil {
		return nil, err
	}

	httpClient, err := HTTPClientFor(config)
	if err != nil {
		return nil, err
	}

	return UnversionedRESTClientForConfigAndClient(config, httpClient)
}

// UnversionedRESTClientForConfigAndClient 与 RESTClientForConfigAndClient 相同，但允许 config.Version 为空。
func UnversionedRESTClientForConfigAndClient(config *Config, httpClient *http.Client) (*RESTClient, error) {
	if config.NegotiatedSerializer == nil {
		return nil, fmt.Errorf("NegotiatedSerializer is required when initializing a RESTClient")
	}

	baseURL, versionedAPIPath, err := DefaultServerUrlFor(config)
	if err != nil {
		return nil, err
	}

	rateLimiter := config.RateLimiter
	if rateLimiter == nil {
		qps := config.QPS
		if config.QPS == 0.0 {
			qps = DefaultQPS
		}
		burst := config.Burst
		if config.Burst == 0 {
			burst = DefaultBurst
		}
		if qps > 0 {
			rateLimiter = flowcontrol.NewTokenBucketRateLimiter(qps, burst)
		}
	}

	// 未版本化场景默认使用 metav1 的 GV，以取得可用的默认编解码器。
	gv := metav1.SchemeGroupVersion
	if config.GroupVersion != nil {
		gv = *config.GroupVersion
	}
	clientContent := ClientContentConfig{
		AcceptContentTypes: config.AcceptContentTypes,
		ContentType:        config.ContentType,
		GroupVersion:       gv,
		Negotiator:         runtime.NewClientNegotiator(config.NegotiatedSerializer, gv),
	}

	restClient, err := NewRESTClient(baseURL, versionedAPIPath, clientContent, rateLimiter, httpClient)
	maybeSetWarningHandler(restClient, config.WarningHandler, config.WarningHandlerWithContext)
	return restClient, err
}

// SetKubernetesDefaults 为访问 Kubernetes API 的客户端配置设置默认值，
// 若任何默认值不可行或非法则返回错误。
//
// 目前只补全 User-Agent；保留返回 error 是为了给未来新增校验留出兼容空间。
func SetKubernetesDefaults(config *Config) error {
	if len(config.UserAgent) == 0 {
		config.UserAgent = DefaultKubernetesUserAgent()
	}
	return nil
}

// adjustCommit 返回 git commit hash 保留足够有效位数的缩写形式。
// 7 位是 git 常用缩写长度，既短又能避免大多数冲突。
func adjustCommit(c string) string {
	if len(c) == 0 {
		return "unknown"
	}
	if len(c) > 7 {
		return c[:7]
	}
	return c
}

// adjustVersion 从 major.minor.patch-[alpha|beta|etc] 形式的版本号中去掉 "alpha"、"beta" 等后缀。
func adjustVersion(v string) string {
	if len(v) == 0 {
		return "unknown"
	}
	seg := strings.SplitN(v, "-", 2)
	return seg[0]
}

// adjustCommand 取操作系统相关命令路径的最后一段，用于 User-Agent。
func adjustCommand(p string) string {
	// 几乎不会发生，但比返回空字符串好。
	if len(p) == 0 {
		return "unknown"
	}
	return filepath.Base(p)
}

// buildUserAgent 根据给定参数构造 User-Agent 字符串。
func buildUserAgent(command, version, os, arch, commit string) string {
	return fmt.Sprintf(
		"%s/%s (%s/%s) kubernetes/%s", command, version, os, arch, commit)
}

// DefaultKubernetesUserAgent 返回由静态全局变量构造的 User-Agent 字符串。
func DefaultKubernetesUserAgent() string {
	return buildUserAgent(
		adjustCommand(os.Args[0]),
		adjustVersion(version.Get().GitVersion),
		gruntime.GOOS,
		gruntime.GOARCH,
		adjustCommit(version.Get().GitCommit))
}

// InClusterConfig 返回使用 Kubernetes 赋予 Pod 的 service account 的配置对象，
// 供预期在 Kubernetes 集群内 Pod 中运行的客户端使用；若在非 Kubernetes 环境
// 的进程中调用会返回 ErrNotInCluster。
//
// 仅依赖 Pod 内注入的固定路径与 KUBERNETES_SERVICE_* 环境变量；
// 不读取 kubeconfig，因此不会意外使用宿主机上的其他凭证。
func InClusterConfig() (*Config, error) {
	const (
		tokenFile  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		rootCAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	)
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if len(host) == 0 || len(port) == 0 {
		return nil, ErrNotInCluster
	}

	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	tlsClientConfig := TLSClientConfig{}

	// 故意只记录而不失败：旧行为如此，且 token 认证在无 CA 时仍可能可用。
	if _, err := certutil.NewPool(rootCAFile); err != nil {
		//nolint:logcheck // The decision to log this instead of returning an error goes back to ~2016. It's part of the client-go API now, so not changing it just to support contextual logging.
		klog.Errorf("Expected to load root CA config from %s, but got err: %v", rootCAFile, err)
	} else {
		tlsClientConfig.CAFile = rootCAFile
	}

	return &Config{
		// TODO：改用集群 DNS。
		Host:            "https://" + net.JoinHostPort(host, port),
		TLSClientConfig: tlsClientConfig,
		BearerToken:     string(token),
		BearerTokenFile: tokenFile,
	}, nil
}

// IsConfigTransportTLS 判断给定 config 传给 restclient.RESTClientFor() 后是否得到受保护的连接，
// 用于决定是否在线路上发送凭证。注意该判断忽略 Insecure 标志，因此仍可能遭受中间人攻击。
func IsConfigTransportTLS(config Config) bool {
	// 只看 URL scheme：Insecure 表示“跳过证书校验”，并不等于明文传输。
	baseURL, _, err := DefaultServerUrlFor(&config)
	if err != nil {
		return false
	}
	return baseURL.Scheme == "https"
}

// LoadTLSFiles 把 CertFile、KeyFile、CAFile 的内容复制到 CertData、KeyData、CAData；无错误时三者要么已填充，要么原本为空。
func LoadTLSFiles(c *Config) error {
	var err error
	c.CAData, err = dataFromSliceOrFile(c.CAData, c.CAFile)
	if err != nil {
		return err
	}

	c.CertData, err = dataFromSliceOrFile(c.CertData, c.CertFile)
	if err != nil {
		return err
	}

	c.KeyData, err = dataFromSliceOrFile(c.KeyData, c.KeyFile)
	return err
}

// dataFromSliceOrFile 返回切片中的数据（若非空），否则返回文件内容；读取文件出错则返回错误。
//
// 切片优先：显式内联数据覆盖文件内容，这是 kubeconfig 中 *-data 字段的约定。
func dataFromSliceOrFile(data []byte, file string) ([]byte, error) {
	if len(data) > 0 {
		return data, nil
	}
	if len(file) > 0 {
		fileData, err := os.ReadFile(file)
		if err != nil {
			return []byte{}, err
		}
		return fileData, nil
	}
	return nil, nil
}

// AddUserAgent 在默认 User-Agent 之后追加调用方标识；直接修改并返回入参。
func AddUserAgent(config *Config, userAgent string) *Config {
	fullUserAgent := DefaultKubernetesUserAgent() + "/" + userAgent
	config.UserAgent = fullUserAgent
	return config
}

// AnonymousClientConfig 返回给定配置的副本，移除所有用户凭证（cert/key、bearer token、
// username/password）与自定义 transport（WrapTransport、Transport）。
//
// 典型用途是访问不需要认证的端点（如 /healthz 或公共发现信息）时避免泄露凭证。
func AnonymousClientConfig(config *Config) *Config {
	// 白名单式复制：新增的凭证字段不会被无意带入匿名配置。
	return &Config{
		Host:          config.Host,
		APIPath:       config.APIPath,
		ContentConfig: config.ContentConfig,
		TLSClientConfig: TLSClientConfig{
			Insecure:   config.Insecure,
			ServerName: config.ServerName,
			CAFile:     config.TLSClientConfig.CAFile,
			CAData:     config.TLSClientConfig.CAData,
			NextProtos: config.TLSClientConfig.NextProtos,
		},
		RateLimiter:               config.RateLimiter,
		WarningHandler:            config.WarningHandler,
		WarningHandlerWithContext: config.WarningHandlerWithContext,
		UserAgent:                 config.UserAgent,
		DisableCompression:        config.DisableCompression,
		QPS:                       config.QPS,
		Burst:                     config.Burst,
		Timeout:                   config.Timeout,
		Dial:                      config.Dial,
		Proxy:                     config.Proxy,
	}
}

// CopyConfig 返回给定配置的副本。
//
// 这是浅拷贝，但会对 ExecProvider.Config 做深拷贝，因为该 runtime.Object
// 可能被多个客户端共享并修改，浅拷贝会导致意外的数据竞争。
func CopyConfig(config *Config) *Config {
	c := &Config{
		Host:            config.Host,
		APIPath:         config.APIPath,
		ContentConfig:   config.ContentConfig,
		Username:        config.Username,
		Password:        config.Password,
		BearerToken:     config.BearerToken,
		BearerTokenFile: config.BearerTokenFile,
		Impersonate: ImpersonationConfig{
			UserName: config.Impersonate.UserName,
			UID:      config.Impersonate.UID,
			Groups:   config.Impersonate.Groups,
			Extra:    config.Impersonate.Extra,
		},
		AuthProvider:        config.AuthProvider,
		AuthConfigPersister: config.AuthConfigPersister,
		ExecProvider:        config.ExecProvider,
		TLSClientConfig: TLSClientConfig{
			Insecure:   config.TLSClientConfig.Insecure,
			ServerName: config.TLSClientConfig.ServerName,
			CertFile:   config.TLSClientConfig.CertFile,
			KeyFile:    config.TLSClientConfig.KeyFile,
			CAFile:     config.TLSClientConfig.CAFile,
			CertData:   config.TLSClientConfig.CertData,
			KeyData:    config.TLSClientConfig.KeyData,
			CAData:     config.TLSClientConfig.CAData,
			NextProtos: config.TLSClientConfig.NextProtos,
		},
		UserAgent:                 config.UserAgent,
		DisableCompression:        config.DisableCompression,
		Transport:                 config.Transport,
		WrapTransport:             config.WrapTransport,
		QPS:                       config.QPS,
		Burst:                     config.Burst,
		RateLimiter:               config.RateLimiter,
		WarningHandler:            config.WarningHandler,
		WarningHandlerWithContext: config.WarningHandlerWithContext,
		Timeout:                   config.Timeout,
		Dial:                      config.Dial,
		Proxy:                     config.Proxy,
	}
	if config.ExecProvider != nil && config.ExecProvider.Config != nil {
		c.ExecProvider.Config = config.ExecProvider.Config.DeepCopyObject()
	}
	return c
}

// CodecFactoryForGeneratedClient 在没有启用的、影响序列化的客户端 feature gate 时
// 原样返回传入的 CodecFactory，否则用提供的 Scheme 构建并返回新的 CodecFactory。
//
// 该函数仅供 client-gen 生成的客户端使用；调用方有责任确保 CodecFactory 参数
// 是用 Scheme 参数构建的。
//
// 仅当 ClientsAllowCBOR 开启时才引入 CBOR 序列化；关闭时原样返回传入的 codecs，
// 避免为每个生成客户端重复构建 CodecFactory。
func CodecFactoryForGeneratedClient(scheme *runtime.Scheme, codecs serializer.CodecFactory) serializer.CodecFactory {
	if !features.FeatureGates().Enabled(features.ClientsAllowCBOR) {
		// 注意：这里假设只要该特性门控存在，client-gen 就不会生成启用 CBOR 的 Codecs。
		return codecs
	}

	return serializer.NewCodecFactory(scheme, serializer.WithSerializer(cbor.NewSerializerInfo))
}
