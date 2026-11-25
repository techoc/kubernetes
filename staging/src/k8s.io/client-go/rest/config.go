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
	// Host must be a host string, a host:port pair, or a URL to the base of the apiserver.
	// If a URL is given then the (optional) Path of that URL represents a prefix that must
	// be appended to all request URIs used to access the apiserver. This allows a frontend
	// proxy to easily relocate all of the apiserver endpoints.
	Host string
	// APIPath is a sub-path that points to an API root.
	APIPath string

	// ContentConfig contains settings that affect how objects are transformed when
	// sent to the server.
	ContentConfig

	// Server requires Basic authentication
	Username string
	Password string `datapolicy:"password"`

	// Server requires Bearer authentication. This client will not attempt to use
	// refresh tokens for an OAuth2 flow.
	// TODO: demonstrate an OAuth2 compatible client.
	BearerToken string `datapolicy:"token"`

	// Path to a file containing a BearerToken.
	// If set, the contents are periodically read.
	// The last successfully read value takes precedence over BearerToken.
	BearerTokenFile string

	// Impersonate is the configuration that RESTClient will use for impersonation.
	Impersonate ImpersonationConfig

	// Server requires plugin-specified authentication.
	AuthProvider *clientcmdapi.AuthProviderConfig

	// Callback to persist config for AuthProvider.
	AuthConfigPersister AuthProviderConfigPersister

	// Exec-based authentication provider.
	ExecProvider *clientcmdapi.ExecConfig

	// TLSClientConfig contains settings to enable transport layer security
	TLSClientConfig

	// UserAgent is an optional field that specifies the caller of this request.
	UserAgent string

	// DisableCompression bypasses automatic GZip compression requests to the
	// server.
	DisableCompression bool

	// Transport may be used for custom HTTP behavior. This attribute may not
	// be specified with the TLS client certificate options. Use WrapTransport
	// to provide additional per-server middleware behavior.
	Transport http.RoundTripper
	// WrapTransport will be invoked for custom HTTP behavior after the underlying
	// transport is initialized (either the transport created from TLSClientConfig,
	// Transport, or http.DefaultTransport). The config may layer other RoundTrippers
	// on top of the returned RoundTripper.
	//
	// A future release will change this field to an array. Use config.Wrap()
	// instead of setting this value directly.
	WrapTransport transport.WrapperFunc

	// QPS indicates the maximum QPS to the master from this client.
	// If it's zero, the created RESTClient will use DefaultQPS: 5
	//
	// Setting this to a negative value will disable client-side ratelimiting
	// unless `Ratelimiter` is also set.
	QPS float32

	// Maximum burst for throttle.
	// If it's zero, the created RESTClient will use DefaultBurst: 10.
	Burst int

	// Rate limiter for limiting connections to the master from this client. If present overwrites QPS/Burst
	RateLimiter flowcontrol.RateLimiter

	// WarningHandler handles warnings in server responses.
	// If this and WarningHandlerWithContext are not set, the
	// default warning handler is used. If both are set,
	// WarningHandlerWithContext is used.
	//
	// See documentation for [SetDefaultWarningHandler] for details.
	//
	//logcheck:context // WarningHandlerWithContext should be used instead of WarningHandler in code which supports contextual logging.
	WarningHandler WarningHandler

	// WarningHandlerWithContext handles warnings in server responses.
	// If this and WarningHandler are not set, the
	// default warning handler is used. If both are set,
	// WarningHandlerWithContext is used.
	//
	// See documentation for [SetDefaultWarningHandler] for details.
	WarningHandlerWithContext WarningHandlerWithContext

	// The maximum length of time to wait before giving up on a server request. A value of zero means no timeout.
	Timeout time.Duration

	// Dial specifies the dial function for creating unencrypted TCP connections.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	// Proxy is the proxy func to be used for all requests made by this
	// transport. If Proxy is nil, http.ProxyFromEnvironment is used. If Proxy
	// returns a nil *URL, no proxy is used.
	//
	// socks5 proxying does not currently support spdy streaming endpoints.
	Proxy func(*http.Request) (*url.URL, error)

	// Version forces a specific version to be used (if registered)
	// Do we need this?
	// Version string
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

// GoString implements fmt.GoStringer and sanitizes sensitive fields of Config
// to prevent accidental leaking via logs.
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
	// Explicitly mark non-empty credential fields as redacted.
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
	// UserName is the username to impersonate on each request.
	UserName string
	// UID is a unique value that identifies the user.
	UID string
	// Groups are the groups to impersonate on each request.
	Groups []string
	// Extra is a free-form field which can be used to link some authentication information
	// to authorization information.  This field allows you to impersonate it.
	Extra map[string][]string
}

// +k8s:deepcopy-gen=true
// TLSClientConfig 描述客户端 TLS 行为。*Data 字段优先于对应的 *File 字段，
// 便于直接嵌入 kubeconfig；若同时提供，文件路径会被忽略。
type TLSClientConfig struct {
	// Server should be accessed without verifying the TLS certificate. For testing only.
	Insecure bool
	// ServerName is passed to the server for SNI and is used in the client to check server
	// certificates against. If ServerName is empty, the hostname used to contact the
	// server is used.
	ServerName string

	// Server requires TLS client certificate authentication
	CertFile string
	// Server requires TLS client certificate authentication
	KeyFile string
	// Trusted root certificates for server
	CAFile string

	// CertData holds PEM-encoded bytes (typically read from a client certificate file).
	// CertData takes precedence over CertFile
	CertData []byte
	// KeyData holds PEM-encoded bytes (typically read from a client certificate key file).
	// KeyData takes precedence over KeyFile
	KeyData []byte `datapolicy:"security-key"`
	// CAData holds PEM-encoded bytes (typically read from a root certificates bundle).
	// CAData takes precedence over CAFile
	CAData []byte

	// NextProtos is a list of supported application level protocols, in order of preference.
	// Used to populate tls.Config.NextProtos.
	// To indicate to the server http/1.1 is preferred over http/2, set to ["http/1.1", "h2"] (though the server is free to ignore that preference).
	// To use only http/1.1, set to ["http/1.1"].
	NextProtos []string
}

var _ fmt.Stringer = TLSClientConfig{}
var _ fmt.GoStringer = TLSClientConfig{}

type sanitizedTLSClientConfig TLSClientConfig

// GoString implements fmt.GoStringer and sanitizes sensitive fields of
// TLSClientConfig to prevent accidental leaking via logs.
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
	// Explicitly mark non-empty credential fields as redacted.
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
	// AcceptContentTypes specifies the types the client will accept and is optional.
	// If not set, ContentType will be used to define the Accept header
	AcceptContentTypes string
	// ContentType specifies the wire format used to communicate with the server.
	// This value will be set as the Accept header on requests made to the server, and
	// as the default content type on any object sent to the server. If not set,
	// "application/json" is used.
	ContentType string
	// GroupVersion is the API version to talk to. Must be provided when initializing
	// a RESTClient directly. When initializing a Client, will be set with the default
	// code version.
	GroupVersion *schema.GroupVersion
	// NegotiatedSerializer is used for obtaining encoders and decoders for multiple
	// supported media types.
	//
	// TODO: NegotiatedSerializer will be phased out as internal clients are removed
	//   from Kubernetes.
	NegotiatedSerializer runtime.NegotiatedSerializer
}

// RESTClientFor returns a RESTClient that satisfies the requested attributes on a client Config
// object. Note that a RESTClient may require fields that are optional when initializing a Client.
// A RESTClient created by this method is generic - it expects to operate on an API that follows
// the Kubernetes conventions, but may not be the Kubernetes API.
// RESTClientFor is equivalent to calling RESTClientForConfigAndClient(config, httpClient),
// where httpClient was generated with HTTPClientFor(config).
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

// RESTClientForConfigAndClient returns a RESTClient that satisfies the requested attributes on a
// client Config object.
// Unlike RESTClientFor, RESTClientForConfigAndClient allows to pass an http.Client that is shared
// between all the API Groups and Versions.
// Note that the http client takes precedence over the transport values configured.
// The http client defaults to the `http.DefaultClient` if nil.
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

	// Validate config.Host before constructing the transport/client so we can fail fast.
	// ServerURL will be obtained later in UnversionedRESTClientForConfigAndClient()
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

// UnversionedRESTClientForConfigAndClient is the same as RESTClientForConfigAndClient,
// except that it allows the config.Version to be empty.
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

// adjustVersion strips "alpha", "beta", etc. from version in form
// major.minor.patch-[alpha|beta|etc].
func adjustVersion(v string) string {
	if len(v) == 0 {
		return "unknown"
	}
	seg := strings.SplitN(v, "-", 2)
	return seg[0]
}

// adjustCommand returns the last component of the
// OS-specific command path for use in User-Agent.
func adjustCommand(p string) string {
	// Unlikely, but better than returning "".
	if len(p) == 0 {
		return "unknown"
	}
	return filepath.Base(p)
}

// buildUserAgent builds a User-Agent string from given args.
func buildUserAgent(command, version, os, arch, commit string) string {
	return fmt.Sprintf(
		"%s/%s (%s/%s) kubernetes/%s", command, version, os, arch, commit)
}

// DefaultKubernetesUserAgent returns a User-Agent string built from static global vars.
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
		// TODO: switch to using cluster DNS.
		Host:            "https://" + net.JoinHostPort(host, port),
		TLSClientConfig: tlsClientConfig,
		BearerToken:     string(token),
		BearerTokenFile: tokenFile,
	}, nil
}

// IsConfigTransportTLS returns true if and only if the provided
// config will result in a protected connection to the server when it
// is passed to restclient.RESTClientFor().  Use to determine when to
// send credentials over the wire.
//
// Note: the Insecure flag is ignored when testing for this value, so MITM attacks are
// still possible.
func IsConfigTransportTLS(config Config) bool {
	// 只看 URL scheme：Insecure 表示“跳过证书校验”，并不等于明文传输。
	baseURL, _, err := DefaultServerUrlFor(&config)
	if err != nil {
		return false
	}
	return baseURL.Scheme == "https"
}

// LoadTLSFiles copies the data from the CertFile, KeyFile, and CAFile fields into the CertData,
// KeyData, and CAFile fields, or returns an error. If no error is returned, all three fields are
// either populated or were empty to start.
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
		// NOTE: This assumes client-gen will not generate CBOR-enabled Codecs as long as
		// the feature gate exists.
		return codecs
	}

	return serializer.NewCodecFactory(scheme, serializer.WithSerializer(cbor.NewSerializerInfo))
}
