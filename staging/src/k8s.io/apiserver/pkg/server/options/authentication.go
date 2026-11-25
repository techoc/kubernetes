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

package options

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/transport"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
)

// DefaultAuthWebhookRetryBackoff 返回 apiserver 认证与授权 webhook 共用的默认退避参数。
// 5 次重试、1.5 倍增长并带抖动，用于在 webhook 后端抖动时限制重试风暴的扇出。
func DefaultAuthWebhookRetryBackoff() *wait.Backoff {
	return &wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   1.5,
		Jitter:   0.2,
		Steps:    5,
	}
}

// RequestHeaderAuthenticationOptions 配置“前置代理通过请求头传递用户身份”的认证方式。
// 安全性依赖 ClientCAFile：只有携带受信客户端证书的连接，其请求头才可信。
type RequestHeaderAuthenticationOptions struct {
	// ClientCAFile is the root certificate bundle to verify client certificates on incoming requests
	// before trusting usernames in headers.
	ClientCAFile string

	// 各类身份字段对应的请求头名称；可配置多个以支持代理切换。
	UsernameHeaders     []string
	UIDHeaders          []string
	GroupHeaders        []string
	ExtraHeaderPrefixes []string
	AllowedNames        []string
}

// Validate 检查请求头配置的合法性。
// 空白名称会被拒绝，因为它既无法匹配请求头，又会在日志中显示为空、难以排查。
func (s *RequestHeaderAuthenticationOptions) Validate() []error {
	allErrors := []error{}

	if err := checkForWhiteSpaceOnly("requestheader-username-headers", s.UsernameHeaders...); err != nil {
		allErrors = append(allErrors, err)
	}
	if err := checkForWhiteSpaceOnly("requestheader-group-headers", s.GroupHeaders...); err != nil {
		allErrors = append(allErrors, err)
	}
	if err := checkForWhiteSpaceOnly("requestheader-extra-headers-prefix", s.ExtraHeaderPrefixes...); err != nil {
		allErrors = append(allErrors, err)
	}
	if err := checkForWhiteSpaceOnly("requestheader-allowed-names", s.AllowedNames...); err != nil {
		allErrors = append(allErrors, err)
	}

	// 标准头缺失只会告警而非报错：聚合层（kube-aggregator）依赖这些固定头名，
	// 但独立 apiserver 仍可能有意使用自定义头。
	if len(s.UsernameHeaders) > 0 && !caseInsensitiveHas(s.UsernameHeaders, "X-Remote-User") {
		klog.Warningf("--requestheader-username-headers is set without specifying the standard X-Remote-User header - API aggregation will not work")
	}
	if len(s.GroupHeaders) > 0 && !caseInsensitiveHas(s.GroupHeaders, "X-Remote-Group") {
		klog.Warningf("--requestheader-group-headers is set without specifying the standard X-Remote-Group header - API aggregation will not work")
	}
	if len(s.ExtraHeaderPrefixes) > 0 && !caseInsensitiveHas(s.ExtraHeaderPrefixes, "X-Remote-Extra-") {
		klog.Warningf("--requestheader-extra-headers-prefix is set without specifying the standard X-Remote-Extra- header prefix - API aggregation will not work")
	}

	// UID 头是后加的特性，未启用门控时直接配置属于错误，防止旧版本代理误信任该头。
	if !utilfeature.DefaultFeatureGate.Enabled(features.RemoteRequestHeaderUID) {
		if len(s.UIDHeaders) > 0 {
			allErrors = append(allErrors, fmt.Errorf("--requestheader-uid-headers requires the %q feature to be enabled", features.RemoteRequestHeaderUID))
		}
	} else {
		if err := checkForWhiteSpaceOnly("requestheader-uid-headers", s.UIDHeaders...); err != nil {
			allErrors = append(allErrors, err)
		}
		if len(s.UIDHeaders) > 0 && !caseInsensitiveHas(s.UIDHeaders, "X-Remote-Uid") {
			// this was added later and so we are able to error out
			allErrors = append(allErrors, fmt.Errorf("--requestheader-uid-headers is set without specifying the standard X-Remote-Uid header - API aggregation will not work"))
		}
	}

	return allErrors
}

// checkForWhiteSpaceOnly 拒绝纯空白名称；逐个检查以便一次只报一个问题。
func checkForWhiteSpaceOnly(flag string, headerNames ...string) error {
	for _, headerName := range headerNames {
		if len(strings.TrimSpace(headerName)) == 0 {
			return fmt.Errorf("empty value in %q", flag)
		}
	}

	return nil
}

// caseInsensitiveHas 按 HTTP 语义做大小写不敏感匹配：头名本身不区分大小写。
func caseInsensitiveHas(headers []string, header string) bool {
	for _, h := range headers {
		if strings.EqualFold(h, header) {
			return true
		}
	}
	return false
}

// AddFlags 注册前置代理请求头认证相关的命令行参数。
func (s *RequestHeaderAuthenticationOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}

	fs.StringSliceVar(&s.UsernameHeaders, "requestheader-username-headers", s.UsernameHeaders, ""+
		"List of request headers to inspect for usernames. X-Remote-User is common.")

	fs.StringSliceVar(&s.UIDHeaders, "requestheader-uid-headers", s.UIDHeaders, ""+
		"List of request headers to inspect for UIDs. X-Remote-Uid is suggested. Requires the RemoteRequestHeaderUID feature to be enabled.")

	fs.StringSliceVar(&s.GroupHeaders, "requestheader-group-headers", s.GroupHeaders, ""+
		"List of request headers to inspect for groups. X-Remote-Group is suggested.")

	fs.StringSliceVar(&s.ExtraHeaderPrefixes, "requestheader-extra-headers-prefix", s.ExtraHeaderPrefixes, ""+
		"List of request header prefixes to inspect. X-Remote-Extra- is suggested.")

	fs.StringVar(&s.ClientCAFile, "requestheader-client-ca-file", s.ClientCAFile, ""+
		"Root certificate bundle to use to verify client certificates on incoming requests "+
		"before trusting usernames in headers specified by --requestheader-username-headers. "+
		"WARNING: generally do not depend on authorization being already done for incoming requests.")

	fs.StringSliceVar(&s.AllowedNames, "requestheader-allowed-names", s.AllowedNames, ""+
		"List of client certificate common names to allow to provide usernames in headers "+
		"specified by --requestheader-username-headers. If empty, any client certificate validated "+
		"by the authorities in --requestheader-client-ca-file is allowed.")
}

// ToAuthenticationRequestHeaderConfig 在需要时返回这些选项对应的 RequestHeaderConfig 配置对象，否则返回 nil。
// 未配置 ClientCAFile 时返回 nil，表示不启用该认证方式；
// 配置了则把 CA 文件包装成动态提供者，使证书轮换无需重启。
func (s *RequestHeaderAuthenticationOptions) ToAuthenticationRequestHeaderConfig() (*authenticatorfactory.RequestHeaderConfig, error) {
	if len(s.ClientCAFile) == 0 {
		return nil, nil
	}

	caBundleProvider, err := dynamiccertificates.NewDynamicCAContentFromFile("request-header", s.ClientCAFile)
	if err != nil {
		return nil, err
	}

	return &authenticatorfactory.RequestHeaderConfig{
		UsernameHeaders:     headerrequest.StaticStringSlice(s.UsernameHeaders),
		UIDHeaders:          headerrequest.StaticStringSlice(s.UIDHeaders),
		GroupHeaders:        headerrequest.StaticStringSlice(s.GroupHeaders),
		ExtraHeaderPrefixes: headerrequest.StaticStringSlice(s.ExtraHeaderPrefixes),
		CAContentProvider:   caBundleProvider,
		AllowedClientNames:  headerrequest.StaticStringSlice(s.AllowedNames),
	}, nil
}

// ClientCertAuthenticationOptions provides different options for client cert auth. You should use `GetClientVerifyOptionFn` to
// get the verify options for your authenticator.
type ClientCertAuthenticationOptions struct {
	// ClientCA is the certificate bundle for all the signers that you'll recognize for incoming client certificates
	ClientCA string

	// CAContentProvider are the options for verifying incoming connections using mTLS and directly assigning to users.
	// Generally this is the CA bundle file used to authenticate client certificates
	// If non-nil, this takes priority over the ClientCA file.
	CAContentProvider dynamiccertificates.CAContentProvider
}

// GetClientVerifyOptionFn 为认证器提供 verify options，并遵循各校验器的优先顺序。
// 显式传入的 CAContentProvider 优先于文件路径：调用方（如聚合层）可能已持有动态 CA。
func (s *ClientCertAuthenticationOptions) GetClientCAContentProvider() (dynamiccertificates.CAContentProvider, error) {
	if s.CAContentProvider != nil {
		return s.CAContentProvider, nil
	}

	if len(s.ClientCA) == 0 {
		return nil, nil
	}

	return dynamiccertificates.NewDynamicCAContentFromFile("client-ca-bundle", s.ClientCA)
}

// AddFlags 注册客户端证书认证相关的命令行参数。
func (s *ClientCertAuthenticationOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&s.ClientCA, "client-ca-file", s.ClientCA, ""+
		"If set, any request presenting a client certificate signed by one of "+
		"the authorities in the client-ca-file is authenticated with an identity "+
		"corresponding to the CommonName of the client certificate.")
}

// DelegatingAuthenticationOptions 让组合式 API 服务器可以便捷地把认证委托给集群根 kube API 服务器。API 联邦组件将充当前置代理，定向来的连接能够委托给核心 kube API 服务器。
// 聚合 apiserver 通常自身不持有用户数据，而是通过 TokenReview/SubjectAccessReview 委托给核心 apiserver 判定。
type DelegatingAuthenticationOptions struct {
	// RemoteKubeConfigFile is the file to use to connect to a "normal" kube API server which hosts the
	// TokenAccessReview.authentication.k8s.io endpoint for checking tokens.
	RemoteKubeConfigFile string
	// RemoteKubeConfigFileOptional is specifying whether not specifying the kubeconfig or
	// a missing in-cluster config will be fatal.
	RemoteKubeConfigFileOptional bool

	// CacheTTL is the length of time that a token authentication answer will be cached.
	CacheTTL time.Duration

	ClientCert    ClientCertAuthenticationOptions
	RequestHeader RequestHeaderAuthenticationOptions

	// SkipInClusterLookup indicates missing authentication configuration should not be retrieved from the cluster configmap
	SkipInClusterLookup bool

	// TolerateInClusterLookupFailure indicates failures to look up authentication configuration from the cluster configmap should not be fatal.
	// Setting this can result in an authenticator that will reject all requests.
	TolerateInClusterLookupFailure bool

	// WebhookRetryBackoff specifies the backoff parameters for the authentication webhook retry logic.
	// This allows us to configure the sleep time at each iteration and the maximum number of retries allowed
	// before we fail the webhook call in order to limit the fan out that ensues when the system is degraded.
	WebhookRetryBackoff *wait.Backoff

	// TokenRequestTimeout specifies a time limit for requests made by the authorization webhook client.
	// The default value is set to 10 seconds.
	TokenRequestTimeout time.Duration

	// CustomRoundTripperFn allows for specifying a middleware function for custom HTTP behaviour for the authentication webhook client.
	CustomRoundTripperFn transport.WrapperFunc

	// Anonymous gives user an option to enable/disable Anonymous authentication.
	Anonymous *apiserver.AnonymousAuthConfig
}

// NewDelegatingAuthenticationOptions 返回带默认值的委托认证配置。
func NewDelegatingAuthenticationOptions() *DelegatingAuthenticationOptions {
	return &DelegatingAuthenticationOptions{
		// 缓存时间很短：既保证令牌吊销能较快生效，又能吸收短时间内的重复校验。
		CacheTTL:   10 * time.Second,
		ClientCert: ClientCertAuthenticationOptions{},
		RequestHeader: RequestHeaderAuthenticationOptions{
			UsernameHeaders: []string{"x-remote-user"},
			// we specifically don't default UID headers as these were introduced
			// later (kube 1.32) and we don't want 3rd parties to be trusting the default headers
			// before we can safely say that all KAS instances know they should
			// remove them from an incoming request in its WithAuthentication handler.
			// The defaulting will be enabled in a future (1.33+) version.
			UIDHeaders:          nil,
			GroupHeaders:        []string{"x-remote-group"},
			ExtraHeaderPrefixes: []string{"x-remote-extra-"},
		},
		WebhookRetryBackoff: DefaultAuthWebhookRetryBackoff(),
		TokenRequestTimeout: 10 * time.Second,
		Anonymous:           &apiserver.AnonymousAuthConfig{Enabled: true},
	}
}

// WithCustomRetryBackoff sets the custom backoff parameters for the authentication webhook retry logic.
func (s *DelegatingAuthenticationOptions) WithCustomRetryBackoff(backoff wait.Backoff) {
	s.WebhookRetryBackoff = &backoff
}

// WithRequestTimeout sets the given timeout for requests made by the authentication webhook client.
func (s *DelegatingAuthenticationOptions) WithRequestTimeout(timeout time.Duration) {
	s.TokenRequestTimeout = timeout
}

// WithCustomRoundTripper allows for specifying a middleware function for custom HTTP behaviour for the authentication webhook client.
func (s *DelegatingAuthenticationOptions) WithCustomRoundTripper(rt transport.WrapperFunc) {
	s.CustomRoundTripperFn = rt
}

// Validate 校验委托认证配置；nil 接收者视为未启用，直接通过。
func (s *DelegatingAuthenticationOptions) Validate() []error {
	if s == nil {
		return nil
	}

	allErrors := []error{}
	allErrors = append(allErrors, s.RequestHeader.Validate()...)

	if s.WebhookRetryBackoff != nil && s.WebhookRetryBackoff.Steps <= 0 {
		allErrors = append(allErrors, fmt.Errorf("number of webhook retry attempts must be greater than 1, but is: %d", s.WebhookRetryBackoff.Steps))
	}

	return allErrors
}

// AddFlags 注册委托认证相关的命令行参数。
func (s *DelegatingAuthenticationOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}

	var optionalKubeConfigSentence string
	if s.RemoteKubeConfigFileOptional {
		optionalKubeConfigSentence = " This is optional. If empty, all token requests are considered to be anonymous and no client CA is looked up in the cluster."
	}
	fs.StringVar(&s.RemoteKubeConfigFile, "authentication-kubeconfig", s.RemoteKubeConfigFile, ""+
		"kubeconfig file pointing at the 'core' kubernetes server with enough rights to create "+
		"tokenreviews.authentication.k8s.io."+optionalKubeConfigSentence)

	fs.DurationVar(&s.CacheTTL, "authentication-token-webhook-cache-ttl", s.CacheTTL,
		"The duration to cache responses from the webhook token authenticator.")

	s.ClientCert.AddFlags(fs)
	s.RequestHeader.AddFlags(fs)

	fs.BoolVar(&s.SkipInClusterLookup, "authentication-skip-lookup", s.SkipInClusterLookup, ""+
		"If false, the authentication-kubeconfig will be used to lookup missing authentication "+
		"configuration from the cluster.")
	fs.BoolVar(&s.TolerateInClusterLookupFailure, "authentication-tolerate-lookup-failure", s.TolerateInClusterLookupFailure, ""+
		"If true, failures to look up missing authentication configuration from the cluster are not considered fatal. "+
		"Note that this can result in authentication that treats all requests as anonymous.")
}

// ApplyTo 组装并安装委托认证器。
// 流程：获取 kubeconfig 客户端 -> 解析客户端 CA 与请求头 CA -> 构造认证链 -> 注入 serving 配置与 OpenAPI 安全定义。
// 任一配置来源失败都返回错误，除非显式容忍（TolerateInClusterLookupFailure）。
func (s *DelegatingAuthenticationOptions) ApplyTo(authenticationInfo *server.AuthenticationInfo, servingInfo *server.SecureServingInfo, openAPIConfig *openapicommon.Config) error {
	if s == nil {
		authenticationInfo.Authenticator = nil
		return nil
	}

	cfg := authenticatorfactory.DelegatingAuthenticatorConfig{
		Anonymous:                &apiserver.AnonymousAuthConfig{Enabled: true},
		CacheTTL:                 s.CacheTTL,
		WebhookRetryBackoff:      s.WebhookRetryBackoff,
		TokenAccessReviewTimeout: s.TokenRequestTimeout,
	}

	client, err := s.getClient()
	if err != nil {
		return fmt.Errorf("failed to get delegated authentication kubeconfig: %v", err)
	}

	// configure token review
	if client != nil {
		cfg.TokenAccessReviewClient = client.AuthenticationV1()
	}

	// 显式配置了客户端证书时优先使用；否则尝试从集群 ConfigMap 读取，
	// 以便与核心 apiserver 的 CA 自动保持一致。
	clientCASpecified := s.ClientCert != ClientCertAuthenticationOptions{}
	var clientCAProvider dynamiccertificates.CAContentProvider
	if clientCASpecified {
		clientCAProvider, err = s.ClientCert.GetClientCAContentProvider()
		if err != nil {
			return fmt.Errorf("unable to load client CA provider: %v", err)
		}
		cfg.ClientCertificateCAContentProvider = clientCAProvider
		if err = authenticationInfo.ApplyClientCert(cfg.ClientCertificateCAContentProvider, servingInfo); err != nil {
			return fmt.Errorf("unable to assign client CA provider: %v", err)
		}

	} else if !s.SkipInClusterLookup {
		if client == nil {
			klog.Warningf("No authentication-kubeconfig provided in order to lookup client-ca-file in configmap/%s in %s, so client certificate authentication won't work.", authenticationConfigMapName, authenticationConfigMapNamespace)
		} else {
			clientCAProvider, err = dynamiccertificates.NewDynamicCAFromConfigMapController("client-ca", authenticationConfigMapNamespace, authenticationConfigMapName, "client-ca-file", client)
			if err != nil {
				return fmt.Errorf("unable to load configmap based client CA file: %v", err)
			}
			cfg.ClientCertificateCAContentProvider = clientCAProvider
			if err = authenticationInfo.ApplyClientCert(cfg.ClientCertificateCAContentProvider, servingInfo); err != nil {
				return fmt.Errorf("unable to assign configmap based client CA file: %v", err)
			}

		}
	}

	requestHeaderCAFileSpecified := len(s.RequestHeader.ClientCAFile) > 0
	// 请求头认证同样有“显式文件 / 集群 ConfigMap / 跳过”三条路径。
	var requestHeaderConfig *authenticatorfactory.RequestHeaderConfig
	if requestHeaderCAFileSpecified {
		requestHeaderConfig, err = s.RequestHeader.ToAuthenticationRequestHeaderConfig()
		if err != nil {
			return fmt.Errorf("unable to create request header authentication config: %v", err)
		}

	} else if !s.SkipInClusterLookup {
		if client == nil {
			klog.Warningf("No authentication-kubeconfig provided in order to lookup requestheader-client-ca-file in configmap/%s in %s, so request-header client certificate authentication won't work.", authenticationConfigMapName, authenticationConfigMapNamespace)
		} else {
			requestHeaderConfig, err = s.createRequestHeaderConfig(client)
			if err != nil {
				if s.TolerateInClusterLookupFailure {
					klog.Warningf("Error looking up in-cluster authentication configuration: %v", err)
					klog.Warning("Continuing without authentication configuration. This may treat all requests as anonymous.")
					klog.Warning("To require authentication configuration lookup to succeed, set --authentication-tolerate-lookup-failure=false")
				} else {
					return fmt.Errorf("unable to load configmap based request-header-client-ca-file: %v", err)
				}
			}
		}
	}
	if requestHeaderConfig != nil {
		cfg.RequestHeaderConfig = requestHeaderConfig
		authenticationInfo.RequestHeaderConfig = requestHeaderConfig
		if err = authenticationInfo.ApplyClientCert(cfg.RequestHeaderConfig.CAContentProvider, servingInfo); err != nil {
			return fmt.Errorf("unable to load request-header-client-ca-file: %v", err)
		}
	}

	// create authenticator
	authenticator, securityDefinitions, err := cfg.New()
	if err != nil {
		return err
	}
	authenticationInfo.Authenticator = authenticator
	if openAPIConfig != nil {
		openAPIConfig.SecurityDefinitions = securityDefinitions
	}

	return nil
}

const (
	authenticationConfigMapNamespace = metav1.NamespaceSystem
	// authenticationConfigMapName is the name of ConfigMap in the kube-system namespace holding the root certificate
	// bundle to use to verify client certificates on incoming requests before trusting usernames in headers specified
	// by --requestheader-username-headers. This is created in the cluster by the kube-apiserver.
	// "WARNING: generally do not depend on authorization being already done for incoming requests.")
	authenticationConfigMapName = "extension-apiserver-authentication"
)

// createRequestHeaderConfig 从集群 ConfigMap 动态构建请求头认证配置。
// 返回的字符串切片提供者每次读取都反映最新配置，从而支持运行期更新头名。
func (s *DelegatingAuthenticationOptions) createRequestHeaderConfig(client kubernetes.Interface) (*authenticatorfactory.RequestHeaderConfig, error) {
	dynamicRequestHeaderProvider, err := newDynamicRequestHeaderController(client)
	if err != nil {
		return nil, fmt.Errorf("unable to create request header authentication config: %v", err)
	}

	//  look up authentication configuration in the cluster and in case of an err defer to authentication-tolerate-lookup-failure flag
	//  We are passing the context to ProxyCerts.RunOnce as it needs to implement RunOnce(ctx) however the
	//  context is not used at all. So passing a empty context shouldn't be a problem
	ctx := context.TODO()
	if err := dynamicRequestHeaderProvider.RunOnce(ctx); err != nil {
		return nil, err
	}

	return &authenticatorfactory.RequestHeaderConfig{
		CAContentProvider:   dynamicRequestHeaderProvider,
		UsernameHeaders:     headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.UsernameHeaders)),
		UIDHeaders:          headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.UIDHeaders)),
		GroupHeaders:        headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.GroupHeaders)),
		ExtraHeaderPrefixes: headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.ExtraHeaderPrefixes)),
		AllowedClientNames:  headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.AllowedClientNames)),
	}, nil
}

// getClient 返回一个 Kubernetes clientset。若 s.RemoteKubeConfigFileOptional 为 true，则当用户未指定 kubeconfig 且找不到 in-cluster 配置时返回 nil。
// 返回 nil 客户端是合法状态：表示未配置委托认证，调用方需据此跳过 TokenReview。
func (s *DelegatingAuthenticationOptions) getClient() (kubernetes.Interface, error) {
	var clientConfig *rest.Config
	var err error
	if len(s.RemoteKubeConfigFile) > 0 {
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: s.RemoteKubeConfigFile}
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

		clientConfig, err = loader.ClientConfig()
	} else {
		// without the remote kubeconfig file, try to use the in-cluster config.  Most addon API servers will
		// use this path. If it is optional, ignore errors.
		clientConfig, err = rest.InClusterConfig()
		if err != nil && s.RemoteKubeConfigFileOptional {
			if err != rest.ErrNotInCluster {
				klog.Warningf("failed to read in-cluster kubeconfig for delegated authentication: %v", err)
			}
			return nil, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get delegated authentication kubeconfig: %v", err)
	}

	// set high qps/burst limits since this will effectively limit API server responsiveness
	clientConfig.QPS = 200
	clientConfig.Burst = 400
	// do not set a timeout on the http client, instead use context for cancellation
	// if multiple timeouts were set, the request will pick the smaller timeout to be applied, leaving other useless.
	//
	// see https://github.com/golang/go/blob/a937729c2c2f6950a32bc5cd0f5b88700882f078/src/net/http/client.go#L364
	if s.CustomRoundTripperFn != nil {
		clientConfig.Wrap(s.CustomRoundTripperFn)
	}

	return kubernetes.NewForConfig(clientConfig)
}
