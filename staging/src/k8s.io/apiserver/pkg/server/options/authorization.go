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
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/apiserver/pkg/authorization/path"
	"k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/transport"
	"k8s.io/klog/v2"
)

// DelegatingAuthorizationOptions 为组合式 API Server 提供了一种便捷方式，
// 把授权委托给根 kube API server。
// 警告：切勿假设每个通过认证的入站请求都已经完成了授权。
// kube API server 中的聚合层今天会做这件事，但该行为并不能保证未来仍然如此。
// 委托授权通过 SubjectAccessReview 询问上层 kube-apiserver；
// 认证通过不等于已授权，每个请求都必须显式经过此授权器。
type DelegatingAuthorizationOptions struct {
	// RemoteKubeConfigFile 是用于连接“普通” kube API server 的文件，
	// 该 server 承载用于校验 token 的 SubjectAccessReview.authorization.k8s.io 端点。
	RemoteKubeConfigFile string
	// RemoteKubeConfigFileOptional 指定未提供 kubeconfig 或
	// 缺少 in-cluster 配置时是否致命。
	RemoteKubeConfigFileOptional bool

	// AllowCacheTTL 是成功授权响应的缓存时长。
	AllowCacheTTL time.Duration

	// DenyCacheTTL 是不成功授权响应的缓存时长；
	// 通常希望“拒绝后重试”的流程响应更灵敏，
	// 因此拒绝结果缓存更短，以便权限授予后能较快生效。
	DenyCacheTTL time.Duration

	// AlwaysAllowPaths 是不经过授权的 HTTP 路径。可以是普通路径，
	// 也可以以 * 结尾表示前缀匹配；开头斜杠可有可无。
	AlwaysAllowPaths []string

	// AlwaysAllowGroups 是可执行任意操作的组。在 kube 中是 system:masters。
	AlwaysAllowGroups []string

	// ClientTimeout 指定 SubjectAccessReviews 客户端发起请求的时间限制。
	// 默认值为 10 秒。
	ClientTimeout time.Duration

	// WebhookRetryBackoff 指定授权 webhook 重试逻辑的退避参数。
	// 用于配置每次迭代的等待时间与最大重试次数，
	// 以限制系统退化时引发的扇出。
	WebhookRetryBackoff *wait.Backoff

	// CustomRoundTripperFn 允许为授权 webhook 客户端指定自定义 HTTP 行为的中间件函数。
	CustomRoundTripperFn transport.WrapperFunc
}

// NewDelegatingAuthorizationOptions 返回带安全默认值的委托授权选项。
func NewDelegatingAuthorizationOptions() *DelegatingAuthorizationOptions {
	return &DelegatingAuthorizationOptions{
		// 很低以保证响应性，但足够高以处理突发流量。
		AllowCacheTTL:       10 * time.Second,
		DenyCacheTTL:        10 * time.Second,
		ClientTimeout:       10 * time.Second,
		WebhookRetryBackoff: DefaultAuthWebhookRetryBackoff(),
		// 这使得 kubelet 无需触发授权检查就能始终获取健康与就绪状态；调用方若不想保留该行为可以清空此字段。
		// 健康检查端点必须忽略授权，否则节点无法在授权系统故障时判断自身状态。
		AlwaysAllowPaths: []string{"/healthz", "/readyz", "/livez"},
		// 在委托给 kube-apiserver 的授权调用中（常见情况），system:masters 在硬编码授权器中拥有完全权限。
		// 因此默认可以跳过对 system:masters 的授权检查。
		//
		// 调用方若不希望该行为可清空此字段。
		AlwaysAllowGroups: []string{"system:masters"},
	}
}

// WithAlwaysAllowGroups 把路径列表追加到 AlwaysAllowGroups。
func (s *DelegatingAuthorizationOptions) WithAlwaysAllowGroups(groups ...string) *DelegatingAuthorizationOptions {
	s.AlwaysAllowGroups = append(s.AlwaysAllowGroups, groups...)
	return s
}

// WithAlwaysAllowPaths 把路径列表追加到 AlwaysAllowPaths。
func (s *DelegatingAuthorizationOptions) WithAlwaysAllowPaths(paths ...string) *DelegatingAuthorizationOptions {
	s.AlwaysAllowPaths = append(s.AlwaysAllowPaths, paths...)
	return s
}

// WithClientTimeout 为此授权器使用的 SAR 客户端设置给定超时。
func (s *DelegatingAuthorizationOptions) WithClientTimeout(timeout time.Duration) {
	s.ClientTimeout = timeout
}

// WithCustomRetryBackoff 为授权 webhook 重试逻辑设置自定义退避参数。
func (s *DelegatingAuthorizationOptions) WithCustomRetryBackoff(backoff wait.Backoff) {
	s.WebhookRetryBackoff = &backoff
}

// WithCustomRoundTripper 允许为授权 webhook 客户端指定自定义 HTTP 行为的中间件函数。
func (s *DelegatingAuthorizationOptions) WithCustomRoundTripper(rt transport.WrapperFunc) {
	s.CustomRoundTripperFn = rt
}

// Validate 校验重试退避参数；nil 选项视为未启用，直接通过。
func (s *DelegatingAuthorizationOptions) Validate() []error {
	if s == nil {
		return nil
	}

	allErrors := []error{}
	// Steps<=0 会导致重试逻辑永不执行，必须在启动前拒绝。
	if s.WebhookRetryBackoff != nil && s.WebhookRetryBackoff.Steps <= 0 {
		allErrors = append(allErrors, fmt.Errorf("number of webhook retry attempts must be greater than 1, but is: %d", s.WebhookRetryBackoff.Steps))
	}

	return allErrors
}

// AddFlags 注册委托授权相关的命令行参数。
func (s *DelegatingAuthorizationOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}

	// kubeconfig 可选时在帮助信息中说明"为空即拒绝一切"的后果，避免误配后难以排查。
	var optionalKubeConfigSentence string
	if s.RemoteKubeConfigFileOptional {
		optionalKubeConfigSentence = " This is optional. If empty, all requests not skipped by authorization are forbidden."
	}
	fs.StringVar(&s.RemoteKubeConfigFile, "authorization-kubeconfig", s.RemoteKubeConfigFile,
		"kubeconfig file pointing at the 'core' kubernetes server with enough rights to create "+
			"subjectaccessreviews.authorization.k8s.io."+optionalKubeConfigSentence)

	fs.DurationVar(&s.AllowCacheTTL, "authorization-webhook-cache-authorized-ttl",
		s.AllowCacheTTL,
		"The duration to cache 'authorized' responses from the webhook authorizer.")

	fs.DurationVar(&s.DenyCacheTTL,
		"authorization-webhook-cache-unauthorized-ttl", s.DenyCacheTTL,
		"The duration to cache 'unauthorized' responses from the webhook authorizer.")

	fs.StringSliceVar(&s.AlwaysAllowPaths, "authorization-always-allow-paths", s.AlwaysAllowPaths,
		"A list of HTTP paths to skip during authorization, i.e. these are authorized without "+
			"contacting the 'core' kubernetes server.")
}

// ApplyTo 构造授权器并写入 AuthorizationInfo；nil 选项等价于允许所有请求，
// 供不需要授权的聚合 apiserver 使用。
func (s *DelegatingAuthorizationOptions) ApplyTo(c *server.AuthorizationInfo) error {
	if s == nil {
		c.Authorizer = authorizerfactory.NewAlwaysAllowAuthorizer()
		return nil
	}

	client, err := s.getClient()
	if err != nil {
		return err
	}

	c.Authorizer, err = s.toAuthorizer(client)
	return err
}

// toAuthorizer 按顺序组合授权器：特权组 -> 白名单路径 -> SubjectAccessReview webhook。
// union 采用“第一个非 NoOpinion 决策生效”的语义。
func (s *DelegatingAuthorizationOptions) toAuthorizer(client kubernetes.Interface) (authorizer.Authorizer, error) {
	var authorizers []union.NamedAuthorizer

	if len(s.AlwaysAllowGroups) > 0 {
		authorizers = append(authorizers, union.NamedAuthorizer{
			AuthorizerName: "kubernetes.io/always-allow-groups",
			Authorizer:     authorizerfactory.NewPrivilegedGroups(s.AlwaysAllowGroups...),
		})
	}

	if len(s.AlwaysAllowPaths) > 0 {
		a, err := path.NewAuthorizer(s.AlwaysAllowPaths)
		if err != nil {
			return nil, err
		}
		authorizers = append(authorizers, union.NamedAuthorizer{
			AuthorizerName: "kubernetes.io/always-allow-paths",
			Authorizer:     a,
		})
	}

	if client == nil {
		// 没有 client 时只能依赖前两个授权器；未命中者一律拒绝。
		klog.Warning("No authorization-kubeconfig provided, so SubjectAccessReview of authorization tokens won't work.")
	} else {
		cfg := authorizerfactory.DelegatingAuthorizerConfig{
			SubjectAccessReviewClient: client.AuthorizationV1(),
			AllowCacheTTL:             s.AllowCacheTTL,
			DenyCacheTTL:              s.DenyCacheTTL,
			WebhookRetryBackoff:       s.WebhookRetryBackoff,
		}
		delegatedAuthorizer, err := cfg.New()
		if err != nil {
			return nil, err
		}
		authorizers = append(authorizers, union.NamedAuthorizer{
			AuthorizerName: "kubernetes.io/webhook",
			Authorizer:     delegatedAuthorizer,
		})
	}

	return union.New(authorizers...)
}

// getClient 构造访问上层 kube-apiserver 的客户端。
func (s *DelegatingAuthorizationOptions) getClient() (kubernetes.Interface, error) {
	var clientConfig *rest.Config
	var err error
	if len(s.RemoteKubeConfigFile) > 0 {
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: s.RemoteKubeConfigFile}
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

		clientConfig, err = loader.ClientConfig()
	} else {
		// 没有远程 kubeconfig 文件时尝试使用 in-cluster 配置。大多数 addon API server 走这条路径。
		// 若配置为可选则忽略错误。
		clientConfig, err = rest.InClusterConfig()
		if err != nil && s.RemoteKubeConfigFileOptional {
			// 非 in-cluster 且明确要求可选时静默返回 nil；其他错误仍打警告便于排查。
			if err != rest.ErrNotInCluster {
				klog.Warningf("failed to read in-cluster kubeconfig for delegated authorization: %v", err)
			}
			return nil, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get delegated authorization kubeconfig: %v", err)
	}

	// 设置较高的 qps/burst 限制，因为这实际决定了 API server 的响应能力：
	// 授权是请求路径上的同步调用，QPS 过低会直接拖慢整个 apiserver。
	clientConfig.QPS = 200
	clientConfig.Burst = 400
	clientConfig.Timeout = s.ClientTimeout
	if s.CustomRoundTripperFn != nil {
		clientConfig.Wrap(s.CustomRoundTripperFn)
	}

	return kubernetes.NewForConfig(clientConfig)
}
