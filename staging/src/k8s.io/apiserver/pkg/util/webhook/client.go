/*
Copyright 2017 The Kubernetes Authors.

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

package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/x509metrics"
	"k8s.io/client-go/rest"
	"k8s.io/utils/lru"
	netutils "k8s.io/utils/net"
)

const (
	// defaultCacheSize 限制缓存客户端数量；webhook 配置数量通常很少，
	// 200 足以覆盖且防止配置频繁变更导致无界增长。
	defaultCacheSize = 200
)

// ClientConfig 是构建 webhook 客户端所需的最小配置，取值来自
// admissionregistration 等 API 的 webhook 定义。
type ClientConfig struct {
	Name     string
	URL      string
	CABundle []byte
	Service  *ClientConfigService
}

// ClientConfigService 定义 Webhook 的服务发现参数。
type ClientConfigService struct {
	Name      string
	Namespace string
	Path      string
	Port      int32
}

// ClientManager 构建与 webhook 通信的 REST 客户端，并缓存客户端以避免重复创建。
// 缓存键基于配置内容的 JSON（忽略 Name），因此多个 webhook 共用同一
// service/URL+CA 时只创建一个连接池，避免连接爆炸。
type ClientManager struct {
	authInfoResolver     AuthenticationInfoResolver
	serviceResolver      ServiceResolver
	negotiatedSerializer runtime.NegotiatedSerializer
	cache                *lru.Cache
}

// NewClientManager 创建 ClientManager。
// gvs 决定 webhook 请求/响应使用的编解码版本；额外的 addToSchemaFuncs
// 用于注册 webhook 负载中出现的类型。
func NewClientManager(gvs []schema.GroupVersion, addToSchemaFuncs ...func(s *runtime.Scheme) error) (ClientManager, error) {
	cache := lru.New(defaultCacheSize)
	hookScheme := runtime.NewScheme()
	for _, addToSchemaFunc := range addToSchemaFuncs {
		if err := addToSchemaFunc(hookScheme); err != nil {
			return ClientManager{}, err
		}
	}
	return ClientManager{
		cache: cache,
		negotiatedSerializer: serializer.NegotiatedSerializerWrapper(runtime.SerializerInfo{
			Serializer: serializer.NewCodecFactory(hookScheme).LegacyCodec(gvs...),
		}),
	}, nil
}

// SetAuthenticationInfoResolverWrapper 设置 AuthenticationInfoResolverWrapper。
// 装饰器模式：在保留已有 resolver 的前提下叠加身份/凭据逻辑。
func (cm *ClientManager) SetAuthenticationInfoResolverWrapper(wrapper AuthenticationInfoResolverWrapper) {
	if wrapper != nil {
		cm.authInfoResolver = wrapper(cm.authInfoResolver)
	}
}

// 设置 AuthenticationInfoResolver。
func (cm *ClientManager) SetAuthenticationInfoResolver(resolver AuthenticationInfoResolver) {
	cm.authInfoResolver = resolver
}

// 设置 ServiceResolver。
func (cm *ClientManager) SetServiceResolver(sr ServiceResolver) {
	if sr != nil {
		cm.serviceResolver = sr
	}
}

// Validate 检查 ClientManager 是否正确装配：一次性汇总所有缺失依赖，便于启动阶段一次修复。
func (cm *ClientManager) Validate() error {
	var errs []error
	if cm.negotiatedSerializer == nil {
		errs = append(errs, fmt.Errorf("the clientManager requires a negotiatedSerializer"))
	}
	if cm.serviceResolver == nil {
		errs = append(errs, fmt.Errorf("the clientManager requires a serviceResolver"))
	}
	if cm.authInfoResolver == nil {
		errs = append(errs, fmt.Errorf("the clientManager requires an authInfoResolver"))
	}
	return utilerrors.NewAggregate(errs)
}

// 从缓存取 RESTClient，或按 Webhook 配置新建一个。
func (cm *ClientManager) HookClient(cc ClientConfig) (*rest.RESTClient, error) {
	// Name 不参与缓存键：同一目标的多个 webhook 应复用同一个客户端。
	ccWithNoName := cc
	ccWithNoName.Name = ""
	cacheKey, err := json.Marshal(ccWithNoName)
	if err != nil {
		return nil, err
	}
	if client, ok := cm.cache.Get(string(cacheKey)); ok {
		return client.(*rest.RESTClient), nil
	}

	cfg, err := cm.hookClientConfig(cc)
	if err != nil {
		return nil, err
	}

	// 仅缓存创建成功的客户端；失败重试时不缓存，以便配置修正后能重新建立。
	client, err := rest.UnversionedRESTClientFor(cfg)
	if err == nil {
		cm.cache.Add(string(cacheKey), client)
	}
	return client, err
}

// hookClientConfig 根据 service 或 URL 两种模式构建 rest.Config。
func (cm *ClientManager) hookClientConfig(cc ClientConfig) (*rest.Config, error) {
	complete := func(cfg *rest.Config) (*rest.Config, error) {
		// 避免对 webhook 后端进行客户端限流；限流应当发生在决定服务多少请求的时候。
		// webhook 调用属于请求处理的一部分，客户端限流会把排队成本转嫁为请求失败。
		cfg.QPS = -1

		// 将配置中的 CAData 与 webhook 自带的既有 CA bundle 合并（追加而非覆盖），以便同时信任集群 CA 与 webhook 自带 CA。
		if len(cfg.TLSClientConfig.CAData) > 0 {
			cfg.TLSClientConfig.CAData = append(cfg.TLSClientConfig.CAData, '\n')
		}
		cfg.TLSClientConfig.CAData = append(cfg.TLSClientConfig.CAData, cc.CABundle...)

		cfg.ContentConfig.NegotiatedSerializer = cm.negotiatedSerializer
		cfg.ContentConfig.ContentType = runtime.ContentTypeJSON

		// 添加一个 transport 包装器，用于检测服务端证书带有已弃用特征的 TLS 连接。
		// 用于告警弱证书（缺少 SAN、SHA1），而不是拒绝连接，保持向后兼容。
		cfg.Wrap(x509metrics.NewDeprecatedCertificateRoundTripperWrapperConstructor(
			x509MissingSANCounter,
			x509InsecureSHA1Counter,
		))
		return cfg, nil
	}

	if cc.Service != nil {
		port := cc.Service.Port
		if port == 0 {
			// 未指定服务端口时默认 443
			port = 443
		}

		restConfig, err := cm.authInfoResolver.ClientConfigForService(cc.Service.Name, cc.Service.Namespace, int(port))
		if err != nil {
			return nil, err
		}
		cfg := rest.CopyConfig(restConfig)

		// 强制使用 http/1.1 而非 http/2：后者不会把并发请求均衡到多个后端，
		// 这是针对该问题的 workaround。
		// 详见 https://issue.k8s.io/75791。
		cfg.NextProtos = []string{"http/1.1"}

		serverName := cc.Service.Name + "." + cc.Service.Namespace + ".svc"

		host := net.JoinHostPort(serverName, strconv.Itoa(int(port)))
		cfg.Host = "https://" + host
		cfg.APIPath = cc.Service.Path
		// 若尚未设置则填充 TLS ServerName：它必须指向 service DNS 名而非解析后的 Pod IP，否则证书校验会失败。
		if len(cfg.TLSClientConfig.ServerName) == 0 {
			cfg.TLSClientConfig.ServerName = serverName
		}

		if !utilfeature.DefaultFeatureGate.Enabled(features.WebhookRoundTripLoadBalancing) {
			// 旧实现：在 Dial 时把 service 名替换为随机后端地址。
			delegateDialer := cfg.Dial
			if delegateDialer == nil {
				var d net.Dialer
				delegateDialer = d.DialContext
			}
			cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == host {
					u, err := cm.serviceResolver.ResolveEndpoint(cc.Service.Namespace, cc.Service.Name, port)
					if err != nil {
						return nil, err
					}
					addr = u.Host
				}
				return delegateDialer(ctx, network, addr)
			}
		} else {
			// 使用自定义 roundtripper，因为 http transport 会按 URL 的 Host 缓存连接。service resolver 提供的是实际端点地址，
			// 若改用自定义 dialer，被缓存的连接可能不会被关闭。
			// 在 RoundTrip 层解析可让每个请求重新做负载均衡，
			// 代价是请求 URL 的 Host 与连接目标不一致，需要显式处理 Host 头。
			cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
				return &resolvingRoundTripper{
					delegate:        rt,
					serviceResolver: cm.serviceResolver,
					namespace:       cc.Service.Namespace,
					serviceName:     cc.Service.Name,
					port:            port,
				}
			})
		}

		return complete(cfg)
	}

	if cc.URL == "" {
		return nil, &ErrCallingWebhook{WebhookName: cc.Name, Reason: errors.New("webhook configuration must have either service or URL")}
	}

	u, err := url.Parse(cc.URL)
	if err != nil {
		return nil, &ErrCallingWebhook{WebhookName: cc.Name, Reason: fmt.Errorf("Unparsable URL: %v", err)}
	}

	hostPort := u.Host
	if len(u.Port()) == 0 {
		// 未指定端口时默认 443
		hostPort = net.JoinHostPort(hostPort, "443")
	}

	restConfig, err := cm.authInfoResolver.ClientConfigFor(hostPort)
	if err != nil {
		return nil, err
	}

	cfg := rest.CopyConfig(restConfig)
	cfg.Host = u.Scheme + "://" + u.Host
	cfg.APIPath = u.Path
	// 对非本机地址禁用 h2：与 service 模式相同的负载均衡考量。
	if !isLocalHost(u) {
		cfg.NextProtos = []string{"http/1.1"}
	}

	return complete(cfg)
}

// isLocalHost 判断 URL 是否指向 localhost/环回地址；
// 本机连接不存在负载均衡问题，可以保留 h2。
func isLocalHost(u *url.URL) bool {
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}

	netIP := netutils.ParseIPSloppy(host)
	if netIP != nil {
		return netIP.IsLoopback()
	}
	return false
}

// resolvingRoundTripper 解析服务对应的端点地址，并改写请求 URL 指向该地址。
type resolvingRoundTripper struct {
	delegate        http.RoundTripper
	serviceResolver ServiceResolver
	namespace       string
	serviceName     string
	port            int32
}

// RoundTrip 仅对指向该 service 的请求做解析替换，其余请求原样透传。
func (r *resolvingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	serverName := r.serviceName + "." + r.namespace + ".svc"
	host := net.JoinHostPort(serverName, strconv.Itoa(int(r.port)))
	if req.URL.Host != host {
		return r.delegate.RoundTrip(req)
	}
	u, err := r.serviceResolver.ResolveEndpoint(r.namespace, r.serviceName, r.port)
	if err != nil {
		return nil, err
	}
	newReq := req.Clone(req.Context())
	// 保留原始 Host 头：只有保留 Host，后端基于 service 名的虚拟主机/证书才能正常工作。
	if len(newReq.Host) == 0 {
		newReq.Host = req.URL.Host
	}
	newReq.URL.Host = u.Host
	return r.delegate.RoundTrip(newReq)
}

// WrappedRoundTripper 返回被包装的 transport，满足通用装饰器接口。
func (r *resolvingRoundTripper) WrappedRoundTripper() http.RoundTripper {
	return r.delegate
}
