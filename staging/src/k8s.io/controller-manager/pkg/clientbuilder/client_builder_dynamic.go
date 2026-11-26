/*
Copyright 2018 The Kubernetes Authors.

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

package clientbuilder

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	v1authenticationapi "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	apiserverserviceaccount "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

var (
	// 动态客户端构建器的两个可调参数：
	// defaultExpirationSeconds 是 TokenRequest 的默认令牌有效期（秒），
	// 以及剩余有效期低于多少比例时触发轮换。
	defaultExpirationSeconds = int64(3600)
	// defaultLeewayPercent 定义客户端触发令牌轮换时尚剩的有效期比例；
	// leewayPercent 取值范围为 [0, 100]；提前轮换可以避免令牌刚好过期时请求失败。
	// range[0, 100]
	defaultLeewayPercent = 20
)

// 动态客户端构建器：为每个控制器创建对应的 ServiceAccount，
// 并通过 TokenRequest API 定期申请/刷新令牌，无需把长期凭据写入 kubeconfig。
type DynamicControllerClientBuilder struct {
	ClientConfig *restclient.Config

	// CoreClient 用于在需要时创建 ServiceAccount 并监听其关联的 Token，以构建控制器客户端，
	// 即创建/查询 ServiceAccount 及其 Token 的核心客户端。
	CoreClient v1core.CoreV1Interface

	// Namespace 是承载这些支撑控制器的 ServiceAccount 的命名空间；
	// 必须是特权级别很高、普通用户无法查看的受限命名空间。
	Namespace string

	// roundTripperFuncMap 为每个 ServiceAccount 缓存对应的 roundtripper 包装函数。
	// 按 ServiceAccount 缓存包装函数，保证同一账号的多个客户端共享同一个令牌源，
	// 避免每个客户端各自刷新令牌造成 TokenRequest 请求风暴。
	roundTripperFuncMap map[string]func(http.RoundTripper) http.RoundTripper

	expirationSeconds int64

	leewayPercent int

	// 保护 roundTripperFuncMap 的并发访问。
	mutex sync.Mutex

	// 可注入的时钟，便于测试中控制时间（生产环境用 RealClock）。
	clock clock.Clock
}

// NewDynamicClientBuilder 返回使用 TokenRequest 特性、并定期刷新 ServiceAccount 令牌的客户端构建器。
// 生产用的构造函数，使用默认令牌有效期与轮换比例。
func NewDynamicClientBuilder(clientConfig *restclient.Config, coreClient v1core.CoreV1Interface, ns string) ControllerClientBuilder {
	builder := &DynamicControllerClientBuilder{
		ClientConfig:        clientConfig,
		CoreClient:          coreClient,
		Namespace:           ns,
		roundTripperFuncMap: map[string]func(http.RoundTripper) http.RoundTripper{},
		expirationSeconds:   defaultExpirationSeconds,
		leewayPercent:       defaultLeewayPercent,
		// 生产环境使用真实时钟。
		clock: clock.RealClock{},
	}
	return builder
}

// NewTestDynamicClientBuilder 仅供测试使用：允许注入自定义的有效期/轮换比例，
// 以便快速验证令牌轮换逻辑，生产代码请勿调用。
func NewTestDynamicClientBuilder(clientConfig *restclient.Config, coreClient v1core.CoreV1Interface, ns string, expirationSeconds int64, leewayPercent int) ControllerClientBuilder {
	builder := &DynamicControllerClientBuilder{
		ClientConfig:        clientConfig,
		CoreClient:          coreClient,
		Namespace:           ns,
		roundTripperFuncMap: map[string]func(http.RoundTripper) http.RoundTripper{},
		expirationSeconds:   expirationSeconds,
		leewayPercent:       leewayPercent,
		clock:               clock.RealClock{},
	}
	return builder
}

// 为指定 ServiceAccount 构造 REST 配置：
// 1) 确保 ServiceAccount 存在；
// 2) 克隆基础配置并加上身份；
// 3) 为该账号取得（或复用）一个带缓存的令牌源并挂到请求传输链上。
func (t *DynamicControllerClientBuilder) Config(saName string) (*restclient.Config, error) {
	_, err := getOrCreateServiceAccount(t.CoreClient, t.Namespace, saName)
	// 先确保账号存在，否则后面申请 Token 会 404。
	if err != nil {
		return nil, err
	}

	configCopy := constructClient(t.Namespace, saName, t.ClientConfig)
	// 复制基础配置并设置 User-Agent。

	t.mutex.Lock()
	// 缓存 map 可能被多个控制器并发访问，必须加锁。
	defer t.mutex.Unlock()

	rt, ok := t.roundTripperFuncMap[saName]
	// 如果该账号已有包装函数，直接复用，保证共享同一令牌缓存。
	if ok {
		// 命中缓存：复用之前的 RoundTripper 包装。
		configCopy.Wrap(rt)
	} else {
		// 未命中缓存：创建一个 TokenRequest 令牌源，并外包一层缓存，
		// 避免每次 HTTP 请求都重新向 apiserver 申请令牌。
		cachedTokenSource := transport.NewCachedTokenSource(&tokenSourceImpl{
			namespace:          t.Namespace,
			serviceAccountName: saName,
			coreClient:         t.CoreClient,
			expirationSeconds:  t.expirationSeconds,
			leewayPercent:      t.leewayPercent,
		})
		configCopy.Wrap(transport.ResettableTokenSourceWrapTransport(cachedTokenSource))
		// 把令牌源包装进传输链，使每个请求自动携带最新令牌。
		// 把包装函数记下来，供同一账号的后续客户端复用。
		t.roundTripperFuncMap[saName] = configCopy.WrapTransport
	}

	return &configCopy, nil
}

// Config 的“出错即退出”版本。
func (t *DynamicControllerClientBuilder) ConfigOrDie(name string) *restclient.Config {
	clientConfig, err := t.Config(name)
	if err != nil {
		klog.Fatal(err)
	}
	return clientConfig
}

// 用动态配置构造 clientset。
func (t *DynamicControllerClientBuilder) Client(name string) (clientset.Interface, error) {
	clientConfig, err := t.Config(name)
	if err != nil {
		return nil, err
	}
	return clientset.NewForConfig(clientConfig)
}

// Client 的“出错即退出”版本。
func (t *DynamicControllerClientBuilder) ClientOrDie(name string) clientset.Interface {
	client, err := t.Client(name)
	if err != nil {
		klog.Fatal(err)
	}
	return client
}

// 发现客户端同样调高 Burst/QPS，理由与 SimpleBuilder 一致。
func (t *DynamicControllerClientBuilder) DiscoveryClient(name string) (discovery.DiscoveryInterface, error) {
	clientConfig, err := t.Config(name)
	if err != nil {
		return nil, err
	}
	// Discovery 请求量大但频率低，放宽突发上限可以让请求成功，并有时间补充令牌。
	// 只需几秒即可完成补充。
	clientConfig.Burst = 200
	clientConfig.QPS = 20
	return clientset.NewForConfig(clientConfig)
}

// DiscoveryClient 的“出错即退出”版本。
func (t *DynamicControllerClientBuilder) DiscoveryClientOrDie(name string) discovery.DiscoveryInterface {
	client, err := t.DiscoveryClient(name)
	if err != nil {
		klog.Fatal(err)
	}
	return client
}

// 实现 oauth2.TokenSource：每次 Token() 调用都会通过 TokenRequest API 申请新令牌。
type tokenSourceImpl struct {
	namespace          string
	serviceAccountName string
	coreClient         v1core.CoreV1Interface
	expirationSeconds  int64
	leewayPercent      int
}

// 申请令牌的完整流程：
// 1) 带指数退避重试，确保 ServiceAccount 存在；
// 2) 调用 CreateToken 申请具有指定有效期的令牌；
// 3) 根据返回的有效期和 leeway 计算提前轮换时间；
// 4) 封装为 oauth2.Token 返回。
func (ts *tokenSourceImpl) Token() (*oauth2.Token, error) {
	var retTokenRequest *v1authenticationapi.TokenRequest

	backoff := wait.Backoff{
		// 退避策略：初始 500ms，每次翻倍，共 4 次，用于容忍 apiserver 短暂不可用。
		Duration: 500 * time.Millisecond,
		Factor:   2, // double the timeout for every failure
		Steps:    4,
	}
	if err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		// 重试条件返回 (false, nil) 表示继续重试；只在成功时返回 true。
		if _, inErr := getOrCreateServiceAccount(ts.coreClient, ts.namespace, ts.serviceAccountName); inErr != nil {
			// 账号可能因清理等原因消失，每次重试都先确认/重建。
			klog.Warningf("get or create service account failed: %v", inErr)
			return false, nil
		}

		// 申请短期令牌：有效期由 expirationSeconds 指定，并随请求一起传给 apiserver。
		tr, inErr := ts.coreClient.ServiceAccounts(ts.namespace).CreateToken(context.TODO(), ts.serviceAccountName, &v1authenticationapi.TokenRequest{
			Spec: v1authenticationapi.TokenRequestSpec{
				ExpirationSeconds: ptr.To[int64](ts.expirationSeconds),
			},
		}, metav1.CreateOptions{})
		if inErr != nil {
			klog.Warningf("get token failed: %v", inErr)
			return false, nil
		}
		retTokenRequest = tr
		// 保存成功结果，退出重试循环。
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to get token for %s/%s: %v", ts.namespace, ts.serviceAccountName, err)
	}

	// 防御性检查：apiserver 未回填有效期时无法计算轮换时间，视为错误。
	if retTokenRequest.Spec.ExpirationSeconds == nil {
		return nil, fmt.Errorf("nil pointer of expiration in token request")
	}

	lifetime := retTokenRequest.Status.ExpirationTimestamp.Time.Sub(time.Now())
	// 计算令牌从现在起还剩多少有效时间。
	if lifetime < time.Minute*10 {
		// 时钟偏差可能算出很短甚至负值，统一兜底为至少 10 分钟的最短令牌生命周期。
		lifetime = time.Minute * 10
	}

	leeway := time.Duration(int64(lifetime) * int64(ts.leewayPercent) / 100)
	// 提前 leeway 比例的时间轮换，留出刷新所需的余量。
	expiry := time.Now().Add(lifetime).Add(-1 * leeway)
	// oauth2 缓存会在 Expiry 到来前重新获取令牌，从而实现自动轮换。

	return &oauth2.Token{
		AccessToken: retTokenRequest.Status.Token,
		TokenType:   "Bearer",
		Expiry:      expiry,
	}, nil
}

// 构造客户端配置：生成 ServiceAccount 形式的用户名，浅拷贝基础配置并设置 User-Agent。
// 浅拷贝保留了基础配置上的 HTTP Wrap 函数，便于未来扩展。
func constructClient(saNamespace, saName string, config *restclient.Config) restclient.Config {
	username := apiserverserviceaccount.MakeUsername(saNamespace, saName)
	// 生成形如 system:serviceaccount:<ns>:<name> 的用户名，作为 User-Agent 标识。
	// 这里对配置做浅拷贝：调用方已在创建时清理过配置，本函数不修改原对象，
	// 保留 HTTP wrappers 等扩展点，便于未来实现按请求的自定义行为。
	ret := *config
	restclient.AddUserAgent(&ret, username)
	return ret
}

// 获取或创建 ServiceAccount：
// 1) 先 Get，存在则直接返回；
// 2) 不存在时确保 Namespace 存在（容忍无权限）；
// 3) 创建 SA；若遇到并发创建（AlreadyExists）则重新 Get。
func getOrCreateServiceAccount(coreClient v1core.CoreV1Interface, namespace, name string) (*v1.ServiceAccount, error) {
	sa, err := coreClient.ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	// 快路径：账号已存在。
	if err == nil {
		return sa, nil
	}
	if !apierrors.IsNotFound(err) {
		// 除了 NotFound 之外的错误都直接返回，不擅自创建。
		return nil, err
	}

	// 无法确认命名空间存在时尝试创建；本组件可能没有创建命名空间的权限，因此容忍失败。
	if _, err := coreClient.Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		if _, err = coreClient.Namespaces().Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			klog.Warningf("create non-exist namespace %s failed:%v", namespace, err)
		}
	}

	// 真正创建 ServiceAccount。
	sa, err = coreClient.ServiceAccounts(namespace).Create(context.TODO(), &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// 并发初始化时别的 goroutine 可能已创建，重新读取即可。
		return coreClient.ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	}
	return sa, err
}
