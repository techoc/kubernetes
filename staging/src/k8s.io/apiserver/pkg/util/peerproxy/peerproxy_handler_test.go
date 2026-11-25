/*
Copyright 2023 The Kubernetes Authors.

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

package peerproxy

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/reconcilers"
	"k8s.io/apiserver/pkg/util/peerproxy/metrics"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	apifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

const (
	requestTimeout  = 30 * time.Second
	localServerID   = "local-apiserver"
	remoteServerID1 = "remote-apiserver-1"
	remoteServerID2 = "remote-apiserver-2"
)

// server 是测试中模拟的对等 apiserver 的对外地址与身份。
type server struct {
	publicIP string
	serverID string
}

// reconciler 描述是否为用例写入 lease，以及写入哪些对等 apiserver 的端点。
type reconciler struct {
	do      bool
	servers []server
}

// TestPeerProxy 验证对等代理的决策与指标。
//
// 覆盖的边界包括——
//   - 非资源请求、已带代理标记的请求、informer 尚未同步：一律本地处理，避免代理环路与启动期误转发；
//   - 本地可服务：本地处理；
//   - 本地不可服务且存在合适的对等端：转发；对等端不可达或无端点时返回 503 并打点。
//
// 指标断言是行为契约的一部分，确保运维能观察到转发与失败。
func TestPeerProxy(t *testing.T) {
	testCases := []struct {
		desc                 string
		informerFinishedSync bool
		requestPath          string
		peerproxiedHeader    string
		reconcilerConfig     reconciler
		localCache           map[schema.GroupVersionResource]bool
		peerCache            map[string]PeerDiscoveryCacheEntry
		wantStatus           int
		wantMetricsData      string
	}{
		{
			desc:        "allow non resource requests",
			requestPath: "/foo/bar/baz",
			wantStatus:  http.StatusOK,
		},
		{
			desc:              "allow if already proxied once",
			requestPath:       "/api/bar/baz",
			peerproxiedHeader: "true",
			wantStatus:        http.StatusOK,
		},
		{
			desc:                 "allow if unsynced informers",
			requestPath:          "/api/bar/baz",
			informerFinishedSync: false,
			wantStatus:           http.StatusOK,
		},
		{
			desc:        "Serve locally if serviceable",
			requestPath: "/api/foo/bar",
			localCache: map[schema.GroupVersionResource]bool{
				{Group: "", Version: "foo", Resource: "bar"}: true,
			},
			wantStatus: http.StatusOK,
		},
		{
			desc:                 "200 if no appropriate peers found, serve locally",
			requestPath:          "/api/foo/bar",
			informerFinishedSync: true,
			wantStatus:           http.StatusOK,
		},
		{
			desc:                 "503 if no endpoint fetched from lease",
			requestPath:          "/api/foo/bar",
			informerFinishedSync: true,
			peerCache: map[string]PeerDiscoveryCacheEntry{
				remoteServerID1: {
					GVRs: map[schema.GroupVersionResource]bool{
						{Group: "", Version: "foo", Resource: "bar"}: true,
					},
				},
			},
			wantStatus: http.StatusServiceUnavailable,
			wantMetricsData: `
					   # HELP apiserver_peer_proxy_errors_total [ALPHA] Total number of errors encountered while proxying requests to a peer kube apiserver
					   # TYPE apiserver_peer_proxy_errors_total counter
					   apiserver_peer_proxy_errors_total{group="",resource="bar",type="endpoint_resolution",version="foo"} 1
					   `,
		},
		{
			desc:                 "503 unreachable peer bind address",
			requestPath:          "/api/foo/bar",
			informerFinishedSync: true,
			peerCache: map[string]PeerDiscoveryCacheEntry{
				remoteServerID1: {
					GVRs: map[schema.GroupVersionResource]bool{
						{Group: "", Version: "foo", Resource: "bar"}: true,
					},
				},
			},
			reconcilerConfig: reconciler{
				do: true,
				servers: []server{
					{
						publicIP: "1.2.3.4",
						serverID: remoteServerID1,
					},
				},
			},
			wantStatus: http.StatusServiceUnavailable,
			wantMetricsData: `
					   # HELP apiserver_rerouted_request_total [ALPHA] Total number of requests that were proxied to a peer kube-apiserver because the local apiserver was not capable of serving it, broken down by 'group', 'version', and 'resource' indicating the GVR of the request. If all three are empty (""), the request is a discovery request.
					   # TYPE apiserver_rerouted_request_total counter
					   apiserver_rerouted_request_total{code="503",group="",resource="bar",version="foo"} 1
					   `,
		},
		{
			desc:                 "503 if one apiserver's endpoint lease wasnt found but another valid (unreachable) apiserver was found",
			requestPath:          "/api/foo/bar",
			informerFinishedSync: true,
			peerCache: map[string]PeerDiscoveryCacheEntry{
				remoteServerID1: {
					GVRs: map[schema.GroupVersionResource]bool{
						{Group: "", Version: "foo", Resource: "bar"}: true,
					},
				},
				remoteServerID2: {
					GVRs: map[schema.GroupVersionResource]bool{
						{Group: "", Version: "foo", Resource: "bar"}: true,
					},
				},
			},
			reconcilerConfig: reconciler{
				do: true,
				servers: []server{
					{
						publicIP: "1.2.3.4",
						serverID: remoteServerID1,
					},
				},
			},
			wantStatus: http.StatusServiceUnavailable,
			wantMetricsData: `
					   # HELP apiserver_rerouted_request_total [ALPHA] Total number of requests that were proxied to a peer kube-apiserver because the local apiserver was not capable of serving it, broken down by 'group', 'version', and 'resource' indicating the GVR of the request. If all three are empty (""), the request is a discovery request.
					   # TYPE apiserver_rerouted_request_total counter
					   apiserver_rerouted_request_total{code="503",group="",resource="bar",version="foo"} 1
					   `,
		},
	}

	metrics.Register()
	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			// 全局指标在每个用例后清零，避免计数累积影响断言。
			defer metrics.Reset()
			lastHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("OK"))
			})
			serverIDs := []string{localServerID}
			for peerID := range tt.peerCache {
				serverIDs = append(serverIDs, peerID)
			}
			fakeReconciler := newFakePeerEndpointReconciler(t)
			handler := newHandlerChain(t, tt.informerFinishedSync, lastHandler, fakeReconciler, tt.localCache, tt.peerCache)
			server, requestGetter := createHTTP2ServerWithClient(handler, requestTimeout*2)
			defer server.Close()

			if tt.reconcilerConfig.do {
				// 端点 lease 只有在 APIServerIdentity 开启时才会被写入。
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.APIServerIdentity, true)

				for _, s := range tt.reconcilerConfig.servers {
					err := fakeReconciler.UpdateLease(s.serverID,
						s.publicIP,
						[]corev1.EndpointPort{{Name: "foo",
							Port: 8080, Protocol: "TCP"}})
					if err != nil {
						t.Errorf("Failed to update lease for server %s", s.serverID)
					}
				}
			}

			req, err := http.NewRequest(http.MethodGet, server.URL+tt.requestPath, nil)
			if err != nil {
				t.Fatalf("failed to create new http request - %v", err)
			}
			// 模拟「已被其他 apiserver 代理过一次」的标记，用于防止代理环路。
			req.Header.Set(PeerProxiedHeader, tt.peerproxiedHeader)

			resp, _ := requestGetter(req)

			// compare response
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			// compare metric
			if tt.wantMetricsData != "" {
				if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tt.wantMetricsData), []string{"apiserver_rerouted_request_total", "apiserver_peer_proxy_errors_total"}...); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

}

// TestGetPeerResources 验证聚合对等端发现信息时会剔除本地条目。
// 本地 server ID 的发现数据已经由本机提供，若再合并会造成重复；
// 多对等端场景则应按 server ID 分组保留各自结果。
func TestGetPeerResources(t *testing.T) {
	testCases := []struct {
		name      string
		peerCache map[string]PeerDiscoveryCacheEntry
		want      map[string][]apidiscoveryv2.APIGroupDiscovery
	}{
		{
			name:      "empty peer cache",
			peerCache: nil,
			want:      map[string][]apidiscoveryv2.APIGroupDiscovery{},
		},
		{
			name: "peer cache with local server, should be skipped",
			peerCache: map[string]PeerDiscoveryCacheEntry{
				localServerID: {
					GroupDiscovery: []apidiscoveryv2.APIGroupDiscovery{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "core"},
						},
					},
				},
				remoteServerID1: {
					GroupDiscovery: []apidiscoveryv2.APIGroupDiscovery{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "apps"},
						},
					},
				},
			},
			want: map[string][]apidiscoveryv2.APIGroupDiscovery{
				remoteServerID1: {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "apps"},
					},
				},
			},
		},
		{
			name: "peer cache with multiple peers",
			peerCache: map[string]PeerDiscoveryCacheEntry{
				remoteServerID1: {
					GroupDiscovery: []apidiscoveryv2.APIGroupDiscovery{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "apps"},
						},
						{
							ObjectMeta: metav1.ObjectMeta{Name: "foo"},
						},
					},
				},
				remoteServerID2: {
					GroupDiscovery: []apidiscoveryv2.APIGroupDiscovery{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "batch"},
						},
					},
				},
			},
			want: map[string][]apidiscoveryv2.APIGroupDiscovery{
				remoteServerID1: {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "apps"},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "foo"},
					},
				},
				remoteServerID2: {
					{
						ObjectMeta: metav1.ObjectMeta{Name: "batch"},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			s := serializer.NewCodecFactory(runtime.NewScheme()).WithoutConversion()
			handler, err := newFakePeerProxyHandler(true, nil, localServerID, s, nil, tt.peerCache)
			if err != nil {
				t.Fatalf("Error creating peer proxy handler: %v", err)
			}

			got := handler.GetPeerResources()
			assert.Equal(t, tt.want, got)
		})
	}
}

// newFakePeerEndpointReconciler 基于真实 etcd 测试实例构造端点 lease 协调器。
// 使用随机 baseKey 隔离各用例的数据；leaseTime 取 1 分钟，
// 确保测试期间 lease 不会过期。
func newFakePeerEndpointReconciler(t *testing.T) reconcilers.PeerEndpointLeaseReconciler {
	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })
	scheme := runtime.NewScheme()
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(corev1.SchemeGroupVersion))
	codecs := serializer.NewCodecFactory(scheme)
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)
	config := *sc.ForResource(schema.GroupResource{Resource: "endpoints"})
	baseKey := "/" + uuid.New().String() + "/peer-testleases/"
	leaseTime := 1 * time.Minute
	reconciler, err := reconcilers.NewPeerEndpointLeaseReconciler(&config, baseKey, leaseTime)
	if err != nil {
		t.Fatalf("Error creating storage: %v", err)
	}
	return reconciler
}

// newHandlerChain 组装与生产一致的过滤链：RequestInfo → 用户信息 → peerproxy。
// 顺序不可颠倒——peerproxy 依赖 RequestInfo 判断是否可本地服务，
// 也依赖已注入的用户信息做授权/审计。
func newHandlerChain(t *testing.T, informerFinishedSync bool, handler http.Handler,
	reconciler reconcilers.PeerEndpointLeaseReconciler,
	localCache map[schema.GroupVersionResource]bool, peerCache map[string]PeerDiscoveryCacheEntry) http.Handler {
	// Add peerproxy handler
	s := serializer.NewCodecFactory(runtime.NewScheme()).WithoutConversion()
	peerProxyHandler, err := newFakePeerProxyHandler(informerFinishedSync, reconciler, localServerID, s, localCache, peerCache)
	if err != nil {
		t.Fatalf("Error creating peer proxy handler: %v", err)
	}
	peerProxyHandler.finishedSync.Store(informerFinishedSync)
	handler = peerProxyHandler.WrapHandler(handler)

	// Add user info
	handler = withFakeUser(handler)

	// Add requestInfo handler
	requestInfoFactory := &apirequest.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	handler = apifilters.WithRequestInfo(handler, requestInfoFactory)
	return handler
}

// newFakePeerProxyHandler 构造被测 handler，并直接注入本地/对等端发现缓存，
// 跳过真实 informer 同步，使用例聚焦于转发决策逻辑。
func newFakePeerProxyHandler(informerFinishedSync bool,
	reconciler reconcilers.PeerEndpointLeaseReconciler, id string, s runtime.NegotiatedSerializer,
	localCache map[schema.GroupVersionResource]bool, peerCache map[string]PeerDiscoveryCacheEntry) (*peerProxyHandler, error) {
	clientset := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)
	leaseInformer := informerFactory.Coordination().V1().Leases()
	clientConfig := &transport.Config{
		TLS: transport.TLSConfig{
			Insecure: false,
		}}
	loopbackClientConfig := &rest.Config{
		Host: "localhost:1010",
	}
	ppH, err := NewPeerProxyHandler(id, "identity=testserver", leaseInformer, reconciler, s, loopbackClientConfig, clientConfig)
	if err != nil {
		return nil, err
	}
	ppH.localDiscoveryInfoCache.Store(localCache)
	ppH.gvExclusionManager.filteredPeerDiscoveryCache.Store(peerCache)

	ppH.finishedSync.Store(informerFinishedSync)
	return ppH, nil
}

// withFakeUser 注入一个仅带 Groups 的用户，模拟认证过滤器已完成的请求。
func withFakeUser(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(apirequest.WithUser(r.Context(), &user.DefaultInfo{
			Groups: r.Header["Groups"],
		}))
		handler.ServeHTTP(w, r)
	})
}

// 返回一个已启动的 http2 server，以及一个可向该 server 发请求的 client 函数。
// 必须启用 HTTP/2——对等代理在转发流式请求时依赖 HTTP/2 的多路复用与
// 流式语义；用 HTTP/1.1 测试会掩盖相关问题。
func createHTTP2ServerWithClient(handler http.Handler, clientTimeout time.Duration) (*httptest.Server, func(req *http.Request) (*http.Response, error)) {
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	cli := server.Client()
	cli.Timeout = clientTimeout
	return server, func(req *http.Request) (*http.Response, error) {
		return cli.Do(req)
	}
}
