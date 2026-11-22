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

package healthcheck

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/dump"
	"k8s.io/utils/ptr"
)

// testNodeName 是测试中使用的节点名称，用于构造带节点信息的假 Node 对象。
const testNodeName = "test-node"

// fakeListener 是 net.Listener 工厂的测试替身，
// 用集合记录当前“打开”的监听地址，避免测试真正占用端口。
type fakeListener struct {
	openPorts sets.Set[string]
}

// newFakeListener 创建一个尚未打开任何端口的 fakeListener。
func newFakeListener() *fakeListener {
	return &fakeListener{
		openPorts: sets.Set[string]{},
	}
}

// hasPort 报告 addr 是否已被该假监听器记录为监听中。
func (fake *fakeListener) hasPort(addr string) bool {
	return fake.openPorts.Has(addr)
}

// Listen 把 addrs 记录为已打开端口并返回对应的假监听器。
// 使用假监听器而非真实 socket，保证测试不依赖本机端口是否可用。
func (fake *fakeListener) Listen(_ context.Context, network string, addrs ...string) (net.Listener, error) {
	fake.openPorts.Insert(addrs...)
	return &fakeNetListener{
		parent: fake,
		addrs:  addrs,
	}, nil
}

// fakeNetListener 是 fakeListener 创建出的 net.Listener 替身，
// 关闭时会从父监听器中移除自己占用的地址。
type fakeNetListener struct {
	parent *fakeListener
	addrs  []string
}

// fakeAddr 是测试用的 net.Addr 实现，提供固定的网络类型与字符串表示。
type fakeAddr struct {
}

// Network 返回假地址的网络类型，这里固定为 tcp。
func (fa fakeAddr) Network() string {
	return "tcp"
}

// String 返回假地址的字符串表示，仅供测试输出使用。
func (fa fakeAddr) String() string {
	return "<test>"
}

// Accept 无需真正实现：健康检查测试不通过该监听器接受连接。
func (fake *fakeNetListener) Accept() (net.Conn, error) {
	// 未实现：测试中不会通过该监听器接受连接。
	return nil, nil
}

// Close 从父监听器中移除该监听器占用的所有地址，
// 使端口计数随服务删除而回落。
func (fake *fakeNetListener) Close() error {
	fake.parent.openPorts.Delete(fake.addrs...)
	return nil
}

// Addr 返回假地址，仅用于满足 net.Listener 接口。
func (fake *fakeNetListener) Addr() net.Addr {
	// 未实现：测试不关心监听地址的具体内容。
	return fakeAddr{}
}

// fakeHTTPServerFactory 是 httpServer 工厂的测试替身，
// 创建的服务器不会真正监听端口或处理连接。
type fakeHTTPServerFactory struct{}

// newFakeHTTPServerFactory 创建一个假 HTTP 服务器工厂。
func newFakeHTTPServerFactory() *fakeHTTPServerFactory {
	return &fakeHTTPServerFactory{}
}

// New 保存传入的 handler，便于测试直接调用它来验证响应内容。
func (fake *fakeHTTPServerFactory) New(handler http.Handler) httpServer {
	return &fakeHTTPServer{
		handler: handler,
	}
}

// fakeHTTPServer 是 httpServer 的测试替身，只保留被测 handler。
type fakeHTTPServer struct {
	handler http.Handler
}

// Serve 立即返回，让启动 HTTP 服务的 goroutine 自然退出。
func (fake *fakeHTTPServer) Serve(listener net.Listener) error {
	return nil // 让启动 HTTP 服务的 goroutine 直接返回
}

// Close 是无操作的关闭实现，仅用于满足 httpServer 接口。
func (fake *fakeHTTPServer) Close() error {
	return nil
}

// mknsn 是构造 NamespacedName 的测试辅助函数，减少重复的字面量。
func mknsn(ns, name string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: ns,
		Name:      name,
	}
}

// hcPayload 描述服务健康检查接口返回的 JSON 负载结构。
type hcPayload struct {
	Service struct {
		Namespace string
		Name      string
	}
	LocalEndpoints      int
	ServiceProxyHealthy bool
}

// fakeProxyHealthChecker 是可控的 ProxyHealthChecker 替身，
// 通过 healthy 字段模拟 kube-proxy 整体健康状态的变化。
type fakeProxyHealthChecker struct {
	healthy bool
}

// Health 返回仅包含 healthy 字段的 ProxyHealth；
// 服务健康检查测试只关心这一个字段。
func (fake fakeProxyHealthChecker) Health() ProxyHealth {
	// 服务健康检查测试只需要 healthy 字段。
	return ProxyHealth{Healthy: fake.healthy}
}

// TestServer 验证 service health server 在服务与端点增删时的状态同步、
// 端口监听情况，以及 /healthz 的响应是否符合预期。
func TestServer(t *testing.T) {
	listener := newFakeListener()
	httpFactory := newFakeHTTPServerFactory()
	nodePortAddresses := proxyutil.NewNodePortAddresses(v1.IPv4Protocol, []string{})
	proxyChecker := &fakeProxyHealthChecker{true}

	hcsi := newServiceHealthServer("hostname", nil, listener, httpFactory, nodePortAddresses, proxyChecker, v1.IPv4Protocol)
	hcs := hcsi.(*server)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 同步空集合：不应产生任何服务或端口
	hcs.SyncServices(nil)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}
	hcs.SyncEndpoints(nil)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 同步未知服务的端点时应被忽略
	hcs.SyncEndpoints(map[types.NamespacedName]int{mknsn("a", "b"): 93})
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 同步一个真实服务：应创建监听器并打开对应端口
	nsn := mknsn("a", "b")
	hcs.SyncServices(map[types.NamespacedName]uint16{nsn: 9376})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	if len(listener.openPorts) != 1 {
		t.Errorf("expected 1 open port, got %d\n%s", len(listener.openPorts), dump.Pretty(listener.openPorts))
	}
	if !listener.hasPort("0.0.0.0:9376") {
		t.Errorf("expected port :9376 to be open\n%s", dump.Pretty(listener.openPorts))
	}
	// 验证 handler 返回值
	testHandler(hcs, nsn, http.StatusServiceUnavailable, 0, t)

	// 同步非零端点后应返回 200
	hcs.SyncEndpoints(map[types.NamespacedName]int{nsn: 18})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 18 {
		t.Errorf("expected 18 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	// 验证 handler 返回值
	testHandler(hcs, nsn, http.StatusOK, 18, t)

	// 端点归零后应返回 503
	hcs.SyncEndpoints(map[types.NamespacedName]int{nsn: 0})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	// 验证 handler 返回值
	testHandler(hcs, nsn, http.StatusServiceUnavailable, 0, t)

	// 恢复端点
	hcs.SyncEndpoints(map[types.NamespacedName]int{nsn: 11})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 11 {
		t.Errorf("expected 18 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	// 传入 nil 端点映射同样应清零端点数
	hcs.SyncEndpoints(nil)
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	// 验证 handler 返回值
	testHandler(hcs, nsn, http.StatusServiceUnavailable, 0, t)

	// 再次恢复端点
	hcs.SyncEndpoints(map[types.NamespacedName]int{nsn: 18})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 18 {
		t.Errorf("expected 18 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	// 清空服务映射应删除所有服务
	hcs.SyncServices(nil)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 一次同步多个服务
	nsn1 := mknsn("a", "b")
	nsn2 := mknsn("c", "d")
	nsn3 := mknsn("e", "f")
	nsn4 := mknsn("g", "h")
	hcs.SyncServices(map[types.NamespacedName]uint16{
		nsn1: 9376,
		nsn2: 12909,
		nsn3: 11113,
	})
	if len(hcs.services) != 3 {
		t.Errorf("expected 3 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn1].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn1].endpoints)
	}
	if hcs.services[nsn2].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn2].endpoints)
	}
	if hcs.services[nsn3].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn3].endpoints)
	}
	if len(listener.openPorts) != 3 {
		t.Errorf("expected 3 open ports, got %d\n%s", len(listener.openPorts), dump.Pretty(listener.openPorts))
	}
	// 逐个验证 handler 响应
	testHandler(hcs, nsn1, http.StatusServiceUnavailable, 0, t)
	testHandler(hcs, nsn2, http.StatusServiceUnavailable, 0, t)
	testHandler(hcs, nsn3, http.StatusServiceUnavailable, 0, t)

	// 同步端点
	hcs.SyncEndpoints(map[types.NamespacedName]int{
		nsn1: 9,
		nsn2: 3,
		nsn3: 7,
	})
	if len(hcs.services) != 3 {
		t.Errorf("expected 3 services, got %d", len(hcs.services))
	}
	if hcs.services[nsn1].endpoints != 9 {
		t.Errorf("expected 9 endpoints, got %d", hcs.services[nsn1].endpoints)
	}
	if hcs.services[nsn2].endpoints != 3 {
		t.Errorf("expected 3 endpoints, got %d", hcs.services[nsn2].endpoints)
	}
	if hcs.services[nsn3].endpoints != 7 {
		t.Errorf("expected 7 endpoints, got %d", hcs.services[nsn3].endpoints)
	}
	// 逐个验证 handler 响应
	testHandler(hcs, nsn1, http.StatusOK, 9, t)
	testHandler(hcs, nsn2, http.StatusOK, 3, t)
	testHandler(hcs, nsn3, http.StatusOK, 7, t)

	// 同步变更后的服务集合：删除、保留、改端口与新增
	hcs.SyncServices(map[types.NamespacedName]uint16{
		//nsn1: 9376, // 删除它
		nsn2: 12909, // 保持不变
		nsn3: 11114, // 修改端口
		nsn4: 11878, // 新增服务
	})
	if len(hcs.services) != 3 {
		t.Errorf("expected 3 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn2].endpoints != 3 {
		t.Errorf("expected 3 endpoints, got %d", hcs.services[nsn2].endpoints)
	}
	if hcs.services[nsn3].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn3].endpoints)
	}
	if hcs.services[nsn4].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn4].endpoints)
	}
	// 逐个验证 handler 响应
	testHandler(hcs, nsn2, http.StatusOK, 3, t)
	testHandler(hcs, nsn3, http.StatusServiceUnavailable, 0, t)
	testHandler(hcs, nsn4, http.StatusServiceUnavailable, 0, t)

	// 同步端点
	hcs.SyncEndpoints(map[types.NamespacedName]int{
		nsn1: 9,
		nsn2: 3,
		nsn3: 7,
		nsn4: 6,
	})
	if len(hcs.services) != 3 {
		t.Errorf("expected 3 services, got %d", len(hcs.services))
	}
	if hcs.services[nsn2].endpoints != 3 {
		t.Errorf("expected 3 endpoints, got %d", hcs.services[nsn2].endpoints)
	}
	if hcs.services[nsn3].endpoints != 7 {
		t.Errorf("expected 7 endpoints, got %d", hcs.services[nsn3].endpoints)
	}
	if hcs.services[nsn4].endpoints != 6 {
		t.Errorf("expected 6 endpoints, got %d", hcs.services[nsn4].endpoints)
	}
	// 逐个验证 handler 响应
	testHandler(hcs, nsn2, http.StatusOK, 3, t)
	testHandler(hcs, nsn3, http.StatusOK, 7, t)
	testHandler(hcs, nsn4, http.StatusOK, 6, t)

	// 同步端点，刻意遗漏 nsn2
	hcs.SyncEndpoints(map[types.NamespacedName]int{
		nsn3: 7,
		nsn4: 6,
	})
	if len(hcs.services) != 3 {
		t.Errorf("expected 3 services, got %d", len(hcs.services))
	}
	if hcs.services[nsn2].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn2].endpoints)
	}
	if hcs.services[nsn3].endpoints != 7 {
		t.Errorf("expected 7 endpoints, got %d", hcs.services[nsn3].endpoints)
	}
	if hcs.services[nsn4].endpoints != 6 {
		t.Errorf("expected 6 endpoints, got %d", hcs.services[nsn4].endpoints)
	}
	// 逐个验证 handler 响应
	testHandler(hcs, nsn2, http.StatusServiceUnavailable, 0, t)
	testHandler(hcs, nsn3, http.StatusOK, 7, t)
	testHandler(hcs, nsn4, http.StatusOK, 6, t)

	// 模拟代理临时不健康：状态码应为 503，端点数照常上报
	proxyChecker.healthy = false
	testHandlerWithHealth(hcs, nsn2, http.StatusServiceUnavailable, 0, false, t)
	testHandlerWithHealth(hcs, nsn3, http.StatusServiceUnavailable, 7, false, t)
	testHandlerWithHealth(hcs, nsn4, http.StatusServiceUnavailable, 6, false, t)

	// 恢复代理健康状态
	proxyChecker.healthy = true
	testHandlerWithHealth(hcs, nsn2, http.StatusServiceUnavailable, 0, true, t)
	testHandlerWithHealth(hcs, nsn3, http.StatusOK, 7, true, t)
	testHandlerWithHealth(hcs, nsn4, http.StatusOK, 6, true, t)
}

// testHandler 在默认代理健康的前提下，校验指定服务的健康检查响应。
func testHandler(hcs *server, nsn types.NamespacedName, status int, endpoints int, t *testing.T) {
	tHandler(hcs, nsn, status, endpoints, true, t)
}

// testHandlerWithHealth 在给定 kube-proxy 健康预期值时，校验服务的健康检查响应。
func testHandlerWithHealth(hcs *server, nsn types.NamespacedName, status int, endpoints int, kubeProxyHealthy bool, t *testing.T) {
	tHandler(hcs, nsn, status, endpoints, kubeProxyHealthy, t)
}

// tHandler 是上述两个辅助函数的共同实现：直接调用 handler，
// 并逐项断言状态码、JSON 负载以及安全与权重相关的响应头。
func tHandler(hcs *server, nsn types.NamespacedName, status int, endpoints int, kubeProxyHealthy bool, t *testing.T) {
	instance := hcs.services[nsn]
	for _, h := range instance.httpServers {
		handler := h.(*fakeHTTPServer).handler

		req, err := http.NewRequest("GET", "/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		if resp.Code != status {
			t.Errorf("expected status code %v, got %v", status, resp.Code)
		}
		var payload hcPayload
		if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Service.Name != nsn.Name || payload.Service.Namespace != nsn.Namespace {
			t.Errorf("expected payload name %q, got %v", nsn.String(), payload.Service)
		}
		if payload.LocalEndpoints != endpoints {
			t.Errorf("expected %d endpoints, got %d", endpoints, payload.LocalEndpoints)
		}
		if payload.ServiceProxyHealthy != kubeProxyHealthy {
			t.Errorf("expected %v kubeProxyHealthy, got %v", kubeProxyHealthy, payload.ServiceProxyHealthy)
		}
		if !cmp.Equal(resp.Header()["Content-Type"], []string{"application/json"}) {
			t.Errorf("expected 'Content-Type: application/json' respose header, got: %v", resp.Header()["Content-Type"])
		}
		if !cmp.Equal(resp.Header()["X-Content-Type-Options"], []string{"nosniff"}) {
			t.Errorf("expected 'X-Content-Type-Options: nosniff' respose header, got: %v", resp.Header()["X-Content-Type-Options"])
		}
		if !cmp.Equal(resp.Header()["X-Load-Balancing-Endpoint-Weight"], []string{strconv.Itoa(endpoints)}) {
			t.Errorf("expected 'X-Load-Balancing-Endpoint-Weight: %d' respose header, got: %v", endpoints, resp.Header()["X-Load-Balancing-Endpoint-Weight"])
		}
	}
}

// nodeTweak 是用于定制测试 Node 对象的函数类型。
type nodeTweak func(n *v1.Node)

// makeNode 构造带默认名称和内网 IP 的测试 Node，并依次应用可选的定制函数。
func makeNode(tweaks ...nodeTweak) *v1.Node {
	n := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
		},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{{
				Type: v1.NodeInternalIP, Address: "192.168.0.1",
			}},
		},
	}
	for _, tw := range tweaks {
		tw(n)
	}
	return n
}

// tweakDeleted 返回一个把 Node 标记为正在删除的定制函数。
func tweakDeleted() nodeTweak {
	return func(n *v1.Node) {
		n.DeletionTimestamp = &metav1.Time{
			Time: time.Now(),
		}
	}
}

// tweakTainted 返回一个给 Node 添加指定污点的定制函数。
func tweakTainted(key string) nodeTweak {
	return func(n *v1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, v1.Taint{Key: key})
	}
}

// serverTest 保存被测 HTTP 服务器、请求路径以及 200/503 响应计数，
// 便于同时断言响应内容与 Prometheus 指标。
type serverTest struct {
	server      httpServer
	url         url
	tracking200 int
	tracking503 int
}

// testNodeManager 是 Node 信息提供方的测试替身，允许测试直接替换当前 Node。
type testNodeManager struct {
	node *v1.Node
}

// Node 返回当前的测试用 Node。
func (t *testNodeManager) Node() *v1.Node {
	return t.node
}

// update 替换当前测试用 Node，用于模拟节点状态变化。
func (t *testNodeManager) update(node *v1.Node) {
	t.node = node
}

// TestHealthzServer 验证 /healthz 在代理更新超时或节点不可调度时返回 503，
// 并同步校验 ProxyHealthzTotal 指标计数。
func TestHealthzServer(t *testing.T) {
	metrics.RegisterMetrics("")
	listener := newFakeListener()
	httpFactory := newFakeHTTPServerFactory()
	fakeClock := testingclock.NewFakeClock(time.Now())

	nodeManager := &testNodeManager{node: makeNode()}
	hs := newProxyHealthServer(listener, httpFactory, fakeClock, "127.0.0.1:10256", 10*time.Second, nodeManager)
	server := hs.httpFactory.New(healthzHandler{hs: hs})

	hsTest := &serverTest{
		server:      server,
		url:         healthzURL,
		tracking200: 0,
		tracking503: 0,
	}

	var expectedPayload ProxyHealth
	// testProxyHealthUpdater 只断言代理自身健康状态，不检查节点是否可用，
	// 因此这里把 nodeEligible 默认设为 true。
	testProxyHealthUpdater(hs, hsTest, fakeClock, ptr.To(true), t)

	// 节点带有无关污点时仍应返回 200
	nodeManager.update(makeNode(tweakTainted("other")))
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: ptr.To(true),
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 节点带有 ToBeDeletedTaint 污点时应返回 503
	nodeManager.update(makeNode(tweakTainted(ToBeDeletedTaint)))
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: ptr.To(false),
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	// 节点带有无关污点时仍应返回 200
	nodeManager.update(makeNode(tweakTainted("other")))
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: ptr.To(true),
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 节点处于删除中时应返回 503
	nodeManager.update(makeNode(tweakDeleted()))
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: ptr.To(false),
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)
}

// TestLivezServer 验证 /livez 只反映代理自身状态，不受节点可用性与污点影响，
// 在代理健康时始终返回 200。
func TestLivezServer(t *testing.T) {
	metrics.RegisterMetrics("")
	listener := newFakeListener()
	httpFactory := newFakeHTTPServerFactory()
	fakeClock := testingclock.NewFakeClock(time.Now())

	nodeManager := &testNodeManager{node: makeNode()}
	hs := newProxyHealthServer(listener, httpFactory, fakeClock, "127.0.0.1:10256", 10*time.Second, nodeManager)
	server := hs.httpFactory.New(livezHandler{hs: hs})

	hsTest := &serverTest{
		server:      server,
		url:         livezURL,
		tracking200: 0,
		tracking503: 0,
	}

	var expectedPayload ProxyHealth
	// /livez 没有“节点可用性”这一概念，
	// 因此期望的 nodeEligible 保持为 nil。
	testProxyHealthUpdater(hs, hsTest, fakeClock, nil, t)

	// 无论节点状态如何都应返回 200
	nodeManager.update(makeNode(tweakTainted("other")))
	expectedPayload = ProxyHealth{
		CurrentTime: fakeClock.Now(),
		LastUpdated: fakeClock.Now(),
		Healthy:     true,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 无论节点状态如何都应返回 200
	nodeManager.update(makeNode(tweakTainted(ToBeDeletedTaint)))
	expectedPayload = ProxyHealth{
		CurrentTime: fakeClock.Now(),
		LastUpdated: fakeClock.Now(),
		Healthy:     true,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 无论节点状态如何都应返回 200
	nodeManager.update(makeNode(tweakTainted("other")))
	expectedPayload = ProxyHealth{
		CurrentTime: fakeClock.Now(),
		LastUpdated: fakeClock.Now(),
		Healthy:     true,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 无论节点状态如何都应返回 200
	nodeManager.update(makeNode(tweakDeleted()))
	expectedPayload = ProxyHealth{
		CurrentTime: fakeClock.Now(),
		LastUpdated: fakeClock.Now(),
		Healthy:     true,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
			v1.IPv6Protocol: {LastUpdated: fakeClock.Now(), Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)
}

// url 是测试用请求路径的别名类型，便于在 serverTest 中与其他字符串区分。
type url string

// healthzURL 与 livezURL 分别对应两个健康检查端点。
var (
	healthzURL url = "/healthz"
	livezURL   url = "/livez"
)

// testProxyHealthUpdater 覆盖代理健康状态的完整变化过程：初始化、
// IPv4/IPv6 更新、单栈或双栈更新超时以及恢复，并逐阶段校验响应。
func testProxyHealthUpdater(hs *ProxyHealthServer, hsTest *serverTest, fakeClock *testingclock.FakeClock, nodeEligible *bool, t *testing.T) {
	var expectedPayload ProxyHealth
	// lastUpdated 记录最近一次模拟更新的时间，用于断言 HTTP 响应中的时间戳。
	// 每次模拟更新后都会同步推进该变量。
	var lastUpdated time.Time
	// 分别跟踪两个协议栈各自的最后更新时间。
	var ipv4LastUpdated time.Time
	var ipv6LastUpdated time.Time

	// 默认状态下应返回 200
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 两个协议栈首次更新后应返回 200
	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()
	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 没有新的更新排队时，即使时间流逝也应继续返回 200
	fakeClock.Step(25 * time.Second)
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// IPv4 协议栈更新超时后应返回 503
	hs.QueuedUpdate(v1.IPv4Protocol)
	fakeClock.Step(25 * time.Second)
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: false},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	// 两个协议栈都完成更新后应恢复 200
	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()
	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	fakeClock.Step(5 * time.Second)
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// IPv6 协议栈更新超时后应返回 503
	hs.QueuedUpdate(v1.IPv6Protocol)
	fakeClock.Step(25 * time.Second)
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: false},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	// 两个协议栈都完成更新后应恢复 200
	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()
	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	fakeClock.Step(5 * time.Second)
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 两个协议栈同时超时后应返回 503
	hs.QueuedUpdate(v1.IPv4Protocol)
	hs.QueuedUpdate(v1.IPv6Protocol)
	fakeClock.Step(25 * time.Second)
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: false},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: false},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	// 两个协议栈都完成更新后应恢复 200
	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()
	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	fakeClock.Step(5 * time.Second)
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 仅 IPv6 超时时，单独更新 IPv4 不应让整体状态恢复健康
	hs.QueuedUpdate(v1.IPv6Protocol)
	fakeClock.Step(25 * time.Second)
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: false},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: false},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	lastUpdated = fakeClock.Now()
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)

	// 双栈都超时时，必须等两者都更新完毕才能恢复 200
	hs.QueuedUpdate(v1.IPv4Protocol)
	hs.QueuedUpdate(v1.IPv6Protocol)
	fakeClock.Step(25 * time.Second)
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  lastUpdated,
		Healthy:      false,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: false},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: false},
		},
	}
	testHTTPHandler(hsTest, http.StatusServiceUnavailable, expectedPayload, t)

	hs.Updated(v1.IPv4Protocol)
	ipv4LastUpdated = fakeClock.Now()

	hs.Updated(v1.IPv6Protocol)
	ipv6LastUpdated = fakeClock.Now()
	// 为保持向后兼容，代理健康时返回的 last_updated 就是 current_time，
	// 因此这里用 fakeClock.Now() 而不是实际记录的 lastUpdated。
	expectedPayload = ProxyHealth{
		CurrentTime:  fakeClock.Now(),
		LastUpdated:  fakeClock.Now(),
		Healthy:      true,
		NodeEligible: nodeEligible,
		Status: map[v1.IPFamily]ProxierHealth{
			v1.IPv4Protocol: {LastUpdated: ipv4LastUpdated, Healthy: true},
			v1.IPv6Protocol: {LastUpdated: ipv6LastUpdated, Healthy: true},
		},
	}
	testHTTPHandler(hsTest, http.StatusOK, expectedPayload, t)
}

// testHTTPHandler 向被测服务器发起 GET 请求，断言状态码、负载内容与
// 对应的 Prometheus 计数器；healthz 与 livez 对 nodeEligible 的要求不同。
func testHTTPHandler(hsTest *serverTest, status int, expectedPayload ProxyHealth, t *testing.T) {
	t.Helper()
	handler := hsTest.server.(*fakeHTTPServer).handler
	req, err := http.NewRequest("GET", string(hsTest.url), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != status {
		t.Errorf("expected status code %v, got %v", status, resp.Code)
	}
	var payload ProxyHealth
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}

	// 断言整体健康状态字段
	if !payload.LastUpdated.Equal(expectedPayload.LastUpdated) {
		t.Errorf("expected last updated: %s; got: %s", expectedPayload.LastUpdated.String(), payload.LastUpdated.String())
	}
	if !payload.CurrentTime.Equal(expectedPayload.CurrentTime) {
		t.Errorf("expected current time: %s; got: %s", expectedPayload.CurrentTime.String(), payload.CurrentTime.String())
	}
	if payload.Healthy != expectedPayload.Healthy {
		t.Errorf("expected healthy: %v, got: %v", expectedPayload.Healthy, payload.Healthy)
	}
	// 逐个协议栈断言健康状态与更新时间
	for ipFamily := range payload.Status {
		if payload.Status[ipFamily].Healthy != expectedPayload.Status[ipFamily].Healthy {
			t.Errorf("expected healthy[%s]: %v, got: %v", ipFamily, expectedPayload.Status[ipFamily].Healthy, payload.Status[ipFamily].Healthy)
		}
		if !payload.Status[ipFamily].LastUpdated.Equal(expectedPayload.Status[ipFamily].LastUpdated) {
			t.Errorf("expected last updated[%s]: %s; got: %s", ipFamily, expectedPayload.Status[ipFamily].LastUpdated.String(), payload.Status[ipFamily].LastUpdated.String())
		}
	}

	if status == http.StatusOK {
		hsTest.tracking200++
	}
	if status == http.StatusServiceUnavailable {
		hsTest.tracking503++
	}
	if hsTest.url == healthzURL {
		if *payload.NodeEligible != *expectedPayload.NodeEligible {
			t.Errorf("expected node eligible: %v; got: %v", *payload.NodeEligible, *expectedPayload.NodeEligible)
		}
		testMetricEquals(metrics.ProxyHealthzTotal.WithLabelValues("200"), float64(hsTest.tracking200), t)
		testMetricEquals(metrics.ProxyHealthzTotal.WithLabelValues("503"), float64(hsTest.tracking503), t)
	}
	if hsTest.url == livezURL {
		if payload.NodeEligible != nil {
			t.Errorf("expected node eligible nil for %s response", livezURL)
		}
		testMetricEquals(metrics.ProxyLivezTotal.WithLabelValues("200"), float64(hsTest.tracking200), t)
		testMetricEquals(metrics.ProxyLivezTotal.WithLabelValues("503"), float64(hsTest.tracking503), t)
	}
}

// testMetricEquals 读取计数器指标的当前值并与期望值比较。
func testMetricEquals(metric basemetrics.CounterMetric, expected float64, t *testing.T) {
	t.Helper()
	val, err := testutil.GetCounterMetricValue(metric)
	if err != nil {
		t.Errorf("unable to retrieve value for metric, err: %v", err)
	}
	if val != expected {
		t.Errorf("expected: %v, found: %v", expected, val)
	}
}

// TestServerWithSelectiveListeningAddress 验证配置监听地址白名单后，
// 服务健康检查服务器只绑定到回环地址。
func TestServerWithSelectiveListeningAddress(t *testing.T) {
	listener := newFakeListener()
	httpFactory := newFakeHTTPServerFactory()
	proxyChecker := &fakeProxyHealthChecker{true}

	// 将监听地址限制为回环网段：
	// 避免依赖探测本机 IP 或区分 IPv4/IPv6，保证测试在任何机器上都能运行。
	nodePortAddresses := proxyutil.NewNodePortAddresses(v1.IPv4Protocol, []string{"127.0.0.0/8"})

	hcsi := newServiceHealthServer("hostname", nil, listener, httpFactory, nodePortAddresses, proxyChecker, v1.IPv4Protocol)
	hcs := hcsi.(*server)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 同步空集合
	hcs.SyncServices(nil)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}
	hcs.SyncEndpoints(nil)
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 未知服务的端点应被丢弃
	hcs.SyncEndpoints(map[types.NamespacedName]int{mknsn("a", "b"): 93})
	if len(hcs.services) != 0 {
		t.Errorf("expected 0 services, got %d", len(hcs.services))
	}

	// 同步真实服务，确认监听地址落在回环网段
	nsn := mknsn("a", "b")
	hcs.SyncServices(map[types.NamespacedName]uint16{nsn: 9376})
	if len(hcs.services) != 1 {
		t.Errorf("expected 1 service, got %d", len(hcs.services))
	}
	if hcs.services[nsn].endpoints != 0 {
		t.Errorf("expected 0 endpoints, got %d", hcs.services[nsn].endpoints)
	}
	if len(listener.openPorts) != 1 {
		t.Errorf("expected 1 open port, got %d\n%s", len(listener.openPorts), dump.Pretty(listener.openPorts))
	}
	if !listener.hasPort("127.0.0.1:9376") {
		t.Errorf("expected port :9376 to be open\n%s", dump.Pretty(listener.openPorts))
	}
	// 验证 handler 响应
	testHandler(hcs, nsn, http.StatusServiceUnavailable, 0, t)
}
