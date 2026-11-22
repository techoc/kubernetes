/*
Copyright The Kubernetes Authors.

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

package localnodeportproxy

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	proxymetrics "k8s.io/kubernetes/pkg/proxy/metrics"
	testingclock "k8s.io/utils/clock/testing"
)

// makeServicePortName 构造测试用的 ServicePortName，便于按命名空间、服务名和端口名定位服务端口。
func makeServicePortName(ns, name, port string) proxy.ServicePortName {
	return proxy.ServicePortName{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
		Port:           port,
		Protocol:       v1.ProtocolTCP,
	}
}

// randomNodePort 在合法范围内随机取端口，避免并发运行的测试因固定端口冲突而相互干扰。
func randomNodePort() int {
	return 30000 + rand.Intn(2767)
}

// startTCPEchoServer 启动 TCP 服务器，将接收到的每个字节原样回显给客户端。
func startTCPEchoServer(t *testing.T, network, addr string) net.Listener {
	t.Helper()
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatalf("Failed to start echo server: %v", err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close() //nolint:errcheck
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return l
}

// assertEchoThroughProxy 通过 network/addr 连接 local NodePort 代理并发送 msg，
// 若回显内容与 msg 不一致则测试失败。
func assertEchoThroughProxy(t *testing.T, network, addr, msg string) {
	t.Helper()
	conn, err := net.DialTimeout(network, addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to nodeport proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck
	_, _ = fmt.Fprint(conn, msg)
	_ = conn.(*net.TCPConn).CloseWrite()
	buf, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("Failed to read from proxy: %v", err)
	}
	if string(buf) != msg {
		t.Errorf("Expected %q, got %q", msg, string(buf))
	}
}

// TestSyncNodePorts_AddAndRemove 验证同步 NodePort 时创建监听器，并在规格移除后关闭端口。
func TestSyncNodePorts_AddAndRemove(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	svcName := makeServicePortName("default", "test-svc", "http")

	// 启动后端回显服务器，用端口 0 让内核分配空闲端口，避免端口冲突。
	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close() //nolint:errcheck
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := net.JoinHostPort("127.0.0.1", strconv.Itoa(backendPort))

	nodePort := randomNodePort()

	desired := []NodePortSpec{{
		ServicePortName: svcName,
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep},
	}}
	key := desired[0].nodePortKey()

	p.SyncNodePorts(desired)

	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener, got %d", len(p.active))
	}
	if _, ok := p.active[key]; !ok {
		t.Fatalf("Expected listener for key %s", key)
	}

	// 通过代理连接后端，确认流量确实经过监听器转发。
	assertEchoThroughProxy(t, "tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), "hello nodeport")

	// 同步空规格列表，模拟服务被删除。
	p.SyncNodePorts(nil)
	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners after removal, got %d", len(p.active))
	}

	// 确认监听移除后端口不再接受连接。
	_, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 500*time.Millisecond)
	if err == nil {
		t.Fatal("Expected connection to fail after listener removal")
	}
}

// TestSyncNodePorts_UpdateEndpoints 验证端点变化时复用已有监听器，只更新其后端列表。
func TestSyncNodePorts_UpdateEndpoints(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	// 启动两个后端服务器，用于验证端点切换。
	backend1 := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend1.Close() //nolint:errcheck
	backend2 := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend2.Close() //nolint:errcheck

	ep1 := net.JoinHostPort("127.0.0.1", strconv.Itoa(backend1.Addr().(*net.TCPAddr).Port))
	ep2 := net.JoinHostPort("127.0.0.1", strconv.Itoa(backend2.Addr().(*net.TCPAddr).Port))

	svcName := makeServicePortName("default", "test-svc", "http")

	nodePort := randomNodePort()

	// 初始只配置 ep1。
	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: svcName,
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep1},
	}})

	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener, got %d", len(p.active))
	}

	// 将端点更新为 ep2。
	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: svcName,
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep2},
	}})

	// 端点更新不应重建监听器，因此活跃监听器数量仍为 1。
	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener after update, got %d", len(p.active))
	}

	// 确认切换到 ep2 后流量仍能正常转发。
	assertEchoThroughProxy(t, "tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), "after update")
}

// TestNoEndpoints 验证无端点时不创建监听器，端点增删时监听器随之创建和拆除。
func TestNoEndpoints(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close() //nolint:errcheck
	ep := net.JoinHostPort("127.0.0.1", strconv.Itoa(backend.Addr().(*net.TCPAddr).Port))

	nodePort := randomNodePort()

	spec := NodePortSpec{
		ServicePortName: makeServicePortName("default", "svc", "http"),
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       nil,
	}

	// 没有端点的规格不应创建监听器，因为此时没有可转发的后端。
	p.SyncNodePorts([]NodePortSpec{spec})
	if len(p.active) != 0 {
		t.Fatalf("Expected no listener for spec with no endpoints, got %d active", len(p.active))
	}

	// 补充端点后应拉起监听器。
	spec.Endpoints = []string{ep}
	p.SyncNodePorts([]NodePortSpec{spec})
	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener, got %d", len(p.active))
	}

	// 端点被清空后应拆除监听器，端口随之停止接受连接。
	spec.Endpoints = nil
	p.SyncNodePorts([]NodePortSpec{spec})
	if len(p.active) != 0 {
		t.Fatalf("Expected listener to be torn down when endpoints drain, got %d active", len(p.active))
	}
	if _, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 500*time.Millisecond); err == nil {
		t.Fatal("Expected dial to fail after endpoints drain")
	}
}

// TestSyncNodePorts_SkipUDP 验证 UDP 规格会被跳过，不在 localhost 上创建监听器。
func TestSyncNodePorts_SkipUDP(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	svcName := makeServicePortName("default", "udp-svc", "dns")

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: svcName,
		Protocol:        v1.ProtocolUDP,
		NodePort:        30053,
		Endpoints:       []string{"10.0.0.1:53"},
	}})

	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners for UDP, got %d", len(p.active))
	}
}

// TestPickEndpoint_NoEndpointsReturnsEmpty 验证没有端点时 pickEndpoint 返回空字符串而非随机结果。
func TestPickEndpoint_NoEndpointsReturnsEmpty(t *testing.T) {
	l := &nodePortListener{}
	if got := l.pickEndpoint(); got != "" {
		t.Errorf("pickEndpoint() with no endpoints = %q, want empty", got)
	}
}

// TestBackendConnectionFailure 验证后端不可连接时代理接受连接后立即关闭，且不向客户端写入任何数据。
func TestBackendConnectionFailure(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	// 使用一个几乎不可能有进程监听的端口作为不可达后端。
	ep := "127.0.0.1:1" // port 1 is almost certainly not listening

	nodePort := randomNodePort()

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: makeServicePortName("default", "fail-svc", "http"),
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep},
	}})

	// 代理端应先接受连接；后端拨号失败后再把连接关闭。
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	buf, _ := io.ReadAll(conn)
	_ = conn.Close()

	if len(buf) != 0 {
		t.Errorf("Expected empty response on backend failure, got %q", string(buf))
	}
}

// TestShutdown 验证 Shutdown 会关闭所有监听器并释放其占用的端口。
func TestShutdown(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)

	// 创建多个监听器，验证 Shutdown 能一次性清理全部。
	var ports []int
	var desired []NodePortSpec
	for range 3 {
		port := randomNodePort()
		ports = append(ports, port)

		desired = append(desired, NodePortSpec{
			ServicePortName: makeServicePortName("default", fmt.Sprintf("svc-%d", port), "http"),
			Protocol:        v1.ProtocolTCP,
			NodePort:        port,
			Endpoints:       []string{"127.0.0.1:1"},
		})
	}

	p.SyncNodePorts(desired)
	if len(p.active) != 3 {
		t.Fatalf("Expected 3 active listeners, got %d", len(p.active))
	}

	p.Shutdown()
	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners after shutdown, got %d", len(p.active))
	}

	// 确认所有端口关闭后都不再接受连接。
	for _, port := range ports {
		_, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			t.Errorf("Port %d still accepting connections after shutdown", port)
		}
	}
}

// TestShutdownClosesInFlightConnections 验证 Shutdown 会一并关闭已建立但空闲的在途连接。
func TestShutdownClosesInFlightConnections(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)

	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close() //nolint:errcheck
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := net.JoinHostPort("127.0.0.1", strconv.Itoa(backendPort))

	nodePort := randomNodePort()

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: makeServicePortName("default", "inflight-svc", "http"),
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep},
	}})

	// 先通过代理建立一条空闲的在途连接。
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// 做一次成功的回显读写，确保 handleTCPConn 已在 Shutdown 之前完成建连。
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Failed to write to proxy: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("Failed to read echo through proxy: %v", err)
	}

	p.Shutdown()

	// Shutdown 后在途连接必须被拆除，阻塞的 Read 应尽快返回而不是一直挂起。
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("Expected connection to be closed after Shutdown, got nil error")
	} else {
		if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
			t.Fatalf("Expected connection close after Shutdown, got read timeout: %v", err)
		}
	}
}

// TestIPv6 验证 IPv6 地址族下代理使用 ::1 监听并能正常转发流量。
func TestIPv6(t *testing.T) {
	// 先探测 ::1 是否可用，宿主机禁用 IPv6 时直接跳过。
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback not available: %v", err)
	}
	_ = l.Close()

	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv6Protocol)
	defer p.Shutdown()

	if p.listenIP != "::1" {
		t.Errorf("Expected listenIP '::1', got %q", p.listenIP)
	}

	// 启动一个 IPv6 回显后端。
	backend := startTCPEchoServer(t, "tcp6", "[::1]:0")
	defer backend.Close() //nolint:errcheck
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := net.JoinHostPort("::1", strconv.Itoa(backendPort))

	nodePort := randomNodePort()

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: makeServicePortName("default", "v6-svc", "http"),
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       []string{ep},
	}})

	assertEchoThroughProxy(t, "tcp6", fmt.Sprintf("[::1]:%d", nodePort), "hello ipv6")
}

// startPortReportingServer 启动 TCP 服务器，向每个客户端返回自身端口号，
// 使测试能判断连接被路由到了哪个后端。
func startPortReportingServer(t *testing.T, network string) (net.Listener, int, string) {
	t.Helper()
	addr := "127.0.0.1:0"
	if network == "tcp6" {
		addr = "[::1]:0"
	}
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatalf("Failed to start backend: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close() //nolint:errcheck
				_, _ = fmt.Fprintf(c, "%d", port)
			}(conn)
		}
	}()
	ip := "127.0.0.1"
	if network == "tcp6" {
		ip = "::1"
	}
	return l, port, net.JoinHostPort(ip, strconv.Itoa(port))
}

// readBackendID 连接 network/addr，返回代理将连接路由到的后端端口号。
func readBackendID(t *testing.T, network, addr string) int {
	t.Helper()
	conn, err := net.DialTimeout(network, addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	buf, err := io.ReadAll(conn)
	_ = conn.Close()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	port, err := strconv.Atoi(string(buf))
	if err != nil {
		t.Fatalf("Failed to parse backend port %q: %v", buf, err)
	}
	return port
}

// TestSessionAffinity_PinsToSingleEndpoint 验证 ClientIP 会话保持把同一客户端的请求固定到同一后端。
func TestSessionAffinity_PinsToSingleEndpoint(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 注入假时钟，让会话保持的时间判定由测试掌控，不依赖真实等待。
	p := newLocalNodePortProxy(ctx, v1.IPv4Protocol, testingclock.NewFakeClock(time.Now()))
	defer p.Shutdown()

	// 启动 3 个后端，每个后端返回自己的端口号作为标识。
	var backends []net.Listener
	var endpoints []string
	for range 3 {
		b, _, ep := startPortReportingServer(t, "tcp4")
		backends = append(backends, b)
		endpoints = append(endpoints, ep)
	}
	defer func() {
		for _, b := range backends {
			_ = b.Close()
		}
	}()

	nodePort := randomNodePort()

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName:     makeServicePortName("default", "sticky-svc", "http"),
		Protocol:            v1.ProtocolTCP,
		NodePort:            nodePort,
		Endpoints:           endpoints,
		SessionAffinityType: v1.ServiceAffinityClientIP,
		StickyMaxAgeSeconds: 10800,
	}})

	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	first := readBackendID(t, "tcp4", addr)
	for range 10 {
		got := readBackendID(t, "tcp4", addr)
		if got != first {
			t.Fatalf("SessionAffinity ClientIP: expected all requests to hit %d, got %d", first, got)
		}
	}
}

// TestSessionAffinity_PinnedEndpointRemoved 验证被固定的端点移除后，流量切换到存活端点而不是丢失。
func TestSessionAffinity_PinnedEndpointRemoved(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	p := newLocalNodePortProxy(ctx, v1.IPv4Protocol, testingclock.NewFakeClock(time.Now()))
	defer p.Shutdown()

	b1, port1, ep1 := startPortReportingServer(t, "tcp4")
	defer b1.Close() //nolint:errcheck
	b2, port2, ep2 := startPortReportingServer(t, "tcp4")
	defer b2.Close() //nolint:errcheck

	nodePort := randomNodePort()

	svcName := makeServicePortName("default", "sticky-svc", "http")
	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName:     svcName,
		Protocol:            v1.ProtocolTCP,
		NodePort:            nodePort,
		Endpoints:           []string{ep1, ep2},
		SessionAffinityType: v1.ServiceAffinityClientIP,
		StickyMaxAgeSeconds: 10800,
	}})

	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	pinned := readBackendID(t, "tcp4", addr)

	// 从端点集合中移除被固定的端点；剩余流量必须打到存活端点，而不能静默丢弃。
	remaining, remainingPort := ep2, port2
	if pinned == port2 {
		remaining, remainingPort = ep1, port1
	}
	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName:     svcName,
		Protocol:            v1.ProtocolTCP,
		NodePort:            nodePort,
		Endpoints:           []string{remaining},
		SessionAffinityType: v1.ServiceAffinityClientIP,
		StickyMaxAgeSeconds: 10800,
	}})

	if got := readBackendID(t, "tcp4", addr); got != remainingPort {
		t.Fatalf("After pinned endpoint removal: expected %d, got %d", remainingPort, got)
	}
}

// TestSessionAffinity_Expires 验证会话保持窗口到期后固定关系释放，流量可切换到其他端点。
func TestSessionAffinity_Expires(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 使用假时钟直接推进 3 小时的老化窗口，避免测试真的等待。
	fakeClock := testingclock.NewFakeClock(time.Now())
	p := newLocalNodePortProxy(ctx, v1.IPv4Protocol, fakeClock)
	defer p.Shutdown()

	var backends []net.Listener
	var endpoints []string
	for range 3 {
		b, _, ep := startPortReportingServer(t, "tcp4")
		backends = append(backends, b)
		endpoints = append(endpoints, ep)
	}
	defer func() {
		for _, b := range backends {
			_ = b.Close()
		}
	}()

	nodePort := randomNodePort()

	const affinitySeconds = 10800
	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName:     makeServicePortName("default", "sticky-svc", "http"),
		Protocol:            v1.ProtocolTCP,
		NodePort:            nodePort,
		Endpoints:           endpoints,
		SessionAffinityType: v1.ServiceAffinityClientIP,
		StickyMaxAgeSeconds: affinitySeconds,
	}})

	// 推进假时钟越过老化窗口，验证固定关系随之失效。
	window := affinitySeconds*time.Second + time.Second
	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	first := readBackendID(t, "tcp4", addr)
	for i := 0; ; i++ {
		if i == 20 {
			t.Fatalf("Affinity pin to %d never expired after %d attempts", first, i)
		}
		fakeClock.Step(window)
		if readBackendID(t, "tcp4", addr) != first {
			break
		}
	}
}

// TestNoSessionAffinity_SpreadsAcrossEndpoints 验证未开启会话保持时连接会分散到所有端点。
func TestNoSessionAffinity_SpreadsAcrossEndpoints(t *testing.T) {
	const numEndpoints = 3
	const maxAttempts = 100

	_, ctx := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	var backends []net.Listener
	var endpoints []string
	for range numEndpoints {
		b, _, ep := startPortReportingServer(t, "tcp4")
		backends = append(backends, b)
		endpoints = append(endpoints, ep)
	}
	defer func() {
		for _, b := range backends {
			_ = b.Close()
		}
	}()

	nodePort := randomNodePort()

	p.SyncNodePorts([]NodePortSpec{{
		ServicePortName: makeServicePortName("default", "roundrobin-svc", "http"),
		Protocol:        v1.ProtocolTCP,
		NodePort:        nodePort,
		Endpoints:       endpoints,
	}})

	// 确认每个端点都被访问到。
	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	seen := map[int]bool{}
	for i := 0; len(seen) < numEndpoints; i++ {
		if i == maxAttempts {
			t.Fatalf("Expected connections to spread across all %d endpoints, only reached %v", numEndpoints, seen)
		}
		seen[readBackendID(t, "tcp4", addr)] = true
	}
}

// TestSyncNodePorts_ListenersGaugeReflectsActiveCount 验证监听器数量指标随活跃监听器增减而更新。
func TestSyncNodePorts_ListenersGaugeReflectsActiveCount(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// 指标必须先注册再读取，否则取值会失败。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeProxyNFTablesLocalhostNodePorts, true)
	proxymetrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)
	gauge := proxymetrics.LocalhostNodePortListeners.WithLabelValues(string(v1.IPv4Protocol))

	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close() //nolint:errcheck
	ep := net.JoinHostPort("127.0.0.1", strconv.Itoa(backend.Addr().(*net.TCPAddr).Port))

	p1, p2 := randomNodePort(), randomNodePort()

	p.SyncNodePorts([]NodePortSpec{
		{ServicePortName: makeServicePortName("ns", "svc1", "p"), Protocol: v1.ProtocolTCP, NodePort: p1, Endpoints: []string{ep}},
		{ServicePortName: makeServicePortName("ns", "svc2", "p"), Protocol: v1.ProtocolTCP, NodePort: p2, Endpoints: []string{ep}},
	})

	got, err := testutil.GetGaugeMetricValue(gauge)
	if err != nil {
		t.Fatalf("Failed to read gauge: %v", err)
	}
	if got != 2 {
		t.Fatalf("After adding 2 listeners: gauge = %v, want 2", got)
	}

	p.SyncNodePorts(nil)
	got, err = testutil.GetGaugeMetricValue(gauge)
	if err != nil {
		t.Fatalf("Failed to read gauge: %v", err)
	}
	if got != 0 {
		t.Fatalf("After removing all listeners: gauge = %v, want 0", got)
	}
}

// TestSyncNodePorts_ListenerCreationFailuresCounter 验证监听器绑定端口失败时失败计数增加。
func TestSyncNodePorts_ListenerCreationFailuresCounter(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// 同样需要在读取计数器前先完成指标注册。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeProxyNFTablesLocalhostNodePorts, true)
	proxymetrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)
	counter := proxymetrics.LocalhostNodePortListenerCreationFailuresTotal.WithLabelValues(string(v1.IPv4Protocol))
	before, err := testutil.GetCounterMetricValue(counter)
	if err != nil {
		t.Fatalf("Failed to read counter: %v", err)
	}

	p := NewLocalNodePortProxy(ctx, v1.IPv4Protocol)
	defer p.Shutdown()

	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close() //nolint:errcheck
	ep := net.JoinHostPort("127.0.0.1", strconv.Itoa(backend.Addr().(*net.TCPAddr).Port))

	// 先占用一个端口，使代理绑定该端口时必然失败。
	taken, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to occupy port: %v", err)
	}
	defer taken.Close() //nolint:errcheck
	takenPort := taken.Addr().(*net.TCPAddr).Port

	p.SyncNodePorts([]NodePortSpec{
		{ServicePortName: makeServicePortName("ns", "svc", "p"), Protocol: v1.ProtocolTCP, NodePort: takenPort, Endpoints: []string{ep}},
	})

	got, err := testutil.GetCounterMetricValue(counter)
	if err != nil {
		t.Fatalf("Failed to read counter: %v", err)
	}
	if got != before+1 {
		t.Fatalf("After a failed bind: counter = %v, want %v", got, before+1)
	}
}
