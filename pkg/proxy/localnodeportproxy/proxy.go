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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/utils/clock"
)

// dialTimeout 是连接后端 Endpoint 的超时时间；避免后端不可达时 goroutine 永久挂起。
const dialTimeout = 5 * time.Second

// NodePortSpec 描述一个需要在 localhost 上代理的 NodePort。
// 调用方在自身的同步循环中构造该结构的切片，并交给 SyncNodePorts 进行对账。
type NodePortSpec struct {
	ServicePortName proxy.ServicePortName
	Protocol        v1.Protocol
	NodePort        int
	// Endpoints 是监听器轮询的 "ip:port" 后端集合；为空时该 NodePort 不需要代理。
	Endpoints []string
	// SessionAffinityType 对应 Service.spec.sessionAffinity。设为 ClientIP 时，
	// 来自 localhost 的连续连接在 StickyMaxAgeSeconds 内固定到同一个 Endpoint。
	SessionAffinityType v1.ServiceAffinity
	// StickyMaxAgeSeconds 是会话保持的超时秒数，仅在 SessionAffinityType 为 ClientIP 时生效。
	StickyMaxAgeSeconds int
}

// affinityTimeout 返回该 spec 实际生效的会话保持时长；未启用 ClientIP 亲和性时返回 0，
// 后续选择 Endpoint 时可据此完全跳过亲和逻辑。
func (s *NodePortSpec) affinityTimeout() time.Duration {
	if s.SessionAffinityType != v1.ServiceAffinityClientIP {
		return 0
	}
	return time.Duration(s.StickyMaxAgeSeconds) * time.Second
}

// LocalNodePortProxy 在 nftables 模式下为 NodePort Service 管理 localhost 上的用户态四层代理监听器，
// 用于接管发往本机 NodePort 的流量并转发到后端 Endpoint。
type LocalNodePortProxy struct {
	ctx      context.Context
	logger   klog.Logger
	listenIP string
	family   string
	clock    clock.Clock

	// mu 保护 active 映射，防止 Sync 与 Shutdown 并发执行时重复创建或泄漏监听器。
	mu     sync.Mutex
	active map[string]*nodePortListener
}

// NewLocalNodePortProxy 为给定 IP 协议族创建代理，使用真实时钟；测试可通过内部构造函数注入假时钟。
func NewLocalNodePortProxy(ctx context.Context, ipFamily v1.IPFamily) *LocalNodePortProxy {
	return newLocalNodePortProxy(ctx, ipFamily, clock.RealClock{})
}

// newLocalNodePortProxy 是内部构造函数：按协议族选择回环地址（IPv4 为 127.0.0.1，IPv6 为 ::1），
// 并允许注入时钟以便在测试中精确控制亲和性窗口。
func newLocalNodePortProxy(ctx context.Context, ipFamily v1.IPFamily, clock clock.Clock) *LocalNodePortProxy {
	listenIP := "127.0.0.1"
	if ipFamily == v1.IPv6Protocol {
		listenIP = "::1"
	}
	return &LocalNodePortProxy{
		ctx:      ctx,
		logger:   klog.FromContext(ctx),
		listenIP: listenIP,
		family:   string(ipFamily),
		clock:    clock,
		active:   make(map[string]*nodePortListener),
	}
}

// SyncNodePorts 对账本机 NodePort 代理：删除不再需要的监听器，创建或更新期望的监听器。
// 无 Endpoint 的 NodePort 不建监听器；指标随实际监听器数量更新，便于观测。
func (p *LocalNodePortProxy) SyncNodePorts(desired []NodePortSpec) {
	byKey := make(map[string]*NodePortSpec, len(desired))
	for i := range desired {
		spec := &desired[i]
		if len(spec.Endpoints) == 0 {
			continue
		}
		byKey[spec.nodePortKey()] = spec
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for key, l := range p.active {
		if _, ok := byKey[key]; !ok {
			p.logger.V(2).Info("Removing localhost nodeport proxy", "protocol", l.protocol, "port", l.port)
			l.shutdown()
			delete(p.active, key)
		}
	}

	for key, spec := range byKey {
		if existing, ok := p.active[key]; ok {
			existing.update(spec)
			continue
		}
		if spec.Protocol != v1.ProtocolTCP {
			p.logger.Error(nil, "Unexpected non-TCP localhost nodeport proxy",
				"service", spec.ServicePortName, "protocol", spec.Protocol, "port", spec.NodePort)
			continue
		}
		l, err := p.newNodePortListener(spec)
		if err != nil {
			p.logger.Error(err, "Failed to create localhost nodeport proxy", "protocol", spec.Protocol, "port", spec.NodePort)
			metrics.LocalhostNodePortListenerCreationFailuresTotal.WithLabelValues(p.family).Inc()
			continue
		}
		p.active[key] = l
		p.logger.V(4).Info("Created localhost nodeport proxy", "protocol", spec.Protocol, "port", spec.NodePort, "endpoints", len(spec.Endpoints))
	}

	metrics.LocalhostNodePortListeners.WithLabelValues(p.family).Set(float64(len(p.active)))
}

// nodePortKey 用协议与端口号唯一标识一个 NodePort 监听器，供 active 映射做增删改对账。
func (s *NodePortSpec) nodePortKey() string {
	return fmt.Sprintf("%s/%d", s.Protocol, s.NodePort)
}

// Shutdown 关闭所有活跃监听器及其在途连接，并将监控指标归零；用于代理退出时的清理。
func (p *LocalNodePortProxy) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, l := range p.active {
		l.shutdown()
		delete(p.active, key)
	}
	metrics.LocalhostNodePortListeners.WithLabelValues(p.family).Set(0)
}

// newNodePortListener 在回环地址的 NodePort 端口上创建 TCP 监听器并启动接受循环。
// 端口被占用等错误会返回给调用方记录指标，避免整体同步失败。
func (p *LocalNodePortProxy) newNodePortListener(spec *NodePortSpec) (*nodePortListener, error) {
	// 绑定回环地址而非通配地址：只代理本机访问 NodePort 的流量，避免与真实 NodePort 端口冲突。
	addr := net.JoinHostPort(p.listenIP, fmt.Sprintf("%d", spec.NodePort))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// 每个监听器有独立的取消上下文，关闭时只终止自己，不影响其他 NodePort。
	ctx, cancel := context.WithCancel(p.ctx)
	l := &nodePortListener{
		protocol:        spec.Protocol,
		port:            spec.NodePort,
		logger:          p.logger,
		endpoints:       spec.Endpoints,
		affinityTimeout: spec.affinityTimeout(),
		listener:        listener,
		cancel:          cancel,
		clock:           p.clock,
	}
	// 接受连接在独立 goroutine 中运行；返回后由 active 映射负责其生命周期。
	go l.acceptLoop(ctx)
	return l, nil
}

// nodePortListener 持有单个 NodePort 的 TCP 监听器、后端集合与会话保持状态。
// 构造完成后不可变字段无需加锁；可变字段由 mu 保护，避免同步循环与连接处理并发读写。
type nodePortListener struct {
	// 构造后不再修改，因此读取无需加锁。
	protocol v1.Protocol
	port     int
	logger   klog.Logger
	listener net.Listener
	cancel   context.CancelFunc
	clock    clock.Clock

	// mu 保护下面的 Endpoint 选择与亲和状态，避免与同步循环并发修改时产生数据竞争。
	mu        sync.Mutex
	endpoints []string
	// affinityTimeout 为 0 表示未启用会话保持；否则表示选中的 Endpoint 对本机流量保持绑定的时长。
	// 由于所有 localhost 流量的源 IP 都是 127.0.0.1 或 ::1，ClientIP 亲和性实际上会把
	// 通过该监听器的全部流量固定到某个 Endpoint，直到绑定过期——这与标准 iptables/nftables 模式一致。
	affinityTimeout time.Duration
	pinnedEndpoint  string // "ip:port" of the currently pinned endpoint, empty if none
	pinnedLastUsed  time.Time
}

// acceptLoop 不断接受新连接并为每个连接启动独立处理 goroutine。
// 监听器关闭或上下文取消时退出；其他 Accept 错误只记录日志，避免单个错误终止整个监听器。
func (l *nodePortListener) acceptLoop(ctx context.Context) {
	for {
		conn, err := l.listener.Accept()
		// 上下文取消或监听器已关闭是正常退出路径，不应记录为错误。
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			l.logger.V(4).Info("Accept error on localhost nodeport proxy", "err", err, "protocol", l.protocol, "port", l.port)
			continue
		}
		go l.handleTCPConn(ctx, conn)
	}
}

// handleTCPConn 为单个连接选择后端、拨号并在客户端与后端之间双向转发数据。
// 拨号失败或转发结束都会关闭两侧连接；关闭监听器时通过上下文强制中断阻塞的 io.Copy，防止 goroutine 泄漏。
func (l *nodePortListener) handleTCPConn(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close() //nolint:errcheck

	// 带超时的拨号：后端不可达时快速失败，不阻塞其他连接。
	ep := l.pickEndpoint()
	backendConn, err := net.DialTimeout("tcp", ep, dialTimeout)
	if err != nil {
		l.logger.V(4).Info("Failed to connect to backend", "err", err, "protocol", l.protocol, "port", l.port, "endpoint", ep)
		return
	}
	defer backendConn.Close() //nolint:errcheck

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = clientConn.Close()
		_ = backendConn.Close()
	}()

	pipe := func(dst, src net.Conn, direction string) {
		_, err := io.Copy(dst, src)
		if err != nil && connCtx.Err() == nil {
			l.logger.V(4).Info("Error proxying localhost nodeport connection",
				"err", err, "protocol", l.protocol, "port", l.port, "direction", direction)
		}
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}

	// 双向转发必须都在进行中：一侧的 CloseWrite 让对端能看到 EOF 并正常收尾。
	var wg sync.WaitGroup
	wg.Go(func() { pipe(backendConn, clientConn, "client->backend") })
	wg.Go(func() { pipe(clientConn, backendConn, "backend->client") })
	wg.Wait()
}

// pickEndpoint 选择一个后端 Endpoint：启用 ClientIP 亲和性且在滑动窗口内时复用已绑定的 Endpoint，
// 否则随机选择。绑定中的 Endpoint 若已从集合移除，则重新选择，避免把流量发往已下线后端。
func (l *nodePortListener) pickEndpoint() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.endpoints) == 0 {
		return ""
	}
	now := l.clock.Now()
	if l.affinityTimeout > 0 && l.pinnedEndpoint != "" && now.Sub(l.pinnedLastUsed) <= l.affinityTimeout {
		for _, ep := range l.endpoints {
			if ep == l.pinnedEndpoint {
				l.pinnedLastUsed = now
				return ep
			}
		}
		// 绑定的 Endpoint 已不在集合中，落到下面的随机选择逻辑。
	}
	ep := l.endpoints[rand.IntN(len(l.endpoints))]
	if l.affinityTimeout > 0 {
		l.pinnedEndpoint = ep
		l.pinnedLastUsed = now
	}
	return ep
}

// update 用最新的 Endpoint 集合与亲和配置刷新监听器；亲和配置变化时清除旧的绑定，
// 防止沿用旧超时时间的绑定，也避免绑定到已被移除的 Endpoint。
func (l *nodePortListener) update(spec *NodePortSpec) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.endpoints = spec.Endpoints
	newTimeout := spec.affinityTimeout()
	if l.affinityTimeout != newTimeout {
		// 亲和配置已变更：清掉旧绑定，避免继续按旧规则固定流量。
		l.pinnedEndpoint = ""
		l.affinityTimeout = newTimeout
	}
}

// shutdown 取消监听器的上下文并关闭监听套接字：
// 前者强制中断在途连接，后者让 Accept 返回，从而回收 acceptLoop goroutine。
func (l *nodePortListener) shutdown() {
	l.cancel()
	_ = l.listener.Close()
}
