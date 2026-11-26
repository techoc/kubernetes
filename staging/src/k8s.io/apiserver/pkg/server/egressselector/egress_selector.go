/*
Copyright 2019 The Kubernetes Authors.

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

// Package egressselector 根据 EgressSelectorConfiguration 为不同网络上下文
// （controlplane/etcd/cluster）选择出站拨号方式：直连、HTTP CONNECT 代理或
// konnectivity gRPC 隧道。
package egressselector

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/apis/apiserver"
	egressmetrics "k8s.io/apiserver/pkg/server/egressselector/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/client"
)

var directDialer utilnet.DialFunc = http.DefaultTransport.(*http.Transport).DialContext

// init 把 konnectivity 客户端指标注册到 apiserver 的 registry，
// 使隧道连接质量可以在同一 /metrics 端点观察。
func init() {
	client.Metrics.RegisterMetrics(legacyregistry.Registerer())
}

// EgressSelector 是网络上下文类型到上下文拨号器的映射，用于网络出站。
// 构造后只读，因此可被多个组件并发 Lookup；缺失的上下文回退到直连。
type EgressSelector struct {
	egressToDialer map[EgressType]utilnet.DialFunc
}

// EgressType 指示发送流量时应使用哪种出口选择。
// 参见 https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/1281-network-proxy/README.md#network-context
type EgressType int

const (
	// ControlPlane 是发往控制面的流量对应的 EgressType：
	// apiserver 访问 kubelet/其他控制面组件时使用。
	ControlPlane EgressType = iota
	// Etcd 是发往 Kubernetes 持久化存储的流量对应的 EgressType。
	Etcd
	// Cluster 是发往由 Kubernetes 管理的系统的流量对应的 EgressType。
	Cluster
)

// NetworkContext 是 Kubernetes API Server 用来指示流量发往何处的结构体。
type NetworkContext struct {
	// EgressSelectionName 是 EgressSelectorConfiguration 中的唯一名字，
	// 决定把流量路由到哪个网络。
	//
	EgressSelectionName EgressType
}

// Lookup 用于获取网络上下文对应的拨号函数。
type Lookup func(networkContext NetworkContext) (utilnet.DialFunc, error)

// String 返回出口类型的规范字符串表示。
func (s EgressType) String() string {
	switch s {
	case ControlPlane:
		return "controlplane"
	case Etcd:
		return "etcd"
	case Cluster:
		return "cluster"
	default:
		return "invalid"
	}
}

// AsNetworkContext 是便捷 helper，用于获取基本的 NetworkContext 对象。
func (s EgressType) AsNetworkContext() NetworkContext {
	return NetworkContext{EgressSelectionName: s}
}

// lookupServiceName 把配置文件中的名字解析为 EgressType。
// 大小写不敏感，但未知名字必须报错——静默回退会隐藏配置拼写错误。
func lookupServiceName(name string) (EgressType, error) {
	switch strings.ToLower(name) {
	case "controlplane":
		return ControlPlane, nil
	case "etcd":
		return Etcd, nil
	case "cluster":
		return Cluster, nil
	}
	return -1, fmt.Errorf("unrecognized service name %s", name)
}

// tunnelHTTPConnect 通过已有代理连接执行 HTTP CONNECT 隧道握手。
// 限制响应头大小以防恶意代理撑爆内存；握手成功后不能有残留缓冲
// 数据，否则后续 TLS 字节流会错位。
func tunnelHTTPConnect(proxyConn net.Conn, proxyAddress, addr string) (net.Conn, error) {
	fmt.Fprintf(proxyConn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", addr, "127.0.0.1")

	// 如 https://go.dev/issue/74633 所述，行为异常的代理服务器
	// 可能导致客户端内存耗尽。https://go.dev/cl/698915 中的修复
	// 只覆盖 http.Transport 的使用者，这里施加同样的限制。
	//
	// 限制代理服务器可发送给我们的响应头大小。
	br := bufio.NewReader(io.LimitReader(proxyConn, http.DefaultMaxHeaderBytes))

	res, err := http.ReadResponse(br, nil)
	if err != nil {
		proxyConn.Close()
		return nil, fmt.Errorf("reading HTTP response from CONNECT to %s via proxy %s failed: %v",
			addr, proxyAddress, err)
	}
	if res.StatusCode != 200 {
		proxyConn.Close()
		return nil, fmt.Errorf("proxy error from %s while dialing %s, code %d: %v",
			proxyAddress, addr, res.StatusCode, res.Status)
	}

	// 这里可以安全地丢弃 bufio.Reader 并直接返回原始 TCP 连接，因为只用于 TLS；
	// TLS 中客户端先发话，因此不会有未缓冲的数据。但仍做一次检查确认。
	//
	// 不会有未缓冲的数据。但仍然复查一次。
	if br.Buffered() > 0 {
		proxyConn.Close()
		return nil, fmt.Errorf("unexpected %d bytes of buffered data from CONNECT proxy %q",
			br.Buffered(), proxyAddress)
	}
	return proxyConn, nil
}

type proxier interface {
	// proxy 返回指向 addr 的连接。
	proxy(ctx context.Context, addr string) (net.Conn, error)
}

var _ proxier = &httpConnectProxier{}

// httpConnectProxier 复用一条代理连接，每次 proxy 都发起新的 CONNECT。
// 调用方需自行保证 conn 的生命周期，proxier 不负责关闭底层连接。
type httpConnectProxier struct {
	conn         net.Conn
	proxyAddress string
}

func (t *httpConnectProxier) proxy(ctx context.Context, addr string) (net.Conn, error) {
	return tunnelHTTPConnect(t.conn, t.proxyAddress, addr)
}

var _ proxier = &grpcProxier{}

// grpcProxier 通过 konnectivity 隧道拨号；隧道由客户端库管理重连。
type grpcProxier struct {
	tunnel client.Tunnel
}

func (g *grpcProxier) proxy(ctx context.Context, addr string) (net.Conn, error) {
	return g.tunnel.DialContext(ctx, "tcp", addr)
}

type proxyServerConnector interface {
	// connect 建立到代理服务器的连接，并返回基于该连接的 proxier。
	//
	//
	// 传入的 Context 必须非 nil，且只用于建立连接。
	// 若连接在 context 过期前未完成，则返回错误。
	// 一旦连接成功建立，之后 context 过期不会影响该连接。
	connect(context.Context) (proxier, error)
}

// tcpHTTPConnectConnector 通过 TCP+TLS 连接 HTTP CONNECT 代理。
type tcpHTTPConnectConnector struct {
	proxyAddress string
	tlsConfig    *tls.Config
}

func (t *tcpHTTPConnectConnector) connect(ctx context.Context) (proxier, error) {
	d := tls.Dialer{
		Config: t.tlsConfig,
	}
	conn, err := d.DialContext(ctx, "tcp", t.proxyAddress)
	if err != nil {
		return nil, err
	}
	return &httpConnectProxier{conn: conn, proxyAddress: t.proxyAddress}, nil
}

// udsHTTPConnectConnector 通过 Unix domain socket 连接本机 CONNECT 代理，
// 常见于代理与应用同 Pod 部署的场景。
type udsHTTPConnectConnector struct {
	udsName string
}

func (u *udsHTTPConnectConnector) connect(ctx context.Context) (proxier, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", u.udsName)
	if err != nil {
		return nil, err
	}
	return &httpConnectProxier{conn: conn, proxyAddress: u.udsName}, nil
}

// udsGRPCConnector 通过 Unix domain socket 与 konnectivity server 建立 gRPC 隧道。
type udsGRPCConnector struct {
	udsName string
}

// connect 通过 gRPC 建立到代理的连接。
// TODO 目前不使用传入的 context。
func (u *udsGRPCConnector) connect(_ context.Context) (proxier, error) {
	udsName := u.udsName
	dialOption := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "unix", udsName)
		if err != nil {
			klog.Errorf("failed to create connection to uds name %s, error: %v", udsName, err)
		}
		return c, err
	})

	// CreateSingleUseGrpcTunnel() 不巧把拨号与连接 context 耦合在一起，因此无法只用 ctx 拨号而单独控制连接生命周期。
	//
	// See https://github.com/kubernetes-sigs/apiserver-network-proxy/issues/357.
	tunnelCtx := context.TODO()
	tunnel, err := client.CreateSingleUseGrpcTunnel(tunnelCtx, udsName, dialOption,
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
		grpc.WithTimeout(30*time.Second), // 与 http.DefaultTransport 的拨号超时一致。
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &grpcProxier{tunnel: tunnel}, nil
}

// dialerCreator 组合连接器与指标标签，生成最终的拨号函数。
type dialerCreator struct {
	connector proxyServerConnector
	direct    bool
	options   metricsOptions
}

// metricsOptions 是拨号指标的两个低基数标签：传输方式与协议。
type metricsOptions struct {
	transport string
	protocol  string
}

// createDialer 返回直连拨号器或带指标的代理拨号器。
// 失败指标区分 connect 与 proxy 两个阶段，便于判断是代理不可达
// 还是目标地址不可达。
func (d *dialerCreator) createDialer() utilnet.DialFunc {
	if d.direct {
		return directDialer
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, span := tracing.Start(ctx, fmt.Sprintf("Proxy via %s protocol over %s", d.options.protocol, d.options.transport), attribute.String("address", addr))
		defer span.End(500 * time.Millisecond)
		start := egressmetrics.Metrics.Clock().Now()
		egressmetrics.Metrics.ObserveDialStart(d.options.protocol, d.options.transport)
		proxier, err := d.connector.connect(ctx)
		if err != nil {
			egressmetrics.Metrics.ObserveDialFailure(d.options.protocol, d.options.transport, egressmetrics.StageConnect)
			return nil, err
		}
		conn, err := proxier.proxy(ctx, addr)
		if err != nil {
			egressmetrics.Metrics.ObserveDialFailure(d.options.protocol, d.options.transport, egressmetrics.StageProxy)
			return nil, err
		}
		egressmetrics.Metrics.ObserveDialLatency(egressmetrics.Metrics.Clock().Now().Sub(start), d.options.protocol, d.options.transport)
		return conn, nil
	}
}

// getTLSConfig 从配置构造客户端 TLS 配置。
// 未提供 CA 时使用系统根证书（certPool=nil），便于在托管环境中
// 复用主机信任链；提供 CA 则只信任该 CA，收紧信任范围。
func getTLSConfig(t *apiserver.TLSConfig) (*tls.Config, error) {
	clientCert := t.ClientCert
	clientKey := t.ClientKey
	caCert := t.CABundle
	clientCerts, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read key pair %s & %s, got %v", clientCert, clientKey, err)
	}
	certPool := x509.NewCertPool()
	if caCert != "" {
		certBytes, err := os.ReadFile(caCert)
		if err != nil {
			return nil, fmt.Errorf("failed to read cert file %s, got %v", caCert, err)
		}
		ok := certPool.AppendCertsFromPEM(certBytes)
		if !ok {
			return nil, fmt.Errorf("failed to append CA cert to the cert pool")
		}
	} else {
		// 使用主机的根 CA 集合，而不是自带 CA。
		certPool = nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{clientCerts},
		RootCAs:      certPool,
		ServerName:   t.TLSServerName,
	}, nil
}

// getProxyAddress 从 URL 中提取 host:port，忽略 scheme 与路径。
func getProxyAddress(urlString string) (string, error) {
	proxyURL, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("invalid proxy server url %q: %v", urlString, err)
	}
	return proxyURL.Host, nil
}

// connectionToDialerCreator 把配置中的连接方式映射为具体连接器。
// gRPC 仅支持 UDS 是当前实现限制，返回明确错误而非静默回退。
func connectionToDialerCreator(c apiserver.Connection) (*dialerCreator, error) {
	switch c.ProxyProtocol {

	case apiserver.ProtocolHTTPConnect:
		if c.Transport.UDS != nil {
			return &dialerCreator{
				connector: &udsHTTPConnectConnector{
					udsName: c.Transport.UDS.UDSName,
				},
				options: metricsOptions{
					transport: egressmetrics.TransportUDS,
					protocol:  egressmetrics.ProtocolHTTPConnect,
				},
			}, nil
		} else if c.Transport.TCP != nil {
			tlsConfig, err := getTLSConfig(c.Transport.TCP.TLSConfig)
			if err != nil {
				return nil, err
			}
			proxyAddress, err := getProxyAddress(c.Transport.TCP.URL)
			if err != nil {
				return nil, err
			}
			return &dialerCreator{
				connector: &tcpHTTPConnectConnector{
					tlsConfig:    tlsConfig,
					proxyAddress: proxyAddress,
				},
				options: metricsOptions{
					transport: egressmetrics.TransportTCP,
					protocol:  egressmetrics.ProtocolHTTPConnect,
				},
			}, nil
		} else {
			return nil, fmt.Errorf("Either a TCP or UDS transport must be specified")
		}
	case apiserver.ProtocolGRPC:
		if c.Transport.UDS != nil {
			return &dialerCreator{
				connector: &udsGRPCConnector{
					udsName: c.Transport.UDS.UDSName,
				},
				options: metricsOptions{
					transport: egressmetrics.TransportUDS,
					protocol:  egressmetrics.ProtocolGRPC,
				},
			}, nil
		}
		return nil, fmt.Errorf("UDS transport must be specified for GRPC")
	case apiserver.ProtocolDirect:
		return &dialerCreator{direct: true}, nil
	default:
		return nil, fmt.Errorf("unrecognized service connection protocol %q", c.ProxyProtocol)
	}

}

// NewEgressSelector 配置 Lookup 的查找机制。
// 它基于启动时读取的 EgressSelectorConfiguration 完成配置。
// 返回 (nil, nil) 表示未配置，调用方应把 nil Lookup 视为"全部直连"；
// 任一选择项配置错误都会让整个构造失败，避免部分流量走错路径。
func NewEgressSelector(config *apiserver.EgressSelectorConfiguration) (*EgressSelector, error) {
	if config == nil || config.EgressSelections == nil {
		// 未配置任何 Connection Services 时 serviceMap 为空，将返回默认拨号器。
		return nil, nil
	}
	cs := &EgressSelector{
		egressToDialer: make(map[EgressType]utilnet.DialFunc),
	}
	for _, service := range config.EgressSelections {
		name, err := lookupServiceName(service.Name)
		if err != nil {
			return nil, err
		}
		dialerCreator, err := connectionToDialerCreator(service.Connection)
		if err != nil {
			return nil, fmt.Errorf("failed to create dialer for egressSelection %q: %v", name, err)
		}
		cs.egressToDialer[name] = dialerCreator.createDialer()
	}
	return cs, nil
}

// NewEgressSelectorWithMap 返回使用给定 EgressType 到 DialFunc 映射的 EgressSelector。
// 主要用于测试或已有自定义拨号器的高级场景；nil map 会被规范化为空 map。
func NewEgressSelectorWithMap(m map[EgressType]utilnet.DialFunc) *EgressSelector {
	if m == nil {
		m = make(map[EgressType]utilnet.DialFunc)
	}
	return &EgressSelector{
		egressToDialer: m,
	}
}

// Lookup 获取该网络上下文对应的拨号器函数。
// 该配置在 Kubernetes API Server 启动时确定。
// 返回 nil 拨号器是合法结果，表示使用默认网络栈；未在配置中出现的上下文也按直连处理，而不是报错。
// 上下文也按直连处理，而不是报错。
func (cs *EgressSelector) Lookup(networkContext NetworkContext) (utilnet.DialFunc, error) {
	if cs.egressToDialer == nil {
		// round trip wrapper 会相应地覆盖 dialContext 方法。
		return nil, nil
	}

	return cs.egressToDialer[networkContext.EgressSelectionName], nil
}
