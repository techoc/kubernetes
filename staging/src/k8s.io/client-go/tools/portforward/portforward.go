/*
Copyright 2015 The Kubernetes Authors.

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

package portforward

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	streamhttp "k8s.io/streaming/pkg/httpstream"
	netutils "k8s.io/utils/net"
)

// PortForwardProtocolV1Name 是端口转发使用的子协议。
// TODO：移至 API machinery，并与 kubelet/server/portforward 统一。
const PortForwardProtocolV1Name = "portforward.k8s.io"

var (
	// ErrLostConnectionToPod 表示与 Pod 的连接已丢失。
	ErrLostConnectionToPod = errors.New("lost connection to pod")

	// 端口转发期间预期会遇到的错误集合。
	networkClosedError = "use of closed network connection"
)

// PortForwarder 监听本地连接，并通过升级后的 HTTP 请求把它们转发到远端 Pod。
//
// 生命周期：ForwardPorts 建立一条到 pod 的复用连接，在其上按需创建数据流；
// 本地监听器与处理协程都依附于该连接，连接断开即整体失效。
type PortForwarder struct {
	addresses []listenAddress
	ports     []ForwardedPort
	stopChan  <-chan struct{}

	logger     klog.Logger
	dialer     httpstream.Dialer
	streamConn httpstream.Connection
	listeners  []io.Closer
	// Ready 在所有端口至少成功监听一个地址后关闭，用于向调用方通知就绪。
	// 为 nil 时 GetPorts 会报错，且 forward 不会尝试关闭它。
	Ready         chan struct{}
	requestIDLock sync.Mutex
	// requestID 为每个本地连接分配唯一编号，服务端据此把数据流与错误流配对。
	requestID int
	out       io.Writer
	errOut    io.Writer
}

// ForwardedPort 保存一组 Local:Remote 端口配对。
type ForwardedPort struct {
	// Local 是本地监听端口；请求端口 0 时会被替换为实际分配的端口。
	Local uint16
	// Remote 是 Pod 内目标端口，必须大于 0。
	Remote uint16
}

/*
合法的端口规格：

5000
- 从 localhost:5000 转发到 pod:5000

8888:5000
- 从 localhost:8888 转发到 pod:5000

0:5000
:5000
  - 随机选择一个可用的本地端口，
    从 localhost:<随机端口> 转发到 pod:5000
*/
// parsePorts 解析 “local:remote” 形式的端口规格。
//
// 仅给一个端口时本地与远端相同；本地为 0 表示由内核分配随机端口。
// 远端端口为 0 没有意义，直接拒绝。
func parsePorts(ports []string) ([]ForwardedPort, error) {
	var forwards []ForwardedPort
	for _, portString := range ports {
		parts := strings.Split(portString, ":")
		var localString, remoteString string
		if len(parts) == 1 {
			localString = parts[0]
			remoteString = parts[0]
		} else if len(parts) == 2 {
			localString = parts[0]
			if localString == "" {
				// 支持 :5000 这种省略本地端口的写法
				localString = "0"
			}
			remoteString = parts[1]
		} else {
			return nil, fmt.Errorf("invalid port format '%s'", portString)
		}

		localPort, err := strconv.ParseUint(localString, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("error parsing local port '%s': %s", localString, err)
		}

		remotePort, err := strconv.ParseUint(remoteString, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("error parsing remote port '%s': %s", remoteString, err)
		}
		if remotePort == 0 {
			return nil, fmt.Errorf("remote port must be > 0")
		}

		forwards = append(forwards, ForwardedPort{uint16(localPort), uint16(remotePort)})
	}

	return forwards, nil
}

// listenAddress 描述一个待监听的地址及其协议与失败策略。
//
// failureMode “all” 表示该组地址必须全部成功（如 localhost 的 v4+v6），
// “any” 表示至少一个成功即可，用于可选地址族。
type listenAddress struct {
	address     string
	protocol    string
	failureMode string
}

// parseAddresses 解析并去重监听地址。
//
// “localhost” 被展开为 127.0.0.1 与 ::1 两个地址；最终排序保证测试与日志
// 顺序稳定。
func parseAddresses(addressesToParse []string) ([]listenAddress, error) {
	var addresses []listenAddress
	parsed := make(map[string]listenAddress)
	for _, address := range addressesToParse {
		if address == "localhost" {
			if _, exists := parsed["127.0.0.1"]; !exists {
				ip := listenAddress{address: "127.0.0.1", protocol: "tcp4", failureMode: "all"}
				parsed[ip.address] = ip
			}
			if _, exists := parsed["::1"]; !exists {
				ip := listenAddress{address: "::1", protocol: "tcp6", failureMode: "all"}
				parsed[ip.address] = ip
			}
		} else if netutils.ParseIPSloppy(address).To4() != nil {
			parsed[address] = listenAddress{address: address, protocol: "tcp4", failureMode: "any"}
		} else if netutils.ParseIPSloppy(address) != nil {
			parsed[address] = listenAddress{address: address, protocol: "tcp6", failureMode: "any"}
		} else {
			return nil, fmt.Errorf("%s is not a valid IP", address)
		}
	}
	addresses = make([]listenAddress, len(parsed))
	id := 0
	for _, v := range parsed {
		addresses[id] = v
		id++
	}
	// 返回前排序，保证地址顺序稳定。
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].address < addresses[j].address })

	return addresses, nil
}

// New 创建一个使用 localhost 监听地址的 PortForwarder。
func New(dialer httpstream.Dialer, ports []string, stopChan <-chan struct{}, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	return NewOnAddresses(dialer, []string{"localhost"}, ports, stopChan, readyChan, out, errOut)
}

// NewForStreaming 创建一个使用 localhost 监听地址的 PortForwarder，
// 供使用 k8s.io/streaming/pkg/httpstream 类型的 in-tree 调用方使用。
func NewForStreaming(dialer streamhttp.Dialer, ports []string, stopChan <-chan struct{}, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	return NewOnAddressesForStreaming(dialer, []string{"localhost"}, ports, stopChan, readyChan, out, errOut)
}

// NewOnAddresses 创建一个使用自定义监听地址的 PortForwarder。
//
//logcheck:context // NewOnAddressesWithContext should be used instead of NewOnAddresses in code which supports contextual logging.
func NewOnAddresses(dialer httpstream.Dialer, addresses []string, ports []string, stopChan <-chan struct{}, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	return NewOnAddressesWithContext(wait.ContextForChannel(stopChan), dialer, addresses, ports, readyChan, out, errOut)
}

// NewOnAddressesWithContext 创建一个使用自定义监听地址的 PortForwarder。
//
// 只做参数校验与解析，不建立连接；真正拨号发生在 ForwardPorts。
func NewOnAddressesWithContext(ctx context.Context, dialer httpstream.Dialer, addresses []string, ports []string, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	if len(addresses) == 0 {
		return nil, errors.New("you must specify at least 1 address")
	}
	parsedAddresses, err := parseAddresses(addresses)
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, errors.New("you must specify at least 1 port")
	}
	parsedPorts, err := parsePorts(ports)
	if err != nil {
		return nil, err
	}
	return &PortForwarder{
		logger:    klog.FromContext(ctx),
		dialer:    dialer,
		addresses: parsedAddresses,
		ports:     parsedPorts,
		stopChan:  ctx.Done(),
		Ready:     readyChan,
		out:       out,
		errOut:    errOut,
	}, nil
}

// NewOnAddressesForStreaming 创建一个使用自定义监听地址的 PortForwarder，
// 供使用 k8s.io/streaming/pkg/httpstream 类型的 in-tree 调用方使用。
//
//logcheck:context // NewOnAddressesForStreamingWithContext should be used instead of NewOnAddressesForStreaming in code which supports contextual logging.
func NewOnAddressesForStreaming(dialer streamhttp.Dialer, addresses []string, ports []string, stopChan <-chan struct{}, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	return NewOnAddressesForStreamingWithContext(wait.ContextForChannel(stopChan), dialer, addresses, ports, readyChan, out, errOut)
}

// NewOnAddressesForStreamingWithContext 创建一个使用自定义监听地址的 PortForwarder，
// 供使用 k8s.io/streaming/pkg/httpstream 类型的 in-tree 调用方使用。
func NewOnAddressesForStreamingWithContext(ctx context.Context, dialer streamhttp.Dialer, addresses []string, ports []string, readyChan chan struct{}, out, errOut io.Writer) (*PortForwarder, error) {
	return NewOnAddressesWithContext(ctx, &compatDialerAdapter{logger: klog.FromContext(ctx), delegate: dialer}, addresses, ports, readyChan, out, errOut)
}

// compatDialerAdapter 把 k8s.io/streaming 的 Dialer 适配为 k8s.io/apimachinery
// 的 httpstream.Dialer，供 in-tree 调用方平滑迁移。
type compatDialerAdapter struct {
	logger   klog.Logger
	delegate streamhttp.Dialer
}

func (d *compatDialerAdapter) Dial(protocols ...string) (httpstream.Connection, string, error) {
	conn, protocol, err := d.delegate.Dial(protocols...)
	if err != nil {
		return nil, "", err
	}
	return &compatConnectionAdapter{logger: d.logger, delegate: conn}, protocol, nil
}

// compatConnectionAdapter 把 streaming 的 Connection 适配为 httpstream.Connection。
type compatConnectionAdapter struct {
	logger   klog.Logger
	delegate streamhttp.Connection
}

func (c *compatConnectionAdapter) CreateStream(headers http.Header) (httpstream.Stream, error) {
	stream, err := c.delegate.CreateStream(headers)
	if err != nil {
		return nil, err
	}
	return &compatStreamAdapter{delegate: stream}, nil
}

func (c *compatConnectionAdapter) Close() error {
	return c.delegate.Close()
}

func (c *compatConnectionAdapter) CloseChan() <-chan bool {
	return c.delegate.CloseChan()
}

func (c *compatConnectionAdapter) SetIdleTimeout(timeout time.Duration) {
	c.delegate.SetIdleTimeout(timeout)
}

func (c *compatConnectionAdapter) RemoveStreams(streams ...httpstream.Stream) {
	streamingStreams := make([]streamhttp.Stream, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		if s, ok := stream.(*compatStreamAdapter); ok {
			streamingStreams = append(streamingStreams, s.delegate)
			continue
		}
		if s, ok := stream.(streamhttp.Stream); ok {
			streamingStreams = append(streamingStreams, s)
			continue
		}
		c.logger.V(5).Info("dropping unadaptable stream in portforward RemoveStreams", "streamType", fmt.Sprintf("%T", stream))
	}
	c.delegate.RemoveStreams(streamingStreams...)
}

// compatStreamAdapter 把 streaming 的 Stream 适配为 httpstream.Stream。
type compatStreamAdapter struct {
	delegate streamhttp.Stream
}

func (s *compatStreamAdapter) Read(p []byte) (int, error) {
	return s.delegate.Read(p)
}

func (s *compatStreamAdapter) Write(p []byte) (int, error) {
	return s.delegate.Write(p)
}

func (s *compatStreamAdapter) Close() error {
	return s.delegate.Close()
}

func (s *compatStreamAdapter) Reset() error {
	return s.delegate.Reset()
}

func (s *compatStreamAdapter) Headers() http.Header {
	return s.delegate.Headers()
}

func (s *compatStreamAdapter) Identifier() uint32 {
	return s.delegate.Identifier()
}

// ForwardPorts 构造并执行一次端口转发请求。连接会一直保持打开，直到 stopChan 关闭。
//
// 必须协商出 portforward.k8s.io 子协议，否则说明服务端不支持该功能；
// 返回时无论成败都会关闭监听器与底层连接。
func (pf *PortForwarder) ForwardPorts() error {
	defer pf.Close()

	var err error
	var protocol string
	pf.streamConn, protocol, err = pf.dialer.Dial(PortForwardProtocolV1Name)
	if err != nil {
		return fmt.Errorf("error upgrading connection: %s", err)
	}
	defer pf.streamConn.Close()
	if protocol != PortForwardProtocolV1Name {
		return fmt.Errorf("unable to negotiate protocol: client supports %q, server returned %q", PortForwardProtocolV1Name, protocol)
	}

	return pf.forward()
}

// forward 拨号远端 host、升级请求、为 ports 中的每个端口启动监听器，
// 并通过数据流把本地连接转发到远端 host。
func (pf *PortForwarder) forward() error {
	var err error

	// 只要有一个端口监听成功就继续：部分端口被占用不应让整个转发失败，
	// 但要向用户报告失败的端口。
	listenSuccess := false
	for i := range pf.ports {
		port := &pf.ports[i]
		err = pf.listenOnPort(port)
		switch {
		case err == nil:
			listenSuccess = true
		default:
			if pf.errOut != nil {
				fmt.Fprintf(pf.errOut, "Unable to listen on port %d: %v\n", port.Local, err)
			}
		}
	}

	if !listenSuccess {
		return fmt.Errorf("unable to listen on any of the requested ports: %v", pf.ports)
	}

	if pf.Ready != nil {
		close(pf.Ready)
	}

	// 等待调用方停止或远程连接断开。后者意味着 pod 已消失或 kubelet 关闭
	// 了流，必须让调用方感知，而不是假装正常结束。
	select {
	case <-pf.stopChan:
	case <-pf.streamConn.CloseChan():
		return ErrLostConnectionToPod
	}

	return nil
}

// listenOnPort 负责创建监听器，并在请求的绑定地址上等待连接。
// 是否报错取决于地址分组（default 与 localhost）及其失败模式。
//
// 判定规则：failureMode=all 的地址组至少要有一个成功；failureMode=any 的
// 地址组一旦有失败就整体失败，避免用户以为转发已覆盖所有地址。
func (pf *PortForwarder) listenOnPort(port *ForwardedPort) error {
	var errors []error
	failCounters := make(map[string]int, 2)
	successCounters := make(map[string]int, 2)
	for _, addr := range pf.addresses {
		err := pf.listenOnPortAndAddress(port, addr.protocol, addr.address)
		if err != nil {
			errors = append(errors, err)
			failCounters[addr.failureMode]++
		} else {
			successCounters[addr.failureMode]++
		}
	}
	if successCounters["all"] == 0 && failCounters["all"] > 0 {
		return fmt.Errorf("%s: %v", "Listeners failed to create with the following errors", errors)
	}
	if failCounters["any"] > 0 {
		return fmt.Errorf("%s: %v", "Listeners failed to create with the following errors", errors)
	}
	return nil
}

// listenOnPortAndAddress 创建监听器，并在后台等待新连接。
//
// 每个 listener 一个接收协程；listener 关闭时协程自然退出。
func (pf *PortForwarder) listenOnPortAndAddress(port *ForwardedPort, protocol string, address string) error {
	listener, err := pf.getListener(protocol, address, port)
	if err != nil {
		return err
	}
	pf.listeners = append(pf.listeners, listener)
	go pf.waitForConnection(listener, *port)
	return nil
}

// getListener 用给定协议在给定端口上为 hostname 指向的网卡创建监听器。
// protocol 采用 net.Listen 风格，基本接受 tcp、tcp4、tcp6 这类取值。
//
// 监听端口为 0 时由内核分配，并把实际端口写回 port.Local，以便 GetPorts 暴露给调用方。
func (pf *PortForwarder) getListener(protocol string, hostname string, port *ForwardedPort) (net.Listener, error) {
	listener, err := net.Listen(protocol, net.JoinHostPort(hostname, strconv.Itoa(int(port.Local))))
	if err != nil {
		return nil, fmt.Errorf("unable to create listener: Error %s", err)
	}
	listenerAddress := listener.Addr().String()
	host, localPort, _ := net.SplitHostPort(listenerAddress)
	localPortUInt, err := strconv.ParseUint(localPort, 10, 16)

	if err != nil {
		fmt.Fprintf(pf.out, "Failed to forward from %s:%d -> %d\n", hostname, localPortUInt, port.Remote)
		return nil, fmt.Errorf("error parsing local port: %s from %s (%s)", err, listenerAddress, host)
	}
	port.Local = uint16(localPortUInt)
	if pf.out != nil {
		fmt.Fprintf(pf.out, "Forwarding from %s -> %d\n", net.JoinHostPort(hostname, strconv.Itoa(int(localPortUInt))), port.Remote)
	}

	return listener, nil
}

// waitForConnection 等待 listener 上的新连接，并在后台处理它们。
func (pf *PortForwarder) waitForConnection(listener net.Listener, port ForwardedPort) {
	for {
		select {
		case <-pf.streamConn.CloseChan():
			// 远程连接已断开，继续 Accept 没有意义。
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				// TODO: 考虑使用类似 https://github.com/hydrogen18/stoppableListener 的方案
				if !strings.Contains(strings.ToLower(err.Error()), networkClosedError) {
					runtime.HandleErrorWithLogger(pf.logger, err, "Error accepting connection", "localPort", port.Local)
				}
				return
			}
			go pf.handleConnection(conn, port)
		}
	}
}

// nextRequestID 在锁保护下分配自增编号，保证同一连接内唯一。
func (pf *PortForwarder) nextRequestID() int {
	pf.requestIDLock.Lock()
	defer pf.requestIDLock.Unlock()
	id := pf.requestID
	pf.requestID++
	return id
}

// handleConnection 在本地连接与到远端 server 的数据流之间双向拷贝数据。
//
// 每个本地连接创建两条流：error 流承载服务端返回的错误文本，data 流承载
// 字节。两者通过 requestID 关联，服务端据此把错误归到正确的连接。
func (pf *PortForwarder) handleConnection(conn net.Conn, port ForwardedPort) {
	defer conn.Close()

	if pf.out != nil {
		fmt.Fprintf(pf.out, "Handling connection for %d\n", port.Local)
	}

	requestID := pf.nextRequestID()

	// 创建错误流
	headers := http.Header{}
	headers.Set(v1.StreamType, v1.StreamTypeError)
	headers.Set(v1.PortHeader, fmt.Sprintf("%d", port.Remote))
	headers.Set(v1.PortForwardRequestIDHeader, strconv.Itoa(requestID))
	errorStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		runtime.HandleErrorWithLogger(pf.logger, err, "Error creating error stream", "localPort", port.Local, "remotePort", port.Remote)
		return
	}
	// 本端不向该流写入数据
	errorStream.Close()
	defer pf.streamConn.RemoveStreams(errorStream)

	// 异步读取错误流：服务端可能先回错误再关闭，必须同时等待数据流与
	// 错误流，不能顺序读取。
	type readAllResult struct {
		message []byte
		err     error
	}
	errorChan := make(chan readAllResult)
	go func() {
		message, err := io.ReadAll(errorStream)
		errorChan <- readAllResult{
			message: message,
			err:     err,
		}
		close(errorChan)
	}()

	// 创建数据流
	headers.Set(v1.StreamType, v1.StreamTypeData)
	dataStream, err := pf.streamConn.CreateStream(headers)
	if err != nil {
		runtime.HandleErrorWithLogger(pf.logger, err, "Error creating forwarding stream", "localPort", port.Local, "remotePort", port.Remote)
		return
	}
	defer pf.streamConn.RemoveStreams(dataStream)

	localError := make(chan struct{})
	remoteDone := make(chan struct{})

	go func() {
		// 把远端数据拷贝回本地端口。
		if _, err := io.Copy(conn, dataStream); err != nil && !strings.Contains(strings.ToLower(err.Error()), networkClosedError) {
			runtime.HandleErrorWithLogger(pf.logger, err, "Error copying from remote stream to local connection", "localPort", port.Local, "remotePort", port.Remote)
		}

		// 告知下面的 select：远端拷贝已完成
		close(remoteDone)
	}()

	go func() {
		// copy 解除阻塞后告知 server 不再发送数据
		defer dataStream.Close()

		// 把本地端口的数据拷贝到远端。
		if _, err := io.Copy(dataStream, conn); err != nil && !strings.Contains(strings.ToLower(err.Error()), networkClosedError) {
			runtime.HandleErrorWithLogger(pf.logger, err, "Error copying from local connection to remote stream", "localPort", port.Local, "remotePort", port.Remote)
			// 直接跳出下面的 select，不再等待另一路拷贝结束
			close(localError)
		}
	}()

	// 等待本地到远端出错，或远端到本地的拷贝完成
	select {
	case <-remoteDone:
	case <-localError:
	}

	// 重置 dataStream 以丢弃尚未发送的数据，避免端口转发被阻塞。
	// 必须在等待 errorChan 之前重置 dataStream，否则阻塞中的数据会影响
	// errorStream，导致 <-errorChan 永久阻塞。
	_ = dataStream.Reset()

	// errorChan 上总会有结果（可能为空）
	errResult := <-errorChan
	switch {
	case errResult.err != nil:
		runtime.HandleErrorWithLogger(pf.logger, errResult.err, "Error reading from error stream", "localPort", port.Local, "remotePort", port.Remote)
		pf.streamConn.Close()
	case len(errResult.message) > 0:
		runtime.HandleErrorWithLogger(pf.logger, errors.New(string(errResult.message)), "An error occurred forwarding", "localPort", port.Local, "remotePort", port.Remote)
		pf.streamConn.Close()
	}
}

// Close 停止 PortForwarder 的全部监听器。
//
// 只关闭本地监听器；远程连接由 ForwardPorts 的 defer 负责关闭。
func (pf *PortForwarder) Close() {
	// 停止所有监听器
	for _, l := range pf.listeners {
		if err := l.Close(); err != nil {
			runtime.HandleErrorWithLogger(pf.logger, err, "Error closing listener")
		}
	}
}

// GetPorts 返回已被转发的端口；当输入端口为 0 时，可据此取得本地实际绑定的端口。
// 若 Ready channel 为 nil 或监听器尚未就绪则返回错误；Ready 关闭后调用即可成功。
//
// 非阻塞：Ready 未关闭时立即返回错误，调用方应自行重试或先等待 Ready。
func (pf *PortForwarder) GetPorts() ([]ForwardedPort, error) {
	if pf.Ready == nil {
		return nil, fmt.Errorf("no Ready channel provided")
	}
	select {
	case <-pf.Ready:
		return pf.ports, nil
	default:
		return nil, fmt.Errorf("listeners not ready")
	}
}
