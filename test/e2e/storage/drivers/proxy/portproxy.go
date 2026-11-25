/*
Copyright 2020 The Kubernetes Authors.

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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
)

// 转发连接数的上限。实际上每个 sidecar 和 kubelet 不需要超过一个连接。
// 把这个数值控制在较小的范围内，可以避免通过 apiserver 和远端内核建立
// 后续并不需要的连接。
//
// 下面的代理代码会预先建立这个数量的连接，而不等待远端有客户端接入。
// 在本地侧，gRPC server 会接受相同数量的连接，然后等待未来客户端发来的数据。
//
// 这样做的好处是远端客户端可以立即开始通信，不会因建立连接而产生延迟。
// 这个延迟足够大，以至于像 node-driver-registrar 这样 gRPC 超时时间很短的
// 客户端确实曾因此不必要地超时。
const maxConcurrentConnections = 10

// 这个延迟决定了我们多快能察觉到集群内有人建立了连接。
// 使用 socat 时不能把它设得太小，否则会产生大量被拒绝的连接。
// 使用 mock driver 作为代理时，只要不请求过多并发连接就不会出现这个问题，
// 因为 mock driver 始终保持着监听端口，Linux 内核会自动接受我们的连接请求。
const connectionPollInterval = 100 * time.Millisecond

// Listen 创建一个 listener，当有人连接到指定 Pod 内运行的 socat 或 mock driver 代理实例时，
// 它会返回新的连接。
//
// 要让这个机制工作，socat 必须以 "<listen>,fork TCP-LISTEN:<port>,reuseport" 启动。
// "<listen>" 可以是任何能接受连接的形式，例如 "UNIX-LISTEN:/csi/csi.sock"。
// 在这个模式下，socat 自身每接受一个连接，就会在指定端口上恰好接受一个连接。
//
// 当 context 结束或调用 Close() 时，监听会停止。
func Listen(ctx context.Context, clientset kubernetes.Interface, restConfig *rest.Config, addr Addr) (net.Listener, error) {
	// 我们通过端口转发建立连接。严格来说，
	// 因为我们并不需要本地端口，这样做有些多余。
	// 但这样可以复用 client-go/tools/portforward，
	// 而不必在自己的代码中重新实现 handleConnection。
	restClient := clientset.CoreV1().RESTClient()
	if restConfig.GroupVersion == nil {
		restConfig.GroupVersion = &schema.GroupVersion{}
	}
	if restConfig.NegotiatedSerializer == nil {
		restConfig.NegotiatedSerializer = scheme.Codecs
	}

	// 实际 portforward 周围的搭建代码来自
	// https://github.com/kubernetes/kubernetes/blob/c652ffbe4a29143623a1aaec39f745575f7e43ad/staging/src/k8s.io/kubectl/pkg/cmd/portforward/portforward.go
	req := restClient.Post().
		Resource("pods").
		Namespace(addr.Namespace).
		Name(addr.PodName).
		SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create round tripper: %w", err)
	}
	dialer := spdy.NewDialerForStreaming(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

	prefix := fmt.Sprintf("port forwarding for %s", addr)
	ctx, cancel := context.WithCancel(ctx)
	l := &listener{
		ctx:    ctx,
		cancel: cancel,
		addr:   addr,
	}

	var connectionsCreated int

	runForwarding := func() {
		klog.V(2).Infof("%s: starting connection polling", prefix)
		defer klog.V(2).Infof("%s: connection polling ended", prefix)

		tryConnect := time.NewTicker(connectionPollInterval)
		defer tryConnect.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tryConnect.C:
				func() {
					l.mutex.Lock()
					defer l.mutex.Unlock()

					for i, c := range l.connections {
						if c == nil {
							// apiserver 停机期间可能会反复失败，这无需担心。
							klog.V(7).Infof("%s: trying to create a new connection #%d", prefix, connectionsCreated)
							stream, err := dial(ctx, fmt.Sprintf("%s #%d", prefix, connectionsCreated), dialer, addr.Port)
							if err != nil {
								klog.V(7).Infof("%s: no connection: %v", prefix, err)
								return
							}
							// 让下面的 Accept 能够拿到这个连接。
							klog.V(5).Infof("%s: created a new connection #%d", prefix, connectionsCreated)
							c := &connection{
								l:       l,
								stream:  stream,
								addr:    addr,
								counter: connectionsCreated,
							}
							l.connections[i] = c
							connectionsCreated++
							return
						}
					}
				}()
			}
		}
	}

	// 端口转发和连接轮询在后台运行。
	go func() {
		for {
			running := false
			pod, err := clientset.CoreV1().Pods(addr.Namespace).Get(ctx, addr.PodName, metav1.GetOptions{})
			if err != nil {
				klog.V(5).Infof("checking for container %q in pod %s/%s: %v", addr.ContainerName, addr.Namespace, addr.PodName, err)
			}
			for i, status := range pod.Status.ContainerStatuses {
				if pod.Spec.Containers[i].Name == addr.ContainerName &&
					status.State.Running != nil {
					running = true
					break
				}
			}

			if running {
				klog.V(2).Infof("container %q in pod %s/%s is running", addr.ContainerName, addr.Namespace, addr.PodName)
				runForwarding()
			}

			select {
			case <-ctx.Done():
				return
			// 重启前稍作等待。这里是等待 Pod 启动的地方。
			case <-time.After(1 * time.Second):
			}
		}
	}()

	return l, nil
}

// Addr 包含 Pod 中某个端口的所有相关参数。
// 尝试建立连接前，容器必须已经运行。
type Addr struct {
	Namespace, PodName, ContainerName string
	Port                              int
}

var _ net.Addr = Addr{}

func (a Addr) Network() string {
	return "port-forwarding"
}

func (a Addr) String() string {
	return fmt.Sprintf("%s/%s:%d", a.Namespace, a.PodName, a.Port)
}

type stream struct {
	httpstream.Stream
	streamConn httpstream.Connection
}

func dial(ctx context.Context, prefix string, dialer httpstream.Dialer, port int) (s *stream, finalErr error) {
	streamConn, _, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
	if err != nil {
		return nil, fmt.Errorf("dialer failed: %w", err)
	}
	requestID := "1"
	defer func() {
		if finalErr != nil {
			streamConn.Close()
		}
	}()

	// 创建 error stream
	headers := http.Header{}
	headers.Set(v1.StreamType, v1.StreamTypeError)
	headers.Set(v1.PortHeader, fmt.Sprintf("%d", port))
	headers.Set(v1.PortForwardRequestIDHeader, requestID)

	// 我们不往这个 stream 写数据，只是从中读取错误消息。
	// 这是异步进行的。
	errorStream, err := streamConn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("error creating error stream: %w", err)
	}
	errorStream.Close()
	go func() {
		message, err := io.ReadAll(errorStream)
		switch {
		case err != nil:
			klog.Errorf("%s: error reading from error stream: %v", prefix, err)
		case len(message) > 0:
			klog.Errorf("%s: an error occurred connecting to the remote port: %v", prefix, string(message))
		}
	}()

	// 创建 data stream
	headers.Set(v1.StreamType, v1.StreamTypeData)
	dataStream, err := streamConn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("error creating data stream: %w", err)
	}

	return &stream{
		Stream:     dataStream,
		streamConn: streamConn,
	}, nil
}

func (s *stream) Close() {
	s.Stream.Close()
	s.streamConn.Close()
}

type listener struct {
	addr   Addr
	ctx    context.Context
	cancel func()

	mutex       sync.Mutex
	connections [maxConcurrentConnections]*connection
}

var _ net.Listener = &listener{}

func (l *listener) Close() error {
	klog.V(5).Infof("forward listener for %s: closing", l.addr)
	l.cancel()

	l.mutex.Lock()
	defer l.mutex.Unlock()
	for _, c := range l.connections {
		if c != nil {
			c.stream.Close()
		}
	}

	return nil
}

func (l *listener) Accept() (net.Conn, error) {
	tryAccept := time.NewTicker(connectionPollInterval)
	defer tryAccept.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return nil, errors.New("listening was stopped")
		case <-tryAccept.C:
			conn := func() net.Conn {
				l.mutex.Lock()
				defer l.mutex.Unlock()

				for _, c := range l.connections {
					if c != nil && !c.accepted {
						klog.V(5).Infof("forward listener for %s: got a new connection #%d", l.addr, c.counter)
						c.accepted = true
						return c
					}
				}
				return nil
			}()
			if conn != nil {
				return conn, nil
			}
		}
	}
}

type connection struct {
	l                *listener
	stream           *stream
	addr             Addr
	counter          int
	mutex            sync.Mutex
	accepted, closed bool
}

var _ net.Conn = &connection{}

func (c *connection) LocalAddr() net.Addr {
	return c.addr
}

func (c *connection) RemoteAddr() net.Addr {
	return c.addr
}

func (c *connection) SetDeadline(t time.Time) error {
	return nil
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *connection) Read(b []byte) (int, error) {
	n, err := c.stream.Read(b)
	if errors.Is(err, io.EOF) {
		klog.V(5).Infof("forward connection #%d for %s: remote side closed the stream", c.counter, c.addr)
	}
	return n, err
}

func (c *connection) Write(b []byte) (int, error) {
	n, err := c.stream.Write(b)
	if errors.Is(err, io.EOF) {
		klog.V(5).Infof("forward connection #%d for %s: remote side closed the stream", c.counter, c.addr)
	}
	return n, err
}

func (c *connection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.closed {
		// 日志和簿记只做一次。这个函数本身可能被多次调用。
		klog.V(5).Infof("forward connection #%d for %s: closing our side", c.counter, c.addr)

		c.l.mutex.Lock()
		defer c.l.mutex.Unlock()
		for i, c2 := range c.l.connections {
			if c2 == c {
				c.l.connections[i] = nil
				break
			}
		}
	}
	c.stream.Close()

	return nil
}

func (l *listener) Addr() net.Addr {
	return l.addr
}
