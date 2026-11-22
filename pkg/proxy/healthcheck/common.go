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
	"net"
	"net/http"

	netutils "k8s.io/utils/net"
)

// listener 抽象了监听行为，便于在测试中替换 ServiceHealthServer 和 ProxyHealthServer 的网络依赖。
type listener interface {
	// Listen 针对给定网络和地址创建 netutils.MultiListen。
	Listen(ctx context.Context, network string, addrs ...string) (net.Listener, error)
}

// httpServerFactory 抽象 HTTP 服务器的创建过程，便于测试。
type httpServerFactory interface {
	// New 创建满足 httpServer 接口的实例，标准 http.Server 即为其典型实现。
	New(handler http.Handler) httpServer
}

// httpServer 抽象 HTTP 服务器的 Serve/Close 行为，使 http.Server 可直接满足该接口。
type httpServer interface {
	// Serve 在给定监听器上提供服务，直到服务器关闭。
	Serve(listener net.Listener) error
	// Close 关闭服务器及其监听的连接。
	Close() error
}

// stdNetListener 基于 netutils.MultiListen 实现 listener，用于生产环境。
type stdNetListener struct{}

// Listen 在给定网络类型上监听所有指定地址。
func (stdNetListener) Listen(ctx context.Context, network string, addrs ...string) (net.Listener, error) {
	return netutils.MultiListen(ctx, network, addrs...)
}

// 编译期断言标准实现满足 listener 接口。
var _ listener = stdNetListener{}

// stdHTTPServerFactory 基于标准 http.Server 实现 httpServerFactory。
type stdHTTPServerFactory struct{}

// New 创建使用给定处理器的 http.Server。
func (stdHTTPServerFactory) New(handler http.Handler) httpServer {
	return &http.Server{
		Handler: handler,
	}
}

// 编译期断言标准实现满足 httpServerFactory 接口。
var _ httpServerFactory = stdHTTPServerFactory{}
