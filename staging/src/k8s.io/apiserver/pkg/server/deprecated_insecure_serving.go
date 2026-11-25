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

package server

import (
	"net"
	"net/http"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/rest"
)

// DeprecatedInsecureServingInfo 是不安全 http 服务器的核心上下文对象。
// 该 HTTP 服务不包含认证与授权。你不应该再使用它——sig-auth 对此很不满。
// 这是历史上 API Server 在 localhost:8080 上提供的明文端口，
// 无 TLS、无认证、无授权，任何能访问该端口的人都是集群管理员。
// 无 TLS、无认证、无授权，任何能访问该端口的人都是集群管理员。
// 保留类型仅为兼容旧测试与旧调用点，新代码必须使用 SecureServingInfo。
// 之所以仍保留 InsecureSuperuser 之类的辅助实现，是为了让仍走该路径的代码
// 不必到处特判 nil 用户——把「不安全」集中在一个显式类型里，便于审计与最终删除。
type DeprecatedInsecureServingInfo struct {
	// Listener 是服务器的网络监听器。名称中的 secure 是历史遗留笔误，这里实际是明文监听器。
	Listener net.Listener
	// optional server name for log messages
	Name string
}

// Serve 用给定 handler 启动一个不安全 http 服务器。仅当初始 listen 调用失败时才报错，不会阻塞。
// 超时参数与安全端口保持一致（IdleTimeout 对齐 http.DefaultTransport 的
// keep-alive，ReadHeaderTimeout 略低于请求总超时上界），以免明文端口成为
// 慢速头部攻击的缺口。MaxHeaderBytes 与头部值数量上限共同限制资源消耗。
// 返回的 stoppedCh 被有意丢弃：明文端口不参与优雅终止编排。
func (s *DeprecatedInsecureServingInfo) Serve(handler http.Handler, shutdownTimeout time.Duration, stopCh <-chan struct{}) error {
	insecureServer := &http.Server{
		Addr:           s.Listener.Addr().String(),
		Handler:        handler,
		MaxHeaderBytes: 1 << 20,

		IdleTimeout:       90 * time.Second, // matches http.DefaultTransport keep-alive timeout
		ReadHeaderTimeout: 32 * time.Second, // just shy of requestTimeoutUpperBound
	}

	// 仅在 go1.27+ 生效；旧版本为空实现，保持构建兼容。
	setMaxHeaderValueCount(insecureServer)

	if len(s.Name) > 0 {
		klog.Infof("Serving %s insecurely on %s", s.Name, s.Listener.Addr())
	} else {
		klog.Infof("Serving insecurely on %s", s.Listener.Addr())
	}
	_, _, err := RunServer(insecureServer, s.Listener, shutdownTimeout, stopCh)
	// NOTE: we do not handle stoppedCh returned by RunServer for graceful termination here
	return err
}

// NewLoopbackClientConfig 返回指向本机明文端口的 rest.Config，供内部组件访问 apiserver。
// 接收者为 nil 时返回 (nil, nil)，让调用方可以无条件调用而不必判空。
// QPS/Burst 调高是因为该 client 会被传给所有准入插件；若沿用默认限流，
// apiserver 高负载时准入调用会被自身限流拖慢（见 #22340、#22422）。
func (s *DeprecatedInsecureServingInfo) NewLoopbackClientConfig() (*rest.Config, error) {
	if s == nil {
		return nil, nil
	}

	host, port, err := LoopbackHostPort(s.Listener.Addr().String())
	if err != nil {
		return nil, err
	}

	return &rest.Config{
		Host: "http://" + net.JoinHostPort(host, port),
		// Increase QPS limits. The client is currently passed to all admission plugins,
		// and those can be throttled in case of higher load on apiserver - see #22340 and #22422
		// for more details. Once #22422 is fixed, we may want to remove it.
		QPS:   50,
		Burst: 100,
	}, nil
}

// InsecureSuperuser 实现 authenticator.Request，始终返回一个超级用户。
// 其效果等价于跳过认证与授权，但能让 apiserver 代码不再需要对 nil 用户
// 做特判来跳过授权检查。
// 认证与授权逻辑本身不需要为「不安全端口」写分支：这里伪造一个
// system:unsecured 用户并加入 system:masters，后续 RBAC 自然放行。
// 这样删掉不安全端口时，只需移除本类型，无需清理散落的 nil 判断。
type InsecureSuperuser struct{}

// AuthenticateRequest 始终成功并返回特权用户；audiences 从请求上下文透传，
// 以免审计与令牌校验逻辑因缺少 audience 而异常。
func (InsecureSuperuser) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	auds, _ := authenticator.AudiencesFrom(req.Context())
	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   "system:unsecured",
			Groups: []string{user.SystemPrivilegedGroup, user.AllAuthenticated},
		},
		Audiences: auds,
	}, true, nil
}
