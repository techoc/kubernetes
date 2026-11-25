/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/util"
)

const (
	// DefaultHostAcceptRE 是默认接受的 host 值。
	DefaultHostAcceptRE = "^localhost$,^127\\.0\\.0\\.1$,^\\[::1\\]$"
	// DefaultPathAcceptRE 是默认接受的路径。
	DefaultPathAcceptRE = "^.*"
	// DefaultPathRejectRE 是默认拒绝的路径集合。
	DefaultPathRejectRE = "^/api/.*/pods/.*/exec,^/api/.*/pods/.*/attach,^/api/.*/nodes/.*/proxy/run"
	// DefaultMethodRejectRE 是默认拒绝的 HTTP 方法集合。
	DefaultMethodRejectRE = "^$"
)

// FilterServer 拒绝与指定正则表达式均不匹配的请求
type FilterServer struct {
	// 只有匹配此正则的路径才会被接受
	AcceptPaths []*regexp.Regexp
	// 匹配此正则的路径会被拒绝，即使它们匹配上述规则
	RejectPaths []*regexp.Regexp
	// host 必须匹配此正则列表
	AcceptHosts []*regexp.Regexp
	// 匹配此正则的方法会被拒绝
	RejectMethods []*regexp.Regexp
	// 处理被接受请求的 delegate。
	delegate http.Handler
}

// MakeRegexpArray 将逗号分隔的正则列表拆分为 Regexp 对象数组。
func MakeRegexpArray(str string) ([]*regexp.Regexp, error) {
	if str == "" {
		return []*regexp.Regexp{}, nil
	}
	parts := strings.Split(str, ",")
	result := make([]*regexp.Regexp, len(parts))
	for ix := range parts {
		re, err := regexp.Compile(parts[ix])
		if err != nil {
			return nil, err
		}
		result[ix] = re
	}
	return result, nil
}

// MakeRegexpArrayOrDie 从字符串创建正则表达式对象数组，失败则退出。
func MakeRegexpArrayOrDie(str string) []*regexp.Regexp {
	result, err := MakeRegexpArray(str)
	if err != nil {
		klog.Fatalf("Error compiling re: %v", err)
	}
	return result
}

func matchesRegexp(str string, regexps []*regexp.Regexp) bool {
	for _, re := range regexps {
		if re.MatchString(str) {
			klog.V(6).Infof("%v matched %s", str, re)
			return true
		}
	}
	return false
}

func (f *FilterServer) accept(method, path, host string) bool {
	if matchesRegexp(path, f.RejectPaths) {
		return false
	}
	if matchesRegexp(method, f.RejectMethods) {
		return false
	}
	if matchesRegexp(path, f.AcceptPaths) && matchesRegexp(host, f.AcceptHosts) {
		return true
	}
	return false
}

// HandlerFor 对 f 做浅拷贝，该副本将请求
// 转交给新的 delegate。
func (f *FilterServer) HandlerFor(delegate http.Handler) *FilterServer {
	f2 := *f
	f2.delegate = delegate
	return &f2
}

// 从形如 "localhost" 或 "localhost:8080" 的 host header 值中提取 host
func extractHost(header string) (host string) {
	host, _, err := net.SplitHostPort(header)
	if err != nil {
		host = header
	}
	return host
}

func (f *FilterServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	host := extractHost(req.Host)
	if f.accept(req.Method, req.URL.Path, host) {
		klog.V(3).Infof("Filter accepting %v %v %v", req.Method, req.URL.Path, host)
		f.delegate.ServeHTTP(rw, req)
		return
	}
	klog.V(3).Infof("Filter rejecting %v %v %v", req.Method, req.URL.Path, host)
	http.Error(rw, http.StatusText(http.StatusForbidden), http.StatusForbidden)
}

// Server 是一个 http.Handler，将 Kubernetes API 代理到远程 API server。
type Server struct {
	handler http.Handler
}

type responder struct{}

func (r *responder) Error(w http.ResponseWriter, req *http.Request, err error) {
	klog.Errorf("Error while proxying request: %v", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// makeUpgradeTransport 创建一个 transport，对必须升级的代理连接
// 显式绕过 HTTP2 支持。
func makeUpgradeTransport(config *rest.Config, keepalive time.Duration) (proxy.UpgradeRequestRoundTripper, error) {
	transportConfig, err := config.TransportConfig()
	if err != nil {
		return nil, err
	}
	tlsConfig, err := transport.TLSConfigFor(transportConfig)
	if err != nil {
		return nil, err
	}
	rt := utilnet.SetOldTransportDefaults(&http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: keepalive,
		}).DialContext,
	})

	upgrader, err := transport.HTTPWrappersForConfig(transportConfig, proxy.MirrorRequest)
	if err != nil {
		return nil, err
	}
	return proxy.NewUpgradeRequestRoundTripper(rt, upgrader), nil
}

// NewServer 创建并安装一个新的 Server。
// 'filter' 若非 nil，仅保护发往 api 的请求。
func NewServer(filebase string, apiProxyPrefix string, staticPrefix string, filter *FilterServer, cfg *rest.Config, keepalive time.Duration, appendLocationPath bool) (*Server, error) {
	proxyHandler, err := NewProxyHandler(apiProxyPrefix, filter, cfg, keepalive, appendLocationPath)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle(apiProxyPrefix, proxyHandler)
	if filebase != "" {
		// 要求用户显式请求该行为，而不是
		// 默认就提供其工作目录的内容。
		mux.Handle(staticPrefix, newFileHandler(staticPrefix, filebase))
	}
	return &Server{handler: mux}, nil
}

// NewProxyHandler 为该集群创建 api 代理 handler
func NewProxyHandler(apiProxyPrefix string, filter *FilterServer, cfg *rest.Config, keepalive time.Duration, appendLocationPath bool) (http.Handler, error) {
	host := cfg.Host
	if !strings.HasSuffix(host, "/") {
		host = host + "/"
	}
	target, err := url.Parse(host)
	if err != nil {
		return nil, err
	}

	responder := &responder{}
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, err
	}
	upgradeTransport, err := makeUpgradeTransport(cfg, keepalive)
	if err != nil {
		return nil, err
	}
	proxy := proxy.NewUpgradeAwareHandler(target, transport, false, false, responder)
	proxy.UpgradeTransport = upgradeTransport
	proxy.UseRequestLocation = true
	proxy.UseLocationHost = true
	proxy.AppendLocationPath = appendLocationPath

	proxyServer := http.Handler(proxy)
	if filter != nil {
		proxyServer = filter.HandlerFor(proxyServer)
	}

	if !strings.HasPrefix(apiProxyPrefix, "/api") {
		proxyServer = stripLeaveSlash(apiProxyPrefix, proxyServer)
	}
	return proxyServer, nil
}

// Listen 是对 net.Listen 的简单封装。
func (s *Server) Listen(address string, port int) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
}

// ListenUnix 对 unix socket 执行 net.Listen
func (s *Server) ListenUnix(path string) (net.Listener, error) {
	// 移除任意 socket 文件（无论是否陈旧），但其他文件类型继续执行后续逻辑
	fi, err := os.Stat(path)
	if err == nil && (fi.Mode()&os.ModeSocket) != 0 {
		os.Remove(path)
	}
	// 默认 socket 仅用户可访问，调用方可按需在之后放宽权限
	oldmask, _ := util.Umask(0077)
	l, err := net.Listen("unix", path)
	util.Umask(oldmask)
	return l, err
}

// ServeOnListener 使用给定的 listener 启动 server，并持续循环运行。
func (s *Server) ServeOnListener(l net.Listener) error {
	server := http.Server{
		Handler: s.handler,
	}
	return server.Serve(l)
}

func newFileHandler(prefix, base string) http.Handler {
	return http.StripPrefix(prefix, http.FileServer(http.Dir(base)))
}

// 类似 http.StripPrefix，但始终保留开头的斜杠
// （以便我们的正则表达式能够正常工作）。
func stripLeaveSlash(prefix string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := strings.TrimPrefix(req.URL.Path, prefix)
		if len(p) >= len(req.URL.Path) {
			http.NotFound(w, req)
			return
		}
		if len(p) > 0 && p[:1] != "/" {
			p = "/" + p
		}
		req.URL.Path = p
		h.ServeHTTP(w, req)
	})
}
