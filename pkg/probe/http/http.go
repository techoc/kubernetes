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

// Package http 实现 HTTP GET 探针，用于检查容器是否可以通过 HTTP 请求响应。
//
// HTTP 探针向容器的指定 URL 发送 GET 请求，根据响应状态码判断健康状态：
//   - 200-399：成功（Success），其中 3xx 重定向返回 Warning
//   - 400+：失败（Failure）
//   - 连接失败或超时：失败（Failure）
//
// 支持 HTTP/1.1、HTTP/2（通过 TLS ALPN 协商）和 h2c（HTTP/2 cleartext）。
// 禁用 TLS 证书验证、Keep-Alive、压缩和系统代理，确保探针行为可预测。
package http

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/kubernetes/pkg/probe"

	"k8s.io/klog/v2"
	utilio "k8s.io/utils/io"
)

const (
	// maxRespBodyLength 是探针响应体读取的最大长度（10KB）
	// 限制响应体大小以防止内存占用过大，同时保留足够的调试信息
	maxRespBodyLength = 10 * 1 << 10 // 10KB
)

// New 创建一个跳过 TLS 证书验证的 HTTP 探针。
// followNonLocalRedirects 控制是否跟随跨域重定向：
//   - true：跟随重定向到不同主机名
//   - false：遇到跨域重定向时返回 Warning，而非失败
func New(followNonLocalRedirects bool) Prober {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	return NewWithTLSConfig(tlsConfig, followNonLocalRedirects)
}

// NewWithTLSConfig 使用自定义 TLS 配置创建 HTTP 探针。
// followNonLocalRedirects 控制是否跟随跨域重定向。
func NewWithTLSConfig(config *tls.Config, followNonLocalRedirects bool) Prober {
	// 探针不使用节点的本地代理设置，避免请求被意外转发
	transport := utilnet.SetTransportDefaults(
		&http.Transport{
			TLSClientConfig:    config,
			DisableKeepAlives:  true,               // 每次探针都是独立请求，不复用连接
			Proxy:              http.ProxyURL(nil), // 禁用系统代理
			DisableCompression: true,               // 禁用压缩，移除 Accept-Encoding 头，简化响应处理
			// DialContext 创建未加密的 TCP 连接，也用于 HTTPS 连接的底层拨号
			// 使用 ProbeDialer 减少 TIME-WAIT 状态的 socket 残留
			DialContext: probe.ProbeDialer().DialContext,
		})

	// HTTP 版本行为说明：
	// - HTTPS 探针：通过 TLS 握手时的 ALPN 协议自动协商 HTTP/2
	// - HTTP 探针：仅使用 HTTP/1.1（明文没有升级机制）
	// - h2cTransport：启用 HTTP/2 cleartext（h2c with prior knowledge），
	//   在没有 TLS 的情况下直接使用 HTTP/2，这是 H2CContainerProbe 功能的基础
	h2cTransport := &http.Transport{
		DisableKeepAlives:  true,
		Proxy:              http.ProxyURL(nil),
		DisableCompression: true,
		DialContext:        probe.ProbeDialer().DialContext,
	}
	h2cTransport.Protocols = new(http.Protocols)
	h2cTransport.Protocols.SetUnencryptedHTTP2(true)

	return httpProber{transport, h2cTransport, followNonLocalRedirects}
}

// Prober 是 HTTP 探针接口，定义 HTTP 就绪/存活检查的方法。
// 支持标准 HTTP/1.1 和 h2c（HTTP/2 cleartext）两种模式。
type Prober interface {
	// Probe 执行 HTTP/1.1 探针检查
	Probe(req *http.Request, timeout time.Duration) (probe.Result, string, error)
	// ProbeH2C 执行 HTTP/2 cleartext（h2c）探针检查
	ProbeH2C(req *http.Request, timeout time.Duration) (probe.Result, string, error)
}

type httpProber struct {
	transport               *http.Transport // 标准 HTTP/HTTPS 传输层，支持 TLS
	h2cTransport            *http.Transport // h2c 传输层，无加密的 HTTP/2
	followNonLocalRedirects bool            // 是否跟随跨域重定向
}

// Probe 执行 HTTP/1.1 探针检查。
// 创建带有超时和重定向检查的 HTTP 客户端，调用 DoHTTPProbe 执行实际请求。
func (pr httpProber) Probe(req *http.Request, timeout time.Duration) (probe.Result, string, error) {
	client := &http.Client{
		Timeout:       timeout,
		Transport:     pr.transport,
		CheckRedirect: RedirectChecker(pr.followNonLocalRedirects),
	}
	return DoHTTPProbe(req, client)
}

// ProbeH2C 执行 HTTP/2 cleartext（h2c）探针检查。
// 与 Probe 类似，但使用 h2cTransport 而非标准 transport，
// 允许在没有 TLS 的情况下直接使用 HTTP/2 协议。
func (pr httpProber) ProbeH2C(req *http.Request, timeout time.Duration) (probe.Result, string, error) {
	client := &http.Client{
		Timeout:       timeout,
		Transport:     pr.h2cTransport,
		CheckRedirect: RedirectChecker(pr.followNonLocalRedirects),
	}
	return DoHTTPProbe(req, client)
}

// GetHTTPInterface 是执行 HTTP 请求的接口，返回响应和错误。
// 抽象 HTTP 客户端以便测试时注入 mock 实现。
type GetHTTPInterface interface {
	Do(req *http.Request) (*http.Response, error)
}

// DoHTTPProbe 检查对指定 URL 的 GET 请求是否成功。
//
// 状态码判断规则：
//   - 200 <= code < 400：成功（Success），其中 3xx 重定向返回 Warning
//   - code >= 400：失败（Failure）
//   - HTTP 通信失败（连接拒绝、超时等）：失败（Failure）
//
// 导出此函数以便其他包直接执行 HTTP 探针。
func DoHTTPProbe(req *http.Request, client GetHTTPInterface) (probe.Result, string, error) {
	url := req.URL
	headers := req.Header
	res, err := client.Do(req)
	if err != nil {
		// 将错误转换为 Failure，包括超时在内的所有通信错误都视为探针失败
		return probe.Failure, err.Error(), nil
	}
	defer res.Body.Close()
	// 限制读取响应体大小，最多 10KB，防止内存占用过大
	b, err := utilio.ReadAtMost(res.Body, maxRespBodyLength)
	if err != nil {
		if err == utilio.ErrLimitReached {
			// 响应体超过 10KB 时截断，非致命错误，记录日志后继续
			klog.V(4).Infof("Non fatal body truncation for %s, Response: %v", url.String(), *res)
		} else {
			// 读取响应体时出现其他错误，返回 Unknown
			return probe.Failure, "", err
		}
	}
	body := string(b)
	// 200-399 视为成功范围
	if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusBadRequest {
		if res.StatusCode >= http.StatusMultipleChoices { // 3xx 重定向
			// 重定向返回 Warning 而非 Failure，因为应用可能配置了重定向但未完成
			klog.V(4).Infof("Probe terminated redirects for %s, Response: %v", url.String(), *res)
			return probe.Warning, fmt.Sprintf("Probe terminated redirects, Response body: %v", body), nil
		}
		klog.V(4).Infof("Probe succeeded for %s, Response: %v", url.String(), *res)
		return probe.Success, body, nil
	}
	// 400+ 状态码视为失败
	klog.V(4).Infof("Probe failed for %s with request headers %v, response body: %v", url.String(), headers, body)
	// 注意：在 https://issue.k8s.io/99425 解决之前，面向用户的失败消息不能包含响应体
	failureMsg := fmt.Sprintf("HTTP probe failed with statuscode: %d", res.StatusCode)
	return probe.Failure, failureMsg, nil
}

// RedirectChecker 返回一个 HTTP 重定向检查函数，用于 http.Client 的 CheckRedirect 字段。
//
// 当 followNonLocalRedirects 为 false 时（默认行为）：
//   - 如果重定向目标与原始请求主机名不同，阻止重定向并返回最后一个响应
//   - 重定向超过 10 次时停止，防止无限循环
//
// 当 followNonLocalRedirects 为 true 时，返回 nil，使用 http.Client 的默认重定向行为。
func RedirectChecker(followNonLocalRedirects bool) func(*http.Request, []*http.Request) error {
	if followNonLocalRedirects {
		return nil // 使用 http.Client 的默认重定向检查器
	}

	return func(req *http.Request, via []*http.Request) error {
		// 检查重定向是否跨域：比较当前请求与初始请求的主机名
		if req.URL.Hostname() != via[0].URL.Hostname() {
			// 跨域重定向时，使用最后一个响应（不跟随重定向）
			// 这会让 DoHTTPProbe 收到 3xx 状态码，返回 Warning
			return http.ErrUseLastResponse
		}
		// 防止无限重定向循环，最多跟随 10 次
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
}
