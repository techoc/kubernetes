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

package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/third_party/forked/golang/netutil"
	"k8s.io/klog/v2"
)

// DialURL 使用传入 RoundTripper 持有的底层 dialer 连接指定 URL，主要用于代理可升级连接。
// 因此 URL scheme 为 https 时，此方法优先协商 HTTP/1.1。
// 如果希望 ALPN 协商 HTTP/2，请在 http.Transport 的 TLSConfig 中设置 NextProto=[]string{"http2"}。
func DialURL(ctx context.Context, url *url.URL, transport http.RoundTripper) (net.Conn, error) {
	//CanonicalAddr 按传输器自身的拨号规则处理默认端口和 IPv6 方括号。
	dialAddr := netutil.CanonicalAddr(url)

	//此错误被有意视为非致命；无法解包传输器时，
	//下面会回退到标准 dialer。
	dialer, err := utilnet.DialerFor(transport)
	if err != nil {
		klog.FromContext(ctx).V(5).Info("Unable to unwrap transport to get dialer", "type", fmt.Sprintf("%T", transport), "err", err)
	}

	switch url.Scheme {
	case "http":
		if dialer != nil {
			return dialer(ctx, "tcp", dialAddr)
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", dialAddr)
	case "https":
		//如果识别出传输器类型，则获取其 TLS 配置。
		tlsConfig, err := utilnet.TLSClientConfig(transport)
		if err != nil {
			//没有配置时普通 tls.Dialer 仍可工作，
			//但无法应用自定义验证设置。
			klog.FromContext(ctx).V(5).Info("Unable to unwrap transport to get at TLS config", "type", fmt.Sprintf("%T", transport), "err", err)
		}

		if dialer != nil {
			//已有 dialer；先建立连接，
			//再基于该连接创建 TLS 客户端。
			netConn, err := dialer(ctx, "tcp", dialAddr)
			if err != nil {
				return nil, err
			}
			if tlsConfig == nil {
				//tls.Client 要求 config 非 nil。
				klog.FromContext(ctx).Info("Warning: using custom dialer with no TLSClientConfig, defaulting to InsecureSkipVerify")
				//tls.Handshake() 要求 ServerName 或 InsecureSkipVerify。
				//自定义 dialer 可能将连接隧道转发到其他主机，
				//无法可靠推断 ServerName，因此显式跳过验证。
				tlsConfig = &tls.Config{
					InsecureSkipVerify: true,
				}
			} else if len(tlsConfig.ServerName) == 0 && !tlsConfig.InsecureSkipVerify {
				//tls.HandshakeContext() 要求 ServerName 或 InsecureSkipVerify。
				//从要连接的主机名推断 ServerName。
				inferredHost := dialAddr
				if host, _, err := net.SplitHostPort(dialAddr); err == nil {
					inferredHost = host
				}
				//创建副本，避免修改调用方提供的配置。
				tlsConfigCopy := tlsConfig.Clone()
				tlsConfigCopy.ServerName = inferredHost
				tlsConfig = tlsConfigCopy
			}

			//此方法主要用于 "Connection: Upgrade" 调用，因此假定调用方将发送 HTTP/1.1 请求。
			//TLSConfig.NextProtos 不应允许 HTTP/2，
			//因此显式设置；仅当 TLSConfig 支持 HTTP/1.1 时才执行此检查。
			if supportsHTTP11(tlsConfig.NextProtos) {
				tlsConfig = tlsConfig.Clone()
				tlsConfig.NextProtos = []string{"http/1.1"}
			}

			tlsConn := tls.Client(netConn, tlsConfig)
			//握手失败时关闭原始连接，避免文件描述符泄漏。
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				netConn.Close()
				return nil, err
			}
			return tlsConn, nil
		} else {
			// Dial。
			tlsDialer := tls.Dialer{
				Config: tlsConfig,
			}
			return tlsDialer.DialContext(ctx, "tcp", dialAddr)
		}
	default:
		return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
	}
}

// supportsHTTP11 判断是否可以协商 HTTP/1.1。NextProtos 为空表示客户端未限制 ALPN，
// 其中包含 HTTP/1.1。
func supportsHTTP11(nextProtos []string) bool {
	if len(nextProtos) == 0 {
		return true
	}
	for _, proto := range nextProtos {
		if proto == "http/1.1" {
			return true
		}
	}
	return false
}
