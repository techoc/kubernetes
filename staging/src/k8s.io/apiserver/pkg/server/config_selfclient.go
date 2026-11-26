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

package server

import (
	"fmt"
	"net"

	restclient "k8s.io/client-go/rest"
	netutils "k8s.io/utils/net"
)

// LoopbackClientServerNameOverride 由 loopback 客户端传给 apiserver，
// 以便使用 TLS 时通过 SNI 选择 loopback 证书。
//
// 该名字同时用于两个目的：SNI 选择 loopback 证书，以及客户端校验证书
// 中的 DNS SAN；两者必须一致，否则握手失败。
const LoopbackClientServerNameOverride = "apiserver-loopback-client"

// NewClientConfig 构造指向本 apiserver 自身的 REST 配置。
// 返回 nil 表示未启用 TLS（此时调用方不应创建回环客户端）。
func (s *SecureServingInfo) NewClientConfig(caCert []byte) (*restclient.Config, error) {
	if s == nil || (s.Cert == nil && len(s.SNICerts) == 0) {
		return nil, nil
	}

	host, port, err := LoopbackHostPort(s.Listener.Addr().String())
	if err != nil {
		return nil, err
	}

	return &restclient.Config{
		// 不限制回环客户端的 QPS：回环客户端代表 apiserver 自身内部调用，
		// 限流会造成自死锁式的排队，因此显式关闭。
		QPS:  -1,
		Host: "https://" + net.JoinHostPort(host, port),
		TLSClientConfig: restclient.TLSClientConfig{
			CAData: caCert,
		},
	}, nil
}

// NewLoopbackClientConfig 在 NewClientConfig 基础上加入 bearer token 和
// SNI ServerName，用于以特权身份访问自身 API。
func (s *SecureServingInfo) NewLoopbackClientConfig(token string, loopbackCert []byte) (*restclient.Config, error) {
	c, err := s.NewClientConfig(loopbackCert)
	if err != nil || c == nil {
		return c, err
	}

	c.BearerToken = token
	// 覆盖 ServerName 以通过 SNI 选择 loopback 证书。客户端也用
	// 该名称校验服务端返回的证书。
	c.TLSClientConfig.ServerName = LoopbackClientServerNameOverride

	return c, nil
}

// LoopbackHostPort 返回 loopback REST 客户端连接服务器时
// 应使用的主机与端口。
func LoopbackHostPort(bindAddress string) (string, string, error) {
	host, port, err := net.SplitHostPort(bindAddress)
	if err != nil {
		// 不应发生
		return "", "", fmt.Errorf("invalid server bind address: %q", bindAddress)
	}

	isIPv6 := netutils.IsIPv6String(host)

	// 期望的值是具体 IP 或 DNS 名，而不是 "0.0.0.0"：
	// 通配地址不能直接用于连接，必须替换为具体回环地址。
	if host == "0.0.0.0" || host == "::" {
		// 获取本地接口 IP，失败则回退到 "localhost"。
		// 注意 Go 标准库会先用外部域名服务器解析 "localhost"。
		// 因此若 localhost.<yoursearchdomain> 可解析，就得不到期望的 127.0.0.1。
		host = getLoopbackAddress(isIPv6)
	}
	return host, port, nil
}

// getLoopbackAddress 返回本地 loopback 接口的 IP 地址。发生错误或找不到 loopback 接口时，回退到 "localhost"
//
// 优先返回字面 IP 而非 "localhost"：某些环境的 DNS 搜索域会把
// localhost.<domain> 解析到非回环地址，导致回环客户端连错地方。
func getLoopbackAddress(wantIPv6 bool) string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addrs {
			if ipnet, ok := address.(*net.IPNet); ok && ipnet.IP.IsLoopback() && wantIPv6 == netutils.IsIPv6(ipnet.IP) {
				return ipnet.IP.String()
			}
		}
	}
	return "localhost"
}
