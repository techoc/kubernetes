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

package rest

import (
	"crypto/tls"
	"errors"
	"net/http"

	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/plugin/pkg/client/auth/exec"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/transport"
)

// HTTPClientFor 返回按 Config 提供认证与传输层安全的 http.Client；无需特殊行为时返回 http.DefaultClient。
//
// HTTPClientFor 返回使用 Config 中认证/TLS 设置的 http.Client。
// 无需特殊行为时返回 http.DefaultClient，以复用其连接池。
func HTTPClientFor(config *Config) (*http.Client, error) {
	transport, err := TransportFor(config)
	if err != nil {
		return nil, err
	}
	var httpClient *http.Client
	// 只有确实需要自定义 transport 或超时时才新建 client，
	// 否则复用 http.DefaultClient 以共享全局连接池。
	if transport != http.DefaultTransport || config.Timeout > 0 {
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
		}
	} else {
		httpClient = http.DefaultClient
	}

	return httpClient, nil
}

// TLSConfigFor 根据 Config 构造提供传输层安全的 tls.Config；未要求时返回 nil。
//
// TLSConfigFor 根据 Config 构造 tls.Config；未要求传输层安全时返回 nil。
func TLSConfigFor(config *Config) (*tls.Config, error) {
	cfg, err := config.TransportConfig()
	if err != nil {
		return nil, err
	}
	return transport.TLSConfigFor(cfg)
}

// TransportFor 返回按 Config 提供认证与传输层安全的 http.RoundTripper；无需特殊行为时返回 http.DefaultTransport。
//
// TransportFor 返回带有认证/TLS 等中间件的 RoundTripper。
func TransportFor(config *Config) (http.RoundTripper, error) {
	cfg, err := config.TransportConfig()
	if err != nil {
		return nil, err
	}
	return transport.New(cfg)
}

// HTTPWrappersForConfig 给 RoundTripper 套上配置中的各层行为，供需要劫持底层连接但仍使用
// HTTP 语义的客户端（如 WebSocket/HTTP2）使用；纯 HTTP 客户端应使用 TransportFor 或 RESTClientFor。
//
// HTTPWrappersForConfig 只给已有 RoundTripper 套上配置中的中间件，
// 供需要劫持底层连接（如 WebSocket/HTTP2）的客户端使用；普通 HTTP 客户端应使用 TransportFor。
func HTTPWrappersForConfig(config *Config, rt http.RoundTripper) (http.RoundTripper, error) {
	cfg, err := config.TransportConfig()
	if err != nil {
		return nil, err
	}
	return transport.HTTPWrappersForConfig(cfg, rt)
}

// TransportConfig 把客户端配置转换为相应的 transport 配置。
//
// TransportConfig 把 client-go 的 Config 转换成更底层的 transport.Config。
func (c *Config) TransportConfig() (*transport.Config, error) {
	metrics.EnsureRegistered()
	conf := &transport.Config{
		UserAgent:          c.UserAgent,
		Transport:          c.Transport,
		WrapTransport:      c.WrapTransport,
		DisableCompression: c.DisableCompression,
		TLS: transport.TLSConfig{
			Insecure:   c.Insecure,
			ServerName: c.ServerName,
			CAFile:     c.CAFile,
			CAData:     c.CAData,
			CertFile:   c.CertFile,
			CertData:   c.CertData,
			KeyFile:    c.KeyFile,
			KeyData:    c.KeyData,
			NextProtos: c.NextProtos,
		},
		Username:        c.Username,
		Password:        c.Password,
		BearerToken:     c.BearerToken,
		BearerTokenFile: c.BearerTokenFile,
		Impersonate: transport.ImpersonationConfig{
			UserName: c.Impersonate.UserName,
			UID:      c.Impersonate.UID,
			Groups:   c.Impersonate.Groups,
			Extra:    c.Impersonate.Extra,
		},
		Proxy: c.Proxy,
	}

	if c.Dial != nil {
		conf.DialHolder = &transport.DialHolder{Dial: c.Dial}
	}

	// exec 插件与旧式 auth provider 互斥，同时设置会导致行为不确定。
	if c.ExecProvider != nil && c.AuthProvider != nil {
		return nil, errors.New("execProvider and authProvider cannot be used in combination")
	}

	if c.ExecProvider != nil {
		var cluster *clientauthentication.Cluster
		// 只有显式要求时才把集群信息传给 exec 插件（涉及敏感数据）。
		if c.ExecProvider.ProvideClusterInfo {
			var err error
			cluster, err = ConfigToExecCluster(c)
			if err != nil {
				return nil, err
			}
		}
		provider, err := exec.GetAuthenticator(c.ExecProvider, cluster)
		if err != nil {
			return nil, err
		}
		if err := provider.UpdateTransportConfig(conf); err != nil {
			return nil, err
		}
	}
	if c.AuthProvider != nil {
		provider, err := GetAuthProvider(c.Host, c.AuthProvider, c.AuthConfigPersister)
		if err != nil {
			return nil, err
		}
		conf.Wrap(provider.WrapTransport)
	}
	return conf, nil
}

// Wrap 追加一个 transport 中间件，在首次 API 调用前包装底层 http.RoundTripper。
// 新中间件位于已有包装之外，因此最先被调用。
//
// Wrap 追加一个 transport 中间件；新中间件在已有包装之外，即最先被调用。
func (c *Config) Wrap(fn transport.WrapperFunc) {
	c.WrapTransport = transport.Wrappers(c.WrapTransport, fn)
}
