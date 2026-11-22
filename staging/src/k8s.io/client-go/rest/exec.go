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

package rest

import (
	"fmt"
	"net/http"
	"net/url"

	clientauthenticationapi "k8s.io/client-go/pkg/apis/clientauthentication"
)

// This file contains Config logic related to exec credential plugins.
//
// 本文件在 rest.Config 与 clientauthentication 的 Cluster 类型之间转换，
// 供 exec 凭证插件在启动子进程前获知集群的 TLS、代理等连接参数，
// 同时避免把客户端认证信息（token、证书等）泄露给插件。

// ConfigToExecCluster creates a clientauthenticationapi.Cluster with the corresponding fields from
// the provided Config.
//
// 只传递连接相关的字段，故意不包含任何认证字段：插件只负责产出凭证，
// 不应看到调用方已有的凭证。
func ConfigToExecCluster(config *Config) (*clientauthenticationapi.Cluster, error) {
	caData, err := dataFromSliceOrFile(config.CAData, config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA bundle for execProvider: %v", err)
	}

	var proxyURL string
	if config.Proxy != nil {
		// Proxy 是一个函数，这里用一个对应 Host 的请求去求值，以便把结果
		// 以字符串形式传给插件。
		req, err := http.NewRequest("", config.Host, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create proxy URL request for execProvider: %w", err)
		}
		url, err := config.Proxy(req)
		if err != nil {
			return nil, fmt.Errorf("failed to get proxy URL for execProvider: %w", err)
		}
		if url != nil {
			proxyURL = url.String()
		}
	}

	return &clientauthenticationapi.Cluster{
		Server:                   config.Host,
		TLSServerName:            config.ServerName,
		InsecureSkipTLSVerify:    config.Insecure,
		CertificateAuthorityData: caData,
		ProxyURL:                 proxyURL,
		DisableCompression:       config.DisableCompression,
		Config:                   config.ExecProvider.Config,
	}, nil
}

// ExecClusterToConfig creates a Config with the corresponding fields from the provided
// clientauthenticationapi.Cluster. The returned Config will be anonymous (i.e., it will not have
// any authentication-related fields set).
//
// 主要用于插件生态中把插件声明的集群信息转换回 Config；由于结果不含认证
// 信息，调用方通常还需要自行补充凭证。
func ExecClusterToConfig(cluster *clientauthenticationapi.Cluster) (*Config, error) {
	var proxy func(*http.Request) (*url.URL, error)
	if cluster.ProxyURL != "" {
		proxyURL, err := url.Parse(cluster.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("cannot parse proxy URL: %w", err)
		}
		proxy = http.ProxyURL(proxyURL)
	}

	return &Config{
		Host: cluster.Server,
		TLSClientConfig: TLSClientConfig{
			Insecure:   cluster.InsecureSkipTLSVerify,
			ServerName: cluster.TLSServerName,
			CAData:     cluster.CertificateAuthorityData,
		},
		Proxy:              proxy,
		DisableCompression: cluster.DisableCompression,
	}, nil
}
