//go:build !windows && !linux

/*
Copyright 2024 The Kubernetes Authors.

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

// Package app 负责 kube-proxy 的配置、启动与运行。
package app

import (
	"context"
	"fmt"
	"runtime"

	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
)

// platformApplyDefaults 在解析命令行参数或读取配置后应用平台默认值；其他平台无需补充。
func (o *Options) platformApplyDefaults(config *kubeproxyconfig.KubeProxyConfiguration) {
}

var unsupportedError = fmt.Errorf(runtime.GOOS + "/" + runtime.GOARCH + "is unsupported")

// platformSetup 在创建 Proxier 前执行平台初始化；当前平台统一返回不支持错误。
func (s *ProxyServer) platformSetup(ctx context.Context) error {
	return unsupportedError
}

// platformCheckSupported 在创建 Proxier 前检查 IP 协议族支持；当前平台始终不可用。
func (s *ProxyServer) platformCheckSupported(ctx context.Context) (ipv4Supported, ipv6Supported, dualStackSupported bool, err error) {
	return false, false, false, unsupportedError
}

// createProxier 创建代理提供器；当前平台统一返回不支持错误。
func (s *ProxyServer) createProxier(ctx context.Context, config *kubeproxyconfig.KubeProxyConfiguration, dualStackMode, initOnly bool) (proxy.Provider, error) {
	return nil, unsupportedError
}

// platformCleanup 清理可安全移除的旧代理规则；当前平台统一返回不支持错误。
func platformCleanup(ctx context.Context, mode kubeproxyconfig.ProxyMode, cleanupAndExit bool) error {
	return unsupportedError
}
