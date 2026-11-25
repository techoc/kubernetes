//go:build windows

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

// Package app 负责 kube-proxy 的配置、启动与运行。
package app

import (
	"context"
	"errors"
	"fmt"
	"net"

	// 导入以注册 pprof HTTP 处理器。
	_ "net/http/pprof"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/winkernel"
)

// platformApplyDefaults 在解析命令行参数或读取配置后应用 Windows 平台默认值。
func (o *Options) platformApplyDefaults(config *kubeproxyconfig.KubeProxyConfiguration) {
	if config.Mode == "" {
		config.Mode = kubeproxyconfig.ProxyModeKernelspace
	}
	if config.Winkernel.RootHnsEndpointName == "" {
		config.Winkernel.RootHnsEndpointName = "cbr0"
	}
}

// platformSetup 在创建 Proxier 前补齐平台字段并完成初始化。
func (s *ProxyServer) platformSetup(ctx context.Context) error {
	// 保留旧版次要 IP 行为，维持 Windows 单栈配置的兼容性。
	if s.PrimaryIPFamily == v1.IPv4Protocol {
		s.NodeIPs[v1.IPv6Protocol] = net.IPv6zero
	} else {
		s.NodeIPs[v1.IPv4Protocol] = net.IPv4zero
	}
	return nil
}

// platformCheckSupported 在创建 Proxier 前检查 Windows 内核代理及 IP 协议族是否可用。
func (s *ProxyServer) platformCheckSupported(ctx context.Context) (ipv4Supported, ipv6Supported, dualStackSupported bool, err error) {
	// 先确认系统能够使用内核代理，再判断协议族能力。
	_, err = winkernel.CanUseWinKernelProxier(winkernel.WindowsKernelCompatTester{})
	if err != nil {
		return false, false, false, err
	}

	// IPv4 与 IPv6 单栈均受支持，双栈能力取决于系统与网络兼容性。
	ipv4Supported = true
	ipv6Supported = true

	compatTester := winkernel.DualStackCompatTester{}
	dualStackSupported = compatTester.DualStackCompatible(s.Config.Winkernel.NetworkName)

	return
}

// createProxier 按双栈能力创建 Windows 内核代理提供器。
// --init-only 在 Windows 上未实现，必须在创建前拒绝。
func (s *ProxyServer) createProxier(ctx context.Context, config *kubeproxyconfig.KubeProxyConfiguration, dualStackMode, initOnly bool) (proxy.Provider, error) {
	if initOnly {
		return nil, fmt.Errorf("--init-only is not implemented on Windows")
	}

	var proxier proxy.Provider
	var err error

	if dualStackMode {
		proxier, err = winkernel.NewDualStackProxier(
			config,
			s.NodeName,
			s.NodeIPs,
			s.Recorder,
			s.HealthzServer,
		)
	} else {
		proxier, err = winkernel.NewProxier(
			config,
			s.PrimaryIPFamily,
			s.NodeName,
			s.NodeIPs[s.PrimaryIPFamily],
			s.Recorder,
			s.HealthzServer,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to create proxier: %v", err)
	}

	return proxier, nil
}

// platformCleanup 清理可安全移除的旧代理规则。
// Windows 尚未实现 --cleanup-and-exit，显式请求时必须报错。
func platformCleanup(ctx context.Context, mode kubeproxyconfig.ProxyMode, cleanupAndExit bool) error {
	if cleanupAndExit {
		return errors.New("--cleanup-and-exit is not implemented on Windows")
	}
	return nil
}
