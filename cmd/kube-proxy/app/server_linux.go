//go:build linux

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
	"os"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/iptables"
	"k8s.io/kubernetes/pkg/proxy/ipvs"
	utilipset "k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	"k8s.io/kubernetes/pkg/proxy/nftables"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
)

var defaultedMode bool

// platformApplyDefaults 在解析命令行参数或读取配置后应用 Linux 平台默认值。
func (o *Options) platformApplyDefaults(config *kubeproxyconfig.KubeProxyConfiguration) {
	if config.Mode == "" {
		config.Mode = kubeproxyconfig.ProxyModeIPTables
		defaultedMode = true
	}

	if config.Mode == kubeproxyconfig.ProxyModeNFTables && len(config.NodePortAddresses) == 0 {
		config.NodePortAddresses = []string{kubeproxyconfig.NodePortAddressesPrimary}
	}

	if config.DetectLocalMode == "" {
		o.logger.V(4).Info("Defaulting detect-local-mode", "localModeClusterCIDR", string(kubeproxyconfig.LocalModeClusterCIDR))
		config.DetectLocalMode = kubeproxyconfig.LocalModeClusterCIDR
	}
	o.logger.V(2).Info("DetectLocalMode", "localMode", string(config.DetectLocalMode))
}

// platformSetup 在创建 Proxier 前补齐平台字段并应用 conntrack 系统参数。
func (s *ProxyServer) platformSetup(ctx context.Context) error {
	if err := conntrack.SetSysctls(ctx, &s.Config.Linux.Conntrack); err != nil {
		return fmt.Errorf("could not set conntrack parameters from kube-proxy configuration: %w", err)
	}

	return nil
}

// isIPTablesBased 判断代理模式是否依赖 iptables，而非使用 nftables。
func isIPTablesBased(mode kubeproxyconfig.ProxyMode) bool {
	return mode == kubeproxyconfig.ProxyModeIPTables || mode == kubeproxyconfig.ProxyModeIPVS
}

// platformCheckSupported 在创建 Proxier 前检查 IPv4、IPv6 与双栈支持，并拒绝不可用配置。
func (s *ProxyServer) platformCheckSupported(ctx context.Context) (ipv4Supported, ipv6Supported, dualStackSupported bool, err error) {
	logger := klog.FromContext(ctx)

	if isIPTablesBased(s.Config.Mode) {
		// 分别探测 iptables 与 ip6tables，避免仅安装其中一个时误判协议族能力。
		errv4 := utiliptables.New(utiliptables.ProtocolIPv4).Present()
		errv6 := utiliptables.New(utiliptables.ProtocolIPv6).Present()

		ipv4Supported = errv4 == nil
		ipv6Supported = errv6 == nil

		if !ipv4Supported && !ipv6Supported {
			// 两个错误通常源于同一原因（未安装或内核模块不可用），合并反而会掩盖主因。
			err = fmt.Errorf("iptables is not available on this host : %w", errv4)
		} else if !ipv4Supported {
			logger.Info("No iptables support for family", "ipFamily", v1.IPv4Protocol, "error", errv4)
		} else if !ipv6Supported {
			logger.Info("No iptables support for family", "ipFamily", v1.IPv6Protocol, "error", errv6)
		}
	} else {
		// nftables 命令行工具本身同时支持 IPv4 与 IPv6。
		ipv4Supported, ipv6Supported = true, true
	}

	// 通过 IPv6 接口表确认内核是否启用 IPv6。
	_, errIPv6 := os.Stat("/proc/net/if_inet6")
	if errIPv6 == nil {
		// 接口表存在仍不代表可用，继续检查内核禁用开关。
		content, err := os.ReadFile("/proc/sys/net/ipv6/conf/all/disable_ipv6")
		if err == nil && strings.TrimSpace(string(content)) == "1" {
			logger.Info("Kernel support disabled for family", "ipFamily", v1.IPv6Protocol)
			ipv6Supported = false
		}
	} else {
		logger.Info("No kernel support for family", "ipFamily", v1.IPv6Protocol)
		ipv6Supported = false
	}

	// Linux 代理同时支持 IPv4 与 IPv6 时即可启用双栈。
	dualStackSupported = ipv4Supported && ipv6Supported
	return
}

// createProxier 按配置模式创建 iptables、ipvs 或 nftables 代理提供器。
// 部分构造函数会修改系统规则，即使最终创建失败也可能留下副作用。
func (s *ProxyServer) createProxier(ctx context.Context, config *kubeproxyconfig.KubeProxyConfiguration, dualStack, initOnly bool) (proxy.Provider, error) {
	logger := klog.FromContext(ctx)
	var proxier proxy.Provider
	var err error

	localDetectors := getLocalDetectors(logger, s.PrimaryIPFamily, config, s.podCIDRs)

	if config.Mode == kubeproxyconfig.ProxyModeIPTables {
		if defaultedMode {
			message := "No kube-proxy mode specified: defaulting to 'iptables'. Note that in Kubernetes 1.40 the default mode will switch to 'nftables'. If you wish to continue using 'iptables' mode, you must explicitly specify 'mode: iptables' in the kube-proxy config file, or '--proxy-mode iptables' on the kube-proxy command line."
			logger.Error(nil, message)
			s.Recorder.Eventf(s.NodeRef, nil, v1.EventTypeWarning, "KubeProxyDefaultMode", "StartKubeProxy", message)
		}
		logger.Info("Using iptables Proxier")
		ipts := utiliptables.NewBestEffort()

		if dualStack {
			// TODO: 此处的系统副作用只应发生在 Run() 被调用时。
			proxier, err = iptables.NewDualStackProxier(
				ctx,
				config,
				ipts,
				utilsysctl.New(),
				localDetectors,
				s.NodeName,
				s.NodeIPs,
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		} else {
			// 节点不支持双栈时只创建单栈代理，避免依赖缺失的 iptables 协议族支持。

			// TODO: 此处的系统副作用只应发生在 Run() 被调用时。
			proxier, err = iptables.NewProxier(
				ctx,
				config,
				s.PrimaryIPFamily,
				ipts[s.PrimaryIPFamily],
				utilsysctl.New(),
				localDetectors[s.PrimaryIPFamily],
				s.NodeName,
				s.NodeIPs[s.PrimaryIPFamily],
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		}

		if err != nil {
			return nil, fmt.Errorf("unable to create proxier: %v", err)
		}
	} else if config.Mode == kubeproxyconfig.ProxyModeIPVS {
		ipsetInterface := utilipset.New()
		ipvsInterface := utilipvs.New()
		if err := ipvs.CanUseIPVSProxier(ctx, ipvsInterface, ipsetInterface, config.IPVS.Scheduler); err != nil {
			return nil, fmt.Errorf("can't use the IPVS proxier: %v", err)
		}
		ipts := utiliptables.NewBestEffort()

		logger.Info("Using ipvs Proxier")
		message := "The ipvs proxier has been deprecated and will be disabled by default in Kubernetes 1.40 and removed in Kubernetes 1.43. Migrate to the 'nftables' proxier instead."
		logger.Error(nil, message)
		s.Recorder.Eventf(s.NodeRef, nil, v1.EventTypeWarning, "IPVSDeprecation", "StartKubeProxy", message)
		if dualStack {
			proxier, err = ipvs.NewDualStackProxier(
				ctx,
				config,
				ipts,
				ipvsInterface,
				ipsetInterface,
				utilsysctl.New(),
				localDetectors,
				s.NodeName,
				s.NodeIPs,
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		} else {
			proxier, err = ipvs.NewProxier(
				ctx,
				config,
				s.PrimaryIPFamily,
				ipts[s.PrimaryIPFamily],
				ipvsInterface,
				ipsetInterface,
				utilsysctl.New(),
				localDetectors[s.PrimaryIPFamily],
				s.NodeName,
				s.NodeIPs[s.PrimaryIPFamily],
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		}
		if err != nil {
			return nil, fmt.Errorf("unable to create proxier: %v", err)
		}
	} else if config.Mode == kubeproxyconfig.ProxyModeNFTables {
		logger.Info("Using nftables Proxier")

		if dualStack {
			// TODO: 此处的系统副作用只应发生在 Run() 被调用时。
			proxier, err = nftables.NewDualStackProxier(
				ctx,
				config,
				localDetectors,
				s.NodeName,
				s.NodeIPs,
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		} else {
			// 节点不支持双栈时只创建单栈代理。
			// TODO: 此处的系统副作用只应发生在 Run() 被调用时。
			proxier, err = nftables.NewProxier(
				ctx,
				config,
				s.PrimaryIPFamily,
				localDetectors[s.PrimaryIPFamily],
				s.NodeName,
				s.NodeIPs[s.PrimaryIPFamily],
				s.Recorder,
				s.HealthzServer,
				initOnly,
			)
		}

		if err != nil {
			return nil, fmt.Errorf("unable to create proxier: %v", err)
		}
	}

	return proxier, nil
}

func getLocalDetectors(logger klog.Logger, primaryIPFamily v1.IPFamily, config *kubeproxyconfig.KubeProxyConfiguration, nodePodCIDRs []string) map[v1.IPFamily]proxyutil.LocalTrafficDetector {
	localDetectors := map[v1.IPFamily]proxyutil.LocalTrafficDetector{
		v1.IPv4Protocol: proxyutil.NewNoOpLocalDetector(),
		v1.IPv6Protocol: proxyutil.NewNoOpLocalDetector(),
	}

	switch config.DetectLocalMode {
	case kubeproxyconfig.LocalModeClusterCIDR:
		for family, cidrs := range proxyutil.MapCIDRsByIPFamily(config.DetectLocal.ClusterCIDRs) {
			localDetectors[family] = proxyutil.NewDetectLocalByCIDR(cidrs[0].String())
		}
		if !localDetectors[primaryIPFamily].IsImplemented() {
			logger.Info("Detect-local-mode set to ClusterCIDR, but no cluster CIDR specified for primary IP family", "ipFamily", primaryIPFamily, "clusterCIDRs", config.DetectLocal.ClusterCIDRs)
		}

	case kubeproxyconfig.LocalModeNodeCIDR:
		for family, cidrs := range proxyutil.MapCIDRsByIPFamily(nodePodCIDRs) {
			localDetectors[family] = proxyutil.NewDetectLocalByCIDR(cidrs[0].String())
		}
		if !localDetectors[primaryIPFamily].IsImplemented() {
			logger.Info("Detect-local-mode set to NodeCIDR, but no PodCIDR defined at node for primary IP family", "ipFamily", primaryIPFamily, "podCIDRs", nodePodCIDRs)
		}

	case kubeproxyconfig.LocalModeBridgeInterface:
		localDetector := proxyutil.NewDetectLocalByBridgeInterface(config.DetectLocal.BridgeInterface)
		localDetectors[v1.IPv4Protocol] = localDetector
		localDetectors[v1.IPv6Protocol] = localDetector

	case kubeproxyconfig.LocalModeInterfaceNamePrefix:
		localDetector := proxyutil.NewDetectLocalByInterfaceNamePrefix(config.DetectLocal.InterfaceNamePrefix)
		localDetectors[v1.IPv4Protocol] = localDetector
		localDetectors[v1.IPv6Protocol] = localDetector

	default:
		logger.Info("Defaulting to no-op detect-local")
	}

	return localDetectors
}

// platformCleanup 清理可安全移除的 kube-proxy 旧规则。
// cleanupAndExit 为 true 时尝试清理所有已知模式；否则只清理当前模式确定不使用的规则。
func platformCleanup(ctx context.Context, mode kubeproxyconfig.ProxyMode, cleanupAndExit bool) error {
	var encounteredError bool

	// 切换到 nftables 或显式清理时，移除 iptables 与 ipvs 旧规则。
	if !isIPTablesBased(mode) || cleanupAndExit {
		encounteredError = iptables.CleanupLeftovers(ctx) || encounteredError
		encounteredError = ipvs.CleanupLeftovers(ctx) || encounteredError
	}

	// 切换到 iptables/ipvs 或显式清理时，移除 nftables 旧规则。
	if isIPTablesBased(mode) || cleanupAndExit {
		encounteredError = nftables.CleanupLeftovers(ctx) || encounteredError
	}

	if encounteredError {
		return errors.New("encountered an error while tearing down rules")
	}
	return nil
}
