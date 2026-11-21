//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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

package kubelet

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
)

const (
	// KubeIPTablesHintChain 该链存在于 iptables-legacy 还是 iptables-nft 中，
	// 可用来判断系统正在使用哪个版本的 iptables。
	KubeIPTablesHintChain utiliptables.Chain = "KUBE-IPTABLES-HINT"

	// KubeFirewallChain 保存 Kubernetes 防火墙规则。
	KubeFirewallChain utiliptables.Chain = "KUBE-FIREWALL"
)

// initNetworkUtil 初始化 kubelet 需要的 iptables 规则并启动变更监控。
func (kl *Kubelet) initNetworkUtil(logger klog.Logger) {
	iptClients := utiliptables.NewBestEffort()
	if len(iptClients) == 0 {
		// 这里不按错误记录：kubelet 自身并不需要这些规则（它们是为其他组件准备的），
		// 而且在仅支持 nftables 的宿主上失败是预期行为
		//（此时也不会有其他组件依赖 iptables 规则）。
		logger.Info("No iptables support on this system; not creating the KUBE-IPTABLES-HINT chain")
		return
	}

	// 分别处理每个 IP 族的 iptables 客户端。
	for family := range iptClients {
		iptClient := iptClients[family]
		// 同步规则成功后才启动监控协程。
		if kl.syncIPTablesRules(logger, iptClient) {
			logger.Info("Initialized iptables rules.", "protocol", iptClient.Protocol())
			// 监控链变化，检测到变更时重新同步规则。
			go iptClient.Monitor(
				utiliptables.Chain("KUBE-KUBELET-CANARY"),
				[]utiliptables.Table{utiliptables.TableMangle, utiliptables.TableNAT, utiliptables.TableFilter},
				func() { kl.syncIPTablesRules(logger, iptClient) },
				1*time.Minute, wait.NeverStop,
			)
		} else {
			logger.Info("Failed to initialize iptables rules; some functionality may be missing.", "protocol", iptClient.Protocol())
		}
	}
}

// syncIPTablesRules 确保 KUBE-IPTABLES-HINT 链存在，并安装火星包保护规则。
func (kl *Kubelet) syncIPTablesRules(logger klog.Logger, iptClient utiliptables.Interface) bool {
	// 创建提示链，让其他组件能判断我们使用的是 iptables-legacy 还是 iptables-nft。
	if _, err := iptClient.EnsureChain(utiliptables.TableMangle, KubeIPTablesHintChain); err != nil {
		logger.Error(err, "Failed to ensure that iptables hint chain exists")
		return false
	}

	if !iptClient.IsIPv6() { // IPv6 不存在该问题
		// 创建 KUBE-FIREWALL 链并安装火星包保护规则（见下文）。

		// 注意：kube-proxy（iptables 模式）会创建完全相同的规则副本。
		// 以后修改该规则时，必须保证与 kube-proxy 创建的旧版本规则能够正确互操作。

		if _, err := iptClient.EnsureChain(utiliptables.TableFilter, KubeFirewallChain); err != nil {
			logger.Error(err, "Failed to ensure that filter table KUBE-FIREWALL chain exists")
			return false
		}

		if _, err := iptClient.EnsureRule(utiliptables.Prepend, utiliptables.TableFilter, utiliptables.ChainOutput, "-j", string(KubeFirewallChain)); err != nil {
			logger.Error(err, "Failed to ensure that OUTPUT chain jumps to KUBE-FIREWALL")
			return false
		}
		if _, err := iptClient.EnsureRule(utiliptables.Prepend, utiliptables.TableFilter, utiliptables.ChainInput, "-j", string(KubeFirewallChain)); err != nil {
			logger.Error(err, "Failed to ensure that INPUT chain jumps to KUBE-FIREWALL")
			return false
		}

		// kube-proxy 通过 route_localnet 支持 localhost 上的 NodePort，
		// 这会造成安全漏洞（https://issue.k8s.io/90259），本 iptables 规则用于缓解该问题。
		// 该规则本应属于 kube-proxy，却误加到了 kubelet；
		// 目前仍保留在 kubelet 中，以防其他第三方组件依赖它。
		if _, err := iptClient.EnsureRule(utiliptables.Append, utiliptables.TableFilter, KubeFirewallChain,
			"-m", "comment", "--comment", "block incoming localnet connections",
			"--dst", "127.0.0.0/8",
			"!", "--src", "127.0.0.0/8",
			"-m", "conntrack",
			"!", "--ctstate", "RELATED,ESTABLISHED,DNAT",
			"-j", "DROP"); err != nil {
			logger.Error(err, "Failed to ensure rule to drop invalid localhost packets in filter table KUBE-FIREWALL chain")
			return false
		}
	}

	return true
}
