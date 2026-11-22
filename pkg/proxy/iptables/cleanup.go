//go:build linux

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

package iptables

import (
	"bytes"
	"context"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
)

// CleanupLeftovers 移除 Proxier 创建的所有 iptables 规则和链。
// 遇到错误时返回 true；错误会被记录到日志。
func CleanupLeftovers(ctx context.Context) (encounteredError bool) {
	ipts := utiliptables.NewBestEffort()
	// 对所有协议族（IPv4/IPv6）都执行清理，且用 || 确保即使某一族失败，
	// 其余族仍会被处理，同时错误状态不会被遗漏。
	for _, ipt := range ipts {
		encounteredError = cleanupLeftoversForFamily(ctx, ipt) || encounteredError
	}
	return
}

// cleanupLeftoversForFamily 清理单个 iptables 协议族中由 kube-proxy 遗留的规则和链。
// 它会先解绑跳转规则，再清空并删除 nat/filter 表中的自定义链，
// 最后删除 mangle 表中的 canary 链；遇到错误时返回 true。
func cleanupLeftoversForFamily(ctx context.Context, ipt utiliptables.Interface) (encounteredError bool) {
	logger := klog.FromContext(ctx)
	// 先断开内置链到 kube-proxy 自定义链的跳转，否则后续清空这些自定义链时
	// 仍会有流量命中它们。清理专用链（cleanupOnly）也在此一并解绑。
	for _, jump := range append(iptablesJumpChains, iptablesCleanupOnlyChains...) {
		args := append([]string{}, jump.extraArgs...)
		args = append(args,
			"-m", "comment", "--comment", jump.comment,
			"-j", string(jump.dstChain),
		)
		if err := ipt.DeleteRule(jump.table, jump.srcChain, args...); err != nil {
			// 规则不存在说明已经清理过，视为正常；只有其他错误才算失败。
			if !utiliptables.IsNotFoundError(err) {
				logger.Error(err, "Error removing pure-iptables proxy rule")
				encounteredError = true
			}
		}
	}

	// 清空并删除 nat 表中所有由 kube-proxy 创建的链。
	iptablesData := bytes.NewBuffer(nil)
	if err := ipt.SaveInto(utiliptables.TableNAT, iptablesData); err != nil {
		logger.Error(err, "Failed to execute iptables-save", "table", utiliptables.TableNAT)
		encounteredError = true
	} else {
		existingNATChains := utiliptables.GetChainsFromTable(iptablesData.Bytes())
		natChains := proxyutil.NewLineBuffer()
		natRules := proxyutil.NewLineBuffer()
		natChains.Write("*nat")
		// 先处理已知的固定链（kube-services、kube-nodeports 等）。
		for _, chain := range []utiliptables.Chain{kubeServicesChain, kubeNodePortsChain, kubePostroutingChain, kubeMarkMasqChain, kubeProxyCanaryChain} {
			if existingNATChains.Has(chain) {
				chainString := string(chain)
				natChains.Write(utiliptables.MakeChainLine(chain)) // flush
				natRules.Write("-X", chainString)                  // delete
			}
		}
		// 再按命名规则搜索并删除每个服务和端点对应的动态链，
		// 这些链的数量和名称在运行时才会确定。
		for chain := range existingNATChains {
			chainString := string(chain)
			if isServiceChainName(chainString) {
				natChains.Write(utiliptables.MakeChainLine(chain)) // flush
				natRules.Write("-X", chainString)                  // delete
			}
		}
		natRules.Write("COMMIT")
		natLines := append(natChains.Bytes(), natRules.Bytes()...)
		// 通过 iptables-restore 原子地提交清空与删除操作；NoFlushTables 保证
		// 只改动本次生成的链，不会波及同一表中的其他规则。
		err = ipt.Restore(utiliptables.TableNAT, natLines, utiliptables.NoFlushTables, utiliptables.RestoreCounters)
		if err != nil {
			logger.Error(err, "Failed to execute iptables-restore", "table", utiliptables.TableNAT)
			metrics.IPTablesRestoreFailuresTotal.WithLabelValues(string(ipt.Protocol())).Inc()
			encounteredError = true
		}
	}

	// 清空并删除 filter 表中所有由 kube-proxy 创建的链。
	iptablesData.Reset()
	if err := ipt.SaveInto(utiliptables.TableFilter, iptablesData); err != nil {
		logger.Error(err, "Failed to execute iptables-save", "table", utiliptables.TableFilter)
		encounteredError = true
	} else {
		existingFilterChains := utiliptables.GetChainsFromTable(iptablesData.Bytes())
		filterChains := proxyutil.NewLineBuffer()
		filterRules := proxyutil.NewLineBuffer()
		filterChains.Write("*filter")
		for _, chain := range []utiliptables.Chain{kubeServicesChain, kubeExternalServicesChain, kubeForwardChain, kubeNodePortsChain, kubeProxyFirewallChain, kubeProxyCanaryChain} {
			if existingFilterChains.Has(chain) {
				chainString := string(chain)
				filterChains.Write(utiliptables.MakeChainLine(chain))
				filterRules.Write("-X", chainString)
			}
		}
		filterRules.Write("COMMIT")
		filterLines := append(filterChains.Bytes(), filterRules.Bytes()...)
		// 与 nat 表同理，用 restore 原子提交，且不清空整张表。
		if err := ipt.Restore(utiliptables.TableFilter, filterLines, utiliptables.NoFlushTables, utiliptables.RestoreCounters); err != nil {
			logger.Error(err, "Failed to execute iptables-restore", "table", utiliptables.TableFilter)
			metrics.IPTablesRestoreFailuresTotal.WithLabelValues(string(ipt.Protocol())).Inc()
			encounteredError = true
		}
	}

	// 删除 mangle 表中的 canary 链；该链本就可能不存在，因此忽略错误。
	_ = ipt.DeleteChain(utiliptables.TableMangle, kubeProxyCanaryChain)

	return encounteredError
}
