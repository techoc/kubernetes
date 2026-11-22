//go:build linux

/*
Copyright 2017 The Kubernetes Authors.

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

package ipvs

import (
	"context"
	"os/exec"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	utilipset "k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
)

// cleanupIptablesLeftovers 移除 ipvs Proxier 创建的所有 iptables 规则和链。
// 遇到错误时返回 true；错误会记录到日志中。
func cleanupIptablesLeftovers(ctx context.Context, ipt utiliptables.Interface) (encounteredError bool) {
	logger := klog.FromContext(ctx)
	// 先删除跳转规则，解除 ipvs Proxier 创建的自定义链与其他链的引用关系。
	for _, jc := range iptablesJumpChain {
		args := []string{
			"-m", "comment", "--comment", jc.comment,
			"-j", string(jc.to),
		}
		if err := ipt.DeleteRule(jc.table, jc.from, args...); err != nil {
			if !utiliptables.IsNotFoundError(err) {
				logger.Error(err, "Error removing iptables rules in ipvs proxier")
				encounteredError = true
			}
		}
	}

	// 先清空再删除自定义链：清空所有链可以同时移除链之间的全部引用。
	for _, ch := range iptablesCleanupChains {
		if err := ipt.FlushChain(ch.table, ch.chain); err != nil {
			if !utiliptables.IsNotFoundError(err) {
				logger.Error(err, "Error removing iptables rules in ipvs proxier")
				encounteredError = true
			}
		}
	}

	// 删除我们创建的所有链。
	for _, ch := range iptablesCleanupChains {
		if err := ipt.DeleteChain(ch.table, ch.chain); err != nil {
			if !utiliptables.IsNotFoundError(err) {
				logger.Error(err, "Error removing iptables rules in ipvs proxier")
				encounteredError = true
			}
		}
	}

	return encounteredError
}

// CleanupLeftovers 清理 ipvs Proxier 创建的所有 ipvs 和 iptables 规则。
func CleanupLeftovers(ctx context.Context) (encounteredError bool) {
	// libipvs.New() 在 "ip_vs" 内核模块（或 "modprobe" 命令）不可用时
	// 会打印错误日志。确实要运行 ipvs proxier 时多打一条错误无妨，
	// 但这里只是尽力而为的清理（例如启动 nftables proxier 时），
	// 多余的错误日志会让人困惑，因此先做与 libipvs 相同的检查，
	// 失败时直接返回，不再调用 libipvs。
	if _, err := exec.Command("modprobe", "-va", "ip_vs").CombinedOutput(); err != nil {
		return false
	}

	ipts := utiliptables.NewBestEffort()
	ipsetInterface := utilipset.New()
	ipvsInterface := utilipvs.New()

	return cleanupLeftovers(ctx, ipvsInterface, ipts, ipsetInterface)
}

// cleanupLeftovers 借助传入的 ipvs、iptables、ipset 接口执行尽力而为的清理，
// 返回清理过程中是否遇到错误；接口由调用方注入，便于测试替换。
func cleanupLeftovers(ctx context.Context, ipvs utilipvs.Interface, ipts map[v1.IPFamily]utiliptables.Interface, ipset utilipset.Interface) (encounteredError bool) {
	logger := klog.FromContext(ctx)
	// 清空所有 ipvs 规则。
	if ipvs != nil {
		err := ipvs.Flush()
		if err != nil {
			logger.Error(err, "Error flushing ipvs rules")
			encounteredError = true
		}
	}
	// 删除 ipvs Proxier 创建的 dummy 虚拟设备。
	nl := NewNetLinkHandle(false)
	err := nl.DeleteDummyDevice(defaultDummyDevice)
	if err != nil {
		logger.Error(err, "Error deleting dummy device created by ipvs proxier", "device", defaultDummyDevice)
		encounteredError = true
	}

	// 清理 ipvs Proxier 创建的 iptables 规则。
	for _, ipt := range ipts {
		encounteredError = cleanupIptablesLeftovers(ctx, ipt) || encounteredError
	}

	// 销毁 ipvs Proxier 创建的 ipset。必须在清理 iptables 之后调用，
	// 因为仍被 iptables 引用的 ipset 无法删除。
	if _, err := ipset.GetVersion(); err == nil {
		for _, set := range ipsetInfo {
			err = ipset.DestroySet(set.name)
			if err != nil {
				if !utilipset.IsNotFoundError(err) {
					logger.Error(err, "Error removing ipset", "ipset", set.name)
					encounteredError = true
				}
			}
		}
	}

	return encounteredError
}
