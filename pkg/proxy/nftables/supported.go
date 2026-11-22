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

package nftables

import (
	"fmt"
	"os"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	utilkernel "k8s.io/kubernetes/pkg/util/kernel"
	"sigs.k8s.io/knftables"
)

// getNFTablesInterface 创建 knftables.Interface，并检查本机是否可以使用 nftables 代理模式。
func getNFTablesInterface(ipFamily v1.IPFamily) (knftables.Interface, error) {
	var nftablesFamily knftables.Family
	if ipFamily == v1.IPv4Protocol {
		nftablesFamily = knftables.IPv4Family
	} else {
		nftablesFamily = knftables.IPv6Family
	}

	var options []knftables.Option
	// Netlink 模式省去每次读写都 fork nft 进程的开销，由特性开关控制。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.NFTablesNetlink) {
		options = append(options, knftables.UseNetlink)
	}

	// 我们要求（确切地说是 knftables.New 要求）nft 版本至少为 1.0.1，
	// 因为更早的版本启动时会解析整份 nft 规则集，即使只想操作单张 table 也一样。
	// 这很危险：新版 nft 可能以旧版本无法解析的方式引入新规则类型，导致旧版崩溃。
	// 因此若 kube-proxy 使用 nft < 1.0.1，其他组件在完全无关的 table 里做的改动
	// 就可能让我们连自己的规则都无法访问。
	nft, err := knftables.New(nftablesFamily, kubeProxyTable, options...)
	if err != nil {
		return nil, err
	}

	// 同理，还要保证主机文件系统上的 nft >= 1.0.1，否则*我们*写的规则可能让
	// *系统自带*的 nft 崩溃。（已知 kube-proxy 使用 nft >= 1.0.3 而系统 nft <= 0.9.8
	// 时，系统的 nft 会完全不可用。）但主机文件系统里的 nft 版本难以获取，所以改为
	// 检查内核版本：假设发行版自带的 nft 与其内核支持的能力一致，那么内核 5.13+
	// 就意味着 nft 1.0.1+。https://issues.k8s.io/122743
	//
	// 用户可以把 KUBE_PROXY_NFTABLES_SKIP_KERNEL_VERSION_CHECK 设为任意非空值来跳过该检查。
	if os.Getenv("KUBE_PROXY_NFTABLES_SKIP_KERNEL_VERSION_CHECK") == "" {
		kernelVersion, err := utilkernel.GetVersion()
		if err != nil {
			return nil, fmt.Errorf("could not check kernel version: %w", err)
		}
		if kernelVersion.LessThan(version.MustParseGeneric(utilkernel.NFTablesKubeProxyKernelVersion)) {
			return nil, fmt.Errorf("kube-proxy in nftables mode requires kernel %s or later", utilkernel.NFTablesKubeProxyKernelVersion)
		}
	}

	return nft, nil
}
