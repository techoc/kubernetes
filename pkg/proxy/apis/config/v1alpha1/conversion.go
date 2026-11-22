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

package v1alpha1

import (
	"strings"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kube-proxy/config/v1alpha1"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
)

// Convert_config_KubeProxyConfiguration_To_v1alpha1_KubeProxyConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_config_KubeProxyConfiguration_To_v1alpha1_KubeProxyConfiguration(in *kubeproxyconfig.KubeProxyConfiguration, out *v1alpha1.KubeProxyConfiguration, scope conversion.Scope) error {
	// 先复用自动生成的字段级转换，再在此处理需要特殊映射的字段。
	if err := autoConvert_config_KubeProxyConfiguration_To_v1alpha1_KubeProxyConfiguration(in, out, scope); err != nil {
		return err
	}
	out.WindowsRunAsService = in.Windows.RunAsService
	out.Conntrack = v1alpha1.KubeProxyConntrackConfiguration(in.Linux.Conntrack)
	out.OOMScoreAdj = in.Linux.OOMScoreAdj
	// v1alpha1 把 MasqueradeAll 放在各后端配置里，而内部类型把它放在 Linux 配置里；
	// 因此需按代理模式写入对应后端，其他模式沿用 iptables 的位置。
	switch in.Mode {
	case kubeproxyconfig.ProxyModeNFTables:
		out.NFTables.MasqueradeAll = in.Linux.MasqueradeAll
	default:
		out.IPTables.MasqueradeAll = in.Linux.MasqueradeAll
	}

	// 同步周期在 v1alpha1 中同样按后端存放，内部类型则是顶层字段；
	// 按模式写入对应后端，避免不同后端共享同一字段时语义混淆。
	switch in.Mode {
	case kubeproxyconfig.ProxyModeIPVS:
		out.IPVS.SyncPeriod = in.SyncPeriod
		out.IPVS.MinSyncPeriod = in.MinSyncPeriod
	case kubeproxyconfig.ProxyModeNFTables:
		out.NFTables.SyncPeriod = in.SyncPeriod
		out.NFTables.MinSyncPeriod = in.MinSyncPeriod
	default:
		out.IPTables.SyncPeriod = in.SyncPeriod
		out.IPTables.MinSyncPeriod = in.MinSyncPeriod
	}

	// v1alpha1 用单个逗号分隔的字符串表示 ClusterCIDR，内部类型用列表；
	// 仅在列表非空时转换，避免产生空字符串覆盖默认值。
	if len(in.DetectLocal.ClusterCIDRs) > 0 {
		out.ClusterCIDR = strings.Join(in.DetectLocal.ClusterCIDRs, ",")
	}
	return nil
}

// Convert_v1alpha1_KubeProxyConfiguration_To_config_KubeProxyConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_v1alpha1_KubeProxyConfiguration_To_config_KubeProxyConfiguration(in *v1alpha1.KubeProxyConfiguration, out *kubeproxyconfig.KubeProxyConfiguration, scope conversion.Scope) error {
	// 自动生成的转换处理同名字段，此处只补充结构不同、需要手工映射的字段。
	if err := autoConvert_v1alpha1_KubeProxyConfiguration_To_config_KubeProxyConfiguration(in, out, scope); err != nil {
		return err
	}
	out.Windows.RunAsService = in.WindowsRunAsService
	out.Linux.Conntrack = kubeproxyconfig.KubeProxyConntrackConfiguration(in.Conntrack)
	out.Linux.OOMScoreAdj = in.OOMScoreAdj
	// 反向转换时需从当前模式对应的后端配置中取回 MasqueradeAll，
	// 合并到内部类型唯一的 Linux.MasqueradeAll 字段中。
	switch kubeproxyconfig.ProxyMode(in.Mode) {
	case kubeproxyconfig.ProxyModeNFTables:
		out.Linux.MasqueradeAll = in.NFTables.MasqueradeAll
	default:
		out.Linux.MasqueradeAll = in.IPTables.MasqueradeAll
	}

	// 同理，从当前模式对应的后端配置中恢复顶层的同步周期字段。
	switch kubeproxyconfig.ProxyMode(in.Mode) {
	case kubeproxyconfig.ProxyModeIPVS:
		out.SyncPeriod = in.IPVS.SyncPeriod
		out.MinSyncPeriod = in.IPVS.MinSyncPeriod
	case kubeproxyconfig.ProxyModeNFTables:
		out.SyncPeriod = in.NFTables.SyncPeriod
		out.MinSyncPeriod = in.NFTables.MinSyncPeriod
	default:
		out.SyncPeriod = in.IPTables.SyncPeriod
		out.MinSyncPeriod = in.IPTables.MinSyncPeriod
	}

	// 仅当 v1alpha1 的 ClusterCIDR 非空时才拆分，保持内部列表为零值。
	if len(in.ClusterCIDR) > 0 {
		out.DetectLocal.ClusterCIDRs = strings.Split(in.ClusterCIDR, ",")
	}
	return nil
}

// Convert_v1alpha1_KubeProxyIPTablesConfiguration_To_config_KubeProxyIPTablesConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_v1alpha1_KubeProxyIPTablesConfiguration_To_config_KubeProxyIPTablesConfiguration(in *v1alpha1.KubeProxyIPTablesConfiguration, out *kubeproxyconfig.KubeProxyIPTablesConfiguration, scope conversion.Scope) error {
	// 该类型的字段布局与内部类型一致，直接委托给自动生成的转换即可。
	return autoConvert_v1alpha1_KubeProxyIPTablesConfiguration_To_config_KubeProxyIPTablesConfiguration(in, out, scope)
}

// Convert_v1alpha1_KubeProxyIPVSConfiguration_To_config_KubeProxyIPVSConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_v1alpha1_KubeProxyIPVSConfiguration_To_config_KubeProxyIPVSConfiguration(in *v1alpha1.KubeProxyIPVSConfiguration, out *kubeproxyconfig.KubeProxyIPVSConfiguration, scope conversion.Scope) error {
	// 该类型的字段布局与内部类型一致，直接委托给自动生成的转换即可。
	return autoConvert_v1alpha1_KubeProxyIPVSConfiguration_To_config_KubeProxyIPVSConfiguration(in, out, scope)
}

// Convert_v1alpha1_KubeProxyNFTablesConfiguration_To_config_KubeProxyNFTablesConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_v1alpha1_KubeProxyNFTablesConfiguration_To_config_KubeProxyNFTablesConfiguration(in *v1alpha1.KubeProxyNFTablesConfiguration, out *kubeproxyconfig.KubeProxyNFTablesConfiguration, scope conversion.Scope) error {
	// 该类型的字段布局与内部类型一致，直接委托给自动生成的转换即可。
	return autoConvert_v1alpha1_KubeProxyNFTablesConfiguration_To_config_KubeProxyNFTablesConfiguration(in, out, scope)
}

// Convert_config_DetectLocalConfiguration_To_v1alpha1_DetectLocalConfiguration 在此手动定义：
// 由于存在告警，公开转换函数并未自动生成。
func Convert_config_DetectLocalConfiguration_To_v1alpha1_DetectLocalConfiguration(in *kubeproxyconfig.DetectLocalConfiguration, out *v1alpha1.DetectLocalConfiguration, s conversion.Scope) error {
	// 该类型的字段布局与 v1alpha1 一致，直接委托给自动生成的转换即可。
	return autoConvert_config_DetectLocalConfiguration_To_v1alpha1_DetectLocalConfiguration(in, out, s)
}
