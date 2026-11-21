/*
Copyright 2016 The Kubernetes Authors.

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

package sysctl

import (
	"context"
	goruntime "runtime"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/klog/v2"
	utilkernel "k8s.io/kubernetes/pkg/util/kernel"
)

// sysctl 描述一个安全 sysctl 及其可用所需的最低内核版本。
type sysctl struct {
	// sysctl 的名称。
	name string
	// 该 sysctl 可用的最低内核版本；为空表示无版本要求。
	kernel string
}

// safeSysctls 是默认允许的安全 sysctl 列表。
var safeSysctls = []sysctl{
	{
		name: "kernel.shm_rmid_forced",
	}, {
		name: "net.ipv4.ip_local_port_range",
	}, {
		name: "net.ipv4.tcp_syncookies",
	}, {
		name: "net.ipv4.ping_group_range",
	}, {
		name: "net.ipv4.ip_unprivileged_port_start",
	}, {
		name:   "net.ipv4.ip_local_reserved_ports",
		kernel: utilkernel.IPLocalReservedPortsNamespacedKernelVersion,
	}, {
		name:   "net.ipv4.tcp_keepalive_time",
		kernel: utilkernel.TCPKeepAliveTimeNamespacedKernelVersion,
	}, {
		name:   "net.ipv4.tcp_fin_timeout",
		kernel: utilkernel.TCPFinTimeoutNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_keepalive_intvl",
		kernel: utilkernel.TCPKeepAliveIntervalNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_keepalive_probes",
		kernel: utilkernel.TCPKeepAliveProbesNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_rmem",
		kernel: utilkernel.TCPReceiveMemoryNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_wmem",
		kernel: utilkernel.TCPTransmitMemoryNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_slow_start_after_idle",
		kernel: utilkernel.TCPSlowStartAfterIdleNamespacedKernelVersion,
	},
	{
		name:   "net.ipv4.tcp_notsent_lowat",
		kernel: utilkernel.TCPNotSentLowatNamespacedKernelVersion,
	},
}

// SafeSysctlAllowlist 返回安全 sysctl 及安全 sysctl 模式（以 * 结尾）的允许列表。
//
// 满足以下条件的 sysctl 才被视为安全：
// - 在容器或 Pod 内具有命名空间隔离；
// - 与其他 Pod 隔离，即不影响同一节点上的其他 Pod。
func SafeSysctlAllowlist(ctx context.Context) []string {
	// 非 Linux 平台没有 sysctl 概念。
	if goruntime.GOOS != "linux" {
		return nil
	}

	return getSafeSysctlAllowlist(ctx, utilkernel.GetVersion)
}

// getSafeSysctlAllowlist 按内核版本过滤安全 sysctl 列表。
func getSafeSysctlAllowlist(ctx context.Context, getVersion func() (*version.Version, error)) []string {
	logger := klog.FromContext(ctx)
	kernelVersion, err := getVersion()
	if err != nil {
		logger.Error(err, "failed to get kernel version, unable to determine which sysctls are available")
	}

	var safeSysctlAllowlist []string
	// 无内核版本要求的条目直接加入；有要求的需满足最低版本。
	for _, sc := range safeSysctls {
		if sc.kernel == "" {
			safeSysctlAllowlist = append(safeSysctlAllowlist, sc.name)
			continue
		}

		if kernelVersion != nil && kernelVersion.AtLeast(version.MustParseGeneric(sc.kernel)) {
			safeSysctlAllowlist = append(safeSysctlAllowlist, sc.name)
		} else {
			// 内核版本过低时丢弃该条目并记录日志。
			logger.Info("kernel version is too old, dropping the sysctl from safe sysctl list", "kernelVersion", kernelVersion, "sysctl", sc.name)
		}
	}
	return safeSysctlAllowlist
}
