//go:build linux

/*
Copyright 2023 The Kubernetes Authors.

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

package conntrack

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/proxy/metrics"
)

// Kubernetes UDP 服务可能受到过期 conntrack 条目的影响：
// 这些条目可能指向已不存在的端点，导致丢包和连接问题。

// CleanStaleEntries 扫描 conntrack 表，清理那些服务对应的、
// 与当前 serving 端点不符的条目。它先列出现有的 conntrack 条目，
// 再根据当前的 Service 和 Endpoints 计算期望的 conntrack 状态。
func CleanStaleEntries(ct Interface, ipFamily v1.IPFamily,
	svcPortMap proxy.ServicePortMap, endpointsMap proxy.EndpointsMap) {

	// 记录耗时用于观测指标，也便于故障排查时评估清理开销。
	start := time.Now()
	klog.V(4).InfoS("Started to reconcile conntrack entries", "ipFamily", ipFamily)

	// 遍历可能返回部分结果：EINTR 表示列举被信号中断，
	// 此时仍用已拿到的部分结果继续清理，避免遗留过期条目。
	entries, err := ct.ListEntries(ipFamilyMap[ipFamily])
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			klog.V(2).ErrorS(err, "received a partial result, continuing to clean with partial result")
		} else {
			klog.ErrorS(err, "Failed to list conntrack entries")
			return
		}
	}

	// serviceIPEndpoints 将服务 IP（ClusterIP、LoadBalancerIP、ExternalIP）与服务端口
	// 映射到 serving 端点（端点 IP 和端口）的集合。
	serviceIPEndpoints := make(map[string]sets.Set[string])
	// serviceNodePortEndpoints 将服务的 NodePort 映射到 serving 端点（端点 IP 和端口）的集合。
	serviceNodePortEndpoints := make(map[int]sets.Set[string])

	for svcName, svc := range svcPortMap {
		// TCP 有连接状态可感知端点变化，只有 UDP 需要靠 conntrack 清理。
		if svc.Protocol() != v1.ProtocolUDP {
			continue
		}

		endpoints := sets.New[string]()
		for _, endpoint := range endpointsMap[svcName] {
			// 需要清理服务（IP 或 NodePort）对应的所有未指向 serving 端点的 conntrack 条目：
			// 收集所有 serving 端点，并清除那些目标是该服务、但 DNAT 后不指向这些端点的条目。
			// 服务的配置变化不应影响已有连接，因此这里不考虑流量策略、拓扑或端点的
			// terminating 状态，以保证 UDP 服务的行为与 TCP 服务一致。
			if endpoint.IsServing() {
				portStr := strconv.Itoa(int(endpoint.Port()))
				endpoints.Insert(net.JoinHostPort(endpoint.IP(), portStr))
			}
		}

		// 注意：即使服务没有任何 serving 端点，也会以空端点集合参与处理，
		// 从而清理指向其前端的所有已有条目。无端点服务安装的 REJECT（iptables）/
		// reject（nftables）规则无法覆盖此前已建立的连接：这些连接在匹配服务 IP 的
		// reject 规则生效之前就已被 DNAT 到（已删除的）端点 IP。单向 UDP 流
		// （如 statsd）每个包都会刷新 conntrack 超时，若不清理就会一直向已删除的
		// 端点 IP 发送流量。

		// 需要筛出目标是服务 IP:Port 前端、但其后端不属于 serving 端点的条目。
		portStr := strconv.Itoa(svc.Port())
		// ClusterIP:Port 前端。
		serviceIPEndpoints[net.JoinHostPort(svc.ClusterIP().String(), portStr)] = endpoints
		// LoadBalancerIP:Port 前端。
		for _, loadBalancerIP := range svc.LoadBalancerVIPs() {
			serviceIPEndpoints[net.JoinHostPort(loadBalancerIP.String(), portStr)] = endpoints
		}
		// ExternalIP:Port 前端。
		for _, externalIP := range svc.ExternalIPs() {
			serviceIPEndpoints[net.JoinHostPort(externalIP.String(), portStr)] = endpoints
		}
		// 需要筛出目标是 *:NodePort、但后端不属于 serving 端点的条目。
		// NodePort 条目只按目的端口匹配，若端点集合为空，则会删除该端口上的
		// 所有 UDP 流，包括不属于 kube-proxy 的流量（例如访问同端口上无关主机的
		// 流量）。因此在能够把匹配范围限定到节点 IP 之前，对无 serving 端点的
		// 服务跳过 NodePort 清理。
		if svc.NodePort() != 0 && endpoints.Len() > 0 {
			// *:NodePort 前端。
			serviceNodePortEndpoints[svc.NodePort()] = endpoints
		}
	}

	var flows []*netlink.ConntrackFlow
	for _, entry := range entries {
		// 只处理 UDP 协议的 conntrack 条目。
		if entry.Forward.Protocol != unix.IPPROTO_UDP {
			continue
		}

		origDst := entry.Forward.DstIP.String()   // 匹配服务 IP
		origPortDst := int(entry.Forward.DstPort) // 匹配服务端口
		origPortDstStr := strconv.Itoa(origPortDst)
		replySrc := entry.Reverse.SrcIP.String()   // 匹配 serving 端点 IP
		replyPortSrc := int(entry.Reverse.SrcPort) // 匹配 serving 端点端口
		replyPortSrcStr := strconv.Itoa(replyPortSrc)

		// 若条目的原始目的地址（--orig-dst）是服务 IP（ClusterIP、LoadBalancerIP
		// 或 ExternalIP）、原始目的端口（--orig-port-dst）是服务端口，而响应源 IP
		// （--reply-src）和端口（--reply-port-src）不属于该服务的 serving 端点，
		// 则清理该条目。
		endpoints, ok := serviceIPEndpoints[net.JoinHostPort(origDst, origPortDstStr)]
		if ok && !endpoints.Has(net.JoinHostPort(replySrc, replyPortSrcStr)) {
			flows = append(flows, entry)
			continue
		}

		// 若条目的原始目的端口（--orig-port-dst）是服务 NodePort，而响应源 IP
		// （--reply-src）和端口（--reply-port-src）不属于该服务的 serving 端点，
		// 则清理该条目。
		endpoints, ok = serviceNodePortEndpoints[origPortDst]
		if ok && !endpoints.Has(net.JoinHostPort(replySrc, replyPortSrcStr)) {
			flows = append(flows, entry)
			continue
		}
	}

	var n int
	if n, err = ct.DeleteEntries(ipFamilyMap[ipFamily], flows); err != nil {
		klog.ErrorS(err, "Failed to clear all conntrack entries", "ipFamily", ipFamily, "entriesDeleted", n, "took", time.Since(start))
	} else {
		klog.V(4).InfoS("Finished reconciling conntrack entries", "ipFamily", ipFamily, "entriesDeleted", n, "took", time.Since(start))
	}
	metrics.ReconcileConntrackFlowsLatency.WithLabelValues(string(ipFamily)).Observe(metrics.SinceInSeconds(start))
	metrics.ReconcileConntrackFlowsDeletedEntriesTotal.WithLabelValues(string(ipFamily)).Add(float64(n))
}

// ipFamilyMap 将 v1.IPFamily 映射到对应的 unix 地址族常量。
var ipFamilyMap = map[v1.IPFamily]uint8{
	v1.IPv4Protocol: unix.AF_INET,
	v1.IPv6Protocol: unix.AF_INET6,
}
