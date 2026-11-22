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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/klog/v2"
	utilipset "k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
)

const (
	// MinIPSetCheckVersion 是我们要求的最低 ipset 版本；IPv6 从 ipset 6.x 起才受支持。
	MinIPSetCheckVersion = "6.0"

	kubeLoopBackIPSetComment = "Kubernetes endpoints dst ip:port, source ip for solving hairpin purpose"
	kubeLoopBackIPSet        = "KUBE-LOOP-BACK"

	kubeClusterIPSetComment = "Kubernetes service cluster ip + port for masquerade purpose"
	kubeClusterIPSet        = "KUBE-CLUSTER-IP"

	kubeExternalIPSetComment = "Kubernetes service external ip + port for masquerade and filter purpose"
	kubeExternalIPSet        = "KUBE-EXTERNAL-IP"

	kubeExternalIPLocalSetComment = "Kubernetes service external ip + port with externalTrafficPolicy=local"
	kubeExternalIPLocalSet        = "KUBE-EXTERNAL-IP-LOCAL"

	kubeLoadBalancerSetComment = "Kubernetes service lb portal"
	kubeLoadBalancerSet        = "KUBE-LOAD-BALANCER"

	kubeLoadBalancerLocalSetComment = "Kubernetes service load balancer ip + port with externalTrafficPolicy=local"
	kubeLoadBalancerLocalSet        = "KUBE-LOAD-BALANCER-LOCAL"

	kubeLoadBalancerFWSetComment = "Kubernetes service load balancer ip + port for load balancer with sourceRange"
	kubeLoadBalancerFWSet        = "KUBE-LOAD-BALANCER-FW"

	kubeLoadBalancerSourceIPSetComment = "Kubernetes service load balancer ip + port + source IP for packet filter purpose"
	kubeLoadBalancerSourceIPSet        = "KUBE-LOAD-BALANCER-SOURCE-IP"

	kubeLoadBalancerSourceCIDRSetComment = "Kubernetes service load balancer ip + port + source cidr for packet filter purpose"
	kubeLoadBalancerSourceCIDRSet        = "KUBE-LOAD-BALANCER-SOURCE-CIDR"

	kubeNodePortSetTCPComment = "Kubernetes nodeport TCP port for masquerade purpose"
	kubeNodePortSetTCP        = "KUBE-NODE-PORT-TCP"

	kubeNodePortLocalSetTCPComment = "Kubernetes nodeport TCP port with externalTrafficPolicy=local"
	kubeNodePortLocalSetTCP        = "KUBE-NODE-PORT-LOCAL-TCP"

	kubeNodePortSetUDPComment = "Kubernetes nodeport UDP port for masquerade purpose"
	kubeNodePortSetUDP        = "KUBE-NODE-PORT-UDP"

	kubeNodePortLocalSetUDPComment = "Kubernetes nodeport UDP port with externalTrafficPolicy=local"
	kubeNodePortLocalSetUDP        = "KUBE-NODE-PORT-LOCAL-UDP"

	kubeNodePortSetSCTPComment = "Kubernetes nodeport SCTP port for masquerade purpose with type 'hash ip:port'"
	kubeNodePortSetSCTP        = "KUBE-NODE-PORT-SCTP-HASH"

	kubeNodePortLocalSetSCTPComment = "Kubernetes nodeport SCTP port with externalTrafficPolicy=local with type 'hash ip:port'"
	kubeNodePortLocalSetSCTP        = "KUBE-NODE-PORT-LOCAL-SCTP-HASH"

	kubeHealthCheckNodePortSetComment = "Kubernetes health check node port"
	kubeHealthCheckNodePortSet        = "KUBE-HEALTH-CHECK-NODE-PORT"

	kubeIPVSSetComment = "Addresses on the ipvs interface"
	kubeIPVSSet        = "KUBE-IPVS-IPS"
)

// IPSetVersioner 可以查询当前 ipset 的版本。
type IPSetVersioner interface {
	// GetVersion 以 "X.Y" 形式返回版本号。
	GetVersion() (string, error)
}

// IPSet 对 IPVS proxier 使用的 util/ipset 做了封装。
type IPSet struct {
	utilipset.IPSet
	// activeEntries 是该 ipset 当前期望的活跃条目集合。
	activeEntries sets.Set[string]
	// handle 是调用 util ipset 的句柄。
	handle utilipset.Interface
}

// NewIPSet 初始化一个新的 IPSet 结构。
// 双栈环境下 IPv4 与 IPv6 集合共存，IPv6 集合改用 "KUBE-6-" 前缀保证名称唯一；
// 因为 ipset 名称最长 31 个字符，改名后还需截断，而 IPv4 名称保持不变以降低升级风险。
func NewIPSet(handle utilipset.Interface, name string, setType utilipset.Type, isIPv6 bool, comment string) *IPSet {
	hashFamily := utilipset.ProtocolFamilyIPV4
	if isIPv6 {
		hashFamily = utilipset.ProtocolFamilyIPV6
		// 双栈环境下 IPv4 与 IPv6 的 ipset 可以共存。为保证名称唯一，
		// IPv6 前缀由 "KUBE-" 改为 "KUBE-6-"；保留 "KUBE-" 前缀是为了向后兼容。
		// ipset 名称最长为 31 个字符，改名时必须考虑这一限制。
		// IPv4 名称保持不变，以尽量降低升级过程中出问题的风险。
		if strings.HasPrefix(name, "KUBE-") {
			name = strings.Replace(name, "KUBE-", "KUBE-6-", 1)
			if len(name) > 31 {
				klog.InfoS("Ipset name truncated", "ipSetName", name, "truncatedName", name[:31])
				name = name[:31]
			}
		}
	}
	set := &IPSet{
		IPSet: utilipset.IPSet{
			Name:       name,
			SetType:    setType,
			HashFamily: hashFamily,
			Comment:    comment,
		},
		activeEntries: sets.New[string](),
		handle:        handle,
	}
	return set
}

// validateEntry 校验条目是否与本集合的类型、地址族等参数兼容。
func (set *IPSet) validateEntry(entry *utilipset.Entry) bool {
	return entry.Validate(&set.IPSet)
}

// isEmpty 报告期望的活跃条目集合是否为空，供 proxier 决定是否生成关联规则。
func (set *IPSet) isEmpty() bool {
	return set.activeEntries.Len() == 0
}

// getComment 返回带引号的 ipset 注释，供 iptables 规则中的 comment 匹配使用。
func (set *IPSet) getComment() string {
	return fmt.Sprintf("\"%s\"", set.Comment)
}

// resetEntries 在每轮同步开始时清空缓存的期望条目，
// 避免上一轮遗留的条目影响本轮的差量计算。
func (set *IPSet) resetEntries() {
	set.activeEntries = sets.New[string]()
}

// syncIPSetEntries 把内核中实际的集合条目与期望的 activeEntries 做差量同步：
// 先删除内核中多余的旧条目，再补齐缺失的新条目。同步是串行执行的，
// 因为对同一 ipset 的并发增删改会相互干扰；"not found" 错误被忽略，
// 因为条目可能已被其他路径删除，删除操作本身是幂等的。
func (set *IPSet) syncIPSetEntries() {
	appliedEntries, err := set.handle.ListEntries(set.Name)
	if err != nil {
		klog.ErrorS(err, "Failed to list ip set entries")
		return
	}

	// currentIPSetEntries 表示内核中实际存在的条目集合。
	currentIPSetEntries := sets.New[string]()
	for _, appliedEntry := range appliedEntries {
		currentIPSetEntries.Insert(appliedEntry)
	}

	if !set.activeEntries.Equal(currentIPSetEntries) {
		// 先清理内核中已不再期望的旧条目。
		for _, entry := range currentIPSetEntries.Difference(set.activeEntries).UnsortedList() {
			if err := set.handle.DelEntry(entry, set.Name); err != nil {
				if !utilipset.IsNotFoundError(err) {
					klog.ErrorS(err, "Failed to delete ip set entry from ip set", "ipSetEntry", entry, "ipSet", set.Name)
				}
			} else {
				klog.V(3).InfoS("Successfully deleted legacy ip set entry from ip set", "ipSetEntry", entry, "ipSet", set.Name)
			}
		}
		// 再补齐期望存在但内核中缺失的条目。
		for _, entry := range set.activeEntries.Difference(currentIPSetEntries).UnsortedList() {
			if err := set.handle.AddEntry(entry, &set.IPSet, true); err != nil {
				klog.ErrorS(err, "Failed to add ip set entry to ip set", "ipSetEntry", entry, "ipSet", set.Name)
			} else {
				klog.V(3).InfoS("Successfully added ip set entry to ip set", "ipSetEntry", entry, "ipSet", set.Name)
			}
		}
	}
}

// ensureIPSet 确保 ipset 集合存在；ignoreExistErr 为 true 时，
// 同名同参数的集合已存在不会被当作错误，便于重复调用。
func ensureIPSet(set *IPSet) error {
	if err := set.handle.CreateSet(&set.IPSet, true); err != nil {
		klog.ErrorS(err, "Failed to make sure existence of ip set", "ipSet", set)
		return err
	}
	return nil
}

// checkMinVersion 检查当前 ipset 版本是否满足最低版本要求；
// 版本字符串无法解析时按不满足处理，以免在未知版本上启用不兼容特性。
func checkMinVersion(vstring string) bool {
	version, err := utilversion.ParseGeneric(vstring)
	if err != nil {
		klog.ErrorS(err, "Got invalid version string", "versionString", vstring)
		return false
	}

	minVersion, err := utilversion.ParseGeneric(MinIPSetCheckVersion)
	if err != nil {
		klog.ErrorS(err, "Got invalid version string", "versionString", MinIPSetCheckVersion)
		return false
	}
	return !version.LessThan(minVersion)
}
