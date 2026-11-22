//go:build linux

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

package conntrack

import (
	"fmt"

	"github.com/vishvananda/netlink"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// flowFilter 基于一组目标连接的键集合实现 netlink.CustomConntrackFilter，
// 用于按连接五元组（含正反向）匹配并筛选 conntrack 条目。
type flowFilter struct {
	flows sets.Set[string]
}

// 编译期断言：flowFilter 必须实现 netlink.CustomConntrackFilter，
// 否则 netlink 删除连接的 API 无法使用它。
var _ netlink.CustomConntrackFilter = (*flowFilter)(nil)

// newFlowFilter 根据给定的连接列表构造过滤器，
// 将每条连接映射为唯一键存入集合，便于 O(1) 匹配。
func newFlowFilter(flows []*netlink.ConntrackFlow) *flowFilter {
	f := &flowFilter{
		flows: sets.New[string](),
	}
	for _, flow := range flows {
		// 以 flowKey 的规范化字符串作为键，避免同一连接因指针不同而被重复计入。
		f.flows.Insert(flowKey(flow))
	}
	return f
}

// flowKey 返回能唯一标识给定连接的键，
// 由正向与反向元组的协议、IP 和端口拼接而成。
func flowKey(flow *netlink.ConntrackFlow) string {
	return fmt.Sprintf("%d#%s#%d#%s#%d#%s#%d#%s#%d",
		flow.Forward.Protocol,
		flow.Forward.SrcIP.String(),
		flow.Forward.SrcPort,
		flow.Forward.DstIP.String(),
		flow.Forward.DstPort,
		flow.Reverse.SrcIP.String(),
		flow.Reverse.SrcPort,
		flow.Reverse.DstIP.String(),
		flow.Reverse.DstPort)
}

// MatchConntrackFlow 判断给定连接是否命中过滤器：
// 命中则返回 true，netlink 会据此删除对应的 conntrack 条目。
func (f *flowFilter) MatchConntrackFlow(flow *netlink.ConntrackFlow) bool {
	if f.flows.Has(flowKey(flow)) {
		// 以 V(6) 日志输出被删除的连接，便于事后排查 conntrack 相关问题。
		klog.V(6).InfoS("Deleting conntrack entry", "flow", flow)
		return true
	}
	return false
}
