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
	"testing"

	"github.com/vishvananda/netlink"

	netutils "k8s.io/utils/net"
)

// TestFlowFilter 验证 flowFilter 只匹配过滤器中已存在的连接，
// 覆盖命中、未命中以及空连接与具体过滤器不匹配三种场景。
func TestFlowFilter(t *testing.T) {
	// 这里只构造 Forward 元组：flowKey 会把缺失的 Reverse 字段按零值编码，
	// 足以验证基本的匹配/不匹配逻辑。
	flow1 := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("1.1.1.1"),
			DstIP:    netutils.ParseIPSloppy("2.2.2.2"),
			SrcPort:  1000,
			DstPort:  2000,
			Protocol: 6,
		},
	}
	flow2 := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("3.3.3.3"),
			DstIP:    netutils.ParseIPSloppy("4.4.4.4"),
			SrcPort:  3000,
			DstPort:  4000,
			Protocol: 17,
		},
	}
	flowEmpty := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			Protocol: 6,
		},
	}

	testCases := []struct {
		name          string
		filterFlows   []*netlink.ConntrackFlow
		matchFlow     *netlink.ConntrackFlow
		expectedMatch bool
	}{
		{
			name:          "Match existing flow",
			filterFlows:   []*netlink.ConntrackFlow{flow1, flow2},
			matchFlow:     flow1,
			expectedMatch: true,
		},
		{
			name:          "No match for non-existent flow",
			filterFlows:   []*netlink.ConntrackFlow{flow1},
			matchFlow:     flow2,
			expectedMatch: false,
		},
		{
			name:          "No match for empty flow against specific filter",
			filterFlows:   []*netlink.ConntrackFlow{flow1},
			matchFlow:     flowEmpty,
			expectedMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFlowFilter(tc.filterFlows)
			match := f.MatchConntrackFlow(tc.matchFlow)
			// 断言实际匹配结果与用例期望一致，防止过滤集合的键编码出现偏差。
			if match != tc.expectedMatch {
				t.Errorf("MatchConntrackFlow() = %v, want %v", match, tc.expectedMatch)
			}
		})
	}
}

// TestFlowFilter_Exhaustive 在正反向元组都填充的场景下验证 flowFilter，
// 额外覆盖过滤器自身也为空连接时应当命中的情形。
func TestFlowFilter_Exhaustive(t *testing.T) {
	// 正反向元组都填充，模拟真实的 conntrack 连接；
	// 这样可以验证 flowKey 同时考虑两个方向的地址与端口。
	flow1 := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("1.1.1.1"),
			DstIP:    netutils.ParseIPSloppy("2.2.2.2"),
			SrcPort:  1000,
			DstPort:  2000,
			Protocol: 6,
		},
		Reverse: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("2.2.2.2"),
			DstIP:    netutils.ParseIPSloppy("1.1.1.1"),
			SrcPort:  2000,
			DstPort:  1000,
			Protocol: 6,
		},
	}
	flow2 := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("3.3.3.3"),
			DstIP:    netutils.ParseIPSloppy("4.4.4.4"),
			SrcPort:  3000,
			DstPort:  4000,
			Protocol: 17,
		},
		Reverse: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("4.4.4.4"),
			DstIP:    netutils.ParseIPSloppy("3.3.3.3"),
			SrcPort:  4000,
			DstPort:  3000,
			Protocol: 17,
		},
	}
	flowEmpty := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			Protocol: 6,
		},
		Reverse: netlink.IPTuple{
			Protocol: 6,
		},
	}

	testCases := []struct {
		name          string
		filterFlows   []*netlink.ConntrackFlow
		matchFlow     *netlink.ConntrackFlow
		expectedMatch bool
	}{
		{
			name:          "Match existing flow",
			filterFlows:   []*netlink.ConntrackFlow{flow1, flow2},
			matchFlow:     flow1,
			expectedMatch: true,
		},
		{
			name:          "No match for non-existent flow",
			filterFlows:   []*netlink.ConntrackFlow{flow1},
			matchFlow:     flow2,
			expectedMatch: false,
		},
		{
			name:          "No match for empty flow against specific filter",
			filterFlows:   []*netlink.ConntrackFlow{flow1},
			matchFlow:     flowEmpty,
			expectedMatch: false,
		},
		{
			// 两边都是零值连接时键相同也必须命中，验证编码不依赖具体地址值。
			name:          "Match empty flow if filter has empty flow",
			filterFlows:   []*netlink.ConntrackFlow{flowEmpty},
			matchFlow:     flowEmpty,
			expectedMatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFlowFilter(tc.filterFlows)
			match := f.MatchConntrackFlow(tc.matchFlow)
			if match != tc.expectedMatch {
				t.Errorf("MatchConntrackFlow() = %v, want %v", match, tc.expectedMatch)
			}
		})
	}
}
