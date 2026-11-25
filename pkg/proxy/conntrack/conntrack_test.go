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

package conntrack

import (
	"testing"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	netutils "k8s.io/utils/net"
)

// TestConntracker_DeleteEntries 验证 DeleteEntries 只删除匹配的 conntrack 表项：
// 覆盖单条/多条删除、不存在的流、字段不全的流以及空删除列表，
// 既检查返回的删除数量，也用 ListEntries 检查剩余表项，确保误删能被发现。
func TestConntracker_DeleteEntries(t *testing.T) {
	// 定义几条公共测试流：TCP(6)、UDP(17) 和其他协议(132)，
	// 正反元组互为镜像，用于模拟真实的 conntrack 双向流。
	// 正向与反向元组端口、地址互换，协议为 TCP(6)。
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
	// 协议为 UDP(17) 的第二条流，用于验证按协议区分表项。
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
	// 协议 132 的第三条流，验证非 TCP/UDP 协议同样可被处理。
	flow3 := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("5.5.5.5"),
			DstIP:    netutils.ParseIPSloppy("6.6.6.6"),
			SrcPort:  5000,
			DstPort:  6000,
			Protocol: 132,
		},
		Reverse: netlink.IPTuple{
			SrcIP:    netutils.ParseIPSloppy("6.6.6.6"),
			DstIP:    netutils.ParseIPSloppy("5.5.5.5"),
			SrcPort:  6000,
			DstPort:  5000,
			Protocol: 132,
		},
	}
	// 边界情况：只有协议的残缺流，用于验证不完整的元组不会误匹配任何表项。
	flowEmpty := &netlink.ConntrackFlow{
		Forward: netlink.IPTuple{
			Protocol: 6,
		},
	}

	// 表驱动测试：expectedDeleted 校验删除计数，
	// expectedLeft 校验 ListEntries 看到的剩余数量，两者共同防止多删或漏删。
	testCases := []struct {
		name            string
		initialEntries  []*netlink.ConntrackFlow
		deleteFlows     []*netlink.ConntrackFlow
		expectedDeleted int
		expectedLeft    int
	}{
		{
			name:            "Delete single flow",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2, flow3},
			deleteFlows:     []*netlink.ConntrackFlow{flow1},
			expectedDeleted: 1,
			expectedLeft:    2,
		},
		{
			name:            "Delete multiple flows",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2, flow3},
			deleteFlows:     []*netlink.ConntrackFlow{flow1, flow3},
			expectedDeleted: 2,
			expectedLeft:    1,
		},
		{
			name:            "Delete non-existent flow",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2},
			deleteFlows:     []*netlink.ConntrackFlow{flow3},
			expectedDeleted: 0,
			expectedLeft:    2,
		},
		{
			name:            "Delete flow with empty fields (should match nothing)",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2, flow3},
			deleteFlows:     []*netlink.ConntrackFlow{flowEmpty},
			expectedDeleted: 0,
			expectedLeft:    3,
		},
		{
			name:            "Delete all flows",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2, flow3},
			deleteFlows:     []*netlink.ConntrackFlow{flow1, flow2, flow3},
			expectedDeleted: 3,
			expectedLeft:    0,
		},
		{
			name:            "Delete with empty list",
			initialEntries:  []*netlink.ConntrackFlow{flow1, flow2},
			deleteFlows:     []*netlink.ConntrackFlow{},
			expectedDeleted: 0,
			expectedLeft:    2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 拷贝切片，避免前面用例的删除操作改动共享指针切片影响后续用例。
			entries := make([]*netlink.ConntrackFlow, len(tc.initialEntries))
			copy(entries, tc.initialEntries)

			// 用假 handler 装载初始表项，使测试不依赖真实 conntrack 子系统。
			handler := &fakeHandler{
				entries: entries,
			}
			ct := newConntracker(handler)

			// 用构造好的流列表执行删除，检查返回值和错误。
			n, err := ct.DeleteEntries(unix.AF_INET, tc.deleteFlows)
			if err != nil {
				t.Fatalf("DeleteEntries() error = %v, want nil", err)
			}

			if n != tc.expectedDeleted {
				t.Errorf("DeleteEntries() = %d, want %d", n, tc.expectedDeleted)
			}

			// 再列出剩余表项，确认删除操作确实只作用于预期条目。
			left, err := ct.ListEntries(unix.AF_INET)
			if err != nil {
				t.Fatalf("ListEntries() error = %v", err)
			}
			if len(left) != tc.expectedLeft {
				t.Errorf("ListEntries() left = %d, want %d", len(left), tc.expectedLeft)
			}
		})
	}
}
