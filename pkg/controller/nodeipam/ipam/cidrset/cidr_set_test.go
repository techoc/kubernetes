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

package cidrset

import (
	"math/big"
	"net"
	"reflect"
	"testing"

	"k8s.io/component-base/metrics/testutil"
	netutils "k8s.io/utils/net"
)

// TestCIDRSetFullyAllocated 验证只有一个子网的 CIDRSet 在分配、耗尽、释放、再分配过程中的行为：
// IPv4 与 IPv6 均应能分配出该唯一子网，分配两次必须失败，释放后又能重新分配。
func TestCIDRSetFullyAllocated(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		subNetMaskSize int
		expectedCIDR   string
		description    string
	}{
		{
			clusterCIDRStr: "127.123.234.0/30",
			subNetMaskSize: 30,
			expectedCIDR:   "127.123.234.0/30",
			description:    "Fully allocated CIDR with IPv4",
		},
		{
			clusterCIDRStr: "beef:1234::/30",
			subNetMaskSize: 30,
			expectedCIDR:   "beef:1234::/30",
			description:    "Fully allocated CIDR with IPv6",
		},
	}
	for _, tc := range cases {
		_, clusterCIDR, _ := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
		a, err := NewCIDRSet(clusterCIDR, tc.subNetMaskSize)
		if err != nil {
			t.Fatalf("unexpected error: %v for %v", err, tc.description)
		}
		p, err := a.AllocateNext()
		if err != nil {
			t.Fatalf("unexpected error: %v for %v", err, tc.description)
		}
		if p.String() != tc.expectedCIDR {
			t.Fatalf("unexpected allocated cidr: %v, expecting %v for %v",
				p.String(), tc.expectedCIDR, tc.description)
		}

		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}

		a.Release(p)

		p, err = a.AllocateNext()
		if err != nil {
			t.Fatalf("unexpected error: %v for %v", err, tc.description)
		}
		if p.String() != tc.expectedCIDR {
			t.Fatalf("unexpected allocated cidr: %v, expecting %v for %v",
				p.String(), tc.expectedCIDR, tc.description)
		}
		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}
	}
}

// TestIndexToCIDRBlock 验证 indexToCIDRBlock 将数字索引转换为对应 CIDR 子网的正确性，
// 覆盖 IPv4/IPv6 的多种前缀与掩码长度，以及非首个子网的偏移量计算。
func TestIndexToCIDRBlock(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		subnetMaskSize int
		index          int
		CIDRBlock      string
		description    string
	}{
		{
			clusterCIDRStr: "127.123.3.0/16",
			subnetMaskSize: 24,
			index:          0,
			CIDRBlock:      "127.123.0.0/24",
			description:    "1st IP address indexed with IPv4",
		},
		{
			clusterCIDRStr: "127.123.0.0/16",
			subnetMaskSize: 24,
			index:          15,
			CIDRBlock:      "127.123.15.0/24",
			description:    "16th IP address indexed with IPv4",
		},
		{
			clusterCIDRStr: "192.168.5.219/28",
			subnetMaskSize: 32,
			index:          5,
			CIDRBlock:      "192.168.5.213/32",
			description:    "5th IP address indexed with IPv4",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:3::/48",
			subnetMaskSize: 64,
			index:          0,
			CIDRBlock:      "2001:db8:1234::/64",
			description:    "1st IP address indexed with IPv6 /64",
		},
		{
			clusterCIDRStr: "2001:0db8:1234::/48",
			subnetMaskSize: 64,
			index:          15,
			CIDRBlock:      "2001:db8:1234:f::/64",
			description:    "16th IP address indexed with IPv6 /64",
		},
		{
			clusterCIDRStr: "2001:0db8:85a3::8a2e:0370:7334/50",
			subnetMaskSize: 63,
			index:          6425,
			CIDRBlock:      "2001:db8:85a3:3232::/63",
			description:    "6426th IP address indexed with IPv6 /63",
		},
		{
			clusterCIDRStr: "2001:0db8::/32",
			subnetMaskSize: 48,
			index:          0,
			CIDRBlock:      "2001:db8::/48",
			description:    "1st IP address indexed with IPv6 /48",
		},
		{
			clusterCIDRStr: "2001:0db8::/32",
			subnetMaskSize: 48,
			index:          15,
			CIDRBlock:      "2001:db8:f::/48",
			description:    "16th IP address indexed with IPv6 /48",
		},
		{
			clusterCIDRStr: "2001:0db8:85a3::8a2e:0370:7334/32",
			subnetMaskSize: 48,
			index:          6425,
			CIDRBlock:      "2001:db8:1919::/48",
			description:    "6426th IP address indexed with IPv6 /48",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:ff00::/56",
			subnetMaskSize: 72,
			index:          0,
			CIDRBlock:      "2001:db8:1234:ff00::/72",
			description:    "1st IP address indexed with IPv6 /72",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:ff00::/56",
			subnetMaskSize: 72,
			index:          15,
			CIDRBlock:      "2001:db8:1234:ff00:f00::/72",
			description:    "16th IP address indexed with IPv6 /72",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:ff00::0370:7334/56",
			subnetMaskSize: 72,
			index:          6425,
			CIDRBlock:      "2001:db8:1234:ff19:1900::/72",
			description:    "6426th IP address indexed with IPv6 /72",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:0:1234::/80",
			subnetMaskSize: 96,
			index:          0,
			CIDRBlock:      "2001:db8:1234:0:1234::/96",
			description:    "1st IP address indexed with IPv6 /96",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:0:1234::/80",
			subnetMaskSize: 96,
			index:          15,
			CIDRBlock:      "2001:db8:1234:0:1234:f::/96",
			description:    "16th IP address indexed with IPv6 /96",
		},
		{
			clusterCIDRStr: "2001:0db8:1234:ff00::0370:7334/80",
			subnetMaskSize: 96,
			index:          6425,
			CIDRBlock:      "2001:db8:1234:ff00:0:1919::/96",
			description:    "6426th IP address indexed with IPv6 /96",
		},
	}
	for _, tc := range cases {
		_, clusterCIDR, _ := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
		a, err := NewCIDRSet(clusterCIDR, tc.subnetMaskSize)
		if err != nil {
			t.Fatalf("error for %v ", tc.description)
		}
		cidr := a.indexToCIDRBlock(tc.index)
		if cidr.String() != tc.CIDRBlock {
			t.Fatalf("error for %v index %d %s", tc.description, tc.index, cidr.String())
		}
	}
}

// TestCIDRSet_RandomishAllocation 验证 CIDRSet 在多轮“全量分配 - 释放 - 再分配”后，
// 分配集合是否保持一致（“随机化”分配顺序下仍应覆盖全部子网，且释放后能完整回收）。
func TestCIDRSet_RandomishAllocation(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		description    string
	}{
		{
			clusterCIDRStr: "127.123.234.0/16",
			description:    "RandomishAllocation with IPv4",
		},
		{
			clusterCIDRStr: "beef:1234::/16",
			description:    "RandomishAllocation with IPv6",
		},
	}
	for _, tc := range cases {
		_, clusterCIDR, _ := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
		a, err := NewCIDRSet(clusterCIDR, 24)
		if err != nil {
			t.Fatalf("Error allocating CIDRSet for %v", tc.description)
		}
		// 分配全部 256 个 /24 CIDR，并在耗尽后验证再次分配会失败。
		var cidrs []*net.IPNet

		for i := 0; i < 256; i++ {
			if c, err := a.AllocateNext(); err == nil {
				cidrs = append(cidrs, c)
			} else {
				t.Fatalf("unexpected error: %v for %v", err, tc.description)
			}
		}

		//var err error
		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}
		// 释放所有 CIDR，为下一轮分配做准备。
		for i := 0; i < len(cidrs); i++ {
			a.Release(cidrs[i])
		}

		// 再次分配全部 256 个 CIDR，验证释放后可重新分配。
		var rcidrs []*net.IPNet
		for i := 0; i < 256; i++ {
			if c, err := a.AllocateNext(); err == nil {
				rcidrs = append(rcidrs, c)
			} else {
				t.Fatalf("unexpected error: %d, %v for %v", i, err, tc.description)
			}
		}
		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}

		if !reflect.DeepEqual(cidrs, rcidrs) {
			t.Fatalf("expected re-allocated cidrs are the same collection for %v", tc.description)
		}
	}
}

// TestCIDRSet_AllocationOccupied 验证混合“释放 + 占用”场景下的分配行为：
// 先分配并释放全部 CIDR，再占用后一半，随后只能分配前一半；
// 最终断言重新分配的 CIDR 与占用标记共同构成原始的完整集合。
func TestCIDRSet_AllocationOccupied(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		description    string
	}{
		{
			clusterCIDRStr: "127.123.234.0/16",
			description:    "AllocationOccupied with IPv4",
		},
		{
			clusterCIDRStr: "beef:1234::/16",
			description:    "AllocationOccupied with IPv6",
		},
	}
	for _, tc := range cases {
		_, clusterCIDR, _ := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
		a, err := NewCIDRSet(clusterCIDR, 24)
		if err != nil {
			t.Fatalf("Error allocating CIDRSet for %v", tc.description)
		}
		// 分配全部 256 个 CIDR。
		var cidrs []*net.IPNet
		var numCIDRs = 256

		for i := 0; i < numCIDRs; i++ {
			if c, err := a.AllocateNext(); err == nil {
				cidrs = append(cidrs, c)
			} else {
				t.Fatalf("unexpected error: %v for %v", err, tc.description)
			}
		}

		//var err error
		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}
		// 释放所有 CIDR。
		for i := 0; i < len(cidrs); i++ {
			a.Release(cidrs[i])
		}
		// 占用后 128 个 CIDR。
		for i := numCIDRs / 2; i < numCIDRs; i++ {
			a.Occupy(cidrs[i])
		}
		// 再次占用后半部分的第一个 CIDR，验证重复占用是幂等的。
		a.Occupy(cidrs[numCIDRs/2])

		// 重新分配前 128 个 CIDR（后半部分已被占用）。
		var rcidrs []*net.IPNet
		for i := 0; i < numCIDRs/2; i++ {
			if c, err := a.AllocateNext(); err == nil {
				rcidrs = append(rcidrs, c)
			} else {
				t.Fatalf("unexpected error: %d, %v for %v", i, err, tc.description)
			}
		}
		_, err = a.AllocateNext()
		if err == nil {
			t.Fatalf("expected error because of fully-allocated range for %v", tc.description)
		}

		// 校验 Occupy() 的结果：将占用过的 CIDR 加入结果集后应与原始集合一致。
		for i := numCIDRs / 2; i < numCIDRs; i++ {
			rcidrs = append(rcidrs, cidrs[i])
		}
		if !reflect.DeepEqual(cidrs, rcidrs) {
			t.Fatalf("expected re-allocated cidrs are the same collection for %v", tc.description)
		}
	}
}

// TestDoubleOccupyRelease 验证重复占用与重复释放 CIDR 时内部计数是否准确：
// 依次执行占用/释放操作，并在每步检查已占用数量；最后验证可分配数量与预期一致。
func TestDoubleOccupyRelease(t *testing.T) {
	// 按顺序执行一系列操作，并在每步之后检查已占用的 CIDR 数量。
	clusterCIDRStr := "10.42.0.0/16"
	operations := []struct {
		cidrStr     string
		operation   string
		numOccupied int
	}{
		// 占用 1 个 CIDR：占用数 +1。
		{
			cidrStr:     "10.42.5.0/24",
			operation:   "occupy",
			numOccupied: 1,
		},
		// 再占用 1 个 CIDR：占用数 +1。
		{
			cidrStr:     "10.42.9.0/24",
			operation:   "occupy",
			numOccupied: 2,
		},
		// 占用 4 个 CIDR，其中一个与前面已占用的重叠：占用数 +3。
		{
			cidrStr:     "10.42.8.0/22",
			operation:   "occupy",
			numOccupied: 5,
		},
		// 再次占用同一个已占用的 CIDR：占用数不变。
		{
			cidrStr:     "10.42.9.0/24",
			operation:   "occupy",
			numOccupied: 5,
		},
		// 释放一个已占用的 CIDR：占用数 -1。
		{
			cidrStr:     "10.42.9.0/24",
			operation:   "release",
			numOccupied: 4,
		},
		// 释放一个未占用的 CIDR：占用数不变。
		{
			cidrStr:     "10.42.9.0/24",
			operation:   "release",
			numOccupied: 4,
		},
		// 释放 4 个 CIDR，其中只有一个之前被占用：占用数 -1。
		{
			cidrStr:     "10.42.4.0/22",
			operation:   "release",
			numOccupied: 3,
		},
	}
	// 所有操作完成后，应恰好剩下 (1 << 8) - 3 个可分配的 /24 子网。
	numAllocatable24s := (1 << 8) - 3

	_, clusterCIDR, _ := netutils.ParseCIDRSloppy(clusterCIDRStr)
	a, err := NewCIDRSet(clusterCIDR, 24)
	if err != nil {
		t.Fatalf("Error allocating CIDRSet")
	}

	// 按顺序执行占用/释放操作，并逐步校验已占用的 CIDR 数量。
	for _, op := range operations {
		_, cidr, _ := netutils.ParseCIDRSloppy(op.cidrStr)
		switch op.operation {
		case "occupy":
			a.Occupy(cidr)
		case "release":
			a.Release(cidr)
		default:
			t.Fatalf("test error: unknown operation %v", op.operation)
		}
		if a.allocatedCIDRs != op.numOccupied {
			t.Fatalf("Expected %d occupied CIDRS, got %d", op.numOccupied, a.allocatedCIDRs)
		}
	}

	// 验证经过上述分配与释放后，恰好还能分配 numAllocatable24s 个 /24 子网。
	for i := 0; i < numAllocatable24s; i++ {
		_, err := a.AllocateNext()
		if err != nil {
			t.Fatalf("Expected to be able to allocate %d CIDRS, failed after %d", numAllocatable24s, i)
		}
	}

	_, err = a.AllocateNext()
	if err == nil {
		t.Fatalf("Expected to be able to allocate exactly %d CIDRS, got one more", numAllocatable24s)
	}
}

// TestGetBitforCIDR 验证 getIndexForIP 对给定 IP 在 CIDRSet 中索引的计算：
// 覆盖 IPv4/IPv6 的多种掩码长度、多段偏移，以及 IP 不属于集群网段时返回错误的场景。
func TestGetBitforCIDR(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		subNetMaskSize int
		subNetCIDRStr  string
		expectedBit    int
		expectErr      bool
		description    string
	}{
		{
			clusterCIDRStr: "127.0.0.0/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "127.0.0.0/16",
			expectedBit:    0,
			expectErr:      false,
			description:    "Get 0 Bit with IPv4",
		},
		{
			clusterCIDRStr: "be00::/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "be00::/16",
			expectedBit:    0,
			expectErr:      false,
			description:    "Get 0 Bit with IPv6",
		},
		{
			clusterCIDRStr: "127.0.0.0/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "127.123.0.0/16",
			expectedBit:    123,
			expectErr:      false,
			description:    "Get 123rd Bit with IPv4",
		},
		{
			clusterCIDRStr: "be00::/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "beef::/16",
			expectedBit:    0xef,
			expectErr:      false,
			description:    "Get xef Bit with IPv6",
		},
		{
			clusterCIDRStr: "127.0.0.0/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "127.168.0.0/16",
			expectedBit:    168,
			expectErr:      false,
			description:    "Get 168th Bit with IPv4",
		},
		{
			clusterCIDRStr: "be00::/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "be68::/16",
			expectedBit:    0x68,
			expectErr:      false,
			description:    "Get x68th Bit with IPv6",
		},
		{
			clusterCIDRStr: "127.0.0.0/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "127.224.0.0/16",
			expectedBit:    224,
			expectErr:      false,
			description:    "Get 224th Bit with IPv4",
		},
		{
			clusterCIDRStr: "be00::/8",
			subNetMaskSize: 16,
			subNetCIDRStr:  "be24::/16",
			expectedBit:    0x24,
			expectErr:      false,
			description:    "Get x24th Bit with IPv6",
		},
		{
			clusterCIDRStr: "192.168.0.0/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "192.168.12.0/24",
			expectedBit:    12,
			expectErr:      false,
			description:    "Get 12th Bit with IPv4",
		},
		{
			clusterCIDRStr: "beef::/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "beef:1200::/24",
			expectedBit:    0x12,
			expectErr:      false,
			description:    "Get x12th Bit with IPv6",
		},
		{
			clusterCIDRStr: "192.168.0.0/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "192.168.151.0/24",
			expectedBit:    151,
			expectErr:      false,
			description:    "Get 151st Bit with IPv4",
		},
		{
			clusterCIDRStr: "beef::/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "beef:9700::/24",
			expectedBit:    0x97,
			expectErr:      false,
			description:    "Get x97st Bit with IPv6",
		},
		{
			clusterCIDRStr: "192.168.0.0/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "127.168.224.0/24",
			expectErr:      true,
			description:    "Get error with IPv4",
		},
		{
			clusterCIDRStr: "beef::/16",
			subNetMaskSize: 24,
			subNetCIDRStr:  "2001:db00::/24",
			expectErr:      true,
			description:    "Get error with IPv6",
		},
	}

	// 子测试名即用例描述；对 expectErr 场景单独断言错误是否出现，避免误判索引值。
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, clusterCIDR, err := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			cs, err := NewCIDRSet(clusterCIDR, tc.subNetMaskSize)
			if err != nil {
				t.Fatalf("Error allocating CIDRSet")
			}
			_, subnetCIDR, err := netutils.ParseCIDRSloppy(tc.subNetCIDRStr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, err := cs.getIndexForIP(subnetCIDR.IP)
			if err == nil && tc.expectErr {
				t.Errorf("expected error but got null")
				return
			}

			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if got != tc.expectedBit {
				t.Errorf("expected %v, but got %v", tc.expectedBit, got)
			}
		})
	}
}

// TestOccupy 验证 Occupy 对不同前缀长度的 CIDR 的占用范围计算是否正确，
// 覆盖 IPv4/IPv6、占用全部/单个/部分/越界偏移等场景，并直接比较内部 used 位图。
func TestOccupy(t *testing.T) {
	cases := []struct {
		clusterCIDRStr    string
		subNetMaskSize    int
		subNetCIDRStr     string
		expectedUsedBegin int
		expectedUsedEnd   int
		expectErr         bool
		description       string
	}{
		{
			clusterCIDRStr:    "127.0.0.0/8",
			subNetMaskSize:    16,
			subNetCIDRStr:     "127.0.0.0/8",
			expectedUsedBegin: 0,
			expectedUsedEnd:   255,
			expectErr:         false,
			description:       "Occupy all Bits with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:1200::/40",
			subNetMaskSize:    48,
			subNetCIDRStr:     "2001:beef:1200::/40",
			expectedUsedBegin: 0,
			expectedUsedEnd:   255,
			expectErr:         false,
			description:       "Occupy all Bits with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/8",
			subNetMaskSize:    16,
			subNetCIDRStr:     "127.0.0.0/2",
			expectedUsedBegin: 0,
			expectedUsedEnd:   255,
			expectErr:         false,
			description:       "Occupy every Bit with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:1200::/40",
			subNetMaskSize:    48,
			subNetCIDRStr:     "2001:beef:1234::/34",
			expectedUsedBegin: 0,
			expectedUsedEnd:   255,
			expectErr:         false,
			description:       "Occupy every Bit with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/8",
			subNetMaskSize:    16,
			subNetCIDRStr:     "127.0.0.0/16",
			expectedUsedBegin: 0,
			expectedUsedEnd:   0,
			expectErr:         false,
			description:       "Occupy 1st Bit with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:1200::/40",
			subNetMaskSize:    48,
			subNetCIDRStr:     "2001:beef:1200::/48",
			expectedUsedBegin: 0,
			expectedUsedEnd:   0,
			expectErr:         false,
			description:       "Occupy 1st Bit with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/8",
			subNetMaskSize:    32,
			subNetCIDRStr:     "127.0.0.0/16",
			expectedUsedBegin: 0,
			expectedUsedEnd:   65535,
			expectErr:         false,
			description:       "Occupy 65535 Bits with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:1200::/48",
			subNetMaskSize:    64,
			subNetCIDRStr:     "2001:beef:1200::/48",
			expectedUsedBegin: 0,
			expectedUsedEnd:   65535,
			expectErr:         false,
			description:       "Occupy 65535 Bits with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/7",
			subNetMaskSize:    16,
			subNetCIDRStr:     "127.0.0.0/15",
			expectedUsedBegin: 256,
			expectedUsedEnd:   257,
			expectErr:         false,
			description:       "Occupy 257th Bit with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:7f00::/39",
			subNetMaskSize:    48,
			subNetCIDRStr:     "2001:beef:7f00::/47",
			expectedUsedBegin: 256,
			expectedUsedEnd:   257,
			expectErr:         false,
			description:       "Occupy 257th Bit with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/7",
			subNetMaskSize:    15,
			subNetCIDRStr:     "127.0.0.0/15",
			expectedUsedBegin: 128,
			expectedUsedEnd:   128,
			expectErr:         false,
			description:       "Occupy 128th Bit with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:7f00::/39",
			subNetMaskSize:    47,
			subNetCIDRStr:     "2001:beef:7f00::/47",
			expectedUsedBegin: 128,
			expectedUsedEnd:   128,
			expectErr:         false,
			description:       "Occupy 128th Bit with IPv6",
		},
		{
			clusterCIDRStr:    "127.0.0.0/7",
			subNetMaskSize:    18,
			subNetCIDRStr:     "127.0.0.0/15",
			expectedUsedBegin: 1024,
			expectedUsedEnd:   1031,
			expectErr:         false,
			description:       "Occupy 1031st Bit with IPv4",
		},
		{
			clusterCIDRStr:    "2001:beef:7f00::/39",
			subNetMaskSize:    50,
			subNetCIDRStr:     "2001:beef:7f00::/47",
			expectedUsedBegin: 1024,
			expectedUsedEnd:   1031,
			expectErr:         false,
			description:       "Occupy 1031st Bit with IPv6",
		},
	}

	// 逐一验证 Occupy 后 used 位图中被置位的索引范围与期望一致；
	// 该位图直接反映哪些子网被标记为已占用。
	for _, tc := range cases {
		_, clusterCIDR, err := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
		if err != nil {
			t.Fatalf("unexpected error: %v for %v", err, tc.description)
		}

		cs, err := NewCIDRSet(clusterCIDR, tc.subNetMaskSize)
		if err != nil {
			t.Fatalf("Error allocating CIDRSet for %v", tc.description)
		}

		_, subnetCIDR, err := netutils.ParseCIDRSloppy(tc.subNetCIDRStr)
		if err != nil {
			t.Fatalf("unexpected error: %v for %v", err, tc.description)
		}

		err = cs.Occupy(subnetCIDR)
		if err == nil && tc.expectErr {
			t.Errorf("expected error but got none for %v", tc.description)
			continue
		}
		if err != nil && !tc.expectErr {
			t.Errorf("unexpected error: %v for %v", err, tc.description)
			continue
		}

		expectedUsed := big.Int{}
		for i := tc.expectedUsedBegin; i <= tc.expectedUsedEnd; i++ {
			expectedUsed.SetBit(&expectedUsed, i, 1)
		}
		if expectedUsed.Cmp(&cs.used) != 0 {
			t.Errorf("error for %v", tc.description)
		}
	}
}

// TestCIDRSetv6 覆盖 IPv6（及部分 IPv4）场景下的 CIDRSet 行为：
// 既检查超出最大子网掩码的构造错误，也检查正常分配时返回的子网是否符合预期顺序。
func TestCIDRSetv6(t *testing.T) {
	cases := []struct {
		clusterCIDRStr string
		subNetMaskSize int
		expectedCIDR   string
		expectedCIDR2  string
		expectErr      bool
		description    string
	}{
		{
			clusterCIDRStr: "127.0.0.0/8",
			subNetMaskSize: 32,
			expectErr:      false,
			expectedCIDR:   "127.0.0.0/32",
			expectedCIDR2:  "127.0.0.1/32",
			description:    "Max cluster subnet size with IPv4",
		},
		{
			clusterCIDRStr: "beef:1234::/32",
			subNetMaskSize: 49,
			expectErr:      true,
			description:    "Max cluster subnet size with IPv6",
		},
		{
			clusterCIDRStr: "2001:beef:1234:369b::/60",
			subNetMaskSize: 64,
			expectedCIDR:   "2001:beef:1234:3690::/64",
			expectedCIDR2:  "2001:beef:1234:3691::/64",
			expectErr:      false,
			description:    "Allocate a few IPv6",
		},
	}
	for _, tc := range cases {
		// 分别校验构造 CIDRSet 的成功/失败预期，以及随后两次 AllocateNext 的返回值。
		t.Run(tc.description, func(t *testing.T) {
			_, clusterCIDR, _ := netutils.ParseCIDRSloppy(tc.clusterCIDRStr)
			a, err := NewCIDRSet(clusterCIDR, tc.subNetMaskSize)
			if gotErr := err != nil; gotErr != tc.expectErr {
				t.Fatalf("NewCIDRSet(%v, %v) = %v, %v; gotErr = %t, want %t", clusterCIDR, tc.subNetMaskSize, a, err, gotErr, tc.expectErr)
			}
			if a == nil {
				return
			}
			p, err := a.AllocateNext()
			if err == nil && tc.expectErr {
				t.Errorf("a.AllocateNext() = nil, want error")
			}
			if err != nil && !tc.expectErr {
				t.Errorf("a.AllocateNext() = %+v, want no error", err)
			}
			if !tc.expectErr {
				if p != nil && p.String() != tc.expectedCIDR {
					t.Fatalf("a.AllocateNext() got %+v, want %+v", p.String(), tc.expectedCIDR)
				}
			}
			p2, err := a.AllocateNext()
			if err == nil && tc.expectErr {
				t.Errorf("a.AllocateNext() = nil, want error")
			}
			if err != nil && !tc.expectErr {
				t.Errorf("a.AllocateNext() = %+v, want no error", err)
			}
			if !tc.expectErr {
				if p2 != nil && p2.String() != tc.expectedCIDR2 {
					t.Fatalf("a.AllocateNext() got %+v, want %+v", p2.String(), tc.expectedCIDR)
				}
			}
		})
	}
}

// TestCidrSetMetrics 验证 CIDRSet 各项指标随分配和释放的变化：
// 使用率、累计分配/释放次数、最大容量等指标均需在每一步操作后与期望值完全一致。
func TestCidrSetMetrics(t *testing.T) {
	cidr := "10.0.0.0/16"
	_, clusterCIDR, _ := netutils.ParseCIDRSloppy(cidr)
	clearMetrics(map[string]string{"clusterCIDR": cidr})

	// 初始状态：/16 前缀按 /24 划分，共有 256 个可用 CIDR。
	a, err := NewCIDRSet(clusterCIDR, 24)
	if err != nil {
		t.Fatalf("unexpected error creating CidrSet: %v", err)
	}

	clusterMaskSize, _ := clusterCIDR.Mask.Size()
	max := getMaxCIDRs(24, clusterMaskSize)
	em := testMetrics{
		usage:      0,
		allocs:     0,
		releases:   0,
		allocTries: 0,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)

	// 逐个分配全部 256 个 CIDR，并在每次分配后校验指标递增。
	for i := 1; i <= 256; i++ {
		_, err := a.AllocateNext()
		if err != nil {
			t.Fatalf("unexpected error allocating a new CIDR: %v", err)
		}
		em := testMetrics{
			usage:      float64(i) / float64(256),
			allocs:     float64(i),
			releases:   0,
			allocTries: 0,
			max:        float64(max),
		}
		expectMetrics(t, cidr, em)
	}
	// 一次性释放全部 CIDR，验证释放计数与使用率归零。
	a.Release(clusterCIDR)
	em = testMetrics{
		usage:      0,
		allocs:     256,
		releases:   256,
		allocTries: 0,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)

	// 通过 Occupy 直接占用全部 CIDR，验证 usage 达到 1。
	a.Occupy(clusterCIDR)
	em = testMetrics{
		usage:      1,
		allocs:     512,
		releases:   256,
		allocTries: 0,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)
}

// TestCidrSetMetricsHistogram 验证 allocTries 直方图指标：
// Occupy 会直接标记 CIDR 已使用但不推进 nextCandidate，因此紧接着的 AllocateNext
// 必须遍历已占用的 CIDR 才能找到下一个空闲块，遍历次数会体现在直方图中。
func TestCidrSetMetricsHistogram(t *testing.T) {
	cidr := "10.0.0.0/16"
	_, clusterCIDR, _ := netutils.ParseCIDRSloppy(cidr)
	clearMetrics(map[string]string{"clusterCIDR": cidr})

	// 初始状态：/16 前缀按 /24 划分，共有 256 个可用 CIDR。
	a, err := NewCIDRSet(clusterCIDR, 24)
	if err != nil {
		t.Fatalf("unexpected error creating CidrSet: %v", err)
	}

	clusterMaskSize, _ := clusterCIDR.Mask.Size()
	max := getMaxCIDRs(24, clusterMaskSize)
	em := testMetrics{
		usage:      0,
		allocs:     0,
		releases:   0,
		allocTries: 0,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)

	// 占用范围的一半 CIDR。
	// 注意：Occupy 不会更新 nextCandidate，这一点由后续直方图断言验证。
	_, halfClusterCIDR, _ := netutils.ParseCIDRSloppy("10.0.0.0/17")
	a.Occupy(halfClusterCIDR)
	em = testMetrics{
		usage:      0.5,
		allocs:     128,
		releases:   0,
		allocTries: 0,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)
	// 下一次 AllocateNext 应跳过已占用的 CIDR，直到找到第一个空闲块；
	// 尝试的次数应与先前通过 Occupy 占用的 CIDR 数量一致。
	_, err = a.AllocateNext()
	if err != nil {
		t.Fatalf("unexpected error allocating a new CIDR: %v", err)
	}
	em = testMetrics{
		usage:      float64(129) / float64(256),
		allocs:     129,
		releases:   0,
		allocTries: 128,
		max:        float64(max),
	}
	expectMetrics(t, cidr, em)
}

// TestCidrSetMetricsDual 验证 IPv4 与 IPv6 CIDR 集合的指标相互独立。
func TestCidrSetMetricsDual(t *testing.T) {
	// 创建 IPv4 cidrSet 并校验其初始指标。
	cidrIPv4 := "10.0.0.0/16"
	_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy(cidrIPv4)
	clearMetrics(map[string]string{"clusterCIDR": cidrIPv4})

	a, err := NewCIDRSet(clusterCIDRv4, 24)
	if err != nil {
		t.Fatalf("unexpected error creating CidrSet: %v", err)
	}

	clusterMaskSize, _ := clusterCIDRv4.Mask.Size()
	maxIPv4 := getMaxCIDRs(24, clusterMaskSize)
	em := testMetrics{
		usage:      0,
		allocs:     0,
		releases:   0,
		allocTries: 0,
		max:        float64(maxIPv4),
	}
	expectMetrics(t, cidrIPv4, em)

	// 创建 IPv6 cidrSet，并与 IPv4 集合分别校验指标。
	cidrIPv6 := "2001:db8::/48"
	_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy(cidrIPv6)
	clearMetrics(map[string]string{"clusterCIDR": cidrIPv6})

	b, err := NewCIDRSet(clusterCIDRv6, 64)
	if err != nil {
		t.Fatalf("unexpected error creating CidrSet: %v", err)
	}

	clusterMaskSize, _ = clusterCIDRv6.Mask.Size()
	maxIPv6 := getMaxCIDRs(64, clusterMaskSize)
	em = testMetrics{
		usage:      0,
		allocs:     0,
		releases:   0,
		allocTries: 0,
		max:        float64(maxIPv6),
	}
	expectMetrics(t, cidrIPv6, em)

	// 占用全部 CIDR，验证双栈场景下各协议族的指标互不干扰。
	a.Occupy(clusterCIDRv4)
	em = testMetrics{
		usage:      1,
		allocs:     256,
		releases:   0,
		allocTries: 0,
		max:        float64(maxIPv4),
	}
	expectMetrics(t, cidrIPv4, em)

	b.Occupy(clusterCIDRv6)
	em = testMetrics{
		usage:      1,
		allocs:     65536,
		releases:   0,
		allocTries: 0,
		max:        float64(maxIPv6),
	}
	expectMetrics(t, cidrIPv6, em)

	// 释放全部 CIDR，验证释放后指标回到初始值且计数累加。
	a.Release(clusterCIDRv4)
	em = testMetrics{
		usage:      0,
		allocs:     256,
		releases:   256,
		allocTries: 0,
		max:        float64(maxIPv4),
	}
	expectMetrics(t, cidrIPv4, em)
	b.Release(clusterCIDRv6)
	em = testMetrics{
		usage:      0,
		allocs:     65536,
		releases:   65536,
		allocTries: 0,
		max:        float64(maxIPv6),
	}
	expectMetrics(t, cidrIPv6, em)
}

// Test_getMaxCIDRs 验证 getMaxCIDRs 计算的 CIDR 集合最大容量：IPv4 /16 按 /24 划分为 256 个，
// IPv6 /48 按 /64 划分为 65536 个。
func Test_getMaxCIDRs(t *testing.T) {
	cidrIPv4 := "10.0.0.0/16"
	_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy(cidrIPv4)

	cidrIPv6 := "2001:db8::/48"
	_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy(cidrIPv6)

	tests := []struct {
		name             string
		subNetMaskSize   int
		clusterCIDR      *net.IPNet
		expectedMaxCIDRs int
	}{
		{
			name:             "IPv4",
			subNetMaskSize:   24,
			clusterCIDR:      clusterCIDRv4,
			expectedMaxCIDRs: 256,
		},
		{
			name:             "IPv6",
			subNetMaskSize:   64,
			clusterCIDR:      clusterCIDRv6,
			expectedMaxCIDRs: 65536,
		},
	}

	for _, test := range tests {
		// 分别验证 IPv4/IPv6 下 getMaxCIDRs 对可容纳子网数的计算。
		t.Run(test.name, func(t *testing.T) {
			clusterMaskSize, _ := test.clusterCIDR.Mask.Size()
			maxCIDRs := getMaxCIDRs(test.subNetMaskSize, clusterMaskSize)
			if test.expectedMaxCIDRs != maxCIDRs {
				t.Errorf("incorrect maxCIDRs, expected: %d, got: %d", test.expectedMaxCIDRs, maxCIDRs)
			}
		})
	}
}

// 指标测试辅助函数。
// clearMetrics 清除指定标签对应的所有 CidrSet 指标，
// 避免同一进程中不同测试用例之间的指标值相互污染。
func clearMetrics(labels map[string]string) {
	cidrSetAllocations.Delete(labels)
	cidrSetReleases.Delete(labels)
	cidrSetUsage.Delete(labels)
	cidrSetAllocationTriesPerRequest.Delete(labels)
	cidrSetMaxCidrs.Delete(labels)
}

// testMetrics 聚合一次断言所需的全部指标期望值。
type testMetrics struct {
	// usage 为已分配 CIDR 的比例（gauge）。
	usage float64
	// allocs 为累计分配次数（counter）。
	allocs float64
	// releases 为累计释放次数（counter）。
	releases float64
	// allocTries 为每次 AllocateNext 请求的尝试次数分布统计（histogram）。
	allocTries float64
	// max 为该 CIDR 集合可容纳的最大子网数（gauge）。
	max float64
}

// expectMetrics 逐一读取所有 CidrSet 监控指标并与期望值比较。
// 对 gauge/counter/histogram 分别使用对应的 testutil 读取函数；任一指标读取失败或值不符都会导致测试失败。
func expectMetrics(t *testing.T, label string, em testMetrics) {
	var m testMetrics
	var err error
	m.usage, err = testutil.GetGaugeMetricValue(cidrSetUsage.WithLabelValues(label))
	if err != nil {
		t.Errorf("failed to get %s value, err: %v", cidrSetUsage.Name, err)
	}
	m.allocs, err = testutil.GetCounterMetricValue(cidrSetAllocations.WithLabelValues(label))
	if err != nil {
		t.Errorf("failed to get %s value, err: %v", cidrSetAllocations.Name, err)
	}
	m.releases, err = testutil.GetCounterMetricValue(cidrSetReleases.WithLabelValues(label))
	if err != nil {
		t.Errorf("failed to get %s value, err: %v", cidrSetReleases.Name, err)
	}
	m.allocTries, err = testutil.GetHistogramMetricValue(cidrSetAllocationTriesPerRequest.WithLabelValues(label))
	if err != nil {
		t.Errorf("failed to get %s value, err: %v", cidrSetAllocationTriesPerRequest.Name, err)
	}
	m.max, err = testutil.GetGaugeMetricValue(cidrSetMaxCidrs.WithLabelValues(label))
	if err != nil {
		t.Errorf("failed to get %s value, err: %v", cidrSetMaxCidrs.Name, err)
	}

	if m != em {
		t.Fatalf("metrics error: expected %v, received %v", em, m)
	}
}

// 以下为性能测试。
// benchmarkAllocateAllIPv6 是 IPv6 分配性能测试的公共实现：
// 在给定前缀和子网掩码下反复分配整个地址段并释放，用于测量分配热路径的吞吐。
func benchmarkAllocateAllIPv6(cidr string, subnetMaskSize int, b *testing.B) {
	_, clusterCIDR, _ := netutils.ParseCIDRSloppy(cidr)
	a, _ := NewCIDRSet(clusterCIDR, subnetMaskSize)
	for n := 0; n < b.N; n++ {
		// 每轮分配整个范围并再尝试一次（预计失败），以覆盖分配耗尽路径。
		for i := 0; i <= a.maxCIDRs; i++ {
			a.AllocateNext()
		}
		// 每轮结束后释放整个网段，使下一轮从相同的初始状态开始。
		a.Release(clusterCIDR)
	}
}

// BenchmarkAllocateAll_48_52 基准测试在 /48 前缀下按 /52 子网分配全部 CIDR。
func BenchmarkAllocateAll_48_52(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/48", 52, b) }

// BenchmarkAllocateAll_48_56 基准测试在 /48 前缀下按 /56 子网分配全部 CIDR。
func BenchmarkAllocateAll_48_56(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/48", 56, b) }

// BenchmarkAllocateAll_48_60 基准测试在 /48 前缀下按 /60 子网分配全部 CIDR。
func BenchmarkAllocateAll_48_60(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/48", 60, b) }

// BenchmarkAllocateAll_48_64 基准测试在 /48 前缀下按 /64 子网分配全部 CIDR。
func BenchmarkAllocateAll_48_64(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/48", 64, b) }

// BenchmarkAllocateAll_64_68 基准测试在 /64 前缀下按 /68 子网分配全部 CIDR。
func BenchmarkAllocateAll_64_68(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/64", 68, b) }

// BenchmarkAllocateAll_64_72 基准测试在 /64 前缀下按 /72 子网分配全部 CIDR。
func BenchmarkAllocateAll_64_72(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/64", 72, b) }

// BenchmarkAllocateAll_64_76 基准测试在 /64 前缀下按 /76 子网分配全部 CIDR。
func BenchmarkAllocateAll_64_76(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/64", 76, b) }

// BenchmarkAllocateAll_64_80 基准测试在 /64 前缀下按 /80 子网分配全部 CIDR。
func BenchmarkAllocateAll_64_80(b *testing.B) { benchmarkAllocateAllIPv6("2001:db8::/64", 80, b) }
