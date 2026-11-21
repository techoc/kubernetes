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

package ipam

import (
	"net"
	"testing"

	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/cidrset"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/test"
)

// TestOccupyServiceCIDR 验证占用 Service CIDR 后，CIDRSet 分配的 Pod CIDR 均不会
// 与 Service CIDR 重叠。既覆盖完全位于集群 CIDR 内的情况，也覆盖部分/完全不重叠的情况。
func TestOccupyServiceCIDR(t *testing.T) {
	const clusterCIDR = "10.1.0.0/16"

TestCase:
	// 用标签跳转跳过当前用例的后续检查：分配出现非预期错误时继续下一个用例没有意义。
	for _, tc := range []struct {
		serviceCIDR string
	}{
		{"10.0.255.0/24"},
		{"10.1.0.0/24"},
		{"10.1.255.0/24"},
		{"10.2.0.0/24"},
	} {
		serviceCIDR := test.MustParseCIDR(tc.serviceCIDR)
		set, err := cidrset.NewCIDRSet(test.MustParseCIDR(clusterCIDR), 24)
		if err != nil {
			t.Errorf("test case %+v: NewCIDRSet() = %v, want nil", tc, err)
		}
		if err := occupyServiceCIDR(set, test.MustParseCIDR(clusterCIDR), serviceCIDR); err != nil {
			t.Errorf("test case %+v: occupyServiceCIDR() = %v, want nil", tc, err)
		}
		// 持续分配直到 CIDRSet 耗尽，以便检查所有已分配网段。
		var cidrs []*net.IPNet
		for {
			cidr, err := set.AllocateNext()
			if err != nil {
				if err == cidrset.ErrCIDRRangeNoCIDRsRemaining {
					break
				}
				t.Errorf("set.AllocateNext() = %v, want %v", err, cidrset.ErrCIDRRangeNoCIDRsRemaining)
				continue TestCase
			}
			cidrs = append(cidrs, cidr)
		}
		// 双向 Contains 检查：网段起点可能落在对方范围内，也可能对方起点落在本范围内，
		// 任一方向命中都说明存在重叠。
		for _, c := range cidrs {
			if c.Contains(serviceCIDR.IP) || serviceCIDR.Contains(c.IP) {
				t.Errorf("test case %+v: allocated CIDR %v from service range", tc, c)
			}
		}
	}
}
