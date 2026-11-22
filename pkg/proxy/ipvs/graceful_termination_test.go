//go:build linux

/*
Copyright 2019 The Kubernetes Authors.

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
	"reflect"
	"testing"

	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	utilipvstest "k8s.io/kubernetes/pkg/proxy/ipvs/util/testing"
	netutils "k8s.io/utils/net"
)

// Test_GracefulDeleteRS 验证 GracefulDeleteRS 的优雅删除策略：
// TCP 有连接时仅把 RS 权重降为 0 并保留记录，无连接时立即删除；
// UDP 不受优雅终止约束，即使有连接也直接删除；RS 不匹配时不做任何改动。
func Test_GracefulDeleteRS(t *testing.T) {
	tests := []struct {
		name         string
		vs           *utilipvs.VirtualServer
		rs           *utilipvs.RealServer
		existingIPVS *utilipvstest.FakeIPVS
		expectedIPVS *utilipvstest.FakeIPVS
		err          error
	}{
		{
			name: "graceful delete, no connections results in deleting the real server immediatetly",
			vs: &utilipvs.VirtualServer{
				Address:  netutils.ParseIPSloppy("1.1.1.1"),
				Protocol: "tcp",
				Port:     uint16(80),
			},
			rs: &utilipvs.RealServer{
				Address:      netutils.ParseIPSloppy("10.0.0.1"),
				Port:         uint16(80),
				Weight:       100,
				ActiveConn:   0,
				InactiveConn: 0,
			},
			existingIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   0,
							InactiveConn: 0,
						},
					},
				},
			},
			expectedIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {},
				},
			},
			err: nil,
		},
		{
			name: "graceful delete, real server has active connections, weight should be 0 but don't delete",
			vs: &utilipvs.VirtualServer{
				Address:  netutils.ParseIPSloppy("1.1.1.1"),
				Protocol: "tcp",
				Port:     uint16(80),
			},
			rs: &utilipvs.RealServer{
				Address:      netutils.ParseIPSloppy("10.0.0.1"),
				Port:         uint16(80),
				Weight:       100,
				ActiveConn:   10,
				InactiveConn: 0,
			},
			existingIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   10,
							InactiveConn: 0,
						},
					},
				},
			},
			expectedIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       0,
							ActiveConn:   10,
							InactiveConn: 0,
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "graceful delete, real server has in-active connections, weight should be 0 but don't delete",
			vs: &utilipvs.VirtualServer{
				Address:  netutils.ParseIPSloppy("1.1.1.1"),
				Protocol: "tcp",
				Port:     uint16(80),
			},
			rs: &utilipvs.RealServer{
				Address:      netutils.ParseIPSloppy("10.0.0.1"),
				Port:         uint16(80),
				Weight:       100,
				ActiveConn:   0,
				InactiveConn: 10,
			},
			existingIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   0,
							InactiveConn: 10,
						},
					},
				},
			},
			expectedIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       0,
							ActiveConn:   0,
							InactiveConn: 10,
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "graceful delete, real server has connections, but udp connections are deleted immediately",
			vs: &utilipvs.VirtualServer{
				Address:  netutils.ParseIPSloppy("1.1.1.1"),
				Protocol: "udp",
				Port:     uint16(80),
			},
			rs: &utilipvs.RealServer{
				Address:      netutils.ParseIPSloppy("10.0.0.1"),
				Port:         uint16(80),
				Weight:       100,
				ActiveConn:   10,
				InactiveConn: 10,
			},
			existingIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "udp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "udp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "udp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   10,
							InactiveConn: 10,
						},
					},
				},
			},
			expectedIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "udp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "udp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "udp",
					}: {}, // UDP 没有优雅终止，RS 会被立即删除
				},
			},
			err: nil,
		},
		{
			name: "graceful delete, real server mismatch should be no-op",
			vs: &utilipvs.VirtualServer{
				Address:  netutils.ParseIPSloppy("1.1.1.1"),
				Protocol: "tcp",
				Port:     uint16(80),
			},
			rs: &utilipvs.RealServer{
				Address:      netutils.ParseIPSloppy("10.0.0.1"),
				Port:         uint16(81), // 端口不匹配：用于验证找不到对应 RS 时不做任何修改
				Weight:       100,
				ActiveConn:   0,
				InactiveConn: 10,
			},
			existingIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   0,
							InactiveConn: 10,
						},
					},
				},
			},
			expectedIPVS: &utilipvstest.FakeIPVS{
				Services: map[utilipvstest.ServiceKey]*utilipvs.VirtualServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						Address:  netutils.ParseIPSloppy("1.1.1.1"),
						Protocol: "tcp",
						Port:     uint16(80),
					},
				},
				Destinations: map[utilipvstest.ServiceKey][]*utilipvs.RealServer{
					{
						IP:       "1.1.1.1",
						Port:     80,
						Protocol: "tcp",
					}: {
						{
							Address:      netutils.ParseIPSloppy("10.0.0.1"),
							Port:         uint16(80),
							Weight:       100,
							ActiveConn:   0,
							InactiveConn: 10,
						},
					},
				},
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ipvs := test.existingIPVS
			gracefulTerminationManager := NewGracefulTerminationManager(ipvs)

			err := gracefulTerminationManager.GracefulDeleteRS(test.vs, test.rs)
			if err != test.err {
				t.Logf("actual err: %v", err)
				t.Logf("expected err: %v", test.err)
				t.Errorf("unexpected error")
			}

			if !reflect.DeepEqual(ipvs, test.expectedIPVS) {
				t.Logf("actual: %+v", ipvs)
				t.Logf("expected : %+v", test.expectedIPVS)
				t.Errorf("unexpected IPVS servers")
			}
		})
	}
}

// Test_RaceTerminateRSList 通过并发向列表写入与 flushList 清空，
// 验证 graceTerminateRSList 在并发访问下不会出现数据竞争；运行 go test -race 时有效。
func Test_RaceTerminateRSList(t *testing.T) {
	ipvs := utilipvstest.NewFake()
	gracefulTerminationManager := NewGracefulTerminationManager(ipvs)

	// 并发写入以制造数据竞争，配合 go test -race 检测
	go func() {
		for i := 1; i <= 10; i++ {
			for j := 1; j <= 100; j++ {
				item := makeListItem(i, j)
				gracefulTerminationManager.rsList.add(item)
			}
		}
	}()

	// 等到列表中确实有元素再清空，否则测不到并发删除的行为
	for gracefulTerminationManager.rsList.len() < 20 {
	}

	// 用假的 handler 绕开对 IPVS 虚拟服务器的查询，只考察列表的并发清空逻辑
	fakeHandler := func(rsToDelete *listItem) (bool, error) {
		return true, nil
	}
	if !gracefulTerminationManager.rsList.flushList(fakeHandler) {
		t.Error("failed to flush entries")
	}
}

// makeListItem 构造一个测试用的 listItem，用 i、j 生成互不相同的 VS/RS 地址，
// 保证并发写入列表时每个元素都是唯一键。
func makeListItem(i, j int) *listItem {
	vs := fmt.Sprintf("%d.%d.%d.%d", 1, 1, i, i)
	rs := fmt.Sprintf("%d.%d.%d.%d", 1, 1, i, j)
	return &listItem{
		VirtualServer: &utilipvs.VirtualServer{
			Address:  netutils.ParseIPSloppy(vs),
			Protocol: "tcp",
			Port:     uint16(80),
		},
		RealServer: &utilipvs.RealServer{
			Address: netutils.ParseIPSloppy(rs),
			Port:    uint16(80),
		},
	}
}
