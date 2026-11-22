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
	"reflect"
	"testing"

	libipvs "github.com/moby/ipvs"
	"golang.org/x/sys/unix"

	netutils "k8s.io/utils/net"
)

// Test_toVirtualServer 验证 toVirtualServer 把 libipvs.Service 转换为 VirtualServer：
// 剔除内核总会设置的 FlagHashed、映射协议名、并对无效 Flags 报错。
func Test_toVirtualServer(t *testing.T) {
	Tests := []struct {
		ipvsService   libipvs.Service
		virtualServer VirtualServer
		expectError   bool
		reason        string
	}{
		{
			libipvs.Service{
				Flags: 0x0,
			},
			VirtualServer{},
			true,
			fmt.Sprintf("IPVS Service Flags should include %x, got 0x0", FlagHashed),
		},
		{
			libipvs.Service{
				Flags: 0x1,
			},
			VirtualServer{},
			true,
			fmt.Sprintf("IPVS Service Flags should include %x, got 0x1", FlagHashed),
		},
		{
			libipvs.Service{
				Protocol:      unix.IPPROTO_TCP,
				Port:          80,
				FWMark:        0,
				SchedName:     "",
				Flags:         uint32(FlagPersistent + FlagHashed),
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       nil,
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("0.0.0.0"),
				Protocol:  "TCP",
				Port:      80,
				Scheduler: "",
				Flags:     ServiceFlags(FlagPersistent),
				Timeout:   0,
			},
			false,
			"",
		},
		{
			libipvs.Service{
				Protocol:      unix.IPPROTO_UDP,
				Port:          33434,
				FWMark:        0,
				SchedName:     "wlc",
				Flags:         uint32(0 + FlagHashed),
				Timeout:       100,
				Netmask:       128,
				AddressFamily: unix.AF_INET6,
				Address:       netutils.ParseIPSloppy("2012::beef"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("2012::beef"),
				Protocol:  "UDP",
				Port:      33434,
				Scheduler: "wlc",
				Flags:     ServiceFlags(0),
				Timeout:   100,
			},
			false,
			"",
		},
		{
			libipvs.Service{
				Protocol:      0,
				Port:          0,
				FWMark:        0,
				SchedName:     "lc",
				Flags:         uint32(0 + FlagHashed),
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       netutils.ParseIPSloppy("1.2.3.4"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("1.2.3.4"),
				Protocol:  "",
				Port:      0,
				Scheduler: "lc",
				Flags:     ServiceFlags(0),
				Timeout:   0,
			},
			false,
			"",
		},
		{
			libipvs.Service{
				Protocol:      0,
				Port:          0,
				FWMark:        0,
				SchedName:     "wrr",
				Flags:         uint32(FlagPersistent + FlagHashed),
				Timeout:       0,
				Netmask:       128,
				AddressFamily: unix.AF_INET6,
				Address:       nil,
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("::0"),
				Protocol:  "",
				Port:      0,
				Scheduler: "wrr",
				Flags:     ServiceFlags(FlagPersistent),
				Timeout:   0,
			},
			false,
			"",
		},
		{
			libipvs.Service{
				Protocol:      0,
				Port:          0,
				FWMark:        0,
				SchedName:     "mh",
				Flags:         uint32(FlagPersistent + FlagHashed + FlagSourceHash),
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       netutils.ParseIPSloppy("1.2.3.4"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("1.2.3.4"),
				Protocol:  "",
				Port:      0,
				Scheduler: "mh",
				Flags:     ServiceFlags(FlagPersistent + FlagSourceHash),
				Timeout:   0,
			},
			false,
			"",
		},
		{
			libipvs.Service{
				Protocol:      unix.IPPROTO_SCTP,
				Port:          80,
				FWMark:        0,
				SchedName:     "",
				Flags:         uint32(FlagPersistent + FlagHashed),
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       nil,
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("0.0.0.0"),
				Protocol:  "SCTP",
				Port:      80,
				Scheduler: "",
				Flags:     ServiceFlags(FlagPersistent),
				Timeout:   0,
			},
			false,
			"",
		},
	}

	// 前两个用例缺少 FlagHashed，转换必须失败；其余用例同时核对各字段的映射结果。
	for i := range Tests {
		got, err := toVirtualServer(&Tests[i].ipvsService)
		if Tests[i].expectError && err == nil {
			t.Errorf("case: %d, expected error: %s, got nil", i, Tests[i].reason)
		}
		if !Tests[i].expectError && err != nil {
			t.Errorf("case: %d, unexpected error: %v", i, err)
		}
		// 只在转换成功时比较结构体，错误用例的零值结果没有比较意义。
		if got != nil {
			if !reflect.DeepEqual(*got, Tests[i].virtualServer) {
				t.Errorf("case: %d, got %#v, want %#v", i, *got, Tests[i].virtualServer)
			}
		}
	}
}

// Test_toIPVSService 验证 toIPVSService 把 VirtualServer 转换为 libipvs.Service，
// 包括协议号映射以及按 IPv4/IPv6 设置地址族与网络掩码。
func Test_toIPVSService(t *testing.T) {
	Tests := []struct {
		ipvsService   libipvs.Service
		virtualServer VirtualServer
	}{
		{
			libipvs.Service{
				Protocol:      unix.IPPROTO_TCP,
				Port:          80,
				FWMark:        0,
				SchedName:     "",
				Flags:         0,
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       netutils.ParseIPSloppy("0.0.0.0"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("0.0.0.0"),
				Protocol:  "TCP",
				Port:      80,
				Scheduler: "",
				Flags:     0,
				Timeout:   0,
			},
		},
		{
			libipvs.Service{
				Protocol:      unix.IPPROTO_UDP,
				Port:          33434,
				FWMark:        0,
				SchedName:     "wlc",
				Flags:         1234,
				Timeout:       100,
				Netmask:       128,
				AddressFamily: unix.AF_INET6,
				Address:       netutils.ParseIPSloppy("2012::beef"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("2012::beef"),
				Protocol:  "UDP",
				Port:      33434,
				Scheduler: "wlc",
				Flags:     1234,
				Timeout:   100,
			},
		},
		{
			libipvs.Service{
				Protocol:      0,
				Port:          0,
				FWMark:        0,
				SchedName:     "lc",
				Flags:         0,
				Timeout:       0,
				Netmask:       0xffffffff,
				AddressFamily: unix.AF_INET,
				Address:       netutils.ParseIPSloppy("1.2.3.4"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("1.2.3.4"),
				Protocol:  "",
				Port:      0,
				Scheduler: "lc",
				Flags:     0,
				Timeout:   0,
			},
		},
		{
			libipvs.Service{
				Protocol:      0,
				Port:          0,
				FWMark:        0,
				SchedName:     "wrr",
				Flags:         0,
				Timeout:       0,
				Netmask:       128,
				AddressFamily: unix.AF_INET6,
				Address:       netutils.ParseIPSloppy("::0"),
				PEName:        "",
			},
			VirtualServer{
				Address:   netutils.ParseIPSloppy("::0"),
				Protocol:  "",
				Port:      0,
				Scheduler: "wrr",
				Flags:     0,
				Timeout:   0,
			},
		},
	}

	// 期望值包含 Netmask/AddressFamily，防止转换时只关注地址和端口。
	for i := range Tests {
		got, err := toIPVSService(&Tests[i].virtualServer)
		if err != nil {
			t.Errorf("case: %d, unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(*got, Tests[i].ipvsService) {
			t.Errorf("case: %d - got %#v, want %#v", i, *got, Tests[i].ipvsService)
		}
	}
}

// Test_toRealServer 验证 toRealServer 把 libipvs.Destination 转换为 RealServer，
// 覆盖 IPv4 与 IPv6 后端地址。
func Test_toRealServer(t *testing.T) {
	Tests := []struct {
		ipvsDestination libipvs.Destination
		realServer      RealServer
	}{
		{
			libipvs.Destination{
				Port:            54321,
				ConnectionFlags: 0,
				Weight:          1,
				Address:         netutils.ParseIPSloppy("1.2.3.4"),
			},
			RealServer{
				Address: netutils.ParseIPSloppy("1.2.3.4"),
				Port:    54321,
				Weight:  1,
			},
		},
		{
			libipvs.Destination{
				Port:            53,
				ConnectionFlags: 0,
				Weight:          1,
				Address:         netutils.ParseIPSloppy("2002::cafe"),
			},
			RealServer{
				Address: netutils.ParseIPSloppy("2002::cafe"),
				Port:    53,
				Weight:  1,
			},
		},
	}
	// 转换不应修改端口、权重和地址，逐字段深度比较以避免遗漏。
	for i := range Tests {
		got, err := toRealServer(&Tests[i].ipvsDestination)
		if err != nil {
			t.Errorf("case %d unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(*got, Tests[i].realServer) {
			t.Errorf("case %d Failed to translate Destination - got %#v, want %#v", i, *got, Tests[i].realServer)
		}
	}
}

// Test_toIPVSDestination 验证 toIPVSDestination 把 RealServer 转换为 libipvs.Destination，
// 覆盖 IPv4 与 IPv6 后端地址。
func Test_toIPVSDestination(t *testing.T) {
	Tests := []struct {
		realServer      RealServer
		ipvsDestination libipvs.Destination
	}{
		{
			RealServer{
				Address: netutils.ParseIPSloppy("1.2.3.4"),
				Port:    54321,
				Weight:  1,
			},
			libipvs.Destination{
				Port:            54321,
				ConnectionFlags: 0,
				Weight:          1,
				Address:         netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			RealServer{
				Address: netutils.ParseIPSloppy("2002::cafe"),
				Port:    53,
				Weight:  1,
			},
			libipvs.Destination{
				Port:            53,
				ConnectionFlags: 0,
				Weight:          1,
				Address:         netutils.ParseIPSloppy("2002::cafe"),
			},
		},
	}
	// 反向转换的期望值与 toRealServer 用例对称，保证往返转换不丢字段。
	for i := range Tests {
		got, err := toIPVSDestination(&Tests[i].realServer)
		if err != nil {
			t.Errorf("case %d unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(*got, Tests[i].ipvsDestination) {
			t.Errorf("case %d failed to translate Destination - got %#v, want %#v", i, *got, Tests[i].ipvsDestination)
		}
	}
}

// Test_stringToProtocol 验证 stringToProtocol 把 TCP/UDP/ICMP/SCTP 名称映射为协议号，
// 其中 ICMP 及未知协议映射到 0。
func Test_stringToProtocol(t *testing.T) {
	tests := []string{
		"TCP", "UDP", "ICMP", "SCTP",
	}
	// ICMP 没有对应的 IPVS 协议号，实现返回 0，这里显式固定该行为。
	expected := []uint16{
		uint16(unix.IPPROTO_TCP), uint16(unix.IPPROTO_UDP), uint16(0), uint16(unix.IPPROTO_SCTP),
	}
	for i := range tests {
		got := stringToProtocol(tests[i])
		if got != expected[i] {
			t.Errorf("stringToProtocol() failed - got %#v, want %#v",
				got, expected[i])
		}
	}
}

// Test_protocolToString 验证 protocolToString 是 stringToProtocol 的逆映射：
// 协议 0 映射为空串，未知协议同样返回空串。
func Test_protocolToString(t *testing.T) {
	tests := []Protocol{
		unix.IPPROTO_TCP, unix.IPPROTO_UDP, Protocol(0), unix.IPPROTO_SCTP,
	}
	// 协议 0 代表未设置，逆映射为空串，与正向转换的约定保持一致。
	expected := []string{
		"TCP", "UDP", "", "SCTP",
	}
	for i := range tests {
		got := protocolToString(tests[i])
		if got != expected[i] {
			t.Errorf("protocolToString() failed - got %#v, want %#v",
				got, expected[i])
		}
	}
}
