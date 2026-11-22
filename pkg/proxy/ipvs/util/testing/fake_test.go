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

package testing

import (
	"testing"

	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	netutils "k8s.io/utils/net"
)

// TestVirtualServer 验证 fake IPVS 对虚拟服务器的增、查、改、删及 Flush 行为
func TestVirtualServer(t *testing.T) {
	// 初始化一个空的 fake ipvs 缓存
	fake := NewFake()
	// 添加一个 IPv4 虚拟服务器
	vs1 := &utilipvs.VirtualServer{
		Address:  netutils.ParseIPSloppy("1.2.3.4"),
		Port:     uint16(80),
		Protocol: string("TCP"),
		Flags:    utilipvs.FlagHashed,
	}
	err := fake.AddVirtualServer(vs1)
	if err != nil {
		t.Errorf("Fail to add virtual server, error: %v", err)
	}
	// 按同一 Key 查询刚添加的虚拟服务器
	got1, err := fake.GetVirtualServer(vs1)
	if err != nil {
		t.Errorf("Fail to get virtual server, error: %v", err)
	}
	if !vs1.Equal(got1) {
		t.Errorf("Expect virtual server: %v, got: %v", vs1, got1)
	}
	// 更新虚拟服务器的 Flags，Key 保持不变
	vs12 := &utilipvs.VirtualServer{
		Address:  netutils.ParseIPSloppy("1.2.3.4"),
		Port:     uint16(80),
		Protocol: string("TCP"),
		Flags:    utilipvs.FlagPersistent,
	}
	err = fake.UpdateVirtualServer(vs12)
	if err != nil {
		t.Errorf("Fail to update virtual server, error: %v", err)
	}
	// 用原 Key 查询，确认取回的是更新后的内容
	got12, err := fake.GetVirtualServer(vs1)
	if err != nil {
		t.Errorf("Fail to get virtual server, error: %v", err)
	}
	if !got12.Equal(vs12) {
		t.Errorf("Expect virtual server: %v, got: %v", vs12, got12)
	}
	// 再添加一个 IPv6/UDP 虚拟服务器
	vs2 := &utilipvs.VirtualServer{
		Address:  netutils.ParseIPSloppy("10::40"),
		Port:     uint16(8080),
		Protocol: string("UDP"),
	}
	err = fake.AddVirtualServer(vs2)
	if err != nil {
		t.Errorf("Unexpected error when add virtual server, error: %v", err)
	}
	// 继续添加同 IP 不同端口的 SCTP 虚拟服务器
	vs3 := &utilipvs.VirtualServer{
		Address:  netutils.ParseIPSloppy("10::40"),
		Port:     uint16(7777),
		Protocol: string("SCTP"),
	}
	err = fake.AddVirtualServer(vs3)
	if err != nil {
		t.Errorf("Unexpected error when add virtual server, error: %v", err)
	}
	// 列出全部虚拟服务器，验证 IPv4/IPv6 多个服务都已注册
	list, err := fake.GetVirtualServers()
	if err != nil {
		t.Errorf("Fail to list virtual servers, error: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("Expect 2 virtual servers, got: %d", len(list))
	}
	// 删除其中一个虚拟服务器
	err = fake.DeleteVirtualServer(vs1)
	if err != nil {
		t.Errorf("Fail to delete virtual server: %v, error: %v", vs1, err)
	}
	// 忽略查询错误，只关心删除后结果应为 nil
	got, _ := fake.GetVirtualServer(vs1)
	if got != nil {
		t.Errorf("Expect nil, got: %v", got)
	}
	// Flush 应清空所有虚拟服务器
	err = fake.Flush()
	if err != nil {
		t.Errorf("Fail to flush virtual servers, error: %v", err)
	}
	// 再次列出，确认 Flush 后缓存为空
	list, err = fake.GetVirtualServers()
	if err != nil {
		t.Errorf("Fail to list virtual servers, error: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("Expect 0 virtual servers, got: %d", len(list))
	}
}

// TestRealServer 验证 fake IPVS 对真实服务器的增删行为，包括任意位置删除及错误路径
func TestRealServer(t *testing.T) {
	// 初始化一个空的 fake ipvs 缓存
	fake := NewFake()
	// 先添加一个虚拟服务器作为真实服务器的宿主
	vs := &utilipvs.VirtualServer{
		Address:  netutils.ParseIPSloppy("10.20.30.40"),
		Port:     uint16(80),
		Protocol: string("TCP"),
	}
	rss := []*utilipvs.RealServer{
		{Address: netutils.ParseIPSloppy("172.16.2.1"), Port: 8080, Weight: 1},
		{Address: netutils.ParseIPSloppy("172.16.2.2"), Port: 8080, Weight: 2},
		{Address: netutils.ParseIPSloppy("172.16.2.3"), Port: 8080, Weight: 3},
	}
	err := fake.AddVirtualServer(vs)
	if err != nil {
		t.Errorf("Fail to add virtual server, error: %v", err)
	}
	// 依次添加三个真实服务器
	for i := range rss {
		if err = fake.AddRealServer(vs, rss[i]); err != nil {
			t.Errorf("Fail to add real server, error: %v", err)
		}
	}
	// 逐个删除真实服务器，确保列表中任意位置的元素都能被正确删除
	rssLen := len(rss)
	for i := range rss {
		// 每轮都重新获取列表，因为上一轮删除后可能已重新添加
		list, err := fake.GetRealServers(vs)
		if err != nil {
			t.Errorf("Fail to get real servers of the virtual server, error: %v", err)
		}
		if len(list) != rssLen {
			t.Errorf("Expect %d virtual servers, got: %d", len(rss), len(list))
		}
		rsToDel := list[i]
		if err = fake.DeleteRealServer(vs, rsToDel); err != nil {
			t.Errorf("Fail to delete real server of the virtual server, error: %v", err)
		} else {
			dests, err := fake.GetRealServers(vs)
			if err != nil {
				t.Errorf("Fail to get real servers of the virtual server, error: %v", err)
			}
			for _, dest := range dests {
				if toRealServerKey(dest).String() == toRealServerKey(rsToDel).String() {
					t.Errorf("Expect real server %q be deleted.", rsToDel.String())
				}
			}
			if err = fake.AddRealServer(vs, rsToDel); err != nil {
				t.Errorf("Fail to add real server, error: %v", err)
			}
		}
	}
	// 删除不存在的真实服务器应返回错误
	rs := &utilipvs.RealServer{
		Address: netutils.ParseIPSloppy("172.16.2.4"),
		Port:    uint16(8080),
		Weight:  1,
	}
	if err = fake.DeleteRealServer(vs, rs); err == nil {
		t.Errorf("Delete real server that not exist, Expect error, got nil")
	}
	// 删除虚拟服务器后，其下的真实服务器也应随之不可访问
	err = fake.DeleteVirtualServer(vs)
	if err != nil {
		t.Errorf("Fail to delete virtual server, error: %v", err)
	}
	_, err = fake.GetRealServers(vs)
	if err == nil {
		t.Errorf("Expect error, got nil")
	}
}
