/*
Copyright 2018 The Kubernetes Authors.

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

// Package testing 提供 proxy util 包的测试辅助工具。
package testing

import "net"

// FakeNetwork 为测试目的实现 NetworkInterfacer 接口。
// 用内存中的假数据替代真实的系统网络接口查询，使测试不依赖运行环境的实际网卡配置。
type FakeNetwork struct {
	NetworkInterfaces []net.Interface
	// Address 的 key 是网络接口的名称。
	Address map[string][]net.Addr
}

// NewFakeNetwork 初始化一个空的 FakeNetwork。
func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		NetworkInterfaces: make([]net.Interface, 0),
		Address:           make(map[string][]net.Addr),
	}
}

// AddInterfaceAddr 为 FakeNetwork 添加一个网络接口及其关联的地址。
func (f *FakeNetwork) AddInterfaceAddr(intf *net.Interface, addrs []net.Addr) {
	f.NetworkInterfaces = append(f.NetworkInterfaces, *intf)
	f.Address[intf.Name] = addrs
}

// InterfaceAddrs 是 NetworkInterfacer 接口的一部分。
// 返回假网络中所有接口的地址，模拟真实接口枚举；测试可通过 AddInterfaceAddr 精确控制返回内容。
func (f *FakeNetwork) InterfaceAddrs() ([]net.Addr, error) {
	addrs := make([]net.Addr, 0)
	for _, value := range f.Address {
		addrs = append(addrs, value...)
	}
	return addrs, nil
}
