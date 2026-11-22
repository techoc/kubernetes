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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/proxy/ipvs"
)

// TestSetGetLocalAddresses 只覆盖 fake 自身的地址查询逻辑。
// （不确定这个测试是否有价值，因为它只测试 fake 的实现。）
func TestSetGetLocalAddresses(t *testing.T) {
	fake := NewFakeNetlinkHandle(false)
	// 编译期断言 fake 满足 NetLinkHandle 接口；若接口新增方法而此处未跟进会编译失败。
	_ = ipvs.NetLinkHandle(fake) // Ensure that the interface is honored
	fake.SetLocalAddresses("eth0", "1.2.3.4")
	var expected, addr sets.Set[string]
	expected = sets.New("1.2.3.4")
	addr, _ = fake.GetLocalAddresses("eth0")
	if !addr.Equal(expected) {
		t.Errorf("Unexpected mismatch, expected: %v, got: %v", expected, addr)
	}
	// 全量查询应与单网卡查询一致：目前只设置了一张网卡。
	addr, _ = fake.GetAllLocalAddresses()
	if !addr.Equal(expected) {
		t.Errorf("Unexpected mismatch, expected: %v, got: %v", expected, addr)
	}
	// 回环地址会被 isValidForSet 过滤，设置后查询结果仍应为空。
	fake.SetLocalAddresses("lo", "127.0.0.1")
	expected = nil
	addr, _ = fake.GetLocalAddresses("lo")
	if !addr.Equal(expected) {
		t.Errorf("Unexpected mismatch, expected: %v, got: %v", expected, addr)
	}
	// kube-ipvs0 上的两个地址都会进入全量结果。
	fake.SetLocalAddresses("kube-ipvs0", "1.2.3.4", "4.3.2.1")
	addr, _ = fake.GetAllLocalAddresses()
	expected = sets.New("1.2.3.4", "4.3.2.1")
	if !addr.Equal(expected) {
		t.Errorf("Unexpected mismatch, expected: %v, got: %v", expected, addr)
	}
	// 排除 kube-ipvs0 时只保留 eth0 的地址；
	// 1.2.3.4 在两张网卡上重复出现，此断言验证了按网卡名而非按地址排除的行为。
	addr, _ = fake.GetAllLocalAddressesExcept("kube-ipvs0")
	expected = sets.New("1.2.3.4")
	if !addr.Equal(expected) {
		t.Errorf("Unexpected mismatch, expected: %v, got: %v", expected, addr)
	}
}
