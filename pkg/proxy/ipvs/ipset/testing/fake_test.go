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
	"k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
)

const testVersion = "v6.19"

// TestSetEntry 覆盖 fake ipset 的集合创建、条目增删查、Flush、列举与销毁全流程。
func TestSetEntry(t *testing.T) {
	fake := NewFake(testVersion)
	version, err := fake.GetVersion()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if version != testVersion {
		t.Errorf("Unexpected version mismatch, expected: %s, got: %s", testVersion, version)
	}
	// 创建 hash:ip,port 类型的 IPv4 集合；后续条目格式依集合类型而定。
	// 创建一个 ipset 集合
	set := &ipset.IPSet{
		Name:       "foo",
		SetType:    ipset.HashIPPort,
		HashFamily: ipset.ProtocolFamilyIPV4,
	}
	if err := fake.CreateSet(set, true); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// 加入两个条目，之后用 ListEntries 验证集合内容与顺序无关。
	// 向集合中加入两个条目
	err = fake.AddEntry("192.168.1.1,tcp:8080", set, true)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	err = fake.AddEntry("192.168.1.2,tcp:8081", set, true)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	entries, err := fake.ListEntries(set.Name)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	expectedEntries := sets.NewString("192.168.1.1,tcp:8080", "192.168.1.2,tcp:8081")
	if !expectedEntries.Equal(sets.NewString(entries...)) {
		t.Errorf("Unexpected entries mismatch, expected: %v, got: %v", expectedEntries, entries)
	}

	// TestEntry 既校验存在性也校验错误值，确保 fake 的查询语义与真实 ipset 一致。
	// 校验集合中的条目
	found, err := fake.TestEntry("192.168.1.1,tcp:8080", set.Name)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !found {
		t.Errorf("Unexpected entry 192.168.1.1,tcp:8080 not found")
	}

	found, err = fake.TestEntry("192.168.1.2,tcp:8081", set.Name)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !found {
		t.Errorf("Unexpected entry 192.168.1.2,tcp:8081 not found")
	}

	// 删除单个条目后，集合中只应剩下另一个条目。
	// 从指定集合中删除条目
	if err := fake.DelEntry("192.168.1.1,tcp:8080", set.Name); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	entries, err = fake.ListEntries(set.Name)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entries, got %d", len(entries))
	}
	expectedEntries = sets.NewString("192.168.1.2,tcp:8081")
	if !expectedEntries.Equal(sets.NewString(entries...)) {
		t.Errorf("Unexpected entries mismatch, expected: %v, got: %v", expectedEntries, entries)
	}

	// FlushSet 只清空条目、保留集合本身，这与 DestroySet 的行为不同。
	// 清空集合中的所有条目
	if err := fake.FlushSet(set.Name); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	entries, err = fake.ListEntries(set.Name)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d, entries: %v", len(entries), entries)
	}

	// 再建一个 IPv6 集合，验证 ListSets 能同时列出两个集合。
	// 创建另一个集合，验证多集合场景
	set2 := &ipset.IPSet{
		Name:       "bar",
		SetType:    ipset.HashIPPortIP,
		HashFamily: ipset.ProtocolFamilyIPV6,
	}
	if err := fake.CreateSet(set2, true); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	setList, err := fake.ListSets()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(setList) != 2 {
		t.Errorf("Expected 2 sets, got %d", len(setList))
	}
	expectedSets := sets.NewString("foo", "bar")
	if !expectedSets.Equal(sets.NewString(setList...)) {
		t.Errorf("Unexpected sets mismatch, expected: %v, got: %v", expectedSets, setList)
	}

	// 销毁单个集合时，集合本身和它的条目记录都必须被清除。
	// 销毁指定的集合
	if err := fake.DestroySet(set.Name); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if fake.Sets[set.Name] != nil {
		t.Errorf("Unexpected set: %v", fake.Sets[set.Name])
	}
	if fake.Entries[set.Name] != nil {
		t.Errorf("Unexpected entries: %v", fake.Entries[set.Name])
	}

	// 销毁所有集合后，两个内部 map 都应为空。
	// 销毁所有集合
	if err := fake.DestroyAllSets(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(fake.Sets) != 0 {
		t.Errorf("Expected 0 sets, got %d, sets: %v", len(fake.Sets), fake.Sets)
	}
	if len(fake.Entries) != 0 {
		t.Errorf("Expected 0 entries, got %d, entries: %v", len(fake.Entries), fake.Entries)
	}
}

// TODO: Test ignoreExistErr=false
