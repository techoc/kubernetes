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
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
)

// FakeIPSet 是 ipset.Interface 的内存模拟实现，用 map 代替真实 ipset 命令，
// 使调用方无需依赖内核 ipset 工具即可完成测试。
type FakeIPSet struct {
	// Version 是模拟的 ipset 工具版本，由 GetVersion 原样返回。
	Version string
	// Sets 保存已创建的 ipset，键为 ipset 名称。
	Sets map[string]*ipset.IPSet
	// Entries 保存各 ipset 中的条目，键为条目所属 ipset 的名称。
	Entries map[string]sets.String
}

// NewFake 创建并初始化一个 FakeIPSet；两个 map 必须初始化，
// 否则后续写入会直接 panic。
func NewFake(version string) *FakeIPSet {
	return &FakeIPSet{
		Version: version,
		Sets:    make(map[string]*ipset.IPSet),
		Entries: make(map[string]sets.String),
	}
}

// GetVersion 返回构造时设定的模拟版本号，是 ipset.Interface 的一部分。
func (f *FakeIPSet) GetVersion() (string, error) {
	return f.Version, nil
}

// FlushSet 删除指定 ipset 中的全部条目但保留集合本身，是 ipset.Interface 的一部分。
func (f *FakeIPSet) FlushSet(set string) error {
	if f.Entries == nil {
		return fmt.Errorf("entries map can't be nil")
	}

	// 用 PopAny 原地清空而非替换整个 map，
	// 这样持有同一 map 引用的其他对象也能观察到清空结果。
	for true {
		if _, has := f.Entries[set].PopAny(); has {
			continue
		}
		break
	}
	return nil
}

// DestroySet 同时删除指定 ipset 的条目记录和集合本身，是 ipset.Interface 的一部分。
func (f *FakeIPSet) DestroySet(set string) error {
	delete(f.Sets, set)
	delete(f.Entries, set)
	return nil
}

// DestroyAllSets 清空所有集合，是 ipset.Interface 的一部分。
// 这里将 map 置为 nil 而非逐项删除：后续操作应当因 nil map 而报错，
// 对应真实场景中"ipset 已全部销毁"的失败语义。
func (f *FakeIPSet) DestroyAllSets() error {
	f.Sets = nil
	f.Entries = nil
	return nil
}

// CreateSet 创建一个 ipset 并初始化其条目集合，是 ipset.Interface 的一部分。
// ignoreExistErr 为 true 时，同名集合已存在直接视为成功。
func (f *FakeIPSet) CreateSet(set *ipset.IPSet, ignoreExistErr bool) error {
	if f.Sets[set.Name] != nil {
		if !ignoreExistErr {
			// 同名集合已存在，除非调用方显式忽略，否则按内核行为报错。
			return fmt.Errorf("Set cannot be created: set with the same name already exists")
		}
		return nil
	}
	f.Sets[set.Name] = set
	// 必须同步创建空的条目集合：AddEntry 等方法直接向该 map 写入，
	// 未初始化会导致向 nil map 写入而 panic。
	f.Entries[set.Name] = sets.NewString()
	return nil
}

// AddEntry 向指定 ipset 添加一个条目，是 ipset.Interface 的一部分。
// ignoreExistErr 为 true 时，条目已存在视为成功，对应内核的 -exist 语义。
func (f *FakeIPSet) AddEntry(entry string, set *ipset.IPSet, ignoreExistErr bool) error {
	if f.Entries[set.Name].Has(entry) {
		if !ignoreExistErr {
			// 条目重复且调用方未允许忽略，按内核行为报错。
			return fmt.Errorf("Element cannot be added to the set: it's already added")
		}
		return nil
	}
	f.Entries[set.Name].Insert(entry)
	return nil
}

// DelEntry 从指定 ipset 删除一个条目，是 ipset.Interface 的一部分。
func (f *FakeIPSet) DelEntry(entry string, set string) error {
	if f.Entries == nil {
		return fmt.Errorf("entries map can't be nil")
	}
	f.Entries[set].Delete(entry)
	return nil
}

// TestEntry 判断条目是否存在于指定 ipset 中，是 ipset.Interface 的一部分。
func (f *FakeIPSet) TestEntry(entry string, set string) (bool, error) {
	if f.Entries == nil {
		return false, fmt.Errorf("entries map can't be nil")
	}
	found := f.Entries[set].Has(entry)
	return found, nil
}

// ListEntries 列出指定 ipset 中的全部条目，是 ipset.Interface 的一部分。
// 返回顺序不确定，调用方不应依赖列表顺序。
func (f *FakeIPSet) ListEntries(set string) ([]string, error) {
	if f.Entries == nil {
		return nil, fmt.Errorf("entries map can't be nil")
	}
	return f.Entries[set].UnsortedList(), nil
}

// ListSets 列出当前所有 ipset 的名称，是 ipset.Interface 的一部分。
func (f *FakeIPSet) ListSets() ([]string, error) {
	res := []string{}
	for set := range f.Sets {
		res = append(res, set)
	}
	return res, nil
}

// 编译期断言：确保 FakeIPSet 始终实现 ipset.Interface，
// 接口新增方法而此处未跟上时会直接编译失败。
var _ = ipset.Interface(&FakeIPSet{})
