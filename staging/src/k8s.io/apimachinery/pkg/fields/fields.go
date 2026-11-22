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

package fields

import (
	"sort"
	"strings"
)

// Fields allows you to present fields independently from their storage.
//
// Fields 抽象"字段名 -> 值"的只读视图。不同实现（如 Set、Selector）可以提供
// 相同的查询接口，而底层存储方式各异。
type Fields interface {
	// Has returns whether the provided field exists.
	Has(field string) (exists bool)

	// Get returns the value for the provided field.
	Get(field string) (value string)
}

// Set is a map of field:value. It implements Fields.
type Set map[string]string

// String returns all fields listed as a human readable string.
// Conveniently, exactly the format that ParseSelector takes.
//
// 排序是必须的：map 迭代顺序随机，不排序会导致日志和测试输出不稳定。
func (ls Set) String() string {
	selector := make([]string, 0, len(ls))
	for key, value := range ls {
		selector = append(selector, key+"="+value)
	}
	// Sort for determinism.
	sort.StringSlice(selector).Sort()
	return strings.Join(selector, ",")
}

// Has returns whether the provided field exists in the map.
func (ls Set) Has(field string) bool {
	_, exists := ls[field]
	return exists
}

// Get returns the value in the map for the provided field.
func (ls Set) Get(field string) string {
	return ls[field]
}

// AsSelector converts fields into a selectors.
//
// 生成的 Selector 要求所有字段精确匹配；空 Set 得到匹配全部的 Selector。
func (ls Set) AsSelector() Selector {
	return SelectorFromSet(ls)
}
