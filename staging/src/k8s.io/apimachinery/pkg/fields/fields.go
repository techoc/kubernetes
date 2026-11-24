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

// Fields 将字段查询接口与具体存储方式解耦。
//
// Fields 抽象"字段名 -> 值"的只读视图。不同实现（如 Set、Selector）可以提供
// 相同的查询接口，而底层存储方式各异。
type Fields interface {
	// Has 判断指定字段是否存在。
	Has(field string) (exists bool)

	// Get 返回指定字段的值。
	Get(field string) (value string)
}

// Set 是 field:value 形式的映射，并实现 Fields 接口。
type Set map[string]string

// String 将集合中的所有字段转换为便于阅读的字符串，格式与 ParseSelector 接受的选择器格式一致。
//
// 排序是必须的：map 迭代顺序随机，不排序会导致日志和测试输出不稳定。
func (ls Set) String() string {
	selector := make([]string, 0, len(ls))
	for key, value := range ls {
		selector = append(selector, key+"="+value)
	}
	// 排序以保证输出稳定。
	sort.StringSlice(selector).Sort()
	return strings.Join(selector, ",")
}

// Has 判断映射中是否存在指定字段。
func (ls Set) Has(field string) bool {
	_, exists := ls[field]
	return exists
}

// Get 返回映射中指定字段对应的值。
func (ls Set) Get(field string) string {
	return ls[field]
}

// AsSelector 将字段集合转换为字段选择器。
//
// 生成的 Selector 要求所有字段精确匹配；空 Set 得到匹配全部的 Selector。
func (ls Set) AsSelector() Selector {
	return SelectorFromSet(ls)
}
