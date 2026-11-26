/*
Copyright 2022 The Kubernetes Authors.

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

package common

import (
	"fmt"
	"strings"
)

// MapList 为 x-kubernetes-list-type=map 的列表提供按 key 查找第一个匹配元素的能力。
type MapList interface {
	// Get 针对所有 x-kubernetes-list-map-keys 返回第一个具有给定 key 的元素；若传入对象本身不是合法的 MapList 元素，则返回 nil。
	// 缺少任一 key 或 key 值不是标量时，同样返回 nil。
	Get(interface{}) interface{}
}

// keyStrategy 负责把对象转换为其在 map 列表中的复合键。
type keyStrategy interface {
	// CompositeKeyFor 尽力为给定对象生成复合键，并返回一个布尔值表示是否成功生成了 key。
	// 无法生成 key（缺字段或值不是标量）时返回 ok=false。
	CompositeKeyFor(map[string]interface{}) (interface{}, bool)
}

// singleKeyStrategy 是恰好只有一个 key 的关联列表的更省开销的实现：单 key 列表无需构造复合字符串，直接取值比较即可。
type singleKeyStrategy struct {
	key string
}

// CompositeKeyFor 直接返回唯一 key 的值；仅接受 bool/float64/int64/string 等标量。
func (ks *singleKeyStrategy) CompositeKeyFor(obj map[string]interface{}) (interface{}, bool) {
	v, ok := obj[ks.key]
	if !ok {
		return nil, false
	}

	switch v.(type) {
	// 只有标量才能安全地用作 map 键。
	case bool, float64, int64, string:
		return v, true
	default:
		return nil, false // non-scalar
	}
}

// 多 key 列表需要把各 key 值编码成一个可比较的复合键。
type multiKeyStrategy struct {
	sts Schema
}

// CompositeKeyFor 按 schema 声明的顺序把各 key 值编码为字符串；任一 key 缺失或非标量即失败。
func (ks *multiKeyStrategy) CompositeKeyFor(obj map[string]interface{}) (interface{}, bool) {
	const keyDelimiter = "\x00" // 0 byte should never appear in the composite key except as delimiter

	// 用 NUL 作分隔符，避免不同 key 值组合拼出相同字符串（前缀歧义）。
	var delimited strings.Builder
	for _, key := range ks.sts.XListMapKeys() {
		v, ok := obj[key]
		if !ok {
			return nil, false
		}

		switch v.(type) {
		case bool:
			fmt.Fprintf(&delimited, keyDelimiter+"%t", v)
		case float64:
			fmt.Fprintf(&delimited, keyDelimiter+"%f", v)
		case int64:
			fmt.Fprintf(&delimited, keyDelimiter+"%d", v)
		case string:
			fmt.Fprintf(&delimited, keyDelimiter+"%q", v)
		default:
			return nil, false // values must be scalars
		}
	}
	return delimited.String(), true
}

// emptyMapList 是一个不含任何元素的 MapList，用于非 map 列表或空列表，始终查不到元素。
type emptyMapList struct{}

// Get 对空列表始终返回 nil。
func (emptyMapList) Get(interface{}) interface{} {
	return nil
}

// mapListImpl 惰性建立 key 索引：首次查询时才为未索引项计算 key，
// 避免对只需少量查找的场景做全量预处理。
type mapListImpl struct {
	sts Schema
	ks  keyStrategy
	// keyedItems 缓存已计算 key 的元素。
	keyedItems map[interface{}]interface{}
	// unkeyedItems 保存尚未计算 key 的元素。
	unkeyedItems []interface{}
}

// Get 返回与给定对象复合键相同的第一个元素；必要时推进惰性索引。
func (a *mapListImpl) Get(obj interface{}) interface{} {
	mobj, ok := obj.(map[string]interface{})
	if !ok {
		// 非对象元素不可能成为 map 列表元素。
		return nil
	}

	key, ok := a.ks.CompositeKeyFor(mobj)
	if !ok {
		// 缺少 key 或 key 非标量，视为无效元素。
		return nil
	}
	if match, ok := a.keyedItems[key]; ok {
		// 命中已建立的索引。
		return match
	}
	// 逐个索引剩余元素，找到目标即可短路；未处理部分继续保持惰性。
	for len(a.unkeyedItems) > 0 {
		// 出队一个无 key 的条目
		item := a.unkeyedItems[0]
		a.unkeyedItems = a.unkeyedItems[1:]

		// 为该条目计算 key
		mitem, ok := item.(map[string]interface{})
		if !ok {
			// 跳过非法元素，与 MakeMapList 接受宽松输入的行为一致。
			continue
		}
		itemKey, ok := a.ks.CompositeKeyFor(mitem)
		if !ok {
			// 没有有效 key 的元素不进入索引。
			continue
		}
		if _, exists := a.keyedItems[itemKey]; !exists {
			// 重复 key 保留先出现者，符合 Get 返回“第一个元素”的语义。
			a.keyedItems[itemKey] = mitem
		}

		// 若匹配则短路返回
		if itemKey == key {
			return mitem
		}
	}

	return nil
}

// makeKeyStrategy 根据 key 数量选择单 key 或复合 key 策略。
func makeKeyStrategy(sts Schema) keyStrategy {
	listMapKeys := sts.XListMapKeys()
	if len(listMapKeys) == 1 {
		key := listMapKeys[0]
		// 单 key 直接取值比较，避免字符串编码开销。
		return &singleKeyStrategy{
			key: key,
		}
	}

	// 多 key 需要按 schema 中声明的顺序编码。
	return &multiKeyStrategy{
		sts: sts,
	}
}

// MakeMapList 为 map 类型数组返回可查询的 MapList；schema 不匹配或列表为空时返回空实现。
func MakeMapList(sts Schema, items []interface{}) (rv MapList) {
	if sts.Type() != "array" || sts.XListType() != "map" || len(sts.XListMapKeys()) == 0 || len(items) == 0 {
		// 没有按 key 查找语义（或没有元素）时返回空实现，调用方无需判空。
		return emptyMapList{}
	}
	ks := makeKeyStrategy(sts)
	return &mapListImpl{
		sts:          sts,
		ks:           ks,
		keyedItems:   map[interface{}]interface{}{},
		unkeyedItems: items,
	}
}
