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

package sets

import (
	"cmp"
	"slices"
)

// Set 是同一类型元素的集合，底层用 map[comparable]struct{} 实现，以最小的内存开销获得 O(1) 查找。
type Set[T comparable] map[T]Empty

// cast 把底层 map 视图转换为 Set[T]，不复制数据。
// 之所以需要它，是因为部分 API 返回 map 而调用方期望 Set。
func cast[T comparable](s map[T]Empty) Set[T] { return s }

// New 从一组值构造 Set。
// 注意：items 为空时无法从参数推断类型参数 T，调用方必须显式指定，例如 New[string]()。
// 空参数时 Go 无法推断 T，必须写成 New[string]()。
func New[T comparable](items ...T) Set[T] {
	ss := make(Set[T], len(items))
	ss.Insert(items...)
	return ss
}

// KeySet 从 map[comparable]V 的键构造 Set。
// 若传入的不是 map（例如 nil 接口），range 会 panic。
func KeySet[T comparable, V any](theMap map[T]V) Set[T] {
	ret := make(Set[T], len(theMap))
	for keyValue := range theMap {
		ret.Insert(keyValue)
	}
	return ret
}

// Insert 向集合添加元素，并返回集合本身以支持链式调用。
func (s Set[T]) Insert(items ...T) Set[T] {
	for _, item := range items {
		s[item] = Empty{}
	}
	return s
}

// Insert 是非方法形式（便于在表达式中使用），行为与 Set.Insert 相同。
func Insert[T comparable](set Set[T], items ...T) Set[T] {
	return set.Insert(items...)
}

// Delete 从集合删除元素，并返回集合本身以支持链式调用。
func (s Set[T]) Delete(items ...T) Set[T] {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Clear 清空集合。
// 优先建议直接新建集合，但存在其他引用指向同一 map 时无法这么做（本方法就地清空）。
func (s Set[T]) Clear() Set[T] {
	clear(s)
	return s
}

// Has 判断元素是否在集合中。
func (s Set[T]) Has(item T) bool {
	_, contained := s[item]
	return contained
}

// HasAll 判断所有元素是否都在集合中；空参数返回 true。
func (s Set[T]) HasAll(items ...T) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny 判断是否至少有一个元素在集合中；空参数返回 false。
func (s Set[T]) HasAny(items ...T) bool {
	for _, item := range items {
		if s.Has(item) {
			return true
		}
	}
	return false
}

// Clone 返回当前集合的副本，修改副本不影响原集合。
func (s Set[T]) Clone() Set[T] {
	result := make(Set[T], len(s))
	for key := range s {
		result.Insert(key)
	}
	return result
}

// Difference 返回存在于 s1 但不在 s2 中的元素集合。
// 例如：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
func (s1 Set[T]) Difference(s2 Set[T]) Set[T] {
	result := New[T]()
	for key := range s1 {
		if !s2.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// SymmetricDifference 返回只属于其中一个集合、不在交集中的元素。
// 例如：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 Set[T]) SymmetricDifference(s2 Set[T]) Set[T] {
	return s1.Difference(s2).Union(s2.Difference(s1))
}

// Union 返回包含 s1 或 s2 中所有元素的新集合。
// 例如：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Set[T]) Union(s2 Set[T]) Set[T] {
	result := s1.Clone()
	for key := range s2 {
		result.Insert(key)
	}
	return result
}

// Intersection 返回同时存在于 s1 与 s2 中的元素集合。
// 例如：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
// 遍历较小的集合，可将查找次数降到两集合规模的较小值。
func (s1 Set[T]) Intersection(s2 Set[T]) Set[T] {
	var walk, other Set[T]
	result := New[T]()
	if s1.Len() < s2.Len() {
		walk = s1
		other = s2
	} else {
		walk = s2
		other = s1
	}
	for key := range walk {
		if other.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// IsSuperset 判断 s1 是否为 s2 的超集（包含 s2 的所有元素）。
func (s1 Set[T]) IsSuperset(s2 Set[T]) bool {
	for item := range s2 {
		if !s1.Has(item) {
			return false
		}
	}
	return true
}

// Equal 判断两个集合作为集合是否相等：元素成员完全相同，与顺序无关。
func (s1 Set[T]) Equal(s2 Set[T]) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

// List 返回排好序的 T 切片。
//
// 这是函数而非方法，因为并非所有可用于 Set 的类型都可排序（cmp.Ordered 限制）。
func List[T cmp.Ordered](s Set[T]) []T {
	res := s.UnsortedList()
	slices.Sort(res)
	return res
}

// UnsortedList 以任意顺序（map 迭代顺序）返回元素切片。
func (s Set[T]) UnsortedList() []T {
	res := make([]T, 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	return res
}

// PopAny 从集合中移除并返回任意一个元素；集合为空时返回零值与 false。
func (s Set[T]) PopAny() (T, bool) {
	for key := range s {
		s.Delete(key)
		return key, true
	}
	var zeroValue T
	return zeroValue, false
}

// Len 返回集合大小。
func (s Set[T]) Len() int {
	return len(s)
}
