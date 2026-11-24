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

// Int64 是 int64 集合，底层使用 map[int64]Empty 实现，以减少额外内存开销。
//
// Deprecated: 请改用泛型 Set。
// 推荐的新写法：
// s1 := Set[int64]{}
// s2 := New[int64]()
//
// 该类型是泛型 Set[int64] 的兼容包装；新代码应直接使用泛型版本。
type Int64 map[int64]Empty

// NewInt64 根据给定元素创建 Int64 集合。
func NewInt64(items ...int64) Int64 {
	return Int64(New[int64](items...))
}

// Int64KeySet 根据 map[int64]T 的键创建 Int64 集合，value 类型不影响结果。
// 该函数要求传入 map；若传入值不是 map，则会 panic。
func Int64KeySet[T any](theMap map[int64]T) Int64 {
	return Int64(KeySet(theMap))
}

// Insert 向集合添加元素。
func (s Int64) Insert(items ...int64) Int64 {
	return Int64(cast(s).Insert(items...))
}

// Delete 从集合中移除所有给定元素。
func (s Int64) Delete(items ...int64) Int64 {
	return Int64(cast(s).Delete(items...))
}

// Has 当且仅当集合包含指定元素时返回 true。
func (s Int64) Has(item int64) bool {
	return cast(s).Has(item)
}

// HasAll 当且仅当集合包含所有给定元素时返回 true。
func (s Int64) HasAll(items ...int64) bool {
	return cast(s).HasAll(items...)
}

// HasAny 当集合包含任一给定元素时返回 true。
func (s Int64) HasAny(items ...int64) bool {
	return cast(s).HasAny(items...)
}

// Clone 返回当前集合的独立副本。
func (s Int64) Clone() Int64 {
	return Int64(cast(s).Clone())
}

// Difference 返回存在于接收者集合、但不在 s2 中的元素集合（差集）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
func (s1 Int64) Difference(s2 Int64) Int64 {
	return Int64(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference 返回只属于两个集合之一、不属于交集的元素（对称差）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 Int64) SymmetricDifference(s2 Int64) Int64 {
	return Int64(cast(s1).SymmetricDifference(cast(s2)))
}

// Union 返回包含 s1 或 s2 中所有元素的新集合（并集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Int64) Union(s2 Int64) Int64 {
	return Int64(cast(s1).Union(cast(s2)))
}

// Intersection 返回同时属于 s1 和 s2 的元素集合（交集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 Int64) Intersection(s2 Int64) Int64 {
	return Int64(cast(s1).Intersection(cast(s2)))
}

// IsSuperset 当且仅当 s1 包含 s2 的所有元素时返回 true。
func (s1 Int64) IsSuperset(s2 Int64) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal 当且仅当两个集合的成员完全相同时返回 true。
// 两个集合相等意味着它们包含完全相同的元素。
// 即元素相同即可，元素顺序不影响比较结果。
func (s1 Int64) Equal(s2 Int64) bool {
	return cast(s1).Equal(cast(s2))
}

// List 返回按升序排列的 int64 切片。
func (s Int64) List() []int64 {
	return List(cast(s))
}

// UnsortedList 以 map 迭代顺序返回集合元素切片，不保证顺序。
func (s Int64) UnsortedList() []int64 {
	return cast(s).UnsortedList()
}

// PopAny 从集合中移除并返回任意一个元素。
func (s Int64) PopAny() (int64, bool) {
	return cast(s).PopAny()
}

// Len 返回集合大小。
func (s Int64) Len() int {
	return len(s)
}
