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

// Int32 是 int32 集合，底层使用 map[int32]Empty 实现，以减少额外内存开销。
//
// Deprecated: 请改用泛型 Set。
// 推荐的新写法：
// s1 := Set[int32]{}
// s2 := New[int32]()
//
// 这是泛型化之前的具体类型，保留仅为兼容；所有方法都只是对泛型实现的
// 薄封装，行为以 Set[int32] 为准。
type Int32 map[int32]Empty

// NewInt32 根据给定元素创建 Int32 集合。
func NewInt32(items ...int32) Int32 {
	return Int32(New[int32](items...))
}

// Int32KeySet 根据 map[int32]T 的键创建 Int32 集合，value 类型不影响结果。
// 该函数要求传入 map；若传入值不是 map，则会 panic。
//
// 类型参数 T 允许任意 value 类型，实际只取 key，因此常用于把
// map[int32]SomeStruct 快速转成集合。
func Int32KeySet[T any](theMap map[int32]T) Int32 {
	return Int32(KeySet(theMap))
}

// Insert 向集合添加元素。
func (s Int32) Insert(items ...int32) Int32 {
	return Int32(cast(s).Insert(items...))
}

// Delete 从集合中移除所有给定元素。
func (s Int32) Delete(items ...int32) Int32 {
	return Int32(cast(s).Delete(items...))
}

// Has 当且仅当集合包含指定元素时返回 true。
func (s Int32) Has(item int32) bool {
	return cast(s).Has(item)
}

// HasAll 当且仅当集合包含所有给定元素时返回 true。
func (s Int32) HasAll(items ...int32) bool {
	return cast(s).HasAll(items...)
}

// HasAny 当集合包含任一给定元素时返回 true。
func (s Int32) HasAny(items ...int32) bool {
	return cast(s).HasAny(items...)
}

// Clone 返回当前集合的独立副本。
func (s Int32) Clone() Int32 {
	return Int32(cast(s).Clone())
}

// Difference 返回存在于接收者集合、但不在 s2 中的元素集合（差集）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
//
// 返回新集合，不修改 s1 或 s2。
func (s1 Int32) Difference(s2 Int32) Int32 {
	return Int32(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference 返回只属于两个集合之一、不属于交集的元素（对称差）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 Int32) SymmetricDifference(s2 Int32) Int32 {
	return Int32(cast(s1).SymmetricDifference(cast(s2)))
}

// Union 返回包含 s1 或 s2 中所有元素的新集合（并集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Int32) Union(s2 Int32) Int32 {
	return Int32(cast(s1).Union(cast(s2)))
}

// Intersection 返回同时属于 s1 和 s2 的元素集合（交集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 Int32) Intersection(s2 Int32) Int32 {
	return Int32(cast(s1).Intersection(cast(s2)))
}

// IsSuperset 当且仅当 s1 包含 s2 的所有元素时返回 true。
func (s1 Int32) IsSuperset(s2 Int32) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal 当且仅当两个集合的成员完全相同时返回 true。
// 两个集合相等意味着它们包含完全相同的元素。
// 即元素相同即可，元素顺序不影响比较结果。
func (s1 Int32) Equal(s2 Int32) bool {
	return cast(s1).Equal(cast(s2))
}

// List 返回按升序排列的 int32 切片。
//
// 排序保证输出稳定，适合用于比较与测试；若不需要顺序可用 UnsortedList。
func (s Int32) List() []int32 {
	return List(cast(s))
}

// UnsortedList 以 map 迭代顺序返回集合元素切片，不保证顺序。
func (s Int32) UnsortedList() []int32 {
	return cast(s).UnsortedList()
}

// PopAny 从集合中移除并返回任意一个元素。
//
// 返回值与“是否非空”，空集时返回零值和 false，调用方必须检查第二个返回值。
func (s Int32) PopAny() (int32, bool) {
	return cast(s).PopAny()
}

// Len 返回集合大小。
func (s Int32) Len() int {
	return len(s)
}
