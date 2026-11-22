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

// Int 是 int 的集合，底层使用 map[int]struct{} 实现，以最小化内存占用。
//
// Deprecated: 请改用泛型 Set。
// 新的写法：
// s1 := Set[int]{}
// s2 := New[int]()
type Int map[int]Empty

// NewInt 由一组值创建 Int。
func NewInt(items ...int) Int {
	return Int(New[int](items...))
}

// IntKeySet 由 map[int](? extends interface{}) 的键创建 Int。
// 若传入的值实际不是 map，则会 panic。
func IntKeySet[T any](theMap map[int]T) Int {
	return Int(KeySet(theMap))
}

// Insert 向集合中添加元素，返回集合自身以便链式调用。
func (s Int) Insert(items ...int) Int {
	return Int(cast(s).Insert(items...))
}

// Delete 从集合中移除所有给定元素。
func (s Int) Delete(items ...int) Int {
	return Int(cast(s).Delete(items...))
}

// Has 当且仅当 item 存在于集合中时返回 true。
func (s Int) Has(item int) bool {
	return cast(s).Has(item)
}

// HasAll 当且仅当所有 items 都在集合中时返回 true。
func (s Int) HasAll(items ...int) bool {
	return cast(s).HasAll(items...)
}

// HasAny 当任意一个 item 存在于集合中时返回 true。
func (s Int) HasAny(items ...int) bool {
	return cast(s).HasAny(items...)
}

// Clone 返回当前集合的一个副本。
//
// 副本与原始集合的底层 map 相互独立，修改任一方都不会影响另一方。
func (s Int) Clone() Int {
	return Int(cast(s).Clone())
}

// Difference 返回存在于 s1 但不在 s2 中的元素集合。
// 例如：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
func (s1 Int) Difference(s2 Int) Int {
	return Int(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference 返回存在于两个集合之一、但不在其交集中的元素集合。
// 例如：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 Int) SymmetricDifference(s2 Int) Int {
	return Int(cast(s1).SymmetricDifference(cast(s2)))
}

// Union 返回包含 s1 或 s2 中元素的新集合。
// 例如：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Int) Union(s2 Int) Int {
	return Int(cast(s1).Union(cast(s2)))
}

// Intersection 返回同时存在于 s1 和 s2 中的元素组成的新集合。
// 例如：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 Int) Intersection(s2 Int) Int {
	return Int(cast(s1).Intersection(cast(s2)))
}

// IsSuperset 当且仅当 s1 是 s2 的超集时返回 true。
func (s1 Int) IsSuperset(s2 Int) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal 当且仅当 s1 与 s2 作为集合相等时返回 true。
// 两个集合相等指其成员完全相同（即元素相同，顺序无关）。
func (s1 Int) Equal(s2 Int) bool {
	return cast(s1).Equal(cast(s2))
}

// List 返回排序后的元素切片。
//
// 排序保证了输出稳定，便于比较与测试断言。
func (s Int) List() []int {
	return List(cast(s))
}

// UnsortedList 返回元素顺序随机的切片。
//
// 当调用方不关心顺序时使用它，可避免排序开销。
func (s Int) UnsortedList() []int {
	return cast(s).UnsortedList()
}

// PopAny 从集合中取出一个元素并返回；集合为空时第二个返回值为 false。
func (s Int) PopAny() (int, bool) {
	return cast(s).PopAny()
}

// Len 返回集合的大小。
func (s Int) Len() int {
	return len(s)
}
