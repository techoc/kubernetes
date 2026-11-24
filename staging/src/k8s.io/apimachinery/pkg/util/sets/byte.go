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

// Byte 是 byte 集合，底层使用 map[byte]Empty 实现，以减少额外内存开销。
//
// Deprecated: 请改用泛型 Set。
// 推荐的新写法：
// s1 := Set[byte]{}
// s2 := New[byte]()
//
// Byte 是字节集合，用 map[byte]struct{} 实现以最小化内存占用。
// 已弃用：请改用泛型 Set，例如 Set[byte]{} 或 New[byte]()。
type Byte map[byte]Empty

// NewByte 根据给定元素创建 Byte 集合。
//
// NewByte 用给定值列表创建 Byte 集合。
func NewByte(items ...byte) Byte {
	return Byte(New[byte](items...))
}

// ByteKeySet 根据 map[byte]T 的键创建 Byte 集合，value 类型不影响结果。
// 该函数要求传入 map；若传入值不是 map，则会 panic。
//
// ByteKeySet 用 map[byte]T 的键创建 Byte 集合；传入的不是 map 时会 panic。
func ByteKeySet[T any](theMap map[byte]T) Byte {
	return Byte(KeySet(theMap))
}

// Insert 向集合添加元素。
//
// Insert 向集合添加元素，返回集合本身以便链式调用。
func (s Byte) Insert(items ...byte) Byte {
	return Byte(cast(s).Insert(items...))
}

// Delete 从集合中移除所有给定元素。
//
// Delete 从集合移除所有给定元素，返回集合本身。
func (s Byte) Delete(items ...byte) Byte {
	return Byte(cast(s).Delete(items...))
}

// Has 当且仅当集合包含指定元素时返回 true。
//
// Has 当且仅当集合包含 item 时返回 true。
func (s Byte) Has(item byte) bool {
	return cast(s).Has(item)
}

// HasAll 当且仅当集合包含所有给定元素时返回 true。
//
// HasAll 当且仅当集合包含所有给定元素时返回 true。
func (s Byte) HasAll(items ...byte) bool {
	return cast(s).HasAll(items...)
}

// HasAny 当集合包含任一给定元素时返回 true。
//
// HasAny 当集合包含任一给定元素时返回 true。
func (s Byte) HasAny(items ...byte) bool {
	return cast(s).HasAny(items...)
}

// Clone 返回当前集合的独立副本。
//
// Clone 返回当前集合的副本。
func (s Byte) Clone() Byte {
	return Byte(cast(s).Clone())
}

// Difference 返回存在于接收者集合、但不在 s2 中的元素集合（差集）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
//
// Difference 返回存在于 s1 但不在 s2 中的元素集合（差集），示例见上。
func (s1 Byte) Difference(s2 Byte) Byte {
	return Byte(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference 返回只属于两个集合之一、不属于交集的元素（对称差）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
//
// SymmetricDifference 返回只属于其中一个集合的元素（对称差），示例见上。
func (s1 Byte) SymmetricDifference(s2 Byte) Byte {
	return Byte(cast(s1).SymmetricDifference(cast(s2)))
}

// Union 返回包含 s1 或 s2 中所有元素的新集合（并集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
//
// Union 返回包含 s1 或 s2 中元素的并集，示例见上。
func (s1 Byte) Union(s2 Byte) Byte {
	return Byte(cast(s1).Union(cast(s2)))
}

// Intersection 返回同时属于 s1 和 s2 的元素集合（交集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
//
// Intersection 返回同时属于 s1 与 s2 的元素（交集），示例见上。
func (s1 Byte) Intersection(s2 Byte) Byte {
	return Byte(cast(s1).Intersection(cast(s2)))
}

// IsSuperset 当且仅当 s1 包含 s2 的所有元素时返回 true。
//
// IsSuperset 当且仅当 s1 是 s2 的超集时返回 true。
func (s1 Byte) IsSuperset(s2 Byte) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal 当且仅当两个集合的成员完全相同时返回 true。
// 两个集合相等意味着它们包含完全相同的元素。
// 即元素相同即可，元素顺序不影响比较结果。
//
// Equal 当且仅当 s1 与 s2 作为集合相等时返回 true：
// 两者成员完全相同（即元素相同，顺序无关）。
func (s1 Byte) Equal(s2 Byte) bool {
	return cast(s1).Equal(cast(s2))
}

// List 返回按升序排列的 byte 切片。
//
// List 以已排序的 byte 切片返回集合内容。
func (s Byte) List() []byte {
	return List(cast(s))
}

// UnsortedList 以 map 迭代顺序返回集合元素切片，不保证顺序。
//
// UnsortedList 以随机顺序返回集合内容的切片。
func (s Byte) UnsortedList() []byte {
	return cast(s).UnsortedList()
}

// PopAny 从集合中移除并返回任意一个元素。
//
// PopAny 从集合中移除并返回任意一个元素；集合为空时返回零值与 false。
func (s Byte) PopAny() (byte, bool) {
	return cast(s).PopAny()
}

// Len 返回集合大小。
//
// Len 返回集合大小。
func (s Byte) Len() int {
	return len(s)
}
