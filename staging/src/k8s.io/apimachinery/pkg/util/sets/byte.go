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

// Byte is a set of bytes, implemented via map[byte]struct{} for minimal memory consumption.
//
// Deprecated: use generic Set instead.
// new ways:
// s1 := Set[byte]{}
// s2 := New[byte]()
//
// Byte 是字节集合，用 map[byte]struct{} 实现以最小化内存占用。
// 已弃用：请改用泛型 Set，例如 Set[byte]{} 或 New[byte]()。
type Byte map[byte]Empty

// NewByte creates a Byte from a list of values.
//
// NewByte 用给定值列表创建 Byte 集合。
func NewByte(items ...byte) Byte {
	return Byte(New[byte](items...))
}

// ByteKeySet creates a Byte from a keys of a map[byte](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
//
// ByteKeySet 用 map[byte]T 的键创建 Byte 集合；传入的不是 map 时会 panic。
func ByteKeySet[T any](theMap map[byte]T) Byte {
	return Byte(KeySet(theMap))
}

// Insert adds items to the set.
//
// Insert 向集合添加元素，返回集合本身以便链式调用。
func (s Byte) Insert(items ...byte) Byte {
	return Byte(cast(s).Insert(items...))
}

// Delete removes all items from the set.
//
// Delete 从集合移除所有给定元素，返回集合本身。
func (s Byte) Delete(items ...byte) Byte {
	return Byte(cast(s).Delete(items...))
}

// Has returns true if and only if item is contained in the set.
//
// Has 当且仅当集合包含 item 时返回 true。
func (s Byte) Has(item byte) bool {
	return cast(s).Has(item)
}

// HasAll returns true if and only if all items are contained in the set.
//
// HasAll 当且仅当集合包含所有给定元素时返回 true。
func (s Byte) HasAll(items ...byte) bool {
	return cast(s).HasAll(items...)
}

// HasAny returns true if any items are contained in the set.
//
// HasAny 当集合包含任一给定元素时返回 true。
func (s Byte) HasAny(items ...byte) bool {
	return cast(s).HasAny(items...)
}

// Clone returns a new set which is a copy of the current set.
//
// Clone 返回当前集合的副本。
func (s Byte) Clone() Byte {
	return Byte(cast(s).Clone())
}

// Difference returns a set of objects that are not in s2.
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
//
// Difference 返回存在于 s1 但不在 s2 中的元素集合（差集），示例见上。
func (s1 Byte) Difference(s2 Byte) Byte {
	return Byte(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference returns a set of elements which are in either of the sets, but not in their intersection.
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
//
// SymmetricDifference 返回只属于其中一个集合的元素（对称差），示例见上。
func (s1 Byte) SymmetricDifference(s2 Byte) Byte {
	return Byte(cast(s1).SymmetricDifference(cast(s2)))
}

// Union returns a new set which includes items in either s1 or s2.
// For example:
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
//
// Union 返回包含 s1 或 s2 中元素的并集，示例见上。
func (s1 Byte) Union(s2 Byte) Byte {
	return Byte(cast(s1).Union(cast(s2)))
}

// Intersection returns a new set which includes the item in BOTH s1 and s2
// For example:
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
//
// Intersection 返回同时属于 s1 与 s2 的元素（交集），示例见上。
func (s1 Byte) Intersection(s2 Byte) Byte {
	return Byte(cast(s1).Intersection(cast(s2)))
}

// IsSuperset returns true if and only if s1 is a superset of s2.
//
// IsSuperset 当且仅当 s1 是 s2 的超集时返回 true。
func (s1 Byte) IsSuperset(s2 Byte) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal returns true if and only if s1 is equal (as a set) to s2.
// Two sets are equal if their membership is identical.
// (In practice, this means same elements, order doesn't matter)
//
// Equal 当且仅当 s1 与 s2 作为集合相等时返回 true：
// 两者成员完全相同（即元素相同，顺序无关）。
func (s1 Byte) Equal(s2 Byte) bool {
	return cast(s1).Equal(cast(s2))
}

// List returns the contents as a sorted byte slice.
//
// List 以已排序的 byte 切片返回集合内容。
func (s Byte) List() []byte {
	return List(cast(s))
}

// UnsortedList returns the slice with contents in random order.
//
// UnsortedList 以随机顺序返回集合内容的切片。
func (s Byte) UnsortedList() []byte {
	return cast(s).UnsortedList()
}

// PopAny returns a single element from the set.
//
// PopAny 从集合中移除并返回任意一个元素；集合为空时返回零值与 false。
func (s Byte) PopAny() (byte, bool) {
	return cast(s).PopAny()
}

// Len returns the size of the set.
//
// Len 返回集合大小。
func (s Byte) Len() int {
	return len(s)
}
