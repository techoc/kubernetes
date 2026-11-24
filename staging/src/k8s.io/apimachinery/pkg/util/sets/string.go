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

// String 是字符串集合，底层使用 map[string]Empty 实现，以减少额外内存开销。
//
// Deprecated: 请改用泛型 Set。
// 推荐的新写法：
// s1 := Set[string]{}
// s2 := New[string]()
//
// 中文说明：仅为兼容保留；新代码应直接使用泛型 Set[string]，本类型在
// 每次操作时都要做 map 类型转换。
type String map[string]Empty

// NewString 根据给定元素创建 String 集合。
func NewString(items ...string) String {
	return String(New[string](items...))
}

// StringKeySet 根据 map[string]T 的键创建 String 集合，value 类型不影响结果。
// 该函数要求传入 map；若传入值不是 map，则会 panic。
// 中文说明：用泛型约束把“必须是 map[string]T”的检查前移到编译期，
// 因此函数体内的 panic 实际上不可达。
func StringKeySet[T any](theMap map[string]T) String {
	return String(KeySet(theMap))
}

// Insert 向集合添加元素。
func (s String) Insert(items ...string) String {
	return String(cast(s).Insert(items...))
}

// Delete 从集合中移除所有给定元素。
func (s String) Delete(items ...string) String {
	return String(cast(s).Delete(items...))
}

// Has 当且仅当集合包含指定元素时返回 true。
func (s String) Has(item string) bool {
	return cast(s).Has(item)
}

// HasAll 当且仅当集合包含所有给定元素时返回 true。
func (s String) HasAll(items ...string) bool {
	return cast(s).HasAll(items...)
}

// HasAny 当集合包含任一给定元素时返回 true。
func (s String) HasAny(items ...string) bool {
	return cast(s).HasAny(items...)
}

// Clone 返回当前集合的独立副本。
func (s String) Clone() String {
	return String(cast(s).Clone())
}

// Difference 返回存在于接收者集合、但不在 s2 中的元素集合（差集）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
//
// 中文说明：结果是一个新集合，不修改 s1 或 s2。
func (s1 String) Difference(s2 String) String {
	return String(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference 返回只属于两个集合之一、不属于交集的元素（对称差）。
// 示例：
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 String) SymmetricDifference(s2 String) String {
	return String(cast(s1).SymmetricDifference(cast(s2)))
}

// Union 返回包含 s1 或 s2 中所有元素的新集合（并集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 String) Union(s2 String) String {
	return String(cast(s1).Union(cast(s2)))
}

// Intersection 返回同时属于 s1 和 s2 的元素集合（交集）。
// 示例：
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 String) Intersection(s2 String) String {
	return String(cast(s1).Intersection(cast(s2)))
}

// IsSuperset 当且仅当 s1 包含 s2 的所有元素时返回 true。
func (s1 String) IsSuperset(s2 String) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal 当且仅当两个集合的成员完全相同时返回 true。
// 两个集合相等意味着它们包含完全相同的元素。
// 即元素相同即可，元素顺序不影响比较结果。
func (s1 String) Equal(s2 String) bool {
	return cast(s1).Equal(cast(s2))
}

// List 返回按升序排列的 string 切片。
// 中文说明：排序保证输出确定，适合用于生成需要稳定比较的结果。
func (s String) List() []string {
	return List(cast(s))
}

// UnsortedList 以 map 迭代顺序返回集合元素切片，不保证顺序。
func (s String) UnsortedList() []string {
	return cast(s).UnsortedList()
}

// PopAny 从集合中移除并返回任意一个元素。
// 中文说明：结果不确定（map 遍历顺序），因此只在“任意一个即可”的场景使用；
// 会修改集合。
func (s String) PopAny() (string, bool) {
	return cast(s).PopAny()
}

// Len 返回集合大小。
func (s String) Len() int {
	return len(s)
}
