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

// String is a set of strings, implemented via map[string]struct{} for minimal memory consumption.
//
// Deprecated: use generic Set instead.
// new ways:
// s1 := Set[string]{}
// s2 := New[string]()
//
// 中文说明：仅为兼容保留；新代码应直接使用泛型 Set[string]，本类型在
// 每次操作时都要做 map 类型转换。
type String map[string]Empty

// NewString creates a String from a list of values.
func NewString(items ...string) String {
	return String(New[string](items...))
}

// StringKeySet creates a String from a keys of a map[string](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
// 中文说明：用泛型约束把“必须是 map[string]T”的检查前移到编译期，
// 因此函数体内的 panic 实际上不可达。
func StringKeySet[T any](theMap map[string]T) String {
	return String(KeySet(theMap))
}

// Insert adds items to the set.
func (s String) Insert(items ...string) String {
	return String(cast(s).Insert(items...))
}

// Delete removes all items from the set.
func (s String) Delete(items ...string) String {
	return String(cast(s).Delete(items...))
}

// Has returns true if and only if item is contained in the set.
func (s String) Has(item string) bool {
	return cast(s).Has(item)
}

// HasAll returns true if and only if all items are contained in the set.
func (s String) HasAll(items ...string) bool {
	return cast(s).HasAll(items...)
}

// HasAny returns true if any items are contained in the set.
func (s String) HasAny(items ...string) bool {
	return cast(s).HasAny(items...)
}

// Clone returns a new set which is a copy of the current set.
func (s String) Clone() String {
	return String(cast(s).Clone())
}

// Difference returns a set of objects that are not in s2.
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
//
// 中文说明：结果是一个新集合，不修改 s1 或 s2。
func (s1 String) Difference(s2 String) String {
	return String(cast(s1).Difference(cast(s2)))
}

// SymmetricDifference returns a set of elements which are in either of the sets, but not in their intersection.
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.SymmetricDifference(s2) = {a3, a4, a5}
// s2.SymmetricDifference(s1) = {a3, a4, a5}
func (s1 String) SymmetricDifference(s2 String) String {
	return String(cast(s1).SymmetricDifference(cast(s2)))
}

// Union returns a new set which includes items in either s1 or s2.
// For example:
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 String) Union(s2 String) String {
	return String(cast(s1).Union(cast(s2)))
}

// Intersection returns a new set which includes the item in BOTH s1 and s2
// For example:
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 String) Intersection(s2 String) String {
	return String(cast(s1).Intersection(cast(s2)))
}

// IsSuperset returns true if and only if s1 is a superset of s2.
func (s1 String) IsSuperset(s2 String) bool {
	return cast(s1).IsSuperset(cast(s2))
}

// Equal returns true if and only if s1 is equal (as a set) to s2.
// Two sets are equal if their membership is identical.
// (In practice, this means same elements, order doesn't matter)
func (s1 String) Equal(s2 String) bool {
	return cast(s1).Equal(cast(s2))
}

// List returns the contents as a sorted string slice.
// 中文说明：排序保证输出确定，适合用于生成需要稳定比较的结果。
func (s String) List() []string {
	return List(cast(s))
}

// UnsortedList returns the slice with contents in random order.
func (s String) UnsortedList() []string {
	return cast(s).UnsortedList()
}

// PopAny returns a single element from the set.
// 中文说明：结果不确定（map 遍历顺序），因此只在“任意一个即可”的场景使用；
// 会修改集合。
func (s String) PopAny() (string, bool) {
	return cast(s).PopAny()
}

// Len returns the size of the set.
func (s String) Len() int {
	return len(s)
}
