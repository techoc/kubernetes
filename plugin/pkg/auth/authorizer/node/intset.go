/*
Copyright 2018 The Kubernetes Authors.

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

package node

// intSet 维护一个 id 到引用计数的映射。
// 用于 destinationEdgeIndex 中记录每个顶点可达的 Node ID 集合。
// 通过引用计数支持多条路径到达同一 Node 的情况。
type intSet struct {
	// members is a map of id to refcounts
	members map[int]int
}

func newIntSet() *intSet {
	return &intSet{members: map[int]int{}}
}

// has 返回指定 id 的引用计数是否大于 0。
// 并发安全：可以与 has 并发调用，但不能与其他方法并发调用。
func (s *intSet) has(i int) bool {
	if s == nil {
		return false
	}
	return s.members[i] > 0
}

// reset 清除所有 id，相当于将所有引用计数重置为 0。
// 非线程安全。
func (s *intSet) reset() {
	for k := range s.members {
		delete(s.members, k)
	}
}

// increment 将指定 id 的引用计数加 1。
// 非线程安全。
func (s *intSet) increment(i int) {
	s.members[i]++
}

// decrement 将指定 id 的引用计数减 1，
// 如果结果引用计数 <= 0，则从集合中移除该 id。
// 不跟踪小于零的引用计数。
// 非线程安全。
func (s *intSet) decrement(i int) {
	if s.members[i] <= 1 {
		delete(s.members, i)
	} else {
		s.members[i]--
	}
}
