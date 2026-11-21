/*
Copyright 2016 The Kubernetes Authors.

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

package util

import (
	"sort"

	"k8s.io/kubernetes/pkg/apis/flowcontrol"
)

// 编译期断言：确保 FlowSchemaSequence 实现了 sort.Interface，
// 这样它可以作为有序序列被标准库排序。
var _ sort.Interface = FlowSchemaSequence{}

// FlowSchemaSequence 保存一组指向 FlowSchema 对象的指针，并保持排序。
// FlowSchemaSequence 实现了 `sort.Interface`。
type FlowSchemaSequence []*flowcontrol.FlowSchema

// Len 返回序列中 FlowSchema 的数量。
func (s FlowSchemaSequence) Len() int {
	return len(s)
}

// Less 判断第 i 个 FlowSchema 是否应排在第 j 个之前。
// 先按匹配优先级升序，匹配优先级越低越靠前；
// 相同优先级时再按名称排序，保证顺序稳定可预测。
func (s FlowSchemaSequence) Less(i, j int) bool {
	// 匹配优先级较低（数值较小）的 flow-schema 排在前面。
	if ip, jp := s[i].Spec.MatchingPrecedence, s[j].Spec.MatchingPrecedence; ip != jp {
		return ip < jp
	}
	// 优先级相同时按名称字母序排列。
	return s[i].Name < s[j].Name
}

// Swap 交换序列中第 i 个和第 j 个元素。
func (s FlowSchemaSequence) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
