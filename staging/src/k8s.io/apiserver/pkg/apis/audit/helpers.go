/*
Copyright 2017 The Kubernetes Authors.

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

package audit

// ordLevel 把审计级别映射为可比较的序号。
//
// 级别存在包含关系：RequestResponse 包含 Request，Request 包含 Metadata，
// 因此用递增序号表达"信息量"，才能实现 Less/GreaterOrEqual 这类包含判断。
// 未知或空级别映射为 0，低于所有有效级别，使配置缺省时表现为"不记录"。
func ordLevel(l Level) int {
	switch l {
	case LevelMetadata:
		return 1
	case LevelRequest:
		return 2
	case LevelRequestResponse:
		return 3
	default:
		return 0
	}
}

// Less 报告 a 是否比 b 记录更少的信息（即 b 的级别包含 a）。
func (a Level) Less(b Level) bool {
	return ordLevel(a) < ordLevel(b)
}

// GreaterOrEqual 报告 a 是否至少与 b 一样详细。
// 用于按策略阈值过滤：配置的级别越高，实际记录的事件就必须越详细。
func (a Level) GreaterOrEqual(b Level) bool {
	return ordLevel(a) >= ordLevel(b)
}
