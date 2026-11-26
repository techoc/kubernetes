/*
Copyright The Kubernetes Authors.

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

package sharding

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

// Selector 表示按元数据字段的哈希范围匹配对象的分片选择器，
// 遵循 Kubernetes API 中 labels.Selector 的模式。
type Selector interface {
	// Matches 判断给定对象是否命中该分片选择器。
	Matches(obj runtime.Object) (bool, error)

	// Empty 在匹配一切（不做过滤）时返回 true。
	Empty() bool

	// String 返回可经 Parse 往返的线上格式字符串。
	String() string

	// Requirements 返回分片范围要求列表。
	Requirements() []ShardRangeRequirement

	// DeepCopySelector 返回选择器的深拷贝。
	DeepCopySelector() Selector
}

// Everything 返回匹配所有对象的选择器。
func Everything() Selector {
	return &everythingSelector{}
}

type everythingSelector struct{}

// 空选择器对任意对象都返回匹配，且不携带任何要求。
func (s *everythingSelector) Matches(_ runtime.Object) (bool, error) { return true, nil }
func (s *everythingSelector) Empty() bool                            { return true }
func (s *everythingSelector) String() string                         { return "" }
func (s *everythingSelector) Requirements() []ShardRangeRequirement  { return nil }
func (s *everythingSelector) DeepCopySelector() Selector             { return &everythingSelector{} }

// shardSelector 用一条或多条分片范围要求实现 Selector。
type shardSelector struct {
	requirements []ShardRangeRequirement
}

func (s *shardSelector) Matches(obj runtime.Object) (bool, error) {
	if len(s.requirements) == 0 {
		return true, nil
	}
	// 所有条件必须针对同一个 key，这样只需解析字段值并计算一次哈希。
	// 解析器已保证这一点，这里再次校验，以防选择器由其它途径构造。
	// 所有要求必须使用同一 key，这样字段值与哈希只需计算一次。
	// 解析器已强制该约束，这里再校验一次以防绕过解析器构造选择器。
	key := s.requirements[0].Key
	for _, req := range s.requirements[1:] {
		if req.Key != key {
			return false, fmt.Errorf("inconsistent shard keys: %q vs %q", key, req.Key)
		}
	}

	value, err := ResolveFieldValue(obj, key)
	if err != nil {
		return false, err
	}
	hash := "0x" + HashField(value)

	// 采用左闭右开区间 [Start, End)，保证相邻分片不会重叠或遗漏。
	for _, req := range s.requirements {
		if !HexLess(hash, req.Start) && HexLess(hash, req.End) {
			return true, nil
		}
	}
	return false, nil
}

// HexLess 按数值比较两个带 0x 前缀的小写十六进制字符串。
// 两个值都必须规范化为 16 个十六进制数字（如 "0x0000000000000000"），
// 唯一例外是特殊上界 "0x10000000000000000"（2^64），它有 17 位。
func HexLess(a, b string) bool {
	// 长度不同意味着大小不同（规范化后较长的数值更大）。
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

func (s *shardSelector) Empty() bool {
	return len(s.requirements) == 0
}

func (s *shardSelector) String() string {
	parts := make([]string, 0, len(s.requirements))
	for _, req := range s.requirements {
		parts = append(parts, fmt.Sprintf("shardRange(%s, '%s', '%s')", req.Key, req.Start, req.End))
	}
	return strings.Join(parts, " || ")
}

// Requirements 返回要求列表的副本，避免调用方修改内部状态。
func (s *shardSelector) Requirements() []ShardRangeRequirement {
	result := make([]ShardRangeRequirement, len(s.requirements))
	copy(result, s.requirements)
	return result
}

// DeepCopySelector 复制要求切片，返回与原选择器互不共享底层数组的新选择器。
func (s *shardSelector) DeepCopySelector() Selector {
	reqs := make([]ShardRangeRequirement, len(s.requirements))
	copy(reqs, s.requirements)
	return &shardSelector{requirements: reqs}
}

// NewSelector 用给定要求创建 Selector。
// 所有要求必须使用同一 Key，该约束在匹配时校验。
// 未提供任何要求时返回 Everything()，避免下游对空选择器做特殊处理。
func NewSelector(reqs ...ShardRangeRequirement) Selector {
	if len(reqs) == 0 {
		return Everything()
	}
	return &shardSelector{
		requirements: reqs,
	}
}
