/*
Copyright 2024 The Kubernetes Authors.

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

package job

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	batch "k8s.io/api/batch/v1"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// TestMatchSuccessPolicy 表驱动验证 matchSuccessPolicy 对 SuccessPolicy 各条规则的匹配结果，
// 同时断言返回的提示消息（其中的规则下标）与布尔结果，确保控制器依据正确的规则下标
// 写入 SuccessCriteriaMet 条件。
func TestMatchSuccessPolicy(t *testing.T) {
	// successPolicy 为 nil、Rules 为空或 succeededIndexes 为空时都不应匹配；
	// succeededIndexes 用区间表示已成功的 Pod 下标集合，覆盖子集、真子集、相交及空集等边界。
	// wantMessage 中的下标标识命中的具体规则，是状态写回定位的关键信息。
	testCases := map[string]struct {
		successPolicy        *batch.SuccessPolicy
		completions          int32
		succeededIndexes     orderedIntervals
		wantMessage          string
		wantMetSuccessPolicy bool
	}{
		// 没有策略时永不判定成功，防止默认行为意外提前完成 Job。
		"successPolicy is null": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 0}},
		},
		// 规则列表为空等价于无策略，同样不应命中。
		"any rules are nothing": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 0}},
			successPolicy:    &batch.SuccessPolicy{Rules: []batch.SuccessPolicyRule{}},
		},
		// 下标字符串非法时该规则应被跳过，而不是误判为匹配。
		"rules.succeededIndexes is invalid format": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 0}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("invalid-form"),
				}},
			},
		},
		// 规则区间完全包含于已成功区间，命中第 0 条规则。
		"rules.succeededIndexes is specified; succeededIndexes matched rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}, {4, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-2"),
				}},
			},
			wantMessage:          "Matched rules at index 0",
			wantMetSuccessPolicy: true,
		},
		// 要求成功的下标不在已成功集合中，不应命中。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("3"),
				}},
			},
		},
		// 仅指定数量且已达阈值，说明 succeededCount 分支独立生效。
		"rules.succeededCount is specified; succeededIndexes matched rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededCount: ptr.To[int32](2),
				}},
			},
			wantMessage:          "Matched rules at index 0",
			wantMetSuccessPolicy: true,
		},
		// 仅指定数量但未达阈值，验证阈值判断不是“有规则即命中”。
		"rules.succeededCount is specified; succeededIndexes didn't match rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededCount: ptr.To[int32](4),
				}},
			},
		},
		// 多条规则时命中其中一条，消息必须报告正确的规则下标。
		"multiple rules; rules.succeededIndexes is specified; succeededIndexes met one of rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}, {4, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{
					{
						SucceededIndexes: ptr.To("9"),
					},
					{
						SucceededIndexes: ptr.To("4,6"),
					},
				},
			},
			wantMessage:          "Matched rules at index 1",
			wantMetSuccessPolicy: true,
		},
		// 多条规则全部命中时取第一条，验证短路顺序确定。
		"multiple rules; rules.succeededIndexes is specified; succeededIndexes met all rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}, {4, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{
					{
						SucceededIndexes: ptr.To("0,1"),
					},
					{
						SucceededIndexes: ptr.To("5"),
					},
				},
			},
			wantMessage:          "Matched rules at index 0",
			wantMetSuccessPolicy: true,
		},
		// 同时指定下标和数量且下标完全满足，验证二者组合时仍能命中。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes met all rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}, {4, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("3-6"),
					SucceededCount:   ptr.To[int32](2),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 下标未完全满足时即使数量达标也不命中，防止组合条件被弱化。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes didn't match rules": {
			completions:      10,
			succeededIndexes: orderedIntervals{{0, 2}, {6, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("3-6"),
					SucceededCount:   ptr.To[int32](2),
				}},
			},
		},
		// 无任何成功下标时提前返回，避免空集合意外匹配。
		"rules.succeededIndexes is specified; succeededIndexes are nothing": {
			completions:      10,
			succeededIndexes: orderedIntervals{},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededCount: ptr.To[int32](4),
				}},
			},
		},
		// 规则是已成功集合的真子集：覆盖“部分成功即可完成”的语义。
		"rules.succeededIndexes is specified; succeededIndexes matched rules; rules is proper subset of succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 5}, {6, 9}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("2-4,6-8"),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 规则与已成功集合相等，验证边界相等时的包含判断。
		"rules.succeededIndexes is specified; succeededIndexes matched rules; rules equals succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{2, 4}, {6, 9}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("2-4,6-9"),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 规则区间包含于更大区间时仍应命中，防止子集判断受区间划分影响。
		"rules.succeededIndexes is specified; succeededIndexes matched rules; rules is subset of succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{2, 5}, {7, 15}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("2-4,8-12"),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 规则下标为空而实际非空：不应把空集当作“已满足”。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules is an empty set": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To(""),
				}},
			},
		},
		// 实际下标为空而规则非空，不应命中。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; succeededIndexes is an empty set": {
			completions:      10,
			succeededIndexes: orderedIntervals{},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To(""),
				}},
			},
		},
		// 双方都为空时也不命中，保持“空即未完成”的一致语义。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules and succeededIndexes are empty set": {
			completions:      10,
			succeededIndexes: orderedIntervals{},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To(""),
				}},
			},
		},
		// 规则下标完全不在已成功集合中，验证完全不相交的情况。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; all elements of rules.succeededIndexes aren't included in succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {5, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("10-12,14-16"),
				}},
			},
		},
		// 规则与已成功集合部分重叠但不完全包含，重叠不应被误判为成功。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules overlaps succeededIndexes at first": {
			completions:      10,
			succeededIndexes: orderedIntervals{{2, 4}, {6, 8}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("1-3,5-7"),
				}},
			},
		},
		// 同上但重叠数量达到 succeededCount，验证数量兜底可使部分重叠命中。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; rules overlaps succeededIndexes at first": {
			completions:      10,
			succeededIndexes: orderedIntervals{{2, 4}, {6, 8}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("1-3,5-7"),
					SucceededCount:   ptr.To[int32](4),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 尾部重叠仍不满足完整包含，覆盖区间扫描末端的处理。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules overlaps succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {5, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("2-4,6-9"),
				}},
			},
		},
		// 尾部重叠但数量达标应命中，验证末端计数同样准确。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; rules overlaps succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {5, 7}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("2-4,6-9"),
					SucceededCount:   ptr.To[int32](4),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 规则区间完全覆盖已成功区间（规则更大），不应命中。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules completely overlaps succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {7, 8}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-4,6-9"),
				}},
			},
		},
		// 规则更大但重叠数量达标时应命中，防止大区间规则被无条件拒绝。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; rules completely overlaps succeededIndexes": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {7, 8}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-4,6-9"),
					SucceededCount:   ptr.To[int32](5),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 规则跨多个已成功区间但仍有缺口，验证跨区间的累计计数不会误判。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules overlaps multiple succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {5, 9}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-6,8-9"),
				}},
			},
		},
		// 跨多个区间且累计达标应命中，验证累计交集的计算正确性。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; rules overlaps multiple succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 3}, {5, 9}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-6,8-9"),
					SucceededCount:   ptr.To[int32](7),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 首段重叠、末段相等但仍有缺口，验证混合情况下不会错误命中。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; rules overlaps succeededIndexes at first, and rules equals succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 5}, {7, 10}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-5,7-9"),
				}},
			},
		},
		// 混合情况下数量达标应命中，验证复杂区间组合的计数。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; rules overlaps succeededIndexes at first, and rules equals succeededIndexes at last": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 5}, {7, 10}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-5,7-9"),
					SucceededCount:   ptr.To[int32](8),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
		// 首段区间重叠且后续有缺口，覆盖较宽区间下的不完整匹配。
		"rules.succeededIndexes is specified; succeededIndexes didn't match rules; the first rules overlaps succeededIndexes at first": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 10}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-3,6-9"),
				}},
			},
		},
		// 同样的宽区间在数量达标时命中，完成交集计数的边界覆盖。
		"rules.succeededIndexes and rules.succeededCount are specified; succeededIndexes matched rules; the first rules overlaps succeededIndexes at first": {
			completions:      10,
			succeededIndexes: orderedIntervals{{1, 10}},
			successPolicy: &batch.SuccessPolicy{
				Rules: []batch.SuccessPolicyRule{{
					SucceededIndexes: ptr.To("0-3,6-9"),
					SucceededCount:   ptr.To[int32](7),
				}},
			},
			wantMetSuccessPolicy: true,
			wantMessage:          "Matched rules at index 0",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 用 ktesting 的 logger 缓冲日志，避免解析失败等路径向测试输出噪声。
			logger := ktesting.NewLogger(t,
				ktesting.NewConfig(
					ktesting.BufferLogs(true),
				),
			)
			gotMessage, gotMetSuccessPolicy := matchSuccessPolicy(logger, tc.successPolicy, tc.completions, tc.succeededIndexes)
			// 先断言布尔结果：判定是否成功直接影响 Job 状态转换，不能出错。
			if tc.wantMetSuccessPolicy != gotMetSuccessPolicy {
				t.Errorf("Unexpected bool from matchSuccessPolicy\nwant:%v\ngot:%v\n", tc.wantMetSuccessPolicy, gotMetSuccessPolicy)
			}
			// 再断言消息内容，确保命中规则的索引正确，状态条件中的原因信息可追溯。
			if diff := cmp.Diff(tc.wantMessage, gotMessage); diff != "" {
				t.Errorf("Unexpected message from matchSuccessPolicy (-want,+got):\n%s", diff)
			}
		})
	}
}
