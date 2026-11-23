/*
Copyright 2020 The Kubernetes Authors.

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

package nodeaffinity

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestNodeSelectorMatch 验证 NodeSelector 的解析与匹配契约。
// 关键在“先校验、后匹配”：只要任一 term 的字段或表达式非法，
// NewNodeSelector 就返回聚合错误，后续 Match 根本不会被调用；
// 而空 term 被当作“不匹配任何对象”，从语义上排除空选择器误命中所有节点的可能。
func TestNodeSelectorMatch(t *testing.T) {
	tests := []struct {
		name         string
		nodeSelector v1.NodeSelector
		node         *v1.Node
		wantErr      error
		wantMatch    bool
	}{
		{
			// nil 节点：直接判为不匹配，避免调用方拿到已删除节点时发生 panic。
			name:      "nil node",
			wantMatch: false,
		},
		{
			// 非法字段选择器（In 给了两个值）与非法 key：构造阶段就应聚合出两个错误，
			// 此时 Match 根本不会被调用。
			name: "invalid field selector and label selector",
			nodeSelector: v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1", "host_2"},
					}},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1"},
					}},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "invalid key",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_value"},
					}},
				},
			}},
			wantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "nodeSelectorTerms[0].matchFields[0].values",
					Detail:   "must have one element",
					BadValue: []string{"host_1", "host_2"},
				},
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "nodeSelectorTerms[2].matchExpressions[0].key",
					Detail:   `name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')`,
					BadValue: "invalid key",
				},
			}.ToAggregate(),
		},
		{
			// 字段命中但标签缺失：同一 term 内 matchExpressions 与 matchFields 是“与”关系，
			// 任一部分不满足则整个 term 不匹配。
			name: "node matches field selector, but not labels",
			nodeSelector: v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1"},
					}},
				},
			}},
			node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
		},
		{
			// 字段与标签同时命中：验证“与”关系成立时整个 term 才匹配。
			name: "node matches field selector and label selector",
			nodeSelector: v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1"},
					}},
				},
			}},
			node:      &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1", Labels: map[string]string{"label_1": "label_1_val"}}},
			wantMatch: true,
		},
		{
			// 第一个 term 只查标签（不匹配），第二个 term 只查字段（命中）：
			// 验证 term 之间是“或”关系，命中任意一个即可。
			name: "second term matches",
			nodeSelector: v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val"},
					}},
				},
				{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1"},
					}},
				},
			}},
			node:      &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			wantMatch: true,
		},
	}
	// 每个用例先校验构造阶段的错误是否符合预期；只有期望无错误时才继续执行匹配，
	// 这样非法输入不会在 Match 阶段产生误导性的断言失败。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeSelector, err := NewNodeSelector(&tt.nodeSelector)
			if !reflect.DeepEqual(tt.wantErr, err) {
				t.Errorf("NewNodeSelector returned unexpected error (-want,+got):\n%s", diff.Diff(tt.wantErr, err))
			}
			// 期望报错时无需构造选择器，直接结束本用例。
			if tt.wantErr != nil {
				return
			}
			match := nodeSelector.Match(tt.node)
			// 比对匹配结果，验证“先校验、后匹配”的语义。
			if match != tt.wantMatch {
				t.Errorf("NodeSelector.Match returned %t, want %t", match, tt.wantMatch)
			}
		})
	}
}

// TestPreferredSchedulingTermsScore 锁定打分规则：命中的 preferred term
// 按各自的 Weight 累加，未命中的不加分。
// 权重为 0 的项会在解析阶段被跳过，因此其中的非法字段不会报错——
// 这条行为在用例 "invalid field selector but no weight, error not reported" 中被明确固定下来。
func TestPreferredSchedulingTermsScore(t *testing.T) {
	tests := []struct {
		name           string
		prefSchedTerms []v1.PreferredSchedulingTerm
		node           *v1.Node
		wantErr        error
		wantScore      int64
	}{
		{
			// 两个非法项（多值字段选择器、非法 key）：构造阶段应聚合出两条错误，
			// 打分阶段不会被执行。
			name: "invalid field selector and label selector",
			prefSchedTerms: []v1.PreferredSchedulingTerm{
				{
					Weight: 1,
					Preference: v1.NodeSelectorTerm{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				},
				{
					Weight: 1,
					Preference: v1.NodeSelectorTerm{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val"},
						}},
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1"},
						}},
					},
				},
				{
					Weight: 1,
					Preference: v1.NodeSelectorTerm{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "invalid key",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_value"},
						}},
					},
				},
			},
			wantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "[0].matchFields[0].values",
					Detail:   "must have one element",
					BadValue: []string{"host_1", "host_2"},
				},
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "[2].matchExpressions[0].key",
					Detail:   `name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')`,
					BadValue: "invalid key",
				},
			}.ToAggregate(),
		},
		{
			// 权重为 0 的项即使字段选择器非法也被跳过：不报错、不计分。
			name: "invalid field selector but no weight, error not reported",
			prefSchedTerms: []v1.PreferredSchedulingTerm{
				{
					Weight: 0,
					Preference: v1.NodeSelectorTerm{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				},
			},
			node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
		},
		{
			// 三项权重分别为 5/7/11；节点命中第 1、3 项，期望得分 5+11=16。
			// 验证命中项累加权重、未命中项不加分。
			name: "first and third term match",
			prefSchedTerms: []v1.PreferredSchedulingTerm{
				{
					Weight: 5,
					Preference: v1.NodeSelectorTerm{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1"},
						}},
					},
				},
				{
					Weight: 7,
					Preference: v1.NodeSelectorTerm{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "unknown_label",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"unknown_label_val"},
						}},
					},
				},
				{
					Weight: 11,
					Preference: v1.NodeSelectorTerm{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val"},
						}},
					},
				},
			},
			node:      &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1", Labels: map[string]string{"label_1": "label_1_val"}}},
			wantScore: 16,
		},
	}
	// 同样先校验构造错误；无错误时才对节点打分并比对期望分数。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefSchedTerms, err := NewPreferredSchedulingTerms(tt.prefSchedTerms)
			if !reflect.DeepEqual(tt.wantErr, err) {
				t.Errorf("NewPreferredSchedulingTerms returned unexpected error (-want,+got):\n%s", diff.Diff(tt.wantErr, err))
			}
			// 期望报错时跳过打分，避免 nil 指针解引用。
			if tt.wantErr != nil {
				return
			}
			score := prefSchedTerms.Score(tt.node)
			// 分数是所有命中 term 的权重和，必须与 wantScore 一致。
			if score != tt.wantScore {
				t.Errorf("PreferredSchedulingTerms.Score returned %d, want %d", score, tt.wantScore)
			}
		})
	}
}

// TestNodeSelectorRequirementsAsSelector 检查 NodeSelectorRequirement 列表到
// labels.Selector 的转换：nil/空列表等价于 labels.Nothing()，即不匹配任何标签。
// 各操作符（In/Exists/Gt/Lt）的可选值约束不同，例如 Exists 不允许带 Values，
// 这些差异由 field.Path 带路径的错误信息固定下来，便于定位到具体哪个 requirement 出错。
func TestNodeSelectorRequirementsAsSelector(t *testing.T) {
	matchExpressions := []v1.NodeSelectorRequirement{{
		Key:      "foo",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{"bar", "baz"},
	}}
	// mustParse 把字符串解析成 labels.Selector，作为期望值的便捷写法；
	// 测试数据写错时直接 panic，便于第一时间发现用例本身的问题。
	mustParse := func(s string) labels.Selector {
		out, e := labels.Parse(s)
		if e != nil {
			panic(e)
		}
		return out
	}
	tc := []struct {
		in      []v1.NodeSelectorRequirement
		out     labels.Selector
		wantErr []error
	}{
		// nil 列表：返回 labels.Nothing()，表示不匹配任何对象。
		{in: nil, out: labels.Nothing()},
		// 空列表与 nil 等价，不能退化成“匹配全部”。
		{in: []v1.NodeSelectorRequirement{}, out: labels.Nothing()},
		{
			// In 操作符：foo 取值属于集合 (bar,baz)，应与手写选择器等价。
			in:  matchExpressions,
			out: mustParse("foo in (baz,bar)"),
		},
		{
			// Exists 是存在性判断，不允许携带 Values；期望带字段路径的 Invalid 错误。
			in: []v1.NodeSelectorRequirement{{
				Key:      "foo",
				Operator: v1.NodeSelectorOpExists,
				Values:   []string{"bar", "baz"},
			}},
			wantErr: []error{
				field.ErrorList{
					field.Invalid(field.NewPath("root").Index(0).Child("values"), []string{"bar", "baz"}, "values set must be empty for exists and does not exist"),
				}.ToAggregate(),
			},
		},
		{
			// Gt 操作符：按整数比较，foo 的值需大于 1。
			in: []v1.NodeSelectorRequirement{{
				Key:      "foo",
				Operator: v1.NodeSelectorOpGt,
				Values:   []string{"1"},
			}},
			out: mustParse("foo>1"),
		},
		{
			// Lt 操作符：按整数比较，bar 的值需小于 7。
			in: []v1.NodeSelectorRequirement{{
				Key:      "bar",
				Operator: v1.NodeSelectorOpLt,
				Values:   []string{"7"},
			}},
			out: mustParse("bar<7"),
		},
		{
			// 未知操作符：期望 NotSupported 错误并列出所有合法操作符。
			in: []v1.NodeSelectorRequirement{{
				Key:      "baz",
				Operator: "invalid",
				Values:   []string{"5"},
			}},
			wantErr: []error{
				field.NotSupported(field.NewPath("root").Index(0).Child("operator"), v1.NodeSelectorOperator("invalid"), validSelectorOperators),
			},
		},
	}

	// 统一的“root”路径前缀便于断言错误信息中的字段下标。
	// 先比对错误，再比对转换得到的选择器对象；两者都符合期望才算通过。
	for i, tc := range tc {
		out, err := nodeSelectorRequirementsAsSelector(tc.in, field.NewPath("root"))
		if !reflect.DeepEqual(tc.wantErr, err) {
			t.Errorf("nodeSelectorRequirementsAsSelector returned unexpected error (-want,+got):\n%s", diff.Diff(tc.wantErr, err))
		}
		// 出错时 out 为 nil，也必须与期望值一致，防止部分结果被误用。
		if !reflect.DeepEqual(out, tc.out) {
			t.Errorf("[%v]expected:\n\t%+v\nbut got:\n\t%+v", i, tc.out, out)
		}
	}
}

// TestPodMatchesNodeSelectorAndAffinityTerms 验证“硬性”调度条件：
// NodeSelector（旧字段）与 NodeAffinity 的 requiredDuringScheduling 是 AND 关系，
// 两者都必须满足；同一 term 内的多条表达式是 AND，多个 term 之间是 OR。
// 表中还专门覆盖了 nil/空 term 不匹配、无 Affinity 视为匹配、
// 以及 matchFields 与 matchExpressions 同处一个 term 时必须同时成立等边界，
// 因为这些语义直接决定 pod 会不会被放到错误的节点上。
func TestPodMatchesNodeSelectorAndAffinityTerms(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		labels   map[string]string
		nodeName string
		want     bool
	}{
		{
			// Pod 没有任何选择器：无约束时任何节点都可用。
			name: "no selector",
			pod:  &v1.Pod{},
			want: true,
		},
		{
			// Pod 要求 foo=bar，但节点没有任何标签：标签缺失导致不匹配。
			name: "missing labels",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			want: false,
		},
		{
			// 节点标签恰好满足 nodeSelector：精确匹配，命中。
			name: "same labels",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// 节点标签是选择器要求的超集：多余的标签不影响匹配。
			name: "node labels are superset",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
				"baz": "blah",
			},
			want: true,
		},
		{
			// 节点标签只是选择器要求的子集：缺少 baz=blah，不匹配。
			name: "node labels are subset",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
						"baz": "blah",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
		},
		{
			// matchExpressions 的 In 操作符：节点 foo=bar 在候选集合 {bar,value2} 中，命中。
			name: "Pod with matchExpressions using In operator that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// Gt 操作符：用两位主版本+两位次版本表示版本号，0206 大于 0204，命中。
			name: "Pod with matchExpressions using Gt operator that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "kernel-version",
												Operator: v1.NodeSelectorOpGt,
												Values:   []string{"0204"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				// 版本号用两位表示主版本、两位表示次版本，便于按数值比较。
				"kernel-version": "0206",
			},
			want: true,
		},
		{
			// NotIn 操作符：节点 mem-type 为 DDR3，不在排除列表 {DDR,DDR2} 中，命中。
			name: "Pod with matchExpressions using NotIn operator that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "mem-type",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"DDR", "DDR2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"mem-type": "DDR3",
			},
			want: true,
		},
		{
			// Exists 操作符：只要节点存在 GPU 标签即可，不关心其值，命中。
			name: "Pod with matchExpressions using Exists operator that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			want: true,
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"value1", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
			// 亲和性要求 foo∈{value1,value2}，节点为 bar：不匹配，Pod 不会被调度。
			name: "Pod with affinity that don't match node's labels won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: nil,
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
			// required 段的 term 列表为 nil：没有任何 term 可命中，不匹配。
			name: "Pod with a nil []NodeSelectorTerm in affinity, can't match the node's labels and won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
			// required 段的 term 列表为空切片：同样没有 term 可命中，不匹配。
			name: "Pod with an empty []NodeSelectorTerm in affinity, can't match the node's labels and won't schedule onto the node",
		},
		{
			// 唯一 term 的 MatchExpressions 为空：该 term 被忽略，无 term 可命中，不匹配。
			name: "Pod with empty MatchExpressions is not a valid value will match no objects and won't schedule onto the node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
		},
		{
			// 完全没有 Affinity：没有硬性约束，任何节点都匹配。
			name: "Pod with no Affinity will schedule onto a node",
			pod:  &v1.Pod{},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// Affinity 存在但 required 段为 nil：等价于无硬性约束，匹配。
			name: "Pod with Affinity but nil NodeSelector will schedule onto a node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: nil,
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// 同一 term 内两条表达式是“与”关系：GPU 存在且取值不属于 {AMD,INTER}，均满足，命中。
			name: "Pod with multiple matchExpressions ANDed that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											}, {
												Key:      "GPU",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"AMD", "INTER"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			want: true,
		},
		{
			// 同一 term 内 GPU 需存在且取值属于 {AMD,INTER}；节点 GPU 为 K1，与关系不成立，不匹配。
			name: "Pod with multiple matchExpressions ANDed that doesn't match the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											}, {
												Key:      "GPU",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"AMD", "INTER"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			want: false,
		},
		{
			// 多个 term 之间是“或”关系：第一个 term 命中 foo=bar，整体匹配。
			name: "Pod with multiple NodeSelectorTerms ORed in affinity, matches the node's labels and will schedule onto the node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar", "value2"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "diffkey",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"wrong", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// 旧 nodeSelector 与新 Affinity 同时满足：两者是“与”关系，命中。
			name: "Pod with an Affinity and a PodSpec.NodeSelector(the old thing that we are deprecating) " +
				"both are satisfied, will schedule onto the node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			// Affinity 命中但旧 nodeSelector 不满足（foo=barrrrrr≠bar）：“与”关系失败，不匹配。
			name: "Pod with an Affinity matches node's labels but the PodSpec.NodeSelector(the old thing that we are deprecating) " +
				"is not satisfied, won't schedule onto the node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "barrrrrr",
			},
			want: false,
		},
		{
			// term 中 NotIn 的取值含非法字符：解析失败按不匹配处理，Pod 不被调度。
			name: "Pod with an invalid value in Affinity term won't be scheduled onto the node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"invalid value: ___@#$%^"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			want: false,
		},
		{
			// matchFields 用 metadata.name 匹配节点名：node_1 命中，匹配。
			name: "Pod with matchFields using In operator that matches the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_1",
			want:     true,
		},
		{
			// 节点名为 node_2，与要求的 node_1 不符：字段不匹配，不匹配。
			name: "Pod with matchFields using In operator that does not match the existing node",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			want:     false,
		},
		{
			// 两个 term：字段 term 不匹配但标签 term 匹配；term 间是“或”关系，整体匹配。
			name: "Pod with two terms: matchFields does not match, but matchExpressions matches",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			want:     true,
		},
		{
			// 同一 term 内字段要求 node_1、标签要求 foo=bar：字段不匹配，“与”关系失败，不匹配。
			name: "Pod with one term: matchFields does not match, but matchExpressions matches",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			want:     false,
		},
		{
			// 同一 term 内字段与标签都匹配：“与”关系成立，命中。
			name: "Pod with one term: both matchFields and matchExpressions match",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_1",
			labels:   map[string]string{"foo": "bar"},
			want:     true,
		},
		{
			// 两个 term 分别因字段和标签不匹配：没有任何 term 命中，不匹配。
			name: "Pod with two terms: both matchFields and matchExpressions do not match",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"not-match-to-bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			want:     false,
		},
	}

	// 用用例中的 nodeName 与 labels 构造被测节点；Match 的错误返回值在此忽略，
	// 因为表中只验证硬性约束的布尔结果（解析错误已在其他用例单独覆盖）。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := v1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   test.nodeName,
				Labels: test.labels,
			}}
			got, _ := GetRequiredNodeAffinity(test.pod).Match(&node)
			// 比对期望的调度可行性；nodeSelector 与 nodeAffinity 必须同时满足。
			if test.want != got {
				t.Errorf("expected: %v got %v", test.want, got)
			}
		})
	}
}
