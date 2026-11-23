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

package corev1

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
)

// TestPodPriority 验证 PodPriority 在 priority 为 nil 与显式设置两种情形下的返回值。
func TestPodPriority(t *testing.T) {
	p := int32(20)
	// 表驱动用例：name 为用例名，pod 为输入 Pod，expectedPriority 为期望返回的优先级。
	tests := []struct {
		name             string
		pod              *v1.Pod
		expectedPriority int32
	}{
		// 未设置 priority 的 Pod 必须取默认 0，否则会被误当成高优先级参与抢占。
		{
			name: "no priority pod resolves to static default priority",
			pod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{
					{Name: "container", Image: "image"}},
				},
			},
			expectedPriority: 0,
		},
		{
			name: "pod with priority resolves correctly",
			pod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{
					{Name: "container", Image: "image"}},
					Priority: &p,
				},
			},
			expectedPriority: p,
		},
	}
	for _, test := range tests {
		if PodPriority(test.pod) != test.expectedPriority {
			t.Errorf("expected pod priority: %v, got %v", test.expectedPriority, PodPriority(test.pod))
		}

	}
}

// TestMatchNodeSelectorTerms 覆盖标签与字段选择的交叉情况：
// 两类约束在同一 term 内是“与”，多个 term 之间是“或”。
// 故意构造“同名 key 放在 label 而非 field 里”的用例，
// 确保实现不会把 label 和 metadata.name 这类字段混为一谈。
func TestMatchNodeSelectorTerms(t *testing.T) {
	// args 聚合一次匹配调用所需的两个输入：nodeSelector 为节点选择器，node 为待匹配的节点。
	type args struct {
		nodeSelector *v1.NodeSelector
		node         *v1.Node
	}

	// 表驱动用例：name 为用例名，args 为输入参数，want 为期望的匹配结果（true 表示匹配）。
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil terms",
			args: args{
				nodeSelector: nil,
				node:         nil,
			},
			want: false,
		},
		{
			name: "node label matches matchExpressions terms",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"label_1": "label_1_val"}}},
			},
			want: true,
		},
		{
			name: "node field matches matchFields terms",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			},
			want: true,
		},
		// metadata.name 是单值字段，In 里给多个值不可能成立。
		{
			name: "invalid node field requirement",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			},
			want: false,
		},
		{
			name: "fieldSelectorTerm with node labels",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "not_host_1", Labels: map[string]string{
					"metadata.name": "host_1",
				}}},
			},
			want: false,
		},
		{
			name: "labelSelectorTerm with node fields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			},
			want: false,
		},
		{
			name: "labelSelectorTerm and fieldSelectorTerm was set, but only node fields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
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
			want: false,
		},
		{
			name: "labelSelectorTerm and fieldSelectorTerm was set, both node fields and labels (both matched)",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
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
					}},
				},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val",
					}}},
			},
			want: true,
		},
		{
			name: "labelSelectorTerm and fieldSelectorTerm was set, both node fields and labels (one mismatched)",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
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
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val-failed",
					}}},
			},
			want: false,
		},
		// 两个 term 分别只看 label 和 field；label 不匹配时仍可凭 field 命中。
		{
			name: "multi-selector was set, both node fields and labels (one mismatched)",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
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
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val-failed",
					}}},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 本测试只关心匹配结果，error 用 _ 丢弃（非法选择器场景由其他用例覆盖）。
			if got, _ := MatchNodeSelectorTerms(tt.args.node, tt.args.nodeSelector); got != tt.want {
				t.Errorf("MatchNodeSelectorTermsORed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMatchNodeSelectorTermsStateless 确保 MatchNodeSelectorTerms 以“无状态”方式调用，
// 即调用后 nodeSelector（包括其嵌套字段）不应被深度修改。
//
// 同一个 NodeSelector 会被多个调度周期、多个节点重复使用。若匹配时重排 Values
// 或原地改字段，会污染其他调用方，甚至影响缓存中的 Pod 模板。
// 用例特别包含乱序 Values，目的是发现“排序后忘还原”这类隐蔽的就地修改。
func TestMatchNodeSelectorTermsStateless(t *testing.T) {
	// args 聚合一次匹配调用所需的两个输入：nodeSelector 为节点选择器，node 为待匹配的节点。
	type args struct {
		nodeSelector *v1.NodeSelector
		node         *v1.Node
	}

	// 表驱动用例：name 为用例名，args 为输入参数，want 为匹配后 nodeSelector 应保持的原样。
	tests := []struct {
		name string
		args args
		want *v1.NodeSelector
	}{
		{
			name: "nil terms",
			args: args{
				nodeSelector: nil,
				node:         nil,
			},
			want: nil,
		},
		{
			name: "nodeLabels: preordered matchExpressions and nil matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val", "label_2_val"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"label_1": "label_1_val"}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val", "label_2_val"},
					}},
				},
			}},
		},
		{
			name: "nodeLabels: unordered matchExpressions and nil matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_2_val", "label_1_val"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"label_1": "label_1_val"}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_2_val", "label_1_val"},
					}},
				},
			}},
		},
		{
			name: "nodeFields: nil matchExpressions and preordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1", "host_2"},
					}},
				},
			}},
		},
		{
			name: "nodeFields: nil matchExpressions and unordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_2", "host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1"}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_2", "host_1"},
					}},
				},
			}},
		},
		{
			name: "nodeLabels and nodeFields: ordered matchExpressions and ordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val", "label_2_val"},
						}},
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val",
					}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val", "label_2_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1", "host_2"},
					}},
				},
			}},
		},
		{
			name: "nodeLabels and nodeFields: ordered matchExpressions and unordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_1_val", "label_2_val"},
						}},
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_2", "host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val",
					}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_1_val", "label_2_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_2", "host_1"},
					}},
				},
			}},
		},
		{
			name: "nodeLabels and nodeFields: unordered matchExpressions and ordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_2_val", "label_1_val"},
						}},
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_1", "host_2"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val",
					}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_2_val", "label_1_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_1", "host_2"},
					}},
				},
			}},
		},
		{
			name: "nodeLabels and nodeFields: unordered matchExpressions and unordered matchFields",
			args: args{
				nodeSelector: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{{
							Key:      "label_1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"label_2_val", "label_1_val"},
						}},
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"host_2", "host_1"},
						}},
					},
				}},
				node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host_1",
					Labels: map[string]string{
						"label_1": "label_1_val",
					}}},
			},
			want: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{{
						Key:      "label_1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"label_2_val", "label_1_val"},
					}},
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"host_2", "host_1"},
					}},
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = MatchNodeSelectorTerms(tt.args.node, tt.args.nodeSelector)
			if !apiequality.Semantic.DeepEqual(tt.args.nodeSelector, tt.want) {
				// 一旦 nodeSelector 被深度修改就判失败。
				// 断言的是输入对象本身，而不是返回值：只要匹配过程碰过输入就算失败。
				t.Errorf("MatchNodeSelectorTerms() got = %v, want %v", tt.args.nodeSelector, tt.want)
			}
		})
	}
}

// TestGetAvoidPodsFromNode 验证 GetAvoidPodsFromNodeAnnotations 的行为，
// 同时覆盖合法注解、缺省注解和格式损坏三种情况。
// 注解内容是序列化的 JSON，解析失败必须报错而不是当成空配置：
// 静默忽略会让本应避免的 Pod 照常落到节点上。
func TestGetAvoidPodsFromNode(t *testing.T) {
	controllerFlag := true
	// 表驱动用例：node 为待读取注解的节点，expectValue 为期望的避让配置，
	// expectErr 表示是否期望 JSON 解析报错。
	testCases := []struct {
		node        *v1.Node
		expectValue v1.AvoidPods
		expectErr   bool
	}{
		{
			node:        &v1.Node{},
			expectValue: v1.AvoidPods{},
			expectErr:   false,
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.PreferAvoidPodsAnnotationKey: `
							{
							    "preferAvoidPods": [
							        {
							            "podSignature": {
							                "podController": {
						                            "apiVersion": "v1",
						                            "kind": "ReplicationController",
						                            "name": "foo",
						                            "uid": "abcdef123456",
						                            "controller": true
							                }
							            },
							            "reason": "some reason",
							            "message": "some message"
							        }
							    ]
							}`,
					},
				},
			},
			expectValue: v1.AvoidPods{
				PreferAvoidPods: []v1.PreferAvoidPodsEntry{
					{
						PodSignature: v1.PodSignature{
							PodController: &metav1.OwnerReference{
								APIVersion: "v1",
								Kind:       "ReplicationController",
								Name:       "foo",
								UID:        "abcdef123456",
								Controller: &controllerFlag,
							},
						},
						Reason:  "some reason",
						Message: "some message",
					},
				},
			},
			expectErr: false,
		},
		{
			// 故意截断 JSON 结构，验证解析错误会向上传播。
			node: &v1.Node{
				// 故意缺失 "podController" 与 "podSignature" 的结束符号，构造非法 JSON。
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.PreferAvoidPodsAnnotationKey: `
							{
							    "preferAvoidPods": [
							        {
							            "podSignature": {
							                "podController": {
							                    "kind": "ReplicationController",
							                    "apiVersion": "v1"
							            "reason": "some reason",
							            "message": "some message"
							        }
							    ]
							}`,
					},
				},
			},
			expectValue: v1.AvoidPods{},
			expectErr:   true,
		},
	}

	// 逐一校验：错误出现与否是否符合 expectErr，解析结果是否与期望值深度相等。
	for i, tc := range testCases {
		v, err := GetAvoidPodsFromNodeAnnotations(tc.node.Annotations)
		if err == nil && tc.expectErr {
			t.Errorf("[%v]expected error but got none.", i)
		}
		if err != nil && !tc.expectErr {
			t.Errorf("[%v]did not expect error but got: %v", i, err)
		}
		if !reflect.DeepEqual(tc.expectValue, v) {
			t.Errorf("[%v]expect value %v but got %v with %v", i, tc.expectValue, v, v.PreferAvoidPods[0].PodSignature.PodController.Controller)
		}
	}
}

// TestFindMatchingUntoleratedTaint 验证 FindMatchingUntoleratedTaint 的判定逻辑，
// 覆盖过滤器、容忍匹配以及数值比较操作符三大块。
// 关键不变量是：只有“过滤后仍未被任何 toleration 覆盖的 taint”才构成阻碍；
// 空 taint 集永远可通过，过滤器把所有 taint 排除掉也视为可通过。
// 数值用例还验证了非法值会报错，而不是被当成不匹配后静默放过。
func TestFindMatchingUntoleratedTaint(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：description 为用例描述；tolerations 为容忍列表；taints 为污点列表；
	// applyFilter 为污点过滤函数；expectTolerated 表示期望是否全部被容忍；
	// expectError 表示是否期望出错（数值比较遇到非法值）；
	// enableComparisonOperatorsFG 控制数值比较操作符特性开关。
	testCases := []struct {
		description                 string
		tolerations                 []v1.Toleration
		taints                      []v1.Taint
		applyFilter                 taintsFilterFunc
		expectTolerated             bool
		expectError                 bool
		enableComparisonOperatorsFG bool
	}{
		{
			description:     "empty tolerations tolerate empty taints",
			tolerations:     []v1.Toleration{},
			taints:          []v1.Taint{},
			applyFilter:     func(t *v1.Taint) bool { return true },
			expectTolerated: true,
		},
		{
			description: "non-empty tolerations tolerate empty taints",
			tolerations: []v1.Toleration{
				{
					Key:      "foo",
					Operator: "Exists",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints:          []v1.Taint{},
			applyFilter:     func(t *v1.Taint) bool { return true },
			expectTolerated: true,
		},
		{
			description: "tolerations match all taints, expect tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "foo",
					Operator: "Exists",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "foo",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:     func(t *v1.Taint) bool { return true },
			expectTolerated: true,
		},
		{
			description: "tolerations don't match taints, but no taints apply to the filter, expect tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "foo",
					Operator: "Exists",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "bar",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:     func(t *v1.Taint) bool { return false },
			expectTolerated: true,
		},
		// applyFilter 为 nil 时语义是“不过滤任何 taint”，而不是“全部排除”。
		{
			description: "no filterFunc indicated, means all taints apply to the filter, tolerations don't match taints, expect untolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "foo",
					Operator: "Exists",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "bar",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:     nil,
			expectTolerated: false,
		},
		{
			description: "tolerations match taints, expect tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "foo",
					Operator: "Exists",
					Effect:   v1.TaintEffectNoExecute,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "foo",
					Effect: v1.TaintEffectNoExecute,
				},
				{
					Key:    "bar",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:     func(t *v1.Taint) bool { return t.Effect == v1.TaintEffectNoExecute },
			expectTolerated: true,
		},
		{
			description: "numeric Gt operator with taint value below threshold, expect not tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "node.kubernetes.io/sla",
					Operator: "Gt",
					Value:    "950",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "node.kubernetes.io/sla",
					Value:  "800",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:                 func(t *v1.Taint) bool { return true },
			expectTolerated:             false,
			enableComparisonOperatorsFG: true,
		},
		{
			description: "numeric Gt operator with taint value above threshold, expect tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "node.kubernetes.io/sla",
					Operator: "Gt",
					Value:    "750",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "node.kubernetes.io/sla",
					Value:  "950",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:                 func(t *v1.Taint) bool { return true },
			expectTolerated:             true,
			enableComparisonOperatorsFG: true,
		},
		{
			description: "numeric Lt operator with taint value above threshold, expect not tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "node.kubernetes.io/sla",
					Operator: "Lt",
					Value:    "800",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "node.kubernetes.io/sla",
					Value:  "950",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:                 func(t *v1.Taint) bool { return true },
			expectTolerated:             false,
			enableComparisonOperatorsFG: true,
		},
		// 非数字值无法与数值阈值比较，应当报错；否则调用方无法区分“不满足”和“数据坏了”。
		{
			description: "numeric Gt operator with non-numeric taint value, expect not tolerated",
			tolerations: []v1.Toleration{
				{
					Key:      "node.kubernetes.io/sla",
					Operator: "Gt",
					Value:    "950",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			taints: []v1.Taint{
				{
					Key:    "node.kubernetes.io/sla",
					Value:  "high",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			applyFilter:                 func(t *v1.Taint) bool { return true },
			expectTolerated:             false,
			expectError:                 true,
			enableComparisonOperatorsFG: true,
		},
	}

	// 对每个用例，将“期望是否全部容忍”与函数返回的 untolerated 取反结果做比较。
	for _, tc := range testCases {
		_, untolerated := FindMatchingUntoleratedTaint(logger, tc.taints, tc.tolerations, tc.applyFilter, tc.enableComparisonOperatorsFG)
		if tc.expectTolerated != !untolerated {
			filteredTaints := []v1.Taint{}
			for _, taint := range tc.taints {
				if tc.applyFilter != nil && !tc.applyFilter(&taint) {
					continue
				}
				filteredTaints = append(filteredTaints, taint)
			}
			t.Errorf("[%s] expect tolerations %+v tolerate filtered taints %+v in taints %+v", tc.description, tc.tolerations, filteredTaints, tc.taints)
		}
	}
}
