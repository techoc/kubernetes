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

package webhook

import (
	"errors"
	"reflect"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// mustLabelRequirement 解析 selector 并返回其 requirements；解析失败直接 panic，
// 因为测试用例里的 selector 都是静态合法的。
func mustLabelRequirement(selector string) labels.Requirements {
	ret, err := labels.Parse(selector)
	if err != nil {
		panic(err)
	}
	requirements, _ := ret.Requirements()
	return requirements
}

// Test_resourceAttributesFrom 验证把授权属性中的 field/label selector
// 转换为 SubjectAccessReview 的 ResourceAttributes：
// 只保留授权模型能够表达的操作符，未知操作符被跳过，空结果不产生 selector 字段。
func Test_resourceAttributesFrom(t *testing.T) {
	type args struct {
		attr authorizer.Attributes
	}
	tests := []struct {
		name string
		args args
		want *authorizationv1.ResourceAttributes
	}{
		{
			// 解析错误存在时仍能送出已解析的要求；错误交由调用方决定是否拒绝。
			name: "field selector: ignore error",
			args: args{
				attr: authorizer.AttributesRecord{
					FieldSelectorRequirements: fields.Requirements{
						fields.OneTermEqualSelector("foo", "bar").Requirements()[0],
					},
					FieldSelectorParsingErr: errors.New("failed"),
				},
			},
			want: &authorizationv1.ResourceAttributes{
				FieldSelector: &authorizationv1.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{{Key: "foo", Operator: "In", Values: []string{"bar"}}},
				},
			},
		},
		{
			// 标签 selector 的解析错误同样被忽略。
			name: "label selector: ignore error",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("foo in (bar,baz)"),
					LabelSelectorParsingErr:   errors.New("failed"),
				},
			},
			want: &authorizationv1.ResourceAttributes{
				LabelSelector: &authorizationv1.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{{Key: "foo", Operator: "In", Values: []string{"bar", "baz"}}},
				},
			},
		},
		{
			// =、==、in 在授权模型中统一表示为 In。
			name: "field selector: equals, double equals, in",
			args: args{
				attr: authorizer.AttributesRecord{
					FieldSelectorRequirements: fields.Requirements{
						{Operator: selection.Equals, Field: "foo", Value: "bar"},
						{Operator: selection.DoubleEquals, Field: "one", Value: "two"},
						{Operator: selection.In, Field: "apple", Value: "banana"},
					},
					FieldSelectorParsingErr: nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				FieldSelector: &authorizationv1.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "foo",
							Operator: "In",
							Values:   []string{"bar"},
						},
						{
							Key:      "one",
							Operator: "In",
							Values:   []string{"two"},
						},
						{
							Key:      "apple",
							Operator: "In",
							Values:   []string{"banana"},
						},
					},
				},
			},
		},
		{
			// != 与 notin 统一表示为 NotIn。
			name: "field selector: not equals, not in",
			args: args{
				attr: authorizer.AttributesRecord{
					FieldSelectorRequirements: fields.Requirements{
						{Operator: selection.NotEquals, Field: "foo", Value: "bar"},
						{Operator: selection.NotIn, Field: "apple", Value: "banana"},
					},
					FieldSelectorParsingErr: nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				FieldSelector: &authorizationv1.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "foo",
							Operator: "NotIn",
							Values:   []string{"bar"},
						},
						{
							Key:      "apple",
							Operator: "NotIn",
							Values:   []string{"banana"},
						},
					},
				},
			},
		},
		{
			// 无法映射的操作符被跳过，而不是让整个转换失败。
			name: "field selector: unknown operator skipped",
			args: args{
				attr: authorizer.AttributesRecord{
					FieldSelectorRequirements: fields.Requirements{
						{Operator: selection.NotEquals, Field: "foo", Value: "bar"},
						{Operator: selection.Operator("bad"), Field: "apple", Value: "banana"},
					},
					FieldSelectorParsingErr: nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				FieldSelector: &authorizationv1.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "foo",
							Operator: "NotIn",
							Values:   []string{"bar"},
						},
					},
				},
			},
		},
		{
			// 没有任何可用要求时不设置 FieldSelector，保持请求体最小化。
			name: "field selector: no requirements has no fieldselector",
			args: args{
				attr: authorizer.AttributesRecord{
					FieldSelectorRequirements: fields.Requirements{
						{Operator: selection.Operator("bad"), Field: "apple", Value: "banana"},
					},
					FieldSelectorParsingErr: nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{},
		},
		{
			// 标签要求输出按 key 排序，保证同一输入的请求体稳定。
			name: "label selector: in, equals, double equals",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("foo in (bar,baz), one=two, apple==banana"),
					LabelSelectorParsingErr:   nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				LabelSelector: &authorizationv1.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "apple",
							Operator: "In",
							Values:   []string{"banana"},
						},
						{
							Key:      "foo",
							Operator: "In",
							Values:   []string{"bar", "baz"},
						},
						{
							Key:      "one",
							Operator: "In",
							Values:   []string{"two"},
						},
					},
				},
			},
		},
		{
			// notin 与 != 统一为 NotIn。
			name: "label selector: not in, not equals",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("foo notin (bar,baz), one!=two"),
					LabelSelectorParsingErr:   nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				LabelSelector: &authorizationv1.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "foo",
							Operator: "NotIn",
							Values:   []string{"bar", "baz"},
						},
						{
							Key:      "one",
							Operator: "NotIn",
							Values:   []string{"two"},
						},
					},
				},
			},
		},
		{
			// 存在性要求映射为 Exists/DoesNotExist，不带 Values。
			name: "label selector: exists, not exists",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("foo, !one"),
					LabelSelectorParsingErr:   nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				LabelSelector: &authorizationv1.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "foo",
							Operator: "Exists",
						},
						{
							Key:      "one",
							Operator: "DoesNotExist",
						},
					},
				},
			},
		},
		{
			// 大于等无法表达的操作符被跳过。
			name: "label selector: unknown operator skipped",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("foo != bar, apple > 1"),
					LabelSelectorParsingErr:   nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{
				LabelSelector: &authorizationv1.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "foo",
							Operator: "NotIn",
							Values:   []string{"bar"},
						},
					},
				},
			},
		},
		{
			// 没有任何可用要求时不设置 LabelSelector。
			name: "label selector: no requirements has no labelselector",
			args: args{
				attr: authorizer.AttributesRecord{
					LabelSelectorRequirements: mustLabelRequirement("apple > 1"),
					LabelSelectorParsingErr:   nil,
				},
			},
			want: &authorizationv1.ResourceAttributes{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resourceAttributesFrom(tt.args.attr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resourceAttributesFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}
