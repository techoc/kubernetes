/*
Copyright 2014 The Kubernetes Authors.

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

package validation

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	authorizationapi "k8s.io/kubernetes/pkg/apis/authorization"
)

// TestValidateSARSpec 测试 SubjectAccessReviewSpec 的校验。
// 成功用例覆盖资源/非资源请求两种形态与各类选择器；错误用例覆盖互斥字段、缺失主体及选择器的各种非法组合。
func TestValidateSARSpec(t *testing.T) {
	// 成功用例：包含原始选择器因为原始字符串无需本地解析，以及未知 operator 因为版本漂移时应被容忍。
	successCases := []authorizationapi.SubjectAccessReviewSpec{
		{ResourceAttributes: &authorizationapi.ResourceAttributes{}, User: "me"},
		{NonResourceAttributes: &authorizationapi.NonResourceAttributes{}, Groups: []string{"my-group"}},
		{ // 字段原始选择器：原始字符串无需本地解析即可通过校验
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					RawSelector: "***foo",
				},
			},
		},
		{ // 标签原始选择器：同上，原始字符串直接提交即可
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					RawSelector: "***foo",
				},
			},
		},
		{ // 未知字段 operator：版本漂移时应被容忍：服务端应允许新版客户端使用自己认识的 operator
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.FieldSelectorOperator("fake"),
							Values:   []string{"val"},
						},
					},
				},
			},
		},
		{ // 未知标签 operator：版本漂移时应被容忍：同上，标签选择器也必须容忍未知 operator
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.LabelSelectorOperator("fake"),
							Values:   []string{"val"},
						},
					},
				},
			},
		},
	}
	for _, successCase := range successCases {
		if errs := ValidateSubjectAccessReviewSpec(successCase, field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	// 错误用例：每条都针对一条明确的不变量，例如联合字段二选一、主体不可为空、
	// rawSelector 与 requirements 互斥，以及 selector requirement 自身的取值约束。
	errorCases := []struct {
		name string
		obj  authorizationapi.SubjectAccessReviewSpec
		msg  string
	}{{ // neither request：资源和非资源属性均未指定，无法确定请求类型
		name: "neither request",
		obj:  authorizationapi.SubjectAccessReviewSpec{User: "me"},
		msg:  "exactly one of nonResourceAttributes or resourceAttributes must be specified",
	}, { // both requests：两个属性都指定，语义冲突
		name: "both requests",
		obj: authorizationapi.SubjectAccessReviewSpec{
			ResourceAttributes:    &authorizationapi.ResourceAttributes{},
			NonResourceAttributes: &authorizationapi.NonResourceAttributes{},
			User:                  "me",
		},
		msg: "exactly one of nonResourceAttributes or resourceAttributes must be specified",
	}, { // no subject：既无 user 也无 group，无法判断检查对象
		name: "no subject",
		obj: authorizationapi.SubjectAccessReviewSpec{
			ResourceAttributes: &authorizationapi.ResourceAttributes{},
		},
		msg: `spec.user: Invalid value: "": at least one of user or group must be specified`,
	}, { // field selector 同时给出原始串和结构化条件，二者含义可能不一致
		name: "resource attributes: field selector specify both",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					RawSelector: "foo",
					Requirements: []metav1.FieldSelectorRequirement{
						{},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.fieldSelector.rawSelector: Invalid value: "foo": may not specified at the same time as requirements`,
	}, { // field selector 两个字段都为空，等于没有表达任何限制
		name: "resource attributes: field selector specify neither",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{},
			},
		},
		msg: `spec.resourceAttributes.fieldSelector.requirements: Required value: when spec.resourceAttributes.fieldSelector is specified, requirements or rawSelector is required`,
	}, { // 条件缺 key：没有 key 的条件无法匹配任何字段
		name: "resource attributes: field selector no key",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key: "",
						},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.fieldSelector.requirements[0].key: Required value: must be specified`,
	}, { // In 操作符必须带取值，否则无法构造匹配集合
		name: "resource attributes: field selector no value for in",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.FieldSelectorOpIn,
							Values:   []string{},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.fieldSelector.requirements[0].values: Required value: must be specified when `operator` is 'In' or 'NotIn'",
	}, { // NotIn 同理，也必须有取值
		name: "resource attributes: field selector no value for not in",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.FieldSelectorOpNotIn,
							Values:   []string{},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.fieldSelector.requirements[0].values: Required value: must be specified when `operator` is 'In' or 'NotIn'",
	}, { // Exists 只判断存在性，带取值属于非法冗余输入
		name: "resource attributes: field selector values for exists",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.FieldSelectorOpExists,
							Values:   []string{"val"},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.fieldSelector.requirements[0].values: Forbidden: may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
	}, { // DoesNotExist 同理，不允许携带取值
		name: "resource attributes: field selector values for not exists",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				FieldSelector: &authorizationapi.FieldSelectorAttributes{
					Requirements: []metav1.FieldSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.FieldSelectorOpDoesNotExist,
							Values:   []string{"val"},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.fieldSelector.requirements[0].values: Forbidden: may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
	}, { // 标签选择器同样禁止原始串与结构化条件并存
		name: "resource attributes: label selector specify both",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					RawSelector: "foo",
					Requirements: []metav1.LabelSelectorRequirement{
						{},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.rawSelector: Invalid value: "foo": may not specified at the same time as requirements`,
	}, { // 标签选择器两个字段都为空，等于没有表达任何限制
		name: "resource attributes: label selector specify neither",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.requirements: Required value: when spec.resourceAttributes.labelSelector is specified, requirements or rawSelector is required`,
	}, { // 标签 key 不能为空：空 key 不构成合法的标签选择器
		name: "resource attributes: label selector no key",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key: "",
						},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.requirements[0].key: Invalid value: "": name part must be non-empty`,
	}, { // 非法标签名包含不允许的字符，需由标签名校验规则拒绝
		name: "resource attributes: label selector invalid label name",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key: "()foo",
						},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.requirements[0].key: Invalid value: "()foo": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')`,
	}, { // 标签 In 操作符必须带取值
		name: "resource attributes: label selector no value for in",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.labelSelector.requirements[0].values: Required value: must be specified when `operator` is 'In' or 'NotIn'",
	}, { // 标签 NotIn 操作符必须带取值
		name: "resource attributes: label selector no value for not in",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.labelSelector.requirements[0].values: Required value: must be specified when `operator` is 'In' or 'NotIn'",
	}, { // 标签 Exists 操作符不允许携带取值
		name: "resource attributes: label selector values for exists",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.LabelSelectorOpExists,
							Values:   []string{"val"},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.labelSelector.requirements[0].values: Forbidden: may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
	}, { // 标签 DoesNotExist 操作符不允许携带取值
		name: "resource attributes: label selector values for not exists",
		obj: authorizationapi.SubjectAccessReviewSpec{
			User: "me",
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					Requirements: []metav1.LabelSelectorRequirement{
						{
							Key:      "k",
							Operator: metav1.LabelSelectorOpDoesNotExist,
							Values:   []string{"val"},
						},
					},
				},
			},
		},
		msg: "spec.resourceAttributes.labelSelector.requirements[0].values: Forbidden: may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
	}}

	for _, c := range errorCases {
		t.Run(c.name, func(t *testing.T) {
			errs := ValidateSubjectAccessReviewSpec(c.obj, field.NewPath("spec"))
			if len(errs) == 0 {
				t.Errorf("%s: expected failure for %q", c.name, c.msg)
			} else if !strings.Contains(errs[0].Error(), c.msg) {
				t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
			}

			// 同一个非法 spec 在 SAR 和 LocalSAR 两个入口中也必须被拒绝，
			// 因为二者都复用了 ValidateSubjectAccessReviewSpec。
			errs = ValidateSubjectAccessReview(&authorizationapi.SubjectAccessReview{Spec: c.obj})
			if len(errs) == 0 {
				t.Errorf("%s: expected failure for %q", c.name, c.msg)
			} else if !strings.Contains(errs[0].Error(), c.msg) {
				t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
			}
			errs = ValidateLocalSubjectAccessReview(&authorizationapi.LocalSubjectAccessReview{Spec: c.obj})
			if len(errs) == 0 {
				t.Errorf("%s: expected failure for %q", c.name, c.msg)
			} else if !strings.Contains(errs[0].Error(), c.msg) {
				t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
			}
		})
	}
}

// TestValidateSelfSAR 测试 SelfSubjectAccessReview 的校验。
// 由于主体隐含为当前用户，这里不需要 user/group，但仍必须满足属性二选一等约束。
func TestValidateSelfSAR(t *testing.T) {
	successCases := []authorizationapi.SelfSubjectAccessReviewSpec{
		{ResourceAttributes: &authorizationapi.ResourceAttributes{}},
	}
	for _, successCase := range successCases {
		if errs := ValidateSelfSubjectAccessReviewSpec(successCase, field.NewPath("spec")); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	errorCases := []struct {
		name string
		obj  authorizationapi.SelfSubjectAccessReviewSpec
		msg  string
	}{{ // neither request：属性均未指定
		name: "neither request",
		obj:  authorizationapi.SelfSubjectAccessReviewSpec{},
		msg:  "exactly one of nonResourceAttributes or resourceAttributes must be specified",
	}, { // both requests：两个属性同时指定
		name: "both requests",
		obj: authorizationapi.SelfSubjectAccessReviewSpec{
			ResourceAttributes:    &authorizationapi.ResourceAttributes{},
			NonResourceAttributes: &authorizationapi.NonResourceAttributes{},
		},
		msg: "exactly one of nonResourceAttributes or resourceAttributes must be specified",
	}, {
		// 只用一个选择器用例验证该校验函数确实被调用，详尽用例已在上面的测试中覆盖。
		name: "resource attributes: label selector specify both",
		obj: authorizationapi.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				LabelSelector: &authorizationapi.LabelSelectorAttributes{
					RawSelector: "foo",
					Requirements: []metav1.LabelSelectorRequirement{
						{},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.rawSelector: Invalid value: "foo": may not specified at the same time as requirements`,
	}}

	for _, c := range errorCases {
		errs := ValidateSelfSubjectAccessReviewSpec(c.obj, field.NewPath("spec"))
		if len(errs) == 0 {
			t.Errorf("%s: expected failure for %q", c.name, c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
		}

		// SelfSAR 对象入口也应复用同一 spec 校验，因此对相同输入同样报错。
		errs = ValidateSelfSubjectAccessReview(&authorizationapi.SelfSubjectAccessReview{Spec: c.obj})
		if len(errs) == 0 {
			t.Errorf("%s: expected failure for %q", c.name, c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
		}
	}
}

// TestValidateLocalSAR 测试 LocalSubjectAccessReview 的校验。
// 重点验证 metadata 仅允许 namespace、spec 中的 namespace 需与之一致，以及禁止非资源请求。
func TestValidateLocalSAR(t *testing.T) {
	successCases := []authorizationapi.LocalSubjectAccessReview{{
		Spec: authorizationapi.SubjectAccessReviewSpec{
			ResourceAttributes: &authorizationapi.ResourceAttributes{},
			User:               "user",
		},
	}}
	for _, successCase := range successCases {
		if errs := ValidateLocalSubjectAccessReview(&successCase); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	errorCases := []struct {
		name string
		obj  *authorizationapi.LocalSubjectAccessReview
		msg  string
	}{{ // name：除 namespace 外不允许携带 name 等元数据
		name: "name",
		obj: &authorizationapi.LocalSubjectAccessReview{
			ObjectMeta: metav1.ObjectMeta{Name: "a"},
			Spec: authorizationapi.SubjectAccessReviewSpec{
				ResourceAttributes: &authorizationapi.ResourceAttributes{},
				User:               "user",
			},
		},
		msg: "must be empty except for namespace",
	}, { // namespace conflict：spec 中的 namespace 与 metadata.namespace 不一致会导致作用域混淆
		name: "namespace conflict",
		obj: &authorizationapi.LocalSubjectAccessReview{
			ObjectMeta: metav1.ObjectMeta{Namespace: "a"},
			Spec: authorizationapi.SubjectAccessReviewSpec{
				ResourceAttributes: &authorizationapi.ResourceAttributes{},
				User:               "user",
			},
		},
		msg: "must match metadata.namespace",
	}, { // nonresource：本地请求必须有命名空间，非资源请求不属于任何命名空间
		name: "nonresource",
		obj: &authorizationapi.LocalSubjectAccessReview{
			ObjectMeta: metav1.ObjectMeta{Namespace: "a"},
			Spec: authorizationapi.SubjectAccessReviewSpec{
				NonResourceAttributes: &authorizationapi.NonResourceAttributes{},
				User:                  "user",
			},
		},
		msg: "disallowed on this kind of request",
	}, {
		// 只用一个选择器用例验证 spec 校验被复用，详尽用例已在上面的测试中覆盖。
		name: "resource attributes: label selector specify both",
		obj: &authorizationapi.LocalSubjectAccessReview{
			Spec: authorizationapi.SubjectAccessReviewSpec{
				User: "user",
				ResourceAttributes: &authorizationapi.ResourceAttributes{
					LabelSelector: &authorizationapi.LabelSelectorAttributes{
						RawSelector: "foo",
						Requirements: []metav1.LabelSelectorRequirement{
							{},
						},
					},
				},
			},
		},
		msg: `spec.resourceAttributes.labelSelector.rawSelector: Invalid value: "foo": may not specified at the same time as requirements`,
	}}

	for _, c := range errorCases {
		errs := ValidateLocalSubjectAccessReview(c.obj)
		if len(errs) == 0 {
			t.Errorf("%s: expected failure for %q", c.name, c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("%s: unexpected error: %q, expected: %q", c.name, errs[0], c.msg)
		}
	}
}
