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

package fuzzer

import (
	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"

	"k8s.io/kubernetes/pkg/apis/admissionregistration"
)

// Funcs 返回 admissionregistration API 组的 fuzzer 函数集合。
// 这些函数会在通用随机填充之后被调用，用于把对象修正为满足 API 约束的合法状态，
// 从而让序列化往返（roundtrip）测试不会因为必填字段缺失而误报。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// Rule 的 scope 有默认值，随机填充可能留空；这里补上默认的全作用域，
		// 确保内部类型与版本化类型在往返测试中表现一致。
		func(obj *admissionregistration.Rule, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数造成死循环
			if obj.Scope == nil {
				s := admissionregistration.AllScopes
				obj.Scope = &s
			}
		},
		// ValidatingWebhook 的若干可选字段在版本化类型上有默认取值，
		// 这里显式补齐，防止随机生成nil导致默认值校验差异。
		func(obj *admissionregistration.ValidatingWebhook, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.FailurePolicy == nil {
				p := admissionregistration.FailurePolicyType("Fail")
				obj.FailurePolicy = &p
			}
			if obj.MatchPolicy == nil {
				m := admissionregistration.MatchPolicyType("Exact")
				obj.MatchPolicy = &m
			}
			if obj.SideEffects == nil {
				s := admissionregistration.SideEffectClassUnknown
				obj.SideEffects = &s
			}
			if obj.TimeoutSeconds == nil {
				i := int32(30)
				obj.TimeoutSeconds = &i
			}
			// AdmissionReviewVersions 是必填字段，固定为一个已知可序列化的值。
			obj.AdmissionReviewVersions = []string{"v1beta1"}
		},
		// MutatingWebhook 与 ValidatingWebhook 类似，但额外包含重新调用策略。
		func(obj *admissionregistration.MutatingWebhook, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.FailurePolicy == nil {
				p := admissionregistration.FailurePolicyType("Fail")
				obj.FailurePolicy = &p
			}
			if obj.MatchPolicy == nil {
				m := admissionregistration.MatchPolicyType("Exact")
				obj.MatchPolicy = &m
			}
			if obj.SideEffects == nil {
				s := admissionregistration.SideEffectClassUnknown
				obj.SideEffects = &s
			}
			if obj.ReinvocationPolicy == nil {
				r := admissionregistration.NeverReinvocationPolicy
				obj.ReinvocationPolicy = &r
			}
			if obj.TimeoutSeconds == nil {
				i := int32(30)
				obj.TimeoutSeconds = &i
			}
			// AdmissionReviewVersions 是必填字段，固定为一个已知可序列化的值。
			obj.AdmissionReviewVersions = []string{"v1beta1"}
		},
		// ValidatingAdmissionPolicySpec 的 failurePolicy 有默认值，补齐以防往返测试不一致。
		func(obj *admissionregistration.ValidatingAdmissionPolicySpec, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.FailurePolicy == nil {
				p := admissionregistration.FailurePolicyType("Fail")
				obj.FailurePolicy = &p
			}
		},
		// ValidatingAdmissionPolicyBindingSpec 的 validationActions 有默认值（Deny），补齐保证往返一致。
		func(obj *admissionregistration.ValidatingAdmissionPolicyBindingSpec, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.ValidationActions == nil {
				obj.ValidationActions = []admissionregistration.ValidationAction{admissionregistration.Deny}
			}
		},
		// MatchResources 的 matchPolicy 有默认值，补齐保证内部类型和版本化类型一致。
		func(obj *admissionregistration.MatchResources, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.MatchPolicy == nil {
				m := admissionregistration.MatchPolicyType("Exact")
				obj.MatchPolicy = &m
			}
		},
		// ParamRef 的 parameterNotFoundAction 是必填字段，随机填充可能留空，需补默认值。
		func(obj *admissionregistration.ParamRef, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数

			// 填充必填字段，避免校验失败。
			if obj.ParameterNotFoundAction == nil {
				v := admissionregistration.DenyAction
				obj.ParameterNotFoundAction = &v
			}
		},
		// MutatingAdmissionPolicySpec 的 failurePolicy 和 reinvocationPolicy 均有默认值，需补齐。
		func(obj *admissionregistration.MutatingAdmissionPolicySpec, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.FailurePolicy == nil {
				p := admissionregistration.FailurePolicyType("Fail")
				obj.FailurePolicy = &p
			}
			obj.ReinvocationPolicy = admissionregistration.NeverReinvocationPolicy
		},
		// Mutation 中 JSONPatch 和 ApplyConfiguration 互斥；随机选中一种类型后，
		// 只保留对应字段，避免出现同时设置两个补丁源的非法对象。
		func(obj *admissionregistration.Mutation, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			patchTypes := []admissionregistration.PatchType{admissionregistration.PatchTypeJSONPatch, admissionregistration.PatchTypeApplyConfiguration}
			obj.PatchType = patchTypes[c.Rand.Intn(len(patchTypes))]
			if obj.PatchType == admissionregistration.PatchTypeJSONPatch {
				obj.JSONPatch = &admissionregistration.JSONPatch{}
				c.Fill(&obj.JSONPatch)
				obj.ApplyConfiguration = nil
			}
			if obj.PatchType == admissionregistration.PatchTypeApplyConfiguration {
				obj.ApplyConfiguration = &admissionregistration.ApplyConfiguration{}
				c.Fill(obj.ApplyConfiguration)
				obj.JSONPatch = nil
			}
		},
	}
}
