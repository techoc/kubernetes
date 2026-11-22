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

package mutating

import (
	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
)

// NewMutatingAdmissionPolicyAccessor 把内部的 Policy 包装成 generic.PolicyAccessor，
// 使通用 PolicySource 无需依赖 mutating 包的具体类型。
func NewMutatingAdmissionPolicyAccessor(obj *Policy) generic.PolicyAccessor {
	return &mutatingAdmissionPolicyAccessor{
		Policy: obj,
	}
}

// NewMutatingAdmissionPolicyBindingAccessor 把内部的 PolicyBinding 包装成 generic.BindingAccessor。
func NewMutatingAdmissionPolicyBindingAccessor(obj *PolicyBinding) generic.BindingAccessor {
	return &mutatingAdmissionPolicyBindingAccessor{
		PolicyBinding: obj,
	}
}

// mutatingAdmissionPolicyAccessor 以嵌入方式复用 Policy 的字段，只做视图适配。
type mutatingAdmissionPolicyAccessor struct {
	*Policy
}

// GetNamespace 返回 policy 所在命名空间；MutatingAdmissionPolicy 是集群级资源，通常为空。
func (v *mutatingAdmissionPolicyAccessor) GetNamespace() string {
	return v.Namespace
}

// GetName 返回 policy 名称。
func (v *mutatingAdmissionPolicyAccessor) GetName() string {
	return v.Name
}

// GetParamKind 返回参数类型声明。这里复制到 v1.ParamKind 而不是直接返回内部类型指针，
// 因为 generic 层只认识 v1 类型。
func (v *mutatingAdmissionPolicyAccessor) GetParamKind() *v1.ParamKind {
	pk := v.Spec.ParamKind
	if pk == nil {
		return nil
	}
	return &v1.ParamKind{
		APIVersion: pk.APIVersion,
		Kind:       pk.Kind,
	}
}

// GetMatchConstraints 返回 policy 的匹配约束。
func (v *mutatingAdmissionPolicyAccessor) GetMatchConstraints() *v1.MatchResources {
	return v.Spec.MatchConstraints
}

// GetFailurePolicy 返回求值失败时的策略（Fail 或 Ignore）。
func (v *mutatingAdmissionPolicyAccessor) GetFailurePolicy() *v1.FailurePolicyType {
	return v.Spec.FailurePolicy
}

// mutatingAdmissionPolicyBindingAccessor 以嵌入方式复用 PolicyBinding 的字段。
type mutatingAdmissionPolicyBindingAccessor struct {
	*PolicyBinding
}

// GetNamespace 返回 binding 所在命名空间。
func (v *mutatingAdmissionPolicyBindingAccessor) GetNamespace() string {
	return v.Namespace
}

// GetName 返回 binding 名称。
func (v *mutatingAdmissionPolicyBindingAccessor) GetName() string {
	return v.Name
}

// GetPolicyName 返回 binding 指向的 policy。命名空间刻意留空：
// MutatingAdmissionPolicy 是集群级资源，binding 只能引用集群级 policy。
func (v *mutatingAdmissionPolicyBindingAccessor) GetPolicyName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: "",
		Name:      v.Spec.PolicyName,
	}
}

// GetMatchResources 返回 binding 的资源匹配规则。
func (v *mutatingAdmissionPolicyBindingAccessor) GetMatchResources() *v1.MatchResources {
	return v.Spec.MatchResources
}

// GetParamRef 返回参数引用。复制字段是为了把内部的 ParameterNotFoundActionType
// 转成 v1 类型；未设置时保持 nil，以免误触发“参数缺失”的处理逻辑。
func (v *mutatingAdmissionPolicyBindingAccessor) GetParamRef() *v1.ParamRef {
	if v.Spec.ParamRef == nil {
		return nil
	}

	var nfa *v1.ParameterNotFoundActionType
	if v.Spec.ParamRef.ParameterNotFoundAction != nil {
		nfa = new(v1.ParameterNotFoundActionType)
		*nfa = v1.ParameterNotFoundActionType(*v.Spec.ParamRef.ParameterNotFoundAction)
	}

	return &v1.ParamRef{
		Name:                    v.Spec.ParamRef.Name,
		Namespace:               v.Spec.ParamRef.Namespace,
		Selector:                v.Spec.ParamRef.Selector,
		ParameterNotFoundAction: nfa,
	}
}
