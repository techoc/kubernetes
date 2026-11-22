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

// Package validating 实现 ValidatingAdmissionPolicy 准入插件，包括策略与
// 绑定的编译、匹配以及 CEL 校验逻辑。
package validating

import (
	"k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
)

// NewValidatingAdmissionPolicyAccessor 用类型化的 ValidatingAdmissionPolicy
// 包装出通用层使用的 PolicyAccessor。使用访问器而非直接依赖 v1 类型，
// 可让通用的策略调度代码与具体 API 版本解耦。
func NewValidatingAdmissionPolicyAccessor(obj *v1.ValidatingAdmissionPolicy) generic.PolicyAccessor {
	return &validatingAdmissionPolicyAccessor{
		ValidatingAdmissionPolicy: obj,
	}
}

// NewValidatingAdmissionPolicyBindingAccessor 用类型化的绑定对象构造
// BindingAccessor。绑定把策略与参数、匹配范围连接起来，通用层只通过
// 该访问器读取所需字段。
func NewValidatingAdmissionPolicyBindingAccessor(obj *v1.ValidatingAdmissionPolicyBinding) generic.BindingAccessor {
	return &validatingAdmissionPolicyBindingAccessor{
		ValidatingAdmissionPolicyBinding: obj,
	}
}

// validatingAdmissionPolicyAccessor 以嵌入方式复用 v1 对象字段，同时
// 实现 generic.PolicyAccessor。嵌入是零拷贝的，不改变对象的序列化行为。
type validatingAdmissionPolicyAccessor struct {
	*v1.ValidatingAdmissionPolicy
}

// GetNamespace 返回策略所在命名空间。ValidatingAdmissionPolicy 是集群级
// 资源，因此该值通常为空字符串。
func (v *validatingAdmissionPolicyAccessor) GetNamespace() string {
	return v.Namespace
}

// GetName 返回策略名称，用于绑定解析与错误信息。
func (v *validatingAdmissionPolicyAccessor) GetName() string {
	return v.Name
}

// GetParamKind 返回参数对象的 GVK；为 nil 表示策略不接受参数。
func (v *validatingAdmissionPolicyAccessor) GetParamKind() *v1.ParamKind {
	return v.Spec.ParamKind
}

// GetMatchConstraints 返回策略自身的匹配约束。策略与绑定的匹配条件
// 取交集后才决定是否执行。
func (v *validatingAdmissionPolicyAccessor) GetMatchConstraints() *v1.MatchResources {
	return v.Spec.MatchConstraints
}

// GetFailurePolicy 返回失败策略（Fail 或 Ignore），决定 CEL 求值出错时
// 是拒绝请求还是放行。
func (v *validatingAdmissionPolicyAccessor) GetFailurePolicy() *v1.FailurePolicyType {
	return v.Spec.FailurePolicy
}

// validatingAdmissionPolicyBindingAccessor 同样是 v1 对象的嵌入式包装，
// 实现 generic.BindingAccessor。
type validatingAdmissionPolicyBindingAccessor struct {
	*v1.ValidatingAdmissionPolicyBinding
}

// GetNamespace 返回绑定所在的命名空间。
func (v *validatingAdmissionPolicyBindingAccessor) GetNamespace() string {
	return v.Namespace
}

// GetName 返回绑定名称，用于去重和指标标识。
func (v *validatingAdmissionPolicyBindingAccessor) GetName() string {
	return v.Name
}

// GetPolicyName 返回绑定所引用的策略名。策略是集群级资源，因此命名空间
// 恒为空；显式写出空命名空间是为了避免调用方误以为可以按命名空间限定。
func (v *validatingAdmissionPolicyBindingAccessor) GetPolicyName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: "",
		Name:      v.Spec.PolicyName,
	}
}

// GetMatchResources 返回绑定级的匹配资源约束；与策略级约束共同决定
// 生效范围。
func (v *validatingAdmissionPolicyBindingAccessor) GetMatchResources() *v1.MatchResources {
	return v.Spec.MatchResources
}

// GetParamRef 返回参数引用；nil 表示该绑定不向策略传参。
func (v *validatingAdmissionPolicyBindingAccessor) GetParamRef() *v1.ParamRef {
	return v.Spec.ParamRef
}
