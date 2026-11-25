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

package generic

import (
	"k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PolicyAccessor 是通用分发器所需的准入策略的窄视图。
// ValidatingAdmissionPolicy 与 MutatingAdmissionPolicy 都实现了它，
// 因此两个插件可以共享同一套分发器逻辑。
type PolicyAccessor interface {
	// GetName 返回策略名称（集群级）。
	GetName() string
	// 对现有集群级策略而言 GetNamespace 恒为空；保留它是为了将来
	// 出现命名空间级策略变体时可以复用该接口。
	GetNamespace() string
	// GetParamKind 描述策略 param 资源的 Kind；策略不带参数时为 nil。
	GetParamKind() *v1.ParamKind
	// GetMatchConstraints 限定策略适用于哪些请求。
	GetMatchConstraints() *v1.MatchResources
	// GetFailurePolicy 决定求值失败时的行为；nil 表示 Fail。
	GetFailurePolicy() *v1.FailurePolicyType
}

// BindingAccessor 是通用分发器所需的策略绑定的窄视图。
// ValidatingAdmissionPolicyBinding 与 MutatingAdmissionPolicyBinding
// 都实现了它。
type BindingAccessor interface {
	GetName() string
	GetNamespace() string

	// GetPolicyName 返回 (Validating/Mutating)AdmissionPolicy 的名字；该策略
	// 是集群级的，因此命名空间通常留空，但也为将来的命名空间级版本留了余地。
	GetPolicyName() types.NamespacedName
	// GetParamRef 指向该绑定所提供的 param 对象（若有）。
	GetParamRef() *v1.ParamRef

	// GetMatchResources 在策略的匹配约束基础上进一步收窄范围。
	GetMatchResources() *v1.MatchResources
}
