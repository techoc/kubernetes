/*
Copyright 2015 The Kubernetes Authors.

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

package authorization

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SubjectAccessReview 用于检查某个用户或群组是否可以执行某个动作。
// 若未填写 spec.namespace，则表示“在所有命名空间中”进行检查。
type SubjectAccessReview struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存待评估请求的相关信息。
	Spec SubjectAccessReviewSpec

	// Status 由服务端填充，表示请求是否被允许。
	Status SubjectAccessReviewStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SelfSubjectAccessReview 用于检查当前用户是否可以执行某个动作。
// 若未填写 spec.namespace，则表示“在所有命名空间中”进行检查。
// Self 是一种特殊情形：用户应当始终能够检查自己是否可以执行某个动作。
type SelfSubjectAccessReview struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存待评估请求的相关信息。
	Spec SelfSubjectAccessReviewSpec

	// Status 由服务端填充，表示请求是否被允许。
	Status SubjectAccessReviewStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LocalSubjectAccessReview 用于检查某个用户或群组在给定命名空间内是否可以执行某个动作。
// 提供命名空间级别的资源，可以更方便地授予包含权限检查的命名空间级策略。
type LocalSubjectAccessReview struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存待评估请求的相关信息。spec.namespace 必须等于发起请求时所针对的命名空间；
	// 若为空，则会使用默认值填充。
	Spec SubjectAccessReviewSpec

	// Status 由服务端填充，表示请求是否被允许。
	Status SubjectAccessReviewStatus
}

// ResourceAttributes 包含资源请求可提供给 Authorizer 接口的授权属性。
type ResourceAttributes struct {
	// Namespace 是被请求动作所在的命名空间。当前，未指定命名空间与所有命名空间之间没有区别：
	// 对于 LocalSubjectAccessReview，""（空）会被填充为默认值；
	// 对于集群作用域资源，""（空）表示空；
	// 对于来自 SubjectAccessReview 或 SelfSubjectAccessReview 的命名空间作用域资源，""（空）表示“所有”。
	Namespace string
	// Verb 是 Kubernetes 资源 API 动词，例如 get、list、watch、create、update、delete、proxy，"*" 表示全部。
	Verb string
	// Group 是资源所属的 API Group，"*" 表示全部。
	Group string
	// Version 是资源的 API Version，"*" 表示全部。
	Version string
	// Resource 是已有的资源类型之一，"*" 表示全部。
	Resource string
	// Subresource 是已有的子资源类型之一，"" 表示没有子资源。
	Subresource string
	// Name 是 "get" 请求中被请求的资源名称，或 "delete" 请求中被删除的资源名称；""（空）表示全部。
	Name string
	// fieldSelector 描述基于字段对访问的限制。它只能缩小访问范围，不能扩大。
	// +optional
	FieldSelector *FieldSelectorAttributes
	// labelSelector 描述基于标签对访问的限制。它只能缩小访问范围，不能扩大。
	// +optional
	LabelSelector *LabelSelectorAttributes
}

// LabelSelectorAttributes 表示基于标签的访问限制。
// Webhook 作者应当：
//   - 确保不同时设置 rawSelector 和 requirements；
//   - 如果设置了 requirements，则考虑该字段；
//   - 如果设置了 rawSelector，不要尝试解析或考虑该字段。这是为了避免再次出现 CVE-2022-2880
//     （即不希望让不同系统就如何精确解析查询达成一致），详见 https://www.oxeye.io/resources/golang-parameter-smuggling-attack。
//
// 对于 kube-apiserver 的 *SubjectAccessReview 端点：
// * 若 rawSelector 为空且 requirements 为空，则请求不受限制；
// * 若存在 rawSelector 且 requirements 为空，则在解析成功的前提下解析并限制 rawSelector；
// * 若 rawSelector 为空且存在 requirements，则应当遵循 requirements；
// * 若 rawSelector 和 requirements 同时存在，则请求无效。
type LabelSelectorAttributes struct {
	// rawSelector 是会出现在查询参数中的字段选择器的序列化形式。
	// Webhook 实现应当忽略 rawSelector。
	// 只要 requirements 不存在，kube-apiserver 的 *SubjectAccessReview 就会解析 rawSelector。
	// +optional
	RawSelector string

	// requirements 是对标签选择器解析后的解释结果。
	// 资源实例必须满足所有条件才能匹配该选择器。
	// Webhook 实现应当处理 requirements，但具体如何处理由 Webhook 自行决定。
	// 由于 requirements 只能缩小请求范围，因此在不理解 requirements 时按不受限制的请求来授权是安全的。
	// +optional
	// +listType=atomic
	Requirements []metav1.LabelSelectorRequirement
}

// FieldSelectorAttributes 表示基于字段的访问限制。
// Webhook 作者应当：
//   - 确保不同时设置 rawSelector 和 requirements；
//   - 如果设置了 requirements，则考虑该字段；
//   - 如果设置了 rawSelector，不要尝试解析或考虑该字段。这是为了避免再次出现 CVE-2022-2880
//     （即不希望让不同系统就如何精确解析查询达成一致），详见 https://www.oxeye.io/resources/golang-parameter-smuggling-attack。
//
// 对于 kube-apiserver 的 *SubjectAccessReview 端点：
// * 若 rawSelector 为空且 requirements 为空，则请求不受限制；
// * 若存在 rawSelector 且 requirements 为空，则在解析成功的前提下解析并限制 rawSelector；
// * 若 rawSelector 为空且存在 requirements，则应当遵循 requirements；
// * 若 rawSelector 和 requirements 同时存在，则请求无效。
type FieldSelectorAttributes struct {
	// rawSelector 是会出现在查询参数中的字段选择器的序列化形式。
	// Webhook 实现应当忽略 rawSelector。
	// 只要 requirements 不存在，kube-apiserver 的 *SubjectAccessReview 就会解析 rawSelector。
	// +optional
	RawSelector string

	// requirements 是对字段选择器解析后的解释结果。
	// 资源实例必须满足所有条件才能匹配该选择器。
	// Webhook 实现应当处理 requirements，但具体如何处理由 Webhook 自行决定。
	// 由于 requirements 只能缩小请求范围，因此在不理解 requirements 时按不受限制的请求来授权是安全的。
	// +optional
	// +listType=atomic
	Requirements []metav1.FieldSelectorRequirement
}

// NonResourceAttributes 包含非资源请求可提供给 Authorizer 接口的授权属性。
type NonResourceAttributes struct {
	// Path 是请求的 URL 路径。
	Path string
	// Verb 是标准的 HTTP 动词。
	Verb string
}

// SubjectAccessReviewSpec 是对访问请求的描述。ResourceAttributes 和 NonResourceAttributes 必须且只能设置其中一个。
type SubjectAccessReviewSpec struct {
	// ResourceAttributes 描述资源访问请求的相关信息。
	ResourceAttributes *ResourceAttributes
	// NonResourceAttributes 描述非资源访问请求的相关信息。
	NonResourceAttributes *NonResourceAttributes

	// User 是待检查的用户。
	// 如果只指定 "User" 而不指定 "Group"，则会被解释为“假设该用户不属于任何群组会怎样”。
	User string
	// Groups 是待检查的群组。
	Groups []string
	// Extra 对应认证器中的 user.Info.GetExtra() 方法。由于它是授权器的输入，因此需要在此处有所体现。
	Extra map[string]ExtraValue
	// UID 是发起请求用户的 UID 信息。
	UID string
}

// ExtraValue 对值进行包装，以便 protobuf 能够生成代码。
// +protobuf.nullable=true
type ExtraValue []string

// SelfSubjectAccessReviewSpec 是对访问请求的描述。ResourceAttributes 和 NonResourceAttributes 必须且只能设置其中一个。
type SelfSubjectAccessReviewSpec struct {
	// ResourceAttributes 描述资源访问请求的相关信息。
	ResourceAttributes *ResourceAttributes
	// NonResourceAttributes 描述非资源访问请求的相关信息。
	NonResourceAttributes *NonResourceAttributes
}

// SubjectAccessReviewStatus 表示 SubjectAccessReview 的当前状态。
type SubjectAccessReviewStatus struct {
	// Allowed 为必填字段。若该动作会被允许则为 true，否则为 false。
	Allowed bool
	// Denied 为可选字段。若该动作会被拒绝则为 true，否则为 false。
	// 若 Allowed 和 Denied 同时为 false，则表示授权器对该动作是否应被授权没有意见。
	// 当 Allowed 为 true 时，Denied 不允许为 true。
	Denied bool
	// Reason 为可选字段，用于说明请求被允许或拒绝的原因。
	Reason string
	// EvaluationError 表示授权检查过程中发生了错误。
	// 即使发生错误，也完全有可能继续确定授权状态。
	// 例如 RBAC 可能缺少某个角色，但仍有足够的角色被绑定，足以推断出该请求的结果。
	EvaluationError string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SelfSubjectRulesReview 枚举当前用户在某个命名空间内可以执行的动作集合。
// 返回的动作列表可能不完整，这取决于服务端的授权模式以及评估过程中遇到的任何错误。
// SelfSubjectRulesReview 应用于 UI 显示或隐藏动作，或帮助最终用户快速了解自身权限。
// 不应被外部系统用于驱动授权决策，否则会引发“混淆代理（confused deputy）”、缓存生命周期/撤销以及正确性问题。
// 将授权决策委托给 API server 的正确方式是 SubjectAccessReview 和 LocalAccessReview。
type SelfSubjectRulesReview struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存待评估请求的相关信息。
	Spec SelfSubjectRulesReviewSpec

	// Status 由服务端填充，表示用户可以执行的动作集合。
	Status SubjectRulesReviewStatus
}

// SelfSubjectRulesReviewSpec 定义 SelfSubjectRulesReview 的规格。
type SelfSubjectRulesReviewSpec struct {
	// Namespace 是要评估规则所在的命名空间，必填。
	Namespace string
}

// SubjectRulesReviewStatus 包含规则检查的结果。根据服务端配置的授权器集合以及评估过程中遇到的任何错误，
// 该检查可能是不完整的。由于授权规则是叠加式的，若某条规则出现在列表中，就可以安全地认为主体拥有该权限，
// 即使该列表并不完整。
type SubjectRulesReviewStatus struct {
	// ResourceRules 是主体被允许对资源执行的动作列表。
	// 列表顺序无关紧要，可能包含重复项，也可能不完整。
	ResourceRules []ResourceRule
	// NonResourceRules 是主体被允许对非资源执行的动作列表。
	// 列表顺序无关紧要，可能包含重复项，也可能不完整。
	NonResourceRules []NonResourceRule
	// 当本次调用返回的规则不完整时，Incomplete 为 true。最常见的情况是某个授权器（例如外部授权器）
	// 不支持规则评估。
	Incomplete bool
	// EvaluationError 可以和 Rules 同时出现。它表示规则评估过程中发生了错误，例如某个授权器不支持规则评估，
	// 以及/或者 ResourceRules 与 NonResourceRules 可能不完整。
	EvaluationError string
}

// ResourceRule 是主体被允许对资源执行的动作列表。列表顺序无关紧要，可能包含重复项，也可能不完整。
type ResourceRule struct {
	// Verbs 是 Kubernetes 资源 API 动词列表，例如 get、list、watch、create、update、delete、proxy，"*" 表示全部。
	Verbs []string
	// APIGroups 是包含这些资源的 APIGroup 名称。若指定了多个 API group，则对其中任一 API group 中列举的资源
	// 发起的任何动作都将被允许，"*" 表示全部。
	APIGroups []string
	// Resources 是该规则适用的资源列表，"*" 表示指定 apiGroups 中的全部资源；
	// "*/foo" 表示指定 apiGroups 中所有资源的子资源 'foo'。
	Resources []string
	// ResourceNames 是规则适用的名称白名单（可选）。空集合表示允许全部，"*" 表示全部。
	ResourceNames []string
}

// NonResourceRule 保存用于描述非资源规则的信息。
type NonResourceRule struct {
	// Verbs 是 Kubernetes 非资源 API 动词列表，例如 get、post、put、delete、patch、head、options，"*" 表示全部。
	Verbs []string

	// NonResourceURLs 是用户应有权访问的部分 URL 集合。允许使用 *，但只能作为路径中完整的最后一级，"*" 表示全部。
	NonResourceURLs []string
}
