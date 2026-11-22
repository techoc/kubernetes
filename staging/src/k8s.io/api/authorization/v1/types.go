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

package v1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6

// SubjectAccessReview 检查某个用户或组是否有权执行某动作。
// +k8s:supportsSubresource="/status"
type SubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估请求的信息。
	Spec SubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，指示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// SelfSubjectAccessReview 检查当前用户是否有权执行某动作。未填写 spec.namespace 表示"所有命名空间"。
// Self 是特例，因为用户应始终能够检查自己是否有权执行某动作。
// +k8s:supportsSubresource="/status"
type SelfSubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估请求的信息。user 和 groups 必须为空。
	Spec SelfSubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，指示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// LocalSubjectAccessReview 检查某个用户或组在给定命名空间内是否有权执行某动作。
// 使用命名空间级资源，可更容易地授予包含权限检查的命名空间级策略。
// +k8s:supportsSubresource="/status"
type LocalSubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估请求的信息。spec.namespace 必须等于发起请求所针对的命名空间。
	// 若为空，会被默认填充。
	Spec SubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，指示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourceAttributes 包含传给 Authorizer 接口的资源请求可用的授权属性。
type ResourceAttributes struct {
	// namespace 是被请求动作所在的命名空间。目前不区分"无命名空间"和"所有命名空间"。
	// 对 LocalSubjectAccessReview，""（空）会被默认填充。
	// 对集群级资源，""（空）表示空。
	// 对来自 SubjectAccessReview 或 SelfSubjectAccessReview 的命名空间级资源，""（空）表示"所有"。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
	// verb 是 Kubernetes 资源 API 的动作，如 get、list、watch、create、update、delete、proxy。"*" 表示全部。
	// +optional
	Verb string `json:"verb,omitempty" protobuf:"bytes,2,opt,name=verb"`
	// group 是资源的 API 组。"*" 表示全部。
	// +optional
	Group string `json:"group,omitempty" protobuf:"bytes,3,opt,name=group"`
	// version 是资源的 API 版本。"*" 表示全部。
	// +optional
	Version string `json:"version,omitempty" protobuf:"bytes,4,opt,name=version"`
	// resource 是现有资源类型之一。"*" 表示全部。
	// +optional
	Resource string `json:"resource,omitempty" protobuf:"bytes,5,opt,name=resource"`
	// subresource 是现有子资源类型之一。"" 表示无。
	// +optional
	Subresource string `json:"subresource,omitempty" protobuf:"bytes,6,opt,name=subresource"`
	// name 是 "get" 请求的资源名或 "delete" 要删除的资源名。""（空）表示全部。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,7,opt,name=name"`
	// fieldSelector 描述基于字段的访问权限限制，只能收窄访问，不能放宽。
	// +optional
	FieldSelector *FieldSelectorAttributes `json:"fieldSelector,omitempty" protobuf:"bytes,8,opt,name=fieldSelector"`
	// labelSelector 描述基于标签的访问权限限制，只能收窄访问，不能放宽。
	// +optional
	LabelSelector *LabelSelectorAttributes `json:"labelSelector,omitempty" protobuf:"bytes,9,opt,name=labelSelector"`
}

// LabelSelectorAttributes 表示基于标签的受限访问。
// Webhook 作者应注意：
//   - 不要同时设置 rawSelector 和 requirements
//   - 若设置了 requirements，应考虑该字段
//   - 若设置了 rawSelector，不要尝试解析或考虑它。这是为了避免再次出现 CVE-2022-2880
//     （让不同系统就查询的具体解析方式达成一致不是我们想要的），详见
//     https://www.oxeye.io/resources/golang-parameter-smuggling-attack。
//
// 对 kube-apiserver 的 *SubjectAccessReview 端点：
// * rawSelector 和 requirements 都为空时，请求不受限制。
// * 仅有 rawSelector 时，会尝试解析它，成功则据此限制。
// * 仅有 requirements 时，应遵守这些要求。
// * 两者同时存在时，请求无效。
type LabelSelectorAttributes struct {
	// rawSelector 是会包含在查询参数中的字段选择器序列化形式。
	// 建议 Webhook 实现忽略 rawSelector。
	// kube-apiserver 的 *SubjectAccessReview 会在 requirements 不存在时解析 rawSelector。
	// +optional
	RawSelector string `json:"rawSelector,omitempty" protobuf:"bytes,1,opt,name=rawSelector"`

	// requirements 是标签选择器解析后的解释。
	// 资源实例需满足所有要求才能匹配该选择器。
	// Webhook 实现应处理 requirements，但具体处理方式由 Webhook 自行决定。
	// 由于 requirements 只能收窄请求，若无法理解这些要求，按不受限制的请求授权是安全的。
	// +optional
	// +listType=atomic
	Requirements []metav1.LabelSelectorRequirement `json:"requirements,omitempty" protobuf:"bytes,2,rep,name=requirements"`
}

// FieldSelectorAttributes 表示基于字段的受限访问。
// Webhook 作者应注意：
//   - 不要同时设置 rawSelector 和 requirements
//   - 若设置了 requirements，应考虑该字段
//   - 若设置了 rawSelector，不要尝试解析或考虑它。这是为了避免再次出现 CVE-2022-2880
//     （让不同系统就查询的具体解析方式达成一致不是我们想要的），详见
//     https://www.oxeye.io/resources/golang-parameter-smuggling-attack。
//
// 对 kube-apiserver 的 *SubjectAccessReview 端点：
// * rawSelector 和 requirements 都为空时，请求不受限制。
// * 仅有 rawSelector 时，会尝试解析它，成功则据此限制。
// * 仅有 requirements 时，应遵守这些要求。
// * 两者同时存在时，请求无效。
type FieldSelectorAttributes struct {
	// rawSelector 是会包含在查询参数中的字段选择器序列化形式。
	// 建议 Webhook 实现忽略 rawSelector。
	// kube-apiserver 的 *SubjectAccessReview 会在 requirements 不存在时解析 rawSelector。
	// +optional
	RawSelector string `json:"rawSelector,omitempty" protobuf:"bytes,1,opt,name=rawSelector"`

	// requirements 是字段选择器解析后的解释。
	// 资源实例需满足所有要求才能匹配该选择器。
	// Webhook 实现应处理 requirements，但具体处理方式由 Webhook 自行决定。
	// 由于 requirements 只能收窄请求，若无法理解这些要求，按不受限制的请求授权是安全的。
	// +optional
	// +listType=atomic
	Requirements []metav1.FieldSelectorRequirement `json:"requirements,omitempty" protobuf:"bytes,2,rep,name=requirements"`
}

// NonResourceAttributes 包含传给 Authorizer 接口的非资源请求可用的授权属性。
type NonResourceAttributes struct {
	// path 是请求的 URL 路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
	// verb 是标准的 HTTP 动词。
	// +optional
	Verb string `json:"verb,omitempty" protobuf:"bytes,2,opt,name=verb"`
}

// SubjectAccessReviewSpec 描述访问请求。resourceAttributes 和 nonResourceAttributes 必须且只能设置一个。
type SubjectAccessReviewSpec struct {
	// resourceAttributes 描述资源访问请求的信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	ResourceAttributes *ResourceAttributes `json:"resourceAttributes,omitempty" protobuf:"bytes,1,opt,name=resourceAttributes"`
	// nonResourceAttributes 描述非资源访问请求的信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	NonResourceAttributes *NonResourceAttributes `json:"nonResourceAttributes,omitempty" protobuf:"bytes,2,opt,name=nonResourceAttributes"`

	// user 是要检查的用户。
	// 若只指定 "User" 而不指定 "Groups"，会解释为"假设该用户不属于任何组"。
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,3,opt,name=user"`
	// groups 是要检查的组。
	// +optional
	// +listType=atomic
	Groups []string `json:"groups,omitempty" protobuf:"bytes,4,rep,name=groups"`
	// extra 对应认证器中的 user.Info.GetExtra() 方法。由于它是授权器的输入，这里需要体现出来。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,5,rep,name=extra"`
	// uid 是请求用户的信息。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,6,opt,name=uid"`
}

// ExtraValue 对值做一层包装，使 protobuf 能够生成代码。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

// String 返回 ExtraValue 的字符串表示。
func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// SelfSubjectAccessReviewSpec 描述访问请求。resourceAttributes 和 nonResourceAttributes 必须且只能设置一个。
type SelfSubjectAccessReviewSpec struct {
	// resourceAttributes 描述资源访问请求的信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	ResourceAttributes *ResourceAttributes `json:"resourceAttributes,omitempty" protobuf:"bytes,1,opt,name=resourceAttributes"`
	// nonResourceAttributes 描述非资源访问请求的信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	NonResourceAttributes *NonResourceAttributes `json:"nonResourceAttributes,omitempty" protobuf:"bytes,2,opt,name=nonResourceAttributes"`
}

// SubjectAccessReviewStatus 是 SubjectAccessReview 的状态结果。
type SubjectAccessReviewStatus struct {
	// allowed 必填。动作被允许时为 true，否则为 false。
	Allowed bool `json:"allowed" protobuf:"varint,1,opt,name=allowed"`
	// denied 可选。动作被拒绝时为 true，否则为 false。
	// 若 allowed 和 denied 都为 false，表示授权器对该动作是否授权没有意见。
	// Allowed 为 true 时 Denied 不能为 true。
	// +optional
	Denied bool `json:"denied,omitempty" protobuf:"varint,4,opt,name=denied"`
	// reason 可选，说明请求被允许或拒绝的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`
	// evaluationError 表示授权检查过程中发生了错误。
	// 即使出现错误，也完全可能继续确定授权状态。
	// 例如 RBAC 可能缺少某个角色，但仍有足够的角色且已绑定，足以判断该请求。
	// +optional
	EvaluationError string `json:"evaluationError,omitempty" protobuf:"bytes,3,opt,name=evaluationError"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// SelfSubjectRulesReview 枚举当前用户在某命名空间内可执行的一组动作。
// 返回的动作列表可能不完整，取决于服务端的授权模式以及评估过程中遇到的错误。
// UI 可用 SelfSubjectRulesReview 显示或隐藏动作，或让终端用户快速了解自己的权限。
// 外部系统不应据此驱动授权决策，否则会引发混淆代理、缓存生命周期/撤销以及正确性问题。
// 将授权决策交给 API server 的正确方式是 SubjectAccessReview 和 LocalAccessReview。
// +k8s:supportsSubresource="/status"
type SelfSubjectRulesReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估请求的信息。
	Spec SelfSubjectRulesReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，指示用户可执行的一组动作。
	// +optional
	Status SubjectRulesReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// SelfSubjectRulesReviewSpec 定义 SelfSubjectRulesReview 的规格。
type SelfSubjectRulesReviewSpec struct {
	// 要评估规则所在的命名空间，必填。
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
}

// SubjectRulesReviewStatus 包含规则检查的结果。检查结果可能不完整，取决于服务端配置的授权器集合
// 以及评估过程中遇到的错误。由于授权规则是叠加的，即使列表不完整，只要某规则出现在列表中，
// 就可以安全地认为主体拥有该权限。
type SubjectRulesReviewStatus struct {
	// resourceRules 是主体被允许对资源执行的动作列表。
	// 列表顺序无意义，可能包含重复项，也可能不完整。
	// +listType=atomic
	ResourceRules []ResourceRule `json:"resourceRules" protobuf:"bytes,1,rep,name=resourceRules"`
	// nonResourceRules 是主体被允许对非资源执行的动作列表。
	// 列表顺序无意义，可能包含重复项，也可能不完整。
	// +listType=atomic
	NonResourceRules []NonResourceRule `json:"nonResourceRules" protobuf:"bytes,2,rep,name=nonResourceRules"`
	// incomplete 为 true 表示本次调用返回的规则不完整。最常见的原因是
	// 某个授权器（如外部授权器）不支持规则评估。
	Incomplete bool `json:"incomplete" protobuf:"bytes,3,rep,name=incomplete"`
	// evaluationError 可与 Rules 同时出现，表示规则评估过程中发生了错误，
	// 例如某授权器不支持规则评估，且 ResourceRules 和/或 NonResourceRules 可能不完整。
	// +optional
	EvaluationError string `json:"evaluationError,omitempty" protobuf:"bytes,4,opt,name=evaluationError"`
}

// ResourceRule 是主体被允许对资源执行的动作列表。列表顺序无意义，可能包含重复项，也可能不完整。
type ResourceRule struct {
	// verbs 是 Kubernetes 资源 API 动词列表，如 get、list、watch、create、update、delete、proxy。"*" 表示全部。
	// +listType=atomic
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// apiGroups 是包含这些资源的 APIGroup 名称。若指定多个 API 组，
	// 对其中任意组中列举的资源执行的任意动作都会被允许。"*" 表示全部。
	// +optional
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,2,rep,name=apiGroups"`
	// resources 是本规则适用的资源列表。"*" 表示指定 apiGroups 中的全部资源。
	// "*/foo" 表示指定 apiGroups 中所有资源的子资源 'foo'。
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`
	// resourceNames 是本规则适用名称的可选白名单。空集表示允许所有。"*" 表示全部。
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,4,rep,name=resourceNames"`
}

// NonResourceRule 保存描述非资源规则的信息。
type NonResourceRule struct {
	// verbs 是 Kubernetes 非资源 API 动词列表，如 get、post、put、delete、patch、head、options。"*" 表示全部。
	// +listType=atomic
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// nonResourceURLs 是用户应有权访问的部分 URL 集合。允许使用 *，但只能作为路径中完整的最后一段。"*" 表示全部。
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,2,rep,name=nonResourceURLs"`
}
