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

package v1beta1

import (
	"fmt"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=authorization.k8s.io,v1,SubjectAccessReview

// SubjectAccessReview 用于检查某个用户或组是否可以执行某项操作。
// +k8s:supportsSubresource="/status"
type SubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息。
	Spec SubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，表示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=authorization.k8s.io,v1,SelfSubjectAccessReview

// SelfSubjectAccessReview 用于检查当前用户是否可以执行某项操作。未填写 spec.namespace 表示“在所有命名空间中”。
// self 是一种特殊情况，因为用户应始终能够检查自己是否可以执行某项操作。
// +k8s:supportsSubresource="/status"
type SelfSubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息。user 和 groups 必须为空。
	Spec SelfSubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，表示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=authorization.k8s.io,v1,LocalSubjectAccessReview

// LocalSubjectAccessReview 用于检查某个用户或组是否可以在给定命名空间中执行某项操作。
// 使用命名空间作用域的资源，可以更轻松地授予包含权限检查的命名空间作用域策略。
// +k8s:supportsSubresource="/status"
type LocalSubjectAccessReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息。spec.namespace 必须等于发起请求时所针对的命名空间。
	// 若为空，则使用默认值。
	Spec SubjectAccessReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，表示请求是否被允许。
	// +optional
	Status SubjectAccessReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourceAttributes 包含可供 Authorizer 接口用于资源请求的授权属性。
type ResourceAttributes struct {
	// namespace 是所请求操作所在的命名空间。目前，未指定命名空间与所有命名空间之间没有区别：
	// 对于 LocalSubjectAccessReviews，""（空）会被填充为默认值；
	// 对于集群作用域资源，""（空）表示空；
	// 对于来自 SubjectAccessReview 或 SelfSubjectAccessReview 的命名空间作用域资源，""（空）表示“全部”。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
	// verb 是 Kubernetes 资源 API 动词，例如 get、list、watch、create、update、delete、proxy。"*" 表示全部。
	// +optional
	Verb string `json:"verb,omitempty" protobuf:"bytes,2,opt,name=verb"`
	// group 是资源所属的 API 组。"*" 表示全部。
	// +optional
	Group string `json:"group,omitempty" protobuf:"bytes,3,opt,name=group"`
	// version 是资源的 API 版本。"*" 表示全部。
	// +optional
	Version string `json:"version,omitempty" protobuf:"bytes,4,opt,name=version"`
	// resource 是现有资源类型之一。"*" 表示全部。
	// +optional
	Resource string `json:"resource,omitempty" protobuf:"bytes,5,opt,name=resource"`
	// subresource 是现有子资源类型之一。"" 表示无子资源。
	// +optional
	Subresource string `json:"subresource,omitempty" protobuf:"bytes,6,opt,name=subresource"`
	// name 是 "get" 请求所请求的资源名称，或 "delete" 操作所删除的资源名称。""（空）表示全部。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,7,opt,name=name"`
	// fieldSelector 描述基于字段的访问权限限制。它只能缩小访问范围，不能扩大。
	// +optional
	FieldSelector *authorizationv1.FieldSelectorAttributes `json:"fieldSelector,omitempty" protobuf:"bytes,8,opt,name=fieldSelector"`
	// labelSelector 描述基于标签的访问权限限制。它只能缩小访问范围，不能扩大。
	// +optional
	LabelSelector *authorizationv1.LabelSelectorAttributes `json:"labelSelector,omitempty" protobuf:"bytes,9,opt,name=labelSelector"`
}

// NonResourceAttributes 包含可供 Authorizer 接口用于非资源请求的授权属性。
type NonResourceAttributes struct {
	// path 是请求的 URL 路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
	// verb 是标准的 HTTP 动词。
	// +optional
	Verb string `json:"verb,omitempty" protobuf:"bytes,2,opt,name=verb"`
}

// SubjectAccessReviewSpec 是对访问请求的描述。resourceAttributes 和 nonResourceAttributes 必须且只能设置其中一个。
type SubjectAccessReviewSpec struct {
	// resourceAttributes 描述资源访问请求的相关信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	ResourceAttributes *ResourceAttributes `json:"resourceAttributes,omitempty" protobuf:"bytes,1,opt,name=resourceAttributes"`
	// nonResourceAttributes 描述非资源访问请求的相关信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	NonResourceAttributes *NonResourceAttributes `json:"nonResourceAttributes,omitempty" protobuf:"bytes,2,opt,name=nonResourceAttributes"`

	// user 是被测试的用户。
	// 如果只指定 "User" 而未指定 "Group"，则解释为“假设 User 不属于任何组会怎样”。
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,3,opt,name=user"`
	// group 是被测试的组。
	// +optional
	// +listType=atomic
	Groups []string `json:"group,omitempty" protobuf:"bytes,4,rep,name=group"`
	// extra 对应认证器中的 user.Info.GetExtra() 方法。由于它也是授权器的输入，因此需要在此处体现。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,5,rep,name=extra"`
	// uid 是发起请求用户的信息。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,6,opt,name=uid"`
}

// ExtraValue 对值进行包装，以便 protobuf 生成代码。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// SelfSubjectAccessReviewSpec 是对访问请求的描述。resourceAttributes 和 nonResourceAttributes 必须且只能设置其中一个。
type SelfSubjectAccessReviewSpec struct {
	// resourceAttributes 描述资源访问请求的相关信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	ResourceAttributes *ResourceAttributes `json:"resourceAttributes,omitempty" protobuf:"bytes,1,opt,name=resourceAttributes"`
	// nonResourceAttributes 描述非资源访问请求的相关信息。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:unionMember
	NonResourceAttributes *NonResourceAttributes `json:"nonResourceAttributes,omitempty" protobuf:"bytes,2,opt,name=nonResourceAttributes"`
}

// SubjectAccessReviewStatus 保存主体访问审查的状态。
type SubjectAccessReviewStatus struct {
	// allowed 是必填项。若该操作将被允许则为 true，否则为 false。
	Allowed bool `json:"allowed" protobuf:"varint,1,opt,name=allowed"`
	// denied 是可选项。若该操作将被拒绝则为 true，否则为 false。
	// 如果 allowed 和 denied 均为 false，则表示授权器对该操作是否授权没有意见。
	// 当 Allowed 为 true 时，Denied 不可为 true。
	// +optional
	Denied bool `json:"denied,omitempty" protobuf:"varint,4,opt,name=denied"`
	// reason 是可选项，说明请求被允许或拒绝的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`
	// evaluationError 表示授权检查过程中发生了错误。
	// 完全有可能在发生错误的情况下仍能继续确定授权状态。
	// 例如，RBAC 可能缺少某个角色，但仍有足够的角色和绑定可用于推断该请求。
	// +optional
	EvaluationError string `json:"evaluationError,omitempty" protobuf:"bytes,3,opt,name=evaluationError"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=authorization.k8s.io,v1,SelfSubjectRulesReview

// SelfSubjectRulesReview 枚举当前用户在某个命名空间内可以执行的操作集合。
// 返回的动作列表可能不完整，具体取决于服务端的授权模式以及评估过程中遇到的任何错误。
// UI 可使用 SelfSubjectRulesReview 来显示或隐藏操作，或让最终用户快速了解自己的权限。
// 不应将其被外部系统用于驱动授权决策，因为这会引发权责混淆、缓存生命周期/撤销以及正确性问题。
// 将授权决策交由 API 服务端处理时，正确的方式是使用 SubjectAccessReview 和 LocalAccessReview。
// +k8s:supportsSubresource="/status"
type SelfSubjectRulesReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息。
	Spec SelfSubjectRulesReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填充，表示用户可以执行的操作集合。
	// +optional
	Status SubjectRulesReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// SelfSubjectRulesReviewSpec 定义 SelfSubjectRulesReview 的规格。
type SelfSubjectRulesReviewSpec struct {
	// 要评估规则所在的命名空间。必填。
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
}

// SubjectRulesReviewStatus 包含规则检查的结果。根据服务端配置的授权器集合以及评估过程中遇到的任何错误，
// 该检查可能不完整。由于授权规则是累加的，如果某条规则出现在列表中，即使列表不完整，
// 也可以安全地认为主体拥有该权限。
type SubjectRulesReviewStatus struct {
	// resourceRules 是主体被允许对资源执行的操作列表。
	// 列表顺序无关紧要，可能包含重复项，也可能不完整。
	// +listType=atomic
	ResourceRules []ResourceRule `json:"resourceRules" protobuf:"bytes,1,rep,name=resourceRules"`
	// nonResourceRules 是主体被允许对非资源执行的操作列表。
	// 列表顺序无关紧要，可能包含重复项，也可能不完整。
	// +listType=atomic
	NonResourceRules []NonResourceRule `json:"nonResourceRules" protobuf:"bytes,2,rep,name=nonResourceRules"`
	// 当本次调用返回的规则不完整时，incomplete 为 true。这种情况最常见于
	// 某个授权器（例如外部授权器）不支持规则评估时。
	Incomplete bool `json:"incomplete" protobuf:"bytes,3,rep,name=incomplete"`
	// evaluationError 可以与 Rules 同时出现。它表示规则评估过程中发生了错误，
	// 例如某个授权器不支持规则评估，并且 ResourceRules 和/或 NonResourceRules 可能不完整。
	// +optional
	EvaluationError string `json:"evaluationError,omitempty" protobuf:"bytes,4,opt,name=evaluationError"`
}

// ResourceRule 是主体被允许对资源执行的操作列表。列表顺序无关紧要，可能包含重复项，也可能不完整。
type ResourceRule struct {
	// verbs 是 Kubernetes 资源 API 动词列表，例如 get、list、watch、create、update、delete、proxy。"*" 表示全部。
	// +listType=atomic
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// apiGroups 是包含这些资源的 APIGroup 名称。如果指定了多个 API 组，
	// 则对任意 API 组中列举资源所请求的任何操作都将被允许。"*" 表示全部。
	// +optional
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,2,rep,name=apiGroups"`
	// resources 是本规则适用的资源列表。"*" 表示指定 apiGroups 中的全部资源。
	//  "*/foo" 表示指定 apiGroups 中所有资源的子资源 'foo'。
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`
	// resourceNames 是本规则适用名称的可选白名单。空集合表示允许全部。"*" 表示全部。
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,4,rep,name=resourceNames"`
}

// NonResourceRule 保存描述非资源规则的信息。
type NonResourceRule struct {
	// verbs 是 Kubernetes 非资源 API 动词列表，例如 get、post、put、delete、patch、head、options。"*" 表示全部。
	// +listType=atomic
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// nonResourceURLs 是用户应可访问的部分 URL 集合。允许使用 *，但只能作为路径中完整的最后一级。"*" 表示全部。
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,2,rep,name=nonResourceURLs"`
}
