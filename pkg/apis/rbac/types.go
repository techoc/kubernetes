/*
Copyright 2016 The Kubernetes Authors.

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

package rbac

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Authorization 按以下顺序评估：
// 1. 评估 ClusterRoleBindings——命中即短路
// 2. 评估所请求命名空间中的 RoleBindings——命中即短路
// 3. 默认拒绝（deny by default）

// APIGroupAll and these consts are default values for rbac authorization.
const (
	APIGroupAll    = "*"
	ResourceAll    = "*"
	VerbAll        = "*"
	NonResourceAll = "*"

	GroupKind          = "Group"
	ServiceAccountKind = "ServiceAccount"
	UserKind           = "User"

	// AutoUpdateAnnotationKey 是防止自动更新的注解名，值为 "false" 时不再执行协调（reconcile）
	AutoUpdateAnnotationKey = "rbac.authorization.kubernetes.io/autoupdate"
)

// PolicyRule holds information that describes a policy rule, but does not contain information
// about who the rule applies to or which namespace the rule applies to.
type PolicyRule struct {
	// Verbs is a list of Verbs that apply to ALL the ResourceKinds contained in this rule. '*' represents all verbs.
	Verbs []string

	// APIGroups is the name of the APIGroup that contains the resources.
	// If multiple API groups are specified, any action requested against one of the enumerated resources in any API group will be allowed. "" represents the core API group and "*" represents all API groups.
	APIGroups []string
	// resources 是本规则适用的资源列表。'*' 表示指定 apiGroups 中的所有资源。
	// '*/foo' 表示指定 apiGroups 中所有资源的子资源 'foo'。
	Resources []string
	// ResourceNames is an optional white list of names that the rule applies to.  An empty set means that everything is allowed.
	ResourceNames []string

	// NonResourceURLs 是用户应有权访问的部分 URL 集合。允许使用 *，
	// 但只能作为路径中完整且最后的一步。
	// 若某个操作不是资源 API 请求，则 URL 会按 '/' 拆分，
	// 并与 NonResourceURLs 比对以寻找匹配项。
	// 非资源 URL 不属于命名空间，因此本字段仅适用于被 ClusterRoleBinding 引用的 ClusterRole
	// 规则只能针对 API 资源（如 "pods"、"secrets"）或非资源 URL 路径（如 "/api"），二者不可兼得
	NonResourceURLs []string
}

// Subject 引用角色绑定所作用的对象或用户身份，既可以是直接的 API 对象引用，
// 也可以是非对象的值，例如用户名和组名
type Subject struct {
	// 被引用对象的 kind。本 API 组定义的值为 "User"、"Group" 和 "ServiceAccount"。
	// 如果 Authorizer 无法识别该 kind 值，应报告错误。
	Kind string
	// APIGroup holds the API group of the referenced subject.
	// Defaults to "" for ServiceAccount subjects.
	// Defaults to "rbac.authorization.k8s.io" for User and Group subjects.
	APIGroup string
	// Name of the object being referenced.
	Name string
	// 被引用对象所在的命名空间。若对象为非命名空间类型（如 "User" 或 "Group"）而此值非空，
	// 授权器应报错
	Namespace string
}

// RoleRef 包含指向所用角色的信息。
type RoleRef struct {
	// APIGroup is the group for the resource being referenced
	APIGroup string
	// kind 为被引用资源的类型。
	Kind string
	// name 是被引用资源的名称。
	Name string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Role 是命名空间级别的 PolicyRules 逻辑分组，可被 RoleBinding 作为一个整体引用
type Role struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ObjectMeta

	// Rules holds all the PolicyRules for this Role
	Rules []PolicyRule
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleBinding 引用角色但不包含角色本身。它可以引用同一命名空间中的 Role，或全局命名空间中的 ClusterRole
// 它通过 Subjects 提供主体信息，通过自身所在命名空间提供命名空间信息。给定命名空间中的 RoleBinding
// 只在该命名空间内生效
type RoleBinding struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// subjects 包含角色所作用对象的引用。
	Subjects []Subject

	// RoleRef 可以引用当前命名空间中的 Role，或全局命名空间中的 ClusterRole。
	// 若 RoleRef 无法解析，授权器必须返回错误
	// 该字段不可变。
	RoleRef RoleRef
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleBindingList is a collection of RoleBindings
type RoleBindingList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items is a list of roleBindings
	Items []RoleBinding
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleList is a collection of Roles
type RoleList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items 为 Role 列表。
	Items []Role
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRole 是集群级别的 PolicyRules 逻辑分组，可被 RoleBinding 或 ClusterRoleBinding 作为一个整体引用
type ClusterRole struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ObjectMeta

	// Rules holds all the PolicyRules for this ClusterRole
	Rules []PolicyRule

	// AggregationRule is an optional field that describes how to build the Rules for this ClusterRole.
	// If AggregationRule is set, then the Rules are controller managed and direct changes to Rules will be
	// stomped by the controller.
	AggregationRule *AggregationRule
}

// AggregationRule 描述如何定位要聚合进本 ClusterRole 的其他 ClusterRole
type AggregationRule struct {
	// ClusterRoleSelectors holds a list of selectors which will be used to find ClusterRoles and create the rules.
	// If any of the selectors match, then the ClusterRole's permissions will be added
	ClusterRoleSelectors []metav1.LabelSelector
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleBinding 引用但不包含 ClusterRole。它只能引用全局命名空间中的 ClusterRole，
// 并通过 Subject 提供主体信息
type ClusterRoleBinding struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ObjectMeta

	// subjects 包含角色所作用对象的引用。
	Subjects []Subject

	// RoleRef 只能引用全局命名空间中的 ClusterRole。
	// 若 RoleRef 无法解析，授权器必须返回错误
	// 该字段不可变。
	RoleRef RoleRef
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleBindingList is a collection of ClusterRoleBindings
type ClusterRoleBindingList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items is a list of ClusterRoleBindings
	Items []ClusterRoleBinding
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleList is a collection of ClusterRoles
type ClusterRoleList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items is a list of ClusterRoles
	Items []ClusterRole
}
