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

// APIGroupAll 以及这些常量是 rbac 授权的默认值。
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

// PolicyRule 持有描述一条策略规则的信息，但不包含该规则
// 适用于谁或适用于哪个命名空间的信息。
type PolicyRule struct {
	// Verbs 是应用于本规则所含全部 ResourceKinds 的 Verb 列表。'*' 表示所有 verb。
	Verbs []string

	// APIGroups 是包含这些资源的 APIGroup 的名称。
	// 若指定了多个 API group，则针对任意 API group 中枚举资源的任何操作请求都将被允许。
	// "" 表示 core API group，"*" 表示所有 API group。
	APIGroups []string
	// resources 是本规则适用的资源列表。'*' 表示指定 apiGroups 中的所有资源。
	// '*/foo' 表示指定 apiGroups 中所有资源的子资源 'foo'。
	Resources []string
	// ResourceNames 是本规则适用的名称白名单（可选）。空集合表示允许一切。
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
	// APIGroup 保存所引用主体的 API group。
	// ServiceAccount 主体默认为 ""。
	// User 和 Group 主体默认为 "rbac.authorization.k8s.io"。
	APIGroup string
	// 被引用对象的名称。
	Name string
	// 被引用对象所在的命名空间。若对象为非命名空间类型（如 "User" 或 "Group"）而此值非空，
	// 授权器应报错
	Namespace string
}

// RoleRef 包含指向所用角色的信息。
type RoleRef struct {
	// APIGroup 是被引用资源所属的 group
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

	// Rules 包含本 Role 的全部 PolicyRules
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

// RoleBindingList 是 RoleBindings 的集合
type RoleBindingList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items 是 roleBindings 列表
	Items []RoleBinding
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleList 是 Roles 的集合
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

	// Rules 包含本 ClusterRole 的全部 PolicyRules
	Rules []PolicyRule

	// AggregationRule 是可选字段，描述如何为本 ClusterRole 构建 Rules。
	// 若设置了 AggregationRule，则 Rules 由控制器管理，对 Rules 的直接修改
	// 会被控制器覆盖。
	AggregationRule *AggregationRule
}

// AggregationRule 描述如何定位要聚合进本 ClusterRole 的其他 ClusterRole
type AggregationRule struct {
	// ClusterRoleSelectors 持有一组 selector，用于查找 ClusterRoles 并创建规则。
	// 只要其中任一 selector 匹配，该 ClusterRole 的权限就会被聚合进来
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

// ClusterRoleBindingList 是 ClusterRoleBindings 的集合
type ClusterRoleBindingList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items 是 ClusterRoleBindings 列表
	Items []ClusterRoleBinding
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleList 是 ClusterRoles 的集合
type ClusterRoleList struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	metav1.ListMeta

	// Items 是 ClusterRoles 列表
	Items []ClusterRole
}
