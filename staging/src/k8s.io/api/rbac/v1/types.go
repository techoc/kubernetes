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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 授权按以下顺序计算：
// 1. 评估 ClusterRoleBindings——命中即短路
// 2. 评估所请求命名空间中的 RoleBindings——命中即短路
// 3. 默认拒绝

const (
	// APIGroupAll 表示所有 API 组
	APIGroupAll = "*"
	// ResourceAll 表示所有资源
	ResourceAll = "*"
	// VerbAll 表示所有操作
	VerbAll = "*"
	// NonResourceAll 表示所有非资源 URL
	NonResourceAll = "*"

	// GroupKind 是组主体的 kind
	GroupKind = "Group"
	// ServiceAccountKind 是服务账号主体的 kind
	ServiceAccountKind = "ServiceAccount"
	// UserKind 是用户主体的 kind
	UserKind = "User"

	// AutoUpdateAnnotationKey 是防止自动更新的注解名，值为 "false" 时不再执行协调（reconcile）
	AutoUpdateAnnotationKey = "rbac.authorization.kubernetes.io/autoupdate"
)

// 授权按以下顺序计算：
// 1. 评估 ClusterRoleBindings——命中即短路
// 2. 评估所请求命名空间中的 RoleBindings——命中即短路
// 3. 默认拒绝

// PolicyRule 描述一条策略规则，但不包含规则适用的主体或命名空间信息
type PolicyRule struct {
	// verbs 是适用于本规则中所有资源种类的动作列表；'*' 表示所有动作
	// +listType=atomic
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// apiGroups 是包含资源的 API 组名。若指定多个 API 组，则对其中任一组内所列资源的任何操作都被允许。
	// "" 表示核心 API 组，"*" 表示所有 API 组
	// +optional
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,2,rep,name=apiGroups"`
	// resources 是本规则适用的资源列表；'*' 表示所有资源
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`
	// resourceNames 是规则适用的名称白名单，可留空；空集表示允许全部名称
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,4,rep,name=resourceNames"`

	// nonResourceURLs 是用户可访问的部分 URL 集合。允许使用 *，但只能作为路径中完整且最后的一段
	// 非资源 URL 不属于命名空间，因此本字段仅适用于被 ClusterRoleBinding 引用的 ClusterRole
	// 规则只能针对 API 资源（如 "pods"、"secrets"）或非资源 URL 路径（如 "/api"），二者不可兼得
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,5,rep,name=nonResourceURLs"`
}

// Subject 引用角色绑定所作用的对象或用户身份，既可以是直接的 API 对象引用，
// 也可以是非对象的值，例如用户名和组名
// +structType=atomic
type Subject struct {
	// 被引用对象的类型。本 API 组定义的值为 "User"、"Group" 和 "ServiceAccount"
	// 若授权器无法识别该类型值，应报错
	// +required
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// apiGroup 是被引用主体所属的 API 组
	// ServiceAccount 主体默认为 ""
	// User 和 Group 主体默认为 "rbac.authorization.k8s.io"
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,2,opt,name=apiGroup"`
	// 被引用对象的名称
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// 被引用对象所在的命名空间。若对象为非命名空间类型（如 "User" 或 "Group"）而此值非空，
	// 授权器应报错
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,4,opt,name=namespace"`
}

// RoleRef 指向所使用的角色
// +structType=atomic
type RoleRef struct {
	// apiGroup 是被引用资源所属的组
	// +optional
	APIGroup string `json:"apiGroup" protobuf:"bytes,1,opt,name=apiGroup"`
	// kind 是被引用资源的类型
	// +required
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// name 是被引用资源的名称
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// Role 是命名空间级别的 PolicyRules 逻辑分组，可被 RoleBinding 作为一个整体引用
type Role struct {
	metav1.TypeMeta `json:""`
	// metadata 是对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 保存该 Role 的所有 PolicyRule
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// RoleBinding 引用角色但不包含角色本身。它可以引用同一命名空间中的 Role，或全局命名空间中的 ClusterRole
// 它通过 Subjects 提供主体信息，通过自身所在命名空间提供命名空间信息。给定命名空间中的 RoleBinding
// 只在该命名空间内生效
type RoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 保存角色所作用对象的引用
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 可引用当前命名空间中的 Role，或全局命名空间中的 ClusterRole
	// 若 RoleRef 无法解析，授权器必须返回错误
	// 本字段不可变
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// RoleBindingList 是 RoleBinding 的集合
type RoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 RoleBinding 列表
	Items []RoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// RoleList 是 Role 的集合
type RoleList struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Role 列表
	Items []Role `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// ClusterRole 是集群级别的 PolicyRules 逻辑分组，可被 RoleBinding 或 ClusterRoleBinding 作为一个整体引用
type ClusterRole struct {
	metav1.TypeMeta `json:""`
	// metadata 是对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 保存该 ClusterRole 的所有 PolicyRule
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`

	// aggregationRule 是可选字段，描述如何为本 ClusterRole 生成 Rules
	// 设置 AggregationRule 后，Rules 由控制器管理，直接修改 Rules 会
	// 被控制器覆盖
	// +optional
	AggregationRule *AggregationRule `json:"aggregationRule,omitempty" protobuf:"bytes,3,opt,name=aggregationRule"`
}

// AggregationRule 描述如何定位要聚合进本 ClusterRole 的其他 ClusterRole
type AggregationRule struct {
	// clusterRoleSelectors 保存用于查找 ClusterRole 并生成规则的选择器列表
	// 任一选择器命中，就会加入该 ClusterRole 的权限
	// +optional
	// +listType=atomic
	ClusterRoleSelectors []metav1.LabelSelector `json:"clusterRoleSelectors,omitempty" protobuf:"bytes,1,rep,name=clusterRoleSelectors"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// ClusterRoleBinding 引用但不包含 ClusterRole。它只能引用全局命名空间中的 ClusterRole，
// 并通过 Subject 提供主体信息
type ClusterRoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 保存角色所作用对象的引用
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 只能引用全局命名空间中的 ClusterRole
	// 若 RoleRef 无法解析，授权器必须返回错误
	// 本字段不可变
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// ClusterRoleBindingList 是 ClusterRoleBinding 的集合
type ClusterRoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ClusterRoleBinding 列表
	Items []ClusterRoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8

// ClusterRoleList 是 ClusterRole 的集合
type ClusterRoleList struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ClusterRole 列表
	Items []ClusterRole `json:"items" protobuf:"bytes,2,rep,name=items"`
}
