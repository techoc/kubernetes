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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 授权计算规则：先评估 ClusterRoleBindings，匹配即短路返回；再评估所请求命名空间中的
// RoleBindings，同样匹配即返回；否则默认拒绝。
const (
	// APIGroupAll 表示匹配所有 API 组。
	APIGroupAll = "*"
	// ResourceAll 表示匹配所有资源。
	ResourceAll = "*"
	// VerbAll 表示匹配所有操作。
	VerbAll = "*"
	// NonResourceAll 表示匹配所有非资源 URL。
	NonResourceAll = "*"

	// GroupKind 表示主体种类为用户组。
	GroupKind = "Group"
	// ServiceAccountKind 表示主体种类为服务账号。
	ServiceAccountKind = "ServiceAccount"
	// UserKind 表示主体种类为用户。
	UserKind = "User"

	// AutoUpdateAnnotationKey 为注解键名，设为 "false" 时可阻止自动协调。
	AutoUpdateAnnotationKey = "rbac.authorization.kubernetes.io/autoupdate"
)

// PolicyRule 描述一条策略规则，但不包含规则适用的主体或命名空间信息。
type PolicyRule struct {
	// verbs 为适用于本规则中所有资源种类的动词列表，'*' 表示所有动词。
	// +listType=atomic
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// apiGroups 为包含这些资源的 API 组名称。若指定多个 API 组，则对任一组中列举资源
	// 请求的任何操作都将被允许。"" 表示核心 API 组，"*" 表示所有 API 组。
	// +optional
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,3,rep,name=apiGroups"`
	// resources 为本规则适用的资源列表，'*' 表示所有资源。
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,4,rep,name=resources"`
	// resourceNames 为规则适用的名称白名单（可选），空集合表示允许全部。
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,5,rep,name=resourceNames"`

	// nonResourceURLs 为用户可访问的部分 URL 集合。允许使用 *，但仅限路径最后一段。
	// 非资源 URL 不属于任何命名空间，因此本字段仅适用于由 ClusterRoleBinding 引用的 ClusterRole。
	// 规则既可对 API 资源（如 "pods"、"secrets"）生效，也可对非资源 URL 路径（如 "/api"）生效，但不可兼而有之。
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,6,rep,name=nonResourceURLs"`
}

// Subject 包含角色绑定所作用的对象或用户身份的引用。
// 它既可以是直接的 API 对象引用，也可以是用户名、组名等非对象值。
type Subject struct {
	// 被引用对象的种类。本 API 组定义的值为 "User"、"Group" 和 "ServiceAccount"。
	// 若授权器无法识别 kind 值，应报错。
	// +required
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// apiVersion 为被引用主体的 API 组和版本。
	// ServiceAccount 主体默认为 "v1"。
	// User 和 Group 主体默认为 "rbac.authorization.k8s.io/v1alpha1"。
	// +k8s:conversion-gen=false
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,2,opt,name=apiVersion"`
	// 被引用对象的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// 被引用对象所属的命名空间。若对象种类不属于命名空间（如 "User" 或 "Group"）
	// 而此值非空，授权器应报错。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,4,opt,name=namespace"`
}

// RoleRef 包含指向所用角色的信息。
type RoleRef struct {
	// apiGroup 为被引用资源所属的组。
	// +optional
	APIGroup string `json:"apiGroup" protobuf:"bytes,1,opt,name=apiGroup"`
	// kind 为被引用资源的类型。
	// +required
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// name 为被引用资源的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Role 是命名空间级别的 PolicyRules 逻辑分组，可被 RoleBinding 整体引用。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 Role，v1.22 起不再提供。
type Role struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 包含此 Role 的所有 PolicyRule。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleBinding 引用角色而非包含角色，可引用同一命名空间中的 Role 或全局命名空间中的 ClusterRole。
// 它通过 Subjects 补充主体信息，并通过自身所在命名空间确定作用范围。
// 某命名空间中的 RoleBinding 仅在该命名空间内生效。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 RoleBinding，v1.22 起不再提供。
type RoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 包含角色所作用对象的引用。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 可引用当前命名空间中的 Role 或全局命名空间中的 ClusterRole。
	// 若无法解析 RoleRef，授权器必须返回错误。
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleBindingList 是 RoleBinding 的集合。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 RoleBindingList，v1.22 起不再提供。
type RoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 为 RoleBinding 列表。
	Items []RoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RoleList 是 Role 的集合。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 RoleList，v1.22 起不再提供。
type RoleList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 为 Role 列表。
	Items []Role `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRole 是集群级别的 PolicyRules 逻辑分组，可被 RoleBinding 或 ClusterRoleBinding 整体引用。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 ClusterRole，v1.22 起不再提供。
type ClusterRole struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 包含此 ClusterRole 的所有 PolicyRule。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`

	// aggregationRule 为可选字段，描述如何构建此 ClusterRole 的 Rules。
	// 设置后 Rules 由控制器管理，直接修改会被控制器覆盖。
	// +optional
	AggregationRule *AggregationRule `json:"aggregationRule,omitempty" protobuf:"bytes,3,opt,name=aggregationRule"`
}

// AggregationRule 描述如何定位要聚合到此 ClusterRole 的其他 ClusterRole。
type AggregationRule struct {
	// clusterRoleSelectors 为一组选择器，用于查找 ClusterRole 并生成规则。
	// 任一选择器匹配时，该 ClusterRole 的权限将被加入。
	// +optional
	// +listType=atomic
	ClusterRoleSelectors []metav1.LabelSelector `json:"clusterRoleSelectors,omitempty" protobuf:"bytes,1,rep,name=clusterRoleSelectors"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleBinding 引用而非包含 ClusterRole，可引用全局命名空间中的 ClusterRole，
// 并通过 Subject 补充主体信息。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 ClusterRoleBinding，v1.22 起不再提供。
type ClusterRoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 包含角色所作用对象的引用。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 只能引用全局命名空间中的 ClusterRole。
	// 若无法解析 RoleRef，授权器必须返回错误。
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleBindingList 是 ClusterRoleBinding 的集合。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 ClusterRoleBindings，v1.22 起不再提供。
type ClusterRoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 为 ClusterRoleBinding 列表。
	Items []ClusterRoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterRoleList 是 ClusterRole 的集合。
// 自 v1.17 起已废弃，改用 rbac.authorization.k8s.io/v1 ClusterRoles，v1.22 起不再提供。
type ClusterRoleList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 为 ClusterRole 列表。
	Items []ClusterRole `json:"items" protobuf:"bytes,2,rep,name=items"`
}
