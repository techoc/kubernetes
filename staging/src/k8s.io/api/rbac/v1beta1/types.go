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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 授权计算顺序：
// 1. 检查 ClusterRoleBindings —— 匹配即短路返回
// 2. 检查请求命名空间中的 RoleBindings —— 匹配即短路返回
// 3. 默认拒绝

const (
	// APIGroupAll 表示匹配所有 API 组。
	APIGroupAll = "*"
	// ResourceAll 表示匹配所有资源。
	ResourceAll = "*"
	// VerbAll 表示匹配所有操作。
	VerbAll = "*"
	// NonResourceAll 表示匹配所有非资源 URL。
	NonResourceAll = "*"

	// GroupKind 是组的 Subject kind。
	GroupKind = "Group"
	// ServiceAccountKind 是 ServiceAccount 的 Subject kind。
	ServiceAccountKind = "ServiceAccount"
	// UserKind 是用户的 Subject kind。
	UserKind = "User"

	// AutoUpdateAnnotationKey 是防止自动同步的 annotation 键名，设为 "false" 时生效。
	AutoUpdateAnnotationKey = "rbac.authorization.kubernetes.io/autoupdate"
)

// 授权计算顺序：
// 1. 检查 ClusterRoleBindings —— 匹配即短路返回
// 2. 检查请求命名空间中的 RoleBindings —— 匹配即短路返回
// 3. 默认拒绝

// PolicyRule 描述一条策略规则，但不包含规则适用于谁、适用于哪个命名空间的信息。
type PolicyRule struct {
	// verbs 是应用于本规则中所有 ResourceKinds 的操作列表。'*' 表示所有操作。
	// +listType=atomic
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// apiGroups 是包含这些资源的 APIGroup 名称。若指定多个 API 组，则对其中任一组所列资源的任何操作都会被允许。
	// "" 表示核心 API 组，"*" 表示所有 API 组。
	// +optional
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,2,rep,name=apiGroups"`
	// resources 是本规则适用的资源列表。'*' 表示指定 apiGroups 中的所有资源。
	// '*/foo' 表示指定 apiGroups 中所有资源的子资源 'foo'。
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`
	// resourceNames 是规则适用的资源名称白名单。空集合表示允许全部。
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,4,rep,name=resourceNames"`

	// nonResourceURLs 是用户可访问的部分 URL 集合。允许使用 *，但只能作为路径的最后一段。
	// 非资源 URL 不属于命名空间，因此此字段仅适用于被 ClusterRoleBinding 引用的 ClusterRole。
	// 规则要么作用于 API 资源（如 "pods" 或 "secrets"），要么作用于非资源 URL 路径（如 "/api"），不能同时作用于两者。
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,5,rep,name=nonResourceURLs"`
}

// Subject 包含角色绑定所适用的对象或用户身份的引用。
// 它可以是直接的 API 对象引用，也可以是用户名、组名等非对象的值。
type Subject struct {
	// 被引用对象的 kind。本 API 组定义的值为 "User"、"Group" 和 "ServiceAccount"。
	// 如果 Authorizer 无法识别该 kind 值，应报告错误。
	// +required
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// apiGroup 是被引用 Subject 所属的 API 组。
	// ServiceAccount 类型默认为 ""。
	// User 和 Group 类型默认为 "rbac.authorization.k8s.io"。
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,2,opt,name=apiGroup"`
	// 被引用对象的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// 被引用对象所属的命名空间。如果对象 kind 不属于命名空间（如 "User" 或 "Group"）而此值非空，
	// Authorizer 应报告错误。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,4,opt,name=namespace"`
}

// RoleRef 包含指向所用角色的信息。
type RoleRef struct {
	// apiGroup 是被引用资源所属的组。
	// +optional
	APIGroup string `json:"apiGroup" protobuf:"bytes,1,opt,name=apiGroup"`
	// kind 是被引用资源的类型。
	// +required
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// name 是被引用资源的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,Role

// Role 是命名空间级别的 PolicyRules 逻辑分组，可被 RoleBinding 作为整体引用。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 Role；v1.22 起不再提供。
type Role struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 保存此 Role 的所有 PolicyRule。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,RoleBinding

// RoleBinding 引用角色但不包含角色本身。它可以引用同一命名空间中的 Role，或全局命名空间中的 ClusterRole。
// 它通过 Subjects 补充“谁”的信息，并通过自身所在命名空间提供命名空间信息。
// 给定命名空间中的 RoleBinding 只在该命名空间内生效。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 RoleBinding；v1.22 起不再提供。
type RoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 保存角色所适用对象的引用。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 可以引用当前命名空间中的 Role，或全局命名空间中的 ClusterRole。
	// 如果 RoleRef 无法解析，Authorizer 必须返回错误。
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,RoleBindingList

// RoleBindingList 是 RoleBinding 的集合。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 RoleBindingList；v1.22 起不再提供。
type RoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 RoleBinding 列表。
	Items []RoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,RoleList

// RoleList 是 Role 的集合。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 RoleList；v1.22 起不再提供。
type RoleList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Role 列表。
	Items []Role `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,ClusterRole

// ClusterRole 是集群级别的 PolicyRules 逻辑分组，可被 RoleBinding 或 ClusterRoleBinding 作为整体引用。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 ClusterRole；v1.22 起不再提供。
type ClusterRole struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// rules 保存此 ClusterRole 的所有 PolicyRule。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`
	// aggregationRule 是可选字段，描述如何为本 ClusterRole 构建 Rules。
	// 设置后，Rules 由控制器管理，直接修改 Rules 会被控制器覆盖。
	// +optional
	AggregationRule *AggregationRule `json:"aggregationRule,omitempty" protobuf:"bytes,3,opt,name=aggregationRule"`
}

// AggregationRule 描述如何查找要聚合进本 ClusterRole 的其他 ClusterRole。
type AggregationRule struct {
	// clusterRoleSelectors 保存用于查找 ClusterRole 并生成规则的选择器列表。
	// 任一选择器匹配时，对应 ClusterRole 的权限都会被加入。
	// +optional
	// +listType=atomic
	ClusterRoleSelectors []metav1.LabelSelector `json:"clusterRoleSelectors,omitempty" protobuf:"bytes,1,rep,name=clusterRoleSelectors"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,ClusterRoleBinding

// ClusterRoleBinding 引用但不包含 ClusterRole。它可引用全局命名空间中的 ClusterRole，
// 并通过 Subject 补充“谁”的信息。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 ClusterRoleBinding；v1.22 起不再提供。
type ClusterRoleBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// subjects 保存角色所适用对象的引用。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Subjects []Subject `json:"subjects,omitempty" protobuf:"bytes,2,rep,name=subjects"`

	// roleRef 只能引用全局命名空间中的 ClusterRole。
	// 如果 RoleRef 无法解析，Authorizer 必须返回错误。
	// +required
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	RoleRef RoleRef `json:"roleRef" protobuf:"bytes,3,opt,name=roleRef"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,ClusterRoleBindingList

// ClusterRoleBindingList 是 ClusterRoleBinding 的集合。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 ClusterRoleBindingList；v1.22 起不再提供。
type ClusterRoleBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ClusterRoleBinding 列表。
	Items []ClusterRoleBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=rbac.authorization.k8s.io,v1,ClusterRoleList

// ClusterRoleList 是 ClusterRole 的集合。
// 自 v1.17 起弃用，改用 rbac.authorization.k8s.io/v1 ClusterRole；v1.22 起不再提供。
type ClusterRoleList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ClusterRole 列表。
	Items []ClusterRole `json:"items" protobuf:"bytes,2,rep,name=items"`
}
