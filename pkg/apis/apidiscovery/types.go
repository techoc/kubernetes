/*
Copyright 2022 The Kubernetes Authors.

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

package apidiscovery

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIGroupDiscoveryList 是包含 APIGroupDiscovery 列表的资源。
// 它是 /api 和 /apis 端点可返回的类型之一，包含一个聚合的
// API 资源列表（内置资源、CRD、来自聚合 server 的资源），
// 即集群支持的资源。
type APIGroupDiscoveryList struct {
	v1.TypeMeta
	// 不会设置 ResourceVersion，因为多个 apiserver 之间不存在可重放的顺序。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	v1.ListMeta
	// items 是用于发现的组列表，按优先级排序。
	Items []APIGroupDiscovery
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIGroupDiscovery 保存该 API 组所有版本所提供资源的信息。
// 它包含 APIVersionDiscovery 列表，后者保存某个版本提供的 APIResourceDiscovery 类型列表。
// 版本按优先级降序排列，第一个是首选版本。
type APIGroupDiscovery struct {
	v1.TypeMeta
	// 标准的对象元数据。
	// 只会填充 name 字段，例如 resourceVersion 会是空的。
	// name 是此处展示发现信息的 API 组名。
	// name 可以为空字符串，表示传统的无组资源。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	v1.ObjectMeta
	// versions 是该组支持的版本，按优先级降序排列，
	// 首选版本排在第一位。
	// +listType=map
	// +listMapKey=version
	Versions []APIVersionDiscovery
}

// APIVersionDiscovery 保存某 API 组内特定版本所提供的 APIResourceDiscovery 类型列表。
type APIVersionDiscovery struct {
	// version 是组版本内的版本名。
	Version string
	// resources 是对应组版本的 APIResourceDiscovery 对象列表。
	// +listType=map
	// +listMapKey=resource
	Resources []APIResourceDiscovery
	// freshness 标记某个组版本的发现文档是否是最新的。
	// "Current" 表示发现文档最近刚
	// 刷新过。"Stale" 表示无法获取发现文档，
	// 因此返回的文档可能已经
	// 明显过时。需要最新发现信息的客户端
	// 在执行操作前应先直接获取所需版本的文档，
	// 而不应使用聚合文档。
	// （承接上一行）。
	Freshness DiscoveryFreshness
}

// APIResourceDiscovery 提供某个 API 资源的发现信息。
type APIResourceDiscovery struct {
	// resource 是资源的复数名称，用于 URL 路径，是该资源在整个 API 组所有版本中的唯一标识。
	// （承接上一行）。
	// 非空组的资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>
	// 空组的资源位于 /api/v1/<APIResourceDiscovery.Resource>
	Resource string
	// responseKind 描述该端点通常返回的对象类型所使用的序列化 schema 的组、版本和 kind。
	// API 可以自行决定返回其他对象类型，例如错误状态、替代表示形式的请求或其他操作相关的行为。
	// 若 APIService 声明了子资源但不支持对父资源执行任何操作，该值为 null 或空。
	ResponseKind *v1.GroupVersionKind
	// scope 表示资源的作用域，取值为 Cluster 或 Namespaced。
	Scope ResourceScope
	// singularResource 是资源的单数名称，使客户端可以统一处理复数与单数形式。
	// 对许多客户端而言，资源的单数形式在面向用户的消息中更易理解，将资源名嵌入句子时应使用单数形式。
	// 例如命令行工具 kubectl 允许用单数资源名代替复数形式。
	// 单数形式应始终是可选元素——不确定时使用规范的资源名。
	SingularResource string
	// verbs 是支持的 API 操作类型列表（包括
	// 但不限于 get、list、watch、create、update、patch、
	// delete、deletecollection 和 proxy）。
	// +listType=set
	Verbs []string
	// shortNames 是该资源推荐的简称列表。
	// +listType=set
	ShortNames []string
	// categories 是该资源所属的资源分组列表（例如 'all'）。
	// 客户端可借此简化对多种资源类型的批量操作。
	// +listType=set
	Categories []string
	// subresources 为此资源提供的子资源列表。子资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>/name-of-instance/<APIResourceDiscovery.subresources[i].subresource>
	// +listType=map
	// +listMapKey=subresource
	Subresources []APISubresourceDiscovery
}

// ResourceScope 是枚举，定义资源可用的不同作用域。
type ResourceScope string

const (
	ScopeCluster   ResourceScope = "Cluster"
	ScopeNamespace ResourceScope = "Namespaced"
)

// DiscoveryFreshness 是枚举，表示 apiservice 发布的发现文档是否为最新。
type DiscoveryFreshness string

const (
	DiscoveryFreshnessCurrent DiscoveryFreshness = "Current"
	DiscoveryFreshnessStale   DiscoveryFreshness = "Stale"
)

// APISubresourceDiscovery 提供某 API 子资源的发现信息。
type APISubresourceDiscovery struct {
	// subresource 为子资源名称，用于 URL 路径，是此资源在所有版本中的唯一标识。
	Subresource string
	// responseKind 描述该端点通常返回的对象类型所使用的序列化 schema 的组、版本和 kind。
	// 部分子资源不返回普通资源，其返回类型为 null 或空。
	ResponseKind *v1.GroupVersionKind
	// acceptedTypes 描述该端点接受的种类。
	// 子资源可接受标准内容类型或定义自定义协商方案。
	// 该列表未必涵盖所有操作。
	// +listType=map
	// +listMapKey=group
	// +listMapKey=version
	// +listMapKey=kind
	AcceptedTypes []v1.GroupVersionKind
	// verbs 是支持的 API 操作类型列表（包括
	// 但不限于 get、list、watch、create、update、patch、
	// delete, deletecollection, and proxy). Subresources may define
	// custom verbs outside the standard Kubernetes verb set. Clients
	// should expect the behavior of standard verbs to align with
	// Kubernetes interaction conventions.
	// +listType=set
	Verbs []string
}
