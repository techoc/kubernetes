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

package v2beta1

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:deprecated=1.32
// +k8s:prerelease-lifecycle-gen:removed=1.35
// 上面列出的弃用和移除版本只是粗略估计，可能会发生变化。我们预计 v2 类型将在 1.28 中可用，
// 并且在弃用之前会支持 4 个同时支持 v2beta1 和 v2 的版本。

// APIGroupDiscoveryList 是包含 APIGroupDiscovery 列表的资源。
// 它是 /api 和 /apis 端点可返回的类型之一，包含集群所支持的
// API 资源（内置资源、自定义资源定义、来自聚合服务器的资源）的聚合列表。
type APIGroupDiscoveryList struct {
	v1.TypeMeta `json:""`
	// ResourceVersion 不会被设置，因为在多个 apiserver 之间它不具备可重放的顺序。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	v1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是待发现的分组列表。分组按优先级顺序列出。
	// +optional
	Items []APIGroupDiscovery `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:deprecated=1.32
// +k8s:prerelease-lifecycle-gen:removed=1.35
// 上面列出的弃用和移除版本只是粗略估计，可能会发生变化。我们预计 v2 类型将在 1.28 中可用，
// 并且在弃用之前会支持 4 个同时支持 v2beta1 和 v2 的版本。

// APIGroupDiscovery 保存 API 组所有版本中正在提供哪些资源的信息。
// 它包含一个 APIVersionDiscovery 列表，其中保存某个版本所提供的 APIResourceDiscovery 类型列表。
// 版本按优先级的降序排列，第一个版本为首选版本。
type APIGroupDiscovery struct {
	v1.TypeMeta `json:""`
	// metadata 是标准对象的元数据。
	// 唯一会填充的字段是 name，例如 resourceVersion 将为空。
	// name 是此处展示其发现信息的 API 组的名称。
	// name 允许为 ""，表示传统的、未分组的资源。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	v1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// versions 是本组支持的版本。它们按优先级降序排列，
	// 首选版本位于第一项。
	// +listType=map
	// +listMapKey=version
	// +optional
	Versions []APIVersionDiscovery `json:"versions,omitempty" protobuf:"bytes,2,rep,name=versions"`
}

// APIVersionDiscovery 保存 API 组中某个特定版本所提供的 APIResourceDiscovery 类型列表。
type APIVersionDiscovery struct {
	// version 是分组版本内该版本的名称。
	// +optional
	Version string `json:"version" protobuf:"bytes,1,opt,name=version"`
	// resources 是对应分组版本的 APIResourceDiscovery 对象列表。
	// +listType=map
	// +listMapKey=resource
	// +optional
	Resources []APIResourceDiscovery `json:"resources,omitempty" protobuf:"bytes,2,rep,name=resources"`
	// freshness 标记某个分组版本的发现文档是否为最新。
	// "Current" 表示发现文档最近被刷新过。"Stale" 表示无法获取发现文档，
	// 返回的发现文档可能已严重过时。要求在执行操作前获取最新发现信息的客户端
	// 不应使用聚合文档。
	// +optional
	Freshness DiscoveryFreshness `json:"freshness,omitempty" protobuf:"bytes,3,opt,name=freshness"`
}

// APIResourceDiscovery 提供某个 API 资源用于发现的信息。
type APIResourceDiscovery struct {
	// resource 是资源的复数名称。它用于 URL 路径，并且是该资源在 API 组所有版本中的唯一标识符。
	// 具有非空分组的资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>
	// 具有空分组的资源位于 /api/v1/<APIResourceDiscovery.Resource>
	// +optional
	Resource string `json:"resource" protobuf:"bytes,1,opt,name=resource"`
	// responseKind 描述此端点通常返回的对象类型的序列化 schema 的组、版本和 kind。
	// API 可以自行决定返回其他对象类型，例如错误情况、对替代表达形式的请求或其他特定于操作的行为。
	// 如果某个 APIService 报告了子资源但不支持对父资源执行任何操作，则该值为 null 或空。
	// +optional
	ResponseKind *v1.GroupVersionKind `json:"responseKind,omitempty" protobuf:"bytes,2,opt,name=responseKind"`
	// scope 表示资源的作用域，取值可以是 Cluster 或 Namespaced。
	// +optional
	Scope ResourceScope `json:"scope" protobuf:"bytes,3,opt,name=scope"`
	// singularResource 是资源的单数名称。它使客户端能够不透明地处理复数和单数形式。
	// 对许多客户端而言，资源的单数形式在阅读消息时对用户更易于理解，
	// 在将资源名称嵌入句子中时应使用该形式。
	// 例如，命令行工具 kubectl 允许使用资源的单数名称代替复数名称。
	// 资源的单数形式应始终作为可选元素——如有疑问，请使用规范的资源名称。
	// +optional
	SingularResource string `json:"singularResource" protobuf:"bytes,4,opt,name=singularResource"`
	// verbs 是受支持的 API 操作类型列表（包括但不限于
	// get、list、watch、create、update、patch、
	// delete、deletecollection 和 proxy）。
	// +listType=set
	// +optional
	Verbs []string `json:"verbs" protobuf:"bytes,5,opt,name=verbs"`
	// shortNames 是资源建议的简称列表。
	// +listType=set
	// +optional
	ShortNames []string `json:"shortNames,omitempty" protobuf:"bytes,6,rep,name=shortNames"`
	// categories 是本资源所属的资源分组列表（例如 'all'）。
	// 客户端可利用它来简化同时对多种资源类型执行操作。
	// +listType=set
	// +optional
	Categories []string `json:"categories,omitempty" protobuf:"bytes,7,rep,name=categories"`
	// subresources 是本资源提供的子资源列表。子资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>/name-of-instance/<APIResourceDiscovery.subresources[i].subresource>
	// +listType=map
	// +listMapKey=subresource
	// +optional
	Subresources []APISubresourceDiscovery `json:"subresources,omitempty" protobuf:"bytes,8,rep,name=subresources"`
}

// ResourceScope 是一个枚举，定义资源可用的不同作用域。
type ResourceScope string

// 资源作用域的枚举值。
const (
	ScopeCluster   ResourceScope = "Cluster"
	ScopeNamespace ResourceScope = "Namespaced"
)

// DiscoveryFreshness 是一个枚举，表示 apiservice 发布的发现文档是否为最新（fresh）。
type DiscoveryFreshness string

// 发现文档新鲜度的枚举值。
const (
	DiscoveryFreshnessCurrent DiscoveryFreshness = "Current"
	DiscoveryFreshnessStale   DiscoveryFreshness = "Stale"
)

// APISubresourceDiscovery 提供某个 API 子资源用于发现的信息。
type APISubresourceDiscovery struct {
	// subresource 是子资源的名称。它用于 URL 路径，并且是该资源在所有版本中的唯一标识符。
	// +optional
	Subresource string `json:"subresource" protobuf:"bytes,1,opt,name=subresource"`
	// responseKind 描述此端点通常返回的对象类型的序列化 schema 的组、版本和 kind。
	// 某些子资源不返回普通资源，这些子资源的返回类型将为 null 或空。
	// +optional
	ResponseKind *v1.GroupVersionKind `json:"responseKind,omitempty" protobuf:"bytes,2,opt,name=responseKind"`
	// acceptedTypes 描述此端点接受的 kind。
	// 子资源可以接受标准内容类型，也可以定义自定义的协商方案。
	// 该列表可能并未涵盖所有操作。
	// +listType=map
	// +listMapKey=group
	// +listMapKey=version
	// +listMapKey=kind
	// +optional
	AcceptedTypes []v1.GroupVersionKind `json:"acceptedTypes,omitempty" protobuf:"bytes,3,rep,name=acceptedTypes"`
	// verbs 是受支持的 API 操作类型列表（包括但不限于
	// get、list、watch、create、update、patch、
	// delete、deletecollection 和 proxy）。子资源可以定义
	// 标准 Kubernetes 动词集之外的自定义动词。客户端
	// 应预期标准动词的行为与 Kubernetes 的交互约定一致。
	// +listType=set
	// +optional
	Verbs []string `json:"verbs" protobuf:"bytes,4,opt,name=verbs"`
}
