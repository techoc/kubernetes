/*
Copyright 2024 The Kubernetes Authors.

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

package v2

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// APIGroupDiscoveryList 是包含 APIGroupDiscovery 列表的资源。
// 它是 /api 与 /apis 端点可返回的类型之一，包含集群支持的 API 资源聚合列表
// （内置资源、自定义资源定义、聚合服务器提供的资源）。
type APIGroupDiscoveryList struct {
	v1.TypeMeta `json:""`
	// ResourceVersion 不会设置，因为多个 apiserver 之间不存在可重放的顺序。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	v1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 为待发现的 API 组列表，按优先级排列。
	// +optional
	Items []APIGroupDiscovery `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// APIGroupDiscovery 描述该 API 组所有版本下正在提供的资源信息。
// 它包含若干 APIVersionDiscovery，每个对应某个版本下提供的 APIResourceDiscovery 列表。
// 版本按优先级降序排列，首个条目即首选版本。
type APIGroupDiscovery struct {
	v1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// 仅填充 name 字段，例如 resourceVersion 为空。
	// name 为此处展示发现信息的 API 组名称。
	// name 允许为 ""，表示旧的无组资源。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	v1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// versions 为该组支持的版本，按优先级降序排列，首选版本位于首位。
	// +listType=map
	// +listMapKey=version
	// +optional
	Versions []APIVersionDiscovery `json:"versions,omitempty" protobuf:"bytes,2,rep,name=versions"`
}

// APIVersionDiscovery 包含某 API 组内特定版本下提供的 APIResourceDiscovery 列表。
type APIVersionDiscovery struct {
	// version 为组版本中的版本名称。
	// +optional
	Version string `json:"version" protobuf:"bytes,1,opt,name=version"`
	// resources 为对应组版本下的 APIResourceDiscovery 对象列表。
	// +listType=map
	// +listMapKey=resource
	// +optional
	Resources []APIResourceDiscovery `json:"resources,omitempty" protobuf:"bytes,2,rep,name=resources"`
	// freshness 标记某组版本的发现文档是否为最新。
	// "Current" 表示发现文档最近刚刷新；"Stale" 表示无法获取该文档，
	// 返回的发现文档可能已严重过时。执行操作前需要最新发现信息的客户端
	// 不应使用此聚合文档。
	// +optional
	Freshness DiscoveryFreshness `json:"freshness,omitempty" protobuf:"bytes,3,opt,name=freshness"`
}

// APIResourceDiscovery 提供某 API 资源的发现信息。
type APIResourceDiscovery struct {
	// resource 为资源的复数名称，用于 URL 路径，是此资源在 API 组所有版本中的唯一标识。
	// 非空组的资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>
	// 空组的资源位于 /api/v1/<APIResourceDiscovery.Resource>
	// +optional
	Resource string `json:"resource" protobuf:"bytes,1,opt,name=resource"`
	// responseKind 描述该端点通常返回的对象类型所对应的序列化模式的组、版本和种类。
	// API 可自行返回其他对象类型，例如错误状态、替代表示请求或其他操作特定行为。
	// 若 APIService 声明了子资源但不支持对父资源的任何操作，此值为 null 或空。
	// +optional
	ResponseKind *v1.GroupVersionKind `json:"responseKind,omitempty" protobuf:"bytes,2,opt,name=responseKind"`
	// scope 表示资源的作用域，取值 Cluster 或 Namespaced
	// +optional
	Scope ResourceScope `json:"scope" protobuf:"bytes,3,opt,name=scope"`
	// singularResource 为资源的单数名称，便于客户端不区分单复数地处理资源。
	// 对用户而言单数形式通常更易理解，将资源名嵌入句子时应优先使用。
	// 例如命令行工具 kubectl 允许用单数名称代替复数名称。
	// 单数形式始终应为可选字段，不确定时使用规范的资源名称。
	// +optional
	SingularResource string `json:"singularResource" protobuf:"bytes,4,opt,name=singularResource"`
	// verbs 为支持的 API 操作类型列表（包括但不限于 get、list、watch、create、
	// update、patch、delete、deletecollection 和 proxy）。
	// +listType=set
	// +optional
	Verbs []string `json:"verbs" protobuf:"bytes,5,opt,name=verbs"`
	// shortNames 为该资源建议的简称列表。
	// +listType=set
	// +optional
	ShortNames []string `json:"shortNames,omitempty" protobuf:"bytes,6,rep,name=shortNames"`
	// categories 为此资源所属的资源分组列表（如 'all'）。
	// 客户端可借此简化对多种资源类型的批量操作。
	// +listType=set
	// +optional
	Categories []string `json:"categories,omitempty" protobuf:"bytes,7,rep,name=categories"`
	// subresources 为此资源提供的子资源列表。子资源位于 /apis/<APIGroupDiscovery.objectMeta.name>/<APIVersionDiscovery.version>/<APIResourceDiscovery.Resource>/name-of-instance/<APIResourceDiscovery.subresources[i].subresource>
	// +listType=map
	// +listMapKey=subresource
	// +optional
	Subresources []APISubresourceDiscovery `json:"subresources,omitempty" protobuf:"bytes,8,rep,name=subresources"`
}

// ResourceScope 是枚举，定义资源可用的不同作用域。
type ResourceScope string

// ResourceScope 的取值集合。
const (
	// ScopeCluster 表示集群级作用域。
	ScopeCluster ResourceScope = "Cluster"
	// ScopeNamespace 表示命名空间级作用域。
	ScopeNamespace ResourceScope = "Namespaced"
)

// DiscoveryFreshness 是枚举，表示 apiservice 发布的发现文档是否为最新。
type DiscoveryFreshness string

// DiscoveryFreshness 的取值集合。
const (
	// DiscoveryFreshnessCurrent 表示发现文档为最新。
	DiscoveryFreshnessCurrent DiscoveryFreshness = "Current"
	// DiscoveryFreshnessStale 表示发现文档可能已过时。
	DiscoveryFreshnessStale DiscoveryFreshness = "Stale"
)

// APISubresourceDiscovery 提供某 API 子资源的发现信息。
type APISubresourceDiscovery struct {
	// subresource 为子资源名称，用于 URL 路径，是此资源在所有版本中的唯一标识。
	// +optional
	Subresource string `json:"subresource" protobuf:"bytes,1,opt,name=subresource"`
	// responseKind 描述该端点通常返回的对象类型所对应的序列化模式的组、版本和种类。
	// 部分子资源不返回普通资源，其返回类型为 null 或空。
	// +optional
	ResponseKind *v1.GroupVersionKind `json:"responseKind,omitempty" protobuf:"bytes,2,opt,name=responseKind"`
	// acceptedTypes 描述该端点接受的种类。
	// 子资源可接受标准内容类型或定义自定义协商方案。
	// 该列表未必涵盖所有操作。
	// +listType=map
	// +listMapKey=group
	// +listMapKey=version
	// +listMapKey=kind
	// +optional
	AcceptedTypes []v1.GroupVersionKind `json:"acceptedTypes,omitempty" protobuf:"bytes,3,rep,name=acceptedTypes"`
	// verbs 为支持的 API 操作类型列表（包括但不限于 get、list、watch、create、
	// update、patch、delete、deletecollection 和 proxy）。子资源可定义
	// 标准 Kubernetes 操作之外的自定义动词。客户端可预期标准动词的行为
	// 与 Kubernetes 交互惯例一致。
	// +listType=set
	// +optional
	Verbs []string `json:"verbs" protobuf:"bytes,4,opt,name=verbs"`
}
