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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// ConversionStrategyType 描述不同的转换类型。
type ConversionStrategyType string

const (
	// KubeAPIApprovedAnnotation 是一个注解，为 k8s.io、*.k8s.io、kubernetes.io 或 *.kubernetes.io
	// 命名空间创建 CRD 时必须设置。
	// 其值应是指向当前 spec 获批位置 URL 的链接，因此 spec 更新时也应同步更新该 URL。
	// 若 API 未获批准，可将注解设为以 `"unapproved"` 开头的字符串，例如
	// `"unapproved, temporarily squatting"` 或 `"unapproved, experimental-only"`。不推荐这样做。
	KubeAPIApprovedAnnotation = "api-approved.kubernetes.io"

	// NoneConverter 是一种仅设置 CR 的 apiVersion、其余内容保持不变的转换器。
	NoneConverter ConversionStrategyType = "None"
	// WebhookConverter 是一种调用外部 webhook 来转换 CR 的转换器。
	WebhookConverter ConversionStrategyType = "Webhook"
)

// CustomResourceDefinitionSpec 描述用户希望其资源呈现的方式
type CustomResourceDefinitionSpec struct {
	// group 是所定义自定义资源的 API 组。
	// 自定义资源在 `/apis/<group>/...` 下提供服务。
	// 必须与 CustomResourceDefinition 的名称一致（形如 `<names.plural>.<group>`）。
	Group string `json:"group" protobuf:"bytes,1,opt,name=group"`
	// version 是所定义自定义资源的 API 版本。
	// 自定义资源在 `/apis/<group>/<version>/...` 下提供服务。
	// 若同时指定 `version` 和 `versions`，必须与 `versions` 列表第一项的名称一致。
	// 若已指定 `versions` 则可选。
	// Deprecated: 请改用 `versions`。
	// +optional
	Version string `json:"version,omitempty" protobuf:"bytes,2,opt,name=version"`
	// names 指定自定义资源的资源名和 kind 名。
	Names CustomResourceDefinitionNames `json:"names" protobuf:"bytes,3,opt,name=names"`
	// scope 表示所定义自定义资源是集群级还是命名空间级。
	// 允许值为 `Cluster` 和 `Namespaced`。默认为 `Namespaced`。
	Scope ResourceScope `json:"scope" protobuf:"bytes,4,opt,name=scope,casttype=ResourceScope"`
	// validation 描述用于校验和裁剪自定义资源的 schema。
	// 若存在，该校验 schema 会用于所有版本。
	// 顶层 schema 与按版本 schema 互斥。
	// +optional
	Validation *CustomResourceValidation `json:"validation,omitempty" protobuf:"bytes,5,opt,name=validation"`
	// subresources 指定所定义自定义资源具有哪些子资源。
	// 若存在，该字段为所有版本配置子资源。
	// 顶层子资源与按版本子资源互斥。
	// +optional
	Subresources *CustomResourceSubresources `json:"subresources,omitempty" protobuf:"bytes,6,opt,name=subresources"`
	// versions 是所定义自定义资源所有 API 版本的列表。
	// 若已指定 `version` 则可选。
	// 若同时指定 `version` 和 `versions`，`versions` 列表第一项的名称必须与 `version` 字段一致。
	// 版本名称用于计算 API discovery 中服务版本的排序。
	// 若版本字符串为 "kube-like"，会排在非 "kube-like" 版本之前，后者按字典序排列。
	// "Kube-like" 版本以 "v" 开头，后跟数字（主版本），再可选地跟 "alpha" 或 "beta"
	// 及另一数字（次版本）。排序先按 GA > beta > alpha（GA 指无 beta/alpha 后缀的版本），
	// 再比较主版本，然后比较次版本。版本排序示例：
	// v10, v2, v1, v11beta2, v10beta3, v3beta1, v12alpha1, v11alpha2, foo1, foo10。
	// +optional
	// +listType=atomic
	Versions []CustomResourceDefinitionVersion `json:"versions,omitempty" protobuf:"bytes,7,rep,name=versions"`
	// additionalPrinterColumns 指定 Table 输出中返回的额外列。
	// 详情见 https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables 。
	// 若存在，该字段为所有版本配置列。
	// 顶层列与按版本列互斥。
	// 若未指定任何顶层或按版本列，则使用显示自定义资源年龄的单列。
	// +optional
	// +listType=atomic
	AdditionalPrinterColumns []CustomResourceColumnDefinition `json:"additionalPrinterColumns,omitempty" protobuf:"bytes,8,rep,name=additionalPrinterColumns"`

	// selectableFields 指定可用作字段选择器的字段路径。
	// 参见 https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors
	//
	// +featureGate=CustomResourceFieldSelectors
	// +optional
	// +listType=atomic
	SelectableFields []SelectableField `json:"selectableFields,omitempty" protobuf:"bytes,11,rep,name=selectableFields"`

	// conversion 定义 CRD 的转换设置。
	// +optional
	Conversion *CustomResourceConversion `json:"conversion,omitempty" protobuf:"bytes,9,opt,name=conversion"`

	// preserveUnknownFields 表示持久化到存储时，OpenAPI schema 中未指定的对象字段应被保留。
	// apiVersion、kind、metadata 以及 metadata 中的已知字段始终保留。
	// 若为 false，则所有版本都必须定义 schema。
	// 为向后兼容，v1beta 中默认为 true。
	// Deprecated: v1 中将要求为 false。未知字段的保留可通过校验 schema 中的
	// `x-kubernetes-preserve-unknown-fields: true` 扩展指定。
	// 详情见 https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#field-pruning 。
	// +optional
	PreserveUnknownFields *bool `json:"preserveUnknownFields,omitempty" protobuf:"varint,10,opt,name=preserveUnknownFields"`
}

// CustomResourceConversion 描述如何在不同版本的 CR 之间转换。
type CustomResourceConversion struct {
	// strategy 指定自定义资源在不同版本间的转换方式。允许值：
	// - `None`：转换器仅修改 apiVersion，不触碰自定义资源中的其他字段。
	// - `Webhook`：API Server 会调用外部 webhook 执行转换。该选项需要附加信息，
	//   要求 spec.preserveUnknownFields 为 false，且已设置 spec.conversion.webhookClientConfig。
	Strategy ConversionStrategyType `json:"strategy" protobuf:"bytes,1,name=strategy"`

	// webhookClientConfig 是 strategy 为 `Webhook` 时如何调用 webhook 的说明。
	// 当 `strategy` 设为 `Webhook` 时必填。
	// +optional
	WebhookClientConfig *WebhookClientConfig `json:"webhookClientConfig,omitempty" protobuf:"bytes,2,name=webhookClientConfig"`

	// conversionReviewVersions 是 webhook 期望的 `ConversionReview` 版本的有序偏好列表。
	// API server 会使用列表中其支持的第一个版本。若列表中没有任何版本受 API server 支持，
	// 则该自定义资源的转换会失败。若已持久化的 Webhook 配置指定了允许的版本，
	// 却不包含 API Server 已知的任何版本，则对 webhook 的调用会失败。
	// 默认为 `["v1beta1"]`。
	// +optional
	// +listType=atomic
	ConversionReviewVersions []string `json:"conversionReviewVersions,omitempty" protobuf:"bytes,3,rep,name=conversionReviewVersions"`
}

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
type WebhookClientConfig struct {
	// url 以标准 URL 形式（`scheme://host:port/path`）给出 webhook 的位置。
	// 必须且只能指定 `url` 或 `service` 之一。
	//
	// `host` 不应指向集群内运行的服务；请改用 `service` 字段。在某些 apiserver 中
	// host 可能通过外部 DNS 解析（例如 `kube-apiserver` 无法解析集群内 DNS，
	// 因为那会破坏分层）。`host` 也可以是 IP 地址。
	//
	// 请注意，使用 `localhost` 或 `127.0.0.1` 作为 `host` 有风险，
	// 除非你非常小心地在所有运行可能调用该 webhook 的 apiserver 的主机上运行此 webhook。
	// 此类安装很可能不可移植，即难以在新集群中搭起来。
	//
	// scheme 必须为 "https"；URL 必须以 "https://" 开头。
	//
	// path 可选，若存在可为 URL 中允许的任意字符串。你可以用 path 向 webhook 传递任意字符串，
	// 例如集群标识符。
	//
	// 不允许使用用户认证或 basic auth，例如 "user:password@"。
	// 也不允许片段（"#..."）和查询参数（"?..."）。
	//
	// +optional
	URL *string `json:"url,omitempty" protobuf:"bytes,3,opt,name=url"`

	// service 是对该 webhook 所用 Service 的引用。必须指定 service 或 url 之一。
	//
	// 若 webhook 运行在集群内，则应使用 `service`。
	//
	// +optional
	Service *ServiceReference `json:"service,omitempty" protobuf:"bytes,1,opt,name=service"`

	// caBundle 是 PEM 编码的 CA 证书包，用于校验 webhook 的服务器证书。
	// 若未指定，则使用 apiserver 上的系统信任根。
	// +optional
	CABundle []byte `json:"caBundle,omitempty" protobuf:"bytes,2,opt,name=caBundle"`
}

// ServiceReference 持有对 Service.legacy.k8s.io 的引用
type ServiceReference struct {
	// namespace 是该 Service 的命名空间。
	// 必填
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// name 是该 Service 的名称。
	// 必填
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// path 是联系 webhook 的可选 URL 路径。
	// +optional
	Path *string `json:"path,omitempty" protobuf:"bytes,3,opt,name=path"`

	// port 是联系 webhook 的可选 Service 端口。
	// `port` 应为合法端口号（1-65535，含边界）。
	// 为向后兼容默认为 443。
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,4,opt,name=port"`
}

// CustomResourceDefinitionVersion 描述 CRD 的一个版本。
type CustomResourceDefinitionVersion struct {
	// name 是版本名称，如 “v1”、“v2beta1” 等。
	// 若 `served` 为 true，自定义资源会在该版本下通过 `/apis/<group>/<version>/...` 提供服务。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// served 是启用/禁用通过 REST API 提供该版本的标志
	Served bool `json:"served" protobuf:"varint,2,opt,name=served"`
	// storage 表示持久化自定义资源到存储时应使用该版本。
	// 必须恰有一个版本的 storage=true。
	Storage bool `json:"storage" protobuf:"varint,3,opt,name=storage"`
	// deprecated 表示该版本的自定义资源 API 已弃用。
	// 设为 true 时，发往该版本的 API 请求会在服务器响应中收到 warning 头。
	// 默认为 false。
	// +optional
	Deprecated bool `json:"deprecated,omitempty" protobuf:"varint,7,opt,name=deprecated"`
	// deprecationWarning 覆盖返回给 API 客户端的默认警告。
	// 仅当 `deprecated` 为 true 时可设置。
	// 默认警告会说明该版本已弃用，并在存在时建议使用同等或更高稳定性的最新服务版本。
	// +optional
	DeprecationWarning *string `json:"deprecationWarning,omitempty" protobuf:"bytes,8,opt,name=deprecationWarning"`
	// schema 描述用于校验和裁剪该版本自定义资源的 schema。
	// 顶层 schema 与按版本 schema 互斥。
	// 按版本 schema 不能全部设为相同值（此时应使用顶层校验 schema）。
	// +optional
	Schema *CustomResourceValidation `json:"schema,omitempty" protobuf:"bytes,4,opt,name=schema"`
	// subresources 指定该版本所定义自定义资源具有哪些子资源。
	// 顶层子资源与按版本子资源互斥。
	// 按版本子资源不能全部设为相同值（此时应使用顶层子资源）。
	// +optional
	Subresources *CustomResourceSubresources `json:"subresources,omitempty" protobuf:"bytes,5,opt,name=subresources"`
	// additionalPrinterColumns 指定 Table 输出中返回的额外列。
	// 详情见 https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables 。
	// 顶层列与按版本列互斥。
	// 按版本列不能全部设为相同值（此时应使用顶层列）。
	// 若未指定任何顶层或按版本列，则使用显示自定义资源年龄的单列。
	// +optional
	// +listType=atomic
	AdditionalPrinterColumns []CustomResourceColumnDefinition `json:"additionalPrinterColumns,omitempty" protobuf:"bytes,6,rep,name=additionalPrinterColumns"`

	// selectableFields 指定可用作字段选择器的字段路径。
	// 参见 https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors
	//
	// +featureGate=CustomResourceFieldSelectors
	// +optional
	// +listType=atomic
	SelectableFields []SelectableField `json:"selectableFields,omitempty" protobuf:"bytes,9,rep,name=selectableFields"`
}

// SelectableField 指定可与字段选择器一起使用的字段的 JSON 路径。
type SelectableField struct {
	// jsonPath 是一个简单 JSON 路径，对每个自定义资源求值以产生字段选择器的值。
	// 只允许不带数组记法的 JSON 路径。
	// 必须指向 string、boolean 或 integer 类型的字段。带枚举值的类型和带格式的字符串也允许。
	// 若 jsonPath 指向资源中不存在的字段，则求值为空字符串。
	// 不得指向 metadata 字段。
	// 必填。
	JSONPath string `json:"jsonPath" protobuf:"bytes,1,opt,name=jsonPath"`
}

// CustomResourceColumnDefinition 指定服务端打印的一列。
type CustomResourceColumnDefinition struct {
	// name 是该列的人类可读名称。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// type 是该列的 OpenAPI 类型定义。
	// 详情见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types 。
	Type string `json:"type" protobuf:"bytes,2,opt,name=type"`
	// format 是该列可选的 OpenAPI 类型定义。'name' 格式会应用于主标识列，
	// 帮助客户端识别该列即资源名。
	// 详情见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types 。
	// +optional
	Format string `json:"format,omitempty" protobuf:"bytes,3,opt,name=format"`
	// description 是该列的人类可读描述。
	// +optional
	Description string `json:"description,omitempty" protobuf:"bytes,4,opt,name=description"`
	// priority 是定义该列相对其他列重要性的整数。数字越小优先级越高。
	// 在空间受限场景中可能被省略的列，其 priority 应大于 0。
	// +optional
	Priority int32 `json:"priority,omitempty" protobuf:"bytes,5,opt,name=priority"`
	// JSONPath 是一个简单 JSON 路径（即带数组记法），对每个自定义资源求值以产生该列的值。
	JSONPath string `json:"JSONPath" protobuf:"bytes,6,opt,name=JSONPath"`
}

// CustomResourceDefinitionNames 指明为该 CustomResourceDefinition 提供服务的名称
type CustomResourceDefinitionNames struct {
	// plural 是要提供服务的资源的复数名。
	// 自定义资源在 `/apis/<group>/<version>/.../<plural>` 下提供服务。
	// 必须与 CustomResourceDefinition 的名称一致（形如 `<names.plural>.<group>`）。
	// 必须全小写。
	Plural string `json:"plural" protobuf:"bytes,1,opt,name=plural"`
	// singular 是资源的单数名，必须全小写。默认为小写的 `kind`。
	// +optional
	Singular string `json:"singular,omitempty" protobuf:"bytes,2,opt,name=singular"`
	// shortNames 是资源的短名，在 API discovery 文档中公开，
	// 客户端用它支持 `kubectl get <shortname>` 之类的调用。
	// 必须全小写。
	// +optional
	// +listType=atomic
	ShortNames []string `json:"shortNames,omitempty" protobuf:"bytes,3,opt,name=shortNames"`
	// kind 是资源的序列化 kind，通常为 CamelCase 且为单数。
	// 自定义资源实例在 API 调用中会将该值用作 `kind` 属性。
	Kind string `json:"kind" protobuf:"bytes,4,opt,name=kind"`
	// listKind 是该资源列表的序列化 kind。默认为 "`kind`List"。
	// +optional
	ListKind string `json:"listKind,omitempty" protobuf:"bytes,5,opt,name=listKind"`
	// categories 是该自定义资源所属的分组资源列表（如 'all'）。
	// 它会发布在 API discovery 文档中，客户端用它支持 `kubectl get all` 之类的调用。
	// +optional
	// +listType=atomic
	Categories []string `json:"categories,omitempty" protobuf:"bytes,6,rep,name=categories"`
}

// ResourceScope 是定义自定义资源可用作用域的枚举
type ResourceScope string

const (
	// ClusterScoped 表示资源为集群作用域。
	ClusterScoped ResourceScope = "Cluster"
	// NamespaceScoped 表示资源为命名空间作用域。
	NamespaceScoped ResourceScope = "Namespaced"
)

// ConditionStatus 是条件状态的字符串类型。
type ConditionStatus string

// 以下为合法的条件状态。"ConditionTrue" 表示资源处于该条件。
// "ConditionFalse" 表示资源不处于该条件。"ConditionUnknown" 表示 kubernetes
// 无法判断资源是否处于该条件。未来可能添加其他中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// CustomResourceDefinitionConditionType 是 CustomResourceDefinitionCondition.Type 的合法值
type CustomResourceDefinitionConditionType string

const (
	// Established 表示资源已生效。资源在首次所有名称都被接受且无冲突时即建立。
	// 资源会保持 Established 直到被删除，即使之后因名称变更而出现 NamesAccepted。注意并非所有名称都可变更。
	Established CustomResourceDefinitionConditionType = "Established"
	// NamesAccepted 表示为该 CustomResourceDefinition 选择的名称与该组中其他资源不冲突，
	// 因而被接受。
	NamesAccepted CustomResourceDefinitionConditionType = "NamesAccepted"
	// NonStructuralSchema 表示一个或多个 OpenAPI schema 不是 structural 的。
	//
	// 若 schema 为所有值指定了类型，则为 structural；唯一例外是以下情况：
	// - x-kubernetes-int-or-string: true —— 用于可为整数或字符串的字段
	// - x-kubernetes-preserve-unknown-fields: true —— 用于原始、未指定的 JSON 值
	// 并且在 allOf、anyOf、oneOf 或 not 下未指定 type、additionalProperties、default、nullable
	// 或 x-kubernetes-* 厂商扩展。
	//
	// v1 API 组将不再允许非 structural schema。此外，非 structural CRD 无法使用新特性：
	// - 裁剪
	// - 默认值填充
	// - 只读
	// - OpenAPI 发布
	// - webhook 转换
	NonStructuralSchema CustomResourceDefinitionConditionType = "NonStructuralSchema"
	// Terminating 表示该 CustomResourceDefinition 已被删除，正在清理。
	Terminating CustomResourceDefinitionConditionType = "Terminating"
	// KubernetesAPIApprovalPolicyConformant 表示 *.k8s.io 或 *.kubernetes.io 下的 API 是否获批。
	// 对于这些组之外的 CRD，不会设置该条件。对于这些组内的 CRD，
	// 若 .metadata.annotations["api-approved.kubernetes.io"] 设为 URL 则为 true，否则为 false。
	// 详情见 https://github.com/kubernetes/enhancements/pull/1111 。
	KubernetesAPIApprovalPolicyConformant CustomResourceDefinitionConditionType = "KubernetesAPIApprovalPolicyConformant"
)

// CustomResourceDefinitionCondition 包含此 pod 当前条件的详情。
type CustomResourceDefinitionCondition struct {
	// type 是条件类型。类型包括 Established、NamesAccepted 和 Terminating。
	Type CustomResourceDefinitionConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=CustomResourceDefinitionConditionType"`
	// status 是条件状态。
	// 可为 True、False、Unknown。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// lastTransitionTime 是条件上次从一个状态转换到另一个状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// reason 是条件上次转换的唯一、单词、CamelCase 原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// message 是表明上次转换详情的人类可读消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
	// observedGeneration 表示设置该条件所依据的 .metadata.generation。
	// 例如，若当前 .metadata.generation 为 12，而 .status.conditions[x].observedGeneration 为 9，
	// 则该条件相对于实例当前状态已过期。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,6,opt,name=observedGeneration"`
}

// CustomResourceDefinitionStatus 表示 CustomResourceDefinition 的状态
type CustomResourceDefinitionStatus struct {
	// conditions 表示 CustomResourceDefinition 各个方面的状态
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []CustomResourceDefinitionCondition `json:"conditions" protobuf:"bytes,1,opt,name=conditions"`

	// acceptedNames 是实际用于提供 discovery 服务的名称。
	// 它们可能不同于 spec 中的名称。
	// +optional
	AcceptedNames CustomResourceDefinitionNames `json:"acceptedNames" protobuf:"bytes,2,opt,name=acceptedNames"`

	// storedVersions 列出曾被持久化的 CustomResource 的所有版本。跟踪这些版本
	// 可为 etcd 中的存储版本提供迁移路径。该字段可变，
	// 以便迁移控制器完成到另一版本的迁移（确保存储中不再残留旧对象）后，
	// 从该列表中移除其余版本。
	// 当某版本仍存在于该列表时，不得将其从 `spec.versions` 中移除。
	// +optional
	// +listType=atomic
	StoredVersions []string `json:"storedVersions" protobuf:"bytes,3,rep,name=storedVersions"`
	// CRD 控制器观测到的 generation。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,4,opt,name=observedGeneration"`
}

// CustomResourceCleanupFinalizer 是删除 CustomResourceDefinition 实例所用 finalizer 的名称
const CustomResourceCleanupFinalizer = "customresourcecleanup.apiextensions.k8s.io"

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=apiextensions.k8s.io,v1,CustomResourceDefinition

// CustomResourceDefinition 表示应在 API server 上暴露的资源。其名称必须符合
// <.spec.name>.<.spec.group> 格式。
// 自 v1.16 起弃用，计划在 v1.22 移除。请改用 apiextensions.k8s.io/v1 的 CustomResourceDefinition。
type CustomResourceDefinition struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 描述用户希望资源呈现的方式
	Spec CustomResourceDefinitionSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// status 表示 CustomResourceDefinition 的实际状态
	// +optional
	Status CustomResourceDefinitionStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=apiextensions.k8s.io,v1,CustomResourceDefinitionList

// CustomResourceDefinitionList 是 CustomResourceDefinition 对象的列表。
type CustomResourceDefinitionList struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 列出各个 CustomResourceDefinition 对象
	Items []CustomResourceDefinition `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CustomResourceValidation 是 CustomResource 校验方法的列表。
type CustomResourceValidation struct {
	// openAPIV3Schema 是用于校验和裁剪的 OpenAPI v3 schema。
	// +optional
	OpenAPIV3Schema *JSONSchemaProps `json:"openAPIV3Schema,omitempty" protobuf:"bytes,1,opt,name=openAPIV3Schema"`
}

// CustomResourceSubresources 定义 CustomResource 的 status 和 scale 子资源。
type CustomResourceSubresources struct {
	// status 表示自定义资源应提供 `/status` 子资源。
	// 启用后：
	// 1. 发往自定义资源主端点的请求会忽略对对象 `status` 部分的修改。
	// 2. 发往自定义资源 `/status` 子资源的请求会忽略对对象 `status` 部分之外任何内容的修改。
	// +optional
	Status *CustomResourceSubresourceStatus `json:"status,omitempty" protobuf:"bytes,1,opt,name=status"`
	// scale 表示自定义资源应提供 `/scale` 子资源，返回 `autoscaling/v1` Scale 对象。
	// +optional
	Scale *CustomResourceSubresourceScale `json:"scale,omitempty" protobuf:"bytes,2,opt,name=scale"`
}

// CustomResourceSubresourceStatus 定义如何为 CustomResource 提供 status 子资源。
// Status 由 CustomResource 内的 `.status` JSON 路径表示。设置后：
// * 为自定义资源暴露 /status 子资源
// * 发往 /status 子资源的 PUT 请求接收自定义资源对象，并忽略 status 部分之外的任何修改
// * 发往自定义资源的 PUT/POST/PATCH 请求忽略对 status 部分的修改
type CustomResourceSubresourceStatus struct{}

// CustomResourceSubresourceScale 定义如何为 CustomResource 提供 scale 子资源。
type CustomResourceSubresourceScale struct {
	// specReplicasPath 定义自定义资源内对应 Scale `spec.replicas` 的 JSON 路径。
	// 只允许不带数组记法的 JSON 路径。
	// 必须是 `.spec` 下的 JSON 路径。
	// 若自定义资源中给定路径下没有值，`/scale` 子资源在 GET 时会返回错误。
	SpecReplicasPath string `json:"specReplicasPath" protobuf:"bytes,1,name=specReplicasPath"`
	// statusReplicasPath 定义自定义资源内对应 Scale `status.replicas` 的 JSON 路径。
	// 只允许不带数组记法的 JSON 路径。
	// 必须是 `.status` 下的 JSON 路径。
	// 若自定义资源中给定路径下没有值，`/scale` 子资源中的 `status.replicas` 值
	// 默认为 0。
	StatusReplicasPath string `json:"statusReplicasPath" protobuf:"bytes,2,opt,name=statusReplicasPath"`
	// labelSelectorPath 定义自定义资源内对应 Scale `status.selector` 的 JSON 路径。
	// 只允许不带数组记法的 JSON 路径。
	// 必须是 `.status` 或 `.spec` 下的 JSON 路径。
	// 必须设置才能与 HorizontalPodAutoscaler 配合使用。
	// 该 JSON 路径指向的字段必须是字符串字段（而非复杂 selector 结构），
	// 其中包含字符串形式的序列化标签选择器。
	// 更多信息：https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions#scale-subresource
	// 若自定义资源中给定路径下没有值，`/scale` 子资源中的 `status.selector` 值
	// 默认为空字符串。
	// +optional
	LabelSelectorPath *string `json:"labelSelectorPath,omitempty" protobuf:"bytes,3,opt,name=labelSelectorPath"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.13
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// 此 API 从不对外提供。它用于 apiserver 发出的出站请求。这可确保它永远不会被意外提供，
// 且针对该组运行生成器可保护未来可能对外提供的 API。
// +k8s:prerelease-lifecycle-gen:replacement=apiextensions.k8s.io,v1,ConversionReview

// ConversionReview 描述转换请求/响应。
type ConversionReview struct {
	metav1.TypeMeta `json:""`
	// request 描述转换请求的属性。
	// +optional
	Request *ConversionRequest `json:"request,omitempty" protobuf:"bytes,1,opt,name=request"`
	// response 描述转换响应的属性。
	// +optional
	Response *ConversionResponse `json:"response,omitempty" protobuf:"bytes,2,opt,name=response"`
}

// ConversionRequest 描述转换请求参数。
type ConversionRequest struct {
	// uid 是单个请求/响应的标识符。它用于区分原本相同的请求实例（并行请求等）。
	// UID 旨在跟踪 Kubernetes API server 与 webhook 之间的往返（请求/响应），
	// 而非用户请求。它适合用于关联 webhook 与 apiserver 的日志，便于审计或调试。
	UID types.UID `json:"uid" protobuf:"bytes,1,name=uid"`
	// desiredAPIVersion 是将给定对象转换到的版本，例如 "myapi.example.com/v1"
	DesiredAPIVersion string `json:"desiredAPIVersion" protobuf:"bytes,2,name=desiredAPIVersion"`
	// objects 是要转换的自定义资源对象列表。
	// +listType=atomic
	Objects []runtime.RawExtension `json:"objects" protobuf:"bytes,3,rep,name=objects"`
}

// ConversionResponse 描述转换响应。
type ConversionResponse struct {
	// uid 是单个请求/响应的标识符。
	// 应从对应的 `request.uid` 复制过来。
	UID types.UID `json:"uid" protobuf:"bytes,1,name=uid"`
	// convertedObjects 是 `request.objects` 的转换后版本列表（若 `result` 成功），否则为空。
	// webhook 应将这些对象的 `apiVersion` 设为 `request.desiredAPIVersion`。该列表
	// 还必须与输入列表大小相同、对象相同且顺序相同（kind、metadata.uid、metadata.name
	// 和 metadata.namespace 均相等）。
	// webhook 可以修改 labels 和 annotations。对 metadata 的任何其他修改都会被静默忽略。
	// +listType=atomic
	ConvertedObjects []runtime.RawExtension `json:"convertedObjects" protobuf:"bytes,2,rep,name=convertedObjects"`
	// result 包含转换结果；转换失败时附带更多详情。`result.status` 决定
	// 转换失败还是成功。`result.status` 字段必填，表示转换的成功或失败。
	// 成功转换必须将 `result.status` 设为 `Success`。失败转换必须将
	// `result.status` 设为 `Failure`，并在 `result.message` 中提供更多详情，同时返回 HTTP 200。
	// `result.message` 将用于构造最终用户的错误消息。
	Result metav1.Status `json:"result" protobuf:"bytes,3,name=result"`
}
