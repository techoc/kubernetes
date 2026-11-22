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

// Package apiextensions 包含 CustomResourceDefinition 的内部 API 类型。
package apiextensions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConversionStrategyType 描述不同的转换策略类型。
type ConversionStrategyType string

const (
	// NoneConverter 只设置 CR 的 apiVersion，其余内容保持不变。
	NoneConverter ConversionStrategyType = "None"
	// WebhookConverter 调用外部 webhook 来转换 CR。
	WebhookConverter ConversionStrategyType = "Webhook"
)

// CustomResourceDefinitionSpec 描述用户希望资源呈现的样子。
type CustomResourceDefinitionSpec struct {
	// Group 是该资源所属的组。
	Group string
	// Version 是该资源所属的版本。
	// 若提供了 Versions 字段，本字段应始终位于其中第一项。
	// 可选，但 Version 与 Versions 至少要设置一个。
	// Deprecated: 请使用 `Versions`。
	Version string
	// Names 是描述该自定义资源所用的名称。
	Names CustomResourceDefinitionNames
	// Scope 表示资源是集群级还是命名空间级。默认为命名空间级。
	Scope ResourceScope
	// Validation 描述 CustomResource 的校验方式。
	// 可选，是所有版本共用的全局校验 schema。
	// 顶层 schema 与按版本的 schema 互斥。
	// +optional
	Validation *CustomResourceValidation
	// Subresources 描述 CustomResource 的子资源。
	// 可选，是所有版本共用的全局子资源。
	// 顶层子资源与按版本的子资源互斥。
	// +optional
	Subresources *CustomResourceSubresources
	// Versions 是该资源支持的所有版本。
	// 若提供了 Version 字段，本字段为可选。
	// 校验：目前所有版本必须使用相同的校验 schema，即顶层
	// Validation 字段会作用于所有这些版本。
	// 排序：版本名用于计算顺序。
	// 若版本字符串是“kube-like”形式，会排在非“kube-like”字符串之前；后者按字典序排列。
	// “Kube-like”版本以 "v" 开头，后跟数字（主版本号），再可选跟 "alpha" 或 "beta"
	// 以及另一个数字（次版本号）。排序时先按 GA > beta > alpha（GA 即无 beta/alpha 后缀的版本），
	// 再比较主版本号，最后比较次版本号。排序示例：
	// v10, v2, v1, v11beta2, v10beta3, v3beta1, v12alpha1, v11alpha2, foo1, foo10.
	Versions []CustomResourceDefinitionVersion
	// AdditionalPrinterColumns 是额外显示的列，例如 kubectl 在名称旁显示的列。默认有一列创建时间。
	// 可选，是所有版本共用的全局列。
	// 顶层列与按版本的列互斥。
	// +optional
	AdditionalPrinterColumns []CustomResourceColumnDefinition
	// selectableFields 指定可用作字段选择器的字段路径。
	// 最多允许 8 个可选字段。
	// 参见 https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors
	// 顶层列与按版本的列互斥。
	// +optional
	SelectableFields []SelectableField

	// `conversion` 定义 CRD 的转换设置。
	Conversion *CustomResourceConversion

	// preserveUnknownFields 关闭裁剪：不裁剪未在 OpenAPI schema 中声明的对象字段。
	// apiVersion、kind、metadata 以及 metadata 内部已知字段始终保留。
	// v1beta 中默认为 true，v1 中将默认为 false。
	PreserveUnknownFields *bool
}

// CustomResourceConversion 描述如何转换 CR 的不同版本。
type CustomResourceConversion struct {
	// `strategy` 指定转换策略，允许的取值：
	// - `None`：仅修改 apiVersion，不改动 CR 的其他字段。
	// - `Webhook`：API Server 调用外部 webhook 完成转换。该选项还需要
	//   额外信息，并要求 spec.preserveUnknownFields 为 false。
	Strategy ConversionStrategyType

	// 当 strategy 为 `Webhook` 时，`webhookClientConfig` 指示如何调用 webhook。
	WebhookClientConfig *WebhookClientConfig

	// ConversionReviewVersions 是 webhook 期望的 `ConversionReview` 版本按偏好排序的列表。
	// API server 会尝试使用它支持的第一个版本。
	// 若列表中没有任何版本被 API server 支持，该对象的转换将失败。
	// 若持久化的 webhook 配置指定了允许的版本，却不包含 API server 已知的任何版本，
	// 则对该 webhook 的调用会失败。
	// +optional
	ConversionReviewVersions []string
}

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
// 其字段与 admissionregistration.internal.WebhookClientConfig 相同。
type WebhookClientConfig struct {
	// `url` 以标准 URL 形式（`scheme://host:port/path`）给出 webhook 的位置。
	// `url` 与 `service` 必须且只能指定一个。
	//
	// `host` 不应指向集群内运行的服务；请改用 `service` 字段。某些 apiserver 可能
	// 通过外部 DNS 解析该 host（例如 `kube-apiserver` 无法解析集群内 DNS，
	// 否则会造成分层违规）。`host` 也可以是 IP 地址。
	//
	// 注意：除非确保在可能调用该 webhook 的所有 apiserver 所在主机上都运行此 webhook，
	// 否则使用 `localhost` 或 `127.0.0.1` 作为 `host` 是有风险的。此类安装通常不可移植，
	// 即难以在新集群中复现。
	//
	// scheme 必须为 "https"；URL 必须以 "https://" 开头。
	//
	// path 可选；若存在，可为 URL 允许的任意字符串。可用 path 向 webhook
	// 传递任意字符串，例如集群标识符。
	//
	// 不允许使用用户认证或基本认证，例如 "user:password@"。
	// 也不允许使用片段（"#..."）与查询参数（"?..."）。
	//
	// +optional
	URL *string

	// `service` 是该 webhook 对应的 service 引用。`service` 与 `url` 必须指定其中一个。
	//
	// 若 webhook 运行在集群内，应使用 `service`。
	//
	// +optional
	Service *ServiceReference

	// `caBundle` 是 PEM 编码的 CA 证书包，用于校验 webhook 的服务端证书。
	// 若未指定，则使用 apiserver 上的系统信任根。
	// +optional
	CABundle []byte
}

// ServiceReference 持有对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// `namespace` 是 service 所在的命名空间。
	// 必填
	Namespace string
	// `name` 是 service 的名称。
	// 必填
	Name string

	// `path` 是可选的 URL 路径，会随发往该 service 的每个请求发送。
	// +optional
	Path *string

	// 若指定，表示承载 webhook 的 service 端口。
	// `port` 应为合法端口号（1-65535，含两端）。
	// +optional
	Port int32
}

// CustomResourceDefinitionVersion 描述 CRD 的一个版本。
type CustomResourceDefinitionVersion struct {
	// Name 是版本名，例如 “v1”、“v2beta1” 等。
	Name string
	// Served 是启用/禁用该版本是否通过 REST API 提供的标志。
	Served bool
	// Storage 标记该版本为存储版本。必须有且只有一个版本被标记为存储版本。
	Storage bool
	// deprecated 表示该版本的自定义资源 API 已废弃。
	// 设为 true 时，发往该版本的 API 请求会在服务器响应中带上警告头。
	// 默认为 false。
	Deprecated bool
	// deprecationWarning 覆盖返回给 API 客户端的默认警告。
	// 仅当 `deprecated` 为 true 时可以设置。
	// 默认警告会说明该版本已废弃，并建议使用稳定度相同或更高、且已提供的最新版本（若存在）。
	DeprecationWarning *string
	// Schema 描述用于校验、裁剪与默认值填充的 CustomResource schema。
	// 顶层 schema 与按版本的 schema 互斥。
	// 各版本的 schema 不能全部设为相同的值（此时应使用顶层校验 schema）。
	// 该字段为 alpha 级别，仅对启用了 CustomResourceWebhookConversion 特性的服务器生效。
	// +optional
	Schema *CustomResourceValidation
	// Subresources 描述 CustomResource 的子资源。
	// 顶层子资源与按版本的子资源互斥。
	// 各版本的子资源不能全部设为相同的值（此时应使用顶层子资源）。
	// 该字段为 alpha 级别，仅对启用了 CustomResourceWebhookConversion 特性的服务器生效。
	// +optional
	Subresources *CustomResourceSubresources
	// AdditionalPrinterColumns 是额外显示的列，例如 kubectl 在名称旁显示的列。默认有一列创建时间。
	// 顶层列与按版本的列互斥。
	// 各版本的列不能全部设为相同的值（此时应使用顶层列）。
	// 该字段为 alpha 级别，仅对启用了 CustomResourceWebhookConversion 特性的服务器生效。
	// NOTE: 1.13 之前创建的 CRD 会默认填充顶层 additionalPrinterColumns 字段。若要应用
	// 改为按版本 additionalPrinterColumns 的更新，必须显式把顶层 additionalPrinterColumns 字段置为 null。
	// +optional
	AdditionalPrinterColumns []CustomResourceColumnDefinition

	// selectableFields 指定可用作字段选择器的字段路径。
	// 最多允许 8 个可选字段。
	// 参见 https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors
	// +optional
	SelectableFields []SelectableField
}

// SelectableField 指定可用于字段选择器的字段的 JSON path。
type SelectableField struct {
	// jsonPath 是简单 JSON path，会对每个自定义资源求值，以产生字段选择器的值。
	// 仅允许不含数组记法的 JSON path。
	// 必须指向 string、boolean 或 integer 类型的字段。允许带枚举值的类型
	// 以及带 format 的字符串。
	// 若 jsonPath 指向资源中不存在的字段，则求值为空字符串。
	// 不得指向 metadata 字段。
	// 必填。
	JSONPath string
}

// CustomResourceColumnDefinition 定义一个用于服务端输出的列。
type CustomResourceColumnDefinition struct {
	// name 是该列的人类可读名称。
	Name string
	// type 是该列的 OpenAPI 类型定义。
	// 参见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types 了解更多。
	Type string
	// format 是该列可选的 OpenAPI 类型定义。'name' format 会应用于
	// 主标识符列，便于客户端识别该列是资源名称。
	// 参见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types 了解更多。
	Format string
	// description 是该列的人类可读描述。
	Description string
	// priority 是整数，定义该列相对其他列的重要性。数值越小优先级越高。
	// 在空间有限的场景中可能被省略的列应给予更高的 priority。
	Priority int32

	// JSONPath 是简单 JSON path，即不含数组记法。
	JSONPath string
}

// CustomResourceDefinitionNames 表示用于提供该 CustomResourceDefinition 的名称。
type CustomResourceDefinitionNames struct {
	// Plural 是要提供的资源复数名。它还必须与 CustomResourceDefinition 的注册名一致：
	// 即 plural.group，且必须全小写。
	Plural string
	// Singular 是资源的单数名。必须全小写。默认为小写的 <kind>。
	Singular string
	// ShortNames 是资源的短名，必须全小写。
	ShortNames []string
	// Kind 是资源序列化后的 kind。通常为 CamelCase 且为单数。
	Kind string
	// ListKind 是该资源 list 序列化后的 kind。默认为 <kind>List。
	ListKind string
	// Categories 是自定义资源所属的成组资源列表（例如 'all'）。
	// +optional
	Categories []string
}

// ResourceScope 是定义自定义资源可用作用域的枚举。
type ResourceScope string

const (
	// ClusterScoped 表示资源为集群级。
	ClusterScoped ResourceScope = "Cluster"
	// NamespaceScoped 表示资源为命名空间级。
	NamespaceScoped ResourceScope = "Namespaced"
)

// ConditionStatus 是条件状态的枚举值。
type ConditionStatus string

// 以下是合法的条件状态。"ConditionTrue" 表示资源处于该条件中。
// "ConditionFalse" 表示资源不处于该条件中。"ConditionUnknown" 表示 Kubernetes
// 无法判断资源是否处于该条件。未来可以增加其他中间状态，例如 ConditionDegraded。
const (
	// ConditionTrue 表示资源处于该条件。
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示资源不处于该条件。
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示 Kubernetes 无法判断资源是否处于该条件。
	ConditionUnknown ConditionStatus = "Unknown"
)

// CustomResourceDefinitionConditionType 是 CustomResourceDefinitionCondition.Type 的合法取值。
type CustomResourceDefinitionConditionType string

const (
	// Established 表示资源已变为活跃状态。当所有名称首次被无冲突地接受时，
	// 资源即为 Established。资源一旦建立会一直保持，直到被删除，即使之后因名称变更
	// 再次触发 NamesAccepted。注意并非所有名称都可以修改。
	Established CustomResourceDefinitionConditionType = "Established"
	// NamesAccepted 表示为该 CustomResourceDefinition 选择的名称与同组内的其他名称
	// 无冲突，因而被接受。
	NamesAccepted CustomResourceDefinitionConditionType = "NamesAccepted"
	// NonStructuralSchema 表示一个或多个 OpenAPI schema 不是 structural 的。
	//
	// 若 schema 为所有值指定了类型，则为 structural，仅以下例外：
	// - x-kubernetes-int-or-string: true —— 用于可为 integer 或 string 的字段
	// - x-kubernetes-preserve-unknown-fields: true —— 用于原始、未指定的 JSON 值
	// 并且 allOf、anyOf、oneOf 或 not 之下不得指定 type、additionalProperties、default、
	// nullable 或 x-kubernetes-* 厂商扩展。
	//
	// v1 API 组将不再允许非 structural schema。此外，新特性不会对非 structural CRD 可用：
	// - 裁剪（pruning）
	// - 默认值填充（defaulting）
	// - 只读（read-only）
	// - OpenAPI 发布
	// - webhook 转换
	NonStructuralSchema CustomResourceDefinitionConditionType = "NonStructuralSchema"
	// Terminating 表示该 CustomResourceDefinition 已被删除，正在清理。
	Terminating CustomResourceDefinitionConditionType = "Terminating"
	// KubernetesAPIApprovalPolicyConformant 表示 *.k8s.io 或 *.kubernetes.io 中的 API 是否获得批准。
	// 对于这些组之外的 CRD，不会设置该条件。对于这些组内的 CRD，若
	// .metadata.annotations["api-approved.kubernetes.io"] 被设置为 URL 则为 true，否则为 false。
	// 详见 https://github.com/kubernetes/enhancements/pull/1111。
	KubernetesAPIApprovalPolicyConformant CustomResourceDefinitionConditionType = "KubernetesAPIApprovalPolicyConformant"
)

// CustomResourceDefinitionCondition 包含此 pod 当前条件的详情。
type CustomResourceDefinitionCondition struct {
	// Type 是条件的类型。类型包括 Established、NamesAccepted 与 Terminating。
	Type CustomResourceDefinitionConditionType
	// Status 是条件的状态。
	// 可为 True、False、Unknown。
	Status ConditionStatus
	// 条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time
	// 条件最近一次转变的唯一、单词、CamelCase 原因。
	// +optional
	Reason string
	// 人类可读的消息，说明最近一次转变的详情。
	// +optional
	Message string
	// observedGeneration 表示该条件所基于的 .metadata.generation。
	// 例如，若 .metadata.generation 当前为 12，而 .status.conditions[x].observedGeneration 为 9，
	// 则该条件相对于实例当前状态已过期。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64
}

// CustomResourceDefinitionStatus 表示 CustomResourceDefinition 的状态。
type CustomResourceDefinitionStatus struct {
	// Conditions 表示 CustomResourceDefinition 各个方面的状态。
	// +listType=map
	// +listMapKey=type
	Conditions []CustomResourceDefinitionCondition

	// AcceptedNames 是实际用于提供 discovery 的名称。
	// 它们可能与 spec 中的名称不同。
	AcceptedNames CustomResourceDefinitionNames

	// StoredVersions 是所有曾被持久化的 CustomResource 版本。跟踪这些版本
	// 可为 etcd 中的存储版本提供迁移路径。该字段可变，
	// 因此迁移控制器可以先完成到另一个版本的迁移（即存储中不再残留旧对象），
	// 再从列表中移除其余版本。
	// 列表中的任何版本都不能从 spec.Versions 字段中移除。
	StoredVersions []string

	// CRD controller 观察到的 generation。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64
}

// CustomResourceCleanupFinalizer 是用于删除 CustomResourceDefinition 实例的 finalizer 名称。
const CustomResourceCleanupFinalizer = "customresourcecleanup.apiextensions.k8s.io"

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CustomResourceDefinition 表示应在 API server 上暴露的资源。
// 其名称必须为 <.spec.name>.<.spec.group> 格式。
type CustomResourceDefinition struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 描述用户希望资源呈现的样子。
	Spec CustomResourceDefinitionSpec
	// Status 表示 CustomResourceDefinition 的实际状态。
	Status CustomResourceDefinitionStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CustomResourceDefinitionList 是 CustomResourceDefinition 对象的列表。
type CustomResourceDefinitionList struct {
	metav1.TypeMeta
	metav1.ListMeta

	// Items 是各个 CustomResourceDefinition。
	Items []CustomResourceDefinition
}

// CustomResourceValidation 是 CustomResource 校验方式的列表。
type CustomResourceValidation struct {
	// OpenAPIV3Schema 是用于校验的 OpenAPI v3 schema。
	OpenAPIV3Schema *JSONSchemaProps
}

// CustomResourceSubresources 定义 CustomResource 的 status 与 scale 子资源。
type CustomResourceSubresources struct {
	// Status 表示 CustomResource 的 status 子资源。
	Status *CustomResourceSubresourceStatus
	// Scale 表示 CustomResource 的 scale 子资源。
	Scale *CustomResourceSubresourceScale
}

// CustomResourceSubresourceStatus 定义如何为 CustomResource 提供 status 子资源。
// Status 由 CustomResource 内部的 `.status` JSON 路径表示。设置后：
// * 为该自定义资源暴露 /status 子资源
// * 发往 /status 子资源的 PUT 请求接收自定义资源对象，并忽略除 status 段之外的任何改动
// * 发往该自定义资源的 PUT/POST/PATCH 请求忽略对 status 段的改动
type CustomResourceSubresourceStatus struct{}

// CustomResourceSubresourceScale 定义如何为 CustomResource 提供 scale 子资源。
type CustomResourceSubresourceScale struct {
	// SpecReplicasPath 定义 CustomResource 中与 Scale.Spec.Replicas 对应的 JSON 路径。
	// 仅允许不含数组记法的 JSON path。
	// 必须是 .spec 之下的 JSON Path。
	// 若 CustomResource 中该路径下没有值，GET /scale 子资源将返回错误。
	SpecReplicasPath string
	// StatusReplicasPath 定义 CustomResource 中与 Scale.Status.Replicas 对应的 JSON 路径。
	// 仅允许不含数组记法的 JSON path。
	// 必须是 .status 之下的 JSON Path。
	// 若 CustomResource 中该路径下没有值，/scale 子资源中的 status replicas 值将默认取 0。
	StatusReplicasPath string
	// LabelSelectorPath 定义 CustomResource 中与 Scale.Status.Selector 对应的 JSON 路径。
	// 仅允许不含数组记法的 JSON path。
	// 必须是 .status 或 .spec 之下的 JSON Path。
	// 必须设置才能与 HPA 配合工作。
	// 该 JSON 路径指向的字段必须是 string 字段（而非复杂的选择器结构体），
	// 其中包含字符串形式的序列化标签选择器。
	// 更多信息：https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions#scale-subresource
	// 若 CustomResource 中该路径下没有值，/scale 子资源中的 status label selector 值将默认为空字符串。
	// +optional
	LabelSelectorPath *string
}
