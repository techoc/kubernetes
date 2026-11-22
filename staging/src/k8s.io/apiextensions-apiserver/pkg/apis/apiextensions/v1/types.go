/*
Copyright 2019 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// ConversionStrategyType 描述不同的转换类型。
type ConversionStrategyType string

const (
	// KubeAPIApprovedAnnotation 是创建 k8s.io、*.k8s.io、kubernetes.io 或
	// *.kubernetes.io 命名空间下的 CRD 时必须设置的注解。
	// 其值应是指向当前 spec 审批链接的 URL，因此 spec 更新时 URL 也应一并更新。
	// 若 API 未获批准，可把注解设为以 `"unapproved"` 开头的字符串，例如
	// `"unapproved, temporarily squatting"` 或 `"unapproved, experimental-only"`。
	// 不推荐这样做。
	KubeAPIApprovedAnnotation = "api-approved.kubernetes.io"

	// NoneConverter 只修改 CR 的 apiVersion，其余内容保持不变。
	NoneConverter ConversionStrategyType = "None"
	// WebhookConverter 调用外部 webhook 来转换 CR。
	WebhookConverter ConversionStrategyType = "Webhook"
)

// CustomResourceDefinitionSpec 描述用户希望其资源呈现的样子。
type CustomResourceDefinitionSpec struct {
	// group 是所定义自定义资源的 API 组。
	// 自定义资源在 `/apis/<group>/...` 下提供服务。
	// 必须与 CustomResourceDefinition 的名称一致（形式为 `<names.plural>.<group>`）。
	Group string `json:"group" protobuf:"bytes,1,opt,name=group"`
	// names 指定自定义资源的资源名和 kind 名。
	Names CustomResourceDefinitionNames `json:"names" protobuf:"bytes,3,opt,name=names"`
	// scope 表示所定义的自定义资源是集群级还是命名空间级。
	// 允许的值为 `Cluster` 和 `Namespaced`。
	Scope ResourceScope `json:"scope" protobuf:"bytes,4,opt,name=scope,casttype=ResourceScope"`
	// versions 是所定义自定义资源所有 API 版本的列表。
	// 版本名用于计算 API discovery 中列出 served 版本时的排序。
	// 若版本字符串是 "kube-like" 的，则排在非 "kube-like" 版本之前，
	// 后者按字典序排列。"Kube-like" 版本以 "v" 开头，后跟数字（主版本号），
	// 再可选地跟 "alpha" 或 "beta" 加另一个数字（次版本号）。
	// 排序规则是先按 GA > beta > alpha（GA 即无 beta/alpha 等后缀的版本），
	// 再比较主版本号，然后比较次版本号。排序示例：
	// v10, v2, v1, v11beta2, v10beta3, v3beta1, v12alpha1, v11alpha2, foo1, foo10.
	// +listType=atomic
	Versions []CustomResourceDefinitionVersion `json:"versions" protobuf:"bytes,7,rep,name=versions"`

	// conversion 定义 CRD 的转换设置。
	// +optional
	Conversion *CustomResourceConversion `json:"conversion,omitempty" protobuf:"bytes,9,opt,name=conversion"`

	// preserveUnknownFields 表示持久化到存储时，应保留 OpenAPI schema 中
	// 未指定的对象字段。apiVersion、kind、metadata 以及 metadata 中的已知字段
	// 始终保留。
	// 该字段已弃用，建议改为在 `spec.versions[*].schema.openAPIV3Schema` 中
	// 设置 `x-preserve-unknown-fields` 为 true。
	// 详见 https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#field-pruning
	// +optional
	PreserveUnknownFields bool `json:"preserveUnknownFields,omitempty" protobuf:"varint,10,opt,name=preserveUnknownFields"`
}

// CustomResourceConversion 描述如何在 CR 的不同版本之间转换。
type CustomResourceConversion struct {
	// strategy 指定自定义资源在版本间转换的方式。允许的值：
	// - `"None"`：转换器只修改 apiVersion，不碰自定义资源中的其他任何字段。
	// - `"Webhook"`：API Server 将调用外部 webhook 完成转换。该选项还需要额外信息，
	//   要求 spec.preserveUnknownFields 为 false，且 spec.conversion.webhook 已设置。
	Strategy ConversionStrategyType `json:"strategy" protobuf:"bytes,1,name=strategy"`

	// webhook 描述如何调用转换 webhook。`strategy` 为 `"Webhook"` 时必填。
	// +optional
	Webhook *WebhookConversion `json:"webhook,omitempty" protobuf:"bytes,2,opt,name=webhook"`
}

// WebhookConversion 描述如何调用转换 webhook。
type WebhookConversion struct {
	// clientConfig 是 strategy 为 `Webhook` 时调用 webhook 的说明。
	// +optional
	ClientConfig *WebhookClientConfig `json:"clientConfig,omitempty" protobuf:"bytes,2,name=clientConfig"`

	// conversionReviewVersions 是 webhook 期望的 `ConversionReview` 版本的有序列表。
	// API server 会使用其中它支持的第一个版本。若列表中没有任何版本被 API server
	// 支持，该自定义资源的转换就会失败。
	// 若已持久化的 Webhook 配置指定的允许版本中，没有任何版本被 API Server 识别，
	// 调用 webhook 也会失败。
	// +listType=atomic
	ConversionReviewVersions []string `json:"conversionReviewVersions" protobuf:"bytes,3,rep,name=conversionReviewVersions"`
}

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
type WebhookClientConfig struct {
	// url 以标准 URL 形式（`scheme://host:port/path`）给出 webhook 的位置。
	// `url` 和 `service` 必须且只能指定其一。
	//
	// `host` 不应指向集群内运行的服务，那种情况应改用 `service` 字段。
	// 某些 apiserver 可能通过外部 DNS 解析 host（例如 `kube-apiserver` 无法解析
	// 集群内 DNS，因为那会破坏分层）。`host` 也可以是 IP 地址。
	//
	// 注意：除非你非常小心地在所有可能调用该 webhook 的 apiserver 所在主机上
	// 运行它，否则用 `localhost` 或 `127.0.0.1` 作为 `host` 是有风险的。
	// 这类安装通常不可移植，即难以在新集群中复现。
	//
	// scheme 必须是 "https"；URL 必须以 "https://" 开头。
	//
	// path 可选，存在时可以是 URL 中允许的任意字符串。可用它向 webhook 传递
	// 任意字符串，例如集群标识符。
	//
	// 不允许使用用户认证或 basic auth，例如 "user:password@"。
	// 片段（"#..."）和查询参数（"?"）也不允许。
	//
	// +optional
	URL *string `json:"url,omitempty" protobuf:"bytes,3,opt,name=url"`

	// service 是该 webhook 所指向服务的引用。service 与 url 必须指定其一。
	//
	// 若 webhook 运行在集群内，应使用 `service`。
	//
	// +optional
	Service *ServiceReference `json:"service,omitempty" protobuf:"bytes,1,opt,name=service"`

	// caBundle 是 PEM 编码的 CA 证书包，用于验证 webhook 的服务端证书。
	// 未指定时使用 apiserver 的系统信任根。
	// +optional
	CABundle []byte `json:"caBundle,omitempty" protobuf:"bytes,2,opt,name=caBundle"`
}

// ServiceReference 保存对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// namespace 是服务所在的命名空间。
	// 必填
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// name 是服务的名称。
	// 必填
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// path 是联系 webhook 时可选使用的 URL 路径。
	// +optional
	Path *string `json:"path,omitempty" protobuf:"bytes,3,opt,name=path"`

	// port 是联系 webhook 时可选使用的服务端口。
	// `port` 应是合法端口号（1-65535，闭区间）。
	// 为向后兼容默认 443。
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,4,opt,name=port"`
}

// CustomResourceDefinitionVersion 描述 CRD 的一个版本。
type CustomResourceDefinitionVersion struct {
	// name 是版本名，例如 "v1"、"v2beta1" 等。
	// 当 `served` 为 true 时，自定义资源在该版本下通过 `/apis/<group>/<version>/...` 提供服务。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// served 是启用/禁用该版本通过 REST API 提供服务的标志。
	Served bool `json:"served" protobuf:"varint,2,opt,name=served"`
	// storage 表示持久化自定义资源时应使用该版本。
	// 必须有且仅有一个版本的 storage=true。
	Storage bool `json:"storage" protobuf:"varint,3,opt,name=storage"`
	// deprecated 表示该版本的自定义资源 API 已弃用。
	// 设为 true 时，对该版本的 API 请求会在服务器响应中带上 warning 头。
	// 默认 false。
	// +optional
	Deprecated bool `json:"deprecated,omitempty" protobuf:"varint,7,opt,name=deprecated"`
	// deprecationWarning 覆盖返回给 API 客户端的默认警告。
	// 仅当 `deprecated` 为 true 时才可设置。
	// 默认警告会说明该版本已弃用，并建议使用稳定性不低于它的最新 served 版本（若存在）。
	// +optional
	DeprecationWarning *string `json:"deprecationWarning,omitempty" protobuf:"bytes,8,opt,name=deprecationWarning"`
	// schema 描述该版本自定义资源用于校验、prune 和默认化的 schema。
	// +optional
	Schema *CustomResourceValidation `json:"schema,omitempty" protobuf:"bytes,4,opt,name=schema"`
	// subresources 指定该版本自定义资源具备的子资源。
	// +optional
	Subresources *CustomResourceSubresources `json:"subresources,omitempty" protobuf:"bytes,5,opt,name=subresources"`
	// additionalPrinterColumns 指定 Table 输出中额外返回的列。
	// 详见 https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables
	// 若未指定列，则使用单列显示自定义资源的 age。
	// +optional
	// +listType=atomic
	AdditionalPrinterColumns []CustomResourceColumnDefinition `json:"additionalPrinterColumns,omitempty" protobuf:"bytes,6,rep,name=additionalPrinterColumns"`

	// selectableFields 指定可用作 field selector 的字段路径。
	// 最多允许 8 个可选字段。
	// 详见 https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors
	//
	// +featureGate=CustomResourceFieldSelectors
	// +optional
	// +listType=atomic
	SelectableFields []SelectableField `json:"selectableFields,omitempty" protobuf:"bytes,9,rep,name=selectableFields"`
}

// SelectableField 指定可用作 field selector 的字段的 JSON 路径。
type SelectableField struct {
	// jsonPath 是一个简单 JSON 路径，对每个自定义资源求值以产生 field selector 值。
	// 只允许不含数组记法的 JSON 路径。
	// 必须指向 string、boolean 或 integer 类型的字段。带 enum 的类型和带 format 的
	// 字符串都允许。
	// 若 jsonPath 指向资源中不存在的字段，则求值为空串。
	// 不得指向 metadata 字段。
	// 必填。
	JSONPath string `json:"jsonPath" protobuf:"bytes,1,opt,name=jsonPath"`
}

// CustomResourceColumnDefinition 指定服务端打印的一列。
type CustomResourceColumnDefinition struct {
	// name 是该列的人类可读名称。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// type 是该列的 OpenAPI 类型定义。
	// 详见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types
	Type string `json:"type" protobuf:"bytes,2,opt,name=type"`
	// format 是该列可选的 OpenAPI 类型定义。'name' format 用于主标识列，
	// 帮助客户端认出该列是资源名。
	// 详见 https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types
	// +optional
	Format string `json:"format,omitempty" protobuf:"bytes,3,opt,name=format"`
	// description 是该列的人类可读描述。
	// +optional
	Description string `json:"description,omitempty" protobuf:"bytes,4,opt,name=description"`
	// priority 是整数，定义该列相对其他列的重要程度。数字越小优先级越高。
	// 空间有限时可能被省略的列应给出大于 0 的 priority。
	// +optional
	Priority int32 `json:"priority,omitempty" protobuf:"bytes,5,opt,name=priority"`
	// jsonPath 是一个简单 JSON 路径（即允许数组记法），对每个自定义资源求值
	// 以产生该列的值。
	JSONPath string `json:"jsonPath" protobuf:"bytes,6,opt,name=jsonPath"`
}

// CustomResourceDefinitionNames 表示用于 serving 该 CustomResourceDefinition 的名称。
type CustomResourceDefinitionNames struct {
	// plural 是所服务资源的复数名。
	// 自定义资源在 `/apis/<group>/<version>/.../<plural>` 下提供服务。
	// 必须与 CustomResourceDefinition 的名称一致（形式为 `<names.plural>.<group>`）。
	// 必须全小写。
	Plural string `json:"plural" protobuf:"bytes,1,opt,name=plural"`
	// singular 是资源的单数名，必须全小写。默认为小写化的 `kind`。
	// +optional
	Singular string `json:"singular,omitempty" protobuf:"bytes,2,opt,name=singular"`
	// shortNames 是资源的短名，会暴露在 API discovery 文档中，
	// 客户端用它支持 `kubectl get <shortname>` 之类的调用。
	// 必须全小写。
	// +optional
	// +listType=atomic
	ShortNames []string `json:"shortNames,omitempty" protobuf:"bytes,3,opt,name=shortNames"`
	// kind 是资源的序列化 kind，通常是 CamelCase 且为单数。
	// 自定义资源实例在 API 调用中会使用该值作为 `kind` 属性。
	Kind string `json:"kind" protobuf:"bytes,4,opt,name=kind"`
	// listKind 是该资源列表的序列化 kind。默认为 "`kind`List"。
	// +optional
	ListKind string `json:"listKind,omitempty" protobuf:"bytes,5,opt,name=listKind"`
	// categories 是该自定义资源所属分组资源的列表（例如 'all'）。
	// 会发布到 API discovery 文档中，客户端据此支持 `kubectl get all` 之类的调用。
	// +optional
	// +listType=atomic
	Categories []string `json:"categories,omitempty" protobuf:"bytes,6,rep,name=categories"`
}

// ResourceScope 是定义自定义资源可用作用域的枚举。
type ResourceScope string

const (
	// ClusterScoped 表示集群级资源。
	ClusterScoped ResourceScope = "Cluster"
	// NamespaceScoped 表示命名空间级资源。
	NamespaceScoped ResourceScope = "Namespaced"
)

// ConditionStatus 表示 condition 的状态。
type ConditionStatus string

// 以下是合法的 condition 状态。"ConditionTrue" 表示资源处于该 condition；
// "ConditionFalse" 表示资源不处于该 condition；"ConditionUnknown" 表示
// kubernetes 无法判断资源是否处于该 condition。将来可能增加其他中间状态，
// 例如 ConditionDegraded。
const (
	// ConditionTrue 表示资源处于该 condition。
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示资源不处于该 condition。
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示 kubernetes 无法判断资源是否处于该 condition。
	ConditionUnknown ConditionStatus = "Unknown"
)

// CustomResourceDefinitionConditionType 是 CustomResourceDefinitionCondition.Type 的合法值。
type CustomResourceDefinitionConditionType string

const (
	// Established 表示资源已生效。当所有 names 首次被接受且无冲突时资源即 established。
	// 即使之后因 names 变更导致 NamesAccepted 变化，资源也保持 established 直到被删除。
	// 注意并非所有 names 都可变更。
	Established CustomResourceDefinitionConditionType = "Established"
	// NamesAccepted 表示为该 CustomResourceDefinition 选择的 names 与组内其他资源
	// 不冲突，因此被接受。
	NamesAccepted CustomResourceDefinitionConditionType = "NamesAccepted"
	// NonStructuralSchema 表示一个或多个 OpenAPI schema 不是 structural 的。
	//
	// 如果 schema 为所有值都指定了类型，则它是 structural 的，仅以下情况例外：
	// - x-kubernetes-int-or-string: true —— 用于可为 integer 或 string 的字段
	// - x-kubernetes-preserve-unknown-fields: true —— 用于原始、未指定的 JSON 值
	// 并且在 allOf、anyOf、oneOf 或 not 下没有指定 type、additionalProperties、
	// default、nullable 或 x-kubernetes-* 厂商扩展。
	//
	// v1 API 组中将不再允许非 structural schema。此外，非 structural CRD 也
	// 无法使用新特性：
	// - pruning
	// - defaulting
	// - read-only
	// - OpenAPI publishing
	// - webhook conversion
	NonStructuralSchema CustomResourceDefinitionConditionType = "NonStructuralSchema"
	// Terminating 表示该 CustomResourceDefinition 已被删除，正在清理中。
	Terminating CustomResourceDefinitionConditionType = "Terminating"
	// KubernetesAPIApprovalPolicyConformant 表示 *.k8s.io 或 *.kubernetes.io 下的 API
	// 是否已获批准。这些组之外的 CRD 不会设置该 condition。组内的 CRD，
	// 当 .metadata.annotations["api-approved.kubernetes.io"] 被设为 URL 时该 condition
	// 为 true，否则为 false。详见 https://github.com/kubernetes/enhancements/pull/1111
	KubernetesAPIApprovalPolicyConformant CustomResourceDefinitionConditionType = "KubernetesAPIApprovalPolicyConformant"
	// StorageMigrating 表示该 CRD 的底层存储版本正在进行迁移。
	StorageMigrating CustomResourceDefinitionConditionType = "StorageMigrating"
)

// CustomResourceDefinitionCondition 包含该 pod 当前 condition 的详细信息。
type CustomResourceDefinitionCondition struct {
	// type 是 condition 的类型。类型包括 Established、NamesAccepted 和 Terminating。
	Type CustomResourceDefinitionConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=CustomResourceDefinitionConditionType"`
	// status 是 condition 的状态。可为 True、False、Unknown。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// lastTransitionTime 是 condition 上次从一个状态转变到另一个状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// reason 是 condition 上次转变的唯一、单词、CamelCase 的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// message 是人类可读的消息，说明上次转变的细节。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
	// observedGeneration 表示设置该 condition 所依据的 .metadata.generation。
	// 例如，若当前 .metadata.generation 为 12，而 .status.conditions[x].observedGeneration
	// 为 9，则该 condition 相对于实例的当前状态已经过时。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,6,opt,name=observedGeneration"`
}

// CustomResourceDefinitionStatus 表示 CustomResourceDefinition 的状态。
type CustomResourceDefinitionStatus struct {
	// conditions 表示 CustomResourceDefinition 特定方面的状态
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []CustomResourceDefinitionCondition `json:"conditions" protobuf:"bytes,1,opt,name=conditions"`

	// acceptedNames 是实际用于 serving discovery 的 names。
	// 它们可能与 spec 中的 names 不同。
	// +optional
	AcceptedNames CustomResourceDefinitionNames `json:"acceptedNames" protobuf:"bytes,2,opt,name=acceptedNames"`

	// storedVersions 列出曾持久化过的所有 CustomResource 版本。跟踪这些版本可为
	// etcd 中的存储版本提供迁移路径。该字段可变，以便迁移控制器完成到另一版本的迁移
	// （确保存储中不残留旧对象），然后从该列表中移除其余版本。
	// 存在于此列表中的版本不得从 `spec.versions` 中移除。
	// +optional
	// +listType=atomic
	StoredVersions []string `json:"storedVersions" protobuf:"bytes,3,rep,name=storedVersions"`

	// CRD 控制器观测到的 generation。
	// +featureGate=CRDObservedGenerationTracking
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,4,opt,name=observedGeneration"`
}

// CustomResourceCleanupFinalizer 是用于删除 CustomResourceDefinition 实例的
// finalizer 名称。
const CustomResourceCleanupFinalizer = "customresourcecleanup.apiextensions.k8s.io"

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// CustomResourceDefinition 表示应在 API server 上暴露的资源。
// 其名称必须为 <.spec.name>.<.spec.group> 格式。
type CustomResourceDefinition struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 描述用户希望资源呈现的样子
	Spec CustomResourceDefinitionSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// status 表示 CustomResourceDefinition 的实际状态
	// +optional
	Status CustomResourceDefinitionStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// CustomResourceDefinitionList 是 CustomResourceDefinition 对象的列表。
type CustomResourceDefinitionList struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 列出各个 CustomResourceDefinition 对象
	Items []CustomResourceDefinition `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CustomResourceValidation 是 CustomResource 校验方法的集合。
type CustomResourceValidation struct {
	// openAPIV3Schema 是用于校验和 prune 的 OpenAPI v3 schema。
	// +optional
	OpenAPIV3Schema *JSONSchemaProps `json:"openAPIV3Schema,omitempty" protobuf:"bytes,1,opt,name=openAPIV3Schema"`
}

// CustomResourceSubresources 定义 CustomResource 的 status 和 scale 子资源。
type CustomResourceSubresources struct {
	// status 表示自定义资源应提供 `/status` 子资源。
	// 启用后：
	// 1. 对自定义资源主端点的请求会忽略对象 `status` 字段的变化。
	// 2. 对自定义资源 `/status` 子资源的请求会忽略对象 `status` 字段之外的变化。
	// +optional
	Status *CustomResourceSubresourceStatus `json:"status,omitempty" protobuf:"bytes,1,opt,name=status"`
	// scale 表示自定义资源应提供返回 `autoscaling/v1` Scale 对象的 `/scale` 子资源。
	// +optional
	Scale *CustomResourceSubresourceScale `json:"scale,omitempty" protobuf:"bytes,2,opt,name=scale"`
}

// CustomResourceSubresourceStatus 定义如何为 CustomResource 提供 status 子资源。
// status 由 CustomResource 中的 `.status` JSON 路径表示。设置后：
// * 为该自定义资源暴露 /status 子资源
// * 对 /status 子资源的 PUT 请求接收自定义资源对象，并忽略 status 字段之外的变化
// * 对自定义资源的 PUT/POST/PATCH 请求忽略 status 字段的变化
type CustomResourceSubresourceStatus struct{}

// CustomResourceSubresourceScale 定义如何为 CustomResource 提供 scale 子资源。
type CustomResourceSubresourceScale struct {
	// specReplicasPath 定义自定义资源中对应 Scale `spec.replicas` 的 JSON 路径。
	// 只允许不含数组记法的 JSON 路径。
	// 必须是 `.spec` 下的 JSON 路径。
	// 若自定义资源中给定路径下没有值，`/scale` 子资源的 GET 会返回错误。
	SpecReplicasPath string `json:"specReplicasPath" protobuf:"bytes,1,name=specReplicasPath"`
	// statusReplicasPath 定义自定义资源中对应 Scale `status.replicas` 的 JSON 路径。
	// 只允许不含数组记法的 JSON 路径。
	// 必须是 `.status` 下的 JSON 路径。
	// 若自定义资源中给定路径下没有值，`/scale` 子资源的 `status.replicas` 会默认为 0。
	StatusReplicasPath string `json:"statusReplicasPath" protobuf:"bytes,2,opt,name=statusReplicasPath"`
	// labelSelectorPath 定义自定义资源中对应 Scale `status.selector` 的 JSON 路径。
	// 只允许不含数组记法的 JSON 路径。
	// 必须是 `.status` 或 `.spec` 下的 JSON 路径。
	// 要与 HorizontalPodAutoscaler 配合使用则必须设置。
	// 该 JSON 路径指向的字段必须是字符串字段（而非复杂 selector 结构），
	// 其内容为字符串形式的序列化 label selector。
	// 更多信息：https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions#scale-subresource
	// 若自定义资源中给定路径下没有值，`/scale` 子资源的 `status.selector` 会默认为空串。
	// +optional
	LabelSelectorPath *string `json:"labelSelectorPath,omitempty" protobuf:"bytes,3,opt,name=labelSelectorPath"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

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

// ConversionRequest 描述转换请求的参数。
type ConversionRequest struct {
	// uid 是单个请求/响应的标识符。它用于区分其他方面完全相同（例如并行）的请求实例。
	// UID 跟踪的是 Kubernetes API server 与 webhook 之间的一次往返（请求/响应），
	// 而不是用户请求。它适合用于关联 webhook 与 apiserver 之间的日志，供审计或调试。
	UID types.UID `json:"uid" protobuf:"bytes,1,name=uid"`
	// desiredAPIVersion 是给定对象要转换到的版本，例如 "myapi.example.com/v1"
	DesiredAPIVersion string `json:"desiredAPIVersion" protobuf:"bytes,2,name=desiredAPIVersion"`
	// objects 是要转换的自定义资源对象列表。
	// +listType=atomic
	Objects []runtime.RawExtension `json:"objects" protobuf:"bytes,3,rep,name=objects"`
}

// ConversionResponse 描述转换响应。
type ConversionResponse struct {
	// uid 是单个请求/响应的标识符，应从对应的 `request.uid` 复制过来。
	UID types.UID `json:"uid" protobuf:"bytes,1,name=uid"`
	// convertedObjects 是 `request.objects` 在 `result` 成功时的转换后版本列表，否则为空。
	// webhook 应把这些对象的 `apiVersion` 设为 `request.desiredAPIVersion`。该列表的
	// 长度必须与输入列表相同，对象顺序也相同（kind、metadata.uid、metadata.name 和
	// metadata.namespace 相等）。
	// webhook 可以修改 labels 和 annotations。metadata 的其他任何改动都会被静默忽略。
	// +listType=atomic
	ConvertedObjects []runtime.RawExtension `json:"convertedObjects" protobuf:"bytes,2,rep,name=convertedObjects"`
	// result 包含转换结果；转换失败时附带额外细节。`result.status` 决定转换失败还是成功。
	// `result.status` 字段必填，表示转换的成功或失败。成功的转换必须把 `result.status`
	// 设为 `Success`。失败的转换必须把 `result.status` 设为 `Failure`，在
	// `result.message` 中提供更多细节，并返回 http 状态码 200。`result.message`
	// 将用于为终端用户构造错误消息。
	Result metav1.Status `json:"result" protobuf:"bytes,3,name=result"`
}
