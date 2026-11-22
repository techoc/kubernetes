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

package v1beta3

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下是有效的通配符。
const (
	// APIGroupAll 匹配所有 API 组。
	APIGroupAll = "*"
	// ResourceAll 匹配所有资源。
	ResourceAll = "*"
	// VerbAll 匹配所有动词。
	VerbAll = "*"
	// NonResourceAll 匹配所有非资源 URL。
	NonResourceAll = "*"
	// NameAll 匹配所有名称。
	NameAll = "*"

	// NamespaceEvery 匹配每一个具体的命名空间。
	NamespaceEvery = "*" // matches every particular namespace
)

// 系统预设的优先级级别名称。
const (
	// PriorityLevelConfigurationNameExempt 是豁免优先级级别的名称。
	PriorityLevelConfigurationNameExempt = "exempt"
	// PriorityLevelConfigurationNameCatchAll 是兜底优先级级别的名称。
	PriorityLevelConfigurationNameCatchAll = "catch-all"
	// FlowSchemaNameExempt 是豁免 FlowSchema 的名称。
	FlowSchemaNameExempt = "exempt"
	// FlowSchemaNameCatchAll 是兜底 FlowSchema 的名称。
	FlowSchemaNameCatchAll = "catch-all"
)

// 条件类型。
const (
	// FlowSchemaConditionDangling 表示 FlowSchema 引用的优先级级别不存在。
	FlowSchemaConditionDangling = "Dangling"

	// PriorityLevelConfigurationConditionConcurrencyShared 表示并发份额已共享。
	PriorityLevelConfigurationConditionConcurrencyShared = "ConcurrencyShared"
)

// API 校验使用的常量。
const (
	// FlowSchemaMaxMatchingPrecedence 是 FlowSchema 匹配优先级的最大值。
	FlowSchemaMaxMatchingPrecedence int32 = 10000
)

// apiserver 响应头使用的常量。
const (
	// ResponseHeaderMatchedPriorityLevelConfigurationUID 是携带匹配到的优先级级别 UID 的响应头名称。
	ResponseHeaderMatchedPriorityLevelConfigurationUID = "X-Kubernetes-PF-PriorityLevel-UID"
	// ResponseHeaderMatchedFlowSchemaUID 是携带匹配到的 FlowSchema UID 的响应头名称。
	ResponseHeaderMatchedFlowSchemaUID = "X-Kubernetes-PF-FlowSchema-UID"
)

const (
	// AutoUpdateAnnotationKey 是一个注解的名称，设为 'true' 时启用
	// 引导配置对象 spec 的自动更新。
	//
	// 全新安装时，所有引导配置对象都会带有以下注解键并启用自动更新：
	//    apf.kubernetes.io/autoupdate-spec: 'true'
	//
	// kube-apiserver 会定期检查集群中的引导配置对象，并在必要时应用更新。
	//
	// kube-apiserver 对强制配置对象执行“始终自动更新”策略，这意味着：
	// - 若缺少自动更新注解键，则添加该键并设为 'true'。
	// - 若自动更新注解键当前值为布尔 false 或非法的布尔表示，则将其置为 'true'
	//   （集群运维人员将其设为 'false' 也会被覆盖）。
	// - 集群运维人员对 spec 所做的任何修改都会被覆盖，但对名为 "exempt" 的
	//   PriorityLevelConfiguration 的 `nominalConcurrencyShares` 和
	//   `lendablePercent` 字段的修改除外。
	//
	// 对于建议配置，kube-apiserver 在以下情况下应用更新：
	// - 集群运维人员通过设置注解启用了自动更新
	//   （apf.kubernetes.io/autoupdate-spec: 'true'）；或
	// - 注解键缺失但 generation 为 1。
	//
	// 若建议配置对象缺少该注解键，kube-apiserver 将按如下方式更新注解：
	// - 对象 generation 为 '1' 时设为 'true'，通常表示对象的 spec 未被修改过。
	// - 对象 generation 大于 1 时设为 'false'。
	//
	// 目标是让 kube-apiserver 能对先前版本安装的建议配置对象应用更新，
	// 同时不覆盖集群运维人员的修改。注意这种区分并不完美：若运维人员删除
	// 某个建议配置对象后又以变体 spec 重新创建，且此后未更新该对象
	// （generation 为 1），上述方法会错误地判定该对象应被自动更新。
	AutoUpdateAnnotationKey = "apf.kubernetes.io/autoupdate-spec"
)

const (
	// 该注解仅用于 v1beta3。
	//
	// v1beta3 对象中存在此注解时，'NominalConcurrencyShares' 字段的零值
	// 表示真正的零，而不是旧的默认值 30。
	//
	// 若要在 v1beta3 中将 'NominalConcurrencyShares' 字段设为零值，
	// 请将注解设为空字符串：
	//   "flowcontrol.k8s.io/v1beta3-preserve-zero-concurrency-shares": ""
	//
	PriorityLevelPreserveZeroConcurrencySharesKey = "flowcontrol.k8s.io/v1beta3-preserve-zero-concurrency-shares"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1,FlowSchema

// FlowSchema 定义一组流的 schema。一个流由一组具有相似属性的入站 API 请求构成，
// 并由一对字符串标识：FlowSchema 的名称和一个“流区分符”。
// +k8s:supportsSubresource="/status"
type FlowSchema struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `spec` 是 FlowSchema 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec FlowSchemaSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// `status` 是 FlowSchema 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status FlowSchemaStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1,FlowSchemaList

// FlowSchemaList 是 FlowSchema 对象的列表。
type FlowSchemaList struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// `items` 是 FlowSchema 的列表。
	Items []FlowSchema `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// FlowSchemaSpec 描述 FlowSchema 的规格。
type FlowSchemaSpec struct {
	// `priorityLevelConfiguration` 应引用集群中的某个 PriorityLevelConfiguration。
	// 若引用无法解析，该 FlowSchema 将被忽略并在其 status 中标记为无效。
	// 必需。
	// +required
	PriorityLevelConfiguration PriorityLevelConfigurationReference `json:"priorityLevelConfiguration" protobuf:"bytes,1,opt,name=priorityLevelConfiguration"`
	// `matchingPrecedence` 用于在匹配给定请求的多个 FlowSchema 中做选择。
	// 选中的是 MatchingPrecedence 数值最小（逻辑上优先级最高）的 FlowSchema。
	// 每个 MatchingPrecedence 值必须在 [1,10000] 范围内。
	// 注意若未指定优先级，默认为 1000。
	// +optional
	MatchingPrecedence int32 `json:"matchingPrecedence" protobuf:"varint,2,opt,name=matchingPrecedence"`
	// `distinguisherMethod` 定义如何为匹配此 schema 的请求计算流区分符。
	// `nil` 表示禁用区分符，此时区分符始终是空字符串。
	// +optional
	DistinguisherMethod *FlowDistinguisherMethod `json:"distinguisherMethod,omitempty" protobuf:"bytes,3,opt,name=distinguisherMethod"`
	// `rules` 描述哪些请求会匹配此 flow schema。当且仅当 rules 中至少一个成员
	// 匹配请求时，该 FlowSchema 匹配该请求。若为空切片，则没有请求匹配该 FlowSchema。
	// +listType=atomic
	// +optional
	Rules []PolicyRulesWithSubjects `json:"rules,omitempty" protobuf:"bytes,4,rep,name=rules"`
}

// FlowDistinguisherMethodType 是流区分符方法的类型。
type FlowDistinguisherMethodType string

// 以下是有效的流区分符方法。
const (
	// FlowDistinguisherMethodByUserType 指定流区分符为请求中的用户名，
	// 用于在不同用户之间提供一定程度的隔离。
	FlowDistinguisherMethodByUserType FlowDistinguisherMethodType = "ByUser"

	// FlowDistinguisherMethodByNamespaceType 指定流区分符为请求操作对象所在的命名空间。
	// 若对象非命名空间作用域，或请求是非资源请求，则区分符为空字符串。
	// 典型用法是在多租户且每个命名空间专属于一个租户的场景下，
	// 在不同租户之间提供一定程度的隔离。
	FlowDistinguisherMethodByNamespaceType FlowDistinguisherMethodType = "ByNamespace"
)

// FlowDistinguisherMethod 指定流区分符的方法。
type FlowDistinguisherMethod struct {
	// `type` 是流区分符方法的类型，支持 "ByUser" 和 "ByNamespace"。
	// 必需。
	// +required
	Type FlowDistinguisherMethodType `json:"type" protobuf:"bytes,1,opt,name=type"`
}

// PriorityLevelConfigurationReference 包含指向所使用“请求优先级”的信息。
type PriorityLevelConfigurationReference struct {
	// `name` 是被引用的优先级级别配置的名称。
	// 必需。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PolicyRulesWithSubjects 规定了对 apiserver 请求应用的测试。测试考虑发起请求的主体、
// 请求的动词以及操作的目标资源。当且仅当 (a) subjects 中至少一个成员匹配请求，
// 且 (b) resourceRules 或 nonResourceRules 中至少一个成员匹配请求时，
// 该 PolicyRulesWithSubjects 匹配该请求。
type PolicyRulesWithSubjects struct {
	// subjects 是本规则关心的普通用户、serviceaccount 或组列表，至少包含一个成员。
	// 同时包含 system:authenticated 和 system:unauthenticated 用户组的切片匹配所有请求。
	// +listType=atomic
	// 必需。
	// +required
	Subjects []Subject `json:"subjects" protobuf:"bytes,1,rep,name=subjects"`
	// `resourceRules` 是 ResourcePolicyRule 切片，根据动词和目标资源识别匹配的请求。
	// `resourceRules` 和 `nonResourceRules` 至少有一个非空。
	// +listType=atomic
	// +optional
	ResourceRules []ResourcePolicyRule `json:"resourceRules,omitempty" protobuf:"bytes,2,opt,name=resourceRules"`
	// `nonResourceRules` 是 NonResourcePolicyRule 列表，根据动词和目标非资源 URL
	// 识别匹配的请求。
	// +listType=atomic
	// +optional
	NonResourceRules []NonResourcePolicyRule `json:"nonResourceRules,omitempty" protobuf:"bytes,3,opt,name=nonResourceRules"`
}

// Subject 按请求认证系统所识别的信息匹配请求的发起者。有三种匹配发起者的方式：
// 按用户、按组或按 service account。
// +union
type Subject struct {
	// `kind` 指示其他字段中哪一个非空。
	// 必需
	// +required
	// +unionDiscriminator
	Kind SubjectKind `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// `user` 按用户名匹配。
	// +optional
	User *UserSubject `json:"user,omitempty" protobuf:"bytes,2,opt,name=user"`
	// `group` 按用户组名匹配。
	// +optional
	Group *GroupSubject `json:"group,omitempty" protobuf:"bytes,3,opt,name=group"`
	// `serviceAccount` 匹配 ServiceAccount。
	// +optional
	ServiceAccount *ServiceAccountSubject `json:"serviceAccount,omitempty" protobuf:"bytes,4,opt,name=serviceAccount"`
}

// SubjectKind 是主体的种类。
type SubjectKind string

// 支持的主体种类。
const (
	// SubjectKindUser 表示按用户匹配。
	SubjectKindUser SubjectKind = "User"
	// SubjectKindGroup 表示按用户组匹配。
	SubjectKindGroup SubjectKind = "Group"
	// SubjectKindServiceAccount 表示按 service account 匹配。
	SubjectKindServiceAccount SubjectKind = "ServiceAccount"
)

// UserSubject 包含 user 类型主体的详细信息。
type UserSubject struct {
	// `name` 是要匹配的用户名，或 "*" 表示匹配所有用户名。
	// 必需。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// GroupSubject 包含 group 类型主体的详细信息。
type GroupSubject struct {
	// name 是要匹配的用户组，或 "*" 表示匹配所有用户组。
	// 一些 well-known 组名参见
	// https://github.com/kubernetes/apiserver/blob/master/pkg/authentication/user/user.go。
	// 必需。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// ServiceAccountSubject 包含 service-account 类型主体的详细信息。
type ServiceAccountSubject struct {
	// `namespace` 是匹配的 ServiceAccount 对象所在的命名空间。
	// 必需。
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// `name` 是匹配的 ServiceAccount 对象的名称，或 "*" 表示不区分名称。
	// 必需。
	// +required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
}

// ResourcePolicyRule 是一个谓词，根据请求的动词和目标资源匹配某些资源请求。
// 当且仅当 (a) verbs 中至少一个成员匹配请求，(b) apiGroups 中至少一个成员匹配请求，
// (c) resources 中至少一个成员匹配请求，且 (d) 满足以下之一时，
// ResourcePolicyRule 匹配一个资源请求：(d1) 请求未指定命名空间（即 `Namespace==""`）
// 且 clusterScope 为 true；或 (d2) 请求指定了命名空间且 namespaces 中至少一个成员
// 匹配请求的命名空间。
type ResourcePolicyRule struct {
	// `verbs` 是匹配的动词列表，不可为空。"*" 匹配所有动词，
	// 若出现则必须是唯一条目。
	// +listType=set
	// 必需。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// `apiGroups` 是匹配的 API 组列表，不可为空。"*" 匹配所有 API 组，
	// 若出现则必须是唯一条目。
	// +listType=set
	// 必需。
	// +required
	APIGroups []string `json:"apiGroups" protobuf:"bytes,2,rep,name=apiGroups"`

	// `resources` 是匹配的资源列表（即小写复数形式），可按需包含子资源，
	// 例如 ["services", "nodes/status"]。该列表不可为空。"*" 匹配所有资源，
	// 若出现则必须是唯一条目。
	// 必需。
	// +listType=set
	// +required
	Resources []string `json:"resources" protobuf:"bytes,3,rep,name=resources"`

	// `clusterScope` 指示是否匹配未指定命名空间的请求（可能是资源本身非命名空间
	// 作用域，或请求面向所有命名空间）。若省略该字段或为 false，则 `namespaces`
	// 字段必须包含非空列表。
	// +optional
	ClusterScope bool `json:"clusterScope,omitempty" protobuf:"varint,4,opt,name=clusterScope"`

	// `namespaces` 是限制匹配的目标命名空间列表。指定了目标命名空间的请求仅当
	// (a) 该列表包含此目标命名空间，或 (b) 该列表包含 "*" 时才匹配。
	// 注意 "*" 匹配任何指定的命名空间，但不匹配_未指定_命名空间的请求
	// （相关内容见 `clusterScope` 字段）。该列表可以为空，但仅在
	// `clusterScope` 为 true 时允许。
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces" protobuf:"bytes,5,rep,name=namespaces"`
}

// NonResourcePolicyRule 是一个谓词，根据请求的动词和目标非资源 URL 匹配非资源请求。
// 当且仅当 (a) verbs 中至少一个成员匹配请求，且 (b) nonResourceURLs 中至少一个
// 成员匹配请求时，NonResourcePolicyRule 匹配该请求。
type NonResourcePolicyRule struct {
	// `verbs` 是匹配的动词列表，不可为空。"*" 匹配所有动词，
	// 若出现则必须是唯一条目。
	// +listType=set
	// 必需。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`
	// `nonResourceURLs` 是用户应可访问的 URL 前缀集合，不可为空。例如：
	//   - "/healthz" 合法
	//   - "/hea*" 非法
	//   - "/hea" 合法但不匹配任何内容
	//   - "/hea/*" 同样不匹配任何内容
	//   - "/healthz/*" 匹配所有组件健康检查
	// "*" 匹配所有非资源 URL，若出现则必须是唯一条目。
	// +listType=set
	// 必需。
	// +required
	NonResourceURLs []string `json:"nonResourceURLs" protobuf:"bytes,6,rep,name=nonResourceURLs"`
}

// FlowSchemaStatus 表示 FlowSchema 的当前状态。
type FlowSchemaStatus struct {
	// `conditions` 是 FlowSchema 当前状态的列表。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []FlowSchemaCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// FlowSchemaCondition 描述 FlowSchema 的条件。
type FlowSchemaCondition struct {
	// `type` 是条件的类型。
	// 必需。
	// +required
	Type FlowSchemaConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态，应指定并设为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件最近一次转变的原因，使用单个单词的 CamelCase 表示且唯一。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是人类可读的消息，描述最近一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// FlowSchemaConditionType 是 FlowSchemaStatusCondition.Type 的有效值。
type FlowSchemaConditionType string

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1,PriorityLevelConfiguration

// PriorityLevelConfiguration 表示优先级级别的配置。
// +k8s:supportsSubresource="/status"
type PriorityLevelConfiguration struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `spec` 是“请求优先级”期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec PriorityLevelConfigurationSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// `status` 是“请求优先级”的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PriorityLevelConfigurationStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1,PriorityLevelConfigurationList

// PriorityLevelConfigurationList 是 PriorityLevelConfiguration 对象的列表。
type PriorityLevelConfigurationList struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `items` 是请求优先级的列表。
	Items []PriorityLevelConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PriorityLevelConfigurationSpec 指定优先级级别的配置。
// +union
type PriorityLevelConfigurationSpec struct {
	// `type` 指示此优先级级别是否受请求执行限制。值为 `"Exempt"` 表示
	// 该优先级的请求不受限制（因此永不排队），也不会占用其他优先级可用的容量。
	// 值为 `"Limited"` 表示 (a) 该优先级的请求_受_限制，且 (b) 服务端受限容量
	// 的一部分专门分配给此优先级。
	// 必需。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type PriorityLevelEnablement `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `limited` 指定 Limited 优先级级别如何处理请求。
	// 当且仅当 `type` 为 `"Limited"` 时该字段必须非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Limited")=+k8s:required
	Limited *LimitedPriorityLevelConfiguration `json:"limited,omitempty" protobuf:"bytes,2,opt,name=limited"`

	// `exempt` 指定 Exempt 优先级级别如何处理请求。
	// 若 `type` 为 `"Limited"`，该字段必须为空。
	// 若 `type` 为 `"Exempt"`，该字段可以非空。
	// 若为空且 `type` 为 `"Exempt"`，则应用 `ExemptPriorityLevelConfiguration` 的默认值。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Exempt")=+k8s:optional
	Exempt *ExemptPriorityLevelConfiguration `json:"exempt,omitempty" protobuf:"bytes,3,opt,name=exempt"`
}

// PriorityLevelEnablement 指示优先级级别是否启用执行限制。
type PriorityLevelEnablement string

// 支持的优先级级别启用值。
const (
	// PriorityLevelEnablementExempt 表示请求不受限制。
	PriorityLevelEnablementExempt PriorityLevelEnablement = "Exempt"

	// PriorityLevelEnablementLimited 表示请求受限制。
	PriorityLevelEnablementLimited PriorityLevelEnablement = "Limited"
)

// LimitedPriorityLevelConfiguration 指定如何处理受限制的请求。
// 它解决两个问题：
//   - 如何限制此优先级级别的请求？
//   - 对超过限制的请求应如何处置？
type LimitedPriorityLevelConfiguration struct {
	// `nominalConcurrencyShares`（NCS）参与计算此级别的名义并发上限
	// （NominalCL），即此优先级级别可用的执行席位数量。它既用于从此优先级
	// 派发的请求，也用于从其他优先级派发、借用此级别席位的请求。
	// 服务端的并发上限（ServerCL）按各 Limited 优先级级别的 NCS 值比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大，名义并发上限越大，代价是其他所有优先级级别的额度减少。
	// 该字段默认值为 30。
	// +optional
	NominalConcurrencyShares int32 `json:"nominalConcurrencyShares" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`

	// `limitResponse` 指示对当前无法执行的请求如何处理。
	// +required
	LimitResponse LimitResponse `json:"limitResponse,omitempty" protobuf:"bytes,2,opt,name=limitResponse"`

	// `lendablePercent` 规定此级别 NominalCL 中可被其他优先级级别借用的比例。
	// 该字段值必须在 0 到 100 之间（含两端），默认为 0。
	// 其他级别可从此级别借用的席位数（即此级别的 LendableConcurrencyLimit，
	// LendableCL）定义如下。
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,3,opt,name=lendablePercent"`

	// `borrowingLimitPercent` 若存在，则限制此优先级级别可从其他级别借用的席位数。
	// 该限制称为此级别的 BorrowingConcurrencyLimit（BorrowingCL），即此级别
	// 在任意时刻可借用的席位总数上限。该字段保存该上限与本级别名义并发上限的
	// 比值。当该字段非 nil 时，必须是非负整数，上限计算方式如下。
	//
	// BorrowingCL(i) = round( NominalCL(i) * borrowingLimitPercent(i)/100.0 )
	//
	// 该字段值可以超过 100，表示此优先级级别可借用的席位数大于其自身的
	// 名义并发上限（NominalCL）。若该字段为 `nil`，则限制实际上为无穷大。
	// +optional
	BorrowingLimitPercent *int32 `json:"borrowingLimitPercent,omitempty" protobuf:"varint,4,opt,name=borrowingLimitPercent"`
}

// ExemptPriorityLevelConfiguration 描述豁免请求处理中可配置的方面。
// 在强制的 exempt 配置对象中，此处字段的值可由授权用户修改，而 `spec` 的其余部分不行。
type ExemptPriorityLevelConfiguration struct {
	// `nominalConcurrencyShares`（NCS）参与计算此级别的名义并发上限
	// （NominalCL），即名义上为此优先级级别保留的执行席位数量。
	// 这_不_限制从此优先级的派发，但会通过借用机制影响其他优先级级别。
	// 服务端的并发上限（ServerCL）按所有优先级级别的 NCS 值比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大，名义并发上限越大，代价是其他所有优先级级别的额度减少。
	// 该字段默认值为零。
	// +optional
	NominalConcurrencyShares *int32 `json:"nominalConcurrencyShares,omitempty" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`
	// `lendablePercent` 规定此级别 NominalCL 中可被其他优先级级别借用的比例。
	// 该字段值必须在 0 到 100 之间（含两端），默认为 0。
	// 其他级别可从此级别借用的席位数（即此级别的 LendableConcurrencyLimit，
	// LendableCL）定义如下。
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,2,opt,name=lendablePercent"`
	// Exempt 优先级级别的 `BorrowingCL` 隐式等于 `ServerCL`。
	// 换言之，exempt 优先级级别对其借用量没有实际意义上的限制，
	// 此处没有显式表示该值。
}

// LimitResponse 定义如何处理当前无法执行的请求。
// +union
type LimitResponse struct {
	// `type` 为 "Queue" 或 "Reject"。"Queue" 表示到达时无法执行的请求
	// 会在队列中等待，直到可以执行或达到排队上限。"Reject" 表示到达时
	// 无法执行的请求会被拒绝。
	// 必需。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type LimitResponseType `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `queuing` 保存排队配置参数。仅当 `type` 为 `"Queue"` 时该字段可非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Queue")=+k8s:required
	Queuing *QueuingConfiguration `json:"queuing,omitempty" protobuf:"bytes,2,opt,name=queuing"`
}

// LimitResponseType 标识 Limited 优先级级别如何处理当前无法执行的请求。
type LimitResponseType string

// 支持的限制响应。
const (
	// LimitResponseTypeQueue 表示当前无法执行的请求会排队，直到可执行或达到排队上限。
	LimitResponseTypeQueue LimitResponseType = "Queue"

	// LimitResponseTypeReject 表示当前无法执行的请求会被拒绝。
	LimitResponseTypeReject LimitResponseType = "Reject"
)

// QueuingConfiguration 保存排队配置参数。
type QueuingConfiguration struct {
	// `queues` 是此优先级级别的队列数，各 apiserver 独立维护这些队列。
	// 该值必须为正数。设为 1 实际上排除了 shuffle sharding，
	// 从而使关联 flow schema 的区分符方法失去意义。该字段默认值为 64。
	// +optional
	Queues int32 `json:"queues" protobuf:"varint,1,opt,name=queues"`

	// `handSize` 是一个小的正数，配置请求入队的 shuffle sharding。
	// 在此优先级入队时，请求的流标识（字符串对）会被哈希，哈希值用于
	// 打乱队列列表并发出指定大小的一手牌，请求会被放入该手牌中最短的队列之一。
	// `handSize` 不得大于 `queues`，且应显著更小（以免少数重流量占满大部分队列）。
	// 设置该字段的更多指导参见面向用户的文档。该字段默认值为 8。
	// +optional
	HandSize int32 `json:"handSize" protobuf:"varint,2,opt,name=handSize"`

	// `queueLengthLimit` 是此优先级级别单个队列中允许同时等待的最大请求数，
	// 超出的请求会被拒绝。该值必须为正数。若未指定，默认为 50。
	// +optional
	QueueLengthLimit int32 `json:"queueLengthLimit" protobuf:"varint,3,opt,name=queueLengthLimit"`
}

// PriorityLevelConfigurationConditionType 是 PriorityLevelConfigurationStatusCondition.Type 的有效值。
type PriorityLevelConfigurationConditionType string

// PriorityLevelConfigurationStatus 表示“请求优先级”的当前状态。
type PriorityLevelConfigurationStatus struct {
	// `conditions` 是“请求优先级”的当前状态。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []PriorityLevelConfigurationCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// PriorityLevelConfigurationCondition 定义优先级级别的条件。
type PriorityLevelConfigurationCondition struct {
	// `type` 是条件的类型。
	// 必需。
	// +required
	Type PriorityLevelConfigurationConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态，应指定并设为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件最近一次转变的原因，使用单个单词的 CamelCase 表示且唯一。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是人类可读的消息，描述最近一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// ConditionStatus 是条件的状态。
type ConditionStatus string

// 以下是有效的条件状态。"ConditionTrue" 表示资源处于该条件，
// "ConditionFalse" 表示资源不处于该条件，"ConditionUnknown" 表示 kubernetes
// 无法确定资源是否处于该条件。未来可能会添加其他中间状态，如 ConditionDegraded。
const (
	// ConditionTrue 表示资源处于该条件。
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示资源不处于该条件。
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示无法确定资源是否处于该条件。
	ConditionUnknown ConditionStatus = "Unknown"
)
