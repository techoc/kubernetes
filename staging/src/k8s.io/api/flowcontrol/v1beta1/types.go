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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下是合法的通配符。
const (
	APIGroupAll    = "*"
	ResourceAll    = "*"
	VerbAll        = "*"
	NonResourceAll = "*"
	NameAll        = "*"

	NamespaceEvery = "*" // 匹配每一个具体的命名空间
)

// 系统预设的优先级级别名称
const (
	PriorityLevelConfigurationNameExempt   = "exempt"
	PriorityLevelConfigurationNameCatchAll = "catch-all"
	FlowSchemaNameExempt                   = "exempt"
	FlowSchemaNameCatchAll                 = "catch-all"
)

// 条件
const (
	FlowSchemaConditionDangling = "Dangling"

	PriorityLevelConfigurationConditionConcurrencyShared = "ConcurrencyShared"
)

// API 校验使用的常量。
const (
	FlowSchemaMaxMatchingPrecedence int32 = 10000
)

// apiserver 响应头使用的常量。
const (
	ResponseHeaderMatchedPriorityLevelConfigurationUID = "X-Kubernetes-PF-PriorityLevel-UID"
	ResponseHeaderMatchedFlowSchemaUID                 = "X-Kubernetes-PF-FlowSchema-UID"
)

const (
	// AutoUpdateAnnotationKey 是一个注解的名称，将其设为 'true' 时会启用
	// 引导配置对象 spec 的自动更新。
	//
	// 全新安装时，所有引导配置对象都会带上以下注解键并启用自动更新：
	//    apf.kubernetes.io/autoupdate-spec: 'true'
	//
	// kube-apiserver 会定期检查集群中的引导配置对象，并在必要时应用更新。
	//
	// kube-apiserver 对强制配置对象执行"始终自动更新"策略。这意味着：
	// - 如果缺少自动更新注解键，则添加该键并将其值设为 'true'
	// - 如果自动更新注解键的当前值为布尔 false 或非法的布尔表示，
	//   则将其设为 'true'（若集群运维人员将其设为 'false'，会被覆盖）
	// - 集群运维人员对 spec 所做的任何修改都会被覆盖，但名为 "exempt"
	//   的 PriorityLevelConfiguration 的 `nominalConcurrencyShares`
	//   和 `lendablePercent` 字段的修改除外。
	//
	// 对于建议配置，kube-apiserver 在以下情况应用更新：
	// - 集群运维人员通过设置注解启用了自动更新
	//   （apf.kubernetes.io/autoupdate-spec: 'true'），或
	// - 注解键缺失但 generation 为 1
	//
	// 如果建议配置对象缺少该注解键，kube-apiserver 会相应地更新注解：
	// - 如果对象的 generation 为 '1'，则将其设为 'true'；这通常
	//   表示对象的 spec 未被修改过。
	// - 如果对象的 generation 大于 1，则将其设为 'false'。
	//
	// 目标是让 kube-apiserver 能对先前版本安装的建议配置对象应用更新，
	// 但不覆盖集群运维人员所做的修改。
	// 注意，这种区分的检测并不完美：如果运维人员删除了某个建议配置对象，
	// 之后又用不同的 spec 重新创建它，并且此后没有再更新该对象
	// （generation 为 1），上述方法会错误地判定该对象应当自动更新。
	AutoUpdateAnnotationKey = "apf.kubernetes.io/autoupdate-spec"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.20
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,FlowSchema

// FlowSchema 定义了一组流的模式。流由一组具有相似属性的入站 API 请求构成，
// 并通过一对字符串标识：FlowSchema 的名称和一个"流区分符"。
// +k8s:supportsSubresource="/status"
type FlowSchema struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `spec` 描述 FlowSchema 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec FlowSchemaSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// `status` 是 FlowSchema 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status FlowSchemaStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.20
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,FlowSchemaList

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
	// `priorityLevelConfiguration` 应引用集群中的一个 PriorityLevelConfiguration。如果引用
	// 无法解析，则该 FlowSchema 会被忽略，并在其 status 中标记为无效。
	// 必填。
	// +required
	PriorityLevelConfiguration PriorityLevelConfigurationReference `json:"priorityLevelConfiguration" protobuf:"bytes,1,opt,name=priorityLevelConfiguration"`
	// `matchingPrecedence` 用于在匹配给定请求的 FlowSchema 中进行选择。被选中的
	// FlowSchema 是 MatchingPrecedence 数值最小（我们视其为逻辑上最高）的那个。
	// 每个 MatchingPrecedence 值必须在 [1,10000] 范围内。
	// 注意，如果未指定优先级，默认设为 1000。
	// +optional
	MatchingPrecedence int32 `json:"matchingPrecedence" protobuf:"varint,2,opt,name=matchingPrecedence"`
	// `distinguisherMethod` 定义如何为匹配该模式的请求计算流区分符。
	// `nil` 表示禁用区分符，此时区分符始终为空字符串。
	// +optional
	DistinguisherMethod *FlowDistinguisherMethod `json:"distinguisherMethod,omitempty" protobuf:"bytes,3,opt,name=distinguisherMethod"`
	// `rules` 描述哪些请求会匹配该流模式。当且仅当 rules 中至少有一个成员匹配该请求时，
	// 该 FlowSchema 才匹配该请求。
	// 如果切片为空，则没有任何请求会匹配该 FlowSchema。
	// +listType=atomic
	// +optional
	Rules []PolicyRulesWithSubjects `json:"rules,omitempty" protobuf:"bytes,4,rep,name=rules"`
}

// FlowDistinguisherMethodType 是流区分符方法的类型。
type FlowDistinguisherMethodType string

// 以下是合法的流区分符方法。
const (
	// FlowDistinguisherMethodByUserType 指定流区分符为请求中的用户名。
	// 该类型用于在不同用户之间提供一定程度的隔离。
	FlowDistinguisherMethodByUserType FlowDistinguisherMethodType = "ByUser"

	// FlowDistinguisherMethodByNamespaceType 指定流区分符为请求所操作对象的命名空间。
	// 如果对象不是命名空间作用域的，或者请求是非资源请求，则区分符为空字符串。
	// 该类型的一个示例用途是：在存在多个租户且每个命名空间专属于一个租户的场景下，
	// 在租户之间提供一定程度的隔离。
	FlowDistinguisherMethodByNamespaceType FlowDistinguisherMethodType = "ByNamespace"
)

// FlowDistinguisherMethod 指定流区分符的方法。
type FlowDistinguisherMethod struct {
	// `type` 是流区分符方法的类型。
	// 支持的类型为 "ByUser" 和 "ByNamespace"。
	// 必填。
	// +required
	Type FlowDistinguisherMethodType `json:"type" protobuf:"bytes,1,opt,name=type"`
}

// PriorityLevelConfigurationReference 包含指向所使用 "request-priority" 的信息。
type PriorityLevelConfigurationReference struct {
	// `name` 是被引用的优先级级别配置的名称。
	// 必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PolicyRulesWithSubjects 规定了对发往 apiserver 的请求进行测试的规则。测试会考虑
// 发起请求的主体、所请求的动词以及要操作的资源。当且仅当 (a) subjects 中至少一个
// 成员匹配该请求，且 (b) resourceRules 或 nonResourceRules 中至少一个成员匹配该
// 请求时，PolicyRulesWithSubjects 才匹配该请求。
type PolicyRulesWithSubjects struct {
	// subjects 是该规则所关心的普通用户、ServiceAccount 或组的列表。
	// 该切片中必须至少有一个成员。
	// 同时包含 system:authenticated 和 system:unauthenticated 用户组的切片会匹配所有请求。
	// +listType=atomic
	// 必填。
	// +required
	Subjects []Subject `json:"subjects" protobuf:"bytes,1,rep,name=subjects"`
	// `resourceRules` 是 ResourcePolicyRule 的切片，根据请求的动词和
	// 目标资源识别匹配的请求。
	// `resourceRules` 和 `nonResourceRules` 中至少有一个非空。
	// +listType=atomic
	// +optional
	ResourceRules []ResourcePolicyRule `json:"resourceRules,omitempty" protobuf:"bytes,2,opt,name=resourceRules"`
	// `nonResourceRules` 是 NonResourcePolicyRule 的列表，根据请求的动词和
	// 目标非资源 URL 识别匹配的请求。
	// +listType=atomic
	// +optional
	NonResourceRules []NonResourcePolicyRule `json:"nonResourceRules,omitempty" protobuf:"bytes,3,opt,name=nonResourceRules"`
}

// Subject 按请求认证系统识别出的结果匹配请求的发起者。匹配发起者有三种方式：
// 按用户、按组或按服务账号。
// +union
type Subject struct {
	// `kind` 表示其他字段中哪一个是非空的。
	// 必填
	// +required
	// +unionDiscriminator
	Kind SubjectKind `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// `user` 基于用户名匹配。
	// +optional
	User *UserSubject `json:"user,omitempty" protobuf:"bytes,2,opt,name=user"`
	// `group` 基于用户组名匹配。
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
	SubjectKindUser           SubjectKind = "User"
	SubjectKindGroup          SubjectKind = "Group"
	SubjectKindServiceAccount SubjectKind = "ServiceAccount"
)

// UserSubject 保存用户类型主体的详细信息。
type UserSubject struct {
	// `name` 是匹配的用户名，或用 "*" 匹配所有用户名。
	// 必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// GroupSubject 保存组类型主体的详细信息。
type GroupSubject struct {
	// name 是匹配的用户组，或用 "*" 匹配所有用户组。
	// 一些知名的组名参见 https://github.com/kubernetes/apiserver/blob/master/pkg/authentication/user/user.go 。
	// 必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// ServiceAccountSubject 保存服务账号类型主体的详细信息。
type ServiceAccountSubject struct {
	// `namespace` 是所匹配 ServiceAccount 对象的命名空间。
	// 必填。
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// `name` 是所匹配 ServiceAccount 对象的名称，或用 "*" 表示不限制名称。
	// 必填。
	// +required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
}

// ResourcePolicyRule 是一个谓词，用于匹配部分资源请求，检查请求的动词和
// 目标资源。当且仅当以下条件全部满足时，ResourcePolicyRule 才匹配一个资源请求：
// (a) verbs 中至少一个成员匹配该请求，(b) apiGroups 中至少一个成员匹配该请求，
// (c) resources 中至少一个成员匹配该请求，且 (d) 满足 (d1) 请求未指定命名空间
// （即 `Namespace==""`）且 clusterScope 为 true，或 (d2) 请求指定了命名空间且
// namespaces 中至少一个成员匹配该请求的命名空间。
type ResourcePolicyRule struct {
	// `verbs` 是可匹配的动词列表，不可为空。
	// "*" 匹配所有动词；如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// `apiGroups` 是可匹配的 API 组列表，不可为空。
	// "*" 匹配所有 API 组；如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	// +required
	APIGroups []string `json:"apiGroups" protobuf:"bytes,2,rep,name=apiGroups"`

	// `resources` 是可匹配的资源列表（即小写、复数形式），
	// 如需要可包含子资源。例如 [ "services", "nodes/status" ]。该列表不可为空。
	// "*" 匹配所有资源；如果出现，则必须是唯一一项。
	// 必填。
	// +listType=set
	// +required
	Resources []string `json:"resources" protobuf:"bytes,3,rep,name=resources"`

	// `clusterScope` 表示是否匹配未指定命名空间的请求
	// （可能是资源本身不是命名空间作用域的，或者请求面向所有命名空间）。
	// 如果省略该字段或设为 false，则 `namespaces` 字段必须包含非空列表。
	// +optional
	ClusterScope bool `json:"clusterScope,omitempty" protobuf:"varint,4,opt,name=clusterScope"`

	// `namespaces` 是目标命名空间的列表，用于限制匹配范围。指定了目标命名空间的请求
	// 仅在以下情况下才匹配：(a) 该列表包含该目标命名空间，或
	// (b) 该列表包含 "*"。注意 "*" 可匹配任何指定的命名空间，但不匹配_未指定_
	// 命名空间的请求（关于这一点参见 `clusterScope` 字段）。
	// 该列表可以为空，但前提是 `clusterScope` 为 true。
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces" protobuf:"bytes,5,rep,name=namespaces"`
}

// NonResourcePolicyRule 是一个谓词，根据请求的动词和目标非资源 URL 匹配非资源请求。
// 当且仅当 (a) verbs 中至少一个成员匹配该请求，且 (b) nonResourceURLs 中至少一个
// 成员匹配该请求时，NonResourcePolicyRule 才匹配该请求。
type NonResourcePolicyRule struct {
	// `verbs` 是可匹配的动词列表，不可为空。
	// "*" 匹配所有动词。如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`
	// `nonResourceURLs` 是用户应可访问的 URL 前缀集合，不可为空。
	// 例如：
	//   - "/healthz" 合法
	//   - "/hea*" 非法
	//   - "/hea" 合法但不匹配任何内容
	//   - "/hea/*" 也不匹配任何内容
	//   - "/healthz/*" 匹配所有组件健康检查。
	// "*" 匹配所有非资源 URL；如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	// +required
	NonResourceURLs []string `json:"nonResourceURLs" protobuf:"bytes,6,rep,name=nonResourceURLs"`
}

// FlowSchemaStatus 表示 FlowSchema 的当前状态。
type FlowSchemaStatus struct {
	// `conditions` 是 FlowSchema 当前状态的列表。
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []FlowSchemaCondition `json:"conditions,omitempty" protobuf:"bytes,1,rep,name=conditions"`
}

// FlowSchemaCondition 描述 FlowSchema 的条件。
type FlowSchemaCondition struct {
	// `type` 是条件的类型。
	// 必填。
	// +required
	Type FlowSchemaConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态。
	// 应指定该字段并设为 True, False, Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件最后一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件最后一次转变的原因，要求唯一、单个单词、CamelCase 格式。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是可读的消息，说明最后一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// FlowSchemaConditionType 是 FlowSchemaStatusCondition.Type 的合法值。
type FlowSchemaConditionType string

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.20
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,PriorityLevelConfiguration

// PriorityLevelConfiguration 表示一个优先级级别的配置。
// +k8s:supportsSubresource="/status"
type PriorityLevelConfiguration struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `spec` 描述 "request-priority" 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec PriorityLevelConfigurationSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// `status` 是 "request-priority" 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PriorityLevelConfigurationStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.20
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,PriorityLevelConfigurationList

// PriorityLevelConfigurationList 是 PriorityLevelConfiguration 对象的列表。
type PriorityLevelConfigurationList struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `items` 是请求优先级的列表。
	Items []PriorityLevelConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PriorityLevelConfigurationSpec 指定优先级级别的配置。
// +union
type PriorityLevelConfigurationSpec struct {
	// `type` 表示该优先级级别是否受请求执行限制。值为 `"Exempt"`
	// 表示该优先级级别的请求不受限制（因此永不排队），也不会占用
	// 其他优先级级别可用的容量。值为 `"Limited"` 表示 (a) 该优先级
	// 级别的请求_受_限制，且 (b) 服务器有限容量中的一部分专门留给
	// 该优先级级别使用。
	// 必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type PriorityLevelEnablement `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `limited` 指定 Limited 优先级级别下请求的处理方式。
	// 当且仅当 `type` 为 `"Limited"` 时该字段必须非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Limited")=+k8s:required
	Limited *LimitedPriorityLevelConfiguration `json:"limited,omitempty" protobuf:"bytes,2,opt,name=limited"`

	// `exempt` 指定 Exempt 优先级级别下请求的处理方式。
	// 如果 `type` 为 `"Limited"`，该字段必须为空。
	// 如果 `type` 为 `"Exempt"`，该字段可以非空。
	// 如果该字段为空且 `type` 为 `"Exempt"`，则采用 `ExemptPriorityLevelConfiguration` 的默认值。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Exempt")=+k8s:optional
	Exempt *ExemptPriorityLevelConfiguration `json:"exempt,omitempty" protobuf:"bytes,3,opt,name=exempt"`
}

// PriorityLevelEnablement 表示该优先级级别是否启用执行限制。
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
//   - 如何限制该优先级级别的请求？
//   - 对超出限制的请求应如何处理？
type LimitedPriorityLevelConfiguration struct {
	// `assuredConcurrencyShares`（ACS）用于配置执行限制，即在同一时刻
	// 允许执行的该优先级级别请求数量的上限。ACS 必须是正数。
	// 服务器的并发限制（SCL）按各并发受控优先级级别的保证并发份额比例
	// 进行分配。这会为每个此类优先级级别产生保证并发值（ACV）——即
	// 同一时刻可执行的请求数：
	//
	//             ACV(l) = ceil( SCL * ACS(l) / ( sum[priority levels k] ACS(k) ) )
	//
	// ACS 越大，预留的并发请求越多（代价是其他所有优先级级别）。
	// 该字段的默认值为 30。
	// +optional
	AssuredConcurrencyShares int32 `json:"assuredConcurrencyShares" protobuf:"varint,1,opt,name=assuredConcurrencyShares"`

	// `limitResponse` 指明当前无法立即执行的请求应如何处理。
	// +required
	LimitResponse LimitResponse `json:"limitResponse,omitempty" protobuf:"bytes,2,opt,name=limitResponse"`

	// `lendablePercent` 规定该级别的 NominalCL 中可被其他优先级级别借用的比例。
	// 该字段的值必须在 0 到 100 之间（含两端），默认值为 0。
	// 其他级别可从该级别借用的席位数称为该级别的 LendableConcurrencyLimit
	// （LendableCL），定义如下。
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,3,opt,name=lendablePercent"`

	// `borrowingLimitPercent` 如果存在，则限制该优先级级别可从其他优先级级别借用的席位数。
	// 该限制称为该级别的 BorrowingConcurrencyLimit（BorrowingCL），
	// 即该级别在任意时刻可借用的席位总数上限。
	// 该字段保存此限制与该级别名义并发限制之比。当该字段非 nil 时，
	// 必须是非负整数，限制计算如下。
	//
	// BorrowingCL(i) = round( NominalCL(i) * borrowingLimitPercent(i)/100.0 )
	//
	// 该字段的值可以大于 100，表示该优先级级别可借用的席位数
	// 大于其自身的名义并发限制（NominalCL）。
	// 当该字段为 `nil` 时，限制实际上为无限大。
	// +optional
	BorrowingLimitPercent *int32 `json:"borrowingLimitPercent,omitempty" protobuf:"varint,4,opt,name=borrowingLimitPercent"`
}

// ExemptPriorityLevelConfiguration 描述豁免请求处理中可配置的部分。
// 在强制的 exempt 配置对象中，此处字段的值可由授权用户修改，
// 而 `spec` 的其余部分不可修改。
type ExemptPriorityLevelConfiguration struct {
	// `nominalConcurrencyShares`（NCS）参与计算该级别的
	// NominalConcurrencyLimit（NominalCL）。
	// 这是名义上为该优先级级别预留的执行席位数量。
	// 它_不会_限制该优先级级别的分派，但会通过借用机制影响其他优先级级别。
	// 服务器的并发限制（ServerCL）按各优先级级别的 NCS 值比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大，名义并发限制越大，代价是其他所有优先级级别。
	// 该字段的默认值为零。
	// +optional
	NominalConcurrencyShares *int32 `json:"nominalConcurrencyShares,omitempty" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`
	// `lendablePercent` 规定该级别的 NominalCL 中可被其他优先级级别借用的比例。
	// 该字段的值必须在 0 到 100 之间（含两端），默认值为 0。
	// 其他级别可从该级别借用的席位数称为该级别的 LendableConcurrencyLimit
	// （LendableCL），定义如下。
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,2,opt,name=lendablePercent"`
	// Exempt 优先级级别的 `BorrowingCL` 隐式等于 `ServerCL`。
	// 换言之，豁免优先级级别对其借用量没有实际限制。
	// 此处没有对应的显式表示。
}

// LimitResponse 定义如何处理当前无法立即执行的请求。
// +union
type LimitResponse struct {
	// `type` 取值为 "Queue" 或 "Reject"。
	// "Queue" 表示到达时无法立即执行的请求会在队列中等待，
	// 直到可以执行或达到排队上限。
	// "Reject" 表示到达时无法立即执行的请求会被拒绝。
	// 必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type LimitResponseType `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `queuing` 保存排队的配置参数。
	// 仅当 `type` 为 `"Queue"` 时该字段才可以非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Queue")=+k8s:required
	Queuing *QueuingConfiguration `json:"queuing,omitempty" protobuf:"bytes,2,opt,name=queuing"`
}

// LimitResponseType 标识 Limited 优先级级别如何处理当前无法立即执行的请求。
type LimitResponseType string

// 支持的限制响应。
const (
	// LimitResponseTypeQueue 表示当前无法执行的请求会排队，直到可以执行或达到排队上限。
	LimitResponseTypeQueue LimitResponseType = "Queue"

	// LimitResponseTypeReject 表示当前无法执行的请求会被拒绝。
	LimitResponseTypeReject LimitResponseType = "Reject"
)

// QueuingConfiguration 保存排队的配置参数。
type QueuingConfiguration struct {
	// `queues` 是该优先级级别的队列数量。各 apiserver 上的队列相互独立。
	// 该值必须为正数。设为 1 实际上排除了 shuffle sharding，
	// 从而使关联流模式的区分符方法失去意义。该字段默认值为 64。
	// +optional
	Queues int32 `json:"queues" protobuf:"varint,1,opt,name=queues"`

	// `handSize` 是一个小的正数，用于配置请求入队的 shuffle sharding。
	// 在该优先级级别入队时，请求的流标识符（一对字符串）会被哈希，
	// 哈希值用于打乱队列列表并发出指定大小的一组队列。
	// 请求会被放入该组中最短的队列之一。
	// `handSize` 不得大于 `queues`，且应显著更小（以免少数重流
	// 占满大部分队列）。有关设置该字段的更详细指南，请参阅面向用户的文档。
	// 该字段默认值为 8。
	// +optional
	HandSize int32 `json:"handSize" protobuf:"varint,2,opt,name=handSize"`

	// `queueLengthLimit` 是该优先级级别单个队列中同时允许等待的请求数上限；
	// 超出的请求会被拒绝。该值必须为正数。如未指定，默认值为 50。
	// +optional
	QueueLengthLimit int32 `json:"queueLengthLimit" protobuf:"varint,3,opt,name=queueLengthLimit"`
}

// PriorityLevelConfigurationConditionType 是 PriorityLevelConfigurationStatusCondition.Type 的合法值。
type PriorityLevelConfigurationConditionType string

// PriorityLevelConfigurationStatus 表示 "request-priority" 的当前状态。
type PriorityLevelConfigurationStatus struct {
	// `conditions` 是 "request-priority" 的当前状态。
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []PriorityLevelConfigurationCondition `json:"conditions,omitempty" protobuf:"bytes,1,rep,name=conditions"`
}

// PriorityLevelConfigurationCondition 定义优先级级别的条件。
type PriorityLevelConfigurationCondition struct {
	// `type` 是条件的类型。
	// 必填。
	// +required
	Type PriorityLevelConfigurationConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态。
	// 应指定该字段并设为 True, False, Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件最后一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件最后一次转变的原因，要求唯一、单个单词、CamelCase 格式。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是可读的消息，说明最后一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// ConditionStatus 是条件的状态。
type ConditionStatus string

// 以下是合法的条件状态。"ConditionTrue" 表示资源处于该条件。
// "ConditionFalse" 表示资源不处于该条件。"ConditionUnknown" 表示 kubernetes
// 无法判断资源是否处于该条件。将来可能会增加其他中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)
