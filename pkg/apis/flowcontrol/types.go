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

package flowcontrol

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下是合法的通配符值。统一用 "*" 表示“匹配全部”，避免为每个维度分别定义匹配语义。
const (
	APIGroupAll    = "*"
	ResourceAll    = "*"
	VerbAll        = "*"
	NonResourceAll = "*"
	NameAll        = "*"

	NamespaceEvery = "*" // 匹配所有具体的命名空间；与集群作用域请求相区分，仅用于资源请求匹配
)

// 系统预设的优先级级别名称。这些内置对象由系统强制创建且不可删除，
// 因此用户自定义对象不得使用这些保留名称。
const (
	PriorityLevelConfigurationNameExempt   = "exempt"    // 豁免级别：不受执行限制，永不排队
	PriorityLevelConfigurationNameCatchAll = "catch-all" // 兜底级别：匹配其余所有请求
	FlowSchemaNameExempt                   = "exempt"    // 与豁免级别配套的内置 FlowSchema
	FlowSchemaNameCatchAll                 = "catch-all" // 与兜底级别配套的内置 FlowSchema
)

// 条件类型常量。用于对外暴露 FlowSchema / 优先级级别的非正常状态，便于用户诊断配置问题。
const (
	FlowSchemaConditionDangling = "Dangling" // 引用的优先级级别配置不存在

	PriorityLevelConfigurationConditionConcurrencyShared = "ConcurrencyShared" // 并发额度已被借用
)

// 供 API 校验使用的常量：约束 MatchingPrecedence 的上限，保证各 FlowSchema 的优先级可比。
const (
	FlowSchemaMaxMatchingPrecedence int32 = 10000
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FlowSchema 定义一组流的模式（schema）。一个流由一组属性相似的入站 API 请求构成，
// 并通过一对字符串标识：FlowSchema 的名称和一个“流区分符”（flow distinguisher）。
type FlowSchema struct {
	metav1.TypeMeta
	// `metadata` 是对象的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta
	// `spec` 描述 FlowSchema 的期望行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec FlowSchemaSpec
	// `status` 表示 FlowSchema 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status FlowSchemaStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FlowSchemaList 是 FlowSchema 对象的列表。
type FlowSchemaList struct {
	metav1.TypeMeta
	// `metadata` 是列表的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// `items` 是 FlowSchema 对象的列表。
	Items []FlowSchema
}

// FlowSchemaSpec 描述 FlowSchema 的期望规格。
type FlowSchemaSpec struct {
	// `priorityLevelConfiguration` 应引用集群中的一个 PriorityLevelConfiguration。如果无法解析该引用，
	// 该 FlowSchema 将被忽略，并在其 status 中标记为无效。
	// 必填。
	PriorityLevelConfiguration PriorityLevelConfigurationReference
	// `matchingPrecedence` 用于在多个匹配同一请求的 FlowSchema 之间做出选择。被选中的
	// FlowSchema 是其中 MatchingPrecedence 数值最小（我们视其为逻辑上优先级最高）的那一个。
	// 每个 MatchingPrecedence 值必须在 [1,10000] 范围内。
	// 注意：如果未指定该优先级，则默认设置为 1000。
	// +optional
	MatchingPrecedence int32
	// `distinguisherMethod` 定义如何为匹配该模式的请求计算流区分符。
	// `nil` 表示禁用区分符，此时区分符始终为空字符串。
	// +optional
	DistinguisherMethod *FlowDistinguisherMethod
	// `rules` 描述哪些请求会匹配该流模式。当且仅当 rules 中至少有一项匹配该请求时，
	// 该 FlowSchema 才匹配该请求。
	// 如果该切片为空，则不会有任何请求匹配该 FlowSchema。
	// +listType=set
	// +optional
	Rules []PolicyRulesWithSubjects
}

// FlowDistinguisherMethodType 是流区分符方法的类型。
type FlowDistinguisherMethodType string

// 以下是合法的流区分符方法。
const (
	// FlowDistinguisherMethodByUserType 表示流区分符为请求中的用户名。
	// 该类型用于在不同用户之间提供一定程度的隔离。
	FlowDistinguisherMethodByUserType FlowDistinguisherMethodType = "ByUser"

	// FlowDistinguisherMethodByNamespaceType 表示流区分符为请求所操作对象的命名空间。
	// 如果对象不是命名空间作用域的，或者请求是非资源请求，则区分符为空字符串。
	// 该类型的一个典型用途是在多租户场景中（每个命名空间专属于一个租户）提供租户间的隔离。
	FlowDistinguisherMethodByNamespaceType FlowDistinguisherMethodType = "ByNamespace"
)

// FlowDistinguisherMethod 指定流区分符的方法。
type FlowDistinguisherMethod struct {
	// `type` 是流区分符方法的类型。
	// 支持的类型为 "ByUser" 和 "ByNamespace"。
	// 必填。
	Type FlowDistinguisherMethodType
}

// PriorityLevelConfigurationReference 包含指向所使用的“请求优先级”的信息。
type PriorityLevelConfigurationReference struct {
	// `name` 是被引用的优先级级别配置的名称。
	// 必填。
	Name string
}

// PolicyRulesWithSubjects 规定了对发往 apiserver 的请求所做的测试。该测试会考虑发起请求的主体、
// 请求的动词以及被操作的资源。当且仅当 (a) subjects 中至少有一项匹配该请求，且 (b) resourceRules 或
// nonResourceRules 中至少有一项匹配该请求时，该 PolicyRulesWithSubjects 才匹配该请求。
type PolicyRulesWithSubjects struct {
	// subjects 是该规则所关注的普通用户、ServiceAccount 或用户组的列表。
	// 该切片中必须至少有一个成员。
	// 同时包含 system:authenticated 和 system:unauthenticated 用户组的切片可匹配所有请求。
	// +listType=set
	// 必填。
	Subjects []Subject
	// `resourceRules` 是 ResourcePolicyRule 的切片，根据请求的动词和目标资源来识别匹配的请求。
	// `resourceRules` 和 `nonResourceRules` 中至少有一个必须非空。
	// +listType=set
	// +optional
	ResourceRules []ResourcePolicyRule
	// `nonResourceRules` 是 NonResourcePolicyRule 的列表，根据请求的动词和目标非资源 URL 来识别匹配的请求。
	// +listType=set
	// +optional
	NonResourceRules []NonResourcePolicyRule
}

// Subject 用于匹配请求的发起者（由请求认证系统识别）。共有三种匹配发起者的方式：
// 按用户、按用户组或按 ServiceAccount。
// +union
type Subject struct {
	// `kind` 指示其他字段中哪一个非空。
	// 必填。
	// +unionDiscriminator
	Kind SubjectKind
	// `user` 根据用户名进行匹配。
	// +optional
	User *UserSubject
	// `group` 根据用户组名称进行匹配。
	// +optional
	Group *GroupSubject
	// `serviceAccount` 匹配 ServiceAccount。
	// +optional
	ServiceAccount *ServiceAccountSubject
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
	// `name` 是要匹配的用户名，使用 "*" 可匹配所有用户名。
	// 必填。
	Name string
}

// GroupSubject 保存用户组类型主体的详细信息。
type GroupSubject struct {
	// name 是要匹配的用户组，使用 "*" 可匹配所有用户组。
	// 一些众所周知的用户组名称可参见
	// https://github.com/kubernetes/apiserver/blob/master/pkg/authentication/user/user.go
	// 必填。
	Name string
}

// ServiceAccountSubject 保存 ServiceAccount 类型主体的详细信息。
type ServiceAccountSubject struct {
	// `namespace` 是要匹配的 ServiceAccount 对象所在的命名空间。
	// 必填。
	Namespace string
	// `name` 是要匹配的 ServiceAccount 对象的名称，使用 "*" 表示不限制名称。
	// 必填。
	Name string
}

// ResourcePolicyRule 是一个谓词，用于匹配某些资源请求，检查请求的动词和目标资源。
// 当且仅当：(a) verbs 中至少有一项匹配该请求，(b) apiGroups 中至少有一项匹配该请求，
// (c) resources 中至少有一项匹配该请求，且 (d) namespaces 中至少有一项匹配该请求时，
// ResourcePolicyRule 才匹配该资源请求。
type ResourcePolicyRule struct {
	// `verbs` 是要匹配的动词列表，不可为空。
	// "*" 匹配所有动词，如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	Verbs []string

	// `apiGroups` 是要匹配的 API 组列表，不可为空。
	// "*" 匹配所有 API 组，如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	APIGroups []string

	// `resources` 是要匹配的资源列表（即小写复数形式），可按需包含子资源，
	// 例如 ["services", "nodes/status"]。该列表不可为空。
	// "*" 匹配所有资源，如果出现，则必须是唯一一项。
	// 必填。
	// +listType=set
	Resources []string

	// `clusterScope` 指示是否匹配未指定命名空间的请求（这可能是因为资源本身不是命名空间
	// 作用域的，也可能是请求面向所有命名空间）。
	// 如果省略该字段或其值为 false，则 `namespaces` 字段必须包含非空列表。
	// +optional
	ClusterScope bool

	// `namespaces` 是用于限制匹配的目标命名空间列表。对于指定了目标命名空间的请求，
	// 仅当 (a) 该列表包含这个目标命名空间，或 (b) 该列表包含 "*" 时才匹配。
	// 注意，"*" 匹配任意已指定的命名空间，但不匹配_未指定_命名空间的请求
	// （这种情况由 `clusterScope` 字段控制）。
	// 该列表可以为空，但仅在 `clusterScope` 为 true 时才允许。
	// +optional
	// +listType=set
	Namespaces []string
}

// NonResourcePolicyRule 是一个谓词，根据请求的动词和目标非资源 URL 来匹配非资源请求。
// 当且仅当 (a) verbs 中至少有一项匹配该请求，且 (b) nonResourceURLs 中至少有一项匹配该请求时，
// NonResourcePolicyRule 才匹配该请求。
type NonResourcePolicyRule struct {
	// `verbs` 是要匹配的动词列表，不可为空。
	// "*" 匹配所有动词。如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	Verbs []string
	// `nonResourceURLs` 是用户应有权访问的 URL 前缀集合，不可为空。
	// 例如：
	//   - "/healthz" 合法
	//   - "/hea*" 不合法
	//   - "/hea" 合法但不匹配任何内容
	//   - "/hea/*" 同样不匹配任何内容
	//   - "/healthz/*" 匹配所有组件的健康检查
	// "*" 匹配所有非资源 URL。如果出现，则必须是唯一一项。
	// +listType=set
	// 必填。
	NonResourceURLs []string
}

// FlowSchemaStatus 表示 FlowSchema 的当前状态。
type FlowSchemaStatus struct {
	// `conditions` 是 FlowSchema 当前状态的列表。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []FlowSchemaCondition
}

// FlowSchemaCondition 描述 FlowSchema 的条件。
type FlowSchemaCondition struct {
	// `type` 是条件的类型。
	// 必填。
	Type FlowSchemaConditionType
	// `status` 是条件的状态。
	// 取值可以为 True、False、Unknown。
	// 必填。
	Status ConditionStatus
	// `lastTransitionTime` 是条件上一次从一种状态转变为另一种状态的时间。
	LastTransitionTime metav1.Time
	// `reason` 是条件上一次状态转变的原因，使用唯一的单个驼峰命名单词表示。
	Reason string
	// `message` 是人类可读的消息，说明上一次状态转变的详细信息。
	Message string
}

// FlowSchemaConditionType 是 FlowSchemaStatusCondition.Type 的合法取值。
type FlowSchemaConditionType string

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PriorityLevelConfiguration 表示一个优先级级别的配置。
type PriorityLevelConfiguration struct {
	metav1.TypeMeta
	// `metadata` 是对象的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta
	// `spec` 描述“请求优先级”的期望行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec PriorityLevelConfigurationSpec
	// `status` 表示“请求优先级”的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PriorityLevelConfigurationStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PriorityLevelConfigurationList 是 PriorityLevelConfiguration 对象的列表。
type PriorityLevelConfigurationList struct {
	metav1.TypeMeta
	// `metadata` 是对象的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta
	// `items` 是请求优先级的列表。
	Items []PriorityLevelConfiguration
}

// PriorityLevelConfigurationSpec 指定优先级级别的配置。
// +union
type PriorityLevelConfigurationSpec struct {
	// `type` 指示该优先级级别是否受请求执行限制。`"Exempt"` 表示
	// 该级别的请求不受限制（因此永远不会排队），也不会占用分配给其他优先级级别的容量。
	// `"Limited"` 表示：(a) 该级别的请求_受_限制，且 (b) 服务器的一部分受限容量
	// 专门留给该优先级级别使用。
	// 必填。
	// +unionDiscriminator
	Type PriorityLevelEnablement

	// `limited` 指定 Limited 优先级级别下请求的处理方式。
	// 当且仅当 `type` 为 `"Limited"` 时，该字段必须非空。
	// +optional
	Limited *LimitedPriorityLevelConfiguration

	// `exempt` 指定 Exempt 优先级级别下请求的处理方式。
	// 当 `type` 为 `"Limited"` 时，该字段必须为空。
	// 当 `type` 为 `"Exempt"` 时，该字段可以非空。
	// 如果该字段为空且 `type` 为 `"Exempt"`，则采用 `ExemptPriorityLevelConfiguration` 的默认值。
	// +optional
	Exempt *ExemptPriorityLevelConfiguration
}

// PriorityLevelEnablement 指示是否为该优先级级别启用执行限制。
type PriorityLevelEnablement string

// 支持的优先级级别启用取值。
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
	// `nominalConcurrencyShares`（NCS）用于计算该级别的名义并发上限（NominalConcurrencyLimit，NominalCL），
	// 即该优先级级别可用的执行席位数量。它既用于从该优先级级别分发的请求，
	// 也用于从其他优先级级别借用本级别席位的请求。
	// 服务器的并发上限（ServerCL）按各 Limited 优先级级别的 NCS 值比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大意味着名义并发上限越高，但代价是其他所有优先级级别的份额减少。
	// 该字段的默认值为 30。
	//
	// 将该字段设为零可用于为该优先级级别构建一个“牢笼”（jail），以隔离某些请求。
	//
	// +optional
	NominalConcurrencyShares int32

	// `limitResponse` 指示对当前无法执行的请求应如何处理。
	LimitResponse LimitResponse

	// `lendablePercent` 规定该级别名义并发上限中可被其他优先级级别借用的比例。
	// 该字段的取值必须在 0 到 100 之间（含两端），默认值为 0。
	// 其他级别可从本级别借用的席位数称为该级别的 LendableConcurrencyLimit（LendableCL），定义如下：
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32

	// `borrowingLimitPercent` 如果存在，则为该优先级级别可从其他级别借用的席位数设置上限。
	// 该上限称为该级别的 BorrowingConcurrencyLimit（BorrowingCL），即该级别在任意时刻
	// 可借用的席位总数上限。该字段记录的是此上限与本级名义并发上限的比值。
	// 当该字段非 nil 时，其值必须是非负整数，上限计算方式如下：
	//
	// BorrowingCL(i) = round( NominalCL(i) * borrowingLimitPercent(i)/100.0 )
	//
	// 该字段的取值可以超过 100，表示该优先级级别可借用的席位数超过其自身的名义并发上限（NominalCL）。
	// 如果该字段为 `nil`，则上限实际上为无穷大。
	// +optional
	BorrowingLimitPercent *int32
}

// ExemptPriorityLevelConfiguration 描述豁免请求处理过程中可配置的部分。
// 在强制存在的 exempt 配置对象中，与 `spec` 的其余部分不同，
// 此处各字段的值可由授权用户修改。
type ExemptPriorityLevelConfiguration struct {
	// `nominalConcurrencyShares`（NCS）用于计算该级别的名义并发上限（NominalConcurrencyLimit，NominalCL），
	// 即名义上为该优先级级别预留的执行席位数量。它_不会_限制从该优先级级别分发请求，
	// 而是通过借用机制影响其他优先级级别。
	// 服务器的并发上限（ServerCL）按所有优先级级别的 NCS 值比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大意味着名义并发上限越高，但代价是其他所有优先级级别的份额减少。
	// 该字段的默认值为零。
	// +optional
	NominalConcurrencyShares *int32
	// `lendablePercent` 规定该级别名义并发上限中可被其他优先级级别借用的比例。
	// 该字段的取值必须在 0 到 100 之间（含两端），默认值为 0。
	// 其他级别可从本级别借用的席位数称为该级别的 LendableConcurrencyLimit（LendableCL），定义如下：
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32
	// Exempt 优先级级别的 `BorrowingCL` 隐式等于 `ServerCL`。
	// 换句话说，豁免级别对其借用量没有实际限制。
	// 因此这里没有对应的显式字段。
}

// LimitResponse 定义如何处理当前无法执行的请求。
// +union
type LimitResponse struct {
	// `type` 取 "Queue" 或 "Reject"。
	// "Queue" 表示到达时无法执行的请求会在队列中等待，直到可以执行或达到排队上限。
	// "Reject" 表示到达时无法执行的请求会被直接拒绝。
	// 必填。
	// +unionDiscriminator
	Type LimitResponseType

	// `queuing` 保存排队相关的配置参数。
	// 仅当 `type` 为 `"Queue"` 时该字段才可非空。
	// +optional
	Queuing *QueuingConfiguration
}

// LimitResponseType 标识 Limited 优先级级别如何处理当前无法执行的请求。
type LimitResponseType string

// 支持的限制响应方式。
const (
	// LimitResponseTypeQueue 表示当前无法执行的请求会进入队列等待，直到可以执行或达到排队上限。
	LimitResponseTypeQueue LimitResponseType = "Queue"

	// LimitResponseTypeReject 表示当前无法执行的请求会被直接拒绝。
	LimitResponseTypeReject LimitResponseType = "Reject"
)

// QueuingConfiguration 保存排队相关的配置参数。
type QueuingConfiguration struct {
	// `queues` 是该优先级级别的队列数量。各 apiserver 上的队列相互独立。
	// 该值必须为正数。将其设为 1 实际上会禁用 shuffle sharding（洗牌分片），
	// 从而使相关 FlowSchema 的区分符方法失去意义。该字段的默认值为 64。
	// +optional
	Queues int32

	// `handSize` 是一个较小的正整数，用于配置请求到队列的 shuffle sharding（洗牌分片）。
	// 在该优先级级别入队时，请求的流标识符（一对字符串）会被哈希，哈希值用于打乱队列列表
	// 并“发”出这里指定数量的一手队列，请求会被放入其中最短的队列之一。
	// `handSize` 不得大于 `queues`，且应显著更小（以避免少数大流量把大多数队列占满）。
	// 关于该字段设置的更详细指导，参见面向用户的文档。该字段的默认值为 8。
	// +optional
	HandSize int32

	// `queueLengthLimit` 是该优先级级别单个队列中允许同时等待的最大请求数，
	// 超出的请求会被拒绝。该值必须为正数。如果未指定，则默认值为 50。
	// +optional
	QueueLengthLimit int32
}

// PriorityLevelConfigurationConditionType 是 PriorityLevelConfigurationStatusCondition.Type 的合法取值。
type PriorityLevelConfigurationConditionType string

// PriorityLevelConfigurationStatus 表示“请求优先级”的当前状态。
type PriorityLevelConfigurationStatus struct {
	// `conditions` 是“请求优先级”的当前状态。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []PriorityLevelConfigurationCondition
}

// PriorityLevelConfigurationCondition 定义优先级级别的条件。
type PriorityLevelConfigurationCondition struct {
	// `type` 是条件的类型。
	// 必填。
	Type PriorityLevelConfigurationConditionType
	// `status` 是条件的状态。
	// 取值可以为 True、False、Unknown。
	// 必填。
	Status ConditionStatus
	// `lastTransitionTime` 是条件上一次从一种状态转变为另一种状态的时间。
	LastTransitionTime metav1.Time
	// `reason` 是条件上一次状态转变的原因，使用唯一的单个驼峰命名单词表示。
	Reason string
	// `message` 是人类可读的消息，说明上一次状态转变的详细信息。
	Message string
}

// ConditionStatus 表示条件的状态。
type ConditionStatus string

// 以下是合法的条件状态。"ConditionTrue" 表示资源处于该条件中。
// "ConditionFalse" 表示资源不处于该条件中。"ConditionUnknown" 表示 Kubernetes
// 无法判断资源是否处于该条件中。未来可能会增加其他中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)
