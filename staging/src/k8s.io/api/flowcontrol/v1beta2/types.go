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

package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下是合法通配符。
const (
	APIGroupAll    = "*"
	ResourceAll    = "*"
	VerbAll        = "*"
	NonResourceAll = "*"
	NameAll        = "*"

	NamespaceEvery = "*" // 匹配任意具体命名空间。
)

// 系统预设的优先级级别名称。
const (
	PriorityLevelConfigurationNameExempt   = "exempt"
	PriorityLevelConfigurationNameCatchAll = "catch-all"
	FlowSchemaNameExempt                   = "exempt"
	FlowSchemaNameCatchAll                 = "catch-all"
)

// 条件常量。
const (
	FlowSchemaConditionDangling = "Dangling"

	PriorityLevelConfigurationConditionConcurrencyShared = "ConcurrencyShared"
)

// 供 API 校验使用的常量。
const (
	FlowSchemaMaxMatchingPrecedence int32 = 10000
)

// apiserver 响应头使用的常量。
const (
	ResponseHeaderMatchedPriorityLevelConfigurationUID = "X-Kubernetes-PF-PriorityLevel-UID"
	ResponseHeaderMatchedFlowSchemaUID                 = "X-Kubernetes-PF-FlowSchema-UID"
)

const (
	// AutoUpdateAnnotationKey 是启用引导配置对象 spec 自动更新的注解名，设为 'true' 时生效。
	//
	// 全新安装时，所有引导配置对象都会带上如下注解以启用自动更新：
	//    apf.kubernetes.io/autoupdate-spec: 'true'
	//
	// kube-apiserver 定期检查集群中的引导配置对象，必要时应用更新。
	//
	// 对强制配置对象，kube-apiserver 执行“始终自动更新”策略：
	// - 注解缺失时添加，值为 'true'。
	// - 当前值为布尔 false 或非法布尔表示时设为 'true'（集群运维人员设为 'false' 也会被覆盖）。
	// - 集群运维人员对 spec 的任何修改都会被覆盖，名为 "exempt" 的
	//   PriorityLevelConfiguration 的 `nominalConcurrencyShares` 与 `lendablePercent`
	//   字段除外。
	//
	// 满足以下条件之一时，kube-apiserver 会对建议配置应用更新：
	// - 集群运维人员通过注解启用了自动更新（apf.kubernetes.io/autoupdate-spec: 'true'），或
	// - 注解缺失但 generation 为 1。
	//
	// 若建议配置对象缺少该注解，kube-apiserver 会按如下方式更新：
	// - 对象 generation 为 '1'（通常表示 spec 未被修改）时设为 'true'。
	// - 对象 generation 大于 1 时设为 'false'。
	//
	// 目标是让 kube-apiserver 能更新旧版本安装的建议配置对象，同时不覆盖集群运维人员的修改。
	// 注意这种区分并不完美：若运维人员删除了某个建议配置对象，之后以变体 spec 重新创建且
	// 未再更新（generation 为 1），上述方法会错误地判定该对象应被自动更新。
	AutoUpdateAnnotationKey = "apf.kubernetes.io/autoupdate-spec"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.23
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,FlowSchema

// FlowSchema 定义一组流的模式。一个流由一组属性相似的入站 API 请求构成，
// 由 FlowSchema 名称与“流区分符”这两个字符串共同标识。
// +k8s:supportsSubresource="/status"
type FlowSchema struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.23
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,FlowSchemaList

// FlowSchemaList 是 FlowSchema 对象的列表。
type FlowSchemaList struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// `items` 是 FlowSchema 列表。
	Items []FlowSchema `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// FlowSchemaSpec 描述 FlowSchema 的规格构成。
type FlowSchemaSpec struct {
	// `priorityLevelConfiguration` 应引用集群中的某个 PriorityLevelConfiguration。若引用无法解析，
	// 该 FlowSchema 会被忽略并在 status 中标记为无效。必填。
	// +required
	PriorityLevelConfiguration PriorityLevelConfigurationReference `json:"priorityLevelConfiguration" protobuf:"bytes,1,opt,name=priorityLevelConfiguration"`
	// `matchingPrecedence` 用于在匹配同一请求的多个 FlowSchema 中做选择。选中的是数值最小
	// （视为逻辑上最高）的那个。每个 MatchingPrecedence 值必须在 [1,10000] 范围内。
	// 未指定优先级时默认设为 1000。
	// +optional
	MatchingPrecedence int32 `json:"matchingPrecedence" protobuf:"varint,2,opt,name=matchingPrecedence"`
	// `distinguisherMethod` 定义如何为匹配此模式的请求计算流区分符。`nil` 表示禁用区分符，
	// 此时区分符始终为空字符串。
	// +optional
	DistinguisherMethod *FlowDistinguisherMethod `json:"distinguisherMethod,omitempty" protobuf:"bytes,3,opt,name=distinguisherMethod"`
	// `rules` 描述哪些请求会匹配此流模式。当且仅当至少一条规则匹配请求时，该 FlowSchema 才匹配请求。
	// 若为空切片，则没有任何请求匹配此 FlowSchema。
	// +listType=atomic
	// +optional
	Rules []PolicyRulesWithSubjects `json:"rules,omitempty" protobuf:"bytes,4,rep,name=rules"`
}

// FlowDistinguisherMethodType 是流区分符方法的类型。
type FlowDistinguisherMethodType string

// 以下是合法的流区分符方法。
const (
	// FlowDistinguisherMethodByUserType 指定以请求中的用户名作为流区分符，用于在用户间提供一定隔离。
	FlowDistinguisherMethodByUserType FlowDistinguisherMethodType = "ByUser"

	// FlowDistinguisherMethodByNamespaceType 指定以请求所操作对象的命名空间作为流区分符。
	// 若对象无命名空间或请求是非资源请求，区分符为空字符串。例如在多租户且每个命名空间专属于
	// 一个租户的场景下，可在租户间提供一定隔离。
	FlowDistinguisherMethodByNamespaceType FlowDistinguisherMethodType = "ByNamespace"
)

// FlowDistinguisherMethod 指定流区分符的方法。
type FlowDistinguisherMethod struct {
	// `type` 是流区分符方法的类型，支持 "ByUser" 和 "ByNamespace"，必填。
	// +required
	Type FlowDistinguisherMethodType `json:"type" protobuf:"bytes,1,opt,name=type"`
}

// PriorityLevelConfigurationReference 包含指向所用“请求优先级”的信息。
type PriorityLevelConfigurationReference struct {
	// `name` 是被引用优先级级别配置的名称，必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PolicyRulesWithSubjects 规定了对发往 apiserver 的请求执行的测试，考虑发起请求的主体、请求的动词
// 以及要操作的资源。当且仅当 (a) subjects 中至少一项匹配请求，且 (b) resourceRules 或 nonResourceRules
// 中至少一项匹配请求时，该规则才匹配请求。
type PolicyRulesWithSubjects struct {
	// subjects 是本规则关心的普通用户、serviceaccount 或组列表，至少包含一个成员。
	// 同时包含 system:authenticated 和 system:unauthenticated 用户组的切片可匹配所有请求。
	// +listType=atomic
	// 必填。
	// +required
	Subjects []Subject `json:"subjects" protobuf:"bytes,1,rep,name=subjects"`
	// `resourceRules` 是 ResourcePolicyRules 切片，根据动词和目标资源识别匹配的请求。
	// `resourceRules` 与 `nonResourceRules` 至少有一个非空。
	// +listType=atomic
	// +optional
	ResourceRules []ResourcePolicyRule `json:"resourceRules,omitempty" protobuf:"bytes,2,opt,name=resourceRules"`
	// `nonResourceRules` 是 NonResourcePolicyRules 列表，根据动词和目标非资源 URL 识别匹配的请求。
	// +listType=atomic
	// +optional
	NonResourceRules []NonResourcePolicyRule `json:"nonResourceRules,omitempty" protobuf:"bytes,3,opt,name=nonResourceRules"`
}

// Subject 按请求认证系统的标识匹配请求发起方，有三种方式：用户、组或服务账号。
// +union
type Subject struct {
	// `kind` 指明其余字段中哪一个非空，必填。
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
	SubjectKindUser           SubjectKind = "User"
	SubjectKindGroup          SubjectKind = "Group"
	SubjectKindServiceAccount SubjectKind = "ServiceAccount"
)

// UserSubject 保存用户类主体的详细信息。
type UserSubject struct {
	// `name` 是匹配的用户名，"*" 表示匹配所有用户名。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// GroupSubject 保存组类主体的详细信息。
type GroupSubject struct {
	// name 是匹配的用户组，"*" 表示匹配所有用户组。部分知名组名参见
	// https://github.com/kubernetes/apiserver/blob/master/pkg/authentication/user/user.go。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// ServiceAccountSubject 保存服务账号类主体的详细信息。
type ServiceAccountSubject struct {
	// `namespace` 是匹配的 ServiceAccount 对象所在命名空间，必填。
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// `name` 是匹配的 ServiceAccount 对象的名称，"*" 表示不限制名称。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
}

// ResourcePolicyRule 是通过动词和目标资源匹配资源请求的谓词。当且仅当：(a) verbs 中至少一项匹配请求，
// (b) apiGroups 中至少一项匹配请求，(c) resources 中至少一项匹配请求，且 (d) 满足以下之一：
// (d1) 请求未指定命名空间（即 `Namespace==""`）且 clusterScope 为 true，或
// (d2) 请求指定了命名空间且 namespaces 中至少一项匹配该命名空间。
type ResourcePolicyRule struct {
	// `verbs` 是匹配动词的列表，不可为空。"*" 匹配所有动词，出现时必须是唯一条目。
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// `apiGroups` 是匹配 API 组的列表，不可为空。"*" 匹配所有 API 组，出现时必须是唯一条目。
	// +listType=set
	// 必填。
	// +required
	APIGroups []string `json:"apiGroups" protobuf:"bytes,2,rep,name=apiGroups"`

	// `resources` 是匹配资源的列表（小写复数形式，可带子资源），例如 ["services", "nodes/status"]，
	// 不可为空。"*" 匹配所有资源，出现时必须是唯一条目。，必填。
	// +listType=set
	// +required
	Resources []string `json:"resources" protobuf:"bytes,3,rep,name=resources"`

	// `clusterScope` 指示是否匹配未指定命名空间的请求（因资源本身无命名空间，或请求面向所有命名空间）。
	// 若省略此字段或设为 false，则 `namespaces` 字段必须包含非空列表。
	// +optional
	ClusterScope bool `json:"clusterScope,omitempty" protobuf:"varint,4,opt,name=clusterScope"`

	// `namespaces` 是限制匹配范围的目标命名空间列表。指定了目标命名空间的请求仅在以下情况匹配：
	// (a) 此列表包含该目标命名空间，或 (b) 此列表包含 "*"。注意 "*" 匹配任意已指定的命名空间，
	// 但不匹配 _未指定_ 命名空间的请求（那由 `clusterScope` 字段控制）。
	// 此列表可为空，但仅限 `clusterScope` 为 true 时。
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces" protobuf:"bytes,5,rep,name=namespaces"`
}

// NonResourcePolicyRule 是按动词和目标非资源 URL 匹配非资源请求的谓词。当且仅当 (a) verbs 中至少一项
// 匹配请求，且 (b) nonResourceURLs 中至少一项匹配请求时，该规则才匹配请求。
type NonResourcePolicyRule struct {
	// `verbs` 是匹配动词的列表，不可为空。"*" 匹配所有动词，出现时必须是唯一条目。
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`
	// `nonResourceURLs` 是用户应可访问的 URL 前缀集合，不可为空。例如：
	//   - "/healthz" 合法
	//   - "/hea*" 非法
	//   - "/hea" 合法但不匹配任何内容
	//   - "/hea/*" 同样不匹配任何内容
	//   - "/healthz/*" 匹配所有组件的健康检查。
	// "*" 匹配所有非资源 URL，出现时必须是唯一条目。
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
	// `type` 是条件的类型，必填。
	// +required
	Type FlowSchemaConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态，应指定并设为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件上次从一种状态转为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件上次转换原因的唯一单词 CamelCase 表示。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是描述上次转换细节的人类可读消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// FlowSchemaConditionType 是 FlowSchemaStatusCondition.Type 的合法取值。
type FlowSchemaConditionType string

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.23
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,PriorityLevelConfiguration

// PriorityLevelConfiguration 表示一个优先级级别的配置。
// +k8s:supportsSubresource="/status"
type PriorityLevelConfiguration struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.23
// +k8s:prerelease-lifecycle-gen:replacement=flowcontrol.apiserver.k8s.io,v1beta3,PriorityLevelConfigurationList

// PriorityLevelConfigurationList 是 PriorityLevelConfiguration 对象的列表。
type PriorityLevelConfigurationList struct {
	metav1.TypeMeta `json:""`
	// `metadata` 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// `items` 是请求优先级列表。
	Items []PriorityLevelConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PriorityLevelConfigurationSpec 指定优先级级别的配置。
// +union
type PriorityLevelConfigurationSpec struct {
	// `type` 指示此优先级级别是否受请求执行限制。`"Exempt"` 表示此级别请求不受限
	// （因而从不排队），也不占用其他优先级级别可用的容量。`"Limited"` 表示
	// (a) 此级别请求受限制，且 (b) 服务端部分受限容量专供此级别使用。必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type PriorityLevelEnablement `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `limited` 指定 Limited 优先级级别如何处理请求。当且仅当 `type` 为 `"Limited"` 时此字段必须非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Limited")=+k8s:required
	Limited *LimitedPriorityLevelConfiguration `json:"limited,omitempty" protobuf:"bytes,2,opt,name=limited"`

	// `exempt` 指定 exempt 优先级级别如何处理请求。`type` 为 `"Limited"` 时此字段必须为空，
	// 为 `"Exempt"` 时可非空。若为空且 `type` 为 `"Exempt"`，则使用
	// `ExemptPriorityLevelConfiguration` 的默认值。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Exempt")=+k8s:optional
	Exempt *ExemptPriorityLevelConfiguration `json:"exempt,omitempty" protobuf:"bytes,3,opt,name=exempt"`
}

// PriorityLevelEnablement 指示优先级级别是否启用执行限制。
type PriorityLevelEnablement string

// 支持的优先级级别启用取值。
const (
	// PriorityLevelEnablementExempt 表示请求不受限制。
	PriorityLevelEnablementExempt PriorityLevelEnablement = "Exempt"

	// PriorityLevelEnablementLimited 表示请求受限制。
	PriorityLevelEnablementLimited PriorityLevelEnablement = "Limited"
)

// LimitedPriorityLevelConfiguration 指定如何处理受限制的请求，涉及两个问题：
//   - 如何限制此优先级级别的请求？
//   - 对超出限制的请求如何处理？
type LimitedPriorityLevelConfiguration struct {
	// `assuredConcurrencyShares`（ACS）配置执行限制，即某一时刻此优先级级别可执行的请求数上限。
	// ACS 必须为正数。服务端并发限制（SCL）按各并发受控优先级级别的 ACS 比例分配，
	// 由此得到每个级别的保证并发值（ACV），即可同时执行的请求数：
	//
	//             ACV(l) = ceil( SCL * ACS(l) / ( sum[priority levels k] ACS(k) ) )
	//
	// ACS 越大表示预留的并发请求越多（代价是其他优先级级别）。此字段默认值为 30。
	// +optional
	AssuredConcurrencyShares int32 `json:"assuredConcurrencyShares" protobuf:"varint,1,opt,name=assuredConcurrencyShares"`

	// `limitResponse` 指示对当前无法执行的请求如何处理。
	// +required
	LimitResponse LimitResponse `json:"limitResponse,omitempty" protobuf:"bytes,2,opt,name=limitResponse"`

	// `lendablePercent` 规定本级别 NominalCL 中可被其他优先级级别借用的比例，取值须在 0 到 100
	// （含）之间，默认为 0。其他级别可借用的席位数量即本级别的 LendableConcurrencyLimit（LendableCL），
	// 定义如下：
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,3,opt,name=lendablePercent"`

	// `borrowingLimitPercent` 若设置，则限制本优先级级别可从其他级别借用的席位数量。
	// 该限制称为本级别的 BorrowingConcurrencyLimit（BorrowingCL），即任一时刻可借用的席位总数上限。
	// 此字段保存该上限与本级别标称并发限制的比例。非空时必须是非负整数，上限计算如下：
	//
	// BorrowingCL(i) = round( NominalCL(i) * borrowingLimitPercent(i)/100.0 )
	//
	// 此字段值可超过 100，表示本级别可借用的席位数大于自身的标称并发限制（NominalCL）。
	// 为 `nil` 时该限制实际上无限大。
	// +optional
	BorrowingLimitPercent *int32 `json:"borrowingLimitPercent,omitempty" protobuf:"varint,4,opt,name=borrowingLimitPercent"`
}

// ExemptPriorityLevelConfiguration 描述 exempt 请求处理中可配置的部分。
// 在强制 exempt 配置对象中，这些字段的值可由授权用户修改，而 `spec` 其余部分不可。
type ExemptPriorityLevelConfiguration struct {
	// `nominalConcurrencyShares`（NCS）参与计算本级别的 NominalConcurrencyLimit（NominalCL），
	// 即名义上为本优先级级别预留的执行席位数。它并不限制本级别的请求分发，
	// 而是通过借用机制影响其他优先级级别。服务端并发限制（ServerCL）按各级别的 NCS 比例分配：
	//
	// NominalCL(i)  = ceil( ServerCL * NCS(i) / sum_ncs )
	// sum_ncs = sum[priority level k] NCS(k)
	//
	// 数值越大表示标称并发限制越大，代价是其他优先级级别。此字段默认值为 0。
	// +optional
	NominalConcurrencyShares *int32 `json:"nominalConcurrencyShares,omitempty" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`
	// `lendablePercent` 规定本级别 NominalCL 中可被其他优先级级别借用的比例，取值须在 0 到 100
	// （含）之间，默认为 0。其他级别可借用的席位数量即本级别的 LendableConcurrencyLimit（LendableCL），
	// 定义如下。
	//
	// LendableCL(i) = round( NominalCL(i) * lendablePercent(i)/100.0 )
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,2,opt,name=lendablePercent"`
	// Exempt 优先级级别的 `BorrowingCL` 隐式等于 `ServerCL`，即 exempt 级别的借用量
	// 实际上没有有意义的限制，因此这里没有显式字段表示。
}

// LimitResponse 定义如何处理当前无法执行的请求。
// +union
type LimitResponse struct {
	// `type` 取 "Queue" 或 "Reject"。"Queue" 表示到达时无法执行的请求会在队列中等待，
	// 直到可执行或达到队列限制；"Reject" 表示到达时无法执行的请求会被拒绝。必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type LimitResponseType `json:"type" protobuf:"bytes,1,opt,name=type"`

	// `queuing` 保存排队相关的配置参数，仅当 `type` 为 `"Queue"` 时可非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Queue")=+k8s:required
	Queuing *QueuingConfiguration `json:"queuing,omitempty" protobuf:"bytes,2,opt,name=queuing"`
}

// LimitResponseType 标识 Limited 优先级级别如何处理当前无法执行的请求。
type LimitResponseType string

// 支持的限制响应方式。
const (
	// LimitResponseTypeQueue 表示当前无法执行的请求会排队，直到可执行或达到队列限制。
	LimitResponseTypeQueue LimitResponseType = "Queue"

	// LimitResponseTypeReject 表示当前无法执行的请求会被拒绝。
	LimitResponseTypeReject LimitResponseType = "Reject"
)

// QueuingConfiguration 保存排队相关的配置参数。
type QueuingConfiguration struct {
	// `queues` 是本优先级级别的队列数量，各 apiserver 上的队列相互独立，值必须为正。
	// 设为 1 实际上禁用了 shuffle sharding，从而让关联流模式的区分符方法失去意义。
	// 此字段默认值为 64。
	// +optional
	Queues int32 `json:"queues" protobuf:"varint,1,opt,name=queues"`

	// `handSize` 是配置请求 shuffle sharding 入队方式的小正整数。请求入队时，其流标识
	// （一对字符串）被哈希，哈希值用于打乱队列列表并抽取指定大小的一手队列，请求会被放入
	// 其中最短的一个队列。`handSize` 不得大于 `queues`，且应显著更小（以免少数重流占满
	// 大多数队列）。设置此字段的更多指导参见面向用户的文档。此字段默认值为 8。
	// +optional
	HandSize int32 `json:"handSize" protobuf:"varint,2,opt,name=handSize"`

	// `queueLengthLimit` 是本优先级级别单个队列中允许等待的最大请求数，超出的请求会被拒绝。
	// 此值必须为正，未指定时默认为 50。
	// +optional
	QueueLengthLimit int32 `json:"queueLengthLimit" protobuf:"varint,3,opt,name=queueLengthLimit"`
}

// PriorityLevelConfigurationConditionType 是 PriorityLevelConfigurationStatusCondition.Type 的合法取值。
type PriorityLevelConfigurationConditionType string

// PriorityLevelConfigurationStatus 表示“请求优先级”的当前状态。
type PriorityLevelConfigurationStatus struct {
	// `conditions` 是“请求优先级”的当前状态。
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []PriorityLevelConfigurationCondition `json:"conditions,omitempty" protobuf:"bytes,1,rep,name=conditions"`
}

// PriorityLevelConfigurationCondition 定义优先级级别的条件。
type PriorityLevelConfigurationCondition struct {
	// `type` 是条件的类型，必填。
	// +required
	Type PriorityLevelConfigurationConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// `status` 是条件的状态，应指定并设为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// `lastTransitionTime` 是条件上次从一种状态转为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// `reason` 是条件上次转换原因的唯一单词 CamelCase 表示。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// `message` 是描述上次转换细节的人类可读消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// ConditionStatus 是条件的状态。
type ConditionStatus string

// 以下是合法的条件状态。"ConditionTrue" 表示资源处于该条件，"ConditionFalse" 表示不处于，
// "ConditionUnknown" 表示 Kubernetes 无法判断资源是否处于该条件。将来可能新增其他中间状态，
// 例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)
