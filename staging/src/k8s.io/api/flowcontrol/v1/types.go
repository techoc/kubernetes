/*
Copyright 2023 The Kubernetes Authors.

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
)

// 以下是合法的通配符常量。
const (
	APIGroupAll    = "*"
	ResourceAll    = "*"
	VerbAll        = "*"
	NonResourceAll = "*"
	NameAll        = "*"

	NamespaceEvery = "*" // 匹配任意一个具体的命名空间
)

// 系统预设的优先级名称。
const (
	PriorityLevelConfigurationNameExempt   = "exempt"
	PriorityLevelConfigurationNameCatchAll = "catch-all"
	FlowSchemaNameExempt                   = "exempt"
	FlowSchemaNameCatchAll                 = "catch-all"
)

// 条件类型常量。
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
	// 开启 bootstrap 配置 spec 自动更新的注解名，取值为 'true' 时生效。
	// 全新安装时所有 bootstrap 配置对象都带此注解：
	//    apf.kubernetes.io/autoupdate-spec: 'true'
	// kube-apiserver 定期检查集群中的 bootstrap 配置对象并按需应用更新。
	//
	// 对强制配置项，kube-apiserver 执行“始终自动更新”策略：
	// - 注解缺失时补上并置为 'true'；
	// - 注解值为布尔 false 或非法布尔表示时置为 'true'
	//   （运维人员设为 'false' 也会被覆盖）；
	// - 运维人员对 spec 的修改会被覆盖，唯一例外是名为 "exempt" 的
	//   PriorityLevelConfiguration 的 nominalConcurrencyShares 和 lendablePercent 字段。
	//
	// 对建议配置项，满足以下条件之一时应用更新：
	// - 运维人员通过注解显式开启自动更新；
	// - 注解缺失但 generation 为 1。
	// 注解缺失时 kube-apiserver 会据此补全：generation 为 '1' 通常意味着 spec 未被改过，置 'true'；
	// generation 大于 1 则置 'false'。
	// 目的是让 kube-apiserver 能更新旧版本遗留的建议配置，又不覆盖运维人员的改动。
	// 该区分并不严谨：若运维人员删除建议配置后以不同 spec 重建且不再更新（generation 为 1），
	// 上述方法会误判为需要自动更新。
	AutoUpdateAnnotationKey = "apf.kubernetes.io/autoupdate-spec"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.29

// FlowSchema 定义一组流的模式。流由具有相似属性的一组入站 API 请求构成，
// 由 FlowSchema 名和 "流区分符" 这对字符串标识。
// +k8s:supportsSubresource="/status"
type FlowSchema struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// FlowSchema 期望行为的规格。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec FlowSchemaSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// FlowSchema 的当前状态。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status FlowSchemaStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.29

// FlowSchemaList 是 FlowSchema 对象的列表。
type FlowSchemaList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// FlowSchema 列表。
	Items []FlowSchema `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// FlowSchemaSpec 描述 FlowSchema 的规格。
type FlowSchemaSpec struct {
	// 引用集群中的 PriorityLevelConfiguration。引用无法解析时该 FlowSchema 会被忽略，
	// 并在状态中标记为无效。必填。
	// +required
	PriorityLevelConfiguration PriorityLevelConfigurationReference `json:"priorityLevelConfiguration" protobuf:"bytes,1,opt,name=priorityLevelConfiguration"`
	// 当多个 FlowSchema 都匹配请求时据此选择：数值最小者被认为优先级最高。
	// 取值范围 [1,10000]，未指定时默认 1000。
	// +optional
	MatchingPrecedence int32 `json:"matchingPrecedence" protobuf:"varint,2,opt,name=matchingPrecedence"`
	// 计算匹配本模式的请求的流区分符。nil 表示禁用，区分符恒为空串。
	// +optional
	DistinguisherMethod *FlowDistinguisherMethod `json:"distinguisherMethod,omitempty" protobuf:"bytes,3,opt,name=distinguisherMethod"`
	// 描述哪些请求匹配本模式：任一规则命中即匹配；空切片表示不匹配任何请求。
	// +listType=atomic
	// +optional
	Rules []PolicyRulesWithSubjects `json:"rules,omitempty" protobuf:"bytes,4,rep,name=rules"`
}

// FlowDistinguisherMethodType 表示流区分符方法的类型。
type FlowDistinguisherMethodType string

// 以下是合法的流区分符方法。
const (
	// 以请求中的用户名作为流区分符，用于在各用户之间提供一定隔离。
	FlowDistinguisherMethodByUserType FlowDistinguisherMethodType = "ByUser"

	// 以请求操作对象所属的命名空间作为流区分符。对象非命名空间级或请求为非资源请求时，
	// 区分符为空串。适用于多租户且每命名空间专属一个租户的隔离场景。
	FlowDistinguisherMethodByNamespaceType FlowDistinguisherMethodType = "ByNamespace"
)

// FlowDistinguisherMethod 指定流区分符的计算方法。
type FlowDistinguisherMethod struct {
	// 流区分符方法类型，支持 "ByUser" 和 "ByNamespace"。必填。
	// +required
	Type FlowDistinguisherMethodType `json:"type" protobuf:"bytes,1,opt,name=type"`
}

// PriorityLevelConfigurationReference 指向所使用的请求优先级。
type PriorityLevelConfigurationReference struct {
	// 被引用的优先级配置名称。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PolicyRulesWithSubjects 定义对 apiserver 请求的匹配测试，考虑请求主体、操作动词和目标资源。
// 当且仅当 subjects 中至少一项匹配，且 resourceRules 或 nonResourceRules 中至少一项匹配时命中。
type PolicyRulesWithSubjects struct {
	// 本规则关心的普通用户、ServiceAccount 或用户组列表，至少一项。
	// 同时包含 system:authenticated 和 system:unauthenticated 两个组时匹配所有请求。
	// +listType=atomic
	// 必填。
	// +required
	Subjects []Subject `json:"subjects" protobuf:"bytes,1,rep,name=subjects"`
	// 按操作动词和目标资源匹配请求的资源规则。resourceRules 与 nonResourceRules 至少一个非空。
	// +listType=atomic
	// +optional
	ResourceRules []ResourcePolicyRule `json:"resourceRules,omitempty" protobuf:"bytes,2,opt,name=resourceRules"`
	// 按操作动词和目标非资源 URL 匹配请求的规则列表。
	// +listType=atomic
	// +optional
	NonResourceRules []NonResourcePolicyRule `json:"nonResourceRules,omitempty" protobuf:"bytes,3,opt,name=nonResourceRules"`
}

// Subject 匹配请求认证系统识别出的发起者，支持按用户、用户组或 ServiceAccount 三种方式。
// +union
type Subject struct {
	// 指明其他字段中哪一个非空。必填。
	// +required
	// +unionDiscriminator
	Kind SubjectKind `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// 按用户名匹配。
	// +optional
	User *UserSubject `json:"user,omitempty" protobuf:"bytes,2,opt,name=user"`
	// 按用户组名匹配。
	// +optional
	Group *GroupSubject `json:"group,omitempty" protobuf:"bytes,3,opt,name=group"`
	// 匹配 ServiceAccount。
	// +optional
	ServiceAccount *ServiceAccountSubject `json:"serviceAccount,omitempty" protobuf:"bytes,4,opt,name=serviceAccount"`
}

// SubjectKind 表示主体类型。
type SubjectKind string

// 支持的主体类型。
const (
	SubjectKindUser           SubjectKind = "User"
	SubjectKindGroup          SubjectKind = "Group"
	SubjectKindServiceAccount SubjectKind = "ServiceAccount"
)

// UserSubject 保存用户类型主体的详细信息。
type UserSubject struct {
	// 匹配的用户名，'*' 匹配所有用户名。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// GroupSubject 保存用户组类型主体的详细信息。
type GroupSubject struct {
	// 匹配的用户组，'*' 匹配所有组。常见组名见
	// https://github.com/kubernetes/apiserver/blob/master/pkg/authentication/user/user.go。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// ServiceAccountSubject 保存 ServiceAccount 类型主体的详细信息。
type ServiceAccountSubject struct {
	// 待匹配 ServiceAccount 所在的命名空间。必填。
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// 待匹配 ServiceAccount 的名称，'*' 表示不限制名称。必填。
	// +required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
}

// ResourcePolicyRule 是按动词和目标资源匹配资源请求的谓词。当且仅当
// (a) verbs 中至少一项匹配、(b) apiGroups 中至少一项匹配、
// (c) resources 中至少一项匹配，且 (d) 请求未指定命名空间（Namespace==""）
// 且 clusterScope 为 true，或请求指定了命名空间且 namespaces 中至少一项匹配时命中。
type ResourcePolicyRule struct {
	// 匹配的动词列表，不能为空。'*' 匹配所有动词，出现时必须为唯一元素。
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`

	// 匹配的 API 组列表，不能为空。'*' 匹配所有组，出现时必须为唯一元素。
	// +listType=set
	// 必填。
	// +required
	APIGroups []string `json:"apiGroups" protobuf:"bytes,2,rep,name=apiGroups"`

	// 匹配的资源列表（小写复数，可带子资源，如 "services"、"nodes/status"），
	// 不能为空。'*' 匹配所有资源，出现时必须为唯一元素。
	// 必填。
	// +listType=set
	// +required
	Resources []string `json:"resources" protobuf:"bytes,3,rep,name=resources"`

	// 是否匹配未指定命名空间的请求（资源非命名空间级或请求面向所有命名空间）。
	// 省略或为 false 时，namespaces 必须为非空列表。
	// +optional
	ClusterScope bool `json:"clusterScope,omitempty" protobuf:"varint,4,opt,name=clusterScope"`

	// 限制匹配的目标命名空间列表。指定命名空间的请求仅在列表包含该命名空间
	// 或包含 '*' 时命中。注意 '*' 只匹配“指定了”命名空间，不匹配未指定的请求
	// （那由 clusterScope 控制）。仅当 clusterScope 为 true 时该列表可为空。
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces" protobuf:"bytes,5,rep,name=namespaces"`
}

// NonResourcePolicyRule 按动词和目标非资源 URL 匹配非资源请求，
// 当且仅当 verbs 与 nonResourceURLs 各至少一项匹配时命中。
type NonResourcePolicyRule struct {
	// `verbs` is a list of matching verbs and may not be empty.
	// "*" matches all verbs. If it is present, it must be the only entry.
	// +listType=set
	// 必填。
	// +required
	Verbs []string `json:"verbs" protobuf:"bytes,1,rep,name=verbs"`
	// 用户可访问的非资源 URL 前缀集合，不能为空。例如：
	// "/healthz" 合法；"/hea*" 非法；"/hea" 与 "/hea/*" 合法但不匹配任何请求；
	// "/healthz/*" 匹配所有组件健康检查。'*' 匹配所有非资源 URL，出现时必须为唯一元素。
	// +listType=set
	// 必填。
	// +required
	NonResourceURLs []string `json:"nonResourceURLs" protobuf:"bytes,6,rep,name=nonResourceURLs"`
}

// FlowSchemaStatus 表示 FlowSchema 的当前状态。
type FlowSchemaStatus struct {
	// FlowSchema 当前状态的条件列表。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []FlowSchemaCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// FlowSchemaCondition 描述 FlowSchema 的条件。
type FlowSchemaCondition struct {
	// 条件类型。
	// 必填。
	// +required
	Type FlowSchemaConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// 条件状态，应指定为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// 条件上次发生状态变更的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上次状态变更的唯一、单词、驼峰式原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 可读的变更详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// FlowSchemaConditionType 是 FlowSchemaStatusCondition.Type 的合法取值。
type FlowSchemaConditionType string

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.29

// PriorityLevelConfiguration 表示一个优先级配置。
// +k8s:supportsSubresource="/status"
type PriorityLevelConfiguration struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// "请求优先级"期望行为的规格。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec PriorityLevelConfigurationSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// "请求优先级"的当前状态。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PriorityLevelConfigurationStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.29

// PriorityLevelConfigurationList 是优先级配置的列表。
type PriorityLevelConfigurationList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// 请求优先级配置列表。
	Items []PriorityLevelConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PriorityLevelConfigurationSpec 指定优先级的配置。
// +union
type PriorityLevelConfigurationSpec struct {
	// 本优先级是否受请求执行限制。"Exempt" 表示不受限（从不排队），
	// 也不占用其他优先级可用的容量；"Limited" 表示受限，
	// 且服务端部分受限容量专供该优先级使用。必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type PriorityLevelEnablement `json:"type" protobuf:"bytes,1,opt,name=type"`

	// Limited 优先级的请求处理方式。仅当 type 为 "Limited" 时该字段非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Limited")=+k8s:required
	Limited *LimitedPriorityLevelConfiguration `json:"limited,omitempty" protobuf:"bytes,2,opt,name=limited"`

	// Exempt 优先级的请求处理方式。type 为 "Limited" 时必须为空，
	// 为 "Exempt" 时可非空；留空则使用 ExemptPriorityLevelConfiguration 的默认值。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Exempt")=+k8s:optional
	Exempt *ExemptPriorityLevelConfiguration `json:"exempt,omitempty" protobuf:"bytes,3,opt,name=exempt"`
}

// PriorityLevelEnablement 表示优先级是否启用执行限制。
type PriorityLevelEnablement string

// 支持的优先级启用取值。
const (
	// 请求不受限制。
	PriorityLevelEnablementExempt PriorityLevelEnablement = "Exempt"

	// 请求受限制。
	PriorityLevelEnablementLimited PriorityLevelEnablement = "Limited"
)

// LimitedPriorityLevelConfiguration 定义受限请求的处理方式，回答两个问题：
//   - 本优先级的请求如何受限？
//   - 超出的请求如何处理？
type LimitedPriorityLevelConfiguration struct {
	// nominalConcurrencyShares（NCS）用于计算本优先级的名义并发上限（NominalCL），
	// 即本优先级可用的执行席位。本优先级派发的请求以及其他优先级借用的请求都用它。
	// 服务端并发上限（ServerCL）按各 Limited 优先级的 NCS 比例分配：
	// NominalCL(i) = ceil(ServerCL * NCS(i) / sum_ncs)，sum_ncs 为所有优先级 NCS 之和。
	// 数值越大名义并发上限越高，代价是其他优先级可分到的更少。
	// 未指定时默认 30；设为 0 可以把本优先级构造成关押某些请求的 "jail"。
	//
	// +optional
	NominalConcurrencyShares *int32 `json:"nominalConcurrencyShares" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`

	// 当前无法执行的请求如何处理。
	// +required
	LimitResponse LimitResponse `json:"limitResponse,omitempty" protobuf:"bytes,2,opt,name=limitResponse"`

	// 本优先级 NominalCL 中可供其他优先级借用的比例，范围 0-100，默认 0。
	// 可借出的席位数 LendableCL(i) = round(NominalCL(i) * lendablePercent(i)/100.0)。
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,3,opt,name=lendablePercent"`

	// 本优先级可从其他优先级借入席位的上限，即 BorrowingCL，为任意时刻可借入的总席位数。
	// 该字段表示上限与本优先级 NominalCL 的比值，非 nil 时须为非负整数：
	// BorrowingCL(i) = round(NominalCL(i) * borrowingLimitPercent(i)/100.0)。
	// 取值可超过 100，表示可借入超过自身 NominalCL 的席位；为 nil 时上限视为无穷大。
	// +optional
	BorrowingLimitPercent *int32 `json:"borrowingLimitPercent,omitempty" protobuf:"varint,4,opt,name=borrowingLimitPercent"`
}

// ExemptPriorityLevelConfiguration 描述豁免请求处理中可配置的部分。
// 在强制的 exempt 配置对象中，这些字段可由授权用户修改，而 spec 其他部分不可。
type ExemptPriorityLevelConfiguration struct {
	// nominalConcurrencyShares（NCS）用于计算本优先级的名义并发上限（NominalCL），
	// 即名义上为其保留的执行席位数。它不限制本优先级自身的派发，
	// 而是通过借用机制影响其他优先级。
	// ServerCL 按各优先级 NCS 比例分配：NominalCL(i) = ceil(ServerCL * NCS(i) / sum_ncs)。
	// 数值越大名义并发上限越高，代价是其他优先级可分到的更少。默认值为 0。
	// +optional
	NominalConcurrencyShares *int32 `json:"nominalConcurrencyShares,omitempty" protobuf:"varint,1,opt,name=nominalConcurrencyShares"`
	// 本优先级 NominalCL 中可供其他优先级借用的比例，范围 0-100，默认 0。
	// 可借出的席位数 LendableCL(i) = round(NominalCL(i) * lendablePercent(i)/100.0)。
	//
	// +optional
	LendablePercent *int32 `json:"lendablePercent,omitempty" protobuf:"varint,2,opt,name=lendablePercent"`
	// Exempt 优先级的 BorrowingCL 隐式为 ServerCL，即借入量没有实际限制，
	// 因此这里没有显式字段。
}

// LimitResponse 定义当前无法执行的请求如何处理。
// +union
type LimitResponse struct {
	// 取值为 "Queue" 或 "Reject"。Queue 将到达时无法执行的请求排队，
	// 直到可执行或达到队列上限；Reject 直接拒绝。必填。
	// +required
	// +unionDiscriminator
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:modeDiscriminator
	Type LimitResponseType `json:"type" protobuf:"bytes,1,opt,name=type"`

	// 排队参数，仅当 type 为 "Queue" 时可非空。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifMode("Queue")=+k8s:required
	Queuing *QueuingConfiguration `json:"queuing,omitempty" protobuf:"bytes,2,opt,name=queuing"`
}

// LimitResponseType 表示 Limited 优先级如何处理当前无法执行的请求。
type LimitResponseType string

// 支持的处理方式。
const (
	// 无法立即执行的请求进入队列，直到可执行或达到排队上限。
	LimitResponseTypeQueue LimitResponseType = "Queue"

	// 无法立即执行的请求被直接拒绝。
	LimitResponseTypeReject LimitResponseType = "Reject"
)

// QueuingConfiguration 保存排队参数。
type QueuingConfiguration struct {
	// 本优先级的队列数，各 apiserver 独立维护，必须为正数。设为 1 等于禁用
	// shuffle sharding，使关联 FlowSchema 的区分符方法失去意义。默认 64。
	// +optional
	Queues int32 `json:"queues" protobuf:"varint,1,opt,name=queues"`

	// shuffle sharding 的手牌大小，小的正整数。入队时对流的标识（字符串对）
	// 做哈希，据此打乱队列顺序并抽取该数量的队列，请求放入其中最短的一个。
	// 必须不大于 queues，且应显著更小，避免少数大流占满大部分队列。默认 8。
	// +optional
	HandSize int32 `json:"handSize" protobuf:"varint,2,opt,name=handSize"`

	// 本优先级单个队列允许等待的最大请求数，超出的请求被拒绝。
	// 必须为正数，未指定时默认 50。
	// +optional
	QueueLengthLimit int32 `json:"queueLengthLimit" protobuf:"varint,3,opt,name=queueLengthLimit"`
}

// PriorityLevelConfigurationConditionType 是 PriorityLevelConfigurationStatusCondition.Type 的合法取值。
type PriorityLevelConfigurationConditionType string

// PriorityLevelConfigurationStatus 表示请求优先级的当前状态。
type PriorityLevelConfigurationStatus struct {
	// 请求优先级的当前状态条件。
	// +listType=map
	// +listMapKey=type
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +optional
	Conditions []PriorityLevelConfigurationCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// PriorityLevelConfigurationCondition 定义优先级的条件。
type PriorityLevelConfigurationCondition struct {
	// 条件类型。
	// 必填。
	// +required
	Type PriorityLevelConfigurationConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`
	// 条件状态，应指定为 True、False 或 Unknown 之一。
	// +optional
	Status ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// 条件上次发生状态变更的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上次状态变更的唯一、单词、驼峰式原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 可读的变更详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// ConditionStatus 表示条件的当前状态。
type ConditionStatus string

// 合法的条件状态："ConditionTrue" 表示资源处于该条件，
// "ConditionFalse" 表示不处于，"ConditionUnknown" 表示无法判定。
// 未来可能加入其他中间状态，例如 ConditionDegraded。
const (
	// 资源处于该条件。
	ConditionTrue ConditionStatus = "True"
	// 资源不处于该条件。
	ConditionFalse ConditionStatus = "False"
	// 无法判定资源是否处于该条件。
	ConditionUnknown ConditionStatus = "Unknown"
)
