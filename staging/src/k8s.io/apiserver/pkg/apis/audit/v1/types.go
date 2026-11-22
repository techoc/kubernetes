/*
Copyright 2018 The Kubernetes Authors.

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
	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// 审计系统使用的 header key。
const (
	// 请求在服务层级间传递时携带审计 ID 的 header。
	// Audit-ID 应由第一个收到请求的 server（例如 federation server 或 kube-aggregator）设置。
	HeaderAuditID = "Audit-ID"
)

// Level 定义审计时记录的信息量。
type Level string

// 合法的审计级别
const (
	// LevelNone 关闭审计
	LevelNone Level = "None"
	// LevelMetadata 提供基础级别的审计。
	LevelMetadata Level = "Metadata"
	// LevelRequest 在 Metadata 之外额外记录请求对象（不适用于非资源请求）。
	LevelRequest Level = "Request"
	// LevelRequestResponse 在 Request 之外额外记录响应对象（不适用于非资源请求）。
	LevelRequestResponse Level = "RequestResponse"
)

// Stage 定义请求处理过程中可能产生审计事件的阶段。
type Stage string

// 合法的审计阶段。
const (
	// 审计处理器收到请求后、委托给下一层 handler 之前生成的事件所处阶段。
	StageRequestReceived Stage = "RequestReceived"
	// 响应头已发送但响应体尚未发送时生成的事件所处阶段。
	// 该阶段只为长连接请求（如 watch）生成。
	StageResponseStarted Stage = "ResponseStarted"
	// 响应体已完成、不会再有字节发送时生成的事件所处阶段。
	StageResponseComplete Stage = "ResponseComplete"
	// 发生 panic 时生成的事件所处阶段。
	StagePanic Stage = "Panic"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Event 捕获可写入 API 审计日志的全部信息。
type Event struct {
	metav1.TypeMeta `json:""`

	// 生成该事件时的审计级别
	Level Level `json:"level" protobuf:"bytes,1,opt,name=level,casttype=Level"`

	// 每个请求生成的唯一审计 ID。
	AuditID types.UID `json:"auditID" protobuf:"bytes,2,opt,name=auditID,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// 生成该事件实例时所处的请求处理阶段。
	Stage Stage `json:"stage" protobuf:"bytes,3,opt,name=stage,casttype=Stage"`

	// RequestURI 是客户端发送给 server 的请求 URI。
	RequestURI string `json:"requestURI" protobuf:"bytes,4,opt,name=requestURI"`
	// Verb 是与请求关联的 kubernetes verb。
	// 非资源请求时使用小写的 HTTP method。
	Verb string `json:"verb" protobuf:"bytes,5,opt,name=verb"`
	// 已认证的用户信息。
	User authnv1.UserInfo `json:"user" protobuf:"bytes,6,opt,name=user"`
	// 被模拟的用户信息。
	// +optional
	ImpersonatedUser *authnv1.UserInfo `json:"impersonatedUser,omitempty" protobuf:"bytes,7,opt,name=impersonatedUser"`
	// AuthenticationMetadata 包含请求认证方式的细节。
	// +optional
	AuthenticationMetadata *AuthenticationMetadata `json:"authenticationMetadata,omitempty" protobuf:"bytes,17,opt,name=authenticationMetadata"`
	// 请求来源 IP，包括中间代理。按以下顺序列出：
	// 1. X-Forwarded-For 请求头中的 IP
	// 2. X-Real-Ip 头（若不在 X-Forwarded-For 列表中）
	// 3. 连接的远端地址（若与上述列表最后一个 IP 不同）
	// 注意：除最后一个 IP 外，其余都可能由客户端任意伪造。
	// +optional
	// +listType=atomic
	SourceIPs []string `json:"sourceIPs,omitempty" protobuf:"bytes,8,rep,name=sourceIPs"`
	// UserAgent 记录客户端上报的 user agent 字符串。
	// 注意：UserAgent 由客户端提供，不可信任。
	// +optional
	UserAgent string `json:"userAgent,omitempty" protobuf:"bytes,16,opt,name=userAgent"`
	// 请求所针对的对象引用。
	// 不适用于 List 类型请求或非资源请求。
	// +optional
	ObjectRef *ObjectReference `json:"objectRef,omitempty" protobuf:"bytes,9,opt,name=objectRef"`
	// 响应状态，即使 ResponseObject 不是 Status 类型也会填充。
	// 成功响应只包含 Code 与 StatusSuccess。
	// 非 Status 类型的错误响应会自动填充错误 Message。
	// +optional
	ResponseStatus *metav1.Status `json:"responseStatus,omitempty" protobuf:"bytes,10,opt,name=responseStatus"`

	// 请求中的 API 对象，JSON 格式。RequestObject 按请求原样记录（可能被重新编码为 JSON），
	// 记录时机在版本转换、默认值填充、准入与合并之前。它是外部版本对象，
	// 单独看可能不是合法对象。非资源请求省略。仅在 Request 级别及以上记录。
	// +optional
	RequestObject *runtime.Unknown `json:"requestObject,omitempty" protobuf:"bytes,11,opt,name=requestObject"`
	// 响应中返回的 API 对象，JSON 格式。ResponseObject 在转换为外部版本后记录并序列化为 JSON。
	// 非资源请求省略。仅在 Response 级别记录。
	// +optional
	ResponseObject *runtime.Unknown `json:"responseObject,omitempty" protobuf:"bytes,12,opt,name=responseObject"`
	// 请求到达 apiserver 的时间。
	// +optional
	RequestReceivedTimestamp metav1.MicroTime `json:"requestReceivedTimestamp" protobuf:"bytes,13,opt,name=requestReceivedTimestamp"`
	// 请求到达当前审计阶段的时间。
	// +optional
	StageTimestamp metav1.MicroTime `json:"stageTimestamp" protobuf:"bytes,14,opt,name=stageTimestamp"`

	// Annotations 是附加在审计事件上的非结构化键值对，可由请求处理链中的插件
	// （认证、授权、准入等）设置。注意这些注解属于审计事件，与提交对象的
	// metadata.annotations 无关。key 应能唯一标识设置方以避免冲突
	// （例如 podsecuritypolicy.admission.k8s.io/policy），value 应简短。
	// 注解在 Metadata 级别也会被记录。
	// +optional
	Annotations map[string]string `json:"annotations,omitempty" protobuf:"bytes,15,rep,name=annotations"`
}

// AuthenticationMetadata 记录与认证相关的补充信息。
type AuthenticationMetadata struct {
	// ImpersonationConstraint 是用于授权本次审计事件对应 ImpersonatedUser 的
	// 受限模拟模式所关联的 verb。仅在使用受限模拟时设置。
	// +optional
	ImpersonationConstraint string `json:"impersonationConstraint,omitempty" protobuf:"bytes,1,opt,name=impersonationConstraint"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EventList 是审计 Event 的列表。
type EventList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Event `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Policy 定义审计日志的配置，以及不同请求类别应如何记录的规则。
type Policy struct {
	metav1.TypeMeta `json:""`
	// 包含 ObjectMeta 是为了与 API 基础设施互操作。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Rules 指定请求应记录的审计 Level。
	// 一个请求可能匹配多条规则，此时使用第一条匹配的规则。
	// 默认审计级别为 None，可在列表末尾用 catch-all 规则覆盖。
	// PolicyRules 严格有序。
	// +listType=atomic
	Rules []PolicyRule `json:"rules" protobuf:"bytes,2,rep,name=rules"`

	// OmitStages 是不生成任何事件的阶段列表。也可在单条规则中指定，
	// 此时两者的并集都会被省略。
	// +optional
	// +listType=atomic
	OmitStages []Stage `json:"omitStages,omitempty" protobuf:"bytes,3,rep,name=omitStages"`

	// OmitManagedFields 表示是否在写入 API 审计日志时省略请求体与响应体中的 managed fields。
	// 作为全局默认值：true 时省略，否则包含。
	// 也可在单条规则中指定，规则中的值会覆盖全局默认值。
	// +optional
	OmitManagedFields bool `json:"omitManagedFields,omitempty" protobuf:"varint,4,opt,name=omitManagedFields"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PolicyList 是审计 Policy 的列表。
type PolicyList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Policy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PolicyRule 依据请求元数据把请求映射到审计 Level。
// 请求必须匹配每个字段的规则（各字段规则取交集）。
type PolicyRule struct {
	// 匹配该规则的请求所记录的 Level。
	Level Level `json:"level" protobuf:"bytes,1,opt,name=level,casttype=Level"`

	// 该规则适用的用户（按已认证用户名）。
	// 空列表表示所有用户。
	// +optional
	// +listType=atomic
	Users []string `json:"users,omitempty" protobuf:"bytes,2,rep,name=users"`
	// 该规则适用的用户组。用户属于任一 UserGroups 即视为匹配。
	// 空列表表示所有用户组。
	// +optional
	// +listType=atomic
	UserGroups []string `json:"userGroups,omitempty" protobuf:"bytes,3,rep,name=userGroups"`

	// 匹配该规则的 verb。
	// 空列表表示所有 verb。
	// +optional
	// +listType=atomic
	Verbs []string `json:"verbs,omitempty" protobuf:"bytes,4,rep,name=verbs"`

	// 规则可作用于 API 资源（如 "pods" 或 "secrets"）、非资源 URL 路径（如 "/api"），
	// 或两者都不指定，但不能同时指定两者。
	// 两者都不指定时，该规则视为所有 URL 的默认规则。

	// 该规则匹配的资源。空列表表示所有 API group 中的所有 kind。
	// +optional
	// +listType=atomic
	Resources []GroupResources `json:"resources,omitempty" protobuf:"bytes,5,rep,name=resources"`
	// 该规则匹配的命名空间。
	// 空字符串 "" 匹配非命名空间资源。
	// 空列表表示所有命名空间。
	// +optional
	// +listType=atomic
	Namespaces []string `json:"namespaces,omitempty" protobuf:"bytes,6,rep,name=namespaces"`

	// NonResourceURLs 是需要审计的 URL 路径集合。
	// 允许 `*`，但只能作为路径的最后一整段。
	// 示例：
	// - `/metrics` - 记录 apiserver metrics 请求
	// - `/healthz*` - 记录所有健康检查
	// +optional
	// +listType=atomic
	NonResourceURLs []string `json:"nonResourceURLs,omitempty" protobuf:"bytes,7,rep,name=nonResourceURLs"`

	// OmitStages 是不生成任何事件的阶段列表。也可在 policy 级别指定，
	// 此时两者的并集都会被省略。空列表表示不加限制。
	// +optional
	// +listType=atomic
	OmitStages []Stage `json:"omitStages,omitempty" protobuf:"bytes,8,rep,name=omitStages"`

	// OmitManagedFields 表示是否在写入 API 审计日志时省略请求体与响应体中的 managed fields。
	// - true 时从 API 审计日志中丢弃 managed fields
	// - false 时在 API 审计日志中包含 managed fields
	// 规则中指定的值会覆盖全局默认值；未指定时使用 Policy.OmitManagedFields。
	// +optional
	OmitManagedFields *bool `json:"omitManagedFields,omitempty" protobuf:"varint,9,opt,name=omitManagedFields"`
}

// GroupResources 表示某个 API group 中的资源 kind。
type GroupResources struct {
	// Group 是包含这些资源的 API group 名称。
	// 空字符串表示 core API group。
	// `*` 匹配所有 group
	// +optional
	Group string `json:"group,omitempty" protobuf:"bytes,1,opt,name=group"`
	// Resources 是该规则适用的资源列表。
	//
	// 例如：
	// - `pods` 匹配 pods。
	// - `pods/log` 匹配 pods 的 log 子资源。
	// - `*` 匹配所有资源及其子资源。
	// - `pods/*` 匹配 pods 的所有子资源。
	// - `*/scale` 匹配所有 scale 子资源。
	//
	// 存在通配符时，校验规则会确保资源之间不重叠。
	//
	// 空列表表示该 API group 中的所有资源与子资源。
	// +optional
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,2,rep,name=resources"`
	// ResourceNames 是策略匹配的资源实例名称列表。
	// 使用该字段时必须指定 Resources。
	// 空列表表示匹配该资源的每个实例。
	// +optional
	// +listType=atomic
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,3,rep,name=resourceNames"`
}

// ObjectReference 包含足以定位、检查或修改被引用对象的信息。
// 注意：与 core/v1 的 ObjectReference 不同，审计版本不含 Kind/FieldPath 等字段，
// 因为它们对审计日志而言冗余。
type ObjectReference struct {
	// +optional
	Resource string `json:"resource,omitempty" protobuf:"bytes,1,opt,name=resource"`
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,opt,name=namespace"`
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,3,opt,name=name"`
	// +optional
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,4,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// APIGroup 是包含被引用对象的 API group 名称。
	// 空字符串表示 core API group。
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,5,opt,name=apiGroup"`
	// APIVersion 是包含被引用对象的 API group 的版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,6,opt,name=apiVersion"`
	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,7,opt,name=resourceVersion"`
	// +optional
	Subresource string `json:"subresource,omitempty" protobuf:"bytes,8,opt,name=subresource"`
}
