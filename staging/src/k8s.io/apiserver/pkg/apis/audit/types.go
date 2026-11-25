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

package audit

import (
	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// 审计系统使用的 HTTP 头键。
const (
	// 用于在请求穿过服务层级时承载审计 ID 的头。Audit-ID 头应当由第一个
	// 收到请求的服务器设置（例如联邦服务器或 kube-aggregator）。
	//
	// 审计 ID 也会通过 HTTP 响应头返回给客户端。
	// 并不保证所有请求都会发送 Audit-Id HTTP 头。当 kube-apiserver 未按审计
	// 策略审计事件时，不会返回 Audit-ID。此外，对于 pods/exec、pods/attach、
	// pods/proxy 这类请求，kube-apiserver 像代理一样把请求重定向到 kubelet
	// 所在节点，用户只会拿到 kubelet 节点发送的 HTTP 头，因此运行
	// "kubectl exec" 或 "kubectl attach" 等命令时不会收到 Audit-ID。
	HeaderAuditID = "Audit-ID"
)

// Level 定义审计期间记录信息的多少
//
// 等级越高记录的信息越多，代价也越大；策略中按请求类别选择合适等级。
type Level string

// 合法的审计等级
const (
	// LevelNone 关闭审计。
	LevelNone Level = "None"
	// LevelMetadata 提供基本的审计等级。
	LevelMetadata Level = "Metadata"
	// LevelRequest 在 Metadata 等级的基础上，额外记录请求对象
	// （不适用于非资源类请求）。
	LevelRequest Level = "Request"
	// LevelRequestResponse 在 Request 等级的基础上，额外记录响应对象
	// （不适用于非资源类请求）。
	LevelRequestResponse Level = "RequestResponse"
)

// Stage 定义请求处理过程中可能产生审计事件的各个阶段。
//
// 同一次请求可能在不同阶段产生多条事件，通过 Stage 字段区分。
type Stage string

// 合法的审计阶段。
const (
	// StageRequestReceived：审计处理器一收到请求、尚未向下委派给处理器链
	// 之前生成的事件所处的阶段。
	StageRequestReceived Stage = "RequestReceived"
	// StageResponseStarted：响应头已发出、但响应体尚未发出时生成的事件
	// 所处的阶段。该阶段只对长时间运行的请求（例如 watch）生成。
	StageResponseStarted Stage = "ResponseStarted"
	// StageResponseComplete：响应体已写完、不会再发送任何字节时生成的
	// 事件所处的阶段。
	StageResponseComplete Stage = "ResponseComplete"
	// StagePanic：发生 panic 时生成的事件所处的阶段。
	StagePanic Stage = "Panic"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Event 捕获 API 审计日志中可以包含的全部信息。
//
// 这是审计事件的内部表示，最终会被转换为 audit.k8s.io/v1 等外部版本写出。
type Event struct {
	metav1.TypeMeta

	// 生成该事件时所用的审计等级
	Level Level

	// 唯一的审计 ID，为每个请求生成。
	AuditID types.UID
	// 生成该事件实例时请求处理所处的阶段。
	Stage Stage

	// RequestURI 是客户端发送给服务器的请求 URI。
	RequestURI string
	// Verb 是与该请求关联的 kubernetes 动词。
	// 对于非资源类请求，这里是小写化的 HTTP 方法名。
	Verb string
	// 已认证的用户信息。
	User authnv1.UserInfo
	// 被 impersonate（伪装）的用户信息。
	// +optional
	ImpersonatedUser *authnv1.UserInfo
	// AuthenticationMetadata 包含该请求如何被认证的细节。
	// +optional
	AuthenticationMetadata *AuthenticationMetadata

	// Source IPs：请求的来源 IP 及中间代理。这些 IP 按以下顺序列出：
	// 1. X-Forwarded-For 请求头中的 IP
	// 2. X-Real-Ip 头，若它未出现在 X-Forwarded-For 列表中
	// 3. 连接的远端地址，若它与至此列表中的最后一个 IP
	//    （X-Forwarded-For 或 X-Real-Ip）不一致。
	// 注意：除最后一个 IP 外，其余 IP 均可由客户端任意伪造。
	// +optional
	SourceIPs []string
	// UserAgent 记录客户端上报的 user agent 字符串。
	// 注意：UserAgent 由客户端提供，绝不可信任。
	// +optional
	UserAgent string
	// 该请求指向的对象引用。
	// 不适用于 List 类请求或非资源类请求。
	// +optional
	ObjectRef *ObjectReference
	// 响应状态，即使 ResponseObject 不是 Status 类型也会被填充。
	// 成功响应时只包含 Code 字段；对于非 Status 类型的错误响应，
	// 会自动填充错误的 Message。
	// +optional
	ResponseStatus *metav1.Status

	// 来自请求的 API 对象，JSON 格式。RequestObject 在版本转换、默认值填充、
	// 准入或合并之前按请求原样记录（可能重新编码为 JSON）。它是外部版本化的
	// 对象类型，单独看可能不是合法对象。非资源类请求会省略；仅在 Request
	// 等级及以上记录。
	// +optional
	RequestObject *runtime.Unknown
	// 响应中返回的 API 对象，JSON 格式。ResponseObject 在转换为外部类型之后
	// 记录并序列化为 JSON。非资源类请求会省略；仅在 Response 等级记录。
	// +optional
	ResponseObject *runtime.Unknown

	// 请求到达 apiserver 的时间。
	RequestReceivedTimestamp metav1.MicroTime
	// 请求到达当前审计阶段的时间。
	StageTimestamp metav1.MicroTime

	// Annotations 是与审计事件一起存储的非结构化键值映射，可由请求服务链中的
	// 插件（包括认证、授权和准入插件）设置。注意这些注解属于审计事件，与提交
	// 对象的 metadata.annotations 无关。键应当唯一标识产生信息的组件以避免名称
	// 冲突（例如 podsecuritypolicy.admission.k8s.io/policy）。值应当简短。
	// Annotations 会计入 Metadata 等级。
	// +optional
	Annotations map[string]string
}

// AuthenticationMetadata 记录认证阶段产生的附加信息，供审计与后续授权参考。
type AuthenticationMetadata struct {
	// ImpersonationConstraint 是与本审计事件关联的 ImpersonatedUser 在被授权时
	// 所用的受限 impersonation 模式对应的动词。仅在使用受限 impersonation 时设置。
	// +optional
	ImpersonationConstraint string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EventList 是审计 Event 的列表。
type EventList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Event
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Policy 定义审计日志的配置，以及不同类别请求如何被记录的规则。
//
// 策略中 Rules 严格有序，请求命中第一条匹配规则后即停止；没有任何规则命中时
// 采用等级 None（即不记录）。
type Policy struct {
	metav1.TypeMeta
	// 包含 ObjectMeta 以与 API 基础设施互操作。
	// +optional
	metav1.ObjectMeta

	// Rules 指定请求应被记录到的审计 Level。
	// 一个请求可能匹配多条规则，此时使用第一条（FIRST）匹配的规则。
	// 默认审计等级是 None，但可以用列表末尾的兜底（catch-all）规则覆盖。
	// PolicyRules 是严格有序的。
	Rules []PolicyRule

	// OmitStages 是不生成事件的阶段列表。注意它也可以按规则单独指定，
	// 此时两者（全局与规则级）的并集都会被省略。
	// +optional
	OmitStages []Stage

	// OmitManagedFields 指示是否把请求与响应体中的 managed fields 从
	// API 审计日志中省略掉。它作为全局默认值使用：取值 'true' 会省略
	// managed fields，否则 managed fields 会被写入 API 审计日志。
	// 注意它也可以按规则单独指定，此时规则中指定的值会覆盖全局默认值。
	// +optional
	OmitManagedFields bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PolicyList 是审计 Policy 的列表。
type PolicyList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Policy
}

// PolicyRule 基于元数据将请求映射到一个审计 Level。
// 请求必须匹配每个字段的规则（各字段规则求交集）。
//
// 各字段之间是“与”关系；单个字段内部的列表是“或”关系，空列表表示不限制。
// 资源型规则（Resources/Namespaces）与非资源型规则（NonResourceURLs）不能同时设置。
type PolicyRule struct {
	// 匹配本规则的请求被记录到的 Level。
	Level Level

	// 本规则适用的用户（按已认证用户名）。
	// 空列表表示所有用户。
	// +optional
	Users []string
	// 本规则适用的用户组。用户只要属于任一 UserGroups 即视为匹配。
	// 空列表表示所有用户组。
	// +optional
	UserGroups []string

	// 匹配本规则的动词。
	// 空列表表示所有动词。
	// +optional
	Verbs []string

	// 规则可以作用于 API 资源（如 "pods" 或 "secrets"）、非资源 URL 路径
	//（如 "/api"），或两者都不作用于，但不能同时作用于两者。
	// 若两者都未指定，则该规则被视为对所有 URL 的默认规则。

	// 本规则匹配的资源。空列表表示所有 API 组中的所有类型。
	// +optional
	Resources []GroupResources
	// 本规则匹配的命名空间。
	// 空字符串 "" 匹配非命名空间资源。
	// 空列表表示所有命名空间。
	// +optional
	Namespaces []string

	// NonResourceURLs 是需要审计的 URL 路径集合。
	// 允许使用 `*`，但只能作为路径的最后一个完整步骤。
	// 示例：
	//  `/metrics` - 记录 apiserver 指标的请求
	//  `/healthz*` - 记录所有健康检查
	// +optional
	NonResourceURLs []string

	// OmitStages 是不生成事件的阶段列表。注意它也可以在策略全局层面指定，
	// 此时两者的并集都会被省略。
	// 空列表表示不施加任何限制。
	// +optional
	OmitStages []Stage

	// OmitManagedFields 指示是否把请求与响应体中的 managed fields 从
	// API 审计日志中省略掉：
	// - 取值 'true' 会把 managed fields 从 API 审计日志中丢弃
	// - 取值 'false' 表示 managed fields 应当包含在 API 审计日志中
	// 注意本规则中指定的值（若有）会覆盖全局默认值；
	// 若未指定，则沿用 Policy.OmitManagedFields 中的全局默认值。
	// +optional
	OmitManagedFields *bool
}

// GroupResources 表示某个 API 组内的资源类型。
//
// 一个 GroupResources 项把 API 组与若干资源/子资源、实例名绑定在一起，
// 用于表达“某组下的某些资源”这一规则维度。
type GroupResources struct {
	// Group 是包含这些资源的 API 组名称。
	// 空字符串表示 core API 组。
	// `*` 匹配所有组
	// +optional
	Group string
	// Resources 是本规则适用的资源列表。
	//
	// 例如：
	// - `pods` 匹配 pods。
	// - `pods/log` 匹配 pods 的 log 子资源。
	// - `*` 匹配所有资源及其子资源。
	// - `pods/*` 匹配 pods 的所有子资源。
	// - `*/scale` 匹配所有的 scale 子资源。
	//
	// 若存在通配符，校验规则会确保各资源条目之间不互相重叠。
	//
	// 空列表表示本 API 组内所有资源及子资源都适用。
	// +optional
	Resources []string
	// ResourceNames 是策略匹配的资源实例名列表。
	// 使用本字段必须先指定 Resources。
	// 空列表表示匹配该资源的每一个实例。
	// +optional
	ResourceNames []string
}

// ObjectReference 包含足以检查或修改被引用对象的信息。
//
// 审计事件中只记录定位对象所需的坐标，不复制对象内容。
type ObjectReference struct {
	// +optional
	Resource string
	// +optional
	Namespace string
	// +optional
	Name string
	// +optional
	UID types.UID
	// APIGroup 是包含被引用对象的 API 组名称。
	// 空字符串表示 core API 组。
	// +optional
	APIGroup string
	// APIVersion 是包含被引用对象的 API 组的版本。
	// +optional
	APIVersion string
	// +optional
	ResourceVersion string
	// +optional
	Subresource string
}
