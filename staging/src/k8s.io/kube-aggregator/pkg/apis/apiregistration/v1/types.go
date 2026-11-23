/*
Copyright 2016 The Kubernetes Authors.

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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// 指令 +k8s:prerelease-lifecycle-gen:introduced=1.10 记录该类型首次进入稳定版的版本，
// 供发布周期工具生成弃用/移除的元数据，不参与运行时行为。
// +k8s:prerelease-lifecycle-gen:introduced=1.10

// APIServiceList is a list of APIService objects.
// APIService 是集群级资源，列表同样不带命名空间；列表项直接内联对象而不是只放引用，
// 这样一次 LIST 就能拿到完整的注册信息。
type APIServiceList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is the list of APIService
	Items []APIService `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ServiceReference holds a reference to Service.legacy.k8s.io
type ServiceReference struct {
	// Namespace is the namespace of the service
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
	// Name is the name of the service
	Name string `json:"name,omitempty" protobuf:"bytes,2,opt,name=name"`
	// If specified, the port on the service that hosting webhook.
	// Default to 443 for backward compatibility.
	// `port` should be a valid port number (1-65535, inclusive).
	// 端口做成指针是为了区分“没填，走默认 443”和“显式填了 443”；
	// 这个区别在升级旧对象、校验通信目标时仍然可见。
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,3,opt,name=port"`
}

// APIServiceSpec contains information for locating and communicating with a server.
// Only https is supported, though you are able to disable certificate verification.
// 这一份 spec 同时承担两件事：声明某个 GroupVersion 由谁提供，以及决定它在
// 客户端发现结果中的排序；代理转发和发现聚合读的是同一份数据，避免两处配置漂移。
type APIServiceSpec struct {
	// Service is a reference to the service for this API server.  It must communicate
	// on port 443.
	// If the Service is nil, that means the handling for the API groupversion is handled locally on this server.
	// The call will simply delegate to the normal handler chain to be fulfilled.
	// +optional
	Service *ServiceReference `json:"service,omitempty" protobuf:"bytes,1,opt,name=service"`
	// Group is the API group name this server hosts
	Group string `json:"group,omitempty" protobuf:"bytes,2,opt,name=group"`
	// Version is the API version this server hosts.  For example, "v1"
	Version string `json:"version,omitempty" protobuf:"bytes,3,opt,name=version"`

	// InsecureSkipTLSVerify disables TLS certificate verification when communicating with this server.
	// This is strongly discouraged.  You should use the CABundle instead.
	// 这是布尔开关而不是可选指针，因为“未设置”与“显式 false”的语义相同：
	// 缺省就验证书，要放宽只能主动打开，免得默认配置意外降级安全性。
	InsecureSkipTLSVerify bool `json:"insecureSkipTLSVerify,omitempty" protobuf:"varint,4,opt,name=insecureSkipTLSVerify"`
	// CABundle is a PEM encoded CA bundle which will be used to validate an API server's serving certificate.
	// If unspecified, system trust roots on the apiserver are used.
	// +listType=atomic
	// +optional
	CABundle []byte `json:"caBundle,omitempty" protobuf:"bytes,5,opt,name=caBundle"`

	// GroupPriorityMinimum is the priority this group should have at least. Higher priority means that the group is preferred by clients over lower priority ones.
	// Note that other versions of this group might specify even higher GroupPriorityMinimum values such that the whole group gets a higher priority.
	// The primary sort is based on GroupPriorityMinimum, ordered highest number to lowest (20 before 10).
	// The secondary sort is based on the alphabetical comparison of the name of the object.  (v1.bar before v1.foo)
	// We'd recommend something like: *.k8s.io (except extensions) at 18000 and
	// PaaSes (OpenShift, Deis) are recommended to be in the 2000s
	// 用“最低优先级”而不是固定值，是因为同一 GroupVersion 可以用多个 APIService
	// 逐个抬高组的优先级；跨组排序只看组级结果，具体的版本权重由
	// VersionPriority 决定，两级排序让单个 APIService 不必理解整组的优先级。
	GroupPriorityMinimum int32 `json:"groupPriorityMinimum" protobuf:"varint,7,opt,name=groupPriorityMinimum"`

	// VersionPriority controls the ordering of this API version inside of its group.  Must be greater than zero.
	// The primary sort is based on VersionPriority, ordered highest to lowest (20 before 10).
	// Since it's inside of a group, the number can be small, probably in the 10s.
	// In case of equal version priorities, the version string will be used to compute the order inside a group.
	// If the version string is "kube-like", it will sort above non "kube-like" version strings, which are ordered
	// lexicographically. "Kube-like" versions start with a "v", then are followed by a number (the major version),
	// then optionally the string "alpha" or "beta" and another number (the minor version). These are sorted first
	// by GA > beta > alpha (where GA is a version with no suffix such as beta or alpha), and then by comparing major
	// version, then minor version. An example sorted list of versions:
	// v10, v2, v1, v11beta2, v10beta3, v3beta1, v12alpha1, v11alpha2, foo1, foo10.
	// 组内排序不直接比较版本字符串：若按字典序，v10 会排在 v2 前面。
	// 所以先比数值优先级，相等时才按 kube 风格版本号（GA > beta > alpha，再比主次版本）
	// 排序，最后才退回字典序，保证稳定版本天然排在测试版本前面。
	VersionPriority int32 `json:"versionPriority" protobuf:"varint,8,opt,name=versionPriority"`

	// leaving this here so everyone remembers why proto index 6 is skipped
	// 已经被 GroupPriorityMinimum / VersionPriority 取代的旧单一优先级字段。
	// 注释掉而不是删掉，是为了给后来人留下 protobuf 编号 6 的空缺说明：
	// 线协议里的字段号一旦用过就不能回收复用。
	// Priority int64 `json:"priority" protobuf:"varint,6,opt,name=priority"`
}

// ConditionStatus indicates the status of a condition (true, false, or unknown).
// 用字符串三态而不是 bool，是为了区分“探测失败”和“还没探测出结果”：
// 后者不能当作不可用直接切断流量，控制器可能还没来得及更新状态。
type ConditionStatus string

// These are valid condition statuses. "ConditionTrue" means a resource is in the condition;
// "ConditionFalse" means a resource is not in the condition; "ConditionUnknown" means kubernetes
// can't decide if a resource is in the condition or not. In the future, we could add other
// intermediate conditions, e.g. ConditionDegraded.
// 三态常量刻意留出扩展空间：将来要表达 degraded 之类的中间状态，
// 不必改动现有消费者的类型定义。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// APIServiceConditionType is a valid value for APIServiceCondition.Type
type APIServiceConditionType string

const (
	// Available indicates that the service exists and is reachable
	// 目前只定义一种条件类型：聚合层唯一需要对外报告的健康信号就是后端可达性。
	// 保留 Type 字段而不是用单一状态字段，是给以后加入其他条件留位置。
	Available APIServiceConditionType = "Available"
)

// APIServiceCondition describes the state of an APIService at a particular point
type APIServiceCondition struct {
	// Type is the type of the condition.
	Type APIServiceConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=APIServiceConditionType"`
	// Status is the status of the condition.
	// Can be True, False, Unknown.
	// 状态、原因和消息分开存：机器只读 Status 做判断，人和排障工具读 Reason/Message，
	// 避免把面向人的文本塞进需要稳定取值的枚举字段。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// Last time the condition transitioned from one status to another.
	// 只在状态真正翻转时更新，不是每次健康检查都刷新；否则控制器无法判断
	// 故障持续了多久，客户端也会因为元数据频繁变化而不断重新拉取。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// Unique, one-word, CamelCase reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// Human-readable message indicating details about last transition.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// APIServiceStatus contains derived information about an API server
// 状态是控制器写、用户读的派生信息：API 服务器不根据它决定请求路由，
// 改动状态也不会改变后端选择。把它与 spec 分开，才能让状态更新走独立的
// /status 子资源，避免覆盖用户提交的 spec。
type APIServiceStatus struct {
	// Current service state of apiService.
	// +optional
	// 指令 +patchMergeKey=type / +patchStrategy=merge / +listType=map / +listMapKey=type
	// 合起来的意思是：conditions 按 Type 做合并而不是整体替换，
	// 这样多个控制器各更新自己关心的条件时不会互相抹掉对方的结果。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []APIServiceCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// 指令 +genclient:nonNamespaced 说明 APIService 是集群级资源，生成的 client
// 不提供命名空间参数；这与对象名直接编码 GroupVersion 的设计相配套。
// 指令 +k8s:deepcopy-gen:interfaces=... 让生成代码保证该类型实现 runtime.Object，
// 从而能放进 scheme 和 etcd 存储。
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.10

// APIService represents a server for a particular GroupVersion.
// Name must be "version.group".
// 名字里同时带版本和组，是因为一个 GroupVersion 只能有一个注册提供者：
// 名字即主键，冲突在创建对象时就被唯一性约束挡住，不需要额外的索引判断。
type APIService struct {
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec contains information for locating and communicating with a server
	Spec APIServiceSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// Status contains derived information about an API server
	Status APIServiceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}
