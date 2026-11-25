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

package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=apiregistration.k8s.io,v1,APIServiceList

// APIServiceList 是 APIService 对象的列表。
// 指令 +k8s:prerelease-lifecycle-gen:introduced=1.7 等几行记录本类型的生命周期：
// 1.7 引入、1.19 弃用，并指向 v1 版本的同类资源。生成器据此产出元数据，
// 客户端拿到 1.19 之前的版本时才知道该往哪里迁移。
// 这里用的是“对象列表”而不是简单切片，因为列表本身要携带 TypeMeta 和 ListMeta，
// 分页、资源版本这些信息都挂在它上面。
type APIServiceList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is the list of APIService
	Items []APIService `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ServiceReference 持有对 Service.legacy.k8s.io 的引用。
// 聚合层要访问后端时并不直接塞一个 IP，而是引用集群里的 Service，
// 这样后端重启、扩缩容都交给 Service 的端点机制处理。
// Port 用指针而不是 int32，是为了区分“没指定”和“指定端口 0”；
// 默认值逻辑只在前者上落 443，后者则留给校验去拒绝。
type ServiceReference struct {
	// Namespace is the namespace of the service
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,1,opt,name=namespace"`
	// Name is the name of the service
	Name string `json:"name,omitempty" protobuf:"bytes,2,opt,name=name"`
	// If specified, the port on the service that hosting webhook.
	// Default to 443 for backward compatibility.
	// `port` should be a valid port number (1-65535, inclusive).
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,3,opt,name=port"`
}

// APIServiceSpec 包含定位并与服务端通信所需的信息；仅支持 https，但可以关闭证书校验。
// 这个结构同时承担两件事：一是回答“这个 GroupVersion 由谁处理”，
// 二是回答“在 discovery 文档里它应该排在什么位置”。
// 前者靠 Service / CABundle，后者靠两个不同层级的优先级：
// GroupPriorityMinimum 决定整个组的位置，VersionPriority 决定组内版本顺序。
// 两个优先级都要求为正数，校验层会据此拒绝零值和负值。
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
	VersionPriority int32 `json:"versionPriority" protobuf:"varint,8,opt,name=versionPriority"`

	// 这里保留这一行，让所有人都记得 protobuf 编号 6 为什么被跳过：
	// Priority int64 `json:"priority" protobuf:"varint:6,opt,name=priority"`
	// 上面这行注释掉的字段不是遗漏，而是一块“墓碑”：编号 6 过去被占用过，
	// protobuf 的字段编号一旦用过就不能回收，否则旧数据会按新字段被误读。
	// 留在这里就是为了让人别顺手把 6 填回某个新字段上。
}

// ConditionStatus 表示某个条件的状态（true、false 或 unknown）。
// 用 string 而不是 bool，是为了给“目前判断不了”留一个明确的位置：
// 聚合层可能根本连不上后端，此时既不该说可用，也不该断言不可用。
// 这个类型在多个 API 组里都有同名定义，彼此独立，转换时靠显式映射而不是隐式相等。
type ConditionStatus string

// These are valid condition statuses. "ConditionTrue" means a resource is in the condition;
// "ConditionFalse" means a resource is not in the condition; "ConditionUnknown" means kubernetes
// can't decide if a resource is in the condition or not. In the future, we could add other
// intermediate conditions, e.g. ConditionDegraded.
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// APIServiceConditionType 是 APIServiceCondition.Type 的合法取值。
// 当前只有 Available 一种类型，但条件列表本身按“类型”组织而不是按位置，
// 就是为了日后加新条件时不必改动消费方读取条件的方式。
type APIServiceConditionType string

const (
	// Available indicates that the service exists and is reachable
	Available APIServiceConditionType = "Available"
)

// APIServiceCondition 描述某个时间点上 APIService 的状态。
// Reason 和 Message 分开存是有意的：Reason 是给程序用的短标识，
// Message 才是给人看的细节。控制器只在状态真正翻转时才刷新 LastTransitionTime，
// 这样“为什么当前是这个状态”和“这个状态持续了多久”都能回答。
type APIServiceCondition struct {
	// Type is the type of the condition.
	Type APIServiceConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=APIServiceConditionType"`
	// Status is the status of the condition.
	// Can be True, False, Unknown.
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// Last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// Unique, one-word, CamelCase reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// Human-readable message indicating details about last transition.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// APIServiceStatus 包含从 API 服务器派生出的信息。
// 状态字段带 patchMergeKey=type / listType=map 这一组标签，
// 指令 +patchMergeKey=type、指令 +listType=map、指令 +listMapKey=type 的含义是：
// 条件列表按 type 当主键做合并，而不是整体替换。这样多个控制器各写自己的条件时，
// 不会互相把对方的条件覆盖掉。
type APIServiceStatus struct {
	// Current service state of apiService.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []APIServiceCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=apiregistration.k8s.io,v1,APIService

// APIService 表示某个特定 GroupVersion 的服务器，Name 必须形如 “version.group”。
// 指令 +genclient:nonNamespaced 说明这是集群级资源：一个 GroupVersion 在全集群只能有一个入口，
// 放进 namespace 反而会让“谁是权威”变得含糊。名字必须是 version.group，
// 校验层会把这条约束真正执行一遍，避免配置和名字对不上。
// Spec 与 Status 分开是刻意的：前者由用户声明，后者由控制器回写，
// 两条更新路径互不干扰。
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
