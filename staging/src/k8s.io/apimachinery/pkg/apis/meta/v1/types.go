/*
Copyright 2015 The Kubernetes Authors.

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

// Package v1 contains API types that are common to all versions.
//
// The package contains two categories of types:
//   - external (serialized) types that lack their own version (e.g TypeMeta)
//   - internal (never-serialized) types that are needed by several different
//     api groups, and so live here, to avoid duplication and/or import loops
//     (e.g. LabelSelector).
//
// In the future, we will probably move these categories of objects into
// separate packages.
//
// Package v1 包含所有版本通用的 API 类型，分为两类：
//   - 没有独立版本的外部（可序列化）类型，例如 TypeMeta；
//   - 多个 API 组都需要、但从不序列化的内部类型，例如 LabelSelector，
//     放在这里以避免重复定义或产生循环导入。
package v1

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// TypeMeta describes an individual object in an API response or request
// with strings representing the type of the object and its API schema version.
// Structures that are versioned or persisted should inline TypeMeta.
//
// TypeMeta 用字符串描述 API 响应或请求中单个对象的类型及其 API schema 版本。
// 需要版本化或持久化的结构体应内嵌 TypeMeta。
//
// +k8s:deepcopy-gen=false
type TypeMeta struct {
	// Kind is a string value representing the REST resource this object represents.
	// Servers may infer this from the endpoint the client submits requests to.
	// Cannot be updated.
	// In CamelCase.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// Kind 表示该对象对应的 REST 资源，采用 CamelCase；服务器可根据请求端点推断，不可更新。
	Kind string `json:"kind,omitempty" protobuf:"bytes,1,opt,name=kind"`

	// APIVersion defines the versioned schema of this representation of an object.
	// Servers should convert recognized schemas to the latest internal value, and
	// may reject unrecognized values.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources
	// +optional
	//
	// APIVersion 定义该对象表示的版本化 schema（形如 group/version）；
	// 服务器应把可识别的 schema 转换到最新内部版本，并可能拒绝无法识别的值。
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,2,opt,name=apiVersion"`
}

// ListMeta describes metadata that synthetic resources must have, including lists and
// various status objects. A resource may have only one of {ObjectMeta, ListMeta}.
//
// ListMeta 描述列表及各类 status 等合成资源必须具备的元数据。
// 一个资源只能拥有 ObjectMeta 与 ListMeta 之一。
type ListMeta struct {
	// Deprecated: selfLink is a legacy read-only field that is no longer populated by the system.
	// +optional
	//
	// 已弃用：selfLink 是遗留的只读字段，系统已不再填充。
	SelfLink string `json:"selfLink,omitempty" protobuf:"bytes,1,opt,name=selfLink"`

	// String that identifies the server's internal version of this object that
	// can be used by clients to determine when objects have changed.
	// Value must be treated as opaque by clients and passed unmodified back to the server.
	// Populated by the system.
	// Read-only.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
	// +optional
	//
	// ResourceVersion 标识服务器端该对象的内部版本，客户端据此判断对象是否变化。
	// 客户端必须把它视为不透明值，并原样传回服务器；由系统填充，只读。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`

	// continue may be set if the user set a limit on the number of items returned, and indicates that
	// the server has more data available. The value is opaque and may be used to issue another request
	// to the endpoint that served this list to retrieve the next set of available objects. Continuing a
	// consistent list may not be possible if the server configuration has changed or more than a few
	// minutes have passed. The resourceVersion field returned when using this continue value will be
	// identical to the value in the first response, unless you have received this token from an error
	// message.
	//
	// Continue 仅在请求设置了 limit 且服务器仍有数据时设置，表示还有后续数据。
	// 该值不透明，可用于向同一端点发起下一次请求以获取后续对象；
	// 若服务器配置变化或超过数分钟，可能无法继续一致的列表；
	// 使用该 continue 值时返回的 resourceVersion 与首次响应相同（除非 token 来自错误信息）。
	Continue string `json:"continue,omitempty" protobuf:"bytes,3,opt,name=continue"`

	// remainingItemCount is the number of subsequent items in the list which are not included in this
	// list response. If the list request contained label or field selectors, then the number of
	// remaining items is unknown and the field will be left unset and omitted during serialization.
	// If the list is complete (either because it is not chunking or because this is the last chunk),
	// then there are no more remaining items and this field will be left unset and omitted during
	// serialization.
	// Servers older than v1.15 do not set this field.
	// The intended use of the remainingItemCount is *estimating* the size of a collection. Clients
	// should not rely on the remainingItemCount to be set or to be exact.
	// +optional
	//
	// RemainingItemCount 表示本响应之外后续条目的数量。若请求带 label/field selector，
	// 该值未知，会被留空并省略；列表已完整时同样留空省略。
	// v1.15 之前的服务器不会设置该字段。它只用于 *估算* 集合大小，客户端不应依赖其存在或精确性。
	RemainingItemCount *int64 `json:"remainingItemCount,omitempty" protobuf:"bytes,4,opt,name=remainingItemCount"`

	// shardInfo is set when the list is a filtered subset of the full collection,
	// as selected by a shard selector on the request. It echoes back the selector
	// so clients can verify which shard they received and merge sharded responses.
	// Clients should not cache sharded list responses as a full representation
	// of the collection.
	//
	// This is an alpha field and requires enabling the ShardedListAndWatch feature gate.
	// +featureGate=ShardedListAndWatch
	// +optional
	//
	// ShardInfo 在列表是完整集合按请求中的 shard selector 过滤后的子集时设置，
	// 回显 selector 以便客户端确认收到哪个分片并合并各分片响应。
	// 客户端不应把分片列表响应当作集合的完整表示来缓存。
	// 该字段为 alpha，需要开启 ShardedListAndWatch feature gate。
	ShardInfo *ShardInfo `json:"shardInfo,omitempty" protobuf:"bytes,5,opt,name=shardInfo"`
}

// ShardInfo describes the shard selector that was applied to produce a list response.
// Its presence on a list response indicates the list is a filtered subset.
//
// ShardInfo 描述生成列表响应时所用的 shard selector；
// 列表响应中出现该字段即表示这是一个过滤后的子集。
type ShardInfo struct {
	// selector is the shard selector string from the request, echoed back so clients
	// can verify which shard they received and merge responses from multiple shards.
	// +required
	//
	// Selector 是请求中的 shard selector 字符串，回显给客户端以便校验分片并合并多个分片的响应。
	Selector string `json:"selector" protobuf:"bytes,1,opt,name=selector"`
}

// Field path constants that are specific to the internal API
// representation.
const (
	// ObjectNameField 是内部 API 表示中对象名称的字段路径。
	ObjectNameField = "metadata.name"
)

// These are internal finalizer values for Kubernetes-like APIs, must be qualified name unless defined here
const (
	// FinalizerOrphanDependents 表示删除时孤立依赖对象。
	FinalizerOrphanDependents = "orphan"
	// FinalizerDeleteDependents 表示删除时前台级联删除依赖对象。
	FinalizerDeleteDependents = "foregroundDeletion"
)

// ObjectMeta is metadata that all persisted resources must have, which includes all objects
// users must create.
//
// ObjectMeta 是所有持久化资源（即用户创建的所有对象）必须具备的元数据。
type ObjectMeta struct {
	// Name must be unique within a namespace. Is required when creating resources, although
	// some resources may allow a client to request the generation of an appropriate name
	// automatically. Name is primarily intended for creation idempotence and configuration
	// definition.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names
	// +optional
	//
	// Name 在 namespace 内必须唯一，创建资源时通常必填（部分资源允许客户端请求自动生成）。
	// 主要用于创建幂等性与配置定义，不可更新。
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// GenerateName is an optional prefix, used by the server, to generate a unique
	// name ONLY IF the Name field has not been provided.
	// If this field is used, the name returned to the client will be different
	// than the name passed. This value will also be combined with a unique suffix.
	// The provided value has the same validation rules as the Name field,
	// and may be truncated by the length of the suffix required to make the value
	// unique on the server.
	//
	// If this field is specified and the generated name exists, the server will return a 409.
	//
	// Applied only if Name is not specified.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#idempotency
	// +optional
	//
	// GenerateName 是服务器用于生成唯一名称的可选前缀，仅在未提供 Name 时生效。
	// 使用后返回给客户端的名称与传入值不同，会附加唯一后缀；校验规则同 Name，
	// 并可能按服务器所需后缀长度被截断。若生成的名称已存在，服务器返回 409。
	GenerateName string `json:"generateName,omitempty" protobuf:"bytes,2,opt,name=generateName"`

	// Namespace defines the space within which each name must be unique. An empty namespace is
	// equivalent to the "default" namespace, but "default" is the canonical representation.
	// Not all objects are required to be scoped to a namespace - the value of this field for
	// those objects will be empty.
	//
	// Must be a DNS_LABEL.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces
	// +optional
	//
	// Namespace 定义名称必须唯一的空间。空 namespace 等价于 "default"，但 "default" 是规范表示。
	// 并非所有对象都按 namespace 划分，这类对象的该字段为空。必须是 DNS_LABEL，不可更新。
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`

	// Deprecated: selfLink is a legacy read-only field that is no longer populated by the system.
	// +optional
	//
	// 已弃用：selfLink 是遗留的只读字段，系统已不再填充。
	SelfLink string `json:"selfLink,omitempty" protobuf:"bytes,4,opt,name=selfLink"`

	// UID is the unique in time and space value for this object. It is typically generated by
	// the server on successful creation of a resource and is not allowed to change on PUT
	// operations.
	//
	// Populated by the system.
	// Read-only.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#uids
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	//
	// UID 是该对象在时间与空间上唯一的标识，通常由服务器在创建成功时生成，PUT 时不允许修改。
	// 由系统填充，只读。
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,5,opt,name=uid,casttype=k8s.io/kubernetes/pkg/types.UID"`

	// An opaque value that represents the internal version of this object that can
	// be used by clients to determine when objects have changed. May be used for optimistic
	// concurrency, change detection, and the watch operation on a resource or set of resources.
	// Clients must treat these values as opaque and passed unmodified back to the server.
	// They may only be valid for a particular resource or set of resources.
	//
	// Populated by the system.
	// Read-only.
	// Value must be treated as opaque by clients and .
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
	// +optional
	//
	// ResourceVersion 是表示对象内部版本的不透明值，客户端据此判断对象何时变化，
	// 可用于乐观并发、变更检测以及 watch。客户端必须视为不透明并原样传回，
	// 且它可能只对特定资源有效。由系统填充，只读。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,6,opt,name=resourceVersion"`

	// A sequence number representing a specific generation of the desired state.
	// Populated by the system. Read-only.
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:minimum=0
	//
	// Generation 表示期望状态特定代际的序列号，由系统填充，只读。
	Generation int64 `json:"generation,omitempty" protobuf:"varint,7,opt,name=generation"`

	// CreationTimestamp is a timestamp representing the server time when this object was
	// created. It is not guaranteed to be set in happens-before order across separate operations.
	// Clients may not set this value. It is represented in RFC3339 form and is in UTC.
	//
	// Populated by the system.
	// Read-only.
	// Null for lists.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	//
	// CreationTimestamp 表示对象创建时的服务器时间（RFC3339、UTC）。
	// 不保证跨操作满足 happens-before 顺序；客户端不可设置；列表对象为 null。
	// 由系统填充，只读。
	CreationTimestamp Time `json:"creationTimestamp,omitempty,omitzero" protobuf:"bytes,8,opt,name=creationTimestamp"`

	// DeletionTimestamp is RFC 3339 date and time at which this resource will be deleted. This
	// field is set by the server when a graceful deletion is requested by the user, and is not
	// directly settable by a client. The resource is expected to be deleted (no longer visible
	// from resource lists, and not reachable by name) after the time in this field, once the
	// finalizers list is empty. As long as the finalizers list contains items, deletion is blocked.
	// Once the deletionTimestamp is set, this value may not be unset or be set further into the
	// future, although it may be shortened or the resource may be deleted prior to this time.
	// For example, a user may request that a pod is deleted in 30 seconds. The Kubelet will react
	// by sending a graceful termination signal to the containers in the pod. After that 30 seconds,
	// the Kubelet will send a hard termination signal (SIGKILL) to the container and after cleanup,
	// remove the pod from the API. In the presence of network partitions, this object may still
	// exist after this timestamp, until an administrator or automated process can determine the
	// resource is fully terminated.
	// If not set, graceful deletion of the object has not been requested.
	//
	// Populated by the system when a graceful deletion is requested.
	// Read-only.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	//
	// DeletionTimestamp 是资源将被删除的 RFC3339 时间，由服务器在用户请求优雅删除时设置，
	// 客户端不能直接设置。finalizers 为空后，到达该时间资源才会真正删除；
	// 只要 finalizers 非空，删除就被阻塞。该值设置后不可清除或推迟，只能提前或在此之前删除；
	// 网络分区时对象可能在该时间之后仍然存在。未设置表示未请求优雅删除。
	// 由系统填充，只读。
	DeletionTimestamp *Time `json:"deletionTimestamp,omitempty" protobuf:"bytes,9,opt,name=deletionTimestamp"`

	// Number of seconds allowed for this object to gracefully terminate before
	// it will be removed from the system. Only set when deletionTimestamp is also set.
	// May only be shortened.
	// Read-only.
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	//
	// DeletionGracePeriodSeconds 是对象被移除前允许优雅终止的秒数，仅在 deletionTimestamp 设置时存在；
	// 只能缩短，只读。
	DeletionGracePeriodSeconds *int64 `json:"deletionGracePeriodSeconds,omitempty" protobuf:"varint,10,opt,name=deletionGracePeriodSeconds"`

	// Map of string keys and values that can be used to organize and categorize
	// (scope and select) objects. May match selectors of replication controllers
	// and services.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels
	// +optional
	//
	// Labels 是用于组织和分类（限定与选择）对象的字符串键值映射，
	// 可被 replication controller 与 service 的选择器匹配。
	Labels map[string]string `json:"labels,omitempty" protobuf:"bytes,11,rep,name=labels"`

	// Annotations is an unstructured key value map stored with a resource that may be
	// set by external tools to store and retrieve arbitrary metadata. They are not
	// queryable and should be preserved when modifying objects.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations
	// +optional
	//
	// Annotations 是随资源存储的非结构化键值映射，可由外部工具写入任意元数据。
	// 它不可查询，修改对象时应原样保留。
	Annotations map[string]string `json:"annotations,omitempty" protobuf:"bytes,12,rep,name=annotations"`

	// List of objects depended by this object. If ALL objects in the list have
	// been deleted, this object will be garbage collected. If this object is managed by a controller,
	// then an entry in this list will point to this controller, with the controller field set to true.
	// There cannot be more than one managing controller.
	// +optional
	// +patchMergeKey=uid
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=uid
	// +k8s:alpha(since:"1.37")=+k8s:optional
	//
	// OwnerReferences 列出本对象依赖的对象；当列表中的对象全部删除后，本对象会被垃圾回收。
	// 若本对象由控制器管理，列表中会有一项指向该控制器且 controller 为 true；
	// 最多只能有一个管理控制器。
	OwnerReferences []OwnerReference `json:"ownerReferences,omitempty" patchStrategy:"merge" patchMergeKey:"uid" protobuf:"bytes,13,rep,name=ownerReferences"`

	// Must be empty before the object is deleted from the registry. Each entry
	// is an identifier for the responsible component that will remove the entry
	// from the list. If the deletionTimestamp of the object is non-nil, entries
	// in this list can only be removed.
	// Finalizers may be processed and removed in any order.  Order is NOT enforced
	// because it introduces significant risk of stuck finalizers.
	// finalizers is a shared field, any actor with permission can reorder it.
	// If the finalizer list is processed in order, then this can lead to a situation
	// in which the component responsible for the first finalizer in the list is
	// waiting for a signal (field value, external system, or other) produced by a
	// component responsible for a finalizer later in the list, resulting in a deadlock.
	// Without enforced ordering finalizers are free to order amongst themselves and
	// are not vulnerable to ordering changes in the list.
	// +optional
	// +patchStrategy=merge
	// +listType=set
	//
	// Finalizers 必须在对象从 registry 删除前清空。每项标识负责把它从列表中移除的组件。
	// deletionTimestamp 非空后，只能从中移除条目，不能再添加。
	// finalizer 的处理顺序不作强制（顺序处理会带来卡死风险）：finalizers 是共享字段，
	// 有权限的参与者都可以重排；若按顺序处理，靠前的 finalizer 可能等待靠后 finalizer
	// 所在组件产生的信号，从而死锁。不强制顺序后，各 finalizer 可自行排序，也不受列表重排影响。
	Finalizers []string `json:"finalizers,omitempty" patchStrategy:"merge" protobuf:"bytes,14,rep,name=finalizers"`

	// Tombstone: ClusterName was a legacy field that was always cleared by
	// the system and never used.
	// ClusterName string `json:"clusterName,omitempty" protobuf:"bytes,15,opt,name=clusterName"`
	//
	// 墓碑：ClusterName 是遗留字段，系统总是清空且从未使用。

	// ManagedFields maps workflow-id and version to the set of fields
	// that are managed by that workflow. This is mostly for internal
	// housekeeping, and users typically shouldn't need to set or
	// understand this field. A workflow can be the user's name, a
	// controller's name, or the name of a specific apply path like
	// "ci-cd". The set of fields is always in the version that the
	// workflow used when modifying the object.
	//
	// +optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:optional
	//
	// ManagedFields 把 workflow-id 与版本映射到该 workflow 管理的字段集合，
	// 主要供内部维护，用户通常无需设置或理解。workflow 可以是用户名、控制器名，
	// 或 "ci-cd" 之类的 apply 路径名。字段集合始终以该 workflow 修改对象时使用的版本表示。
	ManagedFields []ManagedFieldsEntry `json:"managedFields,omitempty" protobuf:"bytes,17,rep,name=managedFields"`
}

const (
	// NamespaceDefault 表示对象位于 default namespace，客户端未指定时使用。
	NamespaceDefault = "default"
	// NamespaceAll 是列出或过滤所有 namespace 资源时传给 context 的默认参数。
	NamespaceAll = ""
	// NamespaceNone 是无 namespace 场景下传给 context 的参数。
	NamespaceNone = ""
	// NamespaceSystem 是部署系统组件的 namespace。
	NamespaceSystem = "kube-system"
	// NamespacePublic 是存放公开信息（ConfigMap）的 namespace。
	NamespacePublic = "kube-public"
)

// OwnerReference contains enough information to let you identify an owning
// object. An owning object must be in the same namespace as the dependent, or
// be cluster-scoped, so there is no namespace field.
//
// OwnerReference 包含识别属主对象所需的信息。属主必须与被依赖对象处于同一 namespace，
// 或者本身是集群级的，因此没有 namespace 字段。
//
// +structType=atomic
type OwnerReference struct {
	// API version of the referent.
	// +k8s:alpha(since:"1.37")=+k8s:required
	//
	// APIVersion 是被引用对象的 API 版本。
	APIVersion string `json:"apiVersion" protobuf:"bytes,5,opt,name=apiVersion"`
	// Kind of the referent.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +k8s:alpha(since:"1.37")=+k8s:required
	//
	// Kind 是被引用对象的类型。
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names
	// +k8s:alpha(since:"1.37")=+k8s:required
	//
	// Name 是被引用对象的名称。
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// UID of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#uids
	// +k8s:alpha(since:"1.37")=+k8s:required
	//
	// UID 是被引用对象的 UID。
	UID types.UID `json:"uid" protobuf:"bytes,4,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// If true, this reference points to the managing controller.
	// +optional
	//
	// Controller 为 true 时表示该引用指向管理控制器。
	Controller *bool `json:"controller,omitempty" protobuf:"varint,6,opt,name=controller"`
	// If true, AND if the owner has the "foregroundDeletion" finalizer, then
	// the owner cannot be deleted from the key-value store until this
	// reference is removed.
	// See https://kubernetes.io/docs/concepts/architecture/garbage-collection/#foreground-deletion
	// for how the garbage collector interacts with this field and enforces the foreground deletion.
	// Defaults to false.
	// To set this field, a user needs "delete" permission of the owner,
	// otherwise 422 (Unprocessable Entity) will be returned.
	// +optional
	//
	// BlockOwnerDeletion 为 true 且属主带有 "foregroundDeletion" finalizer 时，
	// 在该引用被移除前属主无法从键值存储中删除（用于前台级联删除）。默认为 false。
	// 设置该字段需要属主的 "delete" 权限，否则返回 422。
	BlockOwnerDeletion *bool `json:"blockOwnerDeletion,omitempty" protobuf:"varint,7,opt,name=blockOwnerDeletion"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ListOptions is the query options to a standard REST list call.
//
// ListOptions 是标准 REST list 调用的查询选项。
type ListOptions struct {
	TypeMeta `json:""`

	// A selector to restrict the list of returned objects by their labels.
	// Defaults to everything.
	// +optional
	//
	// LabelSelector 按标签过滤返回的对象，默认为全部。
	LabelSelector string `json:"labelSelector,omitempty" protobuf:"bytes,1,opt,name=labelSelector"`
	// A selector to restrict the list of returned objects by their fields.
	// Defaults to everything.
	// +optional
	//
	// FieldSelector 按字段过滤返回的对象，默认为全部。
	FieldSelector string `json:"fieldSelector,omitempty" protobuf:"bytes,2,opt,name=fieldSelector"`

	// +k8s:deprecated=includeUninitialized,protobuf=6

	// Watch for changes to the described resources and return them as a stream of
	// add, update, and remove notifications. Specify resourceVersion.
	// +optional
	//
	// Watch 为 true 时以 add/update/remove 事件流的形式监听资源变化，需配合 resourceVersion 使用。
	Watch bool `json:"watch,omitempty" protobuf:"varint,3,opt,name=watch"`
	// allowWatchBookmarks requests watch events with type "BOOKMARK".
	// Servers that do not implement bookmarks may ignore this flag and
	// bookmarks are sent at the server's discretion. Clients should not
	// assume bookmarks are returned at any specific interval, nor may they
	// assume the server will send any BOOKMARK event during a session.
	// If this is not a watch, this field is ignored.
	// +optional
	//
	// AllowWatchBookmarks 请求类型为 "BOOKMARK" 的 watch 事件。未实现 bookmarks 的服务器可忽略此标志；
	// 客户端不应假定会以固定间隔收到 bookmark，也不应假定会话期间一定会有 BOOKMARK 事件。
	// 非 watch 请求会忽略该字段。
	AllowWatchBookmarks bool `json:"allowWatchBookmarks,omitempty" protobuf:"varint,9,opt,name=allowWatchBookmarks"`

	// resourceVersion sets a constraint on what resource versions a request may be served from.
	// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
	// details.
	//
	// Defaults to unset
	// +optional
	//
	// ResourceVersion 约束请求可被服务的资源版本范围，详见官方文档；默认未设置。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,4,opt,name=resourceVersion"`

	// resourceVersionMatch determines how resourceVersion is applied to list calls.
	// It is highly recommended that resourceVersionMatch be set for list calls where
	// resourceVersion is set
	// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
	// details.
	//
	// Defaults to unset
	// +optional
	//
	// ResourceVersionMatch 决定 list 调用中 resourceVersion 的应用方式。
	// 设置了 resourceVersion 的 list 请求强烈建议同时设置该字段；默认未设置。
	ResourceVersionMatch ResourceVersionMatch `json:"resourceVersionMatch,omitempty" protobuf:"bytes,10,opt,name=resourceVersionMatch,casttype=ResourceVersionMatch"`
	// Timeout for the list/watch call.
	// This limits the duration of the call, regardless of any activity or inactivity.
	// +optional
	//
	// TimeoutSeconds 限制 list/watch 调用的时长，无论期间是否有活动。
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty" protobuf:"varint,5,opt,name=timeoutSeconds"`

	// limit is a maximum number of responses to return for a list call. If more items exist, the
	// server will set the `continue` field on the list metadata to a value that can be used with the
	// same initial query to retrieve the next set of results. Setting a limit may return fewer than
	// the requested amount of items (up to zero items) in the event all requested objects are
	// filtered out and clients should only use the presence of the continue field to determine whether
	// more results are available. Servers may choose not to support the limit argument and will return
	// all of the available results. If limit is specified and the continue field is empty, clients may
	// assume that no more results are available. This field is not supported if watch is true.
	//
	// The server guarantees that the objects returned when using continue will be identical to issuing
	// a single list call without a limit - that is, no objects created, modified, or deleted after the
	// first request is issued will be included in any subsequent continued requests. This is sometimes
	// referred to as a consistent snapshot, and ensures that a client that is using limit to receive
	// smaller chunks of a very large result can ensure they see all possible objects. If objects are
	// updated during a chunked list the version of the object that was present at the time the first list
	// result was calculated is returned.
	//
	// Limit 是 list 调用最多返回的条目数。若还有更多条目，服务器会在列表元数据中设置 `continue`，
	// 客户端可用它配合相同查询取下一批结果。设置 limit 后可能返回少于请求的数量（甚至为零，
	// 例如对象都被过滤掉），客户端应仅凭 continue 是否存在判断是否还有后续结果。
	// 服务器可以不支持 limit 而返回全部结果。limit 有值但 continue 为空时，客户端可认为没有更多结果；
	// watch 为 true 时不支持该字段。
	// 服务器保证使用 continue 返回的对象与一次性无 limit 的 list 完全一致：
	// 首次请求之后创建/修改/删除的对象不会出现在后续请求中（一致快照）。
	// 若分块期间对象被更新，返回的是首次 list 计算时的版本。
	Limit int64 `json:"limit,omitempty" protobuf:"varint,7,opt,name=limit"`
	// The continue option should be set when retrieving more results from the server. Since this value is
	// server defined, clients may only use the continue value from a previous query result with identical
	// query parameters (except for the value of continue) and the server may reject a continue value it
	// does not recognize. If the specified continue value is no longer valid whether due to expiration
	// (generally five to fifteen minutes) or a configuration change on the server, the server will
	// respond with a 410 ResourceExpired error together with a continue token. If the client needs a
	// consistent list, it must restart their list without the continue field. Otherwise, the client may
	// send another list request with the token received with the 410 error, the server will respond with
	// a list starting from the next key, but from the latest snapshot, which is inconsistent from the
	// previous list results - objects that are created, modified, or deleted after the first list request
	// will be included in the response, as long as their keys are after the "next key".
	//
	// This field is not supported when watch is true. Clients may start a watch from the last
	// resourceVersion value returned by the server and not miss any modifications.
	//
	// Continue 用于向服务器获取后续结果。该值由服务器定义，客户端只能把上一次查询结果中的
	// continue 配合完全相同的查询参数（continue 本身除外）使用，服务器可拒绝无法识别的值。
	// 若 continue 因过期（通常 5-15 分钟）或服务器配置变化失效，服务器会返回 410 ResourceExpired
	// 并附带新的 continue token：需要一致列表的客户端必须去掉 continue 重新开始；
	// 否则可用 410 中的 token 继续，服务器将从最新快照的下一 key 开始返回，
	// 与之前结果不再一致（首次 list 之后创建/修改/删除且 key 在 "next key" 之后的对象会被包含）。
	// watch 为 true 时不支持该字段；客户端可以从服务器返回的最后一个 resourceVersion 开始 watch 以避免遗漏。
	Continue string `json:"continue,omitempty" protobuf:"bytes,8,opt,name=continue"`

	// `sendInitialEvents=true` may be set together with `watch=true`.
	// In that case, the watch stream will begin with synthetic events to
	// produce the current state of objects in the collection. Once all such
	// events have been sent, a synthetic "Bookmark" event  will be sent.
	// The bookmark will report the ResourceVersion (RV) corresponding to the
	// set of objects, and be marked with `"k8s.io/initial-events-end": "true"` annotation.
	// Afterwards, the watch stream will proceed as usual, sending watch events
	// corresponding to changes (subsequent to the RV) to objects watched.
	//
	// When `sendInitialEvents` option is set, we require `resourceVersionMatch`
	// option to also be set. The semantic of the watch request is as following:
	// - `resourceVersionMatch` = NotOlderThan
	//   is interpreted as "data at least as new as the provided `resourceVersion`"
	//   and the bookmark event is send when the state is synced
	//   to a `resourceVersion` at least as fresh as the one provided by the ListOptions.
	//   If `resourceVersion` is unset, this is interpreted as "consistent read" and the
	//   bookmark event is send when the state is synced at least to the moment
	//   when request started being processed.
	// - `resourceVersionMatch` set to any other value or unset
	//   Invalid error is returned.
	//
	// Defaults to true if `resourceVersion=""` or `resourceVersion="0"` (for backward
	// compatibility reasons) and to false otherwise.
	// +optional
	//
	// SendInitialEvents 可与 `watch=true` 一起设置：watch 流会先发送合成事件以呈现集合当前状态，
	// 全部发送完后发送一个合成的 "Bookmark" 事件（带 `"k8s.io/initial-events-end": "true"` 注解，
	// 并报告对应这批对象的 ResourceVersion），之后照常发送该 RV 之后的变化事件。
	// 设置该选项时要求同时设置 `resourceVersionMatch`：仅允许 NotOlderThan，其余值或未设置会返回
	// Invalid 错误。当 `resourceVersion` 为空或 "0" 时默认 true（向后兼容），否则默认 false。
	SendInitialEvents *bool `json:"sendInitialEvents,omitempty" protobuf:"varint,11,opt,name=sendInitialEvents"`

	// shardSelector restricts the list of returned objects using a CEL-based
	// shard selector expression. The format uses the shardRange() function
	// combined with || (logical OR) to specify one or more hash ranges:
	//
	//   shardRange(object.metadata.uid, '0x0', '0x8000000000000000')
	//   shardRange(object.metadata.uid, '0x0', '0x8000000000000000') || shardRange(object.metadata.uid, '0x8000000000000000', '0x10000000000000000')
	//
	// Field paths use CEL-style object-rooted syntax (e.g. "object.metadata.uid"),
	// NOT the fieldSelector format ("metadata.uid"). Currently supported paths:
	//   - object.metadata.uid
	//   - object.metadata.namespace
	//
	// hexStart and hexEnd are single-quoted CEL string literals with a '0x' prefix,
	// defining the inclusive lower and exclusive upper bounds over the 64-bit FNV-1a
	// hash space. The full range is [0x0, 0x10000000000000000), where the exclusive
	// upper bound equals 2^64.
	//
	// Examples:
	//   2-shard split:
	//     shard 0: shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000')
	//     shard 1: shardRange(object.metadata.uid, '0x8000000000000000', '0x10000000000000000')
	//   4-shard split:
	//     shard 0: shardRange(object.metadata.uid, '0x0000000000000000', '0x4000000000000000')
	//     shard 1: shardRange(object.metadata.uid, '0x4000000000000000', '0x8000000000000000')
	//     shard 2: shardRange(object.metadata.uid, '0x8000000000000000', '0xc000000000000000')
	//     shard 3: shardRange(object.metadata.uid, '0xc000000000000000', '0x10000000000000000')
	//
	// This is an alpha field and requires enabling the ShardedListAndWatch feature gate.
	// +featureGate=ShardedListAndWatch
	// +optional
	//
	// ShardSelector 用基于 CEL 的分片选择表达式限制返回对象，通过 shardRange() 与 || 组合
	// 一个或多个哈希区间。字段路径使用以 object 为根的 CEL 语法（如 "object.metadata.uid"），
	// 而非 fieldSelector 的 "metadata.uid"；目前支持 object.metadata.uid 与 object.metadata.namespace。
	// 区间边界是带 '0x' 前缀的单引号 CEL 字符串字面量，作用于 64 位 FNV-1a 哈希空间：
	// 左闭右开，完整范围 [0x0, 0x10000000000000000)，上界即 2^64。
	// 该字段为 alpha，需要开启 ShardedListAndWatch feature gate。
	ShardSelector string `json:"shardSelector,omitempty" protobuf:"bytes,15,opt,name=shardSelector"`
}

const (
	// InitialEventsAnnotationKey the name of the key
	// under which an annotation marking the end of
	// a watchlist stream is stored.
	//
	// The annotation is added to a "Bookmark" event.
	//
	// InitialEventsAnnotationKey 是存放 “watchlist 流结束” 标记的注解键名，
	// 该注解会被添加到 "Bookmark" 事件上。
	InitialEventsAnnotationKey = "k8s.io/initial-events-end"
)

// resourceVersionMatch specifies how the resourceVersion parameter is applied. resourceVersionMatch
// may only be set if resourceVersion is also set.
//
// "NotOlderThan" matches data at least as new as the provided resourceVersion.
// "Exact" matches data at the exact resourceVersion provided.
//
// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
// details.
//
// ResourceVersionMatch 指定 resourceVersion 参数的应用方式，仅在同时设置 resourceVersion 时可用。
// "NotOlderThan" 匹配不早于所给 resourceVersion 的数据；"Exact" 匹配恰好等于所给版本的数据。
type ResourceVersionMatch string

const (
	// ResourceVersionMatchNotOlderThan matches data at least as new as the provided
	// resourceVersion.
	//
	// ResourceVersionMatchNotOlderThan 匹配不早于所给 resourceVersion 的数据。
	ResourceVersionMatchNotOlderThan ResourceVersionMatch = "NotOlderThan"
	// ResourceVersionMatchExact matches data at the exact resourceVersion
	// provided.
	//
	// ResourceVersionMatchExact 匹配恰好等于所给 resourceVersion 的数据。
	ResourceVersionMatchExact ResourceVersionMatch = "Exact"
)

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GetOptions is the standard query options to the standard REST get call.
//
// GetOptions 是标准 REST get 调用的查询选项。
type GetOptions struct {
	TypeMeta `json:""`
	// resourceVersion sets a constraint on what resource versions a request may be served from.
	// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
	// details.
	//
	// Defaults to unset
	// +optional
	//
	// ResourceVersion 约束请求可被服务的资源版本范围，默认未设置。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,1,opt,name=resourceVersion"`
	// +k8s:deprecated=includeUninitialized,protobuf=2
}

// DeletionPropagation decides if a deletion will propagate to the dependents of
// the object, and how the garbage collector will handle the propagation.
//
// DeletionPropagation 决定删除是否传播到依赖对象，以及垃圾回收器如何处理该传播。
type DeletionPropagation string

const (
	// Orphans the dependents.
	//
	// DeletePropagationOrphan 孤立依赖对象。
	DeletePropagationOrphan DeletionPropagation = "Orphan"
	// Deletes the object from the key-value store, the garbage collector will
	// delete the dependents in the background.
	//
	// DeletePropagationBackground 先从键值存储删除对象，垃圾回收器在后台删除依赖对象。
	DeletePropagationBackground DeletionPropagation = "Background"
	// The object exists in the key-value store until the garbage collector
	// deletes all the dependents whose ownerReference.blockOwnerDeletion=true
	// from the key-value store.  API sever will put the "foregroundDeletion"
	// finalizer on the object, and sets its deletionTimestamp.  This policy is
	// cascading, i.e., the dependents will be deleted with Foreground.
	//
	// DeletePropagationForeground 对象会一直保留在键值存储中，直到垃圾回收器删除所有
	// ownerReference.blockOwnerDeletion=true 的依赖对象。API server 会给对象加上
	// "foregroundDeletion" finalizer 并设置 deletionTimestamp；该策略是级联的，
	// 依赖对象也按 Foreground 方式删除。
	DeletePropagationForeground DeletionPropagation = "Foreground"
)

const (
	// DryRunAll means to complete all processing stages, but don't
	// persist changes to storage.
	//
	// DryRunAll 表示完成所有处理阶段，但不把变更持久化到存储。
	DryRunAll = "All"
)

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeleteOptions may be provided when deleting an API object.
//
// DeleteOptions 可在删除 API 对象时提供。
type DeleteOptions struct {
	TypeMeta `json:""`

	// The duration in seconds before the object should be deleted. Value must be non-negative integer.
	// The value zero indicates delete immediately. If this value is nil, the default grace period for the
	// specified type will be used.
	// Defaults to a per object value if not specified. zero means delete immediately.
	// +optional
	//
	// GracePeriodSeconds 是对象删除前的宽限秒数，必须为非负整数；0 表示立即删除。
	// 为 nil 时使用该类型的默认宽限期（未指定时按对象自身取值）。
	GracePeriodSeconds *int64 `json:"gracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=gracePeriodSeconds"`

	// Must be fulfilled before a deletion is carried out. If not possible, a 409 Conflict status will be
	// returned.
	// +k8s:conversion-gen=false
	// +optional
	//
	// Preconditions 必须在执行删除前满足，否则返回 409 Conflict。
	Preconditions *Preconditions `json:"preconditions,omitempty" protobuf:"bytes,2,opt,name=preconditions"`

	// Deprecated: please use the PropagationPolicy, this field will be deprecated in 1.7.
	// Should the dependent objects be orphaned. If true/false, the "orphan"
	// finalizer will be added to/removed from the object's finalizers list.
	// Either this field or PropagationPolicy may be set, but not both.
	// +optional
	//
	// 已弃用：请使用 PropagationPolicy；该字段在 1.7 起废弃。
	// 为 true/false 时会给对象的 finalizers 添加/移除 "orphan" finalizer。
	// 该字段与 PropagationPolicy 只能二选一。
	OrphanDependents *bool `json:"orphanDependents,omitempty" protobuf:"varint,3,opt,name=orphanDependents"`

	// Whether and how garbage collection will be performed.
	// Either this field or OrphanDependents may be set, but not both.
	// The default policy is decided by the existing finalizer set in the
	// metadata.finalizers and the resource-specific default policy.
	// Acceptable values are: 'Orphan' - orphan the dependents; 'Background' -
	// allow the garbage collector to delete the dependents in the background;
	// 'Foreground' - a cascading policy that deletes all dependents in the
	// foreground.
	// +optional
	//
	// PropagationPolicy 指定是否以及如何执行垃圾回收，与 OrphanDependents 只能二选一。
	// 默认策略由 metadata.finalizers 中已有的 finalizer 与资源自身的默认策略决定。
	// 可选值：'Orphan' 孤立依赖对象；'Background' 由垃圾回收器后台删除依赖对象；
	// 'Foreground' 前台级联删除所有依赖对象。
	PropagationPolicy *DeletionPropagation `json:"propagationPolicy,omitempty" protobuf:"varint,4,opt,name=propagationPolicy"`

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	// +optional
	// +listType=atomic
	//
	// DryRun 存在时表示不持久化修改。非法或无法识别的指令会返回错误且不再处理请求。
	// 合法值：- All：执行所有 dry run 阶段。
	DryRun []string `json:"dryRun,omitempty" protobuf:"bytes,5,rep,name=dryRun"`

	// if set to true, it will trigger an unsafe deletion of the resource in
	// case the normal deletion flow fails with a corrupt object error.
	// A resource is considered corrupt if it can not be retrieved from
	// the underlying storage successfully because of a) its data can
	// not be transformed e.g. decryption failure, or b) it fails
	// to decode into an object.
	// NOTE: unsafe deletion ignores finalizer constraints, skips
	// precondition checks, and removes the object from the storage.
	// WARNING: This may potentially break the cluster if the workload
	// associated with the resource being unsafe-deleted relies on normal
	// deletion flow. Use only if you REALLY know what you are doing.
	// The default value is false, and the user must opt in to enable it
	// +optional
	//
	// IgnoreStoreReadErrorWithClusterBreakingPotential 为 true 时，若正常删除因对象损坏而失败，
	// 会触发不安全的删除。对象损坏指无法从底层存储成功读取：a) 数据无法转换（如解密失败），
	// 或 b) 无法解码为对象。
	// 注意：不安全删除会忽略 finalizer 约束、跳过 precondition 检查，并直接从存储移除对象。
	// 警告：若被删除资源关联的工作负载依赖正常删除流程，可能破坏集群；除非非常确定，否则不要使用。
	// 默认 false，需用户显式开启。
	IgnoreStoreReadErrorWithClusterBreakingPotential *bool `json:"ignoreStoreReadErrorWithClusterBreakingPotential,omitempty" protobuf:"varint,6,opt,name=ignoreStoreReadErrorWithClusterBreakingPotential"`
}

const (
	// FieldValidationIgnore ignores unknown/duplicate fields
	//
	// FieldValidationIgnore 忽略未知/重复字段。
	FieldValidationIgnore = "Ignore"
	// FieldValidationWarn responds with a warning, but successfully serve the request
	//
	// FieldValidationWarn 返回警告，但请求仍然成功。
	FieldValidationWarn = "Warn"
	// FieldValidationStrict fails the request on unknown/duplicate fields
	//
	// FieldValidationStrict 遇到未知/重复字段时使请求失败。
	FieldValidationStrict = "Strict"
)

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CreateOptions may be provided when creating an API object.
//
// CreateOptions 可在创建 API 对象时提供。
type CreateOptions struct {
	TypeMeta `json:""`

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	// +optional
	// +listType=atomic
	//
	// DryRun 存在时表示不持久化修改；非法指令会返回错误且不再处理请求。合法值：All。
	DryRun []string `json:"dryRun,omitempty" protobuf:"bytes,1,rep,name=dryRun"`
	// +k8s:deprecated=includeUninitialized,protobuf=2

	// fieldManager is a name associated with the actor or entity
	// that is making these changes. The value must be less than or
	// 128 characters long, and only contain printable characters,
	// as defined by https://golang.org/pkg/unicode/#IsPrint.
	// +optional
	//
	// FieldManager 是与发起变更的参与者/实体关联的名称；长度须小于 128 个字符，
	// 且只包含可打印字符（见 unicode.IsPrint）。
	FieldManager string `json:"fieldManager,omitempty" protobuf:"bytes,3,name=fieldManager"`

	// fieldValidation instructs the server on how to handle
	// objects in the request (POST/PUT/PATCH) containing unknown
	// or duplicate fields. Valid values are:
	// - Ignore: This will ignore any unknown fields that are silently
	// dropped from the object, and will ignore all but the last duplicate
	// field that the decoder encounters. This is the default behavior
	// prior to v1.23.
	// - Warn: This will send a warning via the standard warning response
	// header for each unknown field that is dropped from the object, and
	// for each duplicate field that is encountered. The request will
	// still succeed if there are no other errors, and will only persist
	// the last of any duplicate fields. This is the default in v1.23+
	// - Strict: This will fail the request with a BadRequest error if
	// any unknown fields would be dropped from the object, or if any
	// duplicate fields are present. The error returned from the server
	// will contain all unknown and duplicate fields encountered.
	// +optional
	//
	// FieldValidation 指示服务器如何处理请求（POST/PUT/PATCH）中未知或重复的字段：
	// - Ignore：静默丢弃未知字段，重复字段只保留最后一个；v1.23 之前的默认行为。
	// - Warn：对每个被丢弃的未知字段和遇到的重复字段通过标准 warning 响应头发送警告；
	//   无其他错误时请求仍成功，重复字段只持久化最后一个；v1.23+ 的默认行为。
	// - Strict：若存在会被丢弃的未知字段或重复字段，以 BadRequest 使请求失败，
	//   返回的错误包含所有未知与重复字段。
	FieldValidation string `json:"fieldValidation,omitempty" protobuf:"bytes,4,name=fieldValidation"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PatchOptions may be provided when patching an API object.
// PatchOptions is meant to be a superset of UpdateOptions.
//
// PatchOptions 可在 patch API 对象时提供，是 UpdateOptions 的超集。
type PatchOptions struct {
	TypeMeta `json:""`

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	// +optional
	// +listType=atomic
	//
	// DryRun 存在时表示不持久化修改；非法指令会返回错误且不再处理请求。合法值：All。
	DryRun []string `json:"dryRun,omitempty" protobuf:"bytes,1,rep,name=dryRun"`

	// Force is going to "force" Apply requests. It means user will
	// re-acquire conflicting fields owned by other people. Force
	// flag must be unset for non-apply patch requests.
	// +optional
	//
	// Force 用于 “强制” Apply 请求，表示用户将重新获取他人拥有的冲突字段；
	// 非 apply 的 patch 请求必须不设置该标志。
	Force *bool `json:"force,omitempty" protobuf:"varint,2,opt,name=force"`

	// fieldManager is a name associated with the actor or entity
	// that is making these changes. The value must be less than or
	// 128 characters long, and only contain printable characters,
	// as defined by https://golang.org/pkg/unicode/#IsPrint. This
	// field is required for apply requests
	// (application/apply-patch) but optional for non-apply patch
	// types (JsonPatch, MergePatch, StrategicMergePatch).
	// +optional
	//
	// FieldManager 是与发起变更的参与者/实体关联的名称；长度须小于 128 个字符，
	// 且只包含可打印字符。apply 请求（application/apply-patch）必填，
	// 非 apply 的 patch 类型（JsonPatch、MergePatch、StrategicMergePatch）可选。
	FieldManager string `json:"fieldManager,omitempty" protobuf:"bytes,3,name=fieldManager"`

	// fieldValidation instructs the server on how to handle
	// objects in the request (POST/PUT/PATCH) containing unknown
	// or duplicate fields. Valid values are:
	// - Ignore: This will ignore any unknown fields that are silently
	// dropped from the object, and will ignore all but the last duplicate
	// field that the decoder encounters. This is the default behavior
	// prior to v1.23.
	// - Warn: This will send a warning via the standard warning response
	// header for each unknown field that is dropped from the object, and
	// for each duplicate field that is encountered. The request will
	// still succeed if there are no other errors, and will only persist
	// the last of any duplicate fields. This is the default in v1.23+
	// - Strict: This will fail the request with a BadRequest error if
	// any unknown fields would be dropped from the object, or if any
	// duplicate fields are present. The error returned from the server
	// will contain all unknown and duplicate fields encountered.
	// +optional
	//
	// FieldValidation 指示服务器如何处理请求中未知或重复的字段，
	// 语义同 CreateOptions.FieldValidation（Ignore/Warn/Strict）。
	FieldValidation string `json:"fieldValidation,omitempty" protobuf:"bytes,4,name=fieldValidation"`
}

// ApplyOptions may be provided when applying an API object.
// FieldManager is required for apply requests.
// ApplyOptions is equivalent to PatchOptions. It is provided as a convenience with documentation
// that speaks specifically to how the options fields relate to apply.
//
// ApplyOptions 可在 apply API 对象时提供；apply 请求要求 FieldManager。
// 它等价于 PatchOptions，单独提供是为了给出针对 apply 语义的文档说明。
type ApplyOptions struct {
	TypeMeta `json:""`

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	// +optional
	// +listType=atomic
	//
	// DryRun 存在时表示不持久化修改；非法指令会返回错误且不再处理请求。合法值：All。
	DryRun []string `json:"dryRun,omitempty" protobuf:"bytes,1,rep,name=dryRun"`

	// Force is going to "force" Apply requests. It means user will
	// re-acquire conflicting fields owned by other people.
	//
	// Force 用于 “强制” Apply 请求，表示用户将重新获取他人拥有的冲突字段。
	Force bool `json:"force" protobuf:"varint,2,opt,name=force"`

	// fieldManager is a name associated with the actor or entity
	// that is making these changes. The value must be less than or
	// 128 characters long, and only contain printable characters,
	// as defined by https://golang.org/pkg/unicode/#IsPrint. This
	// field is required.
	//
	// FieldManager 是与发起变更的参与者/实体关联的名称；长度须小于 128 个字符，
	// 且只包含可打印字符。apply 请求中必填。
	FieldManager string `json:"fieldManager" protobuf:"bytes,3,name=fieldManager"`
}

// ToPatchOptions 把 ApplyOptions 转换为等价的 PatchOptions（Force 转为指针形式）。
func (o ApplyOptions) ToPatchOptions() PatchOptions {
	return PatchOptions{DryRun: o.DryRun, Force: &o.Force, FieldManager: o.FieldManager}
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpdateOptions may be provided when updating an API object.
// All fields in UpdateOptions should also be present in PatchOptions.
//
// UpdateOptions 可在更新 API 对象时提供；其中所有字段都应同样存在于 PatchOptions 中。
type UpdateOptions struct {
	TypeMeta `json:""`

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	// +optional
	// +listType=atomic
	//
	// DryRun 存在时表示不持久化修改；非法指令会返回错误且不再处理请求。合法值：All。
	DryRun []string `json:"dryRun,omitempty" protobuf:"bytes,1,rep,name=dryRun"`

	// fieldManager is a name associated with the actor or entity
	// that is making these changes. The value must be less than or
	// 128 characters long, and only contain printable characters,
	// as defined by https://golang.org/pkg/unicode/#IsPrint.
	// +optional
	//
	// FieldManager 是与发起变更的参与者/实体关联的名称；长度须小于 128 个字符，
	// 且只包含可打印字符。
	FieldManager string `json:"fieldManager,omitempty" protobuf:"bytes,2,name=fieldManager"`

	// fieldValidation instructs the server on how to handle
	// objects in the request (POST/PUT/PATCH) containing unknown
	// or duplicate fields. Valid values are:
	// - Ignore: This will ignore any unknown fields that are silently
	// dropped from the object, and will ignore all but the last duplicate
	// field that the decoder encounters. This is the default behavior
	// prior to v1.23.
	// - Warn: This will send a warning via the standard warning response
	// header for each unknown field that is dropped from the object, and
	// for each duplicate field that is encountered. The request will
	// still succeed if there are no other errors, and will only persist
	// the last of any duplicate fields. This is the default in v1.23+
	// - Strict: This will fail the request with a BadRequest error if
	// any unknown fields would be dropped from the object, or if any
	// duplicate fields are present. The error returned from the server
	// will contain all unknown and duplicate fields encountered.
	// +optional
	//
	// FieldValidation 指示服务器如何处理请求中未知或重复的字段，
	// 语义同 CreateOptions.FieldValidation（Ignore/Warn/Strict）。
	FieldValidation string `json:"fieldValidation,omitempty" protobuf:"bytes,3,name=fieldValidation"`
}

// Preconditions must be fulfilled before an operation (update, delete, etc.) is carried out.
//
// Preconditions 必须在执行操作（update、delete 等）前满足。
type Preconditions struct {
	// Specifies the target UID.
	// +optional
	//
	// UID 指定目标对象的 UID。
	UID *types.UID `json:"uid,omitempty" protobuf:"bytes,1,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// Specifies the target ResourceVersion
	// +optional
	//
	// ResourceVersion 指定目标对象的资源版本。
	ResourceVersion *string `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Status is a return value for calls that don't return other objects.
//
// Status 是不返回其他对象的调用的返回值。
type Status struct {
	TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// 标准列表元数据。
	ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Status of the operation.
	// One of: "Success" or "Failure".
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	//
	// Status 是操作状态，取值为 "Success" 或 "Failure" 之一。
	Status string `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
	// A human-readable description of the status of this operation.
	// +optional
	//
	// Message 是本操作状态的人类可读描述。
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// A machine-readable description of why this operation is in the
	// "Failure" status. If this value is empty there
	// is no information available. A Reason clarifies an HTTP status
	// code but does not override it.
	// +optional
	//
	// Reason 是操作为 "Failure" 的机器可读原因；为空表示无信息。
	// Reason 用于细化 HTTP 状态码，但不会覆盖它。
	Reason StatusReason `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason,casttype=StatusReason"`
	// Extended data associated with the reason.  Each reason may define its
	// own extended details. This field is optional and the data returned
	// is not guaranteed to conform to any schema except that defined by
	// the reason type.
	// +optional
	//
	// Details 是与 Reason 关联的扩展数据；各 Reason 可定义自己的扩展细节。
	// 该字段可选，返回的数据只保证符合对应 Reason 类型定义的 schema。
	Details *StatusDetails `json:"details,omitempty" protobuf:"bytes,5,opt,name=details"`
	// Suggested HTTP return code for this status, 0 if not set.
	// +optional
	//
	// Code 建议的 HTTP 返回码，未设置时为 0。
	Code int32 `json:"code,omitempty" protobuf:"varint,6,opt,name=code"`
}

// StatusDetails is a set of additional properties that MAY be set by the
// server to provide additional information about a response. The Reason
// field of a Status object defines what attributes will be set. Clients
// must ignore fields that do not match the defined type of each attribute,
// and should assume that any attribute may be empty, invalid, or under
// defined.
//
// StatusDetails 是服务器可选的附加属性集合，用于补充响应信息。
// Status 的 Reason 字段决定会设置哪些属性。客户端必须忽略与各属性定义类型不符的字段，
// 并假定任何属性都可能为空、非法或定义不完整。
type StatusDetails struct {
	// The name attribute of the resource associated with the status StatusReason
	// (when there is a single name which can be described).
	// +optional
	//
	// Name 是与 StatusReason 关联的资源的名称（仅当可描述的名称只有一个时）。
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	// The group attribute of the resource associated with the status StatusReason.
	// +optional
	//
	// Group 是与 StatusReason 关联的资源的 group。
	Group string `json:"group,omitempty" protobuf:"bytes,2,opt,name=group"`
	// The kind attribute of the resource associated with the status StatusReason.
	// On some operations may differ from the requested resource Kind.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// Kind 是与 StatusReason 关联的资源的类型；某些操作中可能与请求的资源 Kind 不同。
	Kind string `json:"kind,omitempty" protobuf:"bytes,3,opt,name=kind"`
	// UID of the resource.
	// (when there is a single resource which can be described).
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#uids
	// +optional
	//
	// UID 是资源的 UID（仅当可描述的资源只有一个时）。
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,6,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// The Causes array includes more details associated with the StatusReason
	// failure. Not all StatusReasons may provide detailed causes.
	// +optional
	// +listType=atomic
	//
	// Causes 包含与 StatusReason 失败关联的更多细节；并非所有 StatusReason 都提供详细原因。
	Causes []StatusCause `json:"causes,omitempty" protobuf:"bytes,4,rep,name=causes"`
	// If specified, the time in seconds before the operation should be retried. Some errors may indicate
	// the client must take an alternate action - for those errors this field may indicate how long to wait
	// before taking the alternate action.
	// +optional
	//
	// RetryAfterSeconds 表示建议重试操作前等待的秒数。某些错误要求客户端采取替代操作，
	// 此时该字段表示等待多久后再执行替代操作。
	RetryAfterSeconds int32 `json:"retryAfterSeconds,omitempty" protobuf:"varint,5,opt,name=retryAfterSeconds"`
}

// Values of Status.Status
const (
	// StatusSuccess 表示操作成功。
	StatusSuccess = "Success"
	// StatusFailure 表示操作失败。
	StatusFailure = "Failure"
)

// StatusReason is an enumeration of possible failure causes.  Each StatusReason
// must map to a single HTTP status code, but multiple reasons may map
// to the same HTTP status code.
// TODO: move to apiserver
//
// StatusReason 是可能的失败原因枚举。每个 StatusReason 必须映射到单一 HTTP 状态码，
// 但多个 reason 可以映射到同一个状态码。
type StatusReason string

const (
	// StatusReasonUnknown means the server has declined to indicate a specific reason.
	// The details field may contain other information about this error.
	// Status code 500.
	//
	// StatusReasonUnknown 表示服务器未给出具体原因；details 可能包含其他错误信息。状态码 500。
	StatusReasonUnknown StatusReason = ""

	// StatusReasonUnauthorized means the server can be reached and understood the request, but requires
	// the user to present appropriate authorization credentials (identified by the WWW-Authenticate header)
	// in order for the action to be completed. If the user has specified credentials on the request, the
	// server considers them insufficient.
	// Status code 401
	//
	// StatusReasonUnauthorized 表示服务器可达且理解请求，但要求用户提供适当的认证凭据
	// （由 WWW-Authenticate 头标识）才能完成操作；若已提供凭据，服务器认为其不充分。状态码 401。
	StatusReasonUnauthorized StatusReason = "Unauthorized"

	// StatusReasonForbidden means the server can be reached and understood the request, but refuses
	// to take any further action.  It is the result of the server being configured to deny access for some reason
	// to the requested resource by the client.
	// Details (optional):
	//   "kind" string - the kind attribute of the forbidden resource
	//                   on some operations may differ from the requested
	//                   resource.
	//   "id"   string - the identifier of the forbidden resource
	// Status code 403
	//
	// StatusReasonForbidden 表示服务器可达且理解请求，但拒绝进一步处理，通常由访问控制配置导致。
	// Details（可选）："kind" 为被禁止资源的类型（某些操作中可能与请求资源不同），"id" 为其标识。
	// 状态码 403。
	StatusReasonForbidden StatusReason = "Forbidden"

	// StatusReasonNotFound means one or more resources required for this operation
	// could not be found.
	// Details (optional):
	//   "kind" string - the kind attribute of the missing resource
	//                   on some operations may differ from the requested
	//                   resource.
	//   "id"   string - the identifier of the missing resource
	// Status code 404
	//
	// StatusReasonNotFound 表示操作所需的一个或多个资源不存在。
	// Details（可选）："kind" 为缺失资源的类型，"id" 为其标识。状态码 404。
	StatusReasonNotFound StatusReason = "NotFound"

	// StatusReasonAlreadyExists means the resource you are creating already exists.
	// Details (optional):
	//   "kind" string - the kind attribute of the conflicting resource
	//   "id"   string - the identifier of the conflicting resource
	// Status code 409
	//
	// StatusReasonAlreadyExists 表示要创建的资源已存在。
	// Details（可选）："kind" 为冲突资源的类型，"id" 为其标识。状态码 409。
	StatusReasonAlreadyExists StatusReason = "AlreadyExists"

	// StatusReasonConflict means the requested operation cannot be completed
	// due to a conflict in the operation. The client may need to alter the
	// request. Each resource may define custom details that indicate the
	// nature of the conflict.
	// Status code 409
	//
	// StatusReasonConflict 表示操作因冲突无法完成，客户端可能需要修改请求；
	// 各资源可定义自定义 details 说明冲突性质。状态码 409。
	StatusReasonConflict StatusReason = "Conflict"

	// StatusReasonGone means the item is no longer available at the server and no
	// forwarding address is known.
	// Status code 410
	//
	// StatusReasonGone 表示条目在服务器上已不可用，且没有已知的转发地址。状态码 410。
	StatusReasonGone StatusReason = "Gone"

	// StatusReasonInvalid means the requested create or update operation cannot be
	// completed due to invalid data provided as part of the request. The client may
	// need to alter the request. When set, the client may use the StatusDetails
	// message field as a summary of the issues encountered.
	// Details (optional):
	//   "kind" string - the kind attribute of the invalid resource
	//   "id"   string - the identifier of the invalid resource
	//   "causes"      - one or more StatusCause entries indicating the data in the
	//                   provided resource that was invalid.  The code, message, and
	//                   field attributes will be set.
	// Status code 422
	//
	// StatusReasonInvalid 表示请求中的创建或更新操作因数据非法而无法完成，客户端可能需要修改请求；
	// 可通过 StatusDetails 的 message 查看问题摘要。
	// Details（可选）："kind"/"id" 标识非法资源，"causes" 列出具体的非法数据项。状态码 422。
	StatusReasonInvalid StatusReason = "Invalid"

	// StatusReasonServerTimeout means the server can be reached and understood the request,
	// but cannot complete the action in a reasonable time. The client should retry the request.
	// This is may be due to temporary server load or a transient communication issue with
	// another server. Status code 500 is used because the HTTP spec provides no suitable
	// server-requested client retry and the 5xx class represents actionable errors.
	// Details (optional):
	//   "kind" string - the kind attribute of the resource being acted on.
	//   "id"   string - the operation that is being attempted.
	//   "retryAfterSeconds" int32 - the number of seconds before the operation should be retried
	// Status code 500
	//
	// StatusReasonServerTimeout 表示服务器可达且理解请求，但无法在合理时间内完成，客户端应重试；
	// 原因可能是临时负载或与其他服务器间的瞬时通信问题。用 500 是因为 HTTP 规范没有合适的
	// 服务器请求客户端重试的语义，而 5xx 表示可操作的错误。
	// Details（可选）："kind"/"id" 标识资源与操作，"retryAfterSeconds" 为建议重试等待秒数。状态码 500。
	StatusReasonServerTimeout StatusReason = "ServerTimeout"

	// StatusReasonStoreReadError means that the server encountered an error while
	// retrieving resources from the backend object store.
	// This may be due to backend database error, or because processing of the read
	// resource failed.
	// Details:
	//   "kind" string - the kind attribute of the resource being acted on.
	//   "name" string - the prefix where the reading error(s) occurred
	//   "causes" []StatusCause
	//      - (optional):
	//        - "type" CauseType - CauseTypeUnexpectedServerResponse
	//        - "message" string - the error message from the store backend
	//        - "field" string - the full path with the key of the resource that failed reading
	//
	// Status code 500
	//
	// StatusReasonStoreReadError 表示服务器从后端对象存储读取资源时出错，
	// 可能是数据库错误，也可能是读取到的资源处理失败。
	// Details："kind" 为资源类型，"name" 为读取出错的前缀，"causes" 可包含后端错误信息与失败 key 的完整路径。
	// 状态码 500。
	StatusReasonStoreReadError StatusReason = "StorageReadError"

	// StatusReasonTimeout means that the request could not be completed within the given time.
	// Clients can get this response only when they specified a timeout param in the request,
	// or if the server cannot complete the operation within a reasonable amount of time.
	// The request might succeed with an increased value of timeout param. The client *should*
	// wait at least the number of seconds specified by the retryAfterSeconds field.
	// Details (optional):
	//   "retryAfterSeconds" int32 - the number of seconds before the operation should be retried
	// Status code 504
	//
	// StatusReasonTimeout 表示请求未能在给定时间内完成；仅在客户端指定 timeout 参数
	// 或服务器无法在合理时间内完成时返回。增大 timeout 可能成功；
	// 客户端 *应* 至少等待 retryAfterSeconds 指定的秒数。状态码 504。
	StatusReasonTimeout StatusReason = "Timeout"

	// StatusReasonTooManyRequests means the server experienced too many requests within a
	// given window and that the client must wait to perform the action again. A client may
	// always retry the request that led to this error, although the client should wait at least
	// the number of seconds specified by the retryAfterSeconds field.
	// Details (optional):
	//   "retryAfterSeconds" int32 - the number of seconds before the operation should be retried
	// Status code 429
	//
	// StatusReasonTooManyRequests 表示服务器在给定窗口内收到过多请求，客户端必须等待后重试；
	// 该请求总是可以重试，但应至少等待 retryAfterSeconds 秒。状态码 429。
	StatusReasonTooManyRequests StatusReason = "TooManyRequests"

	// StatusReasonBadRequest means that the request itself was invalid, because the request
	// doesn't make any sense, for example deleting a read-only object.  This is different than
	// StatusReasonInvalid above which indicates that the API call could possibly succeed, but the
	// data was invalid.  API calls that return BadRequest can never succeed.
	// Status code 400
	//
	// StatusReasonBadRequest 表示请求本身无效（例如删除只读对象），与 StatusReasonInvalid
	// （数据非法但调用本可能成功）不同：返回 BadRequest 的调用永远不可能成功。状态码 400。
	StatusReasonBadRequest StatusReason = "BadRequest"

	// StatusReasonMethodNotAllowed means that the action the client attempted to perform on the
	// resource was not supported by the code - for instance, attempting to delete a resource that
	// can only be created. API calls that return MethodNotAllowed can never succeed.
	// Status code 405
	//
	// StatusReasonMethodNotAllowed 表示客户端尝试的操作不被支持（例如删除只能创建的资源）；
	// 返回 MethodNotAllowed 的调用永远不可能成功。状态码 405。
	StatusReasonMethodNotAllowed StatusReason = "MethodNotAllowed"

	// StatusReasonNotAcceptable means that the accept types indicated by the client were not acceptable
	// to the server - for instance, attempting to receive protobuf for a resource that supports only json and yaml.
	// API calls that return NotAcceptable can never succeed.
	// Status code 406
	//
	// StatusReasonNotAcceptable 表示客户端声明的 accept 类型不被服务器接受
	// （例如请求仅支持 json/yaml 的资源的 protobuf 表示）；此类调用永远不可能成功。状态码 406。
	StatusReasonNotAcceptable StatusReason = "NotAcceptable"

	// StatusReasonRequestEntityTooLarge means that the request entity is too large.
	// Status code 413
	//
	// StatusReasonRequestEntityTooLarge 表示请求实体过大。状态码 413。
	StatusReasonRequestEntityTooLarge StatusReason = "RequestEntityTooLarge"

	// StatusReasonUnsupportedMediaType means that the content type sent by the client is not acceptable
	// to the server - for instance, attempting to send protobuf for a resource that supports only json and yaml.
	// API calls that return UnsupportedMediaType can never succeed.
	// Status code 415
	//
	// StatusReasonUnsupportedMediaType 表示客户端发送的 content type 不被服务器接受
	// （例如向仅支持 json/yaml 的资源发送 protobuf）；此类调用永远不可能成功。状态码 415。
	StatusReasonUnsupportedMediaType StatusReason = "UnsupportedMediaType"

	// StatusReasonInternalError indicates that an internal error occurred, it is unexpected
	// and the outcome of the call is unknown.
	// Details (optional):
	//   "causes" - The original error
	// Status code 500
	//
	// StatusReasonInternalError 表示发生意外内部错误，调用结果未知。
	// Details（可选）："causes" 为原始错误。状态码 500。
	StatusReasonInternalError StatusReason = "InternalError"

	// StatusReasonExpired indicates that the request is invalid because the content you are requesting
	// has expired and is no longer available. It is typically associated with watches that can't be
	// serviced.
	// Status code 410 (gone)
	//
	// StatusReasonExpired 表示请求内容已过期不可用，请求无效；通常与无法继续服务的 watch 相关。
	// 状态码 410 (gone)。
	StatusReasonExpired StatusReason = "Expired"

	// StatusReasonServiceUnavailable means that the request itself was valid,
	// but the requested service is unavailable at this time.
	// Retrying the request after some time might succeed.
	// Status code 503
	//
	// StatusReasonServiceUnavailable 表示请求本身有效，但所请求的服务当前不可用；
	// 稍后重试可能成功。状态码 503。
	StatusReasonServiceUnavailable StatusReason = "ServiceUnavailable"
)

// StatusCause provides more information about an api.Status failure, including
// cases when multiple errors are encountered.
//
// StatusCause 提供 api.Status 失败的更多信息，包括遇到多个错误的情形。
type StatusCause struct {
	// A machine-readable description of the cause of the error. If this value is
	// empty there is no information available.
	// +optional
	//
	// Type 是错误原因的机器可读描述；为空表示无信息。
	Type CauseType `json:"reason,omitempty" protobuf:"bytes,1,opt,name=reason,casttype=CauseType"`
	// A human-readable description of the cause of the error.  This field may be
	// presented as-is to a reader.
	// +optional
	//
	// Message 是错误原因的人类可读描述，可直接展示给读者。
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// The field of the resource that has caused this error, as named by its JSON
	// serialization. May include dot and postfix notation for nested attributes.
	// Arrays are zero-indexed.  Fields may appear more than once in an array of
	// causes due to fields having multiple errors.
	// Optional.
	//
	// Examples:
	//   "name" - the field "name" on the current resource
	//   "items[0].name" - the field "name" on the first array entry in "items"
	// +optional
	//
	// Field 是导致该错误的资源字段（按 JSON 序列化名称），可包含嵌套属性的点号与下标表示；
	// 数组下标从零开始。同一字段因多个错误可能在 causes 数组中重复出现。可选。
	// 示例："name" 表示当前资源的 name 字段；"items[0].name" 表示 items 第一项的 name。
	Field string `json:"field,omitempty" protobuf:"bytes,3,opt,name=field"`
}

// CauseType is a machine readable value providing more detail about what
// occurred in a status response. An operation may have multiple causes for a
// status (whether Failure or Success).
//
// CauseType 是机器可读值，用于更详细说明 status 响应中发生的情况。
// 一次操作（无论 Failure 还是 Success）可以有多个 cause。
type CauseType string

const (
	// CauseTypeFieldValueNotFound is used to report failure to find a requested value
	// (e.g. looking up an ID).
	//
	// CauseTypeFieldValueNotFound 表示找不到请求的值（例如按 ID 查找）。
	CauseTypeFieldValueNotFound CauseType = "FieldValueNotFound"
	// CauseTypeFieldValueRequired is used to report required values that are not
	// provided (e.g. empty strings, null values, or empty arrays).
	//
	// CauseTypeFieldValueRequired 表示必填值缺失（如空字符串、null 或空数组）。
	CauseTypeFieldValueRequired CauseType = "FieldValueRequired"
	// CauseTypeFieldValueDuplicate is used to report collisions of values that must be
	// unique (e.g. unique IDs).
	//
	// CauseTypeFieldValueDuplicate 表示本应唯一的值发生冲突（如唯一 ID）。
	CauseTypeFieldValueDuplicate CauseType = "FieldValueDuplicate"
	// CauseTypeFieldValueInvalid is used to report malformed values (e.g. failed regex
	// match).
	//
	// CauseTypeFieldValueInvalid 表示值格式非法（如正则不匹配）。
	CauseTypeFieldValueInvalid CauseType = "FieldValueInvalid"
	// CauseTypeFieldValueNotSupported is used to report valid (as per formatting rules)
	// values that can not be handled (e.g. an enumerated string).
	//
	// CauseTypeFieldValueNotSupported 表示值本身格式合法但无法处理（如枚举外的字符串）。
	CauseTypeFieldValueNotSupported CauseType = "FieldValueNotSupported"
	// CauseTypeForbidden is used to report valid (as per formatting rules)
	// values which would be accepted under some conditions, but which are not
	// permitted by the current conditions (such as security policy).  See
	// Forbidden().
	//
	// CauseTypeForbidden 表示值格式合法、在某些条件下可接受，但当前条件（如安全策略）不允许。
	CauseTypeForbidden CauseType = "FieldValueForbidden"
	// CauseTypeTooLong is used to report that the given value is too long.
	// This is similar to ErrorTypeInvalid, but the error will not include the
	// too-long value.  See TooLong().
	//
	// CauseTypeTooLong 表示值过长；类似 ErrorTypeInvalid，但错误信息不会包含过长的值。
	CauseTypeTooLong CauseType = "FieldValueTooLong"
	// CauseTypeTooMany is used to report "too many". This is used to
	// report that a given list has too many items. This is similar to FieldValueTooLong,
	// but the error indicates quantity instead of length.
	//
	// CauseTypeTooMany 表示 “过多”，用于列表条目数超限；类似 FieldValueTooLong，但指示数量而非长度。
	CauseTypeTooMany CauseType = "FieldValueTooMany"
	// CauseTypeInternal is used to report other errors that are not related
	// to user input.  See InternalError().
	//
	// CauseTypeInternal 用于报告与用户输入无关的其他错误。
	CauseTypeInternal CauseType = "InternalError"
	// CauseTypeTypeInvalid is for the value did not match the schema type for that field
	//
	// CauseTypeTypeInvalid 表示值的类型与字段的 schema 类型不匹配。
	CauseTypeTypeInvalid CauseType = "FieldValueTypeInvalid"
	// CauseTypeUnexpectedServerResponse is used to report when the server responded to the client
	// without the expected return type. The presence of this cause indicates the error may be
	// due to an intervening proxy or the server software malfunctioning.
	//
	// CauseTypeUnexpectedServerResponse 表示服务器返回了非预期类型；出现该原因可能意味着
	// 中间代理介入或服务器软件故障。
	CauseTypeUnexpectedServerResponse CauseType = "UnexpectedServerResponse"
	// FieldManagerConflict is used to report when another client claims to manage this field,
	// It should only be returned for a request using server-side apply.
	//
	// CauseTypeFieldManagerConflict 表示另一个客户端声称管理该字段；只应出现在
	// server-side apply 请求的响应中。
	CauseTypeFieldManagerConflict CauseType = "FieldManagerConflict"
	// CauseTypeResourceVersionTooLarge is used to report that the requested resource version
	// is newer than the data observed by the API server, so the request cannot be served.
	//
	// CauseTypeResourceVersionTooLarge 表示请求的 resourceVersion 比 API server 观察到的数据更新，
	// 因此无法服务该请求。
	CauseTypeResourceVersionTooLarge CauseType = "ResourceVersionTooLarge"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// List holds a list of objects, which may not be known by the server.
//
// List 保存一组对象，服务器可能并不认识这些对象的类型。
type List struct {
	TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// 标准列表元数据。
	ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of objects
	//
	// Items 是对象列表。
	Items []runtime.RawExtension `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// APIVersions lists the versions that are available, to allow clients to
// discover the API at /api, which is the root path of the legacy v1 API.
//
// APIVersions 列出可用版本，使客户端能在 /api（遗留 v1 API 的根路径）发现 API。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type APIVersions struct {
	TypeMeta `json:""`
	// versions are the api versions that are available.
	// +listType=atomic
	//
	// Versions 是可用 API 版本列表。
	Versions []string `json:"versions" protobuf:"bytes,1,rep,name=versions"`
	// a map of client CIDR to server address that is serving this group.
	// This is to help clients reach servers in the most network-efficient way possible.
	// Clients can use the appropriate server address as per the CIDR that they match.
	// In case of multiple matches, clients should use the longest matching CIDR.
	// The server returns only those CIDRs that it thinks that the client can match.
	// For example: the master will return an internal IP CIDR only, if the client reaches the server using an internal IP.
	// Server looks at X-Forwarded-For header or X-Real-Ip header or request.RemoteAddr (in that order) to get the client IP.
	// +listType=atomic
	//
	// ServerAddressByClientCIDRs 把客户端 CIDR 映射到服务该组的服务器地址，帮助客户端以最省网络的方式
	// 访问服务器：客户端按匹配的 CIDR 选择地址，多个匹配时使用最长前缀。
	// 服务器只返回它认为客户端可匹配的 CIDR；例如客户端经内网 IP 访问时 master 只返回内网 CIDR。
	// 服务器按 X-Forwarded-For、X-Real-Ip、request.RemoteAddr 的顺序确定客户端 IP。
	ServerAddressByClientCIDRs []ServerAddressByClientCIDR `json:"serverAddressByClientCIDRs" protobuf:"bytes,2,rep,name=serverAddressByClientCIDRs"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIGroupList is a list of APIGroup, to allow clients to discover the API at
// /apis.
//
// APIGroupList 是 APIGroup 的列表，使客户端能在 /apis 发现 API。
type APIGroupList struct {
	TypeMeta `json:""`
	// groups is a list of APIGroup.
	// +listType=atomic
	//
	// Groups 是 APIGroup 列表。
	Groups []APIGroup `json:"groups" protobuf:"bytes,1,rep,name=groups"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIGroup contains the name, the supported versions, and the preferred version
// of a group.
//
// APIGroup 包含组的名称、支持的版本以及首选版本。
type APIGroup struct {
	TypeMeta `json:""`
	// name is the name of the group.
	//
	// Name 是组的名称。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// versions are the versions supported in this group.
	// +listType=atomic
	//
	// Versions 是该组支持的版本。
	Versions []GroupVersionForDiscovery `json:"versions" protobuf:"bytes,2,rep,name=versions"`
	// preferredVersion is the version preferred by the API server, which
	// probably is the storage version.
	// +optional
	//
	// PreferredVersion 是 API server 首选的版本，通常也是存储版本。
	PreferredVersion GroupVersionForDiscovery `json:"preferredVersion,omitempty" protobuf:"bytes,3,opt,name=preferredVersion"`
	// a map of client CIDR to server address that is serving this group.
	// This is to help clients reach servers in the most network-efficient way possible.
	// Clients can use the appropriate server address as per the CIDR that they match.
	// In case of multiple matches, clients should use the longest matching CIDR.
	// The server returns only those CIDRs that it thinks that the client can match.
	// For example: the master will return an internal IP CIDR only, if the client reaches the server using an internal IP.
	// Server looks at X-Forwarded-For header or X-Real-Ip header or request.RemoteAddr (in that order) to get the client IP.
	// +optional
	// +listType=atomic
	//
	// ServerAddressByClientCIDRs 把客户端 CIDR 映射到服务该组的服务器地址，语义同 APIVersions 中的同名字段。
	ServerAddressByClientCIDRs []ServerAddressByClientCIDR `json:"serverAddressByClientCIDRs,omitempty" protobuf:"bytes,4,rep,name=serverAddressByClientCIDRs"`
}

// ServerAddressByClientCIDR helps the client to determine the server address that they should use, depending on the clientCIDR that they match.
//
// ServerAddressByClientCIDR 帮助客户端根据匹配的 clientCIDR 确定应使用的服务器地址。
type ServerAddressByClientCIDR struct {
	// The CIDR with which clients can match their IP to figure out the server address that they should use.
	//
	// ClientCIDR 是客户端用于匹配自身 IP、以确定应使用哪个服务器地址的 CIDR。
	ClientCIDR string `json:"clientCIDR" protobuf:"bytes,1,opt,name=clientCIDR"`
	// Address of this server, suitable for a client that matches the above CIDR.
	// This can be a hostname, hostname:port, IP or IP:port.
	//
	// ServerAddress 是匹配上述 CIDR 的客户端应使用的服务器地址，可为 hostname、hostname:port、IP 或 IP:port。
	ServerAddress string `json:"serverAddress" protobuf:"bytes,2,opt,name=serverAddress"`
}

// GroupVersion contains the "group/version" and "version" string of a version.
// It is made a struct to keep extensibility.
//
// GroupVersionForDiscovery 包含某个版本的 "group/version" 与 "version" 字符串；
// 使用结构体是为了保留扩展性。
type GroupVersionForDiscovery struct {
	// groupVersion specifies the API group and version in the form "group/version"
	//
	// GroupVersion 以 "group/version" 形式指定 API 组与版本。
	GroupVersion string `json:"groupVersion" protobuf:"bytes,1,opt,name=groupVersion"`
	// version specifies the version in the form of "version". This is to save
	// the clients the trouble of splitting the GroupVersion.
	//
	// Version 以 "version" 形式单独给出，省去客户端拆分 GroupVersion 的麻烦。
	Version string `json:"version" protobuf:"bytes,2,opt,name=version"`
}

// APIResource specifies the name of a resource and whether it is namespaced.
//
// APIResource 描述资源名称以及它是否按 namespace 划分。
type APIResource struct {
	// name is the plural name of the resource.
	//
	// Name 是资源的复数名称。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// singularName is the singular name of the resource.  This allows clients to handle plural and singular opaquely.
	// The singularName is more correct for reporting status on a single item and both singular and plural are allowed
	// from the kubectl CLI interface.
	//
	// SingularName 是资源的单数名称，使客户端能统一处理单复数；
	// 对单个条目报告状态时更准确，kubectl 命令行两者都允许。
	SingularName string `json:"singularName" protobuf:"bytes,6,opt,name=singularName"`
	// namespaced indicates if a resource is namespaced or not.
	//
	// Namespaced 表示资源是否按 namespace 划分。
	Namespaced bool `json:"namespaced" protobuf:"varint,2,opt,name=namespaced"`
	// group is the preferred group of the resource.  Empty implies the group of the containing resource list.
	// For subresources, this may have a different value, for example: Scale".
	//
	// Group 是资源的首选组；为空表示与所在资源列表的组相同。
	// 对子资源而言可能不同，例如 Scale。
	Group string `json:"group,omitempty" protobuf:"bytes,8,opt,name=group"`
	// version is the preferred version of the resource.  Empty implies the version of the containing resource list
	// For subresources, this may have a different value, for example: v1 (while inside a v1beta1 version of the core resource's group)".
	//
	// Version 是资源的首选版本；为空表示与所在资源列表的版本相同。
	// 对子资源而言可能不同，例如核心资源组处于 v1beta1 时子资源可以是 v1。
	Version string `json:"version,omitempty" protobuf:"bytes,9,opt,name=version"`
	// kind is the kind for the resource (e.g. 'Foo' is the kind for a resource 'foo')
	//
	// Kind 是资源的类型（例如资源 'foo' 的 kind 为 'Foo'）。
	Kind string `json:"kind" protobuf:"bytes,3,opt,name=kind"`
	// verbs is a list of supported kube verbs (this includes get, list, watch, create,
	// update, patch, delete, deletecollection, and proxy)
	//
	// Verbs 是支持的 kube 动词列表（get、list、watch、create、update、patch、delete、
	// deletecollection 与 proxy）。
	Verbs Verbs `json:"verbs" protobuf:"bytes,4,opt,name=verbs"`
	// shortNames is a list of suggested short names of the resource.
	// +listType=atomic
	//
	// ShortNames 是建议的资源短名称列表。
	ShortNames []string `json:"shortNames,omitempty" protobuf:"bytes,5,rep,name=shortNames"`
	// categories is a list of the grouped resources this resource belongs to (e.g. 'all')
	// +listType=atomic
	//
	// Categories 是该资源所属的分组资源列表（例如 'all'）。
	Categories []string `json:"categories,omitempty" protobuf:"bytes,7,rep,name=categories"`
	// The hash value of the storage version, the version this resource is
	// converted to when written to the data store. Value must be treated
	// as opaque by clients. Only equality comparison on the value is valid.
	// This is an alpha feature and may change or be removed in the future.
	// The field is populated by the apiserver only if the
	// StorageVersionHash feature gate is enabled.
	// This field will remain optional even if it graduates.
	// +optional
	//
	// StorageVersionHash 是存储版本的哈希值（写入数据存储时资源被转换到的版本）。
	// 客户端必须视为不透明值，只能做相等比较。该 alpha 特性可能变化或移除；
	// 仅在开启 StorageVersionHash feature gate 时由 apiserver 填充；即使转正也保持可选。
	StorageVersionHash string `json:"storageVersionHash,omitempty" protobuf:"bytes,10,opt,name=storageVersionHash"`
}

// Verbs masks the value so protobuf can generate
//
// Verbs 是 []string 的包装，便于 protobuf 生成代码。
//
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type Verbs []string

// String 以字符串形式返回动词列表，便于日志与错误信息输出。
func (vs Verbs) String() string {
	return fmt.Sprintf("%v", []string(vs))
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIResourceList is a list of APIResource, it is used to expose the name of the
// resources supported in a specific group and version, and if the resource
// is namespaced.
//
// APIResourceList 是 APIResource 列表，用于公开特定 group/version 支持的资源名称及其是否按 namespace 划分。
type APIResourceList struct {
	TypeMeta `json:""`
	// groupVersion is the group and version this APIResourceList is for.
	//
	// GroupVersion 是本 APIResourceList 对应的组与版本。
	GroupVersion string `json:"groupVersion" protobuf:"bytes,1,opt,name=groupVersion"`
	// resources contains the name of the resources and if they are namespaced.
	// +listType=atomic
	//
	// APIResources 包含资源名称及其是否按 namespace 划分。
	APIResources []APIResource `json:"resources" protobuf:"bytes,2,rep,name=resources"`
}

// RootPaths lists the paths available at root.
// For example: "/healthz", "/apis".
//
// RootPaths 列出根路径下可用的路径，例如 "/healthz"、"/apis"。
type RootPaths struct {
	// paths are the paths available at root.
	// +listType=atomic
	//
	// Paths 是根路径下可用的路径列表。
	Paths []string `json:"paths" protobuf:"bytes,1,rep,name=paths"`
}

// TODO: remove me when watch is refactored
//
// LabelSelectorQueryParam 返回 label selector 的查询参数名；watch 重构后可移除。
func LabelSelectorQueryParam(version string) string {
	return "labelSelector"
}

// TODO: remove me when watch is refactored
//
// FieldSelectorQueryParam 返回 field selector 的查询参数名；watch 重构后可移除。
func FieldSelectorQueryParam(version string) string {
	return "fieldSelector"
}

// String returns available api versions as a human-friendly version string.
//
// String 以逗号分隔的人类可读形式返回可用 API 版本。
func (apiVersions APIVersions) String() string {
	return strings.Join(apiVersions.Versions, ",")
}

// GoString 与 String 相同，便于 %#v 格式化输出。
func (apiVersions APIVersions) GoString() string {
	return apiVersions.String()
}

// Patch is provided to give a concrete name and type to the Kubernetes PATCH request body.
//
// Patch 为 Kubernetes PATCH 请求体提供具体的名称与类型。
type Patch struct{}

// Note:
// There are two different styles of label selectors used in versioned types:
// an older style which is represented as just a string in versioned types, and a
// newer style that is structured.  LabelSelector is an internal representation for the
// latter style.
//
// 注意：版本化类型中有两种 label selector 风格——旧风格在版本化类型中只是字符串，
// 新风格是结构化的。LabelSelector 是后者的内部表示。

// A label selector is a label query over a set of resources. The result of matchLabels and
// matchExpressions are ANDed. An empty label selector matches all objects. A null
// label selector matches no objects.
//
// LabelSelector 是对一组资源的标签查询。matchLabels 与 matchExpressions 的结果按 AND 组合。
// 空的 label selector 匹配所有对象；null 的 label selector 不匹配任何对象。
//
// +structType=atomic
type LabelSelector struct {
	// matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels
	// map is equivalent to an element of matchExpressions, whose key field is "key", the
	// operator is "In", and the values array contains only "value". The requirements are ANDed.
	// +optional
	//
	// MatchLabels 是 {key,value} 映射；其中每个条目等价于 matchExpressions 中 key 为 "key"、
	// operator 为 "In"、values 仅含 "value" 的条目。所有要求按 AND 组合。
	MatchLabels map[string]string `json:"matchLabels,omitempty" protobuf:"bytes,1,rep,name=matchLabels"`
	// matchExpressions is a list of label selector requirements. The requirements are ANDed.
	// +optional
	// +listType=atomic
	//
	// MatchExpressions 是 label selector 要求列表，所有要求按 AND 组合。
	MatchExpressions []LabelSelectorRequirement `json:"matchExpressions,omitempty" protobuf:"bytes,2,rep,name=matchExpressions"`
}

// A label selector requirement is a selector that contains values, a key, and an operator that
// relates the key and values.
//
// LabelSelectorRequirement 是包含 values、key 以及关联二者的 operator 的选择器要求。
type LabelSelectorRequirement struct {
	// key is the label key that the selector applies to.
	//
	// Key 是选择器作用的标签键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
	// operator represents a key's relationship to a set of values.
	// Valid operators are In, NotIn, Exists and DoesNotExist.
	//
	// Operator 表示 key 与一组值的关系；合法取值为 In、NotIn、Exists、DoesNotExist。
	Operator LabelSelectorOperator `json:"operator" protobuf:"bytes,2,opt,name=operator,casttype=LabelSelectorOperator"`
	// values is an array of string values. If the operator is In or NotIn,
	// the values array must be non-empty. If the operator is Exists or DoesNotExist,
	// the values array must be empty. This array is replaced during a strategic
	// merge patch.
	// +optional
	// +listType=atomic
	//
	// Values 是字符串值数组。operator 为 In/NotIn 时必须非空；为 Exists/DoesNotExist 时必须为空。
	// 在 strategic merge patch 中该数组会被整体替换。
	Values []string `json:"values,omitempty" protobuf:"bytes,3,rep,name=values"`
}

// A label selector operator is the set of operators that can be used in a selector requirement.
//
// LabelSelectorOperator 是选择器要求中可用的运算符集合。
type LabelSelectorOperator string

const (
	// LabelSelectorOpIn 表示标签值属于给定集合。
	LabelSelectorOpIn LabelSelectorOperator = "In"
	// LabelSelectorOpNotIn 表示标签值不属于给定集合。
	LabelSelectorOpNotIn LabelSelectorOperator = "NotIn"
	// LabelSelectorOpExists 表示标签键存在。
	LabelSelectorOpExists LabelSelectorOperator = "Exists"
	// LabelSelectorOpDoesNotExist 表示标签键不存在。
	LabelSelectorOpDoesNotExist LabelSelectorOperator = "DoesNotExist"
)

// FieldSelectorRequirement is a selector that contains values, a key, and an operator that
// relates the key and values.
//
// FieldSelectorRequirement 是包含 values、key 以及关联二者的 operator 的字段选择器要求。
type FieldSelectorRequirement struct {
	// key is the field selector key that the requirement applies to.
	//
	// Key 是字段选择器作用的键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
	// operator represents a key's relationship to a set of values.
	// Valid operators are In, NotIn, Exists, DoesNotExist.
	// The list of operators may grow in the future.
	//
	// Operator 表示 key 与一组值的关系；合法取值为 In、NotIn、Exists、DoesNotExist，
	// 未来可能增加新的运算符。
	Operator FieldSelectorOperator `json:"operator" protobuf:"bytes,2,opt,name=operator,casttype=FieldSelectorOperator"`
	// values is an array of string values.
	// If the operator is In or NotIn, the values array must be non-empty.
	// If the operator is Exists or DoesNotExist, the values array must be empty.
	// +optional
	// +listType=atomic
	//
	// Values 是字符串值数组：operator 为 In/NotIn 时必须非空，为 Exists/DoesNotExist 时必须为空。
	Values []string `json:"values,omitempty" protobuf:"bytes,3,rep,name=values"`
}

// A field selector operator is the set of operators that can be used in a selector requirement.
//
// FieldSelectorOperator 是字段选择器要求中可用的运算符集合。
type FieldSelectorOperator string

const (
	// FieldSelectorOpIn 表示字段值属于给定集合。
	FieldSelectorOpIn FieldSelectorOperator = "In"
	// FieldSelectorOpNotIn 表示字段值不属于给定集合。
	FieldSelectorOpNotIn FieldSelectorOperator = "NotIn"
	// FieldSelectorOpExists 表示字段存在。
	FieldSelectorOpExists FieldSelectorOperator = "Exists"
	// FieldSelectorOpDoesNotExist 表示字段不存在。
	FieldSelectorOpDoesNotExist FieldSelectorOperator = "DoesNotExist"
)

// ManagedFieldsEntry is a workflow-id, a FieldSet and the group version of the resource
// that the fieldset applies to.
//
// ManagedFieldsEntry 由 workflow-id、FieldSet 以及该 FieldSet 适用资源的 group/version 组成。
type ManagedFieldsEntry struct {
	// Manager is an identifier of the workflow managing these fields.
	//
	// Manager 是管理这些字段的 workflow 标识。
	Manager string `json:"manager,omitempty" protobuf:"bytes,1,opt,name=manager"`
	// Operation is the type of operation which lead to this ManagedFieldsEntry being created.
	// The only valid values for this field are 'Apply' and 'Update'.
	// +k8s:alpha(since: "1.37")=+k8s:required
	//
	// Operation 是产生该条目时的操作类型，合法值只有 'Apply' 与 'Update'。
	Operation ManagedFieldsOperationType `json:"operation,omitempty" protobuf:"bytes,2,opt,name=operation,casttype=ManagedFieldsOperationType"`
	// APIVersion defines the version of this resource that this field set
	// applies to. The format is "group/version" just like the top-level
	// APIVersion field. It is necessary to track the version of a field
	// set because it cannot be automatically converted.
	//
	// APIVersion 定义该字段集合适用的资源版本，格式同顶层 APIVersion（"group/version"）。
	// 字段集合无法自动转换，因此必须记录其版本。
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,3,opt,name=apiVersion"`
	// Time is the timestamp of when the ManagedFields entry was added. The
	// timestamp will also be updated if a field is added, the manager
	// changes any of the owned fields value or removes a field. The
	// timestamp does not update when a field is removed from the entry
	// because another manager took it over.
	// +optional
	//
	// Time 是该条目被添加的时间戳。新增字段、修改所拥有字段的值或删除字段时都会更新；
	// 但字段因被其他 manager 接管而移除时不会更新。
	Time *Time `json:"time,omitempty" protobuf:"bytes,4,opt,name=time"`

	// Fields is tombstoned to show why 5 is a reserved protobuf tag.
	//Fields *Fields `json:"fields,omitempty" protobuf:"bytes,5,opt,name=fields,casttype=Fields"`
	//
	// Fields 已墓碑化，用于说明 protobuf tag 5 为何被保留。

	// FieldsType is the discriminator for the different fields format and version.
	// There is currently only one possible value: "FieldsV1"
	//
	// FieldsType 用于区分不同的字段格式与版本，目前只有 "FieldsV1" 一种取值。
	FieldsType string `json:"fieldsType,omitempty" protobuf:"bytes,6,opt,name=fieldsType"`
	// FieldsV1 holds the first JSON version format as described in the "FieldsV1" type.
	// +optional
	//
	// FieldsV1 保存 "FieldsV1" 类型描述的第一版 JSON 格式。
	FieldsV1 *FieldsV1 `json:"fieldsV1,omitempty" protobuf:"bytes,7,opt,name=fieldsV1"`

	// Subresource is the name of the subresource used to update that object, or
	// empty string if the object was updated through the main resource. The
	// value of this field is used to distinguish between managers, even if they
	// share the same name. For example, a status update will be distinct from a
	// regular update using the same manager name.
	// Note that the APIVersion field is not related to the Subresource field and
	// it always corresponds to the version of the main resource.
	//
	// Subresource 是更新对象所用子资源的名称，通过主资源更新时为空。
	// 该字段用于区分同名 manager（例如同名 manager 的 status 更新与普通更新视为不同）。
	// 注意 APIVersion 与 Subresource 无关，始终对应主资源的版本。
	Subresource string `json:"subresource,omitempty" protobuf:"bytes,8,opt,name=subresource"`
}

// ManagedFieldsOperationType is the type of operation which lead to a ManagedFieldsEntry being created.
//
// ManagedFieldsOperationType 是产生 ManagedFieldsEntry 的操作类型。
//
// +k8s:alpha(since: "1.37")=+k8s:enum
type ManagedFieldsOperationType string

const (
	// ManagedFieldsOperationApply 表示该条目由 server-side apply 产生。
	ManagedFieldsOperationApply ManagedFieldsOperationType = "Apply"
	// ManagedFieldsOperationUpdate 表示该条目由 update 产生。
	ManagedFieldsOperationUpdate ManagedFieldsOperationType = "Update"
)

// TODO: Table does not generate to protobuf because of the interface{} - fix protobuf
//   generation to support a meta type that can accept any valid JSON. This can be introduced
//   in a v1 because clients a) receive an error if they try to access proto today, and b)
//   once introduced they would be able to gracefully switch over to using it.

// Table is a tabular representation of a set of API resources. The server transforms the
// object into a set of preferred columns for quickly reviewing the objects.
//
// Table 是一组 API 资源的表格表示：服务器把对象转换为便于快速浏览的首选列集合。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +protobuf=false
type Table struct {
	TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// 标准列表元数据。
	ListMeta `json:"metadata,omitempty"`

	// columnDefinitions describes each column in the returned items array. The number of cells per row
	// will always match the number of column definitions.
	// +listType=atomic
	//
	// ColumnDefinitions 描述返回 items 数组中的每一列；每行的 cell 数量始终与列定义数量一致。
	ColumnDefinitions []TableColumnDefinition `json:"columnDefinitions"`
	// rows is the list of items in the table.
	// +listType=atomic
	//
	// Rows 是表格中的行列表。
	Rows []TableRow `json:"rows"`
}

// TableColumnDefinition contains information about a column returned in the Table.
//
// TableColumnDefinition 描述 Table 中返回的某一列。
//
// +protobuf=false
type TableColumnDefinition struct {
	// name is a human readable name for the column.
	//
	// Name 是列的人类可读名称。
	Name string `json:"name"`
	// type is an OpenAPI type definition for this column, such as number, integer, string, or
	// array.
	// See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types for more.
	//
	// Type 是该列的 OpenAPI 类型定义，如 number、integer、string 或 array。
	Type string `json:"type"`
	// format is an optional OpenAPI type modifier for this column. A format modifies the type and
	// imposes additional rules, like date or time formatting for a string. The 'name' format is applied
	// to the primary identifier column which has type 'string' to assist in clients identifying column
	// is the resource name.
	// See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types for more.
	//
	// Format 是可选列修饰符，在 Type 基础上附加规则（如字符串的日期/时间格式）。
	// 'name' 格式用于类型为 'string' 的主标识列，帮助客户端识别该列是资源名称。
	Format string `json:"format"`
	// description is a human readable description of this column.
	//
	// Description 是该列的人类可读描述。
	Description string `json:"description"`
	// priority is an integer defining the relative importance of this column compared to others. Lower
	// numbers are considered higher priority. Columns that may be omitted in limited space scenarios
	// should be given a higher priority.
	//
	// Priority 定义该列相对其他列的重要性：数值越小优先级越高；
	// 在空间受限时可能被省略的列应赋予较大的 priority 值。
	Priority int32 `json:"priority"`
}

// TableRow is an individual row in a table.
//
// TableRow 是表格中的一行。
//
// +protobuf=false
type TableRow struct {
	// cells will be as wide as the column definitions array and may contain strings, numbers (float64 or
	// int64), booleans, simple maps, lists, or null. See the type field of the column definition for a
	// more detailed description.
	// +listType=atomic
	//
	// Cells 宽度与列定义数组一致，可包含字符串、数字（float64 或 int64）、布尔值、简单 map、
	// 列表或 null；具体类型见列定义的 type 字段。
	Cells []interface{} `json:"cells"`
	// conditions describe additional status of a row that are relevant for a human user. These conditions
	// apply to the row, not to the object, and will be specific to table output. The only defined
	// condition type is 'Completed', for a row that indicates a resource that has run to completion and
	// can be given less visual priority.
	// +optional
	// +listType=atomic
	//
	// Conditions 描述该行对人类用户有意义的附加状态；它作用于行而非对象，仅用于表格输出。
	// 目前唯一定义的条件类型是 'Completed'，表示该资源已运行完成，可降低视觉优先级。
	Conditions []TableRowCondition `json:"conditions,omitempty"`
	// This field contains the requested additional information about each object based on the includeObject
	// policy when requesting the Table. If "None", this field is empty, if "Object" this will be the
	// default serialization of the object for the current API version, and if "Metadata" (the default) will
	// contain the object metadata. Check the returned kind and apiVersion of the object before parsing.
	// The media type of the object will always match the enclosing list - if this as a JSON table, these
	// will be JSON encoded objects.
	// +optional
	//
	// Object 按请求 Table 时的 includeObject 策略携带对象信息：
	// "None" 时为空，"Object" 时为当前 API 版本的默认对象序列化，
	// "Metadata"（默认）时仅含对象元数据。解析前请先检查返回对象的 kind 与 apiVersion。
	// 对象的媒体类型始终与所在列表一致（如 JSON 表格中即为 JSON 编码对象）。
	Object runtime.RawExtension `json:"object,omitempty"`
}

// TableRowCondition allows a row to be marked with additional information.
//
// TableRowCondition 用于给行附加额外信息。
//
// +protobuf=false
type TableRowCondition struct {
	// Type of row condition. The only defined value is 'Completed' indicating that the
	// object this row represents has reached a completed state and may be given less visual
	// priority than other rows. Clients are not required to honor any conditions but should
	// be consistent where possible about handling the conditions.
	//
	// Type 是行条件类型，目前唯一定义的值是 'Completed'，表示该行代表的对象已完成，
	// 可降低视觉优先级。客户端不必处理所有条件，但处理方式应尽量一致。
	Type RowConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	//
	// Status 是条件状态，取值为 True、False、Unknown 之一。
	Status ConditionStatus `json:"status"`
	// (brief) machine readable reason for the condition's last transition.
	// +optional
	//
	// Reason 是条件最近一次变更的（简短）机器可读原因。
	Reason string `json:"reason,omitempty"`
	// Human readable message indicating details about last transition.
	// +optional
	//
	// Message 是最近一次变更细节的人类可读信息。
	Message string `json:"message,omitempty"`
}

// RowConditionType 是表格行条件类型的枚举。
type RowConditionType string

// These are valid conditions of a row. This list is not exhaustive and new conditions may be
// included by other resources.
const (
	// RowCompleted means the underlying resource has reached completion and may be given less
	// visual priority than other resources.
	//
	// RowCompleted 表示底层资源已完成，可降低视觉优先级。
	RowCompleted RowConditionType = "Completed"
)

// +k8s:alpha(since: "1.37")=+k8s:enum
type ConditionStatus string

// These are valid condition statuses. "ConditionTrue" means a resource is in the condition.
// "ConditionFalse" means a resource is not in the condition. "ConditionUnknown" means kubernetes
// can't decide if a resource is in the condition or not. In the future, we could add other
// intermediate conditions, e.g. ConditionDegraded.
const (
	// ConditionTrue 表示资源处于该条件。
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示资源不处于该条件。
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示 Kubernetes 无法判断资源是否处于该条件。
	ConditionUnknown ConditionStatus = "Unknown"
)

// IncludeObjectPolicy controls which portion of the object is returned with a Table.
//
// IncludeObjectPolicy 控制 Table 中返回对象的哪一部分。
type IncludeObjectPolicy string

const (
	// IncludeNone returns no object.
	//
	// IncludeNone 不返回对象。
	IncludeNone IncludeObjectPolicy = "None"
	// IncludeMetadata serializes the object containing only its metadata field.
	//
	// IncludeMetadata 仅序列化对象的 metadata 字段。
	IncludeMetadata IncludeObjectPolicy = "Metadata"
	// IncludeObject contains the full object.
	//
	// IncludeObject 返回完整对象。
	IncludeObject IncludeObjectPolicy = "Object"
)

// TableOptions are used when a Table is requested by the caller.
//
// TableOptions 在调用方请求 Table 时使用。
//
// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TableOptions struct {
	TypeMeta `json:""`

	// NoHeaders is only exposed for internal callers. It is not included in our OpenAPI definitions
	// and may be removed as a field in a future release.
	//
	// NoHeaders 仅供内部调用方使用，不包含在 OpenAPI 定义中，未来版本可能移除。
	NoHeaders bool `json:"-"`

	// includeObject decides whether to include each object along with its columnar information.
	// Specifying "None" will return no object, specifying "Object" will return the full object contents, and
	// specifying "Metadata" (the default) will return the object's metadata in the PartialObjectMetadata kind
	// in version v1beta1 of the meta.k8s.io API group.
	//
	// IncludeObject 决定是否在列信息之外附带对象本身："None" 不返回对象；"Object" 返回完整对象；
	// "Metadata"（默认）以 meta.k8s.io/v1beta1 的 PartialObjectMetadata 类型返回对象元数据。
	IncludeObject IncludeObjectPolicy `json:"includeObject,omitempty" protobuf:"bytes,1,opt,name=includeObject,casttype=IncludeObjectPolicy"`
}

// PartialObjectMetadata is a generic representation of any object with ObjectMeta. It allows clients
// to get access to a particular ObjectMeta schema without knowing the details of the version.
//
// PartialObjectMetadata 是任何带 ObjectMeta 的对象的通用表示，
// 使客户端无需了解具体版本细节即可访问特定 ObjectMeta schema。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadata struct {
	TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	//
	// 标准对象元数据。
	ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

// PartialObjectMetadataList contains a list of objects containing only their metadata
//
// PartialObjectMetadataList 是仅含元数据的对象列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadataList struct {
	TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	//
	// 标准列表元数据。
	ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items contains each of the included items.
	//
	// Items 包含列表中的每一项。
	Items []PartialObjectMetadata `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// Condition contains details for one aspect of the current state of this API Resource.
// ---
// This struct is intended for direct use as an array at the field path .status.conditions.  For example,
//
//	type FooStatus struct{
//	    // Represents the observations of a foo's current state.
//	    // Known .status.conditions.type are: "Available", "Progressing", and "Degraded"
//	    // +patchMergeKey=type
//	    // +patchStrategy=merge
//	    // +listType=map
//	    // +listMapKey=type
//	    // +k8s:alpha(since: "1.37")=+k8s:optional
//	    // +k8s:alpha(since: "1.37")=+k8s:listType=map
//	    // +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
//	    Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
//
//	    // other fields
//	}
//
// Condition 描述该 API 资源当前状态某一方面的细节。
// 该结构体设计为直接以数组形式用于 .status.conditions 字段路径，示例见上。
type Condition struct {
	// type of condition in CamelCase or in foo.example.com/CamelCase.
	// ---
	// Many .condition.type values are consistent across resources like Available, but because arbitrary conditions can be
	// useful (see .node.status.conditions), the ability to deconflict is important.
	// The regex it matches is (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	// +k8s:alpha(since: "1.37")=+k8s:required
	//
	// Type 是 CamelCase 或 foo.example.com/CamelCase 形式的条件类型。
	// 许多条件类型在各资源间保持一致（如 Available），但任意条件也有其价值，
	// 因此需要能通过域名前缀消歧。匹配的正则见上。
	Type string `json:"type" protobuf:"bytes,1,opt,name=type"`
	// status of the condition, one of True, False, Unknown.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=True;False;Unknown
	// +k8s:alpha(since: "1.37")=+k8s:required
	//
	// Status 是条件状态，取值为 True、False、Unknown 之一。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`
	// observedGeneration represents the .metadata.generation that the condition was set based upon.
	// For instance, if .metadata.generation is currently 12, but the .status.conditions[x].observedGeneration is 9, the condition is out of date
	// with respect to the current state of the instance.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:minimum=0
	//
	// ObservedGeneration 是设置该条件所依据的 .metadata.generation。
	// 例如当前 .metadata.generation 为 12 而该值为 9，说明条件相对于实例当前状态已过期。
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`
	// lastTransitionTime is the last time the condition transitioned from one status to another.
	// This should be when the underlying condition changed.  If that is not known, then using the time when the API field changed is acceptable.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=date-time
	// +k8s:alpha(since: "1.37")=+k8s:customValidation
	//
	// LastTransitionTime 是条件状态最近一次变化的时间；应取底层条件实际变化的时间，
	// 无法获知时可用 API 字段变更的时间。
	LastTransitionTime Time `json:"lastTransitionTime" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// reason contains a programmatic identifier indicating the reason for the condition's last transition.
	// Producers of specific condition types may define expected values and meanings for this field,
	// and whether the values are considered a guaranteed API.
	// The value should be a CamelCase string.
	// This field may not be empty.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`
	// +k8s:alpha(since: "1.37")=+k8s:required
	// +k8s:alpha(since: "1.37")=+k8s:maxBytes=1024
	//
	// Reason 是表示条件最近一次变更原因的程序化标识，应为 CamelCase 且不可为空。
	// 各条件类型的生产者可定义期望取值及其含义，以及这些值是否属于受保证的 API。
	Reason string `json:"reason" protobuf:"bytes,5,opt,name=reason"`
	// message is a human readable message indicating details about the transition.
	// This may be an empty string.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=32768
	//
	// Message 是描述变更细节的人类可读信息，可以为空字符串。
	Message string `json:"message" protobuf:"bytes,6,opt,name=message"`
}
