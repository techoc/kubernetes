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

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ref "k8s.io/client-go/tools/reference"
)

// The EventExpansion interface allows manually adding extra methods to the EventInterface.
//
// EventExpansion 为 EventInterface 手工补充方法。
// 这些方法之所以单独定义，是因为它们不属于代码生成的 CRUD 模板：事件的 namespace
// 来自 Event 对象本身，而不是客户端的 namespace。
type EventExpansion interface {
	// CreateWithEventNamespace is the same as a Create, except that it sends the request to the event.Namespace.
	//
	// Deprecated: use CreateWithEventNamespaceWithContext instead.
	//
	// CreateWithEventNamespace 与 Create 相同，但请求发往 event.Namespace。
	// 已弃用：请改用 CreateWithEventNamespaceWithContext。
	CreateWithEventNamespace(event *v1.Event) (*v1.Event, error)
	// UpdateWithEventNamespace is the same as a Update, except that it sends the request to the event.Namespace.
	//
	// Deprecated: use UpdateWithEventNamespaceWithContext instead.
	//
	// 已弃用：请改用 UpdateWithEventNamespaceWithContext。
	UpdateWithEventNamespace(event *v1.Event) (*v1.Event, error)
	// PatchWithEventNamespace is the same as a Patch, except that it sends the request to the event.Namespace.
	//
	// Deprecated: use PatchWithEventNamespaceWithContext instead.
	//
	// 已弃用：请改用 PatchWithEventNamespaceWithContext。
	PatchWithEventNamespace(event *v1.Event, data []byte) (*v1.Event, error)
	// Search finds events about the specified object
	//
	// Deprecated: use SearchWithContext instead.
	//
	// Search 查找指定对象相关的事件。
	// 已弃用：请改用 SearchWithContext。
	Search(scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error)
	// CreateWithEventNamespaceWithContext is the same as a Create, except that it sends the request to the event.Namespace.
	CreateWithEventNamespaceWithContext(ctx context.Context, event *v1.Event) (*v1.Event, error)
	// UpdateWithEventNamespaceWithContext is the same as a Update, except that it sends the request to the event.Namespace.
	UpdateWithEventNamespaceWithContext(ctx context.Context, event *v1.Event) (*v1.Event, error)
	// PatchWithEventNamespaceWithContext is the same as a Patch, except that it sends the request to the event.Namespace.
	PatchWithEventNamespaceWithContext(ctx context.Context, event *v1.Event, data []byte) (*v1.Event, error)
	// SearchWithContext finds events about the specified object
	SearchWithContext(ctx context.Context, scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error)
	// Returns the appropriate field selector based on the API version being used to communicate with the server.
	// The returned field selector can be used with List and Watch to filter desired events.
	//
	// GetFieldSelector 返回适合当前 API 版本的 field selector，
	// 可传给 List/Watch 以过滤出与涉及对象相关的事件。
	GetFieldSelector(involvedObjectName, involvedObjectNamespace, involvedObjectKind, involvedObjectUID *string) fields.Selector
}

// CreateWithEventNamespace makes a new event. Returns the copy of the event the server returns,
// or an error. The namespace to create the event within is deduced from the
// event; it must either match this event client's namespace, or this event
// client must have been created with the "" namespace.
//
// Deprecated: use CreateWithEventNamespaceWithContext instead.
//
// 新建事件，返回服务端返回的副本。事件所在 namespace 取自 event 本身，
// 但它必须与客户端 namespace 一致，或客户端是以 "" namespace 创建的（跨 namespace）。
func (e *events) CreateWithEventNamespace(event *v1.Event) (*v1.Event, error) {
	return e.CreateWithEventNamespaceWithContext(context.Background(), event)
}

// CreateWithEventNamespaceWithContext makes a new event. Returns the copy of the event the server returns,
// or an error. The namespace to create the event within is deduced from the
// event; it must either match this event client's namespace, or this event
// client must have been created with the "" namespace.
func (e *events) CreateWithEventNamespaceWithContext(ctx context.Context, event *v1.Event) (*v1.Event, error) {
	// 绑定到具体 namespace 的客户端不允许把事件写到别的 namespace，防止越权/误写。
	if e.GetNamespace() != "" && event.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't create an event with namespace '%v' in namespace '%v'", event.Namespace, e.GetNamespace())
	}
	result := &v1.Event{}
	err := e.GetClient().Post().
		NamespaceIfScoped(event.Namespace, len(event.Namespace) > 0).
		Resource("events").
		Body(event).
		Do(ctx).
		Into(result)
	return result, err
}

// UpdateWithEventNamespace modifies an existing event. It returns the copy of the event that the server returns,
// or an error. The namespace and key to update the event within is deduced from the event. The
// namespace must either match this event client's namespace, or this event client must have been
// created with the "" namespace. Update also requires the ResourceVersion to be set in the event
// object.
//
// Deprecated: use UpdateWithEventNamespaceWithContext instead.
//
// 修改已存在的事件。namespace 与 name 取自 event；resourceVersion 必须已设置，
// 否则服务端会拒绝本次更新。
func (e *events) UpdateWithEventNamespace(event *v1.Event) (*v1.Event, error) {
	return e.UpdateWithEventNamespaceWithContext(context.Background(), event)
}

// UpdateWithEventNamespaceWithContext modifies an existing event. It returns the copy of the event that the server returns,
// or an error. The namespace and key to update the event within is deduced from the event. The
// namespace must either match this event client's namespace, or this event client must have been
// created with the "" namespace. Update also requires the ResourceVersion to be set in the event
// object.
func (e *events) UpdateWithEventNamespaceWithContext(ctx context.Context, event *v1.Event) (*v1.Event, error) {
	if e.GetNamespace() != "" && event.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't update an event with namespace '%v' in namespace '%v'", event.Namespace, e.GetNamespace())
	}
	result := &v1.Event{}
	err := e.GetClient().Put().
		NamespaceIfScoped(event.Namespace, len(event.Namespace) > 0).
		Resource("events").
		Name(event.Name).
		Body(event).
		Do(ctx).
		Into(result)
	return result, err
}

// PatchWithEventNamespace modifies an existing event. It returns the copy of
// the event that the server returns, or an error. The namespace and name of the
// target event is deduced from the incompleteEvent. The namespace must either
// match this event client's namespace, or this event client must have been
// created with the "" namespace.
//
// Deprecated: use PatchWithEventNamespaceWithContext instead.
//
// 对已存在的事件打补丁（strategic merge patch）；namespace 与 name 取自 incompleteEvent。
func (e *events) PatchWithEventNamespace(incompleteEvent *v1.Event, data []byte) (*v1.Event, error) {
	return e.PatchWithEventNamespaceWithContext(context.Background(), incompleteEvent, data)
}

// PatchWithEventNamespaceWithContext modifies an existing event. It returns the copy of
// the event that the server returns, or an error. The namespace and name of the
// target event is deduced from the incompleteEvent. The namespace must either
// match this event client's namespace, or this event client must have been
// created with the "" namespace.
func (e *events) PatchWithEventNamespaceWithContext(ctx context.Context, incompleteEvent *v1.Event, data []byte) (*v1.Event, error) {
	if e.GetNamespace() != "" && incompleteEvent.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't patch an event with namespace '%v' in namespace '%v'", incompleteEvent.Namespace, e.GetNamespace())
	}
	result := &v1.Event{}
	err := e.GetClient().Patch(types.StrategicMergePatchType).
		NamespaceIfScoped(incompleteEvent.Namespace, len(incompleteEvent.Namespace) > 0).
		Resource("events").
		Name(incompleteEvent.Name).
		Body(data).
		Do(ctx).
		Into(result)
	return result, err
}

// Search finds events about the specified object. The namespace of the
// object must match this event's client namespace unless the event client
// was made with the "" namespace.
//
// Deprecated: use SearchWithContext instead.
//
// 查找与指定对象相关的事件；对象 namespace 必须与客户端 namespace 一致，
// 除非客户端是以 "" namespace 创建的。
func (e *events) Search(scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error) {
	return e.SearchWithContext(context.Background(), scheme, objOrRef)
}

// SearchWithContext finds events about the specified object. The namespace of the
// object must match this event's client namespace unless the event client
// was made with the "" namespace.
func (e *events) SearchWithContext(ctx context.Context, scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error) {
	ref, err := ref.GetReference(scheme, objOrRef)
	if err != nil {
		return nil, err
	}
	if len(e.GetNamespace()) > 0 && ref.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("won't be able to find any events of namespace '%v' in namespace '%v'", ref.Namespace, e.GetNamespace())
	}
	// kind/uid 为空时留空指针，让 field selector 不带上对应条件。
	stringRefKind := string(ref.Kind)
	var refKind *string
	if len(stringRefKind) > 0 {
		refKind = &stringRefKind
	}
	stringRefUID := string(ref.UID)
	var refUID *string
	if len(stringRefUID) > 0 {
		refUID = &stringRefUID
	}
	fieldSelector := e.GetFieldSelector(&ref.Name, &ref.Namespace, refKind, refUID)
	return e.List(ctx, metav1.ListOptions{FieldSelector: fieldSelector.String()})
}

// Returns the appropriate field selector based on the API version being used to communicate with the server.
// The returned field selector can be used with List and Watch to filter desired events.
//
// GetFieldSelector 根据传入的非空字段构造 involvedObject.* 的 field selector。
func (e *events) GetFieldSelector(involvedObjectName, involvedObjectNamespace, involvedObjectKind, involvedObjectUID *string) fields.Selector {
	field := fields.Set{}
	if involvedObjectName != nil {
		field["involvedObject.name"] = *involvedObjectName
	}
	if involvedObjectNamespace != nil {
		field["involvedObject.namespace"] = *involvedObjectNamespace
	}
	if involvedObjectKind != nil {
		field["involvedObject.kind"] = *involvedObjectKind
	}
	if involvedObjectUID != nil {
		field["involvedObject.uid"] = *involvedObjectUID
	}
	return field.AsSelector()
}

// Returns the appropriate field label to use for name of the involved object as per the given API version.
// DEPRECATED: please use "involvedObject.name" inline.
//
// GetInvolvedObjectNameFieldLabel 返回涉及对象 name 的字段标签。
// 已弃用：所有版本统一使用 "involvedObject.name"，因此 version 参数已无作用，
// 仅为兼容保留。
func GetInvolvedObjectNameFieldLabel(version string) string {
	return "involvedObject.name"
}

// TODO: This is a temporary arrangement and will be removed once all clients are moved to use the clientset.
//
// EventSinkImpl 把事件 sink 接口适配到 EventInterface；
// 待所有客户端迁移到 clientset 后会移除。
type EventSinkImpl struct {
	Interface EventInterface
}

// Create 通过 CreateWithEventNamespace 创建事件。
func (e *EventSinkImpl) Create(event *v1.Event) (*v1.Event, error) {
	return e.Interface.CreateWithEventNamespace(event)
}

// Update 通过 UpdateWithEventNamespace 更新事件。
func (e *EventSinkImpl) Update(event *v1.Event) (*v1.Event, error) {
	return e.Interface.UpdateWithEventNamespace(event)
}

// Patch 通过 PatchWithEventNamespace 打补丁。
func (e *EventSinkImpl) Patch(event *v1.Event, data []byte) (*v1.Event, error) {
	return e.Interface.PatchWithEventNamespace(event, data)
}
