/*
Copyright 2014 The Kubernetes Authors.

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

package fake

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	core "k8s.io/client-go/testing"
)

// Deprecated: use CreateWithEventNamespaceWithContext instead.
//
// 已弃用：请改用 CreateWithEventNamespaceWithContext；旧方法无法取消或控制超时。
func (c *fakeEvents) CreateWithEventNamespace(event *v1.Event) (*v1.Event, error) {
	return c.CreateWithEventNamespaceWithContext(context.Background(), event)
}

// CreateWithEventNamespaceWithContext 在事件所属命名空间创建 Event。
// 之所以单独提供该方法，是因为 Event 的命名空间由 involvedObject 决定，
// 而非客户端绑定的命名空间，因此需要按 Event 自带的 namespace 路由。
func (c *fakeEvents) CreateWithEventNamespaceWithContext(_ context.Context, event *v1.Event) (*v1.Event, error) {
	var action core.CreateActionImpl
	// 若客户端已绑定命名空间则以其为准，否则采用 Event 自身命名空间，兼容集群级用法。
	if c.Namespace() != "" {
		action = core.NewCreateAction(c.Resource(), c.Namespace(), event)
	} else {
		action = core.NewCreateAction(c.Resource(), event.GetNamespace(), event)
	}
	obj, err := c.Fake.Invokes(action, event)
	if obj == nil {
		return nil, err
	}

	return obj.(*v1.Event), err
}

// Update replaces an existing event. Returns the copy of the event the server returns, or an error.
//
// Deprecated: use UpdateWithEventNamespaceWithContext instead.
//
// 已弃用：请改用 UpdateWithEventNamespaceWithContext。
func (c *fakeEvents) UpdateWithEventNamespace(event *v1.Event) (*v1.Event, error) {
	return c.UpdateWithEventNamespaceWithContext(context.Background(), event)
}

// Update replaces an existing event. Returns the copy of the event the server returns, or an error.
func (c *fakeEvents) UpdateWithEventNamespaceWithContext(_ context.Context, event *v1.Event) (*v1.Event, error) {
	var action core.UpdateActionImpl
	// 与 Create 相同：优先使用客户端命名空间，否则使用 Event 自带命名空间。
	if c.Namespace() != "" {
		action = core.NewUpdateAction(c.Resource(), c.Namespace(), event)
	} else {
		action = core.NewUpdateAction(c.Resource(), event.GetNamespace(), event)
	}
	obj, err := c.Fake.Invokes(action, event)
	if obj == nil {
		return nil, err
	}

	return obj.(*v1.Event), err
}

// PatchWithEventNamespace patches an existing event. Returns the copy of the event the server returns, or an error.
// TODO: Should take a PatchType as an argument probably.
//
// Deprecated: use PatchWithEventNamespaceWithContext instead.
//
// 已弃用：请改用 PatchWithEventNamespaceWithContext。
func (c *fakeEvents) PatchWithEventNamespace(event *v1.Event, data []byte) (*v1.Event, error) {
	return c.PatchWithEventNamespaceWithContext(context.Background(), event, data)
}

// PatchWithEventNamespaceWithContext patches an existing event. Returns the copy of the event the server returns, or an error.
// TODO: Should take a PatchType as an argument probably.
func (c *fakeEvents) PatchWithEventNamespaceWithContext(_ context.Context, event *v1.Event, data []byte) (*v1.Event, error) {
	// TODO: 应可配置以支持更多补丁策略。
	// 当前 fake 使用战略合并补丁，与真实客户端对 Event 的默认补丁策略一致。
	pt := types.StrategicMergePatchType
	var action core.PatchActionImpl
	if c.Namespace() != "" {
		action = core.NewPatchAction(c.Resource(), c.Namespace(), event.Name, pt, data)
	} else {
		action = core.NewPatchAction(c.Resource(), event.GetNamespace(), event.Name, pt, data)
	}
	obj, err := c.Fake.Invokes(action, event)
	if obj == nil {
		return nil, err
	}

	return obj.(*v1.Event), err
}

// Search returns a list of events matching the specified object.
//
// Deprecated: use SearchWithContext instead.
//
// 已弃用：请改用 SearchWithContext。
func (c *fakeEvents) Search(scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error) {
	return c.SearchWithContext(context.Background(), scheme, objOrRef)
}

// SearchWithContext returns a list of events matching the specified object.
//
// SearchWithContext 在 fake 中仅做一次 List 并交给 reactor 处理；
// 真实的字段筛选（involvedObject）由 GetFieldSelector 以 "everything" 的形式近似。
func (c *fakeEvents) SearchWithContext(_ context.Context, scheme *runtime.Scheme, objOrRef runtime.Object) (*v1.EventList, error) {
	var action core.ListActionImpl
	// 无显式命名空间时退回到 default，因为 Event 总是命名空间作用域对象。
	if c.Namespace() != "" {
		action = core.NewListAction(c.Resource(), c.Kind(), c.Namespace(), metav1.ListOptions{})
	} else {
		action = core.NewListAction(c.Resource(), c.Kind(), v1.NamespaceDefault, metav1.ListOptions{})
	}
	obj, err := c.Fake.Invokes(action, &v1.EventList{})
	if obj == nil {
		return nil, err
	}

	return obj.(*v1.EventList), err
}

// GetFieldSelector 本应把 involvedObject 相关参数转换为 field selector，
// 但 fake 不模拟字段筛选，仅发出一个 "get-field-selector" 动作供 reactor 观察，
// 随后返回匹配一切的 selector（相当于不做过滤）。
func (c *fakeEvents) GetFieldSelector(involvedObjectName, involvedObjectNamespace, involvedObjectKind, involvedObjectUID *string) fields.Selector {
	action := core.GenericActionImpl{}
	action.Verb = "get-field-selector"
	action.Resource = c.Resource()

	c.Fake.Invokes(action, nil)
	return fields.Everything()
}
