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

package v1beta1

import (
	"context"
	"fmt"

	"k8s.io/api/events/v1beta1"
	"k8s.io/apimachinery/pkg/types"
)

// EventExpansion 为代码生成的 EventInterface 追加手写方法。
// TODO: 后续为 event expansion 补充查询方法。
//
// 这些方法的命名空间取自 event 对象本身，而不是客户端绑定时的命名空间，
// 用于事件记录器向“事件实际发生”的命名空间写入。
type EventExpansion interface {
	// CreateWithEventNamespace 与 Create 相同，但请求发往 event.Namespace。
	//
	// 中文：按 event.Namespace 创建事件。
	CreateWithEventNamespace(event *v1beta1.Event) (*v1beta1.Event, error)
	// UpdateWithEventNamespace 与 Update 相同，但请求发往 event.Namespace。
	//
	// 中文：按 event.Namespace 更新事件，要求对象已带 ResourceVersion。
	UpdateWithEventNamespace(event *v1beta1.Event) (*v1beta1.Event, error)
	// PatchWithEventNamespace 与 Patch 相同，但请求发往 event.Namespace。
	//
	// 中文：按 event.Namespace 以 strategic merge patch 方式修改事件。
	PatchWithEventNamespace(event *v1beta1.Event, data []byte) (*v1beta1.Event, error)
}

// CreateWithEventNamespace 新建事件，返回服务端返回的副本或错误。事件所在
// namespace 取自 event 本身，必须与客户端 namespace 一致，或客户端以 ""
// namespace 创建（可跨命名空间操作）。
func (e *events) CreateWithEventNamespace(event *v1beta1.Event) (*v1beta1.Event, error) {
	// 客户端若已绑定命名空间，则只允许在该命名空间内写入，防止误写他处。
	if e.GetNamespace() != "" && event.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't create an event with namespace '%v' in namespace '%v'", event.Namespace, e.GetNamespace())
	}
	result := &v1beta1.Event{}
	// NamespaceIfScoped：集群级客户端（命名空间为空）不拼接 namespace 路径段。
	err := e.GetClient().Post().
		NamespaceIfScoped(event.Namespace, len(event.Namespace) > 0).
		Resource("events").
		Body(event).
		Do(context.Background() /* Nothing to do here, v1beta1 will be removed eventually. */).
		Into(result)
	return result, err
}

// UpdateWithEventNamespace 修改事件并返回服务端返回的副本或错误。namespace 与
// name 取自 event；namespace 必须与客户端 namespace 一致，或客户端以 "" namespace
// 创建。Update 还要求 event 对象已设置 ResourceVersion，否则服务端会拒绝更新。
func (e *events) UpdateWithEventNamespace(event *v1beta1.Event) (*v1beta1.Event, error) {
	// 与 Create 相同的命名空间一致性约束。
	if e.GetNamespace() != "" && event.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't update an event with namespace '%v' in namespace '%v'", event.Namespace, e.GetNamespace())
	}
	result := &v1beta1.Event{}
	err := e.GetClient().Put().
		NamespaceIfScoped(event.Namespace, len(event.Namespace) > 0).
		Resource("events").
		Name(event.Name).
		Body(event).
		Do(context.Background() /* Nothing to do here, v1beta1 will be removed eventually. */).
		Into(result)
	return result, err
}

// PatchWithEventNamespace 修改事件并返回服务端返回的副本或错误。目标 namespace
// 与 name 取自 event；namespace 必须与客户端 namespace 一致，或客户端以 ""
// namespace 创建。
func (e *events) PatchWithEventNamespace(event *v1beta1.Event, data []byte) (*v1beta1.Event, error) {
	// 与 Create/Update 相同的命名空间一致性约束。
	if e.GetNamespace() != "" && event.Namespace != e.GetNamespace() {
		return nil, fmt.Errorf("can't patch an event with namespace '%v' in namespace '%v'", event.Namespace, e.GetNamespace())
	}
	result := &v1beta1.Event{}
	err := e.GetClient().Patch(types.StrategicMergePatchType).
		NamespaceIfScoped(event.Namespace, len(event.Namespace) > 0).
		Resource("events").
		Name(event.Name).
		Body(data).
		Do(context.Background() /* Nothing to do here, v1beta1 will be removed eventually. */).
		Into(result)
	return result, err
}
