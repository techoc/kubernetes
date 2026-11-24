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

package event

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// eventStrategy 定义 Event 写入策略；Event 是短生命周期的观测记录，存储层使用 TTL 回收而不是 Owner 级联删除。
type eventStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 Event 时使用的默认策略。
var Strategy = eventStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// Event 不支持普通 OwnerReference 级联垃圾回收，生命周期由 Store 配置的 TTL 控制。
func (eventStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.Unsupported
}

// NamespaceScoped 表示 Event 是命名空间级资源。
func (eventStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 不改写 Event 字段；事件时间、来源和涉及对象由事件生产者提供。
func (eventStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 保留客户端提交的事件更新内容，字段约束由版本感知的 ValidateUpdate 执行。
func (eventStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 根据请求 API Group/Version 执行对应版本的 Event 校验，兼容不同 Event API 版本字段语义。
func (eventStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	groupVersion := requestGroupVersion(ctx)
	event := obj.(*api.Event)
	return validation.ValidateEventCreate(event, groupVersion)
}

// WarningsOnCreate 返回创建 Event 时的非阻断性警告；当前不产生额外警告。
func (eventStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (eventStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 true：为兼容 Event 上报客户端，允许更新请求在对象不存在时创建事件。
func (eventStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// ValidateUpdate 使用请求版本校验更新后的 Event，并结合旧对象检查不可变字段和时间/计数变化规则。
func (eventStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	groupVersion := requestGroupVersion(ctx)
	event := obj.(*api.Event)
	oldEvent := old.(*api.Event)
	return validation.ValidateEventUpdate(event, oldEvent, groupVersion)
}

// WarningsOnUpdate 返回更新 Event 时的非阻断性警告；当前不产生额外警告。
func (eventStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的无条件更新。
func (eventStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs 提取 Event 的 Labels 和可筛选字段，供 List/Watch 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	event, ok := obj.(*api.Event)
	if !ok {
		return nil, nil, fmt.Errorf("not an event")
	}
	return labels.Set(event.Labels), ToSelectableFields(event), nil
}

// Matcher 根据 LabelSelector 和 FieldSelector 创建存储层选择谓词。
func Matcher(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// ToSelectableFields 返回 Event 支持 FieldSelector 查询的字段集合。
// ToSelectableFields 暴露 involvedObject、reason、type、reportingComponent 和旧 source 等事件查询字段。
func ToSelectableFields(event *api.Event) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&event.ObjectMeta, true)
	source := event.Source.Component
	if source == "" {
		source = event.ReportingController
	}
	specificFieldsSet := fields.Set{
		"involvedObject.kind":            event.InvolvedObject.Kind,
		"involvedObject.namespace":       event.InvolvedObject.Namespace,
		"involvedObject.name":            event.InvolvedObject.Name,
		"involvedObject.uid":             string(event.InvolvedObject.UID),
		"involvedObject.apiVersion":      event.InvolvedObject.APIVersion,
		"involvedObject.resourceVersion": event.InvolvedObject.ResourceVersion,
		"involvedObject.fieldPath":       event.InvolvedObject.FieldPath,
		"reason":                         event.Reason,
		"reportingComponent":             event.ReportingController, // 使用 core/v1 API 对外暴露的字段名。
		"source":                         source,
		"type":                           event.Type,
	}
	return generic.MergeFieldsSets(specificFieldsSet, objectMetaFieldsSet)
}

// requestGroupVersion 从请求上下文中提取 API Group/Version；若上下文没有 RequestInfo，则返回零值。
// requestGroupVersion 从 RequestInfo 取出客户端请求版本，供事件版本感知的校验函数使用。
func requestGroupVersion(ctx context.Context) schema.GroupVersion {
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		return schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	}
	return schema.GroupVersion{}
}
