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

package fischer

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"

	"k8s.io/sample-apiserver/pkg/apis/wardle"
)

// NewStrategy creates and returns a fischerStrategy instance
// 名字生成器交给存储层，让 Fischer 也能像内置资源一样自动补 metadata.name。
func NewStrategy(typer runtime.ObjectTyper) fischerStrategy {
	return fischerStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Fischer
// 标签和字段分开返回，供存储层分别处理标签选择器和字段选择器。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*wardle.Fischer)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Fischer")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchFischer is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
// 把选择器判定下推到存储层，避免把不匹配的对象也反序列化一遍。
func MatchFischer(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
// 复用 ObjectMeta 的通用字段就够示例用；true 表示 metadata.name 也可被字段选择器匹配。
func SelectableFields(obj *wardle.Fischer) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

// 私有实现只在本包的 NewREST 里构造，避免外部绕过统一的名字生成器。
type fischerStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Fischer 是集群级示例资源，不按 namespace 隔离。
func (fischerStrategy) NamespaceScoped() bool {
	return false
}

// 示例资源没有创建前需要规整的字段，留空钩子表明走通用创建流程。
func (fischerStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (fischerStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// 没有业务校验规则，返回空列表即视为校验通过。
func (fischerStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnCreate returns warnings for the creation of the given object.
// 无警告要报告；返回 nil 让客户端不必处理空告警字段。
func (fischerStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// 更新路径必须命中已存在的对象，不能借 PUT 偷偷创建。
func (fischerStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// 要求带上 resourceVersion 做乐观并发，避免并发更新互相覆盖。
func (fischerStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

func (fischerStrategy) Canonicalize(obj runtime.Object) {
}

func (fischerStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate returns warnings for the given update.
// 与创建路径一致，示例资源更新也不产生告警。
func (fischerStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
