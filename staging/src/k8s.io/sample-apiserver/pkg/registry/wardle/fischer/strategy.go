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

// NewStrategy 创建 Fischer 存储策略。
// 名字生成器交给存储层，让 Fischer 也能像内置资源一样自动补 metadata.name。
func NewStrategy(typer runtime.ObjectTyper) fischerStrategy {
	return fischerStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs 返回 Fischer 的标签集与字段集；对象类型不符时返回错误。
// 标签和字段分开返回，供存储层分别处理标签选择器和字段选择器。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*wardle.Fischer)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Fischer")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchFischer 为通用 etcd 后端构建 Fischer 的标签与字段选择谓词。
// 把选择器判定下推到存储层，避免把不匹配的对象也反序列化一遍。
func MatchFischer(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields 返回 Fischer 可用于字段选择器的元数据字段。
// 复用 ObjectMeta 的通用字段就够示例用；true 表示 metadata.name 也可被字段选择器匹配。
func SelectableFields(obj *wardle.Fischer) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

// 私有实现只在本包的 NewREST 里构造，避免外部绕过统一的名字生成器。
type fischerStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NamespaceScoped 返回 false，因为 Fischer 是集群级资源，不按 namespace 隔离。
func (fischerStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 不规整字段，示例资源直接走通用创建流程。
func (fischerStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 不规整字段，示例资源直接走通用更新流程。
func (fischerStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 返回空错误列表，因为示例资源没有业务校验规则。
func (fischerStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnCreate 返回创建告警；示例资源无警告，因此返回 nil。
func (fischerStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// AllowCreateOnUpdate 返回 false，更新必须命中已存在的对象，不能借 PUT 偷偷创建。
func (fischerStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// AllowUnconditionalUpdate 返回 false，要求带上 resourceVersion 做乐观并发，避免并发更新互相覆盖。
func (fischerStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// Canonicalize 不修改对象，示例资源没有存储前规范化需求。
func (fischerStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate 返回空错误列表，示例资源没有更新特有的校验。
func (fischerStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate 返回更新告警；与创建路径一致，示例资源返回 nil。
func (fischerStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
