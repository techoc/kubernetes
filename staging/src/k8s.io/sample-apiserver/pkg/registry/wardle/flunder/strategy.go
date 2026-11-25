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

package flunder

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
	"k8s.io/sample-apiserver/pkg/apis/wardle/validation"

	"k8s.io/sample-apiserver/pkg/apis/wardle"
)

// NewStrategy 创建 Flunder 存储策略。
// 注入 ObjectTyper 与名字生成器，让 generic registry 能据此判断类型并生成资源名。
func NewStrategy(typer runtime.ObjectTyper) flunderStrategy {
	return flunderStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs 返回 Flunder 的标签集与字段集；对象类型不符时返回错误。
// 把对象拆成标签和可选字段两部分，供 list/watch 的字段与标签选择器过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*wardle.Flunder)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Flunder")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchFlunder 为通用 etcd 后端构建 Flunder 的标签与字段选择谓词。
// 把选择器与 GetAttrs 打包成 SelectionPredicate；服务端过滤能减少发给客户端的无用事件。
func MatchFlunder(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields 返回 Flunder 可用于字段选择器的元数据字段。
// 复用 ObjectMeta 的通用字段（含 namespace），使 metadata.name/namespace 等可用于字段选择。
func SelectableFields(obj *wardle.Flunder) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

// flunderStrategy 把校验和名字生成交给 generic registry，自身只保留示例所需的最小逻辑。
type flunderStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NamespaceScoped 返回 true，因为 Flunder 的删除和列表都要按 namespace 隔离。
func (flunderStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 不修改对象，因为示例类型没有默认化需求。
func (flunderStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 不修改对象，因为示例类型没有默认化需求。
func (flunderStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 在创建时复用内部版本的校验，避免各版本重复实现规则。
func (flunderStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	flunder := obj.(*wardle.Flunder)
	return validation.ValidateFlunder(flunder)
}

// WarningsOnCreate 返回创建告警；示例类型不产生告警，因此返回 nil。
func (flunderStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// AllowCreateOnUpdate 返回 false，不允许 PUT 尚不存在的对象，创建必须走 CREATE 路径。
func (flunderStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// AllowUnconditionalUpdate 返回 false，更新必须带 resourceVersion，防止并发写覆盖。
func (flunderStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// Canonicalize 不修改对象，示例类型无需规范化字段。
func (flunderStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate 返回空错误列表，示例类型没有更新特有的校验。
func (flunderStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate 返回更新告警；与 WarningsOnCreate 一致，示例类型返回 nil。
func (flunderStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
