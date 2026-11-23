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

// NewStrategy creates and returns a flunderStrategy instance
// 注入 ObjectTyper 与名字生成器，让 generic registry 能据此判断类型并生成资源名。
func NewStrategy(typer runtime.ObjectTyper) flunderStrategy {
	return flunderStrategy{typer, names.SimpleNameGenerator}
}

// GetAttrs returns labels.Set, fields.Set, and error in case the given runtime.Object is not a Flunder
// 把对象拆成标签和可选字段两部分，供 list/watch 的字段与标签选择器过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*wardle.Flunder)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a Flunder")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), SelectableFields(apiserver), nil
}

// MatchFlunder is the filter used by the generic etcd backend to watch events
// from etcd to clients of the apiserver only interested in specific labels/fields.
// 把选择器与 GetAttrs 打包成 SelectionPredicate；服务端过滤能减少发给客户端的无用事件。
func MatchFlunder(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that represents the object.
// 复用 ObjectMeta 的通用字段（含 namespace），使 metadata.name/namespace 等可用于字段选择。
func SelectableFields(obj *wardle.Flunder) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

// flunderStrategy 把校验和名字生成交给 generic registry，自身只保留示例所需的最小逻辑。
type flunderStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Flunder 是带命名空间资源，删除和列表都要按 namespace 隔离。
func (flunderStrategy) NamespaceScoped() bool {
	return true
}

// 示例类型没有默认化需求，故创建前不改动对象。
func (flunderStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// 示例类型没有默认化需求，故更新前不改动对象。
func (flunderStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// 创建时复用内部版本的校验，避免各版本重复实现规则。
func (flunderStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	flunder := obj.(*wardle.Flunder)
	return validation.ValidateFlunder(flunder)
}

// WarningsOnCreate returns warnings for the creation of the given object.
// 示例类型不产生告警；预留该方法是为了满足 registry 接口。
func (flunderStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// 不允许 PUT 一个尚不存在的对象，创建要走专门的 CREATE 路径。
func (flunderStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// 更新必须带 resourceVersion，防止并发写覆盖。
func (flunderStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// 示例类型无需规范化字段。
func (flunderStrategy) Canonicalize(obj runtime.Object) {
}

// 示例类型没有更新特有的校验，返回空列表表示放开。
func (flunderStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate returns warnings for the given update.
// 同 WarningsOnCreate，仅占位满足接口。
func (flunderStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
