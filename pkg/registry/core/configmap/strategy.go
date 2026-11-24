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

package configmap

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	pkgstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// strategy 定义 ConfigMap 的 REST 创建/更新策略，负责 Feature Gate 字段清理、数据校验和字段选择器提取。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating ConfigMap
// objects via the REST API.
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// Strategy should implement rest.RESTCreateStrategy
var _ rest.RESTCreateStrategy = Strategy

// Strategy should implement rest.RESTUpdateStrategy
var _ rest.RESTUpdateStrategy = Strategy

func (strategy) NamespaceScoped() bool {
	return true
}

// 创建前按当前 Feature Gate 清理不可写字段；ConfigMap 的 Data/BinaryData 合法性由 Validate 统一检查。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	configMap := obj.(*api.ConfigMap)
	dropDisabledFields(configMap, nil)
}

// Validate 检查 ConfigMap 的键名、Data/BinaryData 内容以及总数据大小等 API 约束。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	cfg := obj.(*api.ConfigMap)

	return validation.ValidateConfigMap(cfg)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize normalizes the object after validation.
func (strategy) Canonicalize(obj runtime.Object) {
}

func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// 更新时结合新旧对象清理被关闭的字段；具体哪些字段可变及内容限制由 ValidateUpdate 决定。
func (strategy) PrepareForUpdate(ctx context.Context, newObj, oldObj runtime.Object) {
	oldConfigMap := oldObj.(*api.ConfigMap)
	newConfigMap := newObj.(*api.ConfigMap)
	dropDisabledFields(newConfigMap, oldConfigMap)
}

// ValidateUpdate 对比新旧 ConfigMap，执行更新场景下的键名、数据大小和字段规则校验。
func (strategy) ValidateUpdate(ctx context.Context, newObj, oldObj runtime.Object) field.ErrorList {
	oldCfg, newCfg := oldObj.(*api.ConfigMap), newObj.(*api.ConfigMap)

	return validation.ValidateConfigMapUpdate(newCfg, oldCfg)
}

// WarningsOnUpdate returns warnings for the given update.
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string { return nil }

func dropDisabledFields(configMap *api.ConfigMap, oldConfigMap *api.ConfigMap) {
}

func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs returns labels and fields of a given object for filtering purposes.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	configMap, ok := obj.(*api.ConfigMap)
	if !ok {
		return nil, nil, fmt.Errorf("not a configmap")
	}
	return labels.Set(configMap.Labels), SelectableFields(configMap), nil
}

// Matcher returns a selection predicate for a given label and field selector.
// Matcher 将请求中的 LabelSelector/FieldSelector 与 ConfigMap GetAttrs 绑定为存储层过滤谓词。
func Matcher(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that can be used for filter selection
// ConfigMap 当前只暴露标准 ObjectMeta 字段选择器，不把 Data 内容开放为 FieldSelector。
func SelectableFields(obj *api.ConfigMap) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}
