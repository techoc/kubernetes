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

// Strategy 是通过 REST API 创建和更新 ConfigMap 时使用的默认策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现 REST 创建策略接口。
var _ rest.RESTCreateStrategy = Strategy

// 编译期检查：确保 Strategy 实现 REST 更新策略接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 表示 ConfigMap 是命名空间级资源。
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

// WarningsOnCreate 返回创建 ConfigMap 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (strategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：ConfigMap 不能通过更新请求隐式创建。
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

// WarningsOnUpdate 返回更新 ConfigMap 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string { return nil }

// dropDisabledFields 在相关 Feature Gate 关闭时清理新对象中不再允许设置的字段；旧对象已使用的字段需保留以兼容存量数据。
func dropDisabledFields(configMap *api.ConfigMap, oldConfigMap *api.ConfigMap) {
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的无条件更新。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs 提取 ConfigMap 的 Labels 和可筛选字段，供 List/Watch 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	configMap, ok := obj.(*api.ConfigMap)
	if !ok {
		return nil, nil, fmt.Errorf("not a configmap")
	}
	return labels.Set(configMap.Labels), SelectableFields(configMap), nil
}

// Matcher 根据 LabelSelector 和 FieldSelector 创建存储层选择谓词。
// Matcher 将请求中的 LabelSelector/FieldSelector 与 ConfigMap GetAttrs 绑定为存储层过滤谓词。
func Matcher(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields 返回 ConfigMap 支持 FieldSelector 查询的字段集合。
// ConfigMap 当前只暴露标准 ObjectMeta 字段选择器，不把 Data 内容开放为 FieldSelector。
func SelectableFields(obj *api.ConfigMap) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}
