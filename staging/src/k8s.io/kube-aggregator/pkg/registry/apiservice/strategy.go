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

package apiservice

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"

	"k8s.io/kube-aggregator/pkg/apis/apiregistration"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

type apiServerStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// 编译期断言把接口实现绑在声明处：策略方法很多且大多没有显式调用点，
// 一旦签名漂移，错误会立刻出现在实现文件而不是某个遥远的调用方。
// apiServerStrategy 实现 rest.RESTCreateUpdateStrategy。
var _ rest.RESTCreateUpdateStrategy = apiServerStrategy{}
var Strategy = apiServerStrategy{}

// NewStrategy 创建一个新的 apiServerStrategy。
func NewStrategy(typer runtime.ObjectTyper) rest.CreateUpdateResetFieldsStrategy {
	return apiServerStrategy{typer, names.SimpleNameGenerator}
}

func (apiServerStrategy) NamespaceScoped() bool {
	return false
}

// 主资源把 status 列为“应用托管时可重置的字段”：服务端应用（SSA）只声明
// spec，不能靠 apply 顺手写进状态；两个版本都要列，因为不同客户端可能用任一
// 版本 apply，缺一个就会出现同一对象按版本不同行为不一致。
func (apiServerStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apiregistration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"apiregistration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

func (apiServerStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	apiservice := obj.(*apiregistration.APIService)
	// 创建时无条件清空状态：状态是控制器的派生信息，客户端提交的任何值都不能作数，
	// 否则用户可以伪造 Available 绕过后端健康检查。
	apiservice.Status = apiregistration.APIServiceStatus{}

	// 创建时把本地 API service 直接标记为可用。
	// 本地服务不需要经过健康探测：请求由自身 handler 处理，若它不可用整个
	// apiserver 都已经挂了；标成 Available 可以让聚合层立刻开始转发。
	if apiservice.Spec.Service == nil {
		apiregistration.SetAPIServiceCondition(apiservice, apiregistration.NewLocalAvailableAPIServiceCondition())
	}
}

func (apiServerStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newAPIService := obj.(*apiregistration.APIService)
	oldAPIService := old.(*apiregistration.APIService)
	// 主资源更新保留旧状态：状态只能通过 /status 子资源写，
	// 否则一次改 spec 的 PUT 就会把健康检查结论清空，造成短暂的“不可用”抖动。
	newAPIService.Status = oldAPIService.Status
}

func (apiServerStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateAPIService(ctx, obj.(*apiregistration.APIService))
}

// WarningsOnCreate 返回创建对象时的警告。
func (apiServerStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (apiServerStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

func (apiServerStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

func (apiServerStrategy) Canonicalize(obj runtime.Object) {
}

func (apiServerStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateAPIServiceUpdate(ctx, obj.(*apiregistration.APIService), old.(*apiregistration.APIService))
}

// WarningsOnUpdate 返回更新对象时的警告。
func (apiServerStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type apiServerStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStatusStrategy 创建一个新的 apiServerStatusStrategy。
func NewStatusStrategy(typer runtime.ObjectTyper) rest.UpdateResetFieldsStrategy {
	return apiServerStatusStrategy{typer, names.SimpleNameGenerator}
}

func (apiServerStatusStrategy) NamespaceScoped() bool {
	return false
}

// 状态子资源与主资源互补：这里声明 spec 和 metadata 可被 SSA 重置，
// 因为 apply /status 的客户端只应关心 status，其余字段不属于它的管理范围。
func (apiServerStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apiregistration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"apiregistration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
	}

	return fields
}

func (apiServerStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newAPIService := obj.(*apiregistration.APIService)
	oldAPIService := old.(*apiregistration.APIService)
	// 状态子资源只让 status 通过，其余字段全部取旧值：
	// 这是“子资源不是完整对象更新”的最后一道防线，不能依赖调用方自觉只传 status。
	newAPIService.Spec = oldAPIService.Spec
	newAPIService.Labels = oldAPIService.Labels
	newAPIService.Annotations = oldAPIService.Annotations
	newAPIService.Finalizers = oldAPIService.Finalizers
	newAPIService.OwnerReferences = oldAPIService.OwnerReferences
}

func (apiServerStatusStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

func (apiServerStatusStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// Canonicalize 在校验后规范化对象。
func (apiServerStatusStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate 校验 apiServerStatusStrategy 的更新。
func (apiServerStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateAPIServiceStatusUpdate(ctx, obj.(*apiregistration.APIService), old.(*apiregistration.APIService))
}

// WarningsOnUpdate 返回更新对象时的警告。
func (apiServerStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetAttrs 返回 API server 对象的 labels 和 fields，用于过滤。
// 把对象拆成 label/field 两组，etcd 层才能分别用 selector 过滤 watch 事件；
// 类型不匹配返回错误而不是 panic，避免一条坏数据打挂整个 LIST 请求。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*apiregistration.APIService)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a APIService")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), ToSelectableFields(apiserver), nil
}

// MatchAPIService 是通用 etcd 后端使用的过滤器，只把匹配特定 label/field 的事件
// 从 etcd 转发给对应客户端。
func MatchAPIService(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// ToSelectableFields 返回表示该对象的字段集。
// 第二个参数 true 表示把 metadata.namespace 也放进字段集；APIService 虽然是
// 集群级资源，但保持与其他资源一致的字段选择器（按 namespace 精确匹配空串）
// 可以避免通用查询逻辑在这里开特例。
func ToSelectableFields(obj *apiregistration.APIService) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}
