/*
Copyright 2020 The Kubernetes Authors.

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

package ingressclass

import (
	"context"

	"k8s.io/apiserver/pkg/registry/rest"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/networking/validation"
)

// ingressClassStrategy 定义集群级 IngressClass 策略，校验 Controller 字段和参数引用，并维护 Spec Generation。
type ingressClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 IngressClass 对象时使用的默认策略。
var Strategy = ingressClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 IngressClass 是集群级资源，不属于任何命名空间。
func (ingressClassStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建 IngressClass 时将 Generation 初始化为 1。
func (ingressClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ingressClass := obj.(*networking.IngressClass)
	ingressClass.Generation = 1
}

// PrepareForUpdate 在更新时检查 Spec 是否变化，并据此递增 Generation。
func (ingressClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIngressClass := obj.(*networking.IngressClass)
	oldIngressClass := old.(*networking.IngressClass)

	// Spec 发生变化时递增 Generation，供 Ingress Controller 判断类配置是否更新。
	if !apiequality.Semantic.DeepEqual(oldIngressClass.Spec, newIngressClass.Spec) {
		newIngressClass.Generation = oldIngressClass.Generation + 1
	}
}

// Validate 调用 IngressClass 专用校验逻辑检查 Controller 名称和 Parameters 引用。
func (ingressClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ingressClass := obj.(*networking.IngressClass)
	return validation.ValidateIngressClass(ingressClass)
}

// WarningsOnCreate 返回创建 IngressClass 时的非阻断性警告；当前不产生额外警告。
func (ingressClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 IngressClass；当前无需额外处理。
func (ingressClassStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，IngressClass 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (ingressClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 IngressClass 更新是否合法。
func (ingressClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIngressClass := obj.(*networking.IngressClass)
	oldIngressClass := old.(*networking.IngressClass)

	return validation.ValidateIngressClassUpdate(newIngressClass, oldIngressClass)
}

// WarningsOnUpdate 返回更新 IngressClass 时的非阻断性警告；当前不产生额外警告。
func (ingressClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许 IngressClass 更新请求不携带 ResourceVersion。
func (ingressClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
