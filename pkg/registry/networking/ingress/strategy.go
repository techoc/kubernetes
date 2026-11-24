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

package ingress

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/networking/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

const (
	annotationIngressClass = "kubernetes.io/ingress.class"
)

// ingressStrategy 定义 Ingress 主资源策略，处理 Status 隔离、Generation、IngressClass 旧注解兼容和规则校验。
type ingressStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 Ingress 对象时使用的默认策略。
var Strategy = ingressStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，因为 Ingress 必须归属于某个命名空间。
func (ingressStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回主资源策略重置的 Status 字段集合，防止客户端通过普通写入接口修改控制器维护的状态。
func (ingressStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"extensions/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"networking.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"networking.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// 创建时不接受客户端提供的 Status；Generation 从 1 开始，后续仅由 Spec 变化递增。
func (ingressStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ingress := obj.(*networking.Ingress)
	// 创建请求不能设置 Status；状态由 Ingress Controller 后续维护。
	ingress.Status = networking.IngressStatus{}

	ingress.Generation = 1
}

// PrepareForUpdate 在普通更新时恢复旧 Status，并根据 Spec 是否变化维护 Generation。
func (ingressStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIngress := obj.(*networking.Ingress)
	oldIngress := old.(*networking.Ingress)
	// 普通更新请求不能修改 Status，避免绕过 Ingress Controller 的状态管理。
	newIngress.Status = oldIngress.Status

	// Spec 发生变化时递增 Generation；Status 的变化不应改变 Generation。
	// 控制器可通过 Generation 判断当前状态对应的是哪一版 Spec。
	if !apiequality.Semantic.DeepEqual(oldIngress.Spec, newIngress.Spec) {
		newIngress.Generation = oldIngress.Generation + 1
	}

}

// Validate 在创建时校验 Ingress 的规则、TLS 和后端引用等字段。
func (ingressStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ingress := obj.(*networking.Ingress)
	return validation.ValidateIngressCreate(ingress)
}

// WarningsOnCreate 返回创建 Ingress 时的非阻断性警告，包括旧 IngressClass 注解的弃用提示。
func (ingressStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	var warnings []string
	ingress := obj.(*networking.Ingress)
	_, annotationIsSet := ingress.Annotations[annotationIngressClass]
	if annotationIsSet && ingress.Spec.IngressClassName == nil {
		warnings = append(warnings, fmt.Sprintf("annotation %q is deprecated, please use 'spec.ingressClassName' instead", annotationIngressClass))
	}
	return warnings
}

// Canonicalize 在校验后规范化 Ingress；当前无需额外处理。
func (ingressStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，Ingress 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (ingressStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 Ingress 更新是否合法。
func (ingressStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateIngressUpdate(obj.(*networking.Ingress), old.(*networking.Ingress))
}

// WarningsOnUpdate 返回普通 Ingress 更新时的非阻断性警告；当前不产生额外警告。
func (ingressStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许 Ingress 更新请求不携带 ResourceVersion。
func (ingressStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type ingressStatusStrategy struct {
	ingressStrategy
}

// StatusStrategy 定义通过 /status 子资源更新 Ingress 状态时使用的策略。
var StatusStrategy = ingressStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源策略重置的 Spec 字段集合，确保该入口不能修改路由规则。
func (ingressStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"extensions/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
		"networking.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
		"networking.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在 Status 更新时恢复旧 Spec，防止通过 /status 修改路由规则。
func (ingressStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIngress := obj.(*networking.Ingress)
	oldIngress := old.(*networking.Ingress)
	// Status 更新请求不能修改 Spec，必须沿用旧对象中的路由规则。
	newIngress.Spec = oldIngress.Spec
}

// ValidateUpdate 对照旧对象校验 Ingress Status 更新是否合法。
func (ingressStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateIngressStatusUpdate(obj.(*networking.Ingress), old.(*networking.Ingress))
}

// WarningsOnUpdate 返回 Ingress Status 更新时的非阻断性警告；会检查 LoadBalancer 地址中的 IP 格式。
func (ingressStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIngress := obj.(*networking.Ingress)
	var warnings []string

	if len(newIngress.Status.LoadBalancer.Ingress) > 0 {
		fieldPath := field.NewPath("status", "loadBalancer", "ingress")
		for i, ingress := range newIngress.Status.LoadBalancer.Ingress {
			if len(ingress.IP) > 0 {
				warnings = append(warnings, utilvalidation.GetWarningsForIP(fieldPath.Index(i), ingress.IP)...)
			}
		}
	}

	return warnings
}
