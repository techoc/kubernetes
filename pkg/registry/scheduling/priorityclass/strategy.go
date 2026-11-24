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

package priorityclass

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/scheduling/validation"
)

// priorityClassStrategy 定义 PriorityClass 创建、更新和校验时使用的 REST 策略。
type priorityClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 PriorityClass 对象时使用的默认策略。
var Strategy = priorityClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 PriorityClass 是集群级资源。
func (priorityClassStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建 PriorityClass 时将 Generation 初始化为 1。
func (priorityClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pc := obj.(*scheduling.PriorityClass)
	pc.Generation = 1
}

// PrepareForUpdate 是 PriorityClass 更新前的字段预处理入口；当前无需重置字段。
func (priorityClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {}

// Validate 调用 PriorityClass 专用校验逻辑，检查优先级值和全局默认类约束。
func (priorityClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	pc := obj.(*scheduling.PriorityClass)
	return validation.ValidatePriorityClass(pc)
}

// WarningsOnCreate 返回创建 PriorityClass 时的非阻断性警告；当前不产生额外警告。
func (priorityClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 PriorityClass；当前不执行额外转换。
func (priorityClassStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，PriorityClass 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (priorityClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据新旧对象校验 PriorityClass 更新是否合法。
func (priorityClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidatePriorityClassUpdate(obj.(*scheduling.PriorityClass), old.(*scheduling.PriorityClass))
}

// WarningsOnUpdate 返回更新 PriorityClass 时的非阻断性警告；当前不产生额外警告。
func (priorityClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许更新请求不携带 ResourceVersion，并由通用更新流程填入最新版本。
func (priorityClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
