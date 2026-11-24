/*
Copyright 2019 The Kubernetes Authors.

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

package runtimeclass

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	nodeapi "k8s.io/kubernetes/pkg/api/node"
	"k8s.io/kubernetes/pkg/apis/node"
	"k8s.io/kubernetes/pkg/apis/node/validation"
)

// strategy 定义 RuntimeClass 创建、更新和校验时使用的 REST 策略。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 RuntimeClass 对象时使用的默认策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 false，因为 RuntimeClass 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// AllowCreateOnUpdate 返回 true，允许客户端通过 PUT 更新请求创建尚不存在的 RuntimeClass。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// PrepareForCreate 是创建前的字段预处理入口；当前不清理客户端提交的字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 是更新前的字段预处理入口；当前不重置或改写字段。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRuntimeClass := obj.(*node.RuntimeClass)
	oldRuntimeClass := old.(*node.RuntimeClass)

	_, _ = newRuntimeClass, oldRuntimeClass
}

// Validate 调用 RuntimeClass 专用校验逻辑，检查调度配置及运行时处理器名称等字段。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	runtimeClass := obj.(*node.RuntimeClass)
	return validation.ValidateRuntimeClass(runtimeClass)
}

// DeclarativeValidationConfig 实现 rest.DeclarativeValidationConfigurer，
// 为通用 BeforeCreate/BeforeUpdate 流程提供声明式校验所需的规范化规则。
func (strategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{NormalizationRules: validation.NodeNormalizationRules}
}

// WarningsOnCreate 返回创建 RuntimeClass 时的非阻断性警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nodeapi.GetWarningsForRuntimeClass(obj.(*node.RuntimeClass))
}

// Canonicalize 在校验后规范化 RuntimeClass；当前仅确认对象类型，不执行额外转换。
func (strategy) Canonicalize(obj runtime.Object) {
	_ = obj.(*node.RuntimeClass)
}

// ValidateUpdate 同时执行 RuntimeClass 完整校验和新旧对象之间的更新约束校验。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj := obj.(*node.RuntimeClass)
	errorList := validation.ValidateRuntimeClass(newObj)
	errorList = append(errorList, validation.ValidateRuntimeClassUpdate(newObj, old.(*node.RuntimeClass))...)
	return errorList
}

// WarningsOnUpdate 返回更新 RuntimeClass 时的非阻断性警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nodeapi.GetWarningsForRuntimeClass(obj.(*node.RuntimeClass))
}

// AllowUnconditionalUpdate 返回 false，因此更新请求必须携带 ResourceVersion。
// 通用更新流程会校验该版本与 etcd 中最新对象的版本一致，以避免覆盖并发修改。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
