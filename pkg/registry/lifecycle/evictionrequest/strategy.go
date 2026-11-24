/*
Copyright The Kubernetes Authors.

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

package evictionrequest

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/apis/lifecycle/validation"
)

// evictionRequestStrategy is the default logic that applies when creating and updating EvictionRequest objects.
type evictionRequestStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// NewStrategy 创建 EvictionRequest 主资源策略，并绑定声明式校验 Scheme。
func NewStrategy() *evictionRequestStrategy {
	return &evictionRequestStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
	}
}

var _ = rest.ResetFieldsStrategy(&evictionRequestStrategy{})

// NamespaceScoped 返回 true，因为 EvictionRequest 属于目标对象所在的命名空间。
func (*evictionRequestStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止客户端直接修改控制器维护的驱逐请求状态。
func (*evictionRequestStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
	return fields
}

// PrepareForCreate 在创建时清空客户端提交的 Status，并将 Generation 初始化为 1。
func (*evictionRequestStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	evictionRequest := obj.(*lifecycle.EvictionRequest)
	evictionRequest.Status = lifecycle.EvictionRequestStatus{}
	evictionRequest.Generation = 1
}

// PrepareForUpdate 恢复旧 Status；Spec 发生变化时递增 Generation。
func (*evictionRequestStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	oldEvictionRequest := old.(*lifecycle.EvictionRequest)
	newEvictionRequest := obj.(*lifecycle.EvictionRequest)
	newEvictionRequest.Status = oldEvictionRequest.Status

	// Spec 变化时递增 Generation，Status 变化不应改变 Generation。
	if !apiequality.Semantic.DeepEqual(oldEvictionRequest.Spec, newEvictionRequest.Spec) {
		newEvictionRequest.Generation = oldEvictionRequest.Generation + 1
	}
}

// Validate 校验 EvictionRequest 的目标 Pod、驱逐请求参数及字段约束。
func (s *evictionRequestStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	evictionRequest := obj.(*lifecycle.EvictionRequest)

	allErrs := validation.ValidateEvictionRequest(evictionRequest)
	return allErrs
}

// DeclarativeValidationConfig 返回 EvictionRequest 当前使用的声明式校验选项；此处没有额外门控配置。
func (*evictionRequestStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{}
}

func (*evictionRequestStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (*evictionRequestStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，EvictionRequest 必须通过 POST 创建。
func (*evictionRequestStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据新旧对象校验 EvictionRequest 更新是否合法。
func (s *evictionRequestStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	var allErrs field.ErrorList
	evictionRequest := obj.(*lifecycle.EvictionRequest)
	oldEvictionRequest := old.(*lifecycle.EvictionRequest)

	allErrs = validation.ValidateEvictionRequestUpdate(evictionRequest, oldEvictionRequest)
	return allErrs
}

func (*evictionRequestStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新必须携带 ResourceVersion 以避免覆盖并发状态。
func (*evictionRequestStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// evictionRequestStatusStrategy 定义通过 /status 子资源更新 EvictionRequest 状态时使用的策略。
type evictionRequestStatusStrategy struct {
	*evictionRequestStrategy
}

var _ = rest.ResetFieldsStrategy(&evictionRequestStatusStrategy{})

// NewStatusStrategy 基于主资源策略创建独立的 /status 子资源策略。
func NewStatusStrategy(strategy *evictionRequestStrategy) *evictionRequestStatusStrategy {
	return &evictionRequestStatusStrategy{
		strategy,
	}
}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (*evictionRequestStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
	}
}

// PrepareForUpdate 重置受保护的元数据字段并恢复旧 Spec，只允许更新 Status。
func (*evictionRequestStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newEvictionRequest := obj.(*lifecycle.EvictionRequest)
	oldEvictionRequest := old.(*lifecycle.EvictionRequest)
	// Status 更新不应同时授予修改元数据的权限；标签应由 evictionrequest-controller 负责维护，
	// 而不是由响应驱逐请求的一方修改，以免扩大标签写入权限。
	metav1.ResetObjectMetaForStatus(&newEvictionRequest.ObjectMeta, &oldEvictionRequest.ObjectMeta)
	newEvictionRequest.Spec = oldEvictionRequest.Spec
}

// AllowCreateOnUpdate 返回 false，Status 子资源不能通过更新请求创建对象。
func (*evictionRequestStatusStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 EvictionRequest Status 更新是否合法。
func (s *evictionRequestStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	allErrs := validation.ValidateEvictionRequestStatusUpdate(obj.(*lifecycle.EvictionRequest), old.(*lifecycle.EvictionRequest))
	return allErrs
}

// WarningsOnUpdate 返回 EvictionRequest Status 更新时的非阻断性警告；当前不产生额外警告。
func (*evictionRequestStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，Status 更新必须携带 ResourceVersion。
func (*evictionRequestStatusStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
