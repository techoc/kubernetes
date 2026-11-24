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

package eviction

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
	"k8s.io/utils/clock"
)

// evictionStrategy 定义 Eviction 的创建、更新、校验和 Status 子资源策略。
type evictionStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	clock clock.PassiveClock
}

// NewStrategy 创建 Eviction 主资源策略，并注入被动时钟供状态校验使用。
func NewStrategy(clock clock.PassiveClock) *evictionStrategy {
	return &evictionStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
		clock,
	}
}

var _ = rest.ResetFieldsStrategy(&evictionStrategy{})

// NamespaceScoped 返回 true，因为 Eviction 属于目标对象所在的命名空间。
func (*evictionStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止用户绕过控制器直接修改驱逐状态。
func (*evictionStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
	return fields
}

// PrepareForCreate 在创建时清空客户端提交的 Status，并将 Generation 初始化为 1。
func (*evictionStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	eviction := obj.(*lifecycle.Eviction)
	eviction.Status = lifecycle.EvictionStatus{}
	eviction.Generation = 1
}

// PrepareForUpdate 恢复旧 Status；Spec 发生变化时递增 Generation。
func (*evictionStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	oldEviction := old.(*lifecycle.Eviction)
	newEviction := obj.(*lifecycle.Eviction)
	newEviction.Status = oldEviction.Status

	// Spec 变化时递增 Generation，Status 变化不应改变 Generation。
	if !apiequality.Semantic.DeepEqual(oldEviction.Spec, newEviction.Spec) {
		newEviction.Generation = oldEviction.Generation + 1
	}
}

// Validate 校验 Eviction 的目标对象引用、驱逐原因及策略约束。
func (s *evictionStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	eviction := obj.(*lifecycle.Eviction)

	allErrs := validation.ValidateEviction(eviction)
	return allErrs
}

// DeclarativeValidationConfig 返回 Eviction 当前使用的声明式校验选项；此处没有额外门控配置。
func (*evictionStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{}
}

func (*evictionStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (*evictionStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，Eviction 必须通过 POST 创建。
func (*evictionStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据新旧对象校验 Eviction 更新是否合法。
func (s *evictionStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	var allErrs field.ErrorList
	eviction := obj.(*lifecycle.Eviction)
	oldEviction := old.(*lifecycle.Eviction)

	allErrs = validation.ValidateEvictionUpdate(eviction, oldEviction)
	return allErrs
}

func (*evictionStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新必须携带 ResourceVersion 以避免覆盖并发状态。
func (*evictionStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// evictionStatusStrategy 定义通过 /status 子资源更新 Eviction 状态时使用的策略。
type evictionStatusStrategy struct {
	*evictionStrategy
}

var _ = rest.ResetFieldsStrategy(&evictionStatusStrategy{})

// NewStatusStrategy 基于主资源策略创建独立的 /status 子资源策略，并沿用同一时钟。
func NewStatusStrategy(strategy *evictionStrategy) *evictionStatusStrategy {
	return &evictionStatusStrategy{
		strategy,
	}
}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (*evictionStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
	}
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (*evictionStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newEviction := obj.(*lifecycle.Eviction)
	oldEviction := old.(*lifecycle.Eviction)
	newEviction.Spec = oldEviction.Spec
	// Status 更新不应同时授予修改元数据的权限；标签应由 eviction-controller 负责维护，
	// 而不是由响应驱逐请求的一方修改，以免扩大标签写入权限。
	metav1.ResetObjectMetaForStatus(&newEviction.ObjectMeta, &oldEviction.ObjectMeta)
}

// AllowCreateOnUpdate 返回 false，Status 子资源不能通过更新请求创建对象。
func (*evictionStatusStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 使用注入的时钟校验 Eviction Status 更新是否合法，包括状态转换的时间约束。
func (s *evictionStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	allErrs := validation.ValidateEvictionStatusUpdate(obj.(*lifecycle.Eviction), old.(*lifecycle.Eviction), validation.EvictionStatusValidationOptions{
		Clock: s.clock,
	})
	return allErrs
}

// WarningsOnUpdate 返回 Eviction Status 更新时的非阻断性警告；当前不产生额外警告。
func (*evictionStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，Status 更新必须携带 ResourceVersion。
func (*evictionStatusStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
