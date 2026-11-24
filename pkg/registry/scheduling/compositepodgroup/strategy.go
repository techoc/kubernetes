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

package compositepodgroup

import (
	"context"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/scheduling/validation"
	"k8s.io/kubernetes/pkg/features"
)

// compositePodGroupStrategy 定义 CompositePodGroup 的创建、更新、校验及 Status 子资源策略。
type compositePodGroupStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// NewStrategy 创建 CompositePodGroup 主资源策略，并绑定声明式校验 Scheme。
func NewStrategy() *compositePodGroupStrategy {
	return &compositePodGroupStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
	}
}

// NamespaceScoped 返回 true，因为 CompositePodGroup 是命名空间级资源。
func (*compositePodGroupStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止客户端直接写入控制器状态。
func (*compositePodGroupStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"scheduling.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
	return fields
}

// PrepareForCreate 清空客户端提交的 Status，并清理当前 Feature Gate 未启用且旧对象未使用的字段。
func (*compositePodGroupStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	cpg := obj.(*scheduling.CompositePodGroup)
	// 创建请求不能设置 Status；该字段由调度控制器维护。
	cpg.Status = scheduling.CompositePodGroupStatus{}
	dropDisabledCompositePodGroupFields(cpg, nil)
}

// Validate 校验 CompositePodGroup 的调度策略、工作负载引用、拓扑约束和优先级等字段。
func (*compositePodGroupStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	cpg := obj.(*scheduling.CompositePodGroup)
	return validation.ValidateCompositePodGroup(cpg)
}

// DeclarativeValidationConfig 将结构体标签引用的特性门控映射为声明式校验选项，
// 使校验规则只在对应 Feature Gate 启用时生效。
func (*compositePodGroupStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.CompositePodGroup):        utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup),
		string(features.PodGroupPreemptionPolicy): utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy),
	}}
}

// WarningsOnCreate 返回创建 CompositePodGroup 时的非阻断性警告；当前不产生额外警告。
func (*compositePodGroupStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 CompositePodGroup；当前不执行额外转换。
func (*compositePodGroupStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，CompositePodGroup 必须通过 POST 创建。
func (*compositePodGroupStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，并清理当前 Feature Gate 未启用且旧对象未使用的字段。
func (*compositePodGroupStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCPG := obj.(*scheduling.CompositePodGroup)
	oldCPG := old.(*scheduling.CompositePodGroup)
	newCPG.Status = oldCPG.Status
	dropDisabledCompositePodGroupFields(newCPG, oldCPG)
}

// ValidateUpdate 根据新旧对象校验 CompositePodGroup 更新规则和不可变字段。
func (*compositePodGroupStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCPG := obj.(*scheduling.CompositePodGroup)
	oldCPG := old.(*scheduling.CompositePodGroup)
	return validation.ValidateCompositePodGroupUpdate(newCPG, oldCPG)
}

// WarningsOnUpdate 返回 CompositePodGroup 更新时的非阻断性警告；当前不产生额外警告。
func (*compositePodGroupStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion 以执行乐观并发控制。
func (*compositePodGroupStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type compositePodGroupStatusStrategy struct {
	*compositePodGroupStrategy
}

// NewStatusStrategy 基于主资源策略创建独立的 /status 子资源策略。
func NewStatusStrategy(strategy *compositePodGroupStrategy) *compositePodGroupStatusStrategy {
	return &compositePodGroupStatusStrategy{strategy}
}

// GetResetFields 声明 Status 子资源写入时需要重置的 metadata 和 Spec，确保该入口只能修改 Status。
func (*compositePodGroupStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"scheduling.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
	return fields
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (*compositePodGroupStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCPG := obj.(*scheduling.CompositePodGroup)
	oldCPG := old.(*scheduling.CompositePodGroup)
	newCPG.Spec = oldCPG.Spec
	metav1.ResetObjectMetaForStatus(&newCPG.ObjectMeta, &oldCPG.ObjectMeta)
}

// ValidateUpdate 校验 CompositePodGroup Status 的状态转换是否合法。
func (r *compositePodGroupStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCPG := obj.(*scheduling.CompositePodGroup)
	oldCPG := old.(*scheduling.CompositePodGroup)
	return validation.ValidateCompositePodGroupStatusUpdate(newCPG, oldCPG)
}

// WarningsOnUpdate 返回 CompositePodGroup Status 更新时的非阻断性警告；当前不产生额外警告。
func (*compositePodGroupStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// dropDisabledCompositePodGroupFields 清理当前 Feature Gate 未启用且旧对象也未使用的 CompositePodGroup 字段，兼容集群降级。
func dropDisabledCompositePodGroupFields(newCPG, oldCPG *scheduling.CompositePodGroup) {
	var newCPGSpec, oldCPGSpec *scheduling.CompositePodGroupSpec
	if newCPG != nil {
		newCPGSpec = &newCPG.Spec
	}
	if oldCPG != nil {
		oldCPGSpec = &oldCPG.Spec
	}
	dropDisabledCompositePodGroupSpecFields(newCPGSpec, oldCPGSpec)
}

func dropDisabledCompositePodGroupSpecFields(newCPGSpec, oldCPGSpec *scheduling.CompositePodGroupSpec) {
	dropDisabledPreemptionPolicyField(newCPGSpec, oldCPGSpec)
}

// dropDisabledPreemptionPolicyField 在 PodGroupPreemptionPolicy 未启用且旧 Spec 未使用时清空 PreemptionPolicy，
// 保留旧对象已有值以兼容降级。
func dropDisabledPreemptionPolicyField(newCPGSpec, oldCPGSpec *scheduling.CompositePodGroupSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) || preemptionPolicyInUse(oldCPGSpec) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	newCPGSpec.PreemptionPolicy = nil
}

func preemptionPolicyInUse(cpgSpec *scheduling.CompositePodGroupSpec) bool {
	return cpgSpec != nil && cpgSpec.PreemptionPolicy != nil
}
