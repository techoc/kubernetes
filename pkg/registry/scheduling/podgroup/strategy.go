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

package podgroup

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

// podGroupStrategy 定义 PodGroup 的创建、更新、校验及 Status 子资源策略。
type podGroupStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// NewStrategy 创建 PodGroup 主资源策略，并绑定用于声明式校验的 Scheme。
func NewStrategy() *podGroupStrategy {
	return &podGroupStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
	}
}

// NamespaceScoped 返回 true，因为 PodGroup 必须归属于某个命名空间。
func (*podGroupStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，避免用户通过普通创建或更新接口写入控制器状态。
func (*podGroupStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"scheduling.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"scheduling.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空客户端提交的 Status，并按 Feature Gate 清理不允许新设置的字段。
func (*podGroupStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	podGroup := obj.(*scheduling.PodGroup)
	// 创建请求不能设置 Status；该字段由调度控制器维护。
	podGroup.Status = scheduling.PodGroupStatus{}
	dropDisabledPodGroupFields(podGroup, nil)
}

// Validate 校验 PodGroup 的工作负载引用、调度策略、拓扑约束和资源声明等字段。
func (*podGroupStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	podGroup := obj.(*scheduling.PodGroup)
	return validation.ValidatePodGroup(podGroup)
}

// DeclarativeValidationConfig 将结构体标签引用的特性门控映射为声明式校验选项，
// 使校验规则只在对应 Feature Gate 启用时生效。
func (*podGroupStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.TopologyAwareWorkloadScheduling): utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling),
		string(features.DRAWorkloadResourceClaims):       utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims),
		string(features.PodGroupPreemptionPolicy):        utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy),
		string(features.CompositePodGroup):               utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup),
	}}
}

// WarningsOnCreate 返回创建 PodGroup 时的非阻断性警告；当前不产生额外警告。
func (*podGroupStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 PodGroup；当前不执行额外转换。
func (*podGroupStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，PodGroup 必须通过 POST 创建。
func (*podGroupStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，并清理当前 Feature Gate 未启用且旧对象未使用的字段。
func (*podGroupStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPodGroup := obj.(*scheduling.PodGroup)
	oldPodGroup := old.(*scheduling.PodGroup)
	newPodGroup.Status = oldPodGroup.Status
	dropDisabledPodGroupFields(newPodGroup, oldPodGroup)
}

// ValidateUpdate 根据新旧对象校验 PodGroup 更新规则，包括不可变字段约束。
func (*podGroupStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPodGroup := obj.(*scheduling.PodGroup)
	oldPodGroup := old.(*scheduling.PodGroup)
	return validation.ValidatePodGroupUpdate(newPodGroup, oldPodGroup)
}

// WarningsOnUpdate 返回 PodGroup 主资源更新时的非阻断性警告；当前不产生额外警告。
func (*podGroupStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion 以执行乐观并发控制。
func (*podGroupStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type podGroupStatusStrategy struct {
	*podGroupStrategy
}

// NewStatusStrategy 基于主资源策略创建独立的 /status 子资源策略。
func NewStatusStrategy(podGroupStrategy *podGroupStrategy) *podGroupStatusStrategy {
	return &podGroupStatusStrategy{podGroupStrategy}
}

// GetResetFields 声明 Status 子资源写入时需要重置的 metadata 和 Spec，确保该入口只能修改 Status。
func (*podGroupStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"scheduling.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"scheduling.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (*podGroupStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPodGroup := obj.(*scheduling.PodGroup)
	oldPodGroup := old.(*scheduling.PodGroup)
	newPodGroup.Spec = oldPodGroup.Spec
	metav1.ResetObjectMetaForStatus(&newPodGroup.ObjectMeta, &oldPodGroup.ObjectMeta)
	dropDisabledPodGroupStatusFields(newPodGroup, oldPodGroup)
}

// ValidateUpdate 校验 PodGroup Status 的状态转换是否合法。
func (r *podGroupStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPodGroup := obj.(*scheduling.PodGroup)
	oldPodGroup := old.(*scheduling.PodGroup)
	return validation.ValidatePodGroupStatusUpdate(newPodGroup, oldPodGroup)
}

// WarningsOnUpdate 返回 PodGroup Status 更新时的非阻断性警告；当前不产生额外警告。
func (*podGroupStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// dropDisabledPodGroupFields 清理当前 Feature Gate 未启用且旧对象也未使用的 PodGroup 字段，兼容功能关闭或集群降级。
func dropDisabledPodGroupFields(podGroup, oldPodGroup *scheduling.PodGroup) {
	var podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec
	if podGroup != nil {
		podGroupSpec = &podGroup.Spec
	}
	if oldPodGroup != nil {
		oldPodGroupSpec = &oldPodGroup.Spec
	}
	dropDisabledPodGroupSpecFields(podGroupSpec, oldPodGroupSpec)
}

func dropDisabledPodGroupSpecFields(podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec) {
	dropDisabledSchedulingConstraintsFields(podGroupSpec, oldPodGroupSpec)
	dropDisabledDRAWorkloadResourceClaimsFields(podGroupSpec, oldPodGroupSpec)
	dropDisabledPreemptionPolicyField(podGroupSpec, oldPodGroupSpec)
	dropDisabledParentCompositePodGroupNameField(podGroupSpec, oldPodGroupSpec)
}

// dropDisabledPodGroupStatusFields 在 DRAWorkloadResourceClaims 未启用且旧 Spec 未使用资源声明时，清空 Status 中的资源声明状态。
func dropDisabledPodGroupStatusFields(newPodGroup, oldPodGroup *scheduling.PodGroup) {
	var oldPodGroupSpec *scheduling.PodGroupSpec
	if oldPodGroup != nil {
		oldPodGroupSpec = &oldPodGroup.Spec
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims) || draWorkloadResourceClaimsInUse(oldPodGroupSpec) {
		// 对应特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	newPodGroup.Status.ResourceClaimStatuses = nil
}

// dropDisabledSchedulingConstraintsFields 在 TopologyAwareWorkloadScheduling 未启用且旧 PodGroup 未使用该字段时，
// 清空新对象的 SchedulingConstraints；若旧对象已使用，则保留以兼容降级。
func dropDisabledSchedulingConstraintsFields(podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) || schedulingConstraintsInUse(oldPodGroupSpec) {
		// 对应特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	podGroupSpec.SchedulingConstraints = nil
}

// dropDisabledDRAWorkloadResourceClaimsFields 在 DRAWorkloadResourceClaims 未启用且旧 Spec 未使用资源声明时，
// 清空 ResourceClaims；旧对象已使用时保留原值。
func dropDisabledDRAWorkloadResourceClaimsFields(podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims) || draWorkloadResourceClaimsInUse(oldPodGroupSpec) {
		// 对应特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	podGroupSpec.ResourceClaims = nil
}

// dropDisabledPreemptionPolicyField 在 PodGroupPreemptionPolicy 未启用且旧 Spec 未使用时清空 PreemptionPolicy，
// 保留旧对象已有值以兼容降级。
func dropDisabledPreemptionPolicyField(podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) || preemptionPolicyInUse(oldPodGroupSpec) {
		// 对应特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	podGroupSpec.PreemptionPolicy = nil
}

func schedulingConstraintsInUse(podGroupSpec *scheduling.PodGroupSpec) bool {
	return podGroupSpec != nil && podGroupSpec.SchedulingConstraints != nil
}

func draWorkloadResourceClaimsInUse(podGroupSpec *scheduling.PodGroupSpec) bool {
	return podGroupSpec != nil && len(podGroupSpec.ResourceClaims) > 0
}

func preemptionPolicyInUse(podGroupSpec *scheduling.PodGroupSpec) bool {
	return podGroupSpec != nil && podGroupSpec.PreemptionPolicy != nil
}

func parentCompositePodGroupNameInUse(podGroupSpec *scheduling.PodGroupSpec) bool {
	return podGroupSpec != nil && podGroupSpec.ParentCompositePodGroupName != nil
}

// dropDisabledParentCompositePodGroupNameField 在 CompositePodGroup 未启用且旧 Spec 未使用时清空 ParentCompositePodGroupName，
// 旧对象已使用该字段时继续保留。
func dropDisabledParentCompositePodGroupNameField(podGroupSpec, oldPodGroupSpec *scheduling.PodGroupSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) || parentCompositePodGroupNameInUse(oldPodGroupSpec) {
		// 对应特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	podGroupSpec.ParentCompositePodGroupName = nil
}
