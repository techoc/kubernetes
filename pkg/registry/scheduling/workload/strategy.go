/*
Copyright 2025 The Kubernetes Authors.

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

package workload

import (
	"context"

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

// workloadStrategy 定义 Workload 的创建、更新和校验策略。
type workloadStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 Workload 对象时使用的默认策略。
var Strategy = workloadStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，因为 Workload 是命名空间级资源。
func (workloadStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 在创建前清理当前 Feature Gate 未启用且旧对象不存在的嵌套模板字段。
func (workloadStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	dropDisabledWorkloadFields(obj.(*scheduling.Workload), nil)
}

// Validate 校验 Workload 的 PodGroupTemplate、CompositePodGroupTemplate 及引用关系。
func (workloadStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	workloadScheduling := obj.(*scheduling.Workload)
	return validation.ValidateWorkload(workloadScheduling)
}

// DeclarativeValidationConfig 将结构体标签引用的特性门控映射为声明式校验选项，
// 使 Workload 模板中的字段校验与集群实际启用的能力保持一致。
func (workloadStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.TopologyAwareWorkloadScheduling): utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling),
		string(features.DRAWorkloadResourceClaims):       utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims),
		string(features.PodGroupPreemptionPolicy):        utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy),
		string(features.CompositePodGroup):               utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup),
	}}
}

// WarningsOnCreate 返回创建 Workload 时的非阻断性警告；当前不产生额外警告。
func (workloadStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 Workload；当前不执行额外转换。
func (workloadStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，Workload 必须通过 POST 创建。
func (workloadStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 根据旧对象和当前 Feature Gate 清理新对象中不允许新设置的嵌套模板字段。
func (workloadStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	dropDisabledWorkloadFields(obj.(*scheduling.Workload), old.(*scheduling.Workload))
}

// ValidateUpdate 根据新旧 Workload 校验更新规则及不可变字段。
func (workloadStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateWorkloadUpdate(obj.(*scheduling.Workload), old.(*scheduling.Workload))
}

// WarningsOnUpdate 返回更新 Workload 时的非阻断性警告；当前不产生额外警告。
func (workloadStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion 以执行乐观并发控制。
func (workloadStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// dropDisabledWorkloadFields 清理当前 Feature Gate 未启用且旧 Workload 未使用的字段，保留历史字段以兼容集群降级。
func dropDisabledWorkloadFields(workload, oldWorkload *scheduling.Workload) {
	var workloadSpec, oldWorkloadSpec *scheduling.WorkloadSpec
	if workload != nil {
		workloadSpec = &workload.Spec
	}
	if oldWorkload != nil {
		oldWorkloadSpec = &oldWorkload.Spec
	}
	dropDisabledWorkloadSpecFields(workloadSpec, oldWorkloadSpec)
}

func dropDisabledWorkloadSpecFields(workloadSpec, oldWorkloadSpec *scheduling.WorkloadSpec) {
	var templates, oldTemplates []scheduling.PodGroupTemplate
	if workloadSpec != nil {
		templates = workloadSpec.PodGroupTemplates
	}
	if oldWorkloadSpec != nil {
		oldTemplates = oldWorkloadSpec.PodGroupTemplates
	}
	dropDisabledPodGroupTemplatesFields(templates, oldTemplates)

	var cpgTemplates, oldCpgTemplates []scheduling.CompositePodGroupTemplate
	if workloadSpec != nil {
		cpgTemplates = workloadSpec.CompositePodGroupTemplates
	}
	if oldWorkloadSpec != nil {
		oldCpgTemplates = oldWorkloadSpec.CompositePodGroupTemplates
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		if oldWorkloadSpec == nil || len(oldWorkloadSpec.CompositePodGroupTemplates) == 0 {
			if workloadSpec != nil {
				workloadSpec.CompositePodGroupTemplates = nil
			}
		}
	}
	dropDisabledCompositePodGroupTemplatesFields(cpgTemplates, oldCpgTemplates)
}

func dropDisabledPodGroupTemplatesFields(templates, oldTemplates []scheduling.PodGroupTemplate) {
	m := len(oldTemplates)
	for i := range templates {
		var oldTemplate *scheduling.PodGroupTemplate
		if i < m {
			oldTemplate = &oldTemplates[i]
		}
		template := &templates[i]
		dropDisabledSchedulingConstraintsFields(template, oldTemplate)
		dropDisabledDRAWorkloadResourceClaimsFields(template, oldTemplate)
		dropDisabledPGPreemptionPolicyFields(template, oldTemplate)
	}
}

func dropDisabledCompositePodGroupTemplatesFields(templates, oldTemplates []scheduling.CompositePodGroupTemplate) {
	oldTemplatesMap := make(map[string]*scheduling.CompositePodGroupTemplate)
	for i := range oldTemplates {
		oldTemplatesMap[oldTemplates[i].Name] = &oldTemplates[i]
	}
	for i := range templates {
		template := &templates[i]
		oldTemplate := oldTemplatesMap[template.Name]

		dropDisabledCPGPreemptionPolicyFields(template, oldTemplate)

		var cpgTemplates, oldCpgTemplates []scheduling.CompositePodGroupTemplate
		cpgTemplates = template.CompositePodGroupTemplates
		if oldTemplate != nil {
			oldCpgTemplates = oldTemplate.CompositePodGroupTemplates
		}
		if !utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
			if oldTemplate == nil || len(oldTemplate.CompositePodGroupTemplates) == 0 {
				template.CompositePodGroupTemplates = nil
			}
		}
		dropDisabledCompositePodGroupTemplatesFields(cpgTemplates, oldCpgTemplates)

		var pgTemplates, oldPgTemplates []scheduling.PodGroupTemplate
		pgTemplates = template.PodGroupTemplates
		if oldTemplate != nil {
			oldPgTemplates = oldTemplate.PodGroupTemplates
		}
		dropDisabledPodGroupTemplatesFields(pgTemplates, oldPgTemplates)
	}
}

// dropDisabledSchedulingConstraintsFields 在 TopologyAwareWorkloadScheduling 未启用且旧模板未使用时，
// 清空 PodGroupTemplate 的 SchedulingConstraints。
func dropDisabledSchedulingConstraintsFields(template, oldTemplate *scheduling.PodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) || schedulingConstraintsInUse(oldTemplate) {
		return
	}
	template.SchedulingConstraints = nil
}

// dropDisabledDRAWorkloadResourceClaimsFields 在 DRAWorkloadResourceClaims 未启用且旧模板未使用时，
// 清空 PodGroupTemplate 的 ResourceClaims。
func dropDisabledDRAWorkloadResourceClaimsFields(template, oldTemplate *scheduling.PodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims) || draWorkloadResourceClaimsInUse(oldTemplate) {
		return
	}
	template.ResourceClaims = nil
}

// dropDisabledPGPreemptionPolicyFields 在 PodGroupPreemptionPolicy 未启用且旧模板未使用时，
// 清空 PodGroupTemplate 的 PreemptionPolicy。
func dropDisabledPGPreemptionPolicyFields(template, oldTemplate *scheduling.PodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) || preemptionPolicyInUse(oldTemplate) {
		return
	}
	template.PreemptionPolicy = nil
}

func schedulingConstraintsInUse(pgt *scheduling.PodGroupTemplate) bool {
	return pgt != nil && pgt.SchedulingConstraints != nil
}

func draWorkloadResourceClaimsInUse(pgt *scheduling.PodGroupTemplate) bool {
	return pgt != nil && len(pgt.ResourceClaims) > 0
}

func preemptionPolicyInUse(pgt *scheduling.PodGroupTemplate) bool {
	return pgt != nil && pgt.PreemptionPolicy != nil
}

// dropDisabledCPGPreemptionPolicyFields 在 PodGroupPreemptionPolicy 未启用且旧模板未使用时，
// 清空 CompositePodGroupTemplate 的 PreemptionPolicy。
func dropDisabledCPGPreemptionPolicyFields(template, oldTemplate *scheduling.CompositePodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) || cpgPreemptionPolicyInUse(oldTemplate) {
		return
	}
	template.PreemptionPolicy = nil
}

func cpgPreemptionPolicyInUse(cpgt *scheduling.CompositePodGroupTemplate) bool {
	return cpgt != nil && cpgt.PreemptionPolicy != nil
}
