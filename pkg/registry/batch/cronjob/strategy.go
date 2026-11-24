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

package cronjob

import (
	"context"
	"fmt"
	"strings"

	batchv1beta1 "k8s.io/api/batch/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/job"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/batch"
	batchvalidation "k8s.io/kubernetes/pkg/apis/batch/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// cronJobStrategy 定义 CronJob 主资源写入策略，负责调度配置、JobTemplate 字段清理、Generation 和状态隔离。
type cronJobStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 CronJob 主资源的默认 REST 策略；删除传播策略按 API 版本保留历史兼容行为。
var Strategy = cronJobStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// DefaultGarbageCollectionPolicy 按 API 版本选择默认删除传播策略：batch/v1beta1 保留孤儿化兼容行为，其他版本级联删除。
// batch/v1beta1 为兼容旧客户端默认孤儿化删除；当前版本默认级联删除 CronJob 创建的 Job。
func (cronJobStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	var groupVersion schema.GroupVersion
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		groupVersion = schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	}
	switch groupVersion {
	case batchv1beta1.SchemeGroupVersion:
		// 为兼容旧版 batch/v1beta1 客户端，保留孤儿化删除行为。
		return rest.OrphanDependents
	default:
		return rest.DeleteDependents
	}
}

// NamespaceScoped 表示 CronJob 是命名空间级资源，必须归属于某个 Namespace。
func (cronJobStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回写入策略会重置的字段集合；这些字段不接受普通用户通过主资源接口修改。
func (cronJobStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"batch/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"batch/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 CronJob 前清理由控制器维护的状态，并规范化内嵌 JobTemplate。
// 创建时清空 CronJob Status、初始化 Generation，并清理 JobTemplate 与其内嵌 PodTemplate 中被禁用的字段。
func (cronJobStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	cronJob := obj.(*batch.CronJob)
	cronJob.Status = batch.CronJobStatus{}

	cronJob.Generation = 1

	job.DropDisabledFields(&cronJob.Spec.JobTemplate.Spec, nil)

	pod.DropDisabledTemplateFields(&cronJob.Spec.JobTemplate.Spec.Template, nil)
}

// PrepareForUpdate 在更新 CronJob 时恢复旧状态，并清理当前 Feature Gate 不允许设置的 JobTemplate 字段。
// 更新时沿用旧 Status，按 Feature Gate 清理 JobTemplate 字段；Spec 任何语义变化都会递增 Generation。
func (cronJobStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCronJob := obj.(*batch.CronJob)
	oldCronJob := old.(*batch.CronJob)
	newCronJob.Status = oldCronJob.Status

	job.DropDisabledFields(&newCronJob.Spec.JobTemplate.Spec, &oldCronJob.Spec.JobTemplate.Spec)

	pod.DropDisabledTemplateFields(&newCronJob.Spec.JobTemplate.Spec.Template, &oldCronJob.Spec.JobTemplate.Spec.Template)

	// Spec 发生语义变化时递增 Generation，使 CronJob Controller 识别期望配置的新版本。
	if !apiequality.Semantic.DeepEqual(newCronJob.Spec, oldCronJob.Spec) {
		newCronJob.Generation = oldCronJob.Generation + 1
	}
}

// Validate 对新建 CronJob 执行调度配置及内嵌 Job/Pod 模板校验。
// Validate 除 Cron 表达式/时区、并发策略和历史记录限制外，还使用 PodTemplate 选项验证内嵌 JobTemplate。
func (cronJobStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	cronJob := obj.(*batch.CronJob)
	opts := pod.GetValidationOptionsFromPodTemplate(&cronJob.Spec.JobTemplate.Spec.Template, nil)
	return batchvalidation.ValidateCronJobCreate(cronJob, opts)
}

// DeclarativeValidationConfig 为声明式校验提供特性开关配置，键对应类型标签引用的校验选项。
func (cronJobStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.WorkloadWithJob): utilfeature.DefaultFeatureGate.Enabled(features.WorkloadWithJob),
	}}
}

// WarningsOnCreate 返回创建 CronJob 时应告知客户端的非阻断性警告。
func (cronJobStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newCronJob := obj.(*batch.CronJob)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newCronJob.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, job.WarningsForJobSpec(ctx, field.NewPath("spec", "jobTemplate", "spec"), &newCronJob.Spec.JobTemplate.Spec, nil)...)
	return warnings
}

// Canonicalize 在校验完成后规范化对象；当前 CronJob 策略无需额外处理。
func (cronJobStrategy) Canonicalize(obj runtime.Object) {
}

func (cronJobStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// AllowCreateOnUpdate 对 CronJob 返回 false，创建资源必须使用 POST，不能通过更新不存在的对象来创建。
func (cronJobStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对普通用户提交的 CronJob 更新执行完整校验，并结合旧 JobTemplate 应用兼容规则。
// ValidateUpdate 对比新旧 JobTemplate 与调度字段，检查不可变约束及历史 API 兼容规则。
func (cronJobStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCronJob := obj.(*batch.CronJob)
	oldCronJob := old.(*batch.CronJob)

	opts := pod.GetValidationOptionsFromPodTemplate(&newCronJob.Spec.JobTemplate.Spec.Template, &oldCronJob.Spec.JobTemplate.Spec.Template)
	return batchvalidation.ValidateCronJobUpdate(newCronJob, oldCronJob, opts)
}

// WarningsOnUpdate 返回更新 CronJob 时应告知客户端的非阻断性警告。
// Schedule 使用旧 TZ/CRON_TZ 写法时发出弃用 Warning；建议通过 spec.timeZone 设置时区。
func (cronJobStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newCronJob := obj.(*batch.CronJob)
	oldCronJob := old.(*batch.CronJob)
	if newCronJob.Generation != oldCronJob.Generation {
		warnings = job.WarningsForJobSpec(ctx, field.NewPath("spec", "jobTemplate", "spec"), &newCronJob.Spec.JobTemplate.Spec, &oldCronJob.Spec.JobTemplate.Spec)
	}
	if strings.Contains(newCronJob.Spec.Schedule, "TZ") {
		warnings = append(warnings, fmt.Sprintf("cannot use TZ or CRON_TZ in %s, use timeZone instead, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details", field.NewPath("spec", "spec", "schedule")))
	}
	return warnings
}

type cronJobStatusStrategy struct {
	cronJobStrategy
}

// StatusStrategy 定义 CronJob /status 子资源的默认更新策略。
var StatusStrategy = cronJobStatusStrategy{Strategy}

// GetResetFields 返回写入策略会重置的字段集合；这些字段不接受普通用户通过主资源接口修改。
func (cronJobStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"batch/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
		"batch/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// /status 子资源只允许调度控制器更新 LastScheduleTime、Active 和 Conditions 等状态，Spec 从旧对象恢复。
func (cronJobStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newJob := obj.(*batch.CronJob)
	oldJob := old.(*batch.CronJob)
	newJob.Spec = oldJob.Spec
}

func (cronJobStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate 返回更新 CronJob 时应告知客户端的非阻断性警告。
func (cronJobStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
