/*
Copyright 2015 The Kubernetes Authors.

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

package job

import (
	"context"
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/job"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/batch"
	batchvalidation "k8s.io/kubernetes/pkg/apis/batch/validation"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// jobStrategy 定义 Job 主资源的 REST 写入策略，负责 Selector/模板标签生成、暂停 Job 可变字段兼容及终态状态隔离。
type jobStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 Job 主资源的默认 REST 策略；删除传播策略按 API 版本保留历史兼容行为。
var Strategy = jobStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// DefaultGarbageCollectionPolicy 按 API 版本选择默认删除传播策略：batch/v1 保留孤儿化兼容行为，其他版本级联删除。
// batch/v1 为兼容旧客户端默认孤儿化删除，其他版本默认级联删除 Job 所属 Pod。
func (jobStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	var groupVersion schema.GroupVersion
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		groupVersion = schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	}
	switch groupVersion {
	case batchv1.SchemeGroupVersion:
		// 为兼容旧版 batch/v1 客户端，保留孤儿化删除行为。
		return rest.OrphanDependents
	default:
		return rest.DeleteDependents
	}
}

// NamespaceScoped 表示 Job 是命名空间级资源，必须归属于某个 Namespace。
func (jobStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回写入策略会重置的字段集合；这些字段不接受普通用户通过主资源接口修改。
func (jobStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"batch/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 Job 前清理由控制器维护的状态，并规范化用户提交的 Spec。
// 创建时先生成/补全 Job Selector 与模板标签，再清空 Status、初始化 Generation，并清理禁用字段。
func (jobStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	newJob := obj.(*batch.Job)
	generateSelectorIfNeeded(newJob)
	newJob.Status = batch.JobStatus{}

	newJob.Generation = 1

	// JobManagedBy 未启用时不接受用户自定义 managedBy，继续由内置 Job Controller 管理该对象。
	if !utilfeature.DefaultFeatureGate.Enabled(features.JobManagedBy) {
		newJob.Spec.ManagedBy = nil
	}

	job.DropDisabledFields(&newJob.Spec, nil)

	pod.DropDisabledTemplateFields(&newJob.Spec.Template, nil)
}

// PrepareForUpdate 在更新 Job 时恢复只读状态字段，并清理当前 Feature Gate 不允许设置的字段。
// 更新时保留旧 Status，清理禁用字段；只有 Job.Spec 变化才递增 Generation。
func (jobStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newJob := obj.(*batch.Job)
	oldJob := old.(*batch.Job)
	newJob.Status = oldJob.Status

	job.DropDisabledFields(&newJob.Spec, &oldJob.Spec)

	pod.DropDisabledTemplateFields(&newJob.Spec.Template, &oldJob.Spec.Template)

	// Spec 发生语义变化时递增 Generation，使控制器识别期望配置的新版本。
	if !apiequality.Semantic.DeepEqual(newJob.Spec, oldJob.Spec) {
		newJob.Generation = oldJob.Generation + 1
	}

}

// Validate 对新建 Job 执行批处理资源校验，并根据新旧 PodTemplate 计算兼容选项。
func (jobStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	job := obj.(*batch.Job)
	opts := validationOptionsForJob(job, nil)
	return batchvalidation.ValidateJobCreate(job, opts)
}

// DeclarativeValidationConfig 为声明式校验提供特性开关配置，键对应类型标签引用的校验选项。
func (jobStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.WorkloadWithJob): utilfeature.DefaultFeatureGate.Enabled(features.WorkloadWithJob),
	}}
}

// shouldAllowMutablePodTemplate 判断 Job 的 PodTemplate 是否允许修改。
// 只有 Job 已暂停、没有活跃 Pod，且从未启动或已有 JobSuspended=True 条件时才允许修改。
// 仅当 Job 已暂停、没有活跃 Pod，且从未启动或控制器已报告 JobSuspended 时，才允许修改其 PodTemplate。
func shouldAllowMutablePodTemplate(job *batch.Job) bool {
	suspended := job.Spec.Suspend != nil && *job.Spec.Suspend
	notStarted := job.Status.StartTime == nil
	hasSuspendedCondition := batchvalidation.IsConditionTrue(job.Status.Conditions, batch.JobSuspended)
	return suspended && (notStarted || hasSuspendedCondition) && job.Status.Active == 0
}

// validationOptionsForJob 根据新旧 Job 计算兼容校验选项，决定 Selector 旧值、暂停 Job 的调度指令和资源是否可变。
func validationOptionsForJob(newJob, oldJob *batch.Job) batchvalidation.JobValidationOptions {
	var newPodTemplate, oldPodTemplate *core.PodTemplateSpec
	if newJob != nil {
		newPodTemplate = &newJob.Spec.Template
	}
	if oldJob != nil {
		oldPodTemplate = &oldJob.Spec.Template
	}
	opts := batchvalidation.JobValidationOptions{
		PodValidationOptions:  pod.GetValidationOptionsFromPodTemplate(newPodTemplate, oldPodTemplate),
		RequirePrefixedLabels: true,
	}
	if oldJob != nil {
		opts.AllowInvalidLabelValueInSelector = opts.AllowInvalidLabelValueInSelector || metav1validation.LabelSelectorHasInvalidLabelValue(oldJob.Spec.Selector)
		// 普通 Job 的调度指令创建后不可随意修改；旧 Job 已暂停且未启动时才具备兼容更新条件。
		// nodeAffinity、nodeSelector 和 tolerations 等调度指令，仅允许从未启动且处于暂停状态的 Job 修改。
		suspended := oldJob.Spec.Suspend != nil && *oldJob.Spec.Suspend
		notStarted := oldJob.Status.StartTime == nil
		opts.AllowMutableSchedulingDirectives = suspended && notStarted
		allowMutablePodTemplate := shouldAllowMutablePodTemplate(oldJob)
		if utilfeature.DefaultFeatureGate.Enabled(features.MutablePodResourcesForSuspendedJobs) {
			opts.AllowMutablePodResources = allowMutablePodTemplate
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.MutableSchedulingDirectivesForSuspendedJobs) {
			opts.AllowMutableSchedulingDirectives = allowMutablePodTemplate
		}
		// 旧 Job 可能尚未包含新版标签，因此不能仅因缺少这些标签就使更新校验失败。
		// 确认历史对象已普遍具备两个标签后，可移除此兼容逻辑。
		_, hadJobName := oldJob.Spec.Template.Labels[batch.JobNameLabel]
		_, hadControllerUid := oldJob.Spec.Template.Labels[batch.ControllerUidLabel]
		opts.RequirePrefixedLabels = hadJobName && hadControllerUid
	}
	return opts
}

// WarningsOnCreate 返回创建 Job 时应告知客户端的非阻断性警告。
func (jobStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newJob := obj.(*batch.Job)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newJob.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, job.WarningsForJobSpec(ctx, field.NewPath("spec"), &newJob.Spec, nil)...)
	return warnings
}

// generateSelectorIfNeeded 检查 manualSelector 开关；未启用手动 Selector 时，由 API Server 自动生成 Selector 及对应模板标签。
// ManualSelector=false 时由 API Server 生成唯一 Selector，确保 Job Controller 只管理该 Job 创建的 Pod。
func generateSelectorIfNeeded(obj *batch.Job) {
	if !*obj.Spec.ManualSelector {
		generateSelector(obj)
	}
}

// generateSelector 为 Job 生成 Selector，并向 PodTemplate 添加用于唯一识别该 Job 所创建 Pod 的标签。
// 该逻辑仅在用户未要求手动指定 Selector 时调用。
// generateSelector 同时给 PodTemplate 写入友好 JobName 和唯一 ControllerUID 标签，并将唯一 UID 标签写入 Job.Spec.Selector。
func generateSelector(obj *batch.Job) {
	if obj.Spec.Template.Labels == nil {
		obj.Spec.Template.Labels = make(map[string]string)
	}
	// job-name 标签在常规场景下足以区分 Job，且比 UID 更便于用户识别，因此同时写入模板标签。
	jobNameLabels := []string{batch.LegacyJobNameLabel, batch.JobNameLabel}
	for _, value := range jobNameLabels {
		_, found := obj.Spec.Template.Labels[value]
		if found {
			// 用户要求自动生成 Selector，却预先设置了相关标签；这里不覆盖用户值，冲突交由后续校验拒绝。
		} else {
			obj.Spec.Template.Labels[value] = string(obj.ObjectMeta.Name)
		}
	}

	// controller-uid 标签携带 Job 的 UID，使该 Job 创建的 Pod 不会被其他 Job 的 Selector 匹配。
	controllerUidLabels := []string{batch.LegacyControllerUidLabel, batch.ControllerUidLabel}
	for _, value := range controllerUidLabels {
		_, found := obj.Spec.Template.Labels[value]
		if found {
			// 用户要求自动生成 Selector，却预先设置了相关标签；这里不覆盖用户值，冲突交由后续校验拒绝。
		} else {
			obj.Spec.Template.Labels[value] = string(obj.ObjectMeta.UID)
		}
	}
	// 使用 controller-uid 作为 Selector 的核心匹配条件即可保证 Job 归属关系的唯一性。
	if obj.Spec.Selector == nil {
		obj.Spec.Selector = &metav1.LabelSelector{}
	}
	if obj.Spec.Selector.MatchLabels == nil {
		obj.Spec.Selector.MatchLabels = make(map[string]string)
	}

	if _, found := obj.Spec.Selector.MatchLabels[batch.ControllerUidLabel]; !found {
		obj.Spec.Selector.MatchLabels[batch.ControllerUidLabel] = string(obj.ObjectMeta.UID)
	}
	// 如果用户在 MatchLabels 中指定了错误的 controller-uid，Selector 将无法匹配上面写入模板的真实 UID；若模板标签本身冲突，也会由后续校验拒绝。
}

// TODO：将 generateSelector 抽象为可供 ReplicaSet 等控制器资源复用的通用实现；还需要统一访问 UID、Selector 和 Pod 标签字段的接口。

// Canonicalize 在校验完成后规范化对象；当前 Job 策略无需额外处理。
func (jobStrategy) Canonicalize(obj runtime.Object) {
}

func (jobStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// AllowCreateOnUpdate 对 Job 返回 false，创建资源必须使用 POST，不能通过更新不存在的对象来创建。
func (jobStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对普通用户提交的 Job 更新执行完整校验，并结合旧对象应用兼容规则。
func (jobStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	job := obj.(*batch.Job)
	oldJob := old.(*batch.Job)

	opts := validationOptionsForJob(job, oldJob)
	return batchvalidation.ValidateJobUpdate(job, oldJob, opts)
}

// WarningsOnUpdate 返回更新 Job 时应告知客户端的非阻断性警告。
func (jobStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newJob := obj.(*batch.Job)
	oldJob := old.(*batch.Job)
	if newJob.Generation != oldJob.Generation {
		warnings = job.WarningsForJobSpec(ctx, field.NewPath("spec"), &newJob.Spec, &oldJob.Spec)
	}
	return warnings
}

type jobStatusStrategy struct {
	jobStrategy
}

var StatusStrategy = jobStatusStrategy{Strategy}

// GetResetFields 返回写入策略会重置的字段集合；这些字段不接受普通用户通过主资源接口修改。
func (jobStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"batch/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// /status 不允许修改 Job.Spec；所有副本计数、条件和索引完成状态由对应控制器通过此子资源回写。
func (jobStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newJob := obj.(*batch.Job)
	oldJob := old.(*batch.Job)
	newJob.Spec = oldJob.Spec
}

func (jobStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newJob := obj.(*batch.Job)
	oldJob := old.(*batch.Job)

	opts := getStatusValidationOptions(newJob, oldJob)
	return batchvalidation.ValidateJobUpdateStatus(newJob, oldJob, opts)
}

// getStatusValidationOptions 根据新旧 Job 状态变化生成状态校验选项。
// JobManagedBy 启用时强化外部控制器的 Status 状态机约束，阻止终态回退、索引重叠和时间戳非法变化。
func getStatusValidationOptions(newJob, oldJob *batch.Job) batchvalidation.JobStatusValidationOptions {
	// 仅当特性开启时启用强化状态转换规则；并且只校验本次发生变化的状态字段，避免历史异常阻塞无关更新。
	if utilfeature.DefaultFeatureGate.Enabled(features.JobManagedBy) {
		// JobManagedBy 允许外部控制器管理 Job，因此必须强化状态转换校验，确保外部控制器写入的状态符合 Job API 对客户端的承诺，例如 Failed 或 Complete 等终态不能回退为非终态。
		// 以下规则仅在对应状态字段或条件相较旧对象发生变化时才拒绝更新；无关状态字段的更新不应被历史异常连带阻塞。
		// 此外，用户修改 Spec 后，部分状态可能暂时不满足新配置对应的约束，需要给 Job Controller 留出下一轮同步修正状态的时间。例如缩减 completions 后，completedIndexes 可能暂时包含超出新 completions 范围的索引。
		isIndexed := ptr.Deref(newJob.Spec.CompletionMode, batch.NonIndexedCompletion) == batch.IndexedCompletion

		isJobFinishedChanged := batchvalidation.IsJobFinished(oldJob) != batchvalidation.IsJobFinished(newJob)
		isJobCompleteChanged := batchvalidation.IsJobComplete(oldJob) != batchvalidation.IsJobComplete(newJob)
		isJobFailedChanged := batchvalidation.IsJobFailed(oldJob) != batchvalidation.IsJobFailed(newJob)
		isJobFailureTargetChanged := batchvalidation.IsConditionTrue(oldJob.Status.Conditions, batch.JobFailureTarget) != batchvalidation.IsConditionTrue(newJob.Status.Conditions, batch.JobFailureTarget)
		isJobSuccessCriteriaMetChanged := batchvalidation.IsConditionTrue(oldJob.Status.Conditions, batch.JobSuccessCriteriaMet) != batchvalidation.IsConditionTrue(newJob.Status.Conditions, batch.JobSuccessCriteriaMet)
		isCompletedIndexesChanged := oldJob.Status.CompletedIndexes != newJob.Status.CompletedIndexes
		isFailedIndexesChanged := !ptr.Equal(oldJob.Status.FailedIndexes, newJob.Status.FailedIndexes)
		isActiveChanged := oldJob.Status.Active != newJob.Status.Active
		isStartTimeChanged := !ptr.Equal(oldJob.Status.StartTime, newJob.Status.StartTime)
		isCompletionTimeChanged := !ptr.Equal(oldJob.Status.CompletionTime, newJob.Status.CompletionTime)
		isUncountedTerminatedPodsChanged := !apiequality.Semantic.DeepEqual(oldJob.Status.UncountedTerminatedPods, newJob.Status.UncountedTerminatedPods)
		isReadyChanged := !ptr.Equal(oldJob.Status.Ready, newJob.Status.Ready)
		isTerminatingChanged := !ptr.Equal(oldJob.Status.Terminating, newJob.Status.Terminating)
		isSuspendedWithZeroCompletions := ptr.Equal(newJob.Spec.Suspend, ptr.To(true)) && ptr.Equal(newJob.Spec.Completions, ptr.To[int32](0))
		// 通过 JobSuspended 条件从 True 变为 False 识别恢复运行；即使用户已先将 spec.suspend 设为 false，控制器随后才更新 Status，也能正确识别该恢复过程。
		// 相关时序问题参见 https://github.com/kubernetes/kubernetes/issues/134521。
		isJobResuming := batchvalidation.IsConditionTrue(oldJob.Status.Conditions, batch.JobSuspended) &&
			batchvalidation.IsConditionFalse(newJob.Status.Conditions, batch.JobSuspended)

		return batchvalidation.JobStatusValidationOptions{
			// 当 Indexed Job 的 parallelism 与 completions 相等时，缩容可能导致成功 Pod 计数下降，因此允许该计数回退。
			RejectDecreasingSucceededCounter:             !isIndexed || !ptr.Equal(newJob.Spec.Completions, newJob.Spec.Parallelism),
			RejectDecreasingFailedCounter:                true,
			RejectDisablingTerminalCondition:             true,
			RejectInvalidCompletedIndexes:                isCompletedIndexesChanged,
			RejectInvalidFailedIndexes:                   isFailedIndexesChanged,
			RejectCompletedIndexesForNonIndexedJob:       isCompletedIndexesChanged,
			RejectFailedIndexesForNoBackoffLimitPerIndex: isFailedIndexesChanged,
			RejectFailedIndexesOverlappingCompleted:      isFailedIndexesChanged || isCompletedIndexesChanged,
			RejectFailedJobWithoutFailureTarget:          isJobFailedChanged || isFailedIndexesChanged,
			RejectCompleteJobWithoutSuccessCriteriaMet:   isJobCompleteChanged || isJobSuccessCriteriaMetChanged,
			RejectFinishedJobWithActivePods:              isJobFinishedChanged || isActiveChanged,
			RejectFinishedJobWithoutStartTime:            (isJobFinishedChanged || isStartTimeChanged) && !isSuspendedWithZeroCompletions,
			RejectFinishedJobWithUncountedTerminatedPods: isJobFinishedChanged || isUncountedTerminatedPodsChanged,
			RejectStartTimeUpdateForUnsuspendedJob:       isStartTimeChanged && !isJobResuming,
			RejectCompletionTimeBeforeStartTime:          isStartTimeChanged || isCompletionTimeChanged,
			RejectMutatingCompletionTime:                 true,
			RejectNotCompleteJobWithCompletionTime:       isJobCompleteChanged || isCompletionTimeChanged,
			RejectCompleteJobWithoutCompletionTime:       isJobCompleteChanged || isCompletionTimeChanged,
			RejectCompleteJobWithFailedCondition:         isJobCompleteChanged || isJobFailedChanged,
			RejectCompleteJobWithFailureTargetCondition:  isJobCompleteChanged || isJobFailureTargetChanged,
			RejectMoreReadyThanActivePods:                isReadyChanged || isActiveChanged,
			RejectFinishedJobWithTerminatingPods:         isJobFinishedChanged || isTerminatingChanged,
		}
	}

	return batchvalidation.JobStatusValidationOptions{}
}

// WarningsOnUpdate 返回更新 Job 时应告知客户端的非阻断性警告。
func (jobStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// JobSelectableFields 返回 Job 可用于 FieldSelector 匹配的字段集合。
// JobToSelectableFields 暴露 metadata 字段、status.successful 和 spec.* 等 Job 专属 FieldSelector。
func JobToSelectableFields(job *batch.Job) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&job.ObjectMeta, true)
	specificFieldsSet := fields.Set{
		"status.successful": strconv.Itoa(int(job.Status.Succeeded)),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}

// GetAttrs 提取 Job 的标签和可选择字段，供列表、监视等过滤操作使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	job, ok := obj.(*batch.Job)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a job.")
	}
	return labels.Set(job.ObjectMeta.Labels), JobToSelectableFields(job), nil
}

// MatchJob 是通用 etcd 存储使用的事件过滤函数。
// 它根据客户端指定的标签和字段选择器，判断是否向该客户端转发 Job 监视事件。
func MatchJob(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}
