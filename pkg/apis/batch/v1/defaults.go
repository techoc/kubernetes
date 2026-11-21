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

package v1

import (
	"math"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值设定函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Job 为 Job 中未显式指定的字段填充默认值。
func SetDefaults_Job(obj *batchv1.Job) {
	// 非并行 Job 允许 completions 和 parallelism 同时留空，
	// 此时二者的语义都是 1，因此一并补齐，避免下游出现 nil 分支。
	if obj.Spec.Completions == nil && obj.Spec.Parallelism == nil {
		obj.Spec.Completions = ptr.To[int32](1)
		obj.Spec.Parallelism = ptr.To[int32](1)
	}
	// 只设置 parallelism 时，completions 留空表示沿用 parallelism 的值，无需改动。
	if obj.Spec.Parallelism == nil {
		obj.Spec.Parallelism = ptr.To[int32](1)
	}
	// BackoffLimitPerIndex 启用后重试按索引独立计数，
	// 全局 backoffLimit 需放到足够大，否则会误伤整体执行。
	if obj.Spec.BackoffLimit == nil {
		if obj.Spec.BackoffLimitPerIndex != nil {
			obj.Spec.BackoffLimit = ptr.To[int32](math.MaxInt32)
		} else {
			obj.Spec.BackoffLimit = ptr.To[int32](6)
		}
	}
	// Job 本身未带标签时，向下继承 Pod 模板的标签以保持选择器语义一致。
	labels := obj.Spec.Template.Labels
	if labels != nil && len(obj.Labels) == 0 {
		obj.Labels = labels
	}
	// 未指定完成模式时默认使用非索引模式，兼容历史行为。
	if obj.Spec.CompletionMode == nil {
		mode := batchv1.NonIndexedCompletion
		obj.Spec.CompletionMode = &mode
	}
	if obj.Spec.Suspend == nil {
		obj.Spec.Suspend = ptr.To(false)
	}
	// Pod 替换策略依赖失败策略：配置了 PodFailurePolicy 时仅替换失败 Pod 即可，
	// 否则替换终止或失败的 Pod。
	if obj.Spec.PodReplacementPolicy == nil {
		if obj.Spec.PodFailurePolicy != nil {
			obj.Spec.PodReplacementPolicy = ptr.To(batchv1.Failed)
		} else {
			obj.Spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
		}
	}
	if obj.Spec.ManualSelector == nil {
		obj.Spec.ManualSelector = ptr.To(false)
	}
}

// SetDefaults_CronJob 为 CronJob 中未显式指定的并发策略、挂起状态及历史记录上限填充默认值。
func SetDefaults_CronJob(obj *batchv1.CronJob) {
	if obj.Spec.ConcurrencyPolicy == "" {
		obj.Spec.ConcurrencyPolicy = batchv1.AllowConcurrent
	}
	if obj.Spec.Suspend == nil {
		obj.Spec.Suspend = ptr.To(false)
	}
	// 默认保留 3 个成功任务，避免历史无限增长。
	if obj.Spec.SuccessfulJobsHistoryLimit == nil {
		obj.Spec.SuccessfulJobsHistoryLimit = ptr.To[int32](3)
	}
	// 失败记录默认只保留 1 个，便于排查问题同时节省资源。
	if obj.Spec.FailedJobsHistoryLimit == nil {
		obj.Spec.FailedJobsHistoryLimit = ptr.To[int32](1)
	}
}

// SetDefaults_PodFailurePolicyOnPodConditionsPattern 为 Pod 失败条件模式的 status 填充默认值。
func SetDefaults_PodFailurePolicyOnPodConditionsPattern(obj *batchv1.PodFailurePolicyOnPodConditionsPattern) {
	// 条件 status 默认是 True，匹配最常见的健康/就绪类条件语义。
	if obj.Status == "" {
		obj.Status = corev1.ConditionTrue
	}
}
