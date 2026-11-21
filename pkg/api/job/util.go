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

package job

import (
	batchv1 "k8s.io/api/batch/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	workloadbuilder "k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// DropDisabledFields 在特性开关关闭、且旧对象中没有在用这些字段时，
// 从 JobSpec 中移除受特性门控的字段。
// 创建时 oldJobSpec 为 nil。这个函数由 Job 和 CronJob 的 registry strategy 共享，
// 因为两者都内嵌了 JobSpec。
func DropDisabledFields(jobSpec, oldJobSpec *batch.JobSpec) {
	// 特性开启，或旧对象已经在用 scheduling 字段时必须保留，
	// 否则更新会悄悄丢失用户已有的配置。
	if utilfeature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) || schedulingInUse(oldJobSpec) {
		return
	}
	jobSpec.Scheduling = nil
}

// schedulingInUse 判断旧 spec 中是否已经设置了 scheduling 字段。
// 仅在 oldJobSpec 非 nil（即更新场景）且字段非空时返回 true。
func schedulingInUse(jobSpec *batch.JobSpec) bool {
	return jobSpec != nil && jobSpec.Scheduling != nil
}

// WorkloadInputForJobInternal 把 internal 版本的 Job scheduling 配置转换为
// workloadbuilder 的输入结构，供校验和 workload 构建共用。
func WorkloadInputForJobInternal(s *batch.JobSchedulingConfiguration) workloadbuilder.WorkloadInput {
	// nil 配置等价于空配置，后续由 DefaultConfig 提供默认值。
	if s == nil {
		s = &batch.JobSchedulingConfiguration{}
	}
	return WorkloadInput(s.SchedulingPolicy, s.SchedulingConstraints, s.DisruptionMode, s.ResourceClaims)
}

// WorkloadInputForJobV1 是 WorkloadInputForJobInternal 的 v1 版本，
// 把 v1 外部类型的 Job scheduling 配置映射到同一个 workloadbuilder 输入结构。
func WorkloadInputForJobV1(s *batchv1.JobSchedulingConfiguration) workloadbuilder.WorkloadInput {
	if s == nil {
		s = &batchv1.JobSchedulingConfiguration{}
	}
	return WorkloadInput(s.SchedulingPolicy, s.SchedulingConstraints, s.DisruptionMode, s.ResourceClaims)
}

// WorkloadInput 把 Job 的 spec.scheduling 各个组成部分映射到
// workloadbuilder.WorkloadInput。每个组成部分都附带它在 spec.scheduling 下的
// 子路径，这样校验错误才能报告到正确的字段上。
// Job controller 和 internal batch API 都直接内嵌了 schedulingv1alpha3 的这些
// 组成部分，因此这一个映射函数可以同时服务两边。
// 传入 nil 的组成部分会回落到对应条目的 DefaultConfig。
func WorkloadInput(
	policy *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy,
	constraints *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints,
	disruptionMode *schedulingv1alpha3.WorkloadPodGroupDisruptionMode,
	resourceClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim,
) workloadbuilder.WorkloadInput {
	return workloadbuilder.WorkloadInput{
		Policy: workloadbuilder.PolicyInput{
			PodGroupData: policy,
			// 路径是相对于 spec.scheduling 的，与 WorkloadItemForJob 的 path 参数拼接。
			PathElements: []string{"schedulingPolicy"},
		},
		Constraints: workloadbuilder.ConstraintsInput{
			PodGroupData: constraints,
			PathElements: []string{"schedulingConstraints"},
		},
		DisruptionMode: workloadbuilder.DisruptionModeInput{
			PodGroupData: disruptionMode,
			PathElements: []string{"disruptionMode"},
		},
		ResourceClaims: workloadbuilder.ResourceClaimsInput{
			PodGroupData: resourceClaims,
			PathElements: []string{"resourceClaims"},
		},
	}
}

// WorkloadItemForJob 为 Job 组装单节点的逻辑 workload 树。
// Job registry 的校验逻辑与 Job controller 共用此函数，避免两边行为漂移。
// path 参数是 Job 的 spec.scheduling 字段路径，仅用于校验错误定位，
// 因此 controller 调用时可以传 nil。
func WorkloadItemForJob(name, priorityClassName string, parallelism *int32,
	input workloadbuilder.WorkloadInput, path *field.Path) *workloadbuilder.WorkloadItem {
	return &workloadbuilder.WorkloadItem{
		Name: name,
		Path: path,
		// 默认配置：空的基本调度策略 + Job 的 priorityClassName。
		// 用户未显式设置的部分会以此为基础合并。
		DefaultConfig: &workloadbuilder.SchedulingConfig{
			Policy: &workloadbuilder.SchedulingPolicy{
				Basic: &workloadbuilder.BasicSchedulingPolicy{},
			},
			PriorityClassName: priorityClassName,
		},
		Input:     input,
		Callbacks: []workloadbuilder.SchedulingConfigFunc{defaultMinCountForJob(parallelism)},
	}
}

// defaultMinCountForJob 返回一个回调：把未设置的 gang MinCount 默认成 Job 的 parallelism，
// 并下限钳制为 1。暂停的 Job parallelism 可能为 0，但 MinCount 必须为正数。
// 该回调只修改解析出来的配置，绝不把派生值写回 Job 的 spec。
func defaultMinCountForJob(parallelism *int32) workloadbuilder.SchedulingConfigFunc {
	return func(cfg *workloadbuilder.SchedulingConfig) {
		// 只有实际启用了 gang 调度时才需要这个默认值。
		if cfg == nil || cfg.Policy == nil || cfg.Policy.Gang == nil {
			return
		}
		if cfg.Policy.Gang.MinCount == nil {
			cfg.Policy.Gang.MinCount = new(max(int32(1), ptr.Deref(parallelism, 1)))
		}
	}
}
