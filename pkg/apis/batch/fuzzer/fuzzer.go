/*
Copyright 2017 The Kubernetes Authors.

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

package fuzzer

import (
	"math"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/batch"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/randfill"
)

// Funcs 返回 batch API 组的 fuzzer 函数集合。
// 随机填充无法感知字段间的约束和默认值逻辑，这里对 Job、CronJob 等类型
// 做针对性修正，避免序列化往返测试因非法组合而误报。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// Job 的标签有从小节模板继承的默认值，随机生成的标签不能与之冲突。
		func(j *batch.Job, c randfill.Continue) {
			c.FillNoCustom(j) // 只填充自身字段，避免递归调用本函数

			// 与 defaulter 一致：标签为空时从 pod 模板继承，确保往返结果稳定
			if len(j.Labels) == 0 {
				j.Labels = j.Spec.Template.Labels
			}
		},
		// JobSpec 的指针字段和枚举字段有结构与取值约束，
		// 这里显式生成，避免随机填充产生越界或非法组合。
		func(j *batch.JobSpec, c randfill.Continue) {
			c.FillNoCustom(j) // 只填充自身字段，避免递归调用本函数
			completions := c.Int31()
			parallelism := c.Int31()
			backoffLimit := c.Int31()
			j.Completions = &completions
			j.Parallelism = &parallelism
			j.BackoffLimit = &backoffLimit
			j.ManualSelector = ptr.To(c.Bool())
			mode := batch.NonIndexedCompletion
			if c.Bool() {
				// Indexed 模式下每个索引独立计数，只有这时 backoffLimitPerIndex
				// 和 maxFailedIndexes 才有意义。
				mode = batch.IndexedCompletion
				j.BackoffLimitPerIndex = ptr.To[int32](c.Int31())
				j.MaxFailedIndexes = ptr.To[int32](c.Int31())
			}
			if c.Bool() {
				// 随机生成极大值，覆盖接近 int32 上限时的序列化行为。
				j.BackoffLimit = ptr.To[int32](math.MaxInt32)
			}
			j.CompletionMode = &mode
			// 这里 fuzz 的是内部 JobSpec 而非 v1 类型，不必保留 nil 取值。
			j.Suspend = ptr.To(c.Bool())
			// Pod 替换策略是必填字段，随机在两种合法枚举值中选择。
			podReplacementPolicy := batch.TerminatingOrFailed
			if c.Bool() {
				podReplacementPolicy = batch.Failed
			}
			j.PodReplacementPolicy = &podReplacementPolicy
			if c.Bool() {
				// ManagedBy 是可选字段，按需填充以覆盖有无该字段的场景。
				c.Fill(j.ManagedBy)
			}
			// spec.scheduling has structural invariants the random filler can't know
			// about: a policy is a union (one of basic or gang), and a gang
			// minCount must be a positive integer.
			if j.Scheduling != nil && j.Scheduling.SchedulingPolicy != nil {
				if c.Bool() {
					// 调度策略是联合体：basic 与 gang 二选一。
					j.Scheduling.SchedulingPolicy = &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{},
					}
				} else {
					// gang 的 minCount 必须是正整数，随机值需要至少为 1。
					minCount := max(c.Int31(), 1)
					j.Scheduling.SchedulingPolicy = &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: &minCount},
					}
				}
			}
		},
		// CronJobSpec 的数值字段有范围约束，指针字段需显式填充，
		// 以避免随机值越界或留下 nil 导致与版本化类型不一致。
		func(sj *batch.CronJobSpec, c randfill.Continue) {
			c.FillNoCustom(sj) // 只填充自身字段，避免递归调用本函数
			suspend := c.Bool()
			sj.Suspend = &suspend
			sds := int64(c.Uint64())
			sj.StartingDeadlineSeconds = &sds
			sj.Schedule = c.String(0)
			successfulJobsHistoryLimit := c.Int31()
			sj.SuccessfulJobsHistoryLimit = &successfulJobsHistoryLimit
			failedJobsHistoryLimit := c.Int31()
			sj.FailedJobsHistoryLimit = &failedJobsHistoryLimit
		},
		// 并发策略是封闭枚举，只能从三个合法值中随机选择。
		func(cp *batch.ConcurrencyPolicy, c randfill.Continue) {
			policies := []batch.ConcurrencyPolicy{batch.AllowConcurrent, batch.ForbidConcurrent, batch.ReplaceConcurrent}
			*cp = policies[c.Rand.Intn(len(policies))]
		},
		// 条件 status 有默认值，随机填充留下的空串需补齐，否则往返测试会不一致。
		func(p *batch.PodFailurePolicyOnPodConditionsPattern, c randfill.Continue) {
			c.FillNoCustom(p) // 只填充自身字段，避免递归调用本函数
			// 与 defaulter 一致：空 status 默认为 True。
			if p.Status == "" {
				p.Status = api.ConditionTrue
			}
		},
	}
}
