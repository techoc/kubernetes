/*
Copyright 2024 The Kubernetes Authors.

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
	"fmt"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// matchSuccessPolicy 按规则顺序匹配 Job 的成功策略，返回首个命中规则的说明与 true。
// succeededIndexes 为空或策略为空时直接不命中；规则解析失败时跳过该规则，
// 避免因格式错误阻断后续规则匹配。
func matchSuccessPolicy(logger klog.Logger, successPolicy *batch.SuccessPolicy, completions int32, succeededIndexes orderedIntervals) (string, bool) {
	if successPolicy == nil || len(succeededIndexes) == 0 {
		return "", false
	}

	rulesMatchedMsg := "Matched rules at index"
	for index, rule := range successPolicy.Rules {
		// 两种规则互斥：指定下标区间时按下标匹配，否则只按成功数量匹配。
		if rule.SucceededIndexes != nil {
			requiredIndexes := parseIndexesFromString(logger, *rule.SucceededIndexes, int(completions))
			// 格式非法等解析失败会得到空集合，跳过该规则以免误判成功。
			if len(requiredIndexes) == 0 {
				continue
			}
			if matchSucceededIndexesRule(requiredIndexes, succeededIndexes, rule.SucceededCount) {
				return fmt.Sprintf("%s %d", rulesMatchedMsg, index), true
			}
		} else if rule.SucceededCount != nil && succeededIndexes.total() >= int(*rule.SucceededCount) {
			// 成功数量达到阈值即命中，无需关心是哪些下标。
			return fmt.Sprintf("%s %d", rulesMatchedMsg, index), true
		}
	}
	return "", false
}

// hasSuccessCriteriaMetCondition 返回状态为 True 的 SuccessCriteriaMet 条件，
// 不存在或不为 True 时返回 nil，表示成功条件尚未满足。
func hasSuccessCriteriaMetCondition(job *batch.Job) *batch.JobCondition {
	successCriteriaMet := findConditionByType(job.Status.Conditions, batch.JobSuccessCriteriaMet)
	if successCriteriaMet != nil && successCriteriaMet.Status == v1.ConditionTrue {
		return successCriteriaMet
	}
	return nil
}

// isSuccessCriteriaMetCondition 判断条件是否就是已置为 True 的 SuccessCriteriaMet，
// 供事件过滤使用，避免把无关条件变化当作成功信号。
func isSuccessCriteriaMetCondition(cond *batch.JobCondition) bool {
	return cond != nil && cond.Type == batch.JobSuccessCriteriaMet && cond.Status == v1.ConditionTrue
}

// matchSucceededIndexesRule 用双指针遍历两个有序区间集合，统计规则区间与成功下标的重叠数。
// 全部规则下标都被覆盖，或重叠数达到 succeededCount 时视为命中。
func matchSucceededIndexesRule(ruleIndexes, succeededIndexes orderedIntervals, succeededCount *int32) bool {
	var contains, succeededPointer, rulePointer int
	for rulePointer < len(ruleIndexes) && succeededPointer < len(succeededIndexes) {
		// 两区间交集长度即为本次命中的下标数。
		if overlap := min(ruleIndexes[rulePointer].Last, succeededIndexes[succeededPointer].Last) -
			max(ruleIndexes[rulePointer].First, succeededIndexes[succeededPointer].First) + 1; overlap > 0 {
			contains += overlap
		}
		if succeededIndexes[succeededPointer].Last < ruleIndexes[rulePointer].Last {
			// 成功区间结束位置更靠前，推进成功指针以寻找下一个可能的重叠。
			succeededPointer++
		} else if succeededIndexes[succeededPointer].Last > ruleIndexes[rulePointer].Last {
			// 规则区间结束位置更靠前，推进规则指针。
			rulePointer++
		} else {
			// 两区间同终点，重叠已统计完毕，双方同时推进。
			succeededPointer++
			rulePointer++
		}
	}
	return contains == ruleIndexes.total() || (succeededCount != nil && contains >= int(*succeededCount))
}
