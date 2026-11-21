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

package util

import (
	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
)

// FinishedCondition 判断 Job 是否已结束，并返回表明结束原因的 Condition 类型（Complete 或 Failed）。
// 未结束时返回 false 和空类型。
// 只有 Status 为 True 的 Condition 才算数，因为 Condition 为 False 通常表示该终止状态曾被移除。
func FinishedCondition(j *batch.Job) (bool, batch.JobConditionType) {
	for _, c := range j.Status.Conditions {
		// 同时接受成功和失败两种终态，控制器据此停止对 Job 的进一步调谐。
		if (c.Type == batch.JobComplete || c.Type == batch.JobFailed) && c.Status == v1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// IsJobFinished 检查给定 Job 是否已执行结束。
// 该方法不区分成功（Complete）和失败（Failed）两种终态。
func IsJobFinished(j *batch.Job) bool {
	isFinished, _ := FinishedCondition(j)
	return isFinished
}

// IsJobSucceeded 判断 Job 是否成功完成。
// 与 IsJobFinished 不同，失败结束的 Job 在这里返回 false。
func IsJobSucceeded(j *batch.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batch.JobComplete && c.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
