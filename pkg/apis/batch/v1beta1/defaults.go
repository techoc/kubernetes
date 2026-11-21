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

package v1beta1

import (
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 Scheme 注册 v1beta1 版本的默认值处理函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_CronJob 为 CronJob 设置默认值。
// 这些默认值保证旧版本客户端省略字段时，语义仍与集群期望的行为一致：
// 允许并发执行、不挂起，并保留少量历史任务记录。
func SetDefaults_CronJob(obj *batchv1beta1.CronJob) {
	// 并发策略默认为 AllowConcurrent，避免串行执行降低定时任务吞吐量。
	if obj.Spec.ConcurrencyPolicy == "" {
		obj.Spec.ConcurrencyPolicy = batchv1beta1.AllowConcurrent
	}
	// Suspend 是指针，nil 表示未指定；默认不挂起（false）。
	if obj.Spec.Suspend == nil {
		obj.Spec.Suspend = new(bool)
	}
	// 成功任务的历史记录默认保留 3 个，避免无限增长。
	if obj.Spec.SuccessfulJobsHistoryLimit == nil {
		obj.Spec.SuccessfulJobsHistoryLimit = new(int32)
		*obj.Spec.SuccessfulJobsHistoryLimit = 3
	}
	// 失败任务的历史记录默认只保留 1 个，减少噪声并节省资源。
	if obj.Spec.FailedJobsHistoryLimit == nil {
		obj.Spec.FailedJobsHistoryLimit = new(int32)
		*obj.Spec.FailedJobsHistoryLimit = 1
	}
}
