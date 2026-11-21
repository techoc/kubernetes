/*
Copyright 2021 The Kubernetes Authors.

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

package metrics

import (
	"k8s.io/component-base/metrics"
)

// ReplicaSetControllerSubsystem 是 ReplicaSet 控制器所有指标的子系统名前缀。
const ReplicaSetControllerSubsystem = "replicaset_controller"

// 下面两个指标分别衡量缩容时删除 Pod 的排序质量，
// 以及因 watch 缓存陈旧而跳过的同步次数。
var (
	// SortingDeletionAgeRatio 记录被删除 Pod 的平均年龄与当前最年轻 Pod 年龄的比值，
	// 用于评估 ReplicaSet 缩容时按年龄排序删除的效果（只统计 Ready 的 Pod）。
	SortingDeletionAgeRatio = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: ReplicaSetControllerSubsystem,
			Name:      "sorting_deletion_age_ratio",
			Help: "The ratio of chosen deleted pod's ages to the current youngest pod's age (at the time). Should be <2. " +
				"The intent of this metric is to measure the rough efficacy of the sorting (and deletion) of pods when " +
				"a replicaset scales down. This only considers Ready pods when calculating and reporting.",
			Buckets:        metrics.ExponentialBuckets(0.25, 2, 6),
			StabilityLevel: metrics.ALPHA,
		},
	)

	// ReplicaSetRequeueSkips 统计因 watch 缓存陈旧而跳过的 ReplicaSet 同步次数，
	// 标签为资源所属的 group 与 resource。
	ReplicaSetRequeueSkips = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      ReplicaSetControllerSubsystem,
			Name:           "stale_sync_skips_total",
			Help:           "Total number of ReplicaSet syncs skipped due to a stale watch cache.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
)

// Register 通过传入的注册函数逐个注册 ReplicaSet 控制器指标；
// 任一指标注册失败都立即返回错误，便于调用方定位。
func Register(registrationFunc func(metrics.Registerable) error) error {
	if err := registrationFunc(SortingDeletionAgeRatio); err != nil {
		return err
	}
	return registrationFunc(ReplicaSetRequeueSkips)
}
