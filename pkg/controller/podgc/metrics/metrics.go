/*
Copyright 2022 The Kubernetes Authors.

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
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	podGCController = "pod_gc_collector"
)

var (
	DeletingPodsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      podGCController,
			Name:           "force_delete_pods_total",
			Help:           "Number of pods that are being forcefully deleted since the Pod GC Controller started.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"namespace", "reason"},
	)
	DeletingPodsErrorTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      podGCController,
			Name:           "force_delete_pod_errors_total",
			Help:           "Number of errors encountered when forcefully deleting the pods since the Pod GC Controller started.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"namespace", "reason"},
	)
)

const (
	// 以下是上述指标中 "reason" 标签的可能取值。

	// PodGCReasonTerminated 表示 Pod 已终止。
	PodGCReasonTerminated = "terminated"
	// PodGCReasonTerminatingOutOfService 表示 Pod 正在终止，且其所在节点处于 NotReady 状态，
	// 并带有 `node.kubernetes.io/out-of-service` 污点。
	PodGCReasonTerminatingOutOfService = "out-of-service"
	// PodGCReasonOrphaned 表示 Pod 成为孤儿，即其所在节点已被删除。
	PodGCReasonOrphaned = "orphaned"
	// PodGCReasonTerminatingUnscheduled 表示 Pod 正在终止但尚未被调度。
	PodGCReasonTerminatingUnscheduled = "unscheduled"
)

// registerMetrics 保证指标只注册一次。
var registerMetrics sync.Once

// RegisterMetrics 注册需要监控的指标。
func RegisterMetrics() {
	// 使用 sync.Once 避免重复注册时 legacyregistry 因指标冲突而 panic。
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(DeletingPodsTotal)
		legacyregistry.MustRegister(DeletingPodsErrorTotal)
	})
}
