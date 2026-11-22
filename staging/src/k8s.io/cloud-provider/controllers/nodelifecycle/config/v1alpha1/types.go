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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodeLifecycleControllerConfiguration contains elements describing CloudNodeLifecycleController.
// v1alpha1 外部表示；字段集合与内部 config 包严格一致，新增字段需同步两侧与转换函数。
type NodeLifecycleControllerConfiguration struct {
	// NodeMonitorPeriod is the period for syncing NodeStatus in CloudNodeLifecycleController.
	// 巡检周期；为空时由上层默认化逻辑填充推荐值。
	NodeMonitorPeriod metav1.Duration
	// ConcurrentNodeLifecycleSyncs is the number of workers for syncing NodeStatus in CloudNodeLifecycleController.
	// 并发 worker 数；0 表示未设置，由 Recommended 默认化函数补为 1。
	ConcurrentNodeLifecycleSyncs int32
}
