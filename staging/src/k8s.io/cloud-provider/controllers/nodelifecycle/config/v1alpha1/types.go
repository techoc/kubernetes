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

// NodeLifecycleControllerConfiguration 包含描述 CloudNodeLifecycleController 的各个要素。
// v1alpha1 外部表示；字段集合与内部 config 包严格一致，新增字段需同步两侧与转换函数。
type NodeLifecycleControllerConfiguration struct {
	// NodeMonitorPeriod 是 CloudNodeLifecycleController 中同步 NodeStatus 的周期；
	// 为空时由上层默认化逻辑填充推荐值。
	NodeMonitorPeriod metav1.Duration
	// ConcurrentNodeLifecycleSyncs 是 CloudNodeLifecycleController 中同步 NodeStatus 的 worker 数量；
	// 0 表示未设置，由 Recommended 默认化函数补为 1。
	ConcurrentNodeLifecycleSyncs int32
}
