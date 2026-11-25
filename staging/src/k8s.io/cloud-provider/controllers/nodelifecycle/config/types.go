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

// Package config 定义 CloudNodeLifecycleController 的内部配置类型；
// 外部 v1alpha1 版本位于 config/v1alpha1，二者通过生成的转换函数互通。
package config

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodeLifecycleControllerConfiguration 包含描述 CloudNodeLifecycleController 的各个要素。
// 该控制器负责监测节点对应的云实例是否仍存在或已关机，因此配置项围绕巡检周期与并发度。
type NodeLifecycleControllerConfiguration struct {
	// NodeMonitorPeriod 是 CloudNodeLifecycleController 中同步 NodeStatus 的周期。
	// 周期越短，已删除实例对应的 Node 越早被清理，但对云 API 的压力也越大。
	NodeMonitorPeriod metav1.Duration
	// ConcurrentNodeLifecycleSyncs 是 CloudNodeLifecycleController 中同步 NodeStatus 的 worker 数量。
	// 调大可提升大规模集群的巡检吞吐，需与云 API 限流能力匹配。
	ConcurrentNodeLifecycleSyncs int32
}
