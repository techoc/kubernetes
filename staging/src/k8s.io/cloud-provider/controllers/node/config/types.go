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

// Package config 定义 NodeController 的内部组件配置类型。
package config

// NodeControllerConfiguration contains elements describing NodeController.
// NodeControllerConfiguration 控制云节点控制器的并发度：同步节点与更新节点状态
// 是两类不同的 API 负载，因此拆成两个独立参数。
type NodeControllerConfiguration struct {
	// ConcurrentNodeSyncs is the number of workers
	// concurrently synchronizing nodes
	// 并发同步节点的 worker 数；值越大初始化越快，但对云 API 与 API Server 压力越大。
	ConcurrentNodeSyncs int32
	// ConcurrentNodeStatusUpdates is the number of workers
	// concurrently updating node statuses.
	// If unspecified or 0, ConcurrentNodeSyncs is used instead
	// 未设置或为 0 时回退到 ConcurrentNodeSyncs，以保持旧配置的兼容行为。
	ConcurrentNodeStatusUpdates int32
}
