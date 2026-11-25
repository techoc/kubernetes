/*
Copyright 2020 The Kubernetes Authors.

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

// NodeControllerConfiguration contains elements describing NodeController.
type NodeControllerConfiguration struct {
	// ConcurrentNodeSyncs 是并发同步节点的 worker 数量。
	//
	// 每个 worker 会调用云厂商实例接口，需与云 API 配额匹配。
	ConcurrentNodeSyncs int32
	// ConcurrentNodeStatusUpdates 是并发更新节点状态的 worker 数量。
	// 未指定或为 0 时改用 ConcurrentNodeSyncs。
	//
	// 状态刷新比初始化更频繁，通常需要更高的并发；留 0 时复用
	// ConcurrentNodeSyncs，避免用户必须同时配置两个值。
	ConcurrentNodeStatusUpdates int32
}
