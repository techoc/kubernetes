/*
Copyright 2019 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EndpointSliceControllerConfiguration 包含描述 EndpointSliceController 行为的配置项。
type EndpointSliceControllerConfiguration struct {
	// ConcurrentServiceEndpointSyncs 是并发执行服务端点同步操作的数量。
	// 数值越大，EndpointSlice 更新越快，但 CPU（及网络）负载也越高。
	ConcurrentServiceEndpointSyncs int32

	// MaxEndpointsPerSlice 是单个 EndpointSlice 中允许加入的最大端点数量。
	// 每个切片容纳更多端点会减少切片数量、使单个资源变大，
	// 但也会产生更大的资源对象。
	MaxEndpointsPerSlice int32

	// EndpointUpdatesBatchPeriod 用于对端点更新做批处理。
	// 由 Pod 变化触发的端点更新最多延迟 EndpointUpdatesBatchPeriod；
	// 若在该时间段内同一端点涉及的其他 Pod 也发生变化，会被合并为一次端点更新。
	// 默认值 0 表示每次 Pod 更新都立即触发一次端点更新，不做合并。
	EndpointUpdatesBatchPeriod metav1.Duration
}
