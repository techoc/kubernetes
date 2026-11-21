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

// EndpointControllerConfiguration 描述端点控制器（EndpointController）的配置参数。
type EndpointControllerConfiguration struct {
	// ConcurrentEndpointSyncs 是允许并发执行的端点同步操作数。
	// 增大该值可以更快更新 Endpoint，但会增加 CPU 与网络负载。
	ConcurrentEndpointSyncs int32

	// EndpointUpdatesBatchPeriod 用于批量合并端点更新。
	// 由 Pod 变化触发的 Endpoint 更新最多延迟该时长；若同一 Endpoint 中的其他 Pod
	// 在此期间也发生变化，则合并为一次 Endpoint 更新，避免频繁写 API。
	// 默认值为 0，表示每次 Pod 更新都会立即触发一次 Endpoint 更新。
	EndpointUpdatesBatchPeriod metav1.Duration
}
