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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EndpointSliceMirroringControllerConfiguration 描述 EndpointSliceMirroringController 的配置项。
type EndpointSliceMirroringControllerConfiguration struct {
	// MirroringConcurrentServiceEndpointSyncs 是并发执行的服务端点同步操作数。
	// 数值越大，EndpointSlice 更新越快，但 CPU 与网络负载也越高。
	MirroringConcurrentServiceEndpointSyncs int32

	// MirroringMaxEndpointsPerSubset 是每个 EndpointSubset 镜像到单个 EndpointSlice 时的端点数量上限。
	MirroringMaxEndpointsPerSubset int32

	// MirroringEndpointUpdatesBatchPeriod 用于批量合并 EndpointSlice 更新：由端点变更触发的
	// 更新最多延迟该时长。若同一 Endpoints 资源内的其他地址在此期间发生变化，
	// 它们会被合并为一次 EndpointSlice 更新。默认值为 0，表示每次 Endpoints 更新都
	// 立即触发一次 EndpointSlice 更新。
	MirroringEndpointUpdatesBatchPeriod metav1.Duration
}
