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

package v1

const (
	// LabelServiceName 用于标识 Kubernetes Service 的名称。
	LabelServiceName = "kubernetes.io/service-name"
	// LabelManagedBy 用于标识管理某个 EndpointSlice 的控制器或实体。
	// 该标签让同一集群内不同 EndpointSlice 可以由不同控制器或实体分别管理，
	// 强烈建议为所有 EndpointSlice 设置。
	LabelManagedBy = "endpointslice.kubernetes.io/managed-by"
	// LabelSkipMirror 可在 Endpoints 资源上设为 true，表示
	// EndpointSliceMirroring 控制器不要将该资源镜像为 EndpointSlice。
	LabelSkipMirror = "endpointslice.kubernetes.io/skip-mirror"
)
