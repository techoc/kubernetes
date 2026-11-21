/*
Copyright 2025 The Kubernetes Authors.

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

package qos

// ResourceIsolationLevel 定义资源的隔离级别。
type ResourceIsolationLevel string

const (
	// ResourceIsolationHost 表示资源与主机上的其他容器共享。
	ResourceIsolationHost ResourceIsolationLevel = "host"
	// ResourceIsolationPod 表示资源与其他 Pod 隔离，但在 Pod 内共享。
	ResourceIsolationPod ResourceIsolationLevel = "pod"
	// ResourceIsolationContainer 表示资源由容器独占。
	ResourceIsolationContainer ResourceIsolationLevel = "container"
)
