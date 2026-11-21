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

import (
	v1 "k8s.io/api/core/v1"
)

// IsContainerEquivalentQOSGuaranteed 从独占资源管理器的视角判断容器是否可视为 Guaranteed。
// 要求 CPU 和内存的 requests 与 limits 均已设置且相等。
func IsContainerEquivalentQOSGuaranteed(container *v1.Container) bool {
	// 检查容器级 CPU requests 与 limits 是否相等。
	cpuRequest := container.Resources.Requests[v1.ResourceCPU]
	cpuLimit := container.Resources.Limits[v1.ResourceCPU]
	if cpuRequest.IsZero() || !cpuRequest.Equal(cpuLimit) {
		return false
	}

	// 检查容器级内存 requests 与 limits 是否相等。
	memRequest := container.Resources.Requests[v1.ResourceMemory]
	memLimit := container.Resources.Limits[v1.ResourceMemory]
	if memRequest.IsZero() || !memRequest.Equal(memLimit) {
		return false
	}

	return true
}
