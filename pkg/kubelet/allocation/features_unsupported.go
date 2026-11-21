//go:build !linux && !windows

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

package allocation

import v1 "k8s.io/api/core/v1"

// IsInPlacePodVerticalScalingAllowed 在不支持的平台上始终拒绝原地 Pod 垂直扩缩容。
func IsInPlacePodVerticalScalingAllowed(_ *v1.Pod) (allowed bool, msg, reason string) {
	return false, "In-place pod resize is not supported on this node", "unsupported_platform"
}

// IsInPlacePodLevelResourcesVerticalScalingAllowed 在不支持的平台上始终拒绝 Pod 级资源的原地扩缩容。
func IsInPlacePodLevelResourcesVerticalScalingAllowed(pod *v1.Pod) (allowed bool, msg, reason string) {
	return false, "In-place pod-level resources resize is not supported on this node", "unsupported_platform"
}
