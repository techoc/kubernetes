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

// ServiceControllerConfiguration contains elements describing ServiceController.
type ServiceControllerConfiguration struct {
	// concurrentServiceSyncs is the number of services that are
	// allowed to sync concurrently. Larger number = more responsive service
	// management, but more CPU (and network) load.
	//
	// 中文说明：每个 worker 会调用云厂商 LoadBalancer 接口，数值应受云 API
	// 限流配额约束；过大会把限流错误放大成大量重试。
	ConcurrentServiceSyncs int32
}
