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

// ServiceControllerConfiguration contains elements describing ServiceController.
// ServiceControllerConfiguration 控制负载均衡 Service 同步的并发度。
type ServiceControllerConfiguration struct {
	// concurrentServiceSyncs is the number of services that are
	// allowed to sync concurrently. Larger number = more responsive service
	// management, but more CPU (and network) load.
	// 并发度越高，Service 变更生效越快，但会给云厂商 API 与网络带来更大压力。
	ConcurrentServiceSyncs int32
}
