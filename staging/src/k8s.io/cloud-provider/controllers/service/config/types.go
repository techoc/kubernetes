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

// ServiceControllerConfiguration 包含描述 ServiceController 的配置项。
// ServiceControllerConfiguration 控制负载均衡 Service 同步的并发度。
type ServiceControllerConfiguration struct {
	// concurrentServiceSyncs 是允许并发同步的 Service 数，越大响应越快，但 CPU 与网络开销也越高。
	// 并发度越高，Service 变更生效越快，但会给云厂商 API 与网络带来更大压力。
	ConcurrentServiceSyncs int32
}
