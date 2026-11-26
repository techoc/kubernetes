/*
Copyright The Kubernetes Authors.

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

// 本文件定义 DRA 资源健康服务的版本标识常量。
const (
	// 提供 v1 DRAResourceHealth 服务的 DRA 插件注册时，
	// 必须把 DRAResourceHealthService 列入 "supported versions" 数组。
	// 提供 v1 DRAResourceHealth 服务时必须注册它。
	//
	// 其值必须严格等于 [DRAResourceHealth_ServiceDesc.ServiceName]，
	// 由 TestServiceNameConstants 保证。
	//
	// 该字符串必须与 gRPC 服务描述中的 ServiceName 完全一致，否则 kubelet 无法在注册阶段识别插件提供的健康服务；
	// 由 TestServiceNameConstants 守住这条约束。
	DRAResourceHealthService = "v1.DRAResourceHealth"
)
