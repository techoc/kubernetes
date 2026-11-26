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

package v1alpha1

const (
	// 提供 v1alpha1 DRAResourceHealth 服务的插件注册时，
	// 数组中的服务名，供 DRA 插件注册时使用。
	// 必须把该服务名列入 "supported versions" 数组。
	//
	// 其值必须严格等于 [DRAResourceHealth_ServiceDesc.ServiceName]，
	// 在该常量出现之前编译的插件曾用这个字符串
	// 注册服务名，由 TestServiceNameConstants 锁定这一兼容约束。
	// 与 v1 包一致。
	//
	// v1alpha1 已由 v1 取代，新驱动应实现 v1；
	// kubelet 辅助包通过转换包装也提供 v1alpha1，
	// 使仅支持 v1alpha1 的 kubelet 仍能消费健康信息。
	// 更新。转换代码见 v1 包。
	//
	// 字符串值不能改：早期插件把此值硬编码进注册信息，改了就无法向后兼容。
	DRAResourceHealthService = "v1alpha1.DRAResourceHealth"
)
