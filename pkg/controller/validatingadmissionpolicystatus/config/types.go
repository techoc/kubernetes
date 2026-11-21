/*
Copyright 2023 The Kubernetes Authors.

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

// ValidatingAdmissionPolicyStatusControllerConfiguration 包含描述 ValidatingAdmissionPolicyStatusController 的各项配置。
type ValidatingAdmissionPolicyStatusControllerConfiguration struct {
	// ConcurrentPolicySyncs 是允许并发同步的策略对象数量。
	// 数值越大，类型检查越快，但 CPU（及网络）负载也越高。
	// 默认值为 5。
	ConcurrentPolicySyncs int32
}
