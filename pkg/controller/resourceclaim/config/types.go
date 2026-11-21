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

package config

// ResourceClaimControllerConfiguration 包含 resource claim 控制器的配置项。
type ResourceClaimControllerConfiguration struct {
	// ConcurrentSyncs 是可并发执行的操作数（删除 Pod、更新 ResourceClaim 状态等）。
	// 数值越大处理越快，但 CPU（和网络）负载也越高。默认值为 50。
	ConcurrentSyncs int32
}
