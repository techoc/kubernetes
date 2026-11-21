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

// DisruptionControllerConfiguration 描述 DisruptionController（中断控制器）的配置参数。
type DisruptionControllerConfiguration struct {
	// concurrentDisruptionSyncs 是允许并发同步的 PodDisruptionBudget（PDB）对象数量。
	// 值越大，PDB 更新越及时，但 CPU 与网络负载也越高。
	ConcurrentDisruptionSyncs int32
}
