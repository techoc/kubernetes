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

// StatefulSetControllerConfiguration 包含描述 StatefulSetController（StatefulSet 控制器）的各项配置。
type StatefulSetControllerConfiguration struct {
	// concurrentStatefulSetSyncs 表示允许并行同步的 StatefulSet 对象数量。
	// 该值越大，StatefulSet 的响应越及时，但 CPU（及网络）负载也越高。
	ConcurrentStatefulSetSyncs int32
}
