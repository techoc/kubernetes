/*
Copyright 2016 The Kubernetes Authors.

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

package util

// 仅用于计算优先级时，未显式声明这些资源的容器会被视为请求了下方指定的数量。
// 这样既可避免零请求 Pod 全部涌向已用请求量最小的节点，也可避免普通 Pod 误以为
// 零请求 Pod 完全不消耗资源。数值参照集群插件 Pod 的资源配额选取（#10653），
// 但基本是拍脑袋定的。如 #11713 所述，这里用 request 而非 limit 来描述资源需求。
const (
	// DefaultMilliCPURequest 定义默认的 milli CPU 请求量。
	DefaultMilliCPURequest int64 = 100 // 0.1 核
	// DefaultMemoryRequest 定义默认的内存请求量。
	DefaultMemoryRequest int64 = 200 * 1024 * 1024 // 200 MB
)
