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

// GroupResource 通过一个 API 组和资源名唯一描述一类资源（GroupResource）。
type GroupResource struct {
	// Group 是 GroupResource 中的 API 组部分，核心资源组为空字符串。
	Group string
	// Resource 是 GroupResource 中的资源名部分（复数形式）。
	Resource string
}

// GarbageCollectorControllerConfiguration 描述垃圾回收控制器（GarbageCollectorController）的配置参数。
type GarbageCollectorControllerConfiguration struct {
	// EnableGarbageCollector 用于启用通用垃圾回收器。
	// 必须与 kube-apiserver 的对应开关保持一致，否则控制器与 apiserver 行为不一致。
	// 注意：通用垃圾回收器当时仍是 alpha 特性。
	EnableGarbageCollector bool
	// ConcurrentGCSyncs 是允许并发同步的垃圾回收 worker 数量。
	ConcurrentGCSyncs int32
	// GCIgnoredResources 是垃圾回收应忽略的 GroupResource 列表，
	// 用于跳过不希望被级联删除的资源类型。
	GCIgnoredResources []GroupResource
}
