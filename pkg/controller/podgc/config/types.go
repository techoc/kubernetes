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

// PodGCControllerConfiguration 包含描述 PodGCController 的配置项。
type PodGCControllerConfiguration struct {
	// TerminatedPodGCThreshold 是允许存在的已终止 Pod 数量上限，
	// 超过该阈值后，已终止 Pod 垃圾回收器开始清理。
	// 小于或等于 0 表示禁用该垃圾回收器。
	TerminatedPodGCThreshold int32
}
