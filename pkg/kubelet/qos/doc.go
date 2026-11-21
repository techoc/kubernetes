/*
Copyright 2015 The Kubernetes Authors.

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

// Package qos 包含服务质量相关的辅助函数。
// 对每种资源（内存、CPU），kubelet 支持三类容器：
// Guaranteed 容器优先级最高，能获得其所需的全部资源；
// Burstable 容器保证获得 request，并可在资源富余时突发使用更多资源；
// Best-Effort 容器未指定 request，只能使用其他 Pod 未占用的资源。
package qos
