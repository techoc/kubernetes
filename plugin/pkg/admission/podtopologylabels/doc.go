/*
Copyright 2024 The Kubernetes Authors.

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

// Package podtopologylabels 是一个变更 `pod/binding` 请求的插件：
// 它从待准入的 Binding 中所指定的 Node 对象上复制
// `topology.kubernetes.io/{zone,region}` 标签到 Binding，
// 以便 Pod 被调度时这些标签能持久化到 Pod 对象上。
// 对设置了 `spec.nodeName` 的普通 `pods` 资源的请求，
// 该插件同样会按上述方式复制标签。
// 若绑定目标不是 Node 对象，则不执行任何操作。
// 若引用的 Node 对象不存在，同样不执行任何操作。
package podtopologylabels
