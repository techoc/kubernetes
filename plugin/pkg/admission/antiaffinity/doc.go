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

// Package antiaffinity 提供 LimitPodHardAntiAffinityTopology 准入控制器。
// 它拒绝所有指定了 TopologyKey 非 v1.LabelHostname 的“硬”
// (RequiredDuringScheduling) 反亲和性的 Pod。由于反亲和性是对称的，
// 若没有该控制器，用户可能恶意或无意地指定自己的 Pod（一旦调度完成）
// 阻止其他 Pod 调度到同一 zone 或更大的拓扑域中，等同于对集群发起 DoS。
// 将来会通过配额和优先级更完整地解决该问题，但目前该控制器提供一种简单保护：
// 假定硬 Pod 反亲和性的唯一合法用途就是排除同一节点上的其他 Pod。
package antiaffinity
