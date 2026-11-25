/*
Copyright 2022 The Kubernetes Authors.

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

// Package kubeletplugin（中文）提供用于运行动态资源分配（DRA）kubelet 插件的辅助函数。
// 使用该包的 DRA 驱动可以作为 DaemonSet 部署在合适的节点上。借助节点标签（例如通过 NFD，
// https://github.com/kubernetes-sigs/node-feature-discovery），可以让驱动只运行在实际具备所需硬件的节点上。
// DaemonSet 的 ServiceAccount 必须有足够 RBAC 权限：读取 ResourceClaim，并在驱动要发布每节点
// ResourceSlice 时创建和更新 ResourceSlice。把对 ResourceSlice 的访问限制到某个 Pod 所在节点是
// 良好的安全实践（但非强制），可通过 Validating Admission Policy（VAP）实现；参见 DRA 示例驱动的
// 部署清单了解细节：https://github.com/kubernetes-sigs/dra-example-driver/tree/main/deployments/helm/dra-example-driver/templates。
// 传统上 kubelet 不支持插件的滚动更新，因此 DaemonSet 不能把 maxSurge 设为大于零的值。
// 使用默认的 maxSurge: 0 更新驱动 DaemonSet 时，会先关停旧驱动 Pod，再启动替代 Pod。
// 这会带来短暂的服务不可用：
//   - 若 Pod 依赖的 ResourceClaim 尚未准备好，Pod 就无法启动；
//   - 最后一个使用该 claim 的 Pod 离开后，清理工作会延后到驱动恢复可用；此时 Pod 不会被标记为
//     终止，从而阻止把该 Pod 占用的资源复用给其他 Pod；
//   - 就 Kubernetes 而言，正在运行的 Pod 不受影响。但 DRA 驱动可能提供必需的运行时服务，
//     这一点需要由厂商自行说明。
//
// 注意第二点还意味着：排空节点时应先驱逐普通 Pod，最后再驱逐驱动 DaemonSet Pod。
// 从 Kubernetes 1.33 开始，kubelet 支持滚动更新：新旧 Pod 会短暂并存并优雅交接工作，不产生停机。
// 但无法事先判断 DaemonSet 所在节点是否支持该能力；若对尚不支持的 kubelet 执行滚动更新，
// 关停旧 Pod 会注销驱动（即使新 Pod 已在运行），更新因此失败。插件监听器的工作机制详见：
// https://github.com/kubernetes/kubernetes/blob/v1.33.0/pkg/kubelet/pluginmanager/pluginwatcher/README.md#seamless-upgrade
// DRA 驱动既可以把 1.33 作为最低 Kubernetes 版本要求，也可以提供两种 DaemonSet 部署变体：
// 支持滚动更新的变体可把 maxSurge 设为非零值。管理员需要小心选择合适的变体。
package kubeletplugin
