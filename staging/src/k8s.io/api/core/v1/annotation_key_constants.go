/*
Copyright 2017 The Kubernetes Authors.

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

// 此文件应与 pkg/apis/core/annotation_key_constants.go 保持一致。

package v1

const (
	// ImagePolicyFailedOpenKey 在镜像策略 webhook 后端失败而“失败放行”时，
	// 添加到所创建的 Pod 上。
	ImagePolicyFailedOpenKey string = "alpha.image-policy.k8s.io/failed-open"

	// MirrorPodAnnotationKey 表示 kubelet 创建静态 Pod 时设置的注解键。
	MirrorPodAnnotationKey string = "kubernetes.io/config.mirror"

	// TolerationsAnnotationKey 表示 Pod 注解中 tolerations 数据（JSON 序列化）的键。
	TolerationsAnnotationKey string = "scheduler.alpha.kubernetes.io/tolerations"

	// TaintsAnnotationKey 表示 Node 注解中 taints 数据（JSON 序列化）的键。
	TaintsAnnotationKey string = "scheduler.alpha.kubernetes.io/taints"

	// SeccompPodAnnotationKey 表示应用于 Pod 中所有容器的 seccomp profile 的键。
	// 已废弃：请设置 Pod 安全上下文的 `seccompProfile` 字段。
	SeccompPodAnnotationKey string = "seccomp.security.alpha.kubernetes.io/pod"

	// SeccompContainerAnnotationKeyPrefix 表示应用于 Pod 中某个容器的 seccomp profile 的键。
	// 已废弃：请设置容器安全上下文的 `seccompProfile` 字段。
	SeccompContainerAnnotationKeyPrefix string = "container.seccomp.security.alpha.kubernetes.io/"

	// SeccompProfileRuntimeDefault 表示容器运行时使用的默认 seccomp profile。
	// 已废弃：请设置 Pod 或容器安全上下文中类型为 "RuntimeDefault" 的 `seccompProfile`。
	SeccompProfileRuntimeDefault string = "runtime/default"

	// SeccompProfileNameUnconfined 表示不受限（unconfined）的 seccomp profile。
	SeccompProfileNameUnconfined string = "unconfined"

	// SeccompLocalhostProfileNamePrefix 是从节点磁盘加载 profile 的前缀。
	SeccompLocalhostProfileNamePrefix = "localhost/"

	// DeprecatedAppArmorBetaContainerAnnotationKeyPrefix 是指定容器 apparmor profile 的注解键前缀。
	// 已废弃：请设置 Pod 或容器安全上下文的 `appArmorProfile` 字段。
	DeprecatedAppArmorBetaContainerAnnotationKeyPrefix = "container.apparmor.security.beta.kubernetes.io/"

	// DeprecatedAppArmorBetaProfileRuntimeDefault 是指定运行时默认 profile 的 profile。
	DeprecatedAppArmorBetaProfileRuntimeDefault = "runtime/default"

	// DeprecatedAppArmorBetaProfileNamePrefix 是从节点加载 profile 的前缀。
	DeprecatedAppArmorBetaProfileNamePrefix = "localhost/"

	// DeprecatedAppArmorBetaProfileNameUnconfined 是不受限的 AppArmor profile。
	DeprecatedAppArmorBetaProfileNameUnconfined = "unconfined"

	// DeprecatedSeccompProfileDockerDefault 表示 docker 使用的默认 seccomp profile。
	// 已废弃：请设置 Pod 或容器安全上下文中类型为 "RuntimeDefault" 的 `seccompProfile`。
	DeprecatedSeccompProfileDockerDefault string = "docker/default"

	// PreferAvoidPodsAnnotationKey 表示 Node 注解中 preferAvoidPods 数据（JSON 序列化）的键。
	PreferAvoidPodsAnnotationKey string = "scheduler.alpha.kubernetes.io/preferAvoidPods"

	// ObjectTTLAnnotationKey 是给 kubelet 的建议：在从 apiserver 重新获取某个对象
	// （如 secret、config map）之前，可以缓存多长时间。此注解可附加到节点上。
	ObjectTTLAnnotationKey string = "node.alpha.kubernetes.io/ttl"

	// NonConvertibleAnnotationPrefix 是用于标识不可转换 JSON 路径的注解键前缀。
	NonConvertibleAnnotationPrefix = "non-convertible.kubernetes.io"

	kubectlPrefix = "kubectl.kubernetes.io/"

	// LastAppliedConfigAnnotation 用于存储资源的上一版配置，供 UpdateApplyAnnotation
	// 进行三方 diff 时使用。
	LastAppliedConfigAnnotation = kubectlPrefix + "last-applied-configuration"

	// AnnotationLoadBalancerSourceRangesKey 是 Service 上的注解键，用于设置其 LoadBalancer
	// 允许的入站范围。
	//
	// 其值应为以逗号分隔的 CIDR 列表，例如 `0.0.0.0/0` 表示允许全部访问（默认），
	// `18.0.0.0/8,56.0.0.0/8` 表示只允许来自当前分配给 MIT 与 USPS 的 CIDR 的访问。
	//
	// 并非所有云提供商都支持此注解，AWS 和 GCE 支持。
	AnnotationLoadBalancerSourceRangesKey = "service.beta.kubernetes.io/load-balancer-source-ranges"

	// EndpointsLastChangeTriggerTime 是为 Endpoints 对象设置的注解键，表示触发该 Endpoints
	// 对象变更的某个 Pod 或 Service 对象最后一次变更的时间戳（以 RFC 3339 日期时间字符串存储，
	// 例如 '2018-10-22T19:32:52.1Z'）。换言之，若 Pod/Service 在 T0 时间发生变更，
	// Endpoints 控制器在 T1 观察到该变更，Endpoints 对象在 T2 被修改，则
	// EndpointsLastChangeTriggerTime 会设为 T0。
	//
	// 这里的“端点变更触发”指任何导致 Endpoints 对象变更的 Pod 或 Service 变更。
	//
	// 根据“端点变更触发”的定义，请注意此注解仅在由 Pod 或 Service 变更引发的 Endpoints
	// 对象变更时设置；若 Endpoints 对象因其他原因变更，则不会设置（若已设置也不会更新）此注解。
	//
	// 此注解将用于计算集群内网络编程延迟 SLI，参见
	// https://github.com/kubernetes/community/blob/master/sig-scalability/slos/network_programming_latency.md
	EndpointsLastChangeTriggerTime = "endpoints.kubernetes.io/last-change-trigger-time"

	// 当 Endpoints 资源超过 1000 个地址的最大容量时，会在其上设置 EndpointsOverCapacity。
	// 最初 Endpoints 控制器会将该注解的值设为 "warning"；未来版本中控制器可能将其设为
	// "truncated"，以表明超过 1000 限制的地址已被从 Endpoints 资源中截断。
	EndpointsOverCapacity = "endpoints.kubernetes.io/over-capacity"

	// MigratedPluginsAnnotationKey 是为 CSINode 对象设置的注解键，其值是以逗号分隔的
	// in-tree 插件列表，这些插件将由 CSINode 所代表节点上的 CSI 后端提供服务。
	// 挂载/卸载控制器使用此注解来判断特定节点上的卷插件应使用 in-tree 还是 CSI 后端。
	MigratedPluginsAnnotationKey = "storage.alpha.kubernetes.io/migrated-plugins"

	// PodDeletionCost 可设置为 int32，表示删除某个 Pod 相对于同一 ReplicaSet 中其他 Pod 的代价。
	// 删除时代价较低的 Pod 会被优先删除。注意这只是尽力而为，并不保证 Pod 的删除顺序。
	// 未设置此注解的 Pod 其隐含删除代价为 0，允许使用负值。
	//
	// 此注解为 beta 级别，仅在 PodDeletionCost 特性启用时生效。
	PodDeletionCost = "controller.kubernetes.io/pod-deletion-cost"

	// DeprecatedAnnotationTopologyAwareHints 可用于为 Service 启用或禁用拓扑感知提示，
	// 可设置为 "Auto" 或 "Disabled"，其他任何值均按 "Disabled" 处理。
	// 此注解已废弃，推荐使用 "service.kubernetes.io/topology-mode" 注解。
	DeprecatedAnnotationTopologyAwareHints = "service.kubernetes.io/topology-aware-hints"

	// AnnotationTopologyMode 可用于为 Service 启用或禁用拓扑感知路由。已知值为 "Auto"
	// 和 "Disabled"。实现方可以自行开发新的拓扑方式，并通过带域名前缀的值暴露，例如
	// "example.com/lowest-rtt" 就是一个有效的实现自定义值。这些启发式方法通常会在
	// EndpointSlice 上填充拓扑提示，但这并非强制要求。
	AnnotationTopologyMode = "service.kubernetes.io/topology-mode"
)
