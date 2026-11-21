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

// 本文件应与 pkg/api/v1/annotation_key_constants.go 保持一致。

package core

const (
	// ImagePolicyFailedOpenKey 在镜像策略 webhook 后端故障而“失败开放（fail open）”时所创建的 Pod 上添加。
	ImagePolicyFailedOpenKey string = "alpha.image-policy.k8s.io/failed-open"

	// MirrorPodAnnotationKey 表示 kubelet 创建镜像 Pod（mirror pod）时设置的注解键。
	MirrorPodAnnotationKey string = "kubernetes.io/config.mirror"

	// TolerationsAnnotationKey 表示 Pod 注解中容忍（tolerations）数据的键，数据为 JSON 序列化结果。
	TolerationsAnnotationKey string = "scheduler.alpha.kubernetes.io/tolerations"

	// TaintsAnnotationKey 表示 Node 注解中污点（taints）数据的键，数据为 JSON 序列化结果。
	TaintsAnnotationKey string = "scheduler.alpha.kubernetes.io/taints"

	// SeccompPodAnnotationKey 表示应用到 Pod 中所有容器的 seccomp profile 的注解键。
	// Deprecated: 改用 Pod security context 的 `seccompProfile` 字段。
	SeccompPodAnnotationKey string = "seccomp.security.alpha.kubernetes.io/pod"

	// SeccompContainerAnnotationKeyPrefix 表示应用到 Pod 中某个容器的 seccomp profile 的注解键前缀。
	// Deprecated: 改用容器 security context 的 `seccompProfile` 字段。
	SeccompContainerAnnotationKeyPrefix string = "container.seccomp.security.alpha.kubernetes.io/"

	// SeccompProfileRuntimeDefault 表示容器运行时使用的默认 seccomp profile。
	// Deprecated: 改用 Pod 或容器 security context 中类型为 "RuntimeDefault" 的 `seccompProfile`。
	SeccompProfileRuntimeDefault string = "runtime/default"

	// DeprecatedSeccompProfileDockerDefault 表示 docker 使用的默认 seccomp profile。
	// Deprecated: 改用 Pod 或容器 security context 中类型为 "RuntimeDefault" 的 `seccompProfile`。
	DeprecatedSeccompProfileDockerDefault string = "docker/default"

	// DeprecatedAppArmorAnnotationKeyPrefix 是用于指定容器 apparmor profile 的注解键前缀。
	// Deprecated: 改用 Pod 或容器 security context 的 `appArmorProfile` 字段。
	DeprecatedAppArmorAnnotationKeyPrefix = "container.apparmor.security.beta.kubernetes.io/"

	// DeprecatedAppArmorAnnotationValueRuntimeDefault 表示使用运行时默认 profile。
	DeprecatedAppArmorAnnotationValueRuntimeDefault = "runtime/default"

	// DeprecatedAppArmorAnnotationValueLocalhostPrefix 表示指定节点上已加载 profile 的前缀。
	DeprecatedAppArmorAnnotationValueLocalhostPrefix = "localhost/"

	// DeprecatedAppArmorAnnotationValueUnconfined 表示 Unconfined（不受限）AppArmor profile。
	DeprecatedAppArmorAnnotationValueUnconfined = "unconfined"

	// PreferAvoidPodsAnnotationKey 表示 Node 注解中 preferAvoidPods 数据的键，数据为 JSON 序列化结果。
	PreferAvoidPodsAnnotationKey string = "scheduler.alpha.kubernetes.io/preferAvoidPods"

	// ObjectTTLAnnotationKey 是对 kubelet 的建议：在从 apiserver 重新拉取某个对象（如 secret、config map）
	// 之前，可以缓存该对象多长时间。该注解可附加在 node 上。
	ObjectTTLAnnotationKey string = "node.alpha.kubernetes.io/ttl"

	// NonConvertibleAnnotationPrefix 是用于标识不可转换（non-convertible）JSON 路径的注解键前缀。
	NonConvertibleAnnotationPrefix = "non-convertible.kubernetes.io"

	kubectlPrefix = "kubectl.kubernetes.io/"

	// LastAppliedConfigAnnotation 用于存储资源先前的配置，供 UpdateApplyAnnotation 进行三方合并（three way diff）时使用。
	LastAppliedConfigAnnotation = kubectlPrefix + "last-applied-configuration"

	// AnnotationLoadBalancerSourceRangesKey 是 Service 上的注解键，用于设置其 LoadBalancer 允许的入口地址范围。
	//
	// 其值应为以逗号分隔的 CIDR 列表，例如 `0.0.0.0/0` 表示允许全部访问（默认值），
	// 或 `18.0.0.0/8,56.0.0.0/8` 表示仅允许来自当前分配给 MIT 与 USPS 的 CIDR 的访问。
	//
	// 并非所有云厂商都支持该注解，但 AWS 和 GCE 支持。
	AnnotationLoadBalancerSourceRangesKey = "service.beta.kubernetes.io/load-balancer-source-ranges"

	// EndpointsLastChangeTriggerTime 是设置在 Endpoints 对象上的注解键，
	// 表示触发该 Endpoints 对象变更的某个 Pod 或 Service 对象最后一次变更的时间戳
	// （以 RFC 3339 date-time 字符串存储，例如 '2018-10-22T19:32:52.1Z'）。
	// 换句话说，若 Pod / Service 在 T0 时刻发生变更，endpoints 控制器在 T1 时刻观察到该变更，
	// 而 Endpoints 对象在 T2 时刻发生变更，则 EndpointsLastChangeTriggerTime 会被设为 T0。
	//
	// 这里的“endpoints 变更触发”指任何导致 Endpoints 对象变更的 Pod 或 Service 变更。
	//
	// 根据“endpoints 变更触发”的定义，请注意：只有由 Pod 或 Service 变更引起的 Endpoints 对象变更
	// 才会设置该注解。如果 Endpoints 对象因其他原因变更，则不会设置（若已设置也不会更新）该注解。
	//
	// 该注解用于计算集群内网络编程延迟 SLI，参见
	// https://github.com/kubernetes/community/blob/master/sig-scalability/slos/network_programming_latency.md
	EndpointsLastChangeTriggerTime = "endpoints.kubernetes.io/last-change-trigger-time"

	// EndpointsOverCapacity 会在 Endpoints 资源地址数超过 1000 这个最大容量时设置。
	// 最初 endpoints 控制器会把该注解的值设为 "warning"。在后续版本中，控制器可能将其值设为
	// "truncated"，表示超过 1000 上限的地址已被从 Endpoints 资源中截断。
	EndpointsOverCapacity = "endpoints.kubernetes.io/over-capacity"

	// MigratedPluginsAnnotationKey 是设置在 CSINode 对象上的注解键，其值是以逗号分隔的 in-tree 插件列表，
	// 这些插件在 CSINode 所代表的节点上将由 CSI 后端提供服务。
	// Attach Detach Controller 使用该注解来判断某个节点上的卷插件应使用 in-tree 后端还是 CSI 后端。
	MigratedPluginsAnnotationKey = "storage.alpha.kubernetes.io/migrated-plugins"

	// PodDeletionCost 可设置为一个 int32 值，表示删除某个 Pod 相对于同一 ReplicaSet 中其他 Pod 的代价。
	// 删除代价较低的 Pod 会被优先于代价较高的 Pod 删除。
	// 注意这是尽力而为（best-effort）的，因此不保证 Pod 的删除顺序。
	// 未设置该注解的 Pod 其隐含删除代价为 0，允许使用负值。
	//
	// 该注解处于 beta 级别，仅在 PodDeletionCost 特性开启时生效。
	PodDeletionCost = "controller.kubernetes.io/pod-deletion-cost"

	// DeprecatedAnnotationTopologyAwareHints 可用于为 Service 开启或关闭拓扑感知提示（Topology Aware Hints）。
	// 可设为 "Auto" 或 "Disabled"，其他任何值都视为 "Disabled"。
	// 该注解已废弃，推荐使用 `service.kubernetes.io/topology-mode` 注解；
	// 后者同样接受 "Auto" 和 "Disabled"，但不局限于此（它是开放式的，为实验留出空间，
	// 同时我们也在推进通过 spec 配置拓扑能力）。当
	// `service.kubernetes.io/topology-aware-hints` 与 `service.kubernetes.io/topology-mode`
	// 两个注解同时设置时，以 `service.kubernetes.io/topology-aware-hints` 的值为准。
	DeprecatedAnnotationTopologyAwareHints = "service.kubernetes.io/topology-aware-hints"

	// AnnotationTopologyMode 可用于为 Service 开启或关闭拓扑感知路由（Topology Aware Routing）。
	// 众所周知的值是 "Auto" 和 "Disabled"。实现方可以选择开发新的拓扑策略，
	// 并以域名前缀的形式暴露出来。例如 "example.com/lowest-rtt" 可以是该注解的一个特定实现值。
	// 这些启发式策略通常会在 EndpointSlice 上填充拓扑提示（topology hints），但这并非强制要求。
	AnnotationTopologyMode = "service.kubernetes.io/topology-mode"
)
