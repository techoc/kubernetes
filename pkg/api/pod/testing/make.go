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

package testing

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

// Tweak 是修改 Pod 的函数。
type Tweak func(*api.Pod)

// TweakContainer 是修改 Pod 中单个容器的函数。
type TweakContainer func(*api.Container)

// TweakPodStatus 是修改 Pod 状态的函数。
type TweakPodStatus func(*api.PodStatus)

// MakePod 用于构造能通过 API 校验的 Pod 对象，比直接写结构体字面量更简洁清晰。
// 默认生成一个只含一个容器（名为 ctr）的 Pod。
// 调用方可以传入任意数量的 tweak 函数，对结果做进一步修改。
func MakePod(name string, tweaks ...Tweak) *api.Pod {
	// 所有本会被默认值填充的字段都必须在此显式提供有效值。
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: api.PodSpec{
			Containers:                    []api.Container{MakeContainer("ctr")},
			DNSPolicy:                     api.DNSClusterFirst,
			RestartPolicy:                 api.RestartPolicyAlways,
			TerminationGracePeriodSeconds: ptr.To[int64](v1.DefaultTerminationGracePeriodSeconds),
		},
	}

	for _, tweak := range tweaks {
		tweak(pod)
	}

	return pod
}

// MakePodSpec 构造一个 PodSpec：复用 MakePod 生成完整 Pod 后只取其 Spec 部分。
func MakePodSpec(tweaks ...Tweak) api.PodSpec {
	return MakePod("", tweaks...).Spec
}

// SetNamespace 设置 Pod 的命名空间。
func SetNamespace(ns string) Tweak {
	return func(pod *api.Pod) {
		pod.Namespace = ns
	}
}

// SetResourceVersion 设置 Pod 的 resourceVersion。
func SetResourceVersion(rv string) Tweak {
	return func(pod *api.Pod) {
		pod.ResourceVersion = rv
	}
}

// SetPodResources 设置 Pod 级别的资源（Spec.Resources）。
func SetPodResources(resources *api.ResourceRequirements) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Resources = resources
	}
}

// SetContainers 替换 Pod 的普通容器列表。
func SetContainers(containers ...api.Container) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Containers = containers
	}
}

// SetInitContainers 替换 Pod 的 init 容器列表。
func SetInitContainers(containers ...api.Container) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.InitContainers = containers
	}
}

// SetEphemeralContainers 替换 Pod 的临时容器列表。
func SetEphemeralContainers(containers ...api.EphemeralContainer) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.EphemeralContainers = containers
	}
}

// SetVolumes 替换 Pod 的卷列表。
func SetVolumes(volumes ...api.Volume) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Volumes = volumes
	}
}

// MakeEmptyVolume 构造一个 EmptyDir 类型的卷，常用于测试中挂载空目录。
func MakeEmptyVolume(name string) api.Volume {
	return api.Volume{Name: name, VolumeSource: api.VolumeSource{EmptyDir: &api.EmptyDirVolumeSource{}}}
}

// SetNodeSelector 设置 Pod 的节点选择器。
func SetNodeSelector(nodeSelector map[string]string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.NodeSelector = nodeSelector
	}
}

// SetNodeName 直接把 Pod 调度到指定节点（绕过调度器）。
func SetNodeName(name string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.NodeName = name
	}
}

// SetActiveDeadlineSeconds 设置 Pod 的活跃截止时间。
func SetActiveDeadlineSeconds(deadline int64) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.ActiveDeadlineSeconds = &deadline
	}
}

// SetServiceAccountName 同时设置新旧两个 serviceAccount 字段，与默认值填充行为保持一致。
func SetServiceAccountName(name string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.ServiceAccountName = name
		pod.Spec.DeprecatedServiceAccount = name //nolint:staticcheck // SA1019 to match defaulting
	}
}

// SetDeprecatedServiceAccount 只应用于测试校验逻辑是否依赖正确的默认值填充。
// 其他测试场景请用 SetServiceAccountName，它会同时设置新旧两个字段。
func SetDeprecatedServiceAccount(name string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.DeprecatedServiceAccount = name //nolint:staticcheck // SA1019 for testing
	}
}

// SetSecurityContext 设置 Pod 级别的安全上下文。
func SetSecurityContext(ctx *api.PodSecurityContext) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.SecurityContext = ctx
	}
}

// SetHostNetwork 设置是否使用宿主机网络命名空间。
func SetHostNetwork(b bool) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.HostNetwork = b
	}
}

// SetHostPID 设置是否使用宿主机 PID 命名空间。
func SetHostPID(b bool) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.HostPID = b
	}
}

// SetHostIPC 设置是否使用宿主机 IPC 命名空间。
func SetHostIPC(b bool) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.HostIPC = b
	}
}

// SetShareProcessNamespace 设置 Pod 内容器是否共享进程命名空间。
func SetShareProcessNamespace(b *bool) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.ShareProcessNamespace = b
	}
}

// SetHostUsers 设置是否使用宿主机用户命名空间。
func SetHostUsers(b *bool) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.HostUsers = b
	}
}

// SetAffinity 设置 Pod 的亲和性/反亲和性配置。
func SetAffinity(affinity *api.Affinity) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Affinity = affinity
	}
}

// SetHostAliases 设置 Pod 的 /etc/hosts 静态条目。
func SetHostAliases(hostAliases ...api.HostAlias) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.HostAliases = hostAliases
	}
}

// SetPriorityClassName 设置 Pod 的优先级类名。
func SetPriorityClassName(name string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.PriorityClassName = name
	}
}

// SetRuntimeClassName 设置 Pod 的 RuntimeClass 名称。
func SetRuntimeClassName(name string) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.RuntimeClassName = &name
	}
}

// SetOverhead 设置 Pod 的额外资源开销。
func SetOverhead(overhead api.ResourceList) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Overhead = overhead
	}
}

// SetDNSPolicy 设置 Pod 的 DNS 策略。
func SetDNSPolicy(policy api.DNSPolicy) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.DNSPolicy = policy
	}
}

// SetDNSConfig 设置 Pod 的自定义 DNS 配置。
func SetDNSConfig(config *api.PodDNSConfig) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.DNSConfig = config
	}
}

// SetRestartPolicy 设置 Pod 的重启策略。
func SetRestartPolicy(policy api.RestartPolicy) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.RestartPolicy = policy
	}
}

// SetTolerations 设置 Pod 对污点的容忍规则。
func SetTolerations(tolerations ...api.Toleration) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.Tolerations = tolerations
	}
}

// SetAnnotations 设置 Pod 的 annotations。
func SetAnnotations(annos map[string]string) Tweak {
	return func(pod *api.Pod) {
		pod.Annotations = annos
	}
}

// SetLabels 设置 Pod 的 labels。
func SetLabels(annos map[string]string) Tweak {
	return func(pod *api.Pod) {
		pod.Labels = annos
	}
}

// SetGeneration 设置 Pod 的 generation。
func SetGeneration(gen int64) Tweak {
	return func(pod *api.Pod) {
		pod.Generation = gen
	}
}

// SetSchedulingGates 设置 Pod 的调度门控，门控未移除前 Pod 不会被调度。
func SetSchedulingGates(gates ...api.PodSchedulingGate) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.SchedulingGates = gates
	}
}

// SetTerminationGracePeriodSeconds 设置 Pod 的终止宽限期。
func SetTerminationGracePeriodSeconds(grace int64) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.TerminationGracePeriodSeconds = &grace
	}
}

// SetOS 设置 Pod 要求的操作系统（linux/windows）。
func SetOS(name api.OSName) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.OS = &api.PodOS{Name: name}
	}
}

// SetStatus 设置 Pod 的 status。
func SetStatus(status api.PodStatus) Tweak {
	return func(pod *api.Pod) {
		pod.Status = status
	}
}

// SetResourceClaims 设置 Pod 的资源声明列表（DRA）。
func SetResourceClaims(claims ...api.PodResourceClaim) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.ResourceClaims = claims
	}
}

// SetTopologySpreadConstraints 设置 Pod 的拓扑分布约束。
func SetTopologySpreadConstraints(tsc ...api.TopologySpreadConstraint) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.TopologySpreadConstraints = tsc
	}
}

// SetObjectMeta 整体替换 Pod 的 ObjectMeta。
func SetObjectMeta(objectMeta metav1.ObjectMeta) Tweak {
	return func(pod *api.Pod) {
		pod.ObjectMeta = objectMeta
	}
}

// SetSchedulingGroup 设置 Pod 的调度组。
func SetSchedulingGroup(schedulingGroup *api.PodSchedulingGroup) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.SchedulingGroup = schedulingGroup
	}
}

// SetEvictionResponders 设置 Pod 的驱逐响应器列表。
func SetEvictionResponders(evictionResponders ...api.EvictionResponder) Tweak {
	return func(pod *api.Pod) {
		pod.Spec.EvictionResponders = evictionResponders
	}
}

// MakeContainer 构造一个能通过 API 校验的容器：默认镜像、镜像拉取策略和终止消息设置都已填好。
func MakeContainer(name string, tweaks ...TweakContainer) api.Container {
	cnr := api.Container{
		Name: name, Image: "image", ImagePullPolicy: "IfNotPresent",
		TerminationMessagePolicy: "File",
		TerminationMessagePath:   v1.TerminationMessagePathDefault,
	}

	for _, tweak := range tweaks {
		tweak(&cnr)
	}

	return cnr
}

// SetContainerImage 设置容器镜像。
func SetContainerImage(image string) TweakContainer {
	return func(cnr *api.Container) {
		cnr.Image = image
	}
}

// SetContainerLifecycle 设置容器的生命周期钩子。
func SetContainerLifecycle(lifecycle api.Lifecycle) TweakContainer {
	return func(cnr *api.Container) {
		cnr.Lifecycle = &lifecycle
	}
}

// MakeResourceRequirements 把字符串形式的 requests/limits 解析成 Quantity 资源列表。
func MakeResourceRequirements(requests, limits map[string]string) api.ResourceRequirements {
	rr := api.ResourceRequirements{Requests: api.ResourceList{}, Limits: api.ResourceList{}}
	for k, v := range requests {
		rr.Requests[api.ResourceName(k)] = resource.MustParse(v)
	}
	for k, v := range limits {
		rr.Limits[api.ResourceName(k)] = resource.MustParse(v)
	}
	return rr
}

// SetContainerResources 设置容器的资源 requests/limits。
func SetContainerResources(rr api.ResourceRequirements) TweakContainer {
	return func(cnr *api.Container) {
		cnr.Resources = rr
	}
}

// SetContainerPorts 设置容器暴露的端口列表。
func SetContainerPorts(ports ...api.ContainerPort) TweakContainer {
	return func(cnr *api.Container) {
		cnr.Ports = ports
	}
}

// SetContainerResizePolicy 设置容器的资源调整策略。
func SetContainerResizePolicy(policies ...api.ContainerResizePolicy) TweakContainer {
	return func(cnr *api.Container) {
		cnr.ResizePolicy = policies
	}
}

// SetContainerSecurityContext 设置容器级别的安全上下文。
func SetContainerSecurityContext(ctx api.SecurityContext) TweakContainer {
	return func(cnr *api.Container) {
		cnr.SecurityContext = &ctx
	}
}

// SetContainerRestartPolicy 设置容器的重启策略。
func SetContainerRestartPolicy(policy api.ContainerRestartPolicy) TweakContainer {
	return func(cnr *api.Container) {
		cnr.RestartPolicy = &policy
	}
}

// SetContainerRestartPolicyRules 设置容器重启策略规则。
func SetContainerRestartPolicyRules(rules ...api.ContainerRestartRule) TweakContainer {
	return func(cnr *api.Container) {
		cnr.RestartPolicyRules = rules
	}
}

// MakePodStatus 构造 PodStatus，由若干 tweak 函数填充具体字段。
func MakePodStatus(tweaks ...TweakPodStatus) api.PodStatus {
	ps := api.PodStatus{}

	for _, tweak := range tweaks {
		tweak(&ps)
	}

	return ps
}

// SetContainerStatuses 设置普通容器的状态列表。
func SetContainerStatuses(containerStatuses ...api.ContainerStatus) TweakPodStatus {
	return func(podstatus *api.PodStatus) {
		podstatus.ContainerStatuses = containerStatuses
	}
}

// SetInitContainerStatuses 设置 init 容器的状态列表。
func SetInitContainerStatuses(containerStatuses ...api.ContainerStatus) TweakPodStatus {
	return func(podstatus *api.PodStatus) {
		podstatus.InitContainerStatuses = containerStatuses
	}
}

// SetEphemeralContainerStatuses 设置临时容器的状态列表。
func SetEphemeralContainerStatuses(containerStatuses ...api.ContainerStatus) TweakPodStatus {
	return func(podstatus *api.PodStatus) {
		podstatus.EphemeralContainerStatuses = containerStatuses
	}
}

// MakeContainerStatus 构造容器状态，主要用于设置已分配资源。
func MakeContainerStatus(name string, allocatedResources api.ResourceList) api.ContainerStatus {
	cs := api.ContainerStatus{
		Name:               name,
		AllocatedResources: allocatedResources,
	}

	return cs
}

// TweakContainers 将容器 tweak 应用于 Pod 的全部普通容器和 init 容器。
// 通常应在所有容器添加完毕后，再将它加入 Pod tweak。
func TweakContainers(tweaks ...TweakContainer) Tweak {
	return func(pod *api.Pod) {
		for i := range pod.Spec.InitContainers {
			for _, tweak := range tweaks {
				tweak(&pod.Spec.InitContainers[i])
			}
		}
		for i := range pod.Spec.Containers {
			for _, tweak := range tweaks {
				tweak(&pod.Spec.Containers[i])
			}
		}
	}
}
