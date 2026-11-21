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

package v1

import (
	"time"

	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/api/v1/service"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/util/parsers"
)

// addDefaultingFuncs 向 scheme 注册 v1 的所有默认值填充函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ResourceList 将资源值向上取整到毫（milli）级精度，以保持 API 兼容性。
func SetDefaults_ResourceList(obj *v1.ResourceList) {
	for key, val := range *obj {
		// TODO(#18538): We round up resource values to milli scale to maintain API compatibility.
		// In the future, we should instead reject values that need rounding.
		const milliScale = -3
		val.RoundUp(milliScale)

		(*obj)[v1.ResourceName(key)] = val
	}
}

// SetDefaults_ReplicationController 从 Pod 模板的标签中填充 RC 的 selector 与 Labels。
func SetDefaults_ReplicationController(obj *v1.ReplicationController) {
	var labels map[string]string
	if obj.Spec.Template != nil {
		labels = obj.Spec.Template.Labels
	}
	// TODO: support templates defined elsewhere when we support them in the API
	if labels != nil {
		if len(obj.Spec.Selector) == 0 {
			obj.Spec.Selector = labels
		}
		if len(obj.Labels) == 0 {
			obj.Labels = labels
		}
	}
	// obj.Spec.Replicas 通过声明式默认值机制填充。
}

// SetDefaults_Volume 在卷源未指定任何字段时默认使用 EmptyDir；
// 并对 Image 卷根据镜像 tag 填充默认拉取策略。
func SetDefaults_Volume(obj *v1.Volume) {
	if ptr.AllPtrFieldsNil(&obj.VolumeSource) {
		obj.VolumeSource = v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) && obj.Image != nil && obj.Image.PullPolicy == "" {
		// 镜像 tag 为 :latest 时拉取策略默认为 Always，否则为 IfNotPresent。
		_, tag, _, _ := parsers.ParseImageName(obj.Image.Reference)
		if tag == "latest" {
			obj.Image.PullPolicy = v1.PullAlways
		} else {
			obj.Image.PullPolicy = v1.PullIfNotPresent
		}
	}
}

// SetDefaults_Container 填充容器的默认镜像拉取策略、终止消息路径与终止消息策略。
func SetDefaults_Container(obj *v1.Container) {
	if obj.ImagePullPolicy == "" {
		// 忽略解析错误：镜像名的合法性假定已在别处校验过。
		_, tag, _, _ := parsers.ParseImageName(obj.Image)

		// 根据镜像 tag 决定拉取策略。
		if tag == "latest" {
			obj.ImagePullPolicy = v1.PullAlways
		} else {
			obj.ImagePullPolicy = v1.PullIfNotPresent
		}
	}
	if obj.TerminationMessagePath == "" {
		obj.TerminationMessagePath = v1.TerminationMessagePathDefault
	}
	if obj.TerminationMessagePolicy == "" {
		obj.TerminationMessagePolicy = v1.TerminationMessageReadFile
	}
}

// SetDefaults_EphemeralContainer 复用普通容器的默认值填充逻辑处理临时容器的公共字段。
func SetDefaults_EphemeralContainer(obj *v1.EphemeralContainer) {
	SetDefaults_Container((*v1.Container)(&obj.EphemeralContainerCommon))
}

// SetDefaults_Service 填充 Service 的默认会话亲和性、类型、端口协议与目标端口，
// 以及外部/内部流量策略和 LoadBalancer 相关的默认取值。
func SetDefaults_Service(obj *v1.Service) {
	if obj.Spec.SessionAffinity == "" {
		obj.Spec.SessionAffinity = v1.ServiceAffinityNone
	}
	if obj.Spec.SessionAffinity == v1.ServiceAffinityNone {
		obj.Spec.SessionAffinityConfig = nil
	}
	if obj.Spec.SessionAffinity == v1.ServiceAffinityClientIP {
		if obj.Spec.SessionAffinityConfig == nil || obj.Spec.SessionAffinityConfig.ClientIP == nil || obj.Spec.SessionAffinityConfig.ClientIP.TimeoutSeconds == nil {
			timeoutSeconds := v1.DefaultClientIPServiceAffinitySeconds
			obj.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
				ClientIP: &v1.ClientIPConfig{
					TimeoutSeconds: &timeoutSeconds,
				},
			}
		}
	}
	if obj.Spec.Type == "" {
		obj.Spec.Type = v1.ServiceTypeClusterIP
	}
	for i := range obj.Spec.Ports {
		sp := &obj.Spec.Ports[i]
		if sp.Protocol == "" {
			sp.Protocol = v1.ProtocolTCP
		}
		if sp.TargetPort == intstr.FromInt32(0) || sp.TargetPort == intstr.FromString("") {
			sp.TargetPort = intstr.FromInt32(sp.Port)
		}
	}
	// 为保持一致，对可外部访问的 Service 默认设置 ExternalTrafficPolicy 为 Cluster。
	if service.ExternallyAccessible(obj) && obj.Spec.ExternalTrafficPolicy == "" {
		obj.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
	}

	if obj.Spec.InternalTrafficPolicy == nil {
		if obj.Spec.Type == v1.ServiceTypeNodePort || obj.Spec.Type == v1.ServiceTypeLoadBalancer || obj.Spec.Type == v1.ServiceTypeClusterIP {
			serviceInternalTrafficPolicyCluster := v1.ServiceInternalTrafficPolicyCluster
			obj.Spec.InternalTrafficPolicy = &serviceInternalTrafficPolicyCluster
		}
	}

	if obj.Spec.Type == v1.ServiceTypeLoadBalancer {
		if obj.Spec.AllocateLoadBalancerNodePorts == nil {
			obj.Spec.AllocateLoadBalancerNodePorts = ptr.To(true)
		}
	}

	if obj.Spec.Type == v1.ServiceTypeLoadBalancer {
		ipMode := v1.LoadBalancerIPModeVIP
		for i, ing := range obj.Status.LoadBalancer.Ingress {
			if ing.IP != "" && ing.IPMode == nil {
				obj.Status.LoadBalancer.Ingress[i].IPMode = &ipMode
			}
		}
	}

}

// SetDefaults_Pod 填充 Pod 特有（而非 PodSpec 通用）的默认值：
// 修正负的终止宽限期、由 limit 推导 request，以及服务链接、主机网络和探针协议等默认值。
func SetDefaults_Pod(obj *v1.Pod) {
	// 该修正只作用于 Pod 而不作用于 PodSpec：出于历史原因，PodTemplate 不按此方式填充默认值。
	if obj.Spec.TerminationGracePeriodSeconds != nil && *obj.Spec.TerminationGracePeriodSeconds < 0 {
		obj.Spec.TerminationGracePeriodSeconds = ptr.To[int64](1)
	}

	// 若只指定了 limit 而未指定 request，则用 limit 填充 request。
	// 这一逻辑放在这里而不是 ResourceRequirements 的默认值填充中，
	// 是因为只希望对 v1.Pod 生效，而不影响 v1.PodTemplate。
	for i := range obj.Spec.Containers {
		// 未指定 request 但指定了 limit 时，将 request 设为 limit。
		if obj.Spec.Containers[i].Resources.Limits != nil {
			if obj.Spec.Containers[i].Resources.Requests == nil {
				obj.Spec.Containers[i].Resources.Requests = make(v1.ResourceList)
			}
			for key, value := range obj.Spec.Containers[i].Resources.Limits {
				if _, exists := obj.Spec.Containers[i].Resources.Requests[key]; !exists {
					obj.Spec.Containers[i].Resources.Requests[key] = value.DeepCopy()
				}
			}
		}
	}
	for i := range obj.Spec.InitContainers {
		if obj.Spec.InitContainers[i].Resources.Limits != nil {
			if obj.Spec.InitContainers[i].Resources.Requests == nil {
				obj.Spec.InitContainers[i].Resources.Requests = make(v1.ResourceList)
			}
			for key, value := range obj.Spec.InitContainers[i].Resources.Limits {
				if _, exists := obj.Spec.InitContainers[i].Resources.Requests[key]; !exists {
					obj.Spec.InitContainers[i].Resources.Requests[key] = value.DeepCopy()
				}
			}
		}
	}

	// Pod 级 request 的默认值必须在容器级默认值填充之后应用。
	// 开启 PodLevelResourcesFixDefaulting 时，这一步会推迟到 PrepareForCreate，
	// 以便在准入 webhook 注入所有容器之后执行，从而看到完整的 Pod。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) &&
		!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourcesFixDefaulting) {
		defaultHugePagePodLimits(obj)
		defaultPodRequests(obj)
	}

	if obj.Spec.EnableServiceLinks == nil {
		enableServiceLinks := v1.DefaultEnableServiceLinks
		obj.Spec.EnableServiceLinks = &enableServiceLinks
	}

	if obj.Spec.HostNetwork {
		defaultHostNetworkPorts(&obj.Spec.Containers)
		defaultHostNetworkPorts(&obj.Spec.InitContainers)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.H2CContainerProbe) {
		defaultHTTPProbeProtocol(obj.Spec.Containers)
		defaultHTTPProbeProtocol(obj.Spec.InitContainers)
		defaultHTTPProbeProtocolEphemeral(obj.Spec.EphemeralContainers)
	}
}

// SetDefaults_PodStatus 保持单值 PodIP 与列表 PodIPs 同步；两者都存在且不一致时以 PodIP 为准。
func SetDefaults_PodStatus(obj *v1.PodStatus) {
	// Keep the singular PodIP and the PodIPs list in sync.
	hasIP := len(obj.PodIP) > 0
	hasIPs := len(obj.PodIPs) > 0
	switch {
	case hasIP && !hasIPs:
		// 用单值字段填充列表。
		obj.PodIPs = []v1.PodIP{{IP: obj.PodIP}}
	case !hasIP && hasIPs:
		// 用列表填充单值字段。
		obj.PodIP = obj.PodIPs[0].IP
	case hasIP && hasIPs && obj.PodIPs[0].IP != obj.PodIP:
		// 两者都存在且不一致时，为兼容旧 kubelet 以 PodIP 为准。
		obj.PodIPs = []v1.PodIP{{IP: obj.PodIP}}
	}
}

// SetDefaults_PodSpec 填充 PodSpec 的通用默认值（DNS 策略、重启策略、安全上下文、
// 终止宽限期、调度器名称等）。新增字段应优先放到 SetDefaults_Pod 而非此处，
// 以免影响所有携带 PodSpec 的对象。
func SetDefaults_PodSpec(obj *v1.PodSpec) {
	// 在此处新增字段会破坏升级测试：
	// https://github.com/kubernetes/kubernetes/issues/69445
	// 大多数情况下，新的默认值字段应加到 SetDefaults_Pod 而不是这里，
	// 这样它只会出现在 Pod 对象上，而不会出现在所有含 PodSpec 字段的对象上。

	// DeprecatedServiceAccount 是 ServiceAccountName 的别名，需保持两者一致；
	// 两者都设置时以 ServiceAccountName 为准。这一处理历史上是在转换阶段完成的，
	// 且产生完全一致的序列化结果，因此不受上面“新增字段需谨慎”的约束。
	if len(obj.ServiceAccountName) == 0 {
		obj.ServiceAccountName = obj.DeprecatedServiceAccount
	}
	obj.DeprecatedServiceAccount = obj.ServiceAccountName
	if obj.DNSPolicy == "" {
		obj.DNSPolicy = v1.DNSClusterFirst
	}
	if obj.RestartPolicy == "" {
		obj.RestartPolicy = v1.RestartPolicyAlways
	}
	// 始终将空的 securityContext 默认初始化，以保持历史行为。
	if obj.SecurityContext == nil {
		obj.SecurityContext = &v1.PodSecurityContext{}
	}
	if obj.TerminationGracePeriodSeconds == nil {
		period := int64(v1.DefaultTerminationGracePeriodSeconds)
		obj.TerminationGracePeriodSeconds = &period
	}
	if obj.SchedulerName == "" {
		obj.SchedulerName = v1.DefaultSchedulerName
	}
}

// SetDefaults_Probe 填充探针的超时、周期与成功/失败阈值默认值。
func SetDefaults_Probe(obj *v1.Probe) {
	if obj.TimeoutSeconds == 0 {
		obj.TimeoutSeconds = 1
	}
	if obj.PeriodSeconds == 0 {
		obj.PeriodSeconds = 10
	}
	if obj.SuccessThreshold == 0 {
		obj.SuccessThreshold = 1
	}
	if obj.FailureThreshold == 0 {
		obj.FailureThreshold = 3
	}
}

// SetDefaults_SecretVolumeSource 为 Secret 卷填充默认文件权限。
func SetDefaults_SecretVolumeSource(obj *v1.SecretVolumeSource) {
	if obj.DefaultMode == nil {
		perm := int32(v1.SecretVolumeSourceDefaultMode)
		obj.DefaultMode = &perm
	}
}

// SetDefaults_ConfigMapVolumeSource 为 ConfigMap 卷填充默认文件权限。
func SetDefaults_ConfigMapVolumeSource(obj *v1.ConfigMapVolumeSource) {
	if obj.DefaultMode == nil {
		perm := int32(v1.ConfigMapVolumeSourceDefaultMode)
		obj.DefaultMode = &perm
	}
}

// SetDefaults_DownwardAPIVolumeSource 为 DownwardAPI 卷填充默认文件权限。
func SetDefaults_DownwardAPIVolumeSource(obj *v1.DownwardAPIVolumeSource) {
	if obj.DefaultMode == nil {
		perm := int32(v1.DownwardAPIVolumeSourceDefaultMode)
		obj.DefaultMode = &perm
	}
}

// SetDefaults_Secret 将未指定类型的 Secret 默认设为 Opaque 类型。
func SetDefaults_Secret(obj *v1.Secret) {
	if obj.Type == "" {
		obj.Type = v1.SecretTypeOpaque
	}
}

// SetDefaults_ProjectedVolumeSource 为投影卷填充默认文件权限。
func SetDefaults_ProjectedVolumeSource(obj *v1.ProjectedVolumeSource) {
	if obj.DefaultMode == nil {
		perm := int32(v1.ProjectedVolumeSourceDefaultMode)
		obj.DefaultMode = &perm
	}
}

// SetDefaults_ServiceAccountTokenProjection 将 ServiceAccount token 的默认有效期设为 1 小时。
func SetDefaults_ServiceAccountTokenProjection(obj *v1.ServiceAccountTokenProjection) {
	hour := int64(time.Hour.Seconds())
	if obj.ExpirationSeconds == nil {
		obj.ExpirationSeconds = &hour
	}
}

// SetDefaults_PersistentVolume 填充 PV 的默认阶段、回收策略与卷模式。
func SetDefaults_PersistentVolume(obj *v1.PersistentVolume) {
	if obj.Status.Phase == "" {
		obj.Status.Phase = v1.VolumePending
	}
	if obj.Spec.PersistentVolumeReclaimPolicy == "" {
		obj.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
	}
	if obj.Spec.VolumeMode == nil {
		obj.Spec.VolumeMode = new(v1.PersistentVolumeMode)
		*obj.Spec.VolumeMode = v1.PersistentVolumeFilesystem
	}
}

// SetDefaults_PersistentVolumeClaim 填充 PVC 的默认阶段。
func SetDefaults_PersistentVolumeClaim(obj *v1.PersistentVolumeClaim) {
	if obj.Status.Phase == "" {
		obj.Status.Phase = v1.ClaimPending
	}
}

// SetDefaults_PersistentVolumeClaimSpec 填充 PVC spec 的默认卷模式。
func SetDefaults_PersistentVolumeClaimSpec(obj *v1.PersistentVolumeClaimSpec) {
	if obj.VolumeMode == nil {
		obj.VolumeMode = new(v1.PersistentVolumeMode)
		*obj.VolumeMode = v1.PersistentVolumeFilesystem
	}
}

// SetDefaults_Endpoints 将 Endpoints 端口的默认协议设为 TCP。
func SetDefaults_Endpoints(obj *v1.Endpoints) {
	for i := range obj.Subsets {
		ss := &obj.Subsets[i]
		for i := range ss.Ports {
			ep := &ss.Ports[i]
			if ep.Protocol == "" {
				ep.Protocol = v1.ProtocolTCP
			}
		}
	}
}

// SetDefaults_HTTPGetAction 将 HTTPGet 探针的默认路径设为 '/'、默认协议设为 HTTP。
func SetDefaults_HTTPGetAction(obj *v1.HTTPGetAction) {
	if obj.Path == "" {
		obj.Path = "/"
	}
	if obj.Scheme == "" {
		obj.Scheme = v1.URISchemeHTTP
	}
}

// SetDefaults_Namespace 为所有 Namespace 添加默认的名称标签。
func SetDefaults_Namespace(obj *v1.Namespace) {
	// 无法为没有名称（使用 generateName）的 Namespace 设置该标签。
	// 这段代码需要与 Namespace Canonicalize 策略（pkg/registry/core/namespace）中的实现保持一致。

	// 注意这在某些情况下可能导致多次特性开关查询，但我们认为其开销可以忽略。
	if len(obj.Name) > 0 {
		if obj.Labels == nil {
			obj.Labels = map[string]string{}
		}
		obj.Labels[v1.LabelMetadataName] = obj.Name
	}
}

// SetDefaults_NamespaceStatus 将 Namespace 的默认阶段设为 Active。
func SetDefaults_NamespaceStatus(obj *v1.NamespaceStatus) {
	if obj.Phase == "" {
		obj.Phase = v1.NamespaceActive
	}
}

// SetDefaults_NodeStatus 在只有 Capacity 时用其填充 Allocatable。
func SetDefaults_NodeStatus(obj *v1.NodeStatus) {
	if obj.Allocatable == nil && obj.Capacity != nil {
		obj.Allocatable = make(v1.ResourceList, len(obj.Capacity))
		for key, value := range obj.Capacity {
			obj.Allocatable[key] = value.DeepCopy()
		}
		obj.Allocatable = obj.Capacity
	}
}

// SetDefaults_ObjectFieldSelector 将字段选择器的默认 API 版本设为 v1。
func SetDefaults_ObjectFieldSelector(obj *v1.ObjectFieldSelector) {
	if obj.APIVersion == "" {
		obj.APIVersion = "v1"
	}
}

// SetDefaults_LimitRangeItem 为容器类型的 LimitRange 项填充默认限制与默认 request：
// 默认 limit 取自 max，默认 request 取自默认 limit 或 min。
func SetDefaults_LimitRangeItem(obj *v1.LimitRangeItem) {
	// 只对容器类型的限制应用默认值。
	if obj.Type == v1.LimitTypeContainer {

		if obj.Default == nil {
			obj.Default = make(v1.ResourceList)
		}
		if obj.DefaultRequest == nil {
			obj.DefaultRequest = make(v1.ResourceList)
		}

		// 未指定默认 limit 但指定了 max 时，用 max 填充默认 limit。
		for key, value := range obj.Max {
			if _, exists := obj.Default[key]; !exists {
				obj.Default[key] = value.DeepCopy()
			}
		}
		// 已指定默认 limit 但未指定默认 request 时，用 limit 填充 request。
		for key, value := range obj.Default {
			if _, exists := obj.DefaultRequest[key]; !exists {
				obj.DefaultRequest[key] = value.DeepCopy()
			}
		}
		// 未指定默认 request 但指定了 min 时，用 min 填充 request。
		for key, value := range obj.Min {
			if _, exists := obj.DefaultRequest[key]; !exists {
				obj.DefaultRequest[key] = value.DeepCopy()
			}
		}
	}
}

// SetDefaults_ConfigMap 保证 ConfigMap 的 Data map 非 nil。
func SetDefaults_ConfigMap(obj *v1.ConfigMap) {
	if obj.Data == nil {
		obj.Data = make(map[string]string)
	}
}

// defaultHostNetworkPorts 在使用主机网络时，把未设置的 HostPort 默认设为 ContainerPort。
func defaultHostNetworkPorts(containers *[]v1.Container) {
	for i := range *containers {
		for j := range (*containers)[i].Ports {
			if (*containers)[i].Ports[j].HostPort == 0 {
				(*containers)[i].Ports[j].HostPort = (*containers)[i].Ports[j].ContainerPort
			}
		}
	}
}

// defaultHTTPGetProtocol 将 HTTPGet 动作的协议默认设为 HTTP/1.1。
func defaultHTTPGetProtocol(action *v1.HTTPGetAction) {
	if action != nil && action.Protocol == nil {
		defaultProtocol := v1.HTTPProtocolHTTP1
		action.Protocol = &defaultProtocol
	}
}

// defaultHTTPProbeProtocol 为普通容器的各类探针与生命周期钩子中的 HTTPGet 动作填充默认协议。
func defaultHTTPProbeProtocol(containers []v1.Container) {
	for i := range containers {
		if containers[i].LivenessProbe != nil {
			defaultHTTPGetProtocol(containers[i].LivenessProbe.HTTPGet)
		}
		if containers[i].ReadinessProbe != nil {
			defaultHTTPGetProtocol(containers[i].ReadinessProbe.HTTPGet)
		}
		if containers[i].StartupProbe != nil {
			defaultHTTPGetProtocol(containers[i].StartupProbe.HTTPGet)
		}
		if containers[i].Lifecycle != nil {
			if containers[i].Lifecycle.PostStart != nil {
				defaultHTTPGetProtocol(containers[i].Lifecycle.PostStart.HTTPGet)
			}
			if containers[i].Lifecycle.PreStop != nil {
				defaultHTTPGetProtocol(containers[i].Lifecycle.PreStop.HTTPGet)
			}
		}
	}
}

// defaultHTTPProbeProtocolEphemeral 为临时容器的各类探针与生命周期钩子中的 HTTPGet 动作填充默认协议。
func defaultHTTPProbeProtocolEphemeral(containers []v1.EphemeralContainer) {
	for i := range containers {
		if containers[i].LivenessProbe != nil {
			defaultHTTPGetProtocol(containers[i].LivenessProbe.HTTPGet)
		}
		if containers[i].ReadinessProbe != nil {
			defaultHTTPGetProtocol(containers[i].ReadinessProbe.HTTPGet)
		}
		if containers[i].StartupProbe != nil {
			defaultHTTPGetProtocol(containers[i].StartupProbe.HTTPGet)
		}
		if containers[i].Lifecycle != nil {
			if containers[i].Lifecycle.PostStart != nil {
				defaultHTTPGetProtocol(containers[i].Lifecycle.PostStart.HTTPGet)
			}
			if containers[i].Lifecycle.PreStop != nil {
				defaultHTTPGetProtocol(containers[i].Lifecycle.PreStop.HTTPGet)
			}
		}
	}
}

// SetDefaults_HostPathVolumeSource 将 HostPath 卷的默认类型设为 Unset。
func SetDefaults_HostPathVolumeSource(obj *v1.HostPathVolumeSource) {
	typeVol := v1.HostPathUnset
	if obj.Type == nil {
		obj.Type = &typeVol
	}
}

// SetDefaults_PodLogOptions 在 PodLogsQuerySplitStreams 特性开启时，将日志流默认设为 All。
func SetDefaults_PodLogOptions(obj *v1.PodLogOptions) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		if obj.Stream == nil {
			obj.Stream = ptr.To(v1.LogStreamAll)
		}
	}
}

// defaultPodRequests 仅在设置了 Pod 级 limit 时，按以下规则填充 Pod 级 request：
// 1. 若至少一个容器（普通/init/sidecar）设置了 request，则 Pod 级 request 取所有容器有效 request 之和；
// 2. 若没有容器设置 request，则 Pod 级 request 取 Pod 级 limit。
// 这一行为既保证 Pod 级资源统计的一致性，又兼容容器级的资源声明，
// 详见 KEP-2837：https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2837-pod-level-resource-spec/README.md#proposed-validation--defaulting-rules
// TODO(ndixita): Remove defaultPodRequests once PodLevelResourcesFixDefaulting feature gate is GA.
func defaultPodRequests(obj *v1.Pod) {
	// 只有在 Pod 级资源已部分指定时才填充默认值。
	if obj.Spec.Resources == nil {
		return
	}

	if len(obj.Spec.Resources.Limits) == 0 {
		return
	}

	var podReqs v1.ResourceList
	podReqs = obj.Spec.Resources.Requests
	if podReqs == nil {
		podReqs = make(v1.ResourceList)
	}

	aggrCtrReqs := resourcehelper.AggregateContainerRequests(obj, resourcehelper.PodResourcesOptions{})

	// 若容器指定了某资源的 request（PodLevelResources 特性支持）而 Pod 级 request 未设置，
	// 则将 Pod 级 request 设为所有容器对该资源的有效 request 之和。
	for key, aggrCtrLim := range aggrCtrReqs {
		// 对可超卖资源，用容器 request 的聚合值填充 Pod 级 request。
		if _, exists := podReqs[key]; !exists && resourcehelper.IsSupportedPodLevelResource(key) && corev1helper.IsOvercommitAllowed(key) {
			podReqs[key] = aggrCtrLim.DeepCopy()
		}
	}

	// 若没有容器指定某资源的 request，且 Pod 级 limit 存在，则 Pod 级 request 取 Pod 级 limit。
	// Pod 级大页 request 的默认值依赖 defaultHugePagePodLimits：
	// 后者若定义了 limit，这里就会填充对应的 request。
	for key, podLim := range obj.Spec.Resources.Limits {
		if _, exists := podReqs[key]; !exists && resourcehelper.IsSupportedPodLevelResource(key) {
			podReqs[key] = podLim.DeepCopy()
		}
	}

	// 只有在收集容器级 request 与 Pod 级 limit 后结果非空时，才写回 PodSpec 的 Pod 级 request。
	if len(podReqs) > 0 {
		obj.Spec.Resources.Requests = podReqs
	}
}

// defaultHugePagePodLimits 仅在容器设置了大页 limit 而 Pod 级未设置时，
// 用所有容器大页 limit 的聚合值填充 Pod 级 limit。
// TODO(ndixita): Remove defaultHugePagePodLimits once PodLevelResourcesFixDefaulting feature gate is GA.
func defaultHugePagePodLimits(pod *v1.Pod) {
	// 只有在 Pod 级资源已部分指定时才填充大页 limit 默认值。
	if pod.Spec.Resources == nil {
		return
	}

	if len(pod.Spec.Resources.Limits) == 0 && len(pod.Spec.Resources.Requests) == 0 {
		return
	}

	var podLims v1.ResourceList
	podLims = pod.Spec.Resources.Limits
	if podLims == nil {
		podLims = make(v1.ResourceList)
	}

	aggrCtrLims := resourcehelper.AggregateContainerLimits(pod, resourcehelper.PodResourcesOptions{})

	// 若容器指定了大页 limit 而 Pod 级未指定该资源的 limit，
	// 则用所有容器大页 limit 的聚合值填充 Pod 级 limit。
	for key, aggrCtrLim := range aggrCtrLims {
		if !resourcehelper.IsSupportedPodLevelResource(key) || !corev1helper.IsHugePageResourceName(key) {
			continue
		}

		// 已存在大页 request 时不填充 Pod 级大页 limit，避免引入不一致。
		if _, exists := pod.Spec.Resources.Requests[key]; exists {
			continue
		}

		if _, exists := podLims[key]; !exists {
			podLims[key] = aggrCtrLim.DeepCopy()
		}
	}

	// 只有在收集容器级 limit 与大页 Pod 级 limit 后结果非空时，才写回 PodSpec。
	if len(podLims) > 0 {
		pod.Spec.Resources.Limits = podLims
	}
}
