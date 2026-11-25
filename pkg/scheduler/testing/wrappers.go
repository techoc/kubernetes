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

package testing

import (
	"fmt"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

var zero int64

// NodeSelectorWrapper 包装一个 NodeSelector。
type NodeSelectorWrapper struct{ v1.NodeSelector }

// MakeNodeSelector 创建 NodeSelector 包装器。
func MakeNodeSelector() *NodeSelectorWrapper {
	return &NodeSelectorWrapper{v1.NodeSelector{}}
}

// NodeSelectorType 指定匹配表达式写入 MatchExpressions 还是 MatchFields。
type NodeSelectorType int

const (
	// NodeSelectorTypeMatchExpressions 表示将匹配表达式写入 MatchExpressions。
	NodeSelectorTypeMatchExpressions NodeSelectorType = iota
	// NodeSelectorTypeMatchFields 表示将匹配表达式写入 MatchFields。
	NodeSelectorTypeMatchFields
)

// In 向内部 nodeSelector 追加一个使用 IN 操作符的 selectorTerm。
// NOTE: 追加的 selectorTerm 之间是“或”关系。
func (s *NodeSelectorWrapper) In(key string, vals []string, t NodeSelectorType) *NodeSelectorWrapper {
	expression := v1.NodeSelectorRequirement{
		Key:      key,
		Operator: v1.NodeSelectorOpIn,
		Values:   vals,
	}
	selectorTerm := v1.NodeSelectorTerm{}
	if t == NodeSelectorTypeMatchExpressions {
		selectorTerm.MatchExpressions = append(selectorTerm.MatchExpressions, expression)
	} else {
		selectorTerm.MatchFields = append(selectorTerm.MatchFields, expression)
	}
	s.NodeSelectorTerms = append(s.NodeSelectorTerms, selectorTerm)
	return s
}

// NotIn 向内部 nodeSelector 追加一个使用 NotIn 操作符的 selectorTerm。
func (s *NodeSelectorWrapper) NotIn(key string, vals []string) *NodeSelectorWrapper {
	expression := v1.NodeSelectorRequirement{
		Key:      key,
		Operator: v1.NodeSelectorOpNotIn,
		Values:   vals,
	}
	selectorTerm := v1.NodeSelectorTerm{}
	selectorTerm.MatchExpressions = append(selectorTerm.MatchExpressions, expression)
	s.NodeSelectorTerms = append(s.NodeSelectorTerms, selectorTerm)
	return s
}

// Obj 返回内部的 NodeSelector。
func (s *NodeSelectorWrapper) Obj() *v1.NodeSelector {
	return &s.NodeSelector
}

// LabelSelectorWrapper 包装一个 LabelSelector。
type LabelSelectorWrapper struct{ metav1.LabelSelector }

// MakeLabelSelector 创建 LabelSelector 包装器。
func MakeLabelSelector() *LabelSelectorWrapper {
	return &LabelSelectorWrapper{metav1.LabelSelector{}}
}

// Label 向内部 LabelSelector 添加一个 {k,v} 标签对。
func (s *LabelSelectorWrapper) Label(k, v string) *LabelSelectorWrapper {
	if s.MatchLabels == nil {
		s.MatchLabels = make(map[string]string)
	}
	s.MatchLabels[k] = v
	return s
}

// In 向内部 labelSelector 追加一个使用 In 操作符的 matchExpression。
func (s *LabelSelectorWrapper) In(key string, vals []string) *LabelSelectorWrapper {
	expression := metav1.LabelSelectorRequirement{
		Key:      key,
		Operator: metav1.LabelSelectorOpIn,
		Values:   vals,
	}
	s.MatchExpressions = append(s.MatchExpressions, expression)
	return s
}

// NotIn 向内部 labelSelector 追加一个使用 NotIn 操作符的 matchExpression。
func (s *LabelSelectorWrapper) NotIn(key string, vals []string) *LabelSelectorWrapper {
	expression := metav1.LabelSelectorRequirement{
		Key:      key,
		Operator: metav1.LabelSelectorOpNotIn,
		Values:   vals,
	}
	s.MatchExpressions = append(s.MatchExpressions, expression)
	return s
}

// Exists 向内部 labelSelector 追加一个使用 Exists 操作符的 matchExpression。
func (s *LabelSelectorWrapper) Exists(k string) *LabelSelectorWrapper {
	expression := metav1.LabelSelectorRequirement{
		Key:      k,
		Operator: metav1.LabelSelectorOpExists,
	}
	s.MatchExpressions = append(s.MatchExpressions, expression)
	return s
}

// NotExist 向内部 labelSelector 追加一个使用 DoesNotExist 操作符的 matchExpression。
func (s *LabelSelectorWrapper) NotExist(k string) *LabelSelectorWrapper {
	expression := metav1.LabelSelectorRequirement{
		Key:      k,
		Operator: metav1.LabelSelectorOpDoesNotExist,
	}
	s.MatchExpressions = append(s.MatchExpressions, expression)
	return s
}

// Obj 返回内部的 LabelSelector。
func (s *LabelSelectorWrapper) Obj() *metav1.LabelSelector {
	return &s.LabelSelector
}

// ContainerWrapper 包装一个 Container。
type ContainerWrapper struct{ v1.Container }

// MakeContainer 创建 Container 包装器。
func MakeContainer() *ContainerWrapper {
	return &ContainerWrapper{v1.Container{}}
}

// Obj 返回内部的 Container。
func (c *ContainerWrapper) Obj() v1.Container {
	return c.Container
}

// Name 将内部 Container 的名称设为 n。
func (c *ContainerWrapper) Name(n string) *ContainerWrapper {
	c.Container.Name = n
	return c
}

// Image 将内部 Container 的镜像设为 image。
func (c *ContainerWrapper) Image(image string) *ContainerWrapper {
	c.Container.Image = image
	return c
}

// HostPort 将内部 Container 的主机端口设为 hostPort。
func (c *ContainerWrapper) HostPort(hostPort int32) *ContainerWrapper {
	c.Container.Ports = []v1.ContainerPort{{HostPort: hostPort}}
	return c
}

// ContainerPort 将内部 Container 的端口列表设为 ports。
func (c *ContainerWrapper) ContainerPort(ports []v1.ContainerPort) *ContainerWrapper {
	c.Container.Ports = ports
	return c
}

// Resources 将容器的 requests 和 limits 都设为给定的资源表。
func (c *ContainerWrapper) Resources(resMap map[v1.ResourceName]string) *ContainerWrapper {
	res := v1.ResourceList{}
	for k, v := range resMap {
		res[k] = resource.MustParse(v)
	}
	c.Container.Resources = v1.ResourceRequirements{
		Requests: res,
		Limits:   res,
	}
	return c
}

// ResourceRequests 将容器的 requests 设为给定的资源表。
func (c *ContainerWrapper) ResourceRequests(reqMap map[v1.ResourceName]string) *ContainerWrapper {
	res := v1.ResourceList{}
	for k, v := range reqMap {
		res[k] = resource.MustParse(v)
	}
	c.Container.Resources = v1.ResourceRequirements{
		Requests: res,
	}
	return c
}

// ResourceLimits 将容器的 limits 设为给定的资源表。
func (c *ContainerWrapper) ResourceLimits(limMap map[v1.ResourceName]string) *ContainerWrapper {
	res := v1.ResourceList{}
	for k, v := range limMap {
		res[k] = resource.MustParse(v)
	}
	c.Container.Resources = v1.ResourceRequirements{
		Limits: res,
	}
	return c
}

// RestartPolicy 将容器的 restartPolicy 设为给定的策略。
func (c *ContainerWrapper) RestartPolicy(restartPolicy v1.ContainerRestartPolicy) *ContainerWrapper {
	c.Container.RestartPolicy = &restartPolicy
	return c
}

// PodDisruptionBudgetWrapper 包装一个 PodDisruptionBudget。
type PodDisruptionBudgetWrapper struct {
	policy.PodDisruptionBudget
}

// MakePDB 创建 PodDisruptionBudget 包装器。
func MakePDB() *PodDisruptionBudgetWrapper {
	return &PodDisruptionBudgetWrapper{policy.PodDisruptionBudget{}}
}

// Obj 返回内部的 PodDisruptionBudget。
func (p *PodDisruptionBudgetWrapper) Obj() *policy.PodDisruptionBudget {
	return &p.PodDisruptionBudget
}

// Name 将内部 PodDisruptionBudget 的名称设为 name。
func (p *PodDisruptionBudgetWrapper) Name(name string) *PodDisruptionBudgetWrapper {
	p.SetName(name)
	return p
}

// Namespace 将内部 PodDisruptionBudget 的命名空间设为 namespace。
func (p *PodDisruptionBudgetWrapper) Namespace(namespace string) *PodDisruptionBudgetWrapper {
	p.SetNamespace(namespace)
	return p
}

// MinAvailable 将内部 PodDisruptionBudget.Spec.MinAvailable 设为 minAvailable。
func (p *PodDisruptionBudgetWrapper) MinAvailable(minAvailable string) *PodDisruptionBudgetWrapper {
	p.Spec.MinAvailable = &intstr.IntOrString{
		Type:   intstr.String,
		StrVal: minAvailable,
	}
	return p
}

// MatchLabel 向内部 PodDisruptionBudget.Spec.Selector.MatchLabels 添加一个 {key,value}。
func (p *PodDisruptionBudgetWrapper) MatchLabel(key, value string) *PodDisruptionBudgetWrapper {
	selector := p.Spec.Selector
	if selector == nil {
		selector = &metav1.LabelSelector{}
	}
	matchLabels := selector.MatchLabels
	if matchLabels == nil {
		matchLabels = map[string]string{}
	}
	matchLabels[key] = value
	selector.MatchLabels = matchLabels
	p.Spec.Selector = selector
	return p
}

// DisruptionsAllowed 将内部 PodDisruptionBudget.Status.DisruptionsAllowed 设为 disruptionsAllowed。
func (p *PodDisruptionBudgetWrapper) DisruptionsAllowed(disruptionsAllowed int32) *PodDisruptionBudgetWrapper {
	p.Status.DisruptionsAllowed = disruptionsAllowed
	return p
}

// PodWrapper 包装一个 Pod。
type PodWrapper struct{ v1.Pod }

// MakePod 创建 Pod 包装器。
func MakePod() *PodWrapper {
	return &PodWrapper{v1.Pod{}}
}

// Obj 返回内部的 Pod。
func (p *PodWrapper) Obj() *v1.Pod {
	return &p.Pod
}

// Name 将内部 pod 的名称设为 s。
func (p *PodWrapper) Name(s string) *PodWrapper {
	p.SetName(s)
	return p
}

// GenerateName 将内部 pod 的 generateName 设为 s。
func (p *PodWrapper) GenerateName(s string) *PodWrapper {
	p.SetGenerateName(s)
	return p
}

// UID 将内部 pod 的 UID 设为 s。
func (p *PodWrapper) UID(s string) *PodWrapper {
	p.SetUID(types.UID(s))
	return p
}

// SchedulerName 将内部 pod 的调度器名称设为 s。
func (p *PodWrapper) SchedulerName(s string) *PodWrapper {
	p.Spec.SchedulerName = s
	return p
}

// Namespace 将内部 pod 的命名空间设为 s。
func (p *PodWrapper) Namespace(s string) *PodWrapper {
	p.SetNamespace(s)
	return p
}

// Resources 在 pod 级别设置 requests 和 limits。
func (p *PodWrapper) Resources(resources v1.ResourceRequirements) *PodWrapper {
	p.Spec.Resources = &resources
	return p
}

// NodeAffinity 将内部 pod 的 nodeAffinity 设为 nodeAffinity。
// Affinity 为 nil 时先初始化，避免调用方自行构造整条嵌套路径。
func (p *PodWrapper) NodeAffinity(nodeAffinity *v1.NodeAffinity) *PodWrapper {
	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &v1.Affinity{}
	}
	p.Spec.Affinity.NodeAffinity = nodeAffinity
	return p
}

// OwnerReference 更新 pod 的属主控制器。
func (p *PodWrapper) OwnerReference(name string, gvk schema.GroupVersionKind) *PodWrapper {
	p.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: gvk.GroupVersion().String(),
			Kind:       gvk.Kind,
			Name:       name,
			Controller: ptr.To(true),
		},
	}
	return p
}

// Container 向内部 pod 的 PodSpec 追加一个容器，镜像为 s，名称按序号生成。
func (p *PodWrapper) Container(s string) *PodWrapper {
	name := fmt.Sprintf("con%d", len(p.Spec.Containers))
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name(name).Image(s).Obj())
	return p
}

// Containers 将内部 pod 的 PodSpec 中的容器列表设为 containers。
func (p *PodWrapper) Containers(containers []v1.Container) *PodWrapper {
	p.Spec.Containers = containers
	return p
}

// PodResourceClaims 向内部 pod 的 PodSpec 追加 PodResourceClaims。
func (p *PodWrapper) PodResourceClaims(podResourceClaims ...v1.PodResourceClaim) *PodWrapper {
	p.Spec.ResourceClaims = append(p.Spec.ResourceClaims, podResourceClaims...)
	return p
}

// ResourceClaimStatuses 向内部 pod 的 PodStatus 追加 claim 状态。
func (p *PodWrapper) ResourceClaimStatuses(resourceClaimStatuses ...v1.PodResourceClaimStatus) *PodWrapper {
	p.Status.ResourceClaimStatuses = append(p.Status.ResourceClaimStatuses, resourceClaimStatuses...)
	return p
}

// ExendedResourceClaimStatus 设置内部 pod 的 PodStatus 中的 ExtendedResourceClaimStatus。
func (p *PodWrapper) ExtendedResourceClaimStatus(extendedResourceClaimStatus *v1.PodExtendedResourceClaimStatus) *PodWrapper {
	p.Status.ExtendedResourceClaimStatus = extendedResourceClaimStatus
	return p
}

// Priority 将内部 pod 的 PodSpec 中的优先级设为 val。
func (p *PodWrapper) Priority(val int32) *PodWrapper {
	p.Spec.Priority = &val
	return p
}

// CreationTimestamp 设置内部 pod 的 CreationTimestamp。
func (p *PodWrapper) CreationTimestamp(t metav1.Time) *PodWrapper {
	p.ObjectMeta.CreationTimestamp = t
	return p
}

// Terminating 将内部 pod 的 deletionTimestamp 设为当前时间。
func (p *PodWrapper) Terminating() *PodWrapper {
	now := metav1.Now()
	p.DeletionTimestamp = &now
	return p
}

// ZeroTerminationGracePeriod 将内部 pod 的 TerminationGracePeriodSeconds 设为 0。
func (p *PodWrapper) ZeroTerminationGracePeriod() *PodWrapper {
	p.Spec.TerminationGracePeriodSeconds = &zero
	return p
}

// TerminationGracePeriodSeconds 设置内部 pod 的 TerminationGracePeriodSeconds。
func (p *PodWrapper) TerminationGracePeriodSeconds(s int64) *PodWrapper {
	p.Spec.TerminationGracePeriodSeconds = &s
	return p
}

// Node 将内部 pod 的 nodeName 设为 s。
func (p *PodWrapper) Node(s string) *PodWrapper {
	p.Spec.NodeName = s
	return p
}

// Tolerations 将内部 pod 的 tolerations 设为 tolerations。
func (p *PodWrapper) Tolerations(tolerations []v1.Toleration) *PodWrapper {
	p.Spec.Tolerations = tolerations
	return p
}

// NodeSelector 将内部 pod 的 nodeSelector 设为 m。
func (p *PodWrapper) NodeSelector(m map[string]string) *PodWrapper {
	p.Spec.NodeSelector = m
	return p
}

// NodeAffinityIn 创建一个 HARD 类型的 node affinity（操作符 In）并注入内部 pod。
func (p *PodWrapper) NodeAffinityIn(key string, vals []string, t NodeSelectorType) *PodWrapper {
	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &v1.Affinity{}
	}
	if p.Spec.Affinity.NodeAffinity == nil {
		p.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	nodeSelector := MakeNodeSelector().In(key, vals, t).Obj()
	p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return p
}

// NodeAffinityNotIn 创建一个 HARD 类型的 node affinity（使用 MatchExpressions 和 NotIn 操作符）并注入内部 pod。
func (p *PodWrapper) NodeAffinityNotIn(key string, vals []string) *PodWrapper {
	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &v1.Affinity{}
	}
	if p.Spec.Affinity.NodeAffinity == nil {
		p.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	nodeSelector := MakeNodeSelector().NotIn(key, vals).Obj()
	p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return p
}

// StartTime 将内部 pod 的 .status.startTime 设为 t。
func (p *PodWrapper) StartTime(t metav1.Time) *PodWrapper {
	p.Status.StartTime = &t
	return p
}

// NominatedNodeName 将内部 pod 的 .Status.NominatedNodeName 设为 n。
func (p *PodWrapper) NominatedNodeName(n string) *PodWrapper {
	p.Status.NominatedNodeName = n
	return p
}

// Phase 将内部 pod 的 .status.Phase 设为 phase。
func (p *PodWrapper) Phase(phase v1.PodPhase) *PodWrapper {
	p.Status.Phase = phase
	return p
}

// Condition 向 .Status.Conditions 追加一个 condition(Type, Status, Reason)。
func (p *PodWrapper) Condition(t v1.PodConditionType, s v1.ConditionStatus, r string) *PodWrapper {
	p.Status.Conditions = append(p.Status.Conditions, v1.PodCondition{Type: t, Status: s, Reason: r})
	return p
}

// Conditions 将内部 pod 的 .status.Conditions 设为 conditions。
func (p *PodWrapper) Conditions(conditions []v1.PodCondition) *PodWrapper {
	p.Status.Conditions = append(p.Status.Conditions, conditions...)
	return p
}

// Toleration 创建一个 toleration（操作符 Exists）并注入内部 pod。
func (p *PodWrapper) Toleration(key string) *PodWrapper {
	p.Spec.Tolerations = append(p.Spec.Tolerations, v1.Toleration{
		Key:      key,
		Operator: v1.TolerationOpExists,
	})
	return p
}

// HostPort 创建一个带 hostPort 的容器并注入内部 pod。
func (p *PodWrapper) HostPort(port int32) *PodWrapper {
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name("container").Image("pause").HostPort(port).Obj())
	return p
}

// ContainerPort 创建一个带指定端口的容器并注入内部 pod。
func (p *PodWrapper) ContainerPort(ports []v1.ContainerPort) *PodWrapper {
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name("container").Image("pause").ContainerPort(ports).Obj())
	return p
}

// InitContainerPort 创建一个带指定端口的 initContainer 并注入内部 pod；sidecar 为真时将其设为常驻容器。
func (p *PodWrapper) InitContainerPort(sidecar bool, ports []v1.ContainerPort) *PodWrapper {
	c := MakeContainer().
		Name("init-container").
		Image("pause").
		ContainerPort(ports)
	if sidecar {
		c.RestartPolicy(v1.ContainerRestartPolicyAlways)
	}
	p.Spec.InitContainers = append(p.Spec.InitContainers, c.Obj())
	return p
}

// PVC 创建一个使用 PVC 的 Volume 并注入内部 pod。
func (p *PodWrapper) PVC(name string) *PodWrapper {
	p.Spec.Volumes = append(p.Spec.Volumes, v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: name},
		},
	})
	return p
}

// Volume 创建一个 volume 并注入内部 pod。
func (p *PodWrapper) Volume(volume v1.Volume) *PodWrapper {
	p.Spec.Volumes = append(p.Spec.Volumes, volume)
	return p
}

// Volumes 将内部 pod 的 volume 列表设为 volumes。
func (p *PodWrapper) Volumes(volumes []v1.Volume) *PodWrapper {
	p.Spec.Volumes = volumes
	return p
}

// SchedulingGates 向内部 pod 追加调度门禁。
func (p *PodWrapper) SchedulingGates(gates []string) *PodWrapper {
	for _, gate := range gates {
		p.Spec.SchedulingGates = append(p.Spec.SchedulingGates, v1.PodSchedulingGate{Name: gate})
	}
	return p
}

// ResourceVersion 设置内部 pod 的 ResourceVersion。
func (p *PodWrapper) ResourceVersion(version string) *PodWrapper {
	p.ObjectMeta.ResourceVersion = version
	return p
}

// PodAffinityKind 表示 PodAffinity 的不同类型。
type PodAffinityKind int

const (
	// NilPodAffinity 是空操作，不应用任何 PodAffinity。
	NilPodAffinity PodAffinityKind = iota
	// PodAffinityWithRequiredReq 向 pod.spec.affinity.PodAffinity 应用 HARD 约束。
	PodAffinityWithRequiredReq
	// PodAffinityWithPreferredReq 向 pod.spec.affinity.PodAffinity 应用 SOFT 约束。
	PodAffinityWithPreferredReq
	// PodAffinityWithRequiredPreferredReq 向 pod.spec.affinity.PodAffinity 同时应用 HARD 和 SOFT 约束。
	PodAffinityWithRequiredPreferredReq
	// PodAntiAffinityWithRequiredReq 向 pod.spec.affinity.PodAntiAffinity 应用 HARD 约束。
	PodAntiAffinityWithRequiredReq
	// PodAntiAffinityWithPreferredReq 向 pod.spec.affinity.PodAntiAffinity 应用 SOFT 约束。
	PodAntiAffinityWithPreferredReq
	// PodAntiAffinityWithRequiredPreferredReq 向 pod.spec.affinity.PodAntiAffinity 同时应用 HARD 和 SOFT 约束。
	PodAntiAffinityWithRequiredPreferredReq
)

// PodAffinity 用拓扑键和标签选择器创建 PodAffinity 并注入内部 pod。
func (p *PodWrapper) PodAffinity(topologyKey string, labelSelector *metav1.LabelSelector, kind PodAffinityKind) *PodWrapper {
	if kind == NilPodAffinity {
		return p
	}

	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &v1.Affinity{}
	}
	if p.Spec.Affinity.PodAffinity == nil {
		p.Spec.Affinity.PodAffinity = &v1.PodAffinity{}
	}
	term := v1.PodAffinityTerm{LabelSelector: labelSelector, TopologyKey: topologyKey}
	switch kind {
	case PodAffinityWithRequiredReq:
		p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			term,
		)
	case PodAffinityWithPreferredReq:
		p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			v1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: term},
		)
	case PodAffinityWithRequiredPreferredReq:
		p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			term,
		)
		p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			v1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: term},
		)
	}
	return p
}

// PodAntiAffinity 用拓扑键和标签选择器创建 PodAntiAffinity 并注入内部 pod。
func (p *PodWrapper) PodAntiAffinity(topologyKey string, labelSelector *metav1.LabelSelector, kind PodAffinityKind) *PodWrapper {
	if kind == NilPodAffinity {
		return p
	}

	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &v1.Affinity{}
	}
	if p.Spec.Affinity.PodAntiAffinity == nil {
		p.Spec.Affinity.PodAntiAffinity = &v1.PodAntiAffinity{}
	}
	term := v1.PodAffinityTerm{LabelSelector: labelSelector, TopologyKey: topologyKey}
	switch kind {
	case PodAntiAffinityWithRequiredReq:
		p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			term,
		)
	case PodAntiAffinityWithPreferredReq:
		p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			v1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: term},
		)
	case PodAntiAffinityWithRequiredPreferredReq:
		p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			term,
		)
		p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			v1.WeightedPodAffinityTerm{Weight: 1, PodAffinityTerm: term},
		)
	}
	return p
}

// PodAffinityExists 创建操作符为 Exists 的 PodAffinity 并注入内部 pod。
func (p *PodWrapper) PodAffinityExists(labelKey, topologyKey string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().Exists(labelKey).Obj()
	p.PodAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAntiAffinityExists 创建操作符为 Exists 的 PodAntiAffinity 并注入内部 pod。
func (p *PodWrapper) PodAntiAffinityExists(labelKey, topologyKey string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().Exists(labelKey).Obj()
	p.PodAntiAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAffinityNotExists 创建操作符为 NotExists 的 PodAffinity 并注入内部 pod。
func (p *PodWrapper) PodAffinityNotExists(labelKey, topologyKey string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().NotExist(labelKey).Obj()
	p.PodAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAntiAffinityNotExists 创建操作符为 NotExists 的 PodAntiAffinity 并注入内部 pod。
func (p *PodWrapper) PodAntiAffinityNotExists(labelKey, topologyKey string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().NotExist(labelKey).Obj()
	p.PodAntiAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAffinityIn 创建操作符为 In 的 PodAffinity 并注入内部 pod。
func (p *PodWrapper) PodAffinityIn(labelKey, topologyKey string, vals []string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().In(labelKey, vals).Obj()
	p.PodAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAntiAffinityIn 创建操作符为 In 的 PodAntiAffinity 并注入内部 pod。
func (p *PodWrapper) PodAntiAffinityIn(labelKey, topologyKey string, vals []string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().In(labelKey, vals).Obj()
	p.PodAntiAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAffinityNotIn 创建操作符为 NotIn 的 PodAffinity 并注入内部 pod。
func (p *PodWrapper) PodAffinityNotIn(labelKey, topologyKey string, vals []string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().NotIn(labelKey, vals).Obj()
	p.PodAffinity(topologyKey, labelSelector, kind)
	return p
}

// PodAntiAffinityNotIn 创建操作符为 NotIn 的 PodAntiAffinity 并注入内部 pod。
func (p *PodWrapper) PodAntiAffinityNotIn(labelKey, topologyKey string, vals []string, kind PodAffinityKind) *PodWrapper {
	labelSelector := MakeLabelSelector().NotIn(labelKey, vals).Obj()
	p.PodAntiAffinity(topologyKey, labelSelector, kind)
	return p
}

// SpreadConstraint 构造一个 TopologySpreadConstraint 并注入内部 pod。
func (p *PodWrapper) SpreadConstraint(maxSkew int, tpKey string, mode v1.UnsatisfiableConstraintAction, selector *metav1.LabelSelector, minDomains *int32, nodeAffinityPolicy, nodeTaintsPolicy *v1.NodeInclusionPolicy, matchLabelKeys []string) *PodWrapper {
	c := v1.TopologySpreadConstraint{
		MaxSkew:            int32(maxSkew),
		TopologyKey:        tpKey,
		WhenUnsatisfiable:  mode,
		LabelSelector:      selector,
		MinDomains:         minDomains,
		NodeAffinityPolicy: nodeAffinityPolicy,
		NodeTaintsPolicy:   nodeTaintsPolicy,
		MatchLabelKeys:     matchLabelKeys,
	}
	p.Spec.TopologySpreadConstraints = append(p.Spec.TopologySpreadConstraints, c)
	return p
}

// Label 向内部 pod 的标签添加一个 {k,v} 对。
func (p *PodWrapper) Label(k, v string) *PodWrapper {
	if p.ObjectMeta.Labels == nil {
		p.ObjectMeta.Labels = make(map[string]string)
	}
	p.ObjectMeta.Labels[k] = v
	return p
}

// Labels 将 labels 中的所有 {k,v} 对添加到内部 pod 的标签。
func (p *PodWrapper) Labels(labels map[string]string) *PodWrapper {
	for k, v := range labels {
		p.Label(k, v)
	}
	return p
}

// Annotation 向内部 pod 的注解添加一个 {k,v} 对。
func (p *PodWrapper) Annotation(key, value string) *PodWrapper {
	metav1.SetMetaDataAnnotation(&p.ObjectMeta, key, value)
	return p
}

// Annotations 将 annotations 中的所有 {k,v} 对添加到内部 pod 的注解。
func (p *PodWrapper) Annotations(annotations map[string]string) *PodWrapper {
	for k, v := range annotations {
		p.Annotation(k, v)
	}
	return p
}

// Res 用给定的资源表向内部 pod 添加一个新容器。
func (p *PodWrapper) Res(resMap map[v1.ResourceName]string) *PodWrapper {
	if len(resMap) == 0 {
		return p
	}

	name := fmt.Sprintf("con%d", len(p.Spec.Containers))
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name(name).Image(imageutils.GetPauseImageName()).Resources(resMap).Obj())
	return p
}

// Resources 在 pod 级别设置 requests。
func (p *PodWrapper) PodLevelResourceRequests(reqMap map[v1.ResourceName]string) *PodWrapper {
	if len(reqMap) == 0 {
		return p
	}

	res := v1.ResourceList{}
	for k, v := range reqMap {
		res[k] = resource.MustParse(v)
	}
	p.Spec.Resources = &v1.ResourceRequirements{
		Requests: res,
	}
	return p
}

// Req 用给定的 requests 资源表向内部 pod 添加一个新容器。
func (p *PodWrapper) Req(reqMap map[v1.ResourceName]string) *PodWrapper {
	if len(reqMap) == 0 {
		return p
	}

	name := fmt.Sprintf("con%d", len(p.Spec.Containers))
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name(name).Image(imageutils.GetPauseImageName()).ResourceRequests(reqMap).Obj())
	return p
}

// Lim 用给定的 limits 资源表向内部 pod 添加一个新容器。
func (p *PodWrapper) Lim(limMap map[v1.ResourceName]string) *PodWrapper {
	if len(limMap) == 0 {
		return p
	}

	name := fmt.Sprintf("con%d", len(p.Spec.Containers))
	p.Spec.Containers = append(p.Spec.Containers, MakeContainer().Name(name).Image(imageutils.GetPauseImageName()).ResourceLimits(limMap).Obj())
	return p
}

// InitReq 用给定的资源表向内部 pod 添加一个新的 init container。
func (p *PodWrapper) InitReq(resMap map[v1.ResourceName]string) *PodWrapper {
	if len(resMap) == 0 {
		return p
	}

	name := fmt.Sprintf("init-con%d", len(p.Spec.InitContainers))
	p.Spec.InitContainers = append(p.Spec.InitContainers, MakeContainer().Name(name).Image(imageutils.GetPauseImageName()).Resources(resMap).Obj())
	return p
}

// SidecarReq 用给定的资源表向内部 pod 添加一个新的 sidecar container。
func (p *PodWrapper) SidecarReq(resMap map[v1.ResourceName]string) *PodWrapper {
	if len(resMap) == 0 {
		return p
	}

	name := fmt.Sprintf("sidecar-con%d", len(p.Spec.InitContainers))
	p.Spec.InitContainers = append(p.Spec.InitContainers, MakeContainer().Name(name).Image(imageutils.GetPauseImageName()).RestartPolicy(v1.ContainerRestartPolicyAlways).Resources(resMap).Obj())
	return p
}

// PreemptionPolicy 将内部 pod 的抢占策略设为给定值。
func (p *PodWrapper) PreemptionPolicy(policy v1.PreemptionPolicy) *PodWrapper {
	p.Spec.PreemptionPolicy = &policy
	return p
}

// Overhead 将内部 pod 的 Overhead 设为给定的 ResourceList。
func (p *PodWrapper) Overhead(rl v1.ResourceList) *PodWrapper {
	p.Spec.Overhead = rl
	return p
}

// PodGroupName 将内部 pod 的 PodGroupName 设为 name。
func (p *PodWrapper) PodGroupName(name string) *PodWrapper {
	p.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
		PodGroupName: new(name),
	}
	return p
}

// NodeWrapper 包装一个 Node。
type NodeWrapper struct{ v1.Node }

// MakeNode 创建 Node 包装器。
func MakeNode() *NodeWrapper {
	w := &NodeWrapper{v1.Node{}}
	return w.Capacity(nil)
}

// Obj 返回内部的 Node。
func (n *NodeWrapper) Obj() *v1.Node {
	return &n.Node
}

// Name 将内部 node 的名称设为 s。
func (n *NodeWrapper) Name(s string) *NodeWrapper {
	n.SetName(s)
	return n
}

// UID 将内部 node 的 UID 设为 s。
func (n *NodeWrapper) UID(s string) *NodeWrapper {
	n.SetUID(types.UID(s))
	return n
}

// Label 向内部 node 添加一个 {k,v} 标签对。
func (n *NodeWrapper) Label(k, v string) *NodeWrapper {
	if n.Labels == nil {
		n.Labels = make(map[string]string)
	}
	n.Labels[k] = v
	return n
}

// Annotation 向内部 node 添加一个 {k,v} 注解对。
func (n *NodeWrapper) Annotation(k, v string) *NodeWrapper {
	if n.Annotations == nil {
		n.Annotations = make(map[string]string)
	}
	metav1.SetMetaDataAnnotation(&n.ObjectMeta, k, v)
	return n
}

// Capacity 设置内部 node 的 capacity 和 allocatable 资源。
// resources 中每项对应一个资源名称及其数量。
// 默认情况下 pod 的容量和可分配数量均为 32。
func (n *NodeWrapper) Capacity(resources map[v1.ResourceName]string) *NodeWrapper {
	res := v1.ResourceList{
		v1.ResourcePods: resource.MustParse("32"),
	}
	for name, value := range resources {
		res[name] = resource.MustParse(value)
	}
	n.Status.Capacity, n.Status.Allocatable = res, res
	return n
}

// Images 设置内部 node 的镜像列表；images 中每项对应镜像名及其字节大小。
func (n *NodeWrapper) Images(images map[string]int64) *NodeWrapper {
	var containerImages []v1.ContainerImage
	for name, size := range images {
		containerImages = append(containerImages, v1.ContainerImage{Names: []string{name}, SizeBytes: size})
	}
	n.Status.Images = containerImages
	return n
}

// Taints 将内部 node 的污点设为 taints。
func (n *NodeWrapper) Taints(taints []v1.Taint) *NodeWrapper {
	n.Spec.Taints = taints
	return n
}

// Unschedulable 设置内部 node 的 unschedulable 字段。
func (n *NodeWrapper) Unschedulable(unschedulable bool) *NodeWrapper {
	n.Spec.Unschedulable = unschedulable
	return n
}

// DeclaredFeatures 设置 node status 中声明的特性。
func (n *NodeWrapper) DeclaredFeatures(declaredFeatures []string) *NodeWrapper {
	n.Status.DeclaredFeatures = declaredFeatures
	return n
}

// Condition 设置 node 的 condition，并刷新心跳与状态变更时间。
func (n *NodeWrapper) Condition(typ v1.NodeConditionType, status v1.ConditionStatus, message, reason string) *NodeWrapper {
	n.Status.Conditions = []v1.NodeCondition{
		{
			Type:               typ,
			Status:             status,
			Message:            message,
			Reason:             reason,
			LastHeartbeatTime:  metav1.Time{Time: time.Now()},
			LastTransitionTime: metav1.Time{Time: time.Now()},
		},
	}
	return n
}

// PersistentVolumeClaimWrapper 包装一个 PersistentVolumeClaim。
type PersistentVolumeClaimWrapper struct{ v1.PersistentVolumeClaim }

// MakePersistentVolumeClaim 创建 PersistentVolumeClaim 包装器。
func MakePersistentVolumeClaim() *PersistentVolumeClaimWrapper {
	return &PersistentVolumeClaimWrapper{}
}

// Obj 返回内部的 PersistentVolumeClaim。
func (p *PersistentVolumeClaimWrapper) Obj() *v1.PersistentVolumeClaim {
	return &p.PersistentVolumeClaim
}

// Name 将内部 PersistentVolumeClaim 的名称设为 s。
func (p *PersistentVolumeClaimWrapper) Name(s string) *PersistentVolumeClaimWrapper {
	p.SetName(s)
	return p
}

// Namespace 将内部 PersistentVolumeClaim 的命名空间设为 s。
func (p *PersistentVolumeClaimWrapper) Namespace(s string) *PersistentVolumeClaimWrapper {
	p.SetNamespace(s)
	return p
}

// Annotation 向内部 PersistentVolumeClaim 添加一个 {k,v} 对。
func (p *PersistentVolumeClaimWrapper) Annotation(key, value string) *PersistentVolumeClaimWrapper {
	metav1.SetMetaDataAnnotation(&p.ObjectMeta, key, value)
	return p
}

// VolumeName 将内部 PersistentVolumeClaim 的卷名设为 name。
func (p *PersistentVolumeClaimWrapper) VolumeName(name string) *PersistentVolumeClaimWrapper {
	p.PersistentVolumeClaim.Spec.VolumeName = name
	return p
}

// AccessModes 将内部 PersistentVolumeClaim 的访问模式设为 accessModes。
func (p *PersistentVolumeClaimWrapper) AccessModes(accessModes []v1.PersistentVolumeAccessMode) *PersistentVolumeClaimWrapper {
	p.PersistentVolumeClaim.Spec.AccessModes = accessModes
	return p
}

// Resources 将内部 PersistentVolumeClaim 的资源需求设为 resources。
func (p *PersistentVolumeClaimWrapper) Resources(resources v1.VolumeResourceRequirements) *PersistentVolumeClaimWrapper {
	p.PersistentVolumeClaim.Spec.Resources = resources
	return p
}

// StorageClassName 将内部 PersistentVolumeClaim 的 StorageClassName 设为 name。
func (p *PersistentVolumeClaimWrapper) StorageClassName(name *string) *PersistentVolumeClaimWrapper {
	p.PersistentVolumeClaim.Spec.StorageClassName = name
	return p
}

// PersistentVolumeWrapper 包装一个 PersistentVolume。
type PersistentVolumeWrapper struct{ v1.PersistentVolume }

// MakePersistentVolume 创建 PersistentVolume 包装器。
func MakePersistentVolume() *PersistentVolumeWrapper {
	return &PersistentVolumeWrapper{}
}

// Obj 返回内部的 PersistentVolume。
func (p *PersistentVolumeWrapper) Obj() *v1.PersistentVolume {
	return &p.PersistentVolume
}

// Name 将内部 PersistentVolume 的名称设为 s。
func (p *PersistentVolumeWrapper) Name(s string) *PersistentVolumeWrapper {
	p.SetName(s)
	return p
}

// AccessModes 将内部 PersistentVolume 的访问模式设为 accessModes。
func (p *PersistentVolumeWrapper) AccessModes(accessModes []v1.PersistentVolumeAccessMode) *PersistentVolumeWrapper {
	p.PersistentVolume.Spec.AccessModes = accessModes
	return p
}

// Capacity 将内部 PersistentVolume 的资源列表设为 capacity。
func (p *PersistentVolumeWrapper) Capacity(capacity v1.ResourceList) *PersistentVolumeWrapper {
	p.PersistentVolume.Spec.Capacity = capacity
	return p
}

// HostPathVolumeSource 将内部 PersistentVolume 的 hostPath 卷源设为 src。
func (p *PersistentVolumeWrapper) HostPathVolumeSource(src *v1.HostPathVolumeSource) *PersistentVolumeWrapper {
	p.PersistentVolume.Spec.HostPath = src
	return p
}

// PersistentVolumeSource 将内部 PersistentVolume 的卷源设为 src。
func (p *PersistentVolumeWrapper) PersistentVolumeSource(src v1.PersistentVolumeSource) *PersistentVolumeWrapper {
	p.PersistentVolume.Spec.PersistentVolumeSource = src
	return p
}

// NodeAffinityIn 创建一个 HARD 类型的 node affinity（MatchExpressions + In 操作符）并注入 pv。
func (p *PersistentVolumeWrapper) NodeAffinityIn(key string, vals []string) *PersistentVolumeWrapper {
	if p.Spec.NodeAffinity == nil {
		p.Spec.NodeAffinity = &v1.VolumeNodeAffinity{}
	}
	if p.Spec.NodeAffinity.Required == nil {
		p.Spec.NodeAffinity.Required = &v1.NodeSelector{}
	}
	nodeSelector := MakeNodeSelector().In(key, vals, NodeSelectorTypeMatchExpressions).Obj()
	p.Spec.NodeAffinity.Required.NodeSelectorTerms = append(p.Spec.NodeAffinity.Required.NodeSelectorTerms, nodeSelector.NodeSelectorTerms...)
	return p
}

// Labels 将 labels 中的所有 {k,v} 对添加到 pv 的标签。
func (p *PersistentVolumeWrapper) Labels(labels map[string]string) *PersistentVolumeWrapper {
	for k, v := range labels {
		p.Label(k, v)
	}
	return p
}

// Label 向 pv 添加一个 {k,v} 标签对。
func (p *PersistentVolumeWrapper) Label(k, v string) *PersistentVolumeWrapper {
	if p.PersistentVolume.ObjectMeta.Labels == nil {
		p.PersistentVolume.ObjectMeta.Labels = make(map[string]string)
	}
	p.PersistentVolume.ObjectMeta.Labels[k] = v
	return p
}

// StorageClassName 设置内部 PersistentVolume 的 StorageClassName。
func (p *PersistentVolumeWrapper) StorageClassName(name string) *PersistentVolumeWrapper {
	p.PersistentVolume.Spec.StorageClassName = name
	return p
}

// ResourceClaimWrapper 包装一个 ResourceClaim。
type ResourceClaimWrapper struct{ resourceapi.ResourceClaim }

// MakeResourceClaim 创建 ResourceClaim 包装器。
func MakeResourceClaim() *ResourceClaimWrapper {
	return &ResourceClaimWrapper{}
}

// FromResourceClaim 从已有对象创建 ResourceClaim 包装器，内部对象为深拷贝。
func FromResourceClaim(other *resourceapi.ResourceClaim) *ResourceClaimWrapper {
	return &ResourceClaimWrapper{*other.DeepCopy()}
}

// Obj 返回内部的 ResourceClaim。
func (wrapper *ResourceClaimWrapper) Obj() *resourceapi.ResourceClaim {
	return &wrapper.ResourceClaim
}

// Name 将内部对象的名称设为 s。
func (wrapper *ResourceClaimWrapper) Name(s string) *ResourceClaimWrapper {
	wrapper.SetName(s)
	return wrapper
}

// GenerateName 将内部对象的 GenerateName 设为 s。
func (wrapper *ResourceClaimWrapper) GenerateName(s string) *ResourceClaimWrapper {
	wrapper.SetGenerateName(s)
	return wrapper
}

// Annotations 将内部对象的注解设为 s。
func (wrapper *ResourceClaimWrapper) Annotations(s map[string]string) *ResourceClaimWrapper {
	wrapper.SetAnnotations(s)
	return wrapper
}

// UID 将内部对象的 UID 设为 s。
func (wrapper *ResourceClaimWrapper) UID(s string) *ResourceClaimWrapper {
	wrapper.SetUID(types.UID(s))
	return wrapper
}

// Namespace 将内部对象的命名空间设为 s。
func (wrapper *ResourceClaimWrapper) Namespace(s string) *ResourceClaimWrapper {
	wrapper.SetNamespace(s)
	return wrapper
}

// OwnerReference 更新对象的属主控制器。
func (wrapper *ResourceClaimWrapper) OwnerReference(name, uid string, gvk schema.GroupVersionKind) *ResourceClaimWrapper {
	wrapper.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: gvk.GroupVersion().String(),
			Kind:       gvk.Kind,
			Name:       name,
			UID:        types.UID(uid),
			Controller: ptr.To(true),
		},
	}
	return wrapper
}

// OwnerRef 将对象的属主引用设为 ref。
func (wrapper *ResourceClaimWrapper) OwnerRef(ref metav1.OwnerReference) *ResourceClaimWrapper {
	wrapper.OwnerReferences = []metav1.OwnerReference{ref}
	return wrapper
}

// Request 为给定的设备类添加一个设备请求。
func (wrapper *ResourceClaimWrapper) Request(deviceClassName string) *ResourceClaimWrapper {
	wrapper.Spec.Devices.Requests = append(wrapper.Spec.Devices.Requests,
		resourceapi.DeviceRequest{
			Name: fmt.Sprintf("req-%d", len(wrapper.Spec.Devices.Requests)+1),
			Exactly: &resourceapi.ExactDeviceRequest{
				// 单元测试直接使用此对象，不能依赖默认值填充。
				AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
				Count:           1,
				DeviceClassName: deviceClassName,
			},
		},
	)
	return wrapper
}

// RequestWithName 为给定设备类添加一个指定名称的设备请求。
func (wrapper *ResourceClaimWrapper) RequestWithName(name, deviceClassName string) *ResourceClaimWrapper {
	wrapper.Spec.Devices.Requests = append(wrapper.Spec.Devices.Requests,
		resourceapi.DeviceRequest{
			Name: name,
			Exactly: &resourceapi.ExactDeviceRequest{
				// 单元测试直接使用此对象，不能依赖默认值填充。
				AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
				Count:           1,
				DeviceClassName: deviceClassName,
			},
		})
	return wrapper
}

// RequestWithNameCount 为给定设备类添加一个指定名称和数量的设备请求。
func (wrapper *ResourceClaimWrapper) RequestWithNameCount(name, deviceClassName string, count int64) *ResourceClaimWrapper {
	wrapper.Spec.Devices.Requests = append(wrapper.Spec.Devices.Requests,
		resourceapi.DeviceRequest{
			Name: name,
			Exactly: &resourceapi.ExactDeviceRequest{
				// 单元测试直接使用此对象，不能依赖默认值填充。
				AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
				Count:           count,
				DeviceClassName: deviceClassName,
			},
		})
	return wrapper
}

// RequestWithPrioritizedListFromDeviceClasses 添加一个设备请求，每个设备类对应一个子请求。
func (wrapper *ResourceClaimWrapper) RequestWithPrioritizedListFromDeviceClasses(deviceClassNames ...string) *ResourceClaimWrapper {
	var prioritizedList []resourceapi.DeviceSubRequest
	for i, deviceClassName := range deviceClassNames {
		prioritizedList = append(prioritizedList, resourceapi.DeviceSubRequest{
			Name:            fmt.Sprintf("subreq-%d", i+1),
			AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
			Count:           1,
			DeviceClassName: deviceClassName,
		})
	}

	wrapper.Spec.Devices.Requests = append(wrapper.Spec.Devices.Requests,
		resourceapi.DeviceRequest{
			Name:           fmt.Sprintf("req-%d", len(wrapper.Spec.Devices.Requests)+1),
			FirstAvailable: prioritizedList,
		},
	)
	return wrapper
}

// RequestWithPrioritizedList 添加一个带优先级列表的设备请求，名称自动生成。
func (wrapper *ResourceClaimWrapper) RequestWithPrioritizedList(subRequests ...resourceapi.DeviceSubRequest) *ResourceClaimWrapper {
	return wrapper.NamedRequestWithPrioritizedList(fmt.Sprintf("req-%d", len(wrapper.Spec.Devices.Requests)+1), subRequests...)
}

// NamedRequestWithPrioritizedList 添加一个带优先级列表且指定名称的设备请求。
func (wrapper *ResourceClaimWrapper) NamedRequestWithPrioritizedList(name string, subRequests ...resourceapi.DeviceSubRequest) *ResourceClaimWrapper {
	wrapper.Spec.Devices.Requests = append(wrapper.Spec.Devices.Requests,
		resourceapi.DeviceRequest{
			Name:           name,
			FirstAvailable: subRequests,
		},
	)
	return wrapper
}

// SubRequest 构造一个指定名称、设备类和数量的子请求。
func SubRequest(name, deviceClassName string, count int64) resourceapi.DeviceSubRequest {
	return resourceapi.DeviceSubRequest{
		Name:            name,
		AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
		Count:           count,
		DeviceClassName: deviceClassName,
	}
}

// SubRequestWithSelector 构造一个带 CEL 选择器的子请求。
func SubRequestWithSelector(name, deviceClassName, selector string) resourceapi.DeviceSubRequest {
	return resourceapi.DeviceSubRequest{
		Name:            name,
		AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
		Count:           1,
		DeviceClassName: deviceClassName,
		Selectors: []resourceapi.DeviceSelector{
			{
				CEL: &resourceapi.CELDeviceSelector{
					Expression: selector,
				},
			},
		},
	}
}

// Allocation 设置内部对象的分配结果，并补上所需的 finalizer。
func (wrapper *ResourceClaimWrapper) Allocation(allocation *resourceapi.AllocationResult) *ResourceClaimWrapper {
	if !slices.Contains(wrapper.ResourceClaim.Finalizers, resourceapi.Finalizer) {
		wrapper.ResourceClaim.Finalizers = append(wrapper.ResourceClaim.Finalizers, resourceapi.Finalizer)
	}
	wrapper.ResourceClaim.Status.Allocation = allocation
	return wrapper
}

// AllocatedDeviceStatuses 设置内部对象的已分配设备状态。
func (wrapper *ResourceClaimWrapper) AllocatedDeviceStatuses(ads []resourceapi.AllocatedDeviceStatus) *ResourceClaimWrapper {
	wrapper.Status.Devices = ads
	return wrapper
}

// Deleting 设置内部对象的删除时间戳。
func (wrapper *ResourceClaimWrapper) Deleting(time metav1.Time) *ResourceClaimWrapper {
	wrapper.ResourceClaim.DeletionTimestamp = &time
	return wrapper
}

// ReservedFor 设置内部对象的 ReservedFor 字段。
func (wrapper *ResourceClaimWrapper) ReservedFor(consumers ...resourceapi.ResourceClaimConsumerReference) *ResourceClaimWrapper {
	wrapper.ResourceClaim.Status.ReservedFor = consumers
	return wrapper
}

// ReservedForPod 根据一个 pod 的信息设置内部对象的 ReservedFor 字段。
func (wrapper *ResourceClaimWrapper) ReservedForPod(podName string, podUID types.UID) *ResourceClaimWrapper {
	return wrapper.ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: podUID})
}

// ReservedForPodGroup 根据一个 podgroup 的信息设置内部对象的 ReservedFor 字段。
func (wrapper *ResourceClaimWrapper) ReservedForPodGroup(podGroupName string, podGroupUID types.UID) *ResourceClaimWrapper {
	return wrapper.ReservedFor(resourceapi.ResourceClaimConsumerReference{APIGroup: schedulingv1beta1.GroupName, Resource: "podgroups", Name: podGroupName, UID: podGroupUID})
}

// ResourceSliceWrapper 包装一个 ResourceSlice。
type ResourceSliceWrapper struct {
	resourceapi.ResourceSlice
}

// MakeResourceSlice 为指定节点和驱动创建 ResourceSlice 包装器。
func MakeResourceSlice(nodeName, driverName string) *ResourceSliceWrapper {
	wrapper := new(ResourceSliceWrapper)
	wrapper.Name = nodeName + "-" + driverName
	wrapper.Spec.NodeName = &nodeName
	wrapper.Spec.Pool.Name = nodeName
	wrapper.Spec.Pool.ResourceSliceCount = 1
	wrapper.Spec.Driver = driverName
	return wrapper
}

// MakeResourceSliceWithPerDeviceNodeSelection 创建启用逐设备节点选择的 ResourceSlice 包装器。
func MakeResourceSliceWithPerDeviceNodeSelection(namePrefix, driverName string) *ResourceSliceWrapper {
	wrapper := new(ResourceSliceWrapper)
	wrapper.Name = namePrefix + "-" + driverName
	wrapper.Spec.PerDeviceNodeSelection = ptr.To(true)
	wrapper.Spec.Pool.Name = namePrefix
	wrapper.Spec.Pool.ResourceSliceCount = 1
	wrapper.Spec.Driver = driverName
	return wrapper
}

// MakeResourceSliceWithAllNodes 创建覆盖所有节点的 ResourceSlice 包装器。
func MakeResourceSliceWithAllNodes(namePrefix, driverName string) *ResourceSliceWrapper {
	wrapper := new(ResourceSliceWrapper)
	wrapper.Name = namePrefix + "-" + driverName
	wrapper.Spec.AllNodes = new(true)
	wrapper.Spec.Pool.Name = namePrefix
	wrapper.Spec.Pool.ResourceSliceCount = 1
	wrapper.Spec.Driver = driverName
	return wrapper
}

// FromResourceSlice 从已有对象创建 ResourceSlice 包装器，内部对象为深拷贝。
func FromResourceSlice(other *resourceapi.ResourceSlice) *ResourceSliceWrapper {
	return &ResourceSliceWrapper{*other.DeepCopy()}
}

// Obj 返回内部的 ResourceSlice。
func (wrapper *ResourceSliceWrapper) Obj() *resourceapi.ResourceSlice {
	return &wrapper.ResourceSlice
}

// Devices 按名字设置内部对象的 devices 字段。
func (wrapper *ResourceSliceWrapper) Devices(names ...string) *ResourceSliceWrapper {
	for _, name := range names {
		wrapper.Spec.Devices = append(wrapper.Spec.Devices, resourceapi.Device{Name: name})
	}
	return wrapper
}

// NodeName 用于 Device 中按节点名填充设备的 NodeName 字段。
type NodeName string

// Device 向内部对象的 devices 字段追加一个设备。
// 设备必须有名称，并可附带任意其他字段。
func (wrapper *ResourceSliceWrapper) Device(name string, otherFields ...any) *ResourceSliceWrapper {
	device := resourceapi.Device{Name: name}
	for _, field := range otherFields {
		switch typedField := field.(type) {
		case map[resourceapi.QualifiedName]resourceapi.DeviceAttribute:
			device.Attributes = typedField
		case map[resourceapi.QualifiedName]resourceapi.DeviceCapacity:
			device.Capacity = typedField
		case resourceapi.DeviceTaint:
			device.Taints = append(device.Taints, typedField)
		case NodeName:
			device.NodeName = (*string)(&typedField)
		case *v1.NodeSelector:
			device.NodeSelector = typedField
		default:
			panic(fmt.Sprintf("expected a type which matches a field in BasicDevice, got %T", field))
		}
	}
	wrapper.Spec.Devices = append(wrapper.Spec.Devices, device)
	return wrapper
}

// ResourceSliceCount 设置资源池的 ResourceSliceCount。
func (wrapper *ResourceSliceWrapper) ResourceSliceCount(count int) *ResourceSliceWrapper {
	wrapper.Spec.Pool.ResourceSliceCount = int64(count)
	return wrapper
}

// SkipNodeOperations 设置要跳过的节点操作列表。
func (wrapper *ResourceSliceWrapper) SkipNodeOperations(skip ...resourceapi.SkipNodeOperation) *ResourceSliceWrapper {
	wrapper.Spec.SkipNodeOperations = skip
	return wrapper
}

// StorageClassWrapper 包装一个 StorageClass。
type StorageClassWrapper struct{ storagev1.StorageClass }

// MakeStorageClass 创建 StorageClass 包装器。
func MakeStorageClass() *StorageClassWrapper {
	return &StorageClassWrapper{}
}

// Obj 返回内部的 StorageClass。
func (s *StorageClassWrapper) Obj() *storagev1.StorageClass {
	return &s.StorageClass
}

// Name 将内部 StorageClass 的名称设为 n。
func (s *StorageClassWrapper) Name(n string) *StorageClassWrapper {
	s.SetName(n)
	return s
}

// VolumeBindingMode 将内部 StorageClass 的卷绑定模式设为 mode。
func (s *StorageClassWrapper) VolumeBindingMode(mode storagev1.VolumeBindingMode) *StorageClassWrapper {
	s.StorageClass.VolumeBindingMode = &mode
	return s
}

// Provisoner 将内部 StorageClass 的 provisioner 设为 p。
func (s *StorageClassWrapper) Provisioner(p string) *StorageClassWrapper {
	s.StorageClass.Provisioner = p
	return s
}

// AllowedTopologies 设置内部 StorageClass 的 AllowedTopologies。
func (s *StorageClassWrapper) AllowedTopologies(topologies []v1.TopologySelectorTerm) *StorageClassWrapper {
	s.StorageClass.AllowedTopologies = topologies
	return s
}

// Label 向内部 StorageClass 的标签添加一个 {k,v} 对。
func (s *StorageClassWrapper) Label(k, v string) *StorageClassWrapper {
	if s.ObjectMeta.Labels == nil {
		s.ObjectMeta.Labels = make(map[string]string)
	}
	s.ObjectMeta.Labels[k] = v
	return s
}

// CSINodeWrapper 包装一个 CSINode。
type CSINodeWrapper struct{ storagev1.CSINode }

// MakeCSINode 创建 CSINode 包装器。
func MakeCSINode() *CSINodeWrapper {
	return &CSINodeWrapper{}
}

// Obj 返回内部的 CSINode。
func (c *CSINodeWrapper) Obj() *storagev1.CSINode {
	return &c.CSINode
}

// Name 将内部 CSINode 的名称设为 n。
func (c *CSINodeWrapper) Name(n string) *CSINodeWrapper {
	c.SetName(n)
	return c
}

// Annotation 向内部 CSINode 的注解添加一个 {k,v} 对。
func (c *CSINodeWrapper) Annotation(key, value string) *CSINodeWrapper {
	metav1.SetMetaDataAnnotation(&c.ObjectMeta, key, value)
	return c
}

// Driver 向内部 CSINode 添加一个驱动。
func (c *CSINodeWrapper) Driver(driver storagev1.CSINodeDriver) *CSINodeWrapper {
	c.Spec.Drivers = append(c.Spec.Drivers, driver)
	return c
}

// CSIDriverWrapper 包装一个 CSIDriver。
type CSIDriverWrapper struct{ storagev1.CSIDriver }

// MakeCSIDriver 创建 CSIDriver 包装器。
func MakeCSIDriver() *CSIDriverWrapper {
	return &CSIDriverWrapper{}
}

// Obj 返回内部的 CSIDriver。
func (c *CSIDriverWrapper) Obj() *storagev1.CSIDriver {
	return &c.CSIDriver
}

// Name 将内部 CSIDriver 的名称设为 n。
func (c *CSIDriverWrapper) Name(n string) *CSIDriverWrapper {
	c.SetName(n)
	return c
}

// StorageCapacity 设置内部 CSIDriver 的 StorageCapacity。
func (c *CSIDriverWrapper) StorageCapacity(storageCapacity *bool) *CSIDriverWrapper {
	c.Spec.StorageCapacity = storageCapacity
	return c
}

// CSIStorageCapacityWrapper 包装一个 CSIStorageCapacity。
type CSIStorageCapacityWrapper struct{ storagev1.CSIStorageCapacity }

// MakeCSIStorageCapacity 创建 CSIStorageCapacity 包装器。
func MakeCSIStorageCapacity() *CSIStorageCapacityWrapper {
	return &CSIStorageCapacityWrapper{}
}

// Obj 返回内部的 CSIStorageCapacity。
func (c *CSIStorageCapacityWrapper) Obj() *storagev1.CSIStorageCapacity {
	return &c.CSIStorageCapacity
}

// Name 将内部 CSIStorageCapacity 的名称设为 n。
func (c *CSIStorageCapacityWrapper) Name(n string) *CSIStorageCapacityWrapper {
	c.SetName(n)
	return c
}

// StorageClassName 设置内部 CSIStorageCapacity 的 StorageClassName。
func (c *CSIStorageCapacityWrapper) StorageClassName(name string) *CSIStorageCapacityWrapper {
	c.CSIStorageCapacity.StorageClassName = name
	return c
}

// Capacity 设置内部 CSIStorageCapacity 的 Capacity。
func (c *CSIStorageCapacityWrapper) Capacity(capacity *resource.Quantity) *CSIStorageCapacityWrapper {
	c.CSIStorageCapacity.Capacity = capacity
	return c
}

// VolumeAttachmentWrapper 包装一个 VolumeAttachment。
type VolumeAttachmentWrapper struct{ storagev1.VolumeAttachment }

// MakeVolumeAttachment 创建 VolumeAttachment 包装器。
func MakeVolumeAttachment() *VolumeAttachmentWrapper {
	return &VolumeAttachmentWrapper{}
}

// Obj 返回内部的 VolumeAttachment。
func (c *VolumeAttachmentWrapper) Obj() *storagev1.VolumeAttachment {
	return &c.VolumeAttachment
}

// Name 将内部 VolumeAttachment 的名称设为 n。
func (c *VolumeAttachmentWrapper) Name(n string) *VolumeAttachmentWrapper {
	c.SetName(n)
	return c
}

// Attacher 设置卷附加器名称。
func (c *VolumeAttachmentWrapper) Attacher(attacher string) *VolumeAttachmentWrapper {
	c.Spec.Attacher = attacher
	return c
}

// NodeName 设置卷要附加到的节点名称。
func (c *VolumeAttachmentWrapper) NodeName(nodeName string) *VolumeAttachmentWrapper {
	c.Spec.NodeName = nodeName
	return c
}

// Source 设置卷的来源。
func (c *VolumeAttachmentWrapper) Source(source storagev1.VolumeAttachmentSource) *VolumeAttachmentWrapper {
	c.Spec.Source = source
	return c
}

// Attached 设置卷的附加状态。
func (c *VolumeAttachmentWrapper) Attached(attached bool) *VolumeAttachmentWrapper {
	c.Status.Attached = attached
	return c
}

// PodGroupWrapper 包装一个 PodGroup。
type PodGroupWrapper struct{ schedulingv1beta1.PodGroup }

// MakePodGroup 创建 PodGroup 包装器。
func MakePodGroup() *PodGroupWrapper {
	return &PodGroupWrapper{}
}

// Name 将内部 PodGroup 的名称设为 name。
func (wrapper *PodGroupWrapper) Name(name string) *PodGroupWrapper {
	wrapper.PodGroup.Name = name
	return wrapper
}

// Namespace 将内部 PodGroup 的命名空间设为 namespace。
func (wrapper *PodGroupWrapper) Namespace(namespace string) *PodGroupWrapper {
	wrapper.PodGroup.Namespace = namespace
	return wrapper
}

// UID 将内部 PodGroup 的 UID 设为 uid。
func (wrapper *PodGroupWrapper) UID(uid types.UID) *PodGroupWrapper {
	wrapper.PodGroup.UID = uid
	return wrapper
}

// Obj 返回内部的 PodGroup。
func (wrapper *PodGroupWrapper) Obj() *schedulingv1beta1.PodGroup {
	return &wrapper.PodGroup
}

// MinCount 设置 Gang 调度策略的 MinCount。
func (wrapper *PodGroupWrapper) MinCount(count int32) *PodGroupWrapper {
	if wrapper.PodGroup.Spec.SchedulingPolicy.Gang == nil {
		wrapper.PodGroup.Spec.SchedulingPolicy.Gang = &schedulingv1beta1.GangSchedulingPolicy{}
	}
	wrapper.PodGroup.Spec.SchedulingPolicy.Gang.MinCount = count
	return wrapper
}

// BasicPolicy 将 PodGroup 的调度策略设为 Basic。
func (wrapper *PodGroupWrapper) BasicPolicy() *PodGroupWrapper {
	wrapper.PodGroup.Spec.SchedulingPolicy.Basic = &schedulingv1beta1.BasicSchedulingPolicy{}
	return wrapper
}

// WorkloadRef 设置内部 PodGroup 的 WorkloadRef 字段。
func (wrapper *PodGroupWrapper) WorkloadRef(templateName, workloadName string) *PodGroupWrapper {
	wrapper.PodGroup.Spec.WorkloadRef = &schedulingv1beta1.WorkloadReference{
		TemplateName: templateName,
		WorkloadName: workloadName,
	}
	return wrapper
}

// TopologyKey 设置内部 PodGroup 的 SchedulingConstraints 中的拓扑键。
func (wrapper *PodGroupWrapper) TopologyKey(topologyKey string) *PodGroupWrapper {
	wrapper.PodGroup.Spec.SchedulingConstraints = &schedulingv1beta1.PodGroupSchedulingConstraints{
		Topology: []schedulingv1beta1.TopologyConstraint{
			{
				Key: topologyKey,
			},
		},
	}
	return wrapper
}

// ResourceClaims 向内部 PodGroup 追加资源声明。
func (wrapper *PodGroupWrapper) ResourceClaims(claims ...schedulingv1beta1.PodGroupResourceClaim) *PodGroupWrapper {
	wrapper.Spec.ResourceClaims = append(wrapper.Spec.ResourceClaims, claims...)
	return wrapper
}

// ResourceClaimStatuses 向内部 PodGroup 追加资源声明状态。
func (wrapper *PodGroupWrapper) ResourceClaimStatuses(statuses ...schedulingv1beta1.PodGroupResourceClaimStatus) *PodGroupWrapper {
	wrapper.Status.ResourceClaimStatuses = append(wrapper.Status.ResourceClaimStatuses, statuses...)
	return wrapper
}

// DisruptionModeAll 将内部 PodGroup 的中断模式设为 All。
func (wrapper *PodGroupWrapper) DisruptionModeAll() *PodGroupWrapper {
	wrapper.PodGroup.Spec.DisruptionMode = &schedulingv1beta1.DisruptionMode{All: &schedulingv1beta1.AllDisruptionMode{}}
	return wrapper
}

// DisruptionModeSingle 将内部 PodGroup 的中断模式设为 Single。
func (wrapper *PodGroupWrapper) DisruptionModeSingle() *PodGroupWrapper {
	wrapper.PodGroup.Spec.DisruptionMode = &schedulingv1beta1.DisruptionMode{Single: &schedulingv1beta1.SingleDisruptionMode{}}
	return wrapper
}

// Priority 设置内部 PodGroup 的优先级。
func (wrapper *PodGroupWrapper) Priority(priority int32) *PodGroupWrapper {
	wrapper.PodGroup.Spec.Priority = &priority
	return wrapper
}

// PreemptionPolicy 设置内部 PodGroup 的抢占策略。
func (wrapper *PodGroupWrapper) PreemptionPolicy(policy schedulingv1beta1.PreemptionPolicy) *PodGroupWrapper {
	wrapper.PodGroup.Spec.PreemptionPolicy = &policy
	return wrapper
}

// ParentCompositePodGroup 设置内部 PodGroup 的父组合 pod group 名称。
func (wrapper *PodGroupWrapper) ParentCompositePodGroup(parent string) *PodGroupWrapper {
	wrapper.PodGroup.Spec.ParentCompositePodGroupName = &parent
	return wrapper
}

// WorkloadWrapper 包装一个 Workload。
type WorkloadWrapper struct{ schedulingv1beta1.Workload }

// MakeWorkload 创建 Workload 包装器。
func MakeWorkload() *WorkloadWrapper {
	return &WorkloadWrapper{}
}

// Obj 返回内部的 Workload。
func (wrapper *WorkloadWrapper) Obj() *schedulingv1beta1.Workload {
	return &wrapper.Workload
}

// Name 将内部 Workload 的名称设为 name。
func (wrapper *WorkloadWrapper) Name(name string) *WorkloadWrapper {
	wrapper.Workload.Name = name
	return wrapper
}

// Namespace 将内部 Workload 的命名空间设为 namespace。
func (wrapper *WorkloadWrapper) Namespace(namespace string) *WorkloadWrapper {
	wrapper.Workload.Namespace = namespace
	return wrapper
}

// PodGroupTemplate 向 Workload spec 追加一个 PodGroupTemplate。
func (wrapper *WorkloadWrapper) PodGroupTemplate(t schedulingv1beta1.PodGroupTemplate) *WorkloadWrapper {
	wrapper.Workload.Spec.PodGroupTemplates = append(wrapper.Workload.Spec.PodGroupTemplates, t)
	return wrapper
}

// PodGroupTemplateWrapper 包装一个 PodGroupTemplate。
type PodGroupTemplateWrapper struct {
	schedulingv1beta1.PodGroupTemplate
}

// MakePodGroupTemplate 创建 PodGroupTemplate 包装器。
func MakePodGroupTemplate() *PodGroupTemplateWrapper {
	return &PodGroupTemplateWrapper{}
}

// Obj 返回内部的 PodGroupTemplate。
func (wrapper *PodGroupTemplateWrapper) Obj() schedulingv1beta1.PodGroupTemplate {
	return wrapper.PodGroupTemplate
}

// Name 将内部 PodGroupTemplate 的名称设为 name。
func (wrapper *PodGroupTemplateWrapper) Name(name string) *PodGroupTemplateWrapper {
	wrapper.PodGroupTemplate.Name = name
	return wrapper
}

// Priority 设置内部 PodGroupTemplate 的优先级。
func (wrapper *PodGroupTemplateWrapper) Priority(priority int32) *PodGroupTemplateWrapper {
	wrapper.PodGroupTemplate.Priority = &priority
	return wrapper
}

// MinCount 设置 Gang 调度策略的 MinCount。
func (wrapper *PodGroupTemplateWrapper) MinCount(count int32) *PodGroupTemplateWrapper {
	if wrapper.SchedulingPolicy.Gang == nil {
		wrapper.SchedulingPolicy.Gang = &schedulingv1beta1.GangSchedulingPolicy{}
	}
	wrapper.SchedulingPolicy.Gang.MinCount = count
	return wrapper
}

// BasicPolicy 将 PodGroup 的调度策略设为 Basic。
func (wrapper *PodGroupTemplateWrapper) BasicPolicy() *PodGroupTemplateWrapper {
	wrapper.SchedulingPolicy.Basic = &schedulingv1beta1.BasicSchedulingPolicy{}
	return wrapper
}

// DisruptionModeAll 将内部 PodGroupTemplate 的中断模式设为 All。
func (wrapper *PodGroupTemplateWrapper) DisruptionModeAll() *PodGroupTemplateWrapper {
	wrapper.PodGroupTemplate.DisruptionMode = &schedulingv1beta1.DisruptionMode{All: &schedulingv1beta1.AllDisruptionMode{}}
	return wrapper
}

// DisruptionModeSingle 将内部 PodGroupTemplate 的中断模式设为 Single。
func (wrapper *PodGroupTemplateWrapper) DisruptionModeSingle() *PodGroupTemplateWrapper {
	wrapper.PodGroupTemplate.DisruptionMode = &schedulingv1beta1.DisruptionMode{Single: &schedulingv1beta1.SingleDisruptionMode{}}
	return wrapper
}

// CompositePodGroupTemplate 向 Workload spec 追加一个 CompositePodGroupTemplate。
func (wrapper *WorkloadWrapper) CompositePodGroupTemplate(t schedulingv1beta1.CompositePodGroupTemplate) *WorkloadWrapper {
	wrapper.Workload.Spec.CompositePodGroupTemplates = append(wrapper.Workload.Spec.CompositePodGroupTemplates, t)
	return wrapper
}

// Children 向 Workload 添加子模板。
// 接受 *PodGroupTemplateWrapper 和 *CompositePodGroupTemplateWrapper 两种类型。
func (wrapper *WorkloadWrapper) Children(children ...any) *WorkloadWrapper {
	for _, child := range children {
		switch c := child.(type) {
		case *PodGroupTemplateWrapper:
			wrapper.Workload.Spec.PodGroupTemplates = append(wrapper.Workload.Spec.PodGroupTemplates, c.Obj())
		case *CompositePodGroupTemplateWrapper:
			wrapper.Workload.Spec.CompositePodGroupTemplates = append(wrapper.Workload.Spec.CompositePodGroupTemplates, c.Obj())
		default:
			panic(fmt.Sprintf("unexpected child type %T", child))
		}
	}
	return wrapper
}

// CompositePodGroupTemplateWrapper 包装一个 CompositePodGroupTemplate。
type CompositePodGroupTemplateWrapper struct {
	schedulingv1beta1.CompositePodGroupTemplate
}

// MakeCompositePodGroupTemplate 创建 CompositePodGroupTemplate 包装器。
func MakeCompositePodGroupTemplate() *CompositePodGroupTemplateWrapper {
	return &CompositePodGroupTemplateWrapper{}
}

// Obj 返回内部的 CompositePodGroupTemplate。
func (wrapper *CompositePodGroupTemplateWrapper) Obj() schedulingv1beta1.CompositePodGroupTemplate {
	return wrapper.CompositePodGroupTemplate
}

// Name 将内部 CompositePodGroupTemplate 的名称设为 name。
func (wrapper *CompositePodGroupTemplateWrapper) Name(name string) *CompositePodGroupTemplateWrapper {
	wrapper.CompositePodGroupTemplate.Name = name
	return wrapper
}

// Priority 设置内部 CompositePodGroupTemplate 的优先级。
func (wrapper *CompositePodGroupTemplateWrapper) Priority(priority int32) *CompositePodGroupTemplateWrapper {
	wrapper.CompositePodGroupTemplate.Priority = &priority
	return wrapper
}

// Children 向 CompositePodGroupTemplate 添加子模板。
// 接受 *PodGroupTemplateWrapper 和 *CompositePodGroupTemplateWrapper 两种类型。
func (wrapper *CompositePodGroupTemplateWrapper) Children(children ...any) *CompositePodGroupTemplateWrapper {
	for _, child := range children {
		switch c := child.(type) {
		case *PodGroupTemplateWrapper:
			wrapper.PodGroupTemplates = append(wrapper.PodGroupTemplates, c.Obj())
		case *CompositePodGroupTemplateWrapper:
			wrapper.CompositePodGroupTemplates = append(wrapper.CompositePodGroupTemplates, c.Obj())
		default:
			panic(fmt.Sprintf("unexpected child type %T", child))
		}
	}
	return wrapper
}

// MinGroupCount 将调度策略设为 Gang，并指定 minGroupCount。
func (wrapper *CompositePodGroupTemplateWrapper) MinGroupCount(minGroupCount int32) *CompositePodGroupTemplateWrapper {
	wrapper.SchedulingPolicy.Gang = &schedulingv1beta1.CompositeGangSchedulingPolicy{MinGroupCount: minGroupCount}
	return wrapper
}

// BasicPolicy 将调度策略设为 Basic。
func (wrapper *CompositePodGroupTemplateWrapper) BasicPolicy() *CompositePodGroupTemplateWrapper {
	wrapper.SchedulingPolicy.Basic = &schedulingv1beta1.CompositeBasicSchedulingPolicy{}
	return wrapper
}

// CompositePodGroupWrapper 包装一个 CompositePodGroup。
type CompositePodGroupWrapper struct {
	schedulingv1alpha3.CompositePodGroup
}

// MakeCompositePodGroup 创建 CompositePodGroup 包装器。
func MakeCompositePodGroup() *CompositePodGroupWrapper {
	return &CompositePodGroupWrapper{}
}

// Obj 返回内部的 CompositePodGroup。
func (wrapper *CompositePodGroupWrapper) Obj() *schedulingv1alpha3.CompositePodGroup {
	return &wrapper.CompositePodGroup
}

// Name 将内部 CompositePodGroup 的名称设为 name。
func (wrapper *CompositePodGroupWrapper) Name(name string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Name = name
	return wrapper
}

// Priority 设置内部 CompositePodGroup 的优先级。
func (wrapper *CompositePodGroupWrapper) Priority(priority int32) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.Priority = &priority
	return wrapper
}

// PreemptionPolicy 设置内部 CompositePodGroup 的抢占策略。
func (wrapper *CompositePodGroupWrapper) PreemptionPolicy(policy schedulingv1alpha3.PreemptionPolicy) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.PreemptionPolicy = &policy
	return wrapper
}

// Namespace 将内部 CompositePodGroup 的命名空间设为 namespace。
func (wrapper *CompositePodGroupWrapper) Namespace(namespace string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Namespace = namespace
	return wrapper
}

// UID 设置内部 CompositePodGroup 的 UID。
func (wrapper *CompositePodGroupWrapper) UID(uid string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.UID = types.UID(uid)
	return wrapper
}

// ParentCompositePodGroup 设置父 CompositePodGroup 的名称。
func (wrapper *CompositePodGroupWrapper) ParentCompositePodGroup(parent string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.ParentCompositePodGroupName = new(parent)
	return wrapper
}

// MinGroupCount 将调度策略设为 Gang，并指定 minGroupCount。
func (wrapper *CompositePodGroupWrapper) MinGroupCount(minGroupCount int32) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.SchedulingPolicy.Gang = &schedulingv1alpha3.CompositeGangSchedulingPolicy{MinGroupCount: minGroupCount}
	return wrapper
}

// BasicPolicy 将调度策略设为 Basic。
func (wrapper *CompositePodGroupWrapper) BasicPolicy() *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.SchedulingPolicy.Basic = &schedulingv1alpha3.CompositeBasicSchedulingPolicy{}
	return wrapper
}

// DisruptionModeAll 将内部 CompositePodGroup 的中断模式设为 All。
func (wrapper *CompositePodGroupWrapper) DisruptionModeAll() *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.DisruptionMode = &schedulingv1alpha3.CompositeDisruptionMode{All: &schedulingv1alpha3.AllCompositeDisruptionMode{}}
	return wrapper
}

// DisruptionModeSingle 将内部 CompositePodGroup 的中断模式设为 Single。
func (wrapper *CompositePodGroupWrapper) DisruptionModeSingle() *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.DisruptionMode = &schedulingv1alpha3.CompositeDisruptionMode{Single: &schedulingv1alpha3.SingleCompositeDisruptionMode{}}
	return wrapper
}

// WorkloadRef 设置 CompositePodGroup 的 WorkloadReference。
func (wrapper *CompositePodGroupWrapper) WorkloadRef(workloadName, templateName string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.WorkloadRef = &schedulingv1alpha3.WorkloadReference{
		WorkloadName: workloadName,
		TemplateName: templateName,
	}
	return wrapper
}

// TopologyKey 设置内部 CompositePodGroup 的 SchedulingConstraints 中的拓扑键。
func (wrapper *CompositePodGroupWrapper) TopologyKey(topologyKey string) *CompositePodGroupWrapper {
	wrapper.CompositePodGroup.Spec.SchedulingConstraints = &schedulingv1alpha3.CompositePodGroupSchedulingConstraints{
		Topology: []schedulingv1alpha3.TopologyConstraint{
			{
				Key: topologyKey,
			},
		},
	}
	return wrapper
}
