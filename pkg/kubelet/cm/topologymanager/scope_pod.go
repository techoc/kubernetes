/*
Copyright 2020 The Kubernetes Authors.

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

package topologymanager

import (
	"context"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// podScope 以整个 Pod 为粒度计算 NUMA 亲和性。
type podScope struct {
	scope
}

// 编译期断言：podScope 实现 Scope 接口。
var _ Scope = &podScope{}

// NewPodScope 创建 Pod 级拓扑管理器 scope。
func NewPodScope(policy Policy) Scope {
	return &podScope{
		scope{
			name:             PodTopologyScope,
			podTopologyHints: podTopologyHints{},
			policy:           policy,
			podMap:           containermap.NewContainerMap(),
		},
	}
}

// Admit 执行 Pod 准入：启用 PodLevelResourceManagers 时按 Pod 级资源分配，
// 否则按容器逐个分配。
func (s *podScope) Admit(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult {
	// 启用 PodLevelResourceManagers 时，把资源分配交给 hint provider（CPU/Memory manager）
	// 的 AllocatePod 一次性完成。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		return s.admitUsingPodResources(ctx, pod, operation)
	}
	return s.admitUsingContainerResources(ctx, pod, operation)
}

// admitUsingContainerResources 按容器粒度计算亲和性并逐个分配资源。
func (s *podScope) admitUsingContainerResources(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult {
	logger := klog.FromContext(ctx)

	bestHint, admit := s.checkAffinity(logger, pod, operation)
	if !admit {
		return admission.GetPodAdmitResult(NewTopologyAffinityError())
	}

	// init 容器与普通容器都要参与分配；init 容器先执行。
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		logger.Info("Topology Affinity", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name)
		s.setTopologyHints(string(pod.UID), container.Name, bestHint)

		// 单个容器分配失败即拒绝整个 Pod，并记录准入错误。
		err := s.allocateAlignedResources(ctx, pod, &container, operation)
		if err != nil {
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			return admission.GetPodAdmitResult(err)
		}
	}

	s.updateSuccessMetrics(logger, pod)
	return admission.GetPodAdmitResult(nil)
}

// admitUsingPodResources 按 Pod 粒度计算亲和性并一次性分配资源。
func (s *podScope) admitUsingPodResources(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult {
	logger := klog.FromContext(ctx)

	bestHint, admit := s.checkAffinity(logger, pod, operation)
	if !admit {
		return admission.GetPodAdmitResult(NewPodLevelTopologyAffinityError("pod with pod-level resources failed admission under pod-scope topology manager"))
	}

	// 启用 PodLevelResourceManagers 时，把资源分配交给 hint provider（CPU/Memory manager）
	// 的 AllocatePod 一次性完成。
	// 这里只调用一次 AllocatePod，代表整个 Pod 的一次性分配。
	logger.V(4).Info("Calling AllocatePod on hint providers", "pod", klog.KObj(pod))

	// 为所有容器记录同一个最佳 hint，后续状态查询依赖该映射。
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		logger.Info("Topology Affinity", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name)
		s.setTopologyHints(string(pod.UID), container.Name, bestHint)
	}

	// Pod 级分配失败同样拒绝准入并计数。
	err := s.allocatePodAlignedResources(logger, pod, operation)
	if err != nil {
		logger.Error(err, "Pod-level allocation failed", "pod", klog.KObj(pod))
		metrics.TopologyManagerAdmissionErrorsTotal.Inc()
		return admission.GetPodAdmitResult(err)
	}

	s.updateSuccessMetrics(logger, pod)
	return admission.GetPodAdmitResult(nil)
}

// checkAffinity 计算 Pod 整体的最佳 NUMA 亲和性，并在失败时更新指标。
func (s *podScope) checkAffinity(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) (TopologyHint, bool) {
	// 以整个 Pod 为单位计算最佳 NUMA 亲和性。
	bestHint, admit := s.calculateAffinity(logger, pod, operation)
	logger.Info("Best TopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "operation", operation)
	if !admit {
		if IsAlignmentGuaranteed(s.policy) {
			// 只有策略保证对齐时才计入失败指标，避免把尽力而为的失败误报为回归。
			metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopePod, metrics.AlignedNUMANode).Inc()
		}
		metrics.TopologyManagerAdmissionErrorsTotal.Inc()

		return TopologyHint{}, false
	}
	return bestHint, true
}

// updateSuccessMetrics 在策略保证对齐时上报成功指标。
func (s *podScope) updateSuccessMetrics(logger klog.Logger, pod *v1.Pod) {
	if IsAlignmentGuaranteed(s.policy) {
		// 只有保证对齐的策略才上报成功，保持指标口径一致。
		logger.V(4).Info("Resource alignment at pod scope guaranteed", "pod", klog.KObj(pod))
		metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopePod, metrics.AlignedNUMANode).Inc()
	}
}

// accumulateProvidersHints 收集所有 hint provider 给出的 Pod 级拓扑提示。
func (s *podScope) accumulateProvidersHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) []map[string][]TopologyHint {
	var providersHints []map[string][]TopologyHint

	for _, provider := range s.hintProviders {
		// 向每个 provider 索取 Pod 级 hint。
		hints := provider.GetPodTopologyHints(logger, pod, operation)
		providersHints = append(providersHints, hints)
		logger.Info("TopologyHints", "hints", hints, "pod", klog.KObj(pod), "operation", operation)
	}
	return providersHints
}

// calculateAffinity 汇总 provider 的 hint 并交给策略合并出最终结果。
func (s *podScope) calculateAffinity(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) (TopologyHint, bool) {
	providersHints := s.accumulateProvidersHints(logger, pod, operation)
	bestHint, admit := s.policy.Merge(logger, providersHints)
	logger.Info("PodTopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "operation", operation)
	return bestHint, admit
}
