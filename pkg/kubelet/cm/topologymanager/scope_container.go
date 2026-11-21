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

	"k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// containerScope 以容器为粒度进行拓扑对齐。
type containerScope struct {
	scope
}

// 编译期断言：containerScope 实现了 Scope 接口。
var _ Scope = &containerScope{}

// NewContainerScope 创建容器级别的拓扑作用域。
func NewContainerScope(policy Policy) Scope {
	return &containerScope{
		scope{
			name:             ContainerTopologyScope,
			podTopologyHints: podTopologyHints{},
			policy:           policy,
			podMap:           containermap.NewContainerMap(),
		},
	}
}

// Admit 为 Pod 中的每个容器计算拓扑提示并分配对齐资源。
func (s *containerScope) Admit(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult {
	logger := klog.FromContext(ctx)

	// 对 InitContainer 与普通容器逐一计算拓扑亲和性。
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		bestHint, admit := s.calculateAffinity(logger, pod, &container, operation)
		logger.Info("Best TopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)

		// 准入失败时按是否保证对齐记录指标并返回错误。
		if !admit {
			if IsAlignmentGuaranteed(s.policy) {
				metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Inc()
			}
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			// Pod 级资源需要专用错误信息，便于用户定位。
			if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
				return admission.GetPodAdmitResult(NewPodLevelTopologyAffinityError("pod with pod-level resources failed admission with a container-level topology manager"))
			}
			return admission.GetPodAdmitResult(&TopologyAffinityError{})
		}
		logger.Info("Topology Affinity", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
		s.setTopologyHints(string(pod.UID), container.Name, bestHint)

		// 分配对齐资源，失败则拒绝准入。
		err := s.allocateAlignedResources(ctx, pod, &container, operation)
		if err != nil {
			metrics.TopologyManagerAdmissionErrorsTotal.Inc()
			return admission.GetPodAdmitResult(err)
		}

		// 保证对齐的策略在成功分配后记录指标。
		if IsAlignmentGuaranteed(s.policy) {
			logger.V(4).Info("Resource alignment at container scope guaranteed", "pod", klog.KObj(pod))
			metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Inc()
		}
	}
	return admission.GetPodAdmitResult(nil)
}

// accumulateProvidersHints 汇总所有 HintProvider 为单个容器给出的拓扑提示。
func (s *containerScope) accumulateProvidersHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) []map[string][]TopologyHint {
	var providersHints []map[string][]TopologyHint

	for _, provider := range s.hintProviders {
		// 从 HintProvider 获取该容器的拓扑提示。
		hints := provider.GetTopologyHints(logger, pod, container, operation)
		providersHints = append(providersHints, hints)
		logger.Info("TopologyHints", "hints", hints, "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	}
	return providersHints
}

// calculateAffinity 合并各提供者的提示，得到容器的最终拓扑提示与准入结果。
func (s *containerScope) calculateAffinity(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) (TopologyHint, bool) {
	providersHints := s.accumulateProvidersHints(logger, pod, container, operation)
	bestHint, admit := s.policy.Merge(logger, providersHints)
	logger.Info("ContainerTopologyHint", "bestHint", bestHint, "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	return bestHint, admit
}
