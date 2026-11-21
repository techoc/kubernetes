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

package kubelet

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

// defaultPodLimitsForDownwardAPI 复制 Pod 与可选容器，并填充默认资源 limit，
// 供 Downward API 暴露 limits 时使用。
func (kl *Kubelet) defaultPodLimitsForDownwardAPI(ctx context.Context, pod *corev1.Pod, container *corev1.Container) (*corev1.Pod, *corev1.Container, error) {
	logger := klog.FromContext(ctx)

	if pod == nil {
		return nil, nil, fmt.Errorf("invalid input, pod cannot be nil")
	}

	node, err := kl.getNodeAnyWay(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find node object, expected a node")
	}
	allocatable := node.Status.Allocatable
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) && resourcehelper.IsPodLevelLimitsSet(pod) {
		allocatable = allocatable.DeepCopy()
		// Downward API 支持的资源类型。
		for _, resource := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
			// 跳过 Pod 级资源不支持的资源类型。
			if !resourcehelper.IsSupportedPodLevelResource(resource) {
				continue
			}
			if val, exists := pod.Spec.Resources.Limits[resource]; exists && !val.IsZero() {
				if _, exists := allocatable[resource]; exists {
					allocatable[resource] = val.DeepCopy()
				}
			}
		}
	}

	logger.Info("Allocatable", "allocatable", allocatable)
	outputPod := pod.DeepCopy()
	for idx := range outputPod.Spec.Containers {
		resource.MergeContainerResourceLimits(&outputPod.Spec.Containers[idx], allocatable)
	}

	var outputContainer *corev1.Container
	if container != nil {
		outputContainer = container.DeepCopy()
		resource.MergeContainerResourceLimits(outputContainer, allocatable)
	}
	return outputPod, outputContainer, nil
}
