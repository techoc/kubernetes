/*
Copyright 2025 The Kubernetes Authors.

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

package events

import (
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)

// containerAllocation 是事件消息中单个容器的资源分配描述。
type containerAllocation struct {
	// Name 是容器名称。
	Name string `json:"name"`
	// Resources 是容器当前分配的资源。
	Resources v1.ResourceRequirements `json:"resources,omitempty"`
}

// podResourceSummary 是事件消息中的 Pod 资源摘要。
type podResourceSummary struct {
	// TODO: resources v1.ResourceRequirements, add pod-level resources here once resizing pod-level resources is supported
	// InitContainers 是 InitContainer 的资源分配。
	InitContainers []containerAllocation `json:"initContainers,omitempty"`
	// Containers 是普通容器的资源分配。
	Containers []containerAllocation `json:"containers,omitempty"`
	// Generation 是 Pod 的代数。
	Generation int64 `json:"generation"`
	// Error 是可选的错误信息。
	Error string `json:"error,omitempty"`
}

// PodResizeCompletedMsg 生成 Pod 调整资源完成的事件消息。
func PodResizeCompletedMsg(logger klog.Logger, allocatedPod *v1.Pod, generation int64) string {
	return podResizeMessage(logger, allocatedPod, generation, "", "Pod resize completed")
}

// PodResizeStartedMsg 生成 Pod 开始调整资源的事件消息。
func PodResizeStartedMsg(logger klog.Logger, allocatedPod *v1.Pod, generation int64) string {
	return podResizeMessage(logger, allocatedPod, generation, "", "Pod resize started")
}

// PodResizeErrorMsg 生成 Pod 调整资源出错的事件消息。
func PodResizeErrorMsg(logger klog.Logger, allocatedPod *v1.Pod, generation int64, errorMsg string) string {
	return podResizeMessage(logger, allocatedPod, generation, errorMsg, "Pod resize error")
}

// PodResizePendingMsg 生成 Pod 调整资源待处理的事件消息。
func PodResizePendingMsg(logger klog.Logger, pod *v1.Pod, reason, message string, generation int64) string {
	return podResizeMessage(logger, pod, generation, message, fmt.Sprintf("Pod resize %s", reason))
}

// podResizeMessage 拼接前缀与资源摘要，生成最终事件消息。
func podResizeMessage(logger klog.Logger, pod *v1.Pod, generation int64, errorMsg, messagePrefix string) string {
	// 摘要生成失败时退化为仅输出前缀消息。
	resources, err := makeResourceSummaryFromSpec(logger, pod, generation, errorMsg)
	if err != nil {
		return messagePrefix
	}
	return fmt.Sprintf("%s: %s", messagePrefix, resources)

}

// makeResourceSummaryFromSpec 返回 podspec 中期望的资源摘要。
func makeResourceSummaryFromSpec(logger klog.Logger, pod *v1.Pod, generation int64, errorMessage string) (string, error) {
	specResources := &podResourceSummary{Generation: generation, Error: errorMessage}
	// 分别收集 InitContainer 与普通容器的资源。
	for container, containerType := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers|podutil.Containers) {
		allocation := containerAllocation{
			Name:      container.Name,
			Resources: container.Resources,
		}
		switch containerType {
		case podutil.InitContainers:
			specResources.InitContainers = append(specResources.InitContainers, allocation)
		case podutil.Containers:
			specResources.Containers = append(specResources.Containers, allocation)
		}
	}

	// 以 JSON 形式输出资源摘要，便于事件消费者解析。
	message, err := json.Marshal(specResources)
	if err != nil {
		logger.Error(err, "Failed to serialize resource summary", "pod", format.Pod(pod))
		return "", err
	}
	return string(message), nil
}
