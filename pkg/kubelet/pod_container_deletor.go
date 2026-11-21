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
	"sort"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

const (
	// containerDeletorBufferLimit 是容器删除请求缓冲区的上限。
	// 该数值带有一定随意性，后续可能调整。
	containerDeletorBufferLimit = 50
)

// containerStatusbyCreatedList 按创建时间排序的容器状态列表。
type containerStatusbyCreatedList []*kubecontainer.Status

// podContainerDeletor 异步删除 Pod 中超出保留数量的已退出容器。
type podContainerDeletor struct {
	// worker 是删除请求通道。
	worker chan<- kubecontainer.ContainerID
	// containersToKeep 是每个 Pod 需保留的已退出容器数量。
	containersToKeep int
}

// Len 返回列表长度。
func (a containerStatusbyCreatedList) Len() int { return len(a) }

// Swap 交换两个元素。
func (a containerStatusbyCreatedList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 按创建时间倒序排列，最新的在前。
func (a containerStatusbyCreatedList) Less(i, j int) bool {
	return a[i].CreatedAt.After(a[j].CreatedAt)
}

// newPodContainerDeletor 创建容器删除器并启动后台删除协程。
func newPodContainerDeletor(logger klog.Logger, runtime kubecontainer.Runtime, containersToKeep int) *podContainerDeletor {
	buffer := make(chan kubecontainer.ContainerID, containerDeletorBufferLimit)
	// 删除协程不挂在 kubelet 的主上下文树上，避免随主上下文取消而中断，
	// 同时仍把调用方的 logger 传入 DeleteContainer。
	deleteCtx := klog.NewContext(context.TODO(), logger)
	go wait.Until(func() {
		for {
			id := <-buffer
			if err := runtime.DeleteContainer(deleteCtx, id); err != nil {
				logger.Info("DeleteContainer returned error", "containerID", id, "err", err)
			}
		}
	}, 0, wait.NeverStop)

	return &podContainerDeletor{
		worker:           buffer,
		containersToKeep: containersToKeep,
	}
}

// getContainersToDeleteInPod 返回 Pod 中需要删除的已退出容器。
// 若 filterContainerID 非空，只考虑与该容器同名的容器；否则考虑全部已退出容器。
// 结果按创建时间从新到旧排序，并跳过需要保留的最新若干容器。
func getContainersToDeleteInPod(logger klog.Logger, filterContainerID string, podStatus *kubecontainer.PodStatus, containersToKeep int) containerStatusbyCreatedList {
	// 若指定了容器 ID，先找到对应容器，以确定要清理的同名容器。
	matchedContainer := func(filterContainerId string, podStatus *kubecontainer.PodStatus) *kubecontainer.Status {
		if filterContainerId == "" {
			return nil
		}
		for _, containerStatus := range podStatus.ContainerStatuses {
			if containerStatus.ID.ID == filterContainerId {
				return containerStatus
			}
		}
		return nil
	}(filterContainerID, podStatus)

	// 指定容器不存在时无事可做。
	if filterContainerID != "" && matchedContainer == nil {
		logger.Info("Container not found in pod's containers", "containerID", filterContainerID)
		return containerStatusbyCreatedList{}
	}

	// 找出与指定容器同名的已退出容器。
	var candidates containerStatusbyCreatedList
	for _, containerStatus := range podStatus.ContainerStatuses {
		if containerStatus.State != kubecontainer.ContainerStateExited {
			continue
		}
		if matchedContainer == nil || matchedContainer.Name == containerStatus.Name {
			candidates = append(candidates, containerStatus)
		}
	}

	// 未超过保留数量时无需删除。
	if len(candidates) <= containersToKeep {
		return containerStatusbyCreatedList{}
	}
	// 按创建时间从新到旧排序，保留最新的若干容器。
	sort.Sort(candidates)
	return candidates[containersToKeep:]
}

// deleteContainersInPod 为 getContainersToDeleteInPod 选出的容器发送删除请求。
func (p *podContainerDeletor) deleteContainersInPod(logger klog.Logger, filterContainerID string, podStatus *kubecontainer.PodStatus, removeAll bool) {
	containersToKeep := p.containersToKeep
	if removeAll {
		// removeAll 表示不保留任何已退出容器。
		containersToKeep = 0
		filterContainerID = ""
	}

	for _, candidate := range getContainersToDeleteInPod(logger, filterContainerID, podStatus, containersToKeep) {
		// 非阻塞投递；队列满时只记录日志，避免阻塞同步循环。
		select {
		case p.worker <- candidate.ID:
		default:
			logger.Info("Failed to issue the request to remove container", "containerID", candidate.ID)
		}
	}
}
