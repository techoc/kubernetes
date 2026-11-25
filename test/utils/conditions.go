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

package utils

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

type ContainerFailures struct {
	status   *v1.ContainerStateTerminated
	Restarts int
}

// PodRunningReady 检查 Pod p 的阶段是否为 Running，
// 并且是否具有状态为 true 的 Ready condition。
func PodRunningReady(p *v1.Pod) (bool, error) {
	// 检查阶段是否为 Running。
	if p.Status.Phase != v1.PodRunning {
		return false, fmt.Errorf("want pod '%s' on '%s' to be '%v' but was '%v'",
			p.ObjectMeta.Name, p.Spec.NodeName, v1.PodRunning, p.Status.Phase)
	}
	// 检查 Ready condition 是否为 true。
	if !podutil.IsPodReady(p) {
		return false, fmt.Errorf("pod '%s' on '%s' didn't have condition {%v %v}; conditions: %v",
			p.ObjectMeta.Name, p.Spec.NodeName, v1.PodReady, v1.ConditionTrue, p.Status.Conditions)
	}
	return true, nil
}

func PodRunningReadyOrSucceeded(p *v1.Pod) (bool, error) {
	// 检查阶段是否为 Succeeded。
	if p.Status.Phase == v1.PodSucceeded {
		return true, nil
	}
	return PodRunningReady(p)
}

func PodSucceeded(p *v1.Pod) (bool, error) {
	return p.Status.Phase == v1.PodSucceeded, nil
}

// FailedContainers 检查 Pod 中的所有容器，并返回已失败或
// 已重启容器的失败信息。
// 返回一个 map，其中键为 containerID，值为
// 包含重启和失败信息的结构体
func FailedContainers(pod *v1.Pod) map[string]ContainerFailures {
	var state ContainerFailures
	states := make(map[string]ContainerFailures)

	statuses := pod.Status.ContainerStatuses
	if len(statuses) == 0 {
		return nil
	}
	for _, status := range statuses {
		if status.State.Terminated != nil {
			states[status.ContainerID] = ContainerFailures{status: status.State.Terminated}
		} else if status.LastTerminationState.Terminated != nil {
			states[status.ContainerID] = ContainerFailures{status: status.LastTerminationState.Terminated}
		}
		if status.RestartCount > 0 {
			var ok bool
			if state, ok = states[status.ContainerID]; !ok {
				state = ContainerFailures{}
			}
			state.Restarts = int(status.RestartCount)
			states[status.ContainerID] = state
		}
	}

	return states
}

// TerminatedContainers 检查 Pod 中的所有容器，并返回一个 map，
// 其中包含所有当前已终止容器的
// "容器名: 终止原因"。
func TerminatedContainers(pod *v1.Pod) map[string]string {
	states := make(map[string]string)
	statuses := pod.Status.ContainerStatuses
	if len(statuses) == 0 {
		return states
	}
	for _, status := range statuses {
		if status.State.Terminated != nil {
			states[status.Name] = status.State.Terminated.Reason
		}
	}
	return states
}
