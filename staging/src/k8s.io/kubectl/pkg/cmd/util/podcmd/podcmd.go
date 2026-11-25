/*
Copyright 2021 The Kubernetes Authors.

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

package podcmd

import (
	"fmt"
	"io"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// DefaultContainerAnnotationName 是一个 annotation 名称，运行 kubectl 时可用于
// 预先从 pod 中选出目标容器。
const DefaultContainerAnnotationName = "kubectl.kubernetes.io/default-container"

// FindContainerByName 从给定 pod 的 spec 中选出指定名称的容器，
// 若不存在则返回 nil。
func FindContainerByName(pod *v1.Pod, name string) (*v1.Container, string) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i], fmt.Sprintf("spec.containers{%s}", name)
		}
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return &pod.Spec.InitContainers[i], fmt.Sprintf("spec.initContainers{%s}", name)
		}
	}
	for i := range pod.Spec.EphemeralContainers {
		if pod.Spec.EphemeralContainers[i].Name == name {
			return (*v1.Container)(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon), fmt.Sprintf("spec.ephemeralContainers{%s}", name)
		}
	}
	return nil, ""
}

// FindOrDefaultContainerByName 在 pod 中存在容器时默认选择第一个容器，
// 否则返回错误。如果容器不止一个，它会向用户打印一条消息，
// 说明已选定默认容器。
func FindOrDefaultContainerByName(pod *v1.Pod, name string, quiet bool, warn io.Writer) (*v1.Container, error) {
	// 这理论上不会发生，但以防万一
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("pod %s/%s does not have any containers", pod.Namespace, pod.Name)
	}

	// 如果未指定容器名称，则按照以下文档从 annotation 中读取默认容器：
	// https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/2227-kubectl-default-container
	if len(name) == 0 {
		name = pod.Annotations[DefaultContainerAnnotationName]
		if len(name) > 0 {
			klog.V(4).Infof("Attempting default container name from annotation %s", name)
		}
	}

	if len(name) > 0 {
		container, _ := FindContainerByName(pod, name)
		if container == nil {
			return nil, fmt.Errorf("container %s is not valid for pod %s out of: %s", name, pod.Name, AllContainerNames(pod))
		}
		return container, nil
	}

	// 回退到选择第一个容器，以保持现有行为
	container := &pod.Spec.Containers[0]
	if !quiet && (len(pod.Spec.Containers) > 1 || len(pod.Spec.InitContainers) > 0 || len(pod.Spec.EphemeralContainers) > 0) {
		fmt.Fprintf(warn, "Defaulted container %q out of: %s\n", container.Name, AllContainerNames(pod)) // nolint:errcheck
	}
	klog.V(4).Infof("Using the first container in a pod: %s", container.Name)
	return container, nil
}

// AllContainerNames 返回 pod 中所有容器名称的逗号分隔列表，
// 按顺序包括普通容器、ephemeral 容器（标记为 "ephem"）以及 init
// 容器（标记为 "init"）。
func AllContainerNames(pod *v1.Pod) string {
	var containers []string
	for _, container := range pod.Spec.Containers {
		containers = append(containers, container.Name)
	}
	for _, container := range pod.Spec.EphemeralContainers {
		containers = append(containers, fmt.Sprintf("%s (ephem)", container.Name))
	}
	for _, container := range pod.Spec.InitContainers {
		containers = append(containers, fmt.Sprintf("%s (init)", container.Name))
	}
	return strings.Join(containers, ", ")
}
