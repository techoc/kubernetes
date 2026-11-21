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

package container

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// ImplicitContainerPrefix 是容器名前缀，表示该容器是被隐式启动的（例如 Pod infra 容器）。
var ImplicitContainerPrefix = "implicitly required container "

// GenerateContainerRef 生成指向 Pod 内指定容器的 ObjectReference；
// 若无法定位容器在 Pod 中的字段路径，则使用隐式容器前缀。
func GenerateContainerRef(pod *v1.Pod, container *v1.Container) (*v1.ObjectReference, error) {
	fieldPath, err := fieldPath(pod, container)
	if err != nil {
		// TODO: 需要更合理的方式引用隐式启动的容器（如 Pod infra 容器），当前做法并不理想。
		fieldPath = ImplicitContainerPrefix + container.Name
	}
	ref, err := ref.GetPartialReference(legacyscheme.Scheme, pod, fieldPath)
	if err != nil {
		return nil, err
	}
	return ref, nil
}

// fieldPath 返回容器在 Pod 中的字段路径，依次查找普通、初始化与临时容器。
func fieldPath(pod *v1.Pod, container *v1.Container) (string, error) {
	for i := range pod.Spec.Containers {
		here := &pod.Spec.Containers[i]
		if here.Name == container.Name {
			if here.Name == "" {
				return fmt.Sprintf("spec.containers[%d]", i), nil
			}
			return fmt.Sprintf("spec.containers{%s}", here.Name), nil
		}
	}
	for i := range pod.Spec.InitContainers {
		here := &pod.Spec.InitContainers[i]
		if here.Name == container.Name {
			if here.Name == "" {
				return fmt.Sprintf("spec.initContainers[%d]", i), nil
			}
			return fmt.Sprintf("spec.initContainers{%s}", here.Name), nil
		}
	}
	for i := range pod.Spec.EphemeralContainers {
		here := &pod.Spec.EphemeralContainers[i]
		if here.Name == container.Name {
			if here.Name == "" {
				return fmt.Sprintf("spec.ephemeralContainers[%d]", i), nil
			}
			return fmt.Sprintf("spec.ephemeralContainers{%s}", here.Name), nil
		}
	}
	return "", fmt.Errorf("container %q not found in pod %s/%s", container.Name, pod.Namespace, pod.Name)
}
