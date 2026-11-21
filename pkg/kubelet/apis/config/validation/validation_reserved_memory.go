/*
Copyright 2018 The Kubernetes Authors.

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

package validation

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// validateReservedMemoryConfiguration 校验预留内存配置，返回所有非法项的错误列表。
func validateReservedMemoryConfiguration(kc *kubeletconfig.KubeletConfiguration) []error {
	if len(kc.ReservedMemory) == 0 {
		return nil
	}

	var errors []error

	numaTypeDuplicates := map[int32]map[v1.ResourceName]bool{}
	for _, reservedMemory := range kc.ReservedMemory {
		numaNode := reservedMemory.NumaNode
		if _, ok := numaTypeDuplicates[numaNode]; !ok {
			numaTypeDuplicates[numaNode] = map[v1.ResourceName]bool{}
		}

		for resourceName, q := range reservedMemory.Limits {
			if !reservedMemorySupportedLimit(resourceName) {
				errors = append(errors, fmt.Errorf("invalid configuration: the limit type %q for NUMA node %d is not supported, only %v is accepted", resourceName, numaNode, []v1.ResourceName{v1.ResourceMemory, v1.ResourceHugePagesPrefix + "<HugePageSize>"}))
			}

			// 校验 limit 值不能为零。
			if q.IsZero() {
				errors = append(errors, fmt.Errorf("invalid configuration: reserved memory may not be zero for NUMA node %d and resource %q", numaNode, resourceName))
			}

			// 校验同一 NUMA 节点上同一资源类型不能重复配置。
			if _, ok := numaTypeDuplicates[numaNode][resourceName]; ok {
				errors = append(errors, fmt.Errorf("invalid configuration: the reserved memory has a duplicate value for NUMA node %d and resource %q", numaNode, resourceName))
			}
			numaTypeDuplicates[numaNode][resourceName] = true
		}
	}
	return errors
}

// reservedMemorySupportedLimit 判断资源名是否为内存或 HugePage 资源。
func reservedMemorySupportedLimit(resourceName v1.ResourceName) bool {
	return corev1helper.IsHugePageResourceName(resourceName) || resourceName == v1.ResourceMemory
}
