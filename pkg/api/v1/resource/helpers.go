/*
Copyright 2014 The Kubernetes Authors.

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

package resource

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/features"
)

// GetResourceRequestQuantity 查找并返回 Pod 对某种资源的总 request（Quantity 形式）。
// 优先使用 Pod 级别资源（特性开启且已设置时），再加上 Pod 的 overhead。
func GetResourceRequestQuantity(pod *v1.Pod, resourceName v1.ResourceName) resource.Quantity {
	requestQuantity := resource.Quantity{}

	switch resourceName {
	case v1.ResourceCPU:
		requestQuantity = resource.Quantity{Format: resource.DecimalSI}
	case v1.ResourceMemory, v1.ResourceStorage, v1.ResourceEphemeralStorage:
		requestQuantity = resource.Quantity{Format: resource.BinarySI}
	default:
		requestQuantity = resource.Quantity{Format: resource.DecimalSI}
	}

	// Pod 级别资源已设置时优先用它，忽略容器级别的资源，避免重复计数。
	hasPodLevelResources := utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod)

	// TODO(pravk03): 驱逐排序还需考虑 DRA 的可分配资源。
	if rQuantity, ok := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{SkipContainerLevelResources: hasPodLevelResources, ExcludeOverhead: true})[resourceName]; ok {
		requestQuantity.Add(rQuantity)
	}

	// 把 Pod 的额外开销计入总量，但只在总量非零时加，
	// 避免给完全没有该资源需求的 Pod 凭空造出开销。
	if pod.Spec.Overhead != nil {
		if podOverhead, ok := pod.Spec.Overhead[resourceName]; ok && !requestQuantity.IsZero() {
			requestQuantity.Add(podOverhead)
		}
	}

	return requestQuantity
}

// GetResourceRequest 返回 Pod 对某种资源的 request 数值。
// CPU 以毫核（MilliValue）表示，其他资源用整数值；ResourcePods 固定返回 1。
func GetResourceRequest(pod *v1.Pod, resource v1.ResourceName) int64 {
	if resource == v1.ResourcePods {
		return 1
	}

	requestQuantity := GetResourceRequestQuantity(pod, resource)

	if resource == v1.ResourceCPU {
		return requestQuantity.MilliValue()
	}

	return requestQuantity.Value()
}

// ExtractResourceValueByContainerName 按容器名提取资源字段（如 requests.cpu）的值。
func ExtractResourceValueByContainerName(fs *v1.ResourceFieldSelector, pod *v1.Pod, containerName string) (string, error) {
	container, err := findContainerInPod(pod, containerName)
	if err != nil {
		return "", err
	}
	return ExtractContainerResourceValue(fs, container)
}

// ExtractResourceValueByContainerNameAndNodeAllocatable 在提取资源值前，
// 先把容器未设置的 limits 用节点可分配量补齐（大页除外）。
func ExtractResourceValueByContainerNameAndNodeAllocatable(fs *v1.ResourceFieldSelector, pod *v1.Pod, containerName string, nodeAllocatable v1.ResourceList) (string, error) {
	realContainer, err := findContainerInPod(pod, containerName)
	if err != nil {
		return "", err
	}

	container := realContainer.DeepCopy()

	MergeContainerResourceLimits(container, nodeAllocatable)

	return ExtractContainerResourceValue(fs, container)
}

// ExtractContainerResourceValue 从已知容器中提取 Downward API 资源字段的值。
// 支持 CPU/内存/临时存储的 requests/limits，以及动态命名的 hugepages-* 资源。
func ExtractContainerResourceValue(fs *v1.ResourceFieldSelector, container *v1.Container) (string, error) {
	divisor := resource.Quantity{}
	if divisor.Cmp(fs.Divisor) == 0 {
		divisor = resource.MustParse("1")
	} else {
		divisor = fs.Divisor
	}

	switch fs.Resource {
	case "limits.cpu":
		return convertResourceCPUToString(container.Resources.Limits.Cpu(), divisor)
	case "limits.memory":
		return convertResourceMemoryToString(container.Resources.Limits.Memory(), divisor)
	case "limits.ephemeral-storage":
		return convertResourceEphemeralStorageToString(container.Resources.Limits.StorageEphemeral(), divisor)
	case "requests.cpu":
		return convertResourceCPUToString(container.Resources.Requests.Cpu(), divisor)
	case "requests.memory":
		return convertResourceMemoryToString(container.Resources.Requests.Memory(), divisor)
	case "requests.ephemeral-storage":
		return convertResourceEphemeralStorageToString(container.Resources.Requests.StorageEphemeral(), divisor)
	}
	// 处理名称中带参数的扩展资源，
	// 例如 requests.hugepages-<pageSize>。
	if strings.HasPrefix(fs.Resource, "requests.") {
		resourceName := v1.ResourceName(strings.TrimPrefix(fs.Resource, "requests."))
		if IsHugePageResourceName(resourceName) {
			return convertResourceHugePagesToString(container.Resources.Requests.Name(resourceName, resource.BinarySI), divisor)
		}
	}
	if strings.HasPrefix(fs.Resource, "limits.") {
		resourceName := v1.ResourceName(strings.TrimPrefix(fs.Resource, "limits."))
		if IsHugePageResourceName(resourceName) {
			return convertResourceHugePagesToString(container.Resources.Limits.Name(resourceName, resource.BinarySI), divisor)
		}
	}
	return "", fmt.Errorf("unsupported container resource : %v", fs.Resource)
}

// convertResourceCPUToString 按除数换算 CPU 值并向上取整。
func convertResourceCPUToString(cpu *resource.Quantity, divisor resource.Quantity) (string, error) {
	c := int64(math.Ceil(float64(cpu.MilliValue()) / float64(divisor.MilliValue())))
	return strconv.FormatInt(c, 10), nil
}

// convertResourceMemoryToString 按除数换算内存值并向上取整。
func convertResourceMemoryToString(memory *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(memory.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// convertResourceHugePagesToString 按除数换算大页值并向上取整。
func convertResourceHugePagesToString(hugePages *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(hugePages.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// convertResourceEphemeralStorageToString 按除数换算临时存储值并向上取整。
func convertResourceEphemeralStorageToString(ephemeralStorage *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(ephemeralStorage.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// findContainerInPod 按名称在 Pod 的普通容器和 init 容器中查找容器。
func findContainerInPod(pod *v1.Pod, containerName string) (*v1.Container, error) {
	for _, container := range pod.Spec.Containers {
		if container.Name == containerName {
			return &container, nil
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if container.Name == containerName {
			return &container, nil
		}
	}
	return nil, fmt.Errorf("container %s not found", containerName)
}

// MergeContainerResourceLimits 为容器未设置（或为 0）的 CPU/内存/临时存储 limits，
// 填入节点的可分配量。大页不参与：大页不可超售，容器总会有显式 limit。
func MergeContainerResourceLimits(container *v1.Container,
	allocatable v1.ResourceList) {
	if container.Resources.Limits == nil {
		container.Resources.Limits = make(v1.ResourceList)
	}
	// 大页不可超售，容器一定会有显式的 limit，因此不参与补齐。
	// 若用节点可分配量去填大页 limit 反而是错误的值。
	for _, resource := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory, v1.ResourceEphemeralStorage} {
		if quantity, exists := container.Resources.Limits[resource]; !exists || quantity.IsZero() {
			if cap, exists := allocatable[resource]; exists {
				container.Resources.Limits[resource] = cap.DeepCopy()
			}
		}
	}
}

// IsHugePageResourceName 判断资源名是否是大页资源（以 hugepages- 为前缀）。
func IsHugePageResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}
