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

package podresources

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/metrics"

	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
)

// v1PodResourcesServer 实现 PodResourcesListerServer。
type v1PodResourcesServer struct {
	podsProvider             PodsProvider
	devicesProvider          DevicesProvider
	cpusProvider             CPUsProvider
	memoryProvider           MemoryProvider
	dynamicResourcesProvider DynamicResourcesProvider
	useActivePods            bool
	podresourcesv1.UnsafePodResourcesListerServer
}

// NewV1PodResourcesServer 返回 PodResourcesListerServer；
// Pod 列表来自 PodsProvider，设备信息来自 DevicesProvider。
func NewV1PodResourcesServer(ctx context.Context, providers PodResourcesProviders) podresourcesv1.PodResourcesListerServer {
	logger := klog.FromContext(ctx)
	// 特性开启时只列出活跃 Pod，避免暴露已终止 Pod 的资源。
	useActivePods := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.KubeletPodResourcesListUseActivePods)
	logger.Info("podresources", "method", "list", "useActivePods", useActivePods)
	return &v1PodResourcesServer{
		podsProvider:             providers.Pods,
		devicesProvider:          providers.Devices,
		cpusProvider:             providers.Cpus,
		memoryProvider:           providers.Memory,
		dynamicResourcesProvider: providers.DynamicResources,
		useActivePods:            useActivePods,
	}
}

// List 返回节点上各 Pod 已分配的资源信息。
func (p *v1PodResourcesServer) List(ctx context.Context, req *podresourcesv1.ListPodResourcesRequest) (*podresourcesv1.ListPodResourcesResponse, error) {
	// 记录 v1 端点的请求指标。
	metrics.PodResourcesEndpointRequestsTotalCount.WithLabelValues("v1").Inc()
	metrics.PodResourcesEndpointRequestsListCount.WithLabelValues("v1").Inc()

	logger := klog.FromContext(ctx)

	// 根据特性开关选择活跃 Pod 或全部 Pod。
	var pods []*v1.Pod
	if p.useActivePods {
		// GetActivePods 已过滤终止的 Pod，无需再次过滤。
		pods = p.podsProvider.GetActivePods()
	} else {
		pods = p.podsProvider.GetPods()
	}

	podResources := make([]*podresourcesv1.PodResources, len(pods))
	// 先刷新已分配设备，保证返回的设备信息是最新的。
	p.devicesProvider.UpdateAllocatedDevices(logger)

	for i, pod := range pods {
		pRes := podresourcesv1.PodResources{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			Containers: make([]*podresourcesv1.ContainerResources, 0, len(pod.Spec.Containers)),
		}
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResourceManagers) {
			pRes.CpuIds = p.cpusProvider.GetPodCPUs(string(pod.UID))
			pRes.Memory = p.memoryProvider.GetPodMemory(logger, string(pod.UID))
		}

		// 只为可重启 init 容器（边车）和普通容器上报资源。
		pRes.Containers = make([]*podresourcesv1.ContainerResources, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
		for _, container := range pod.Spec.InitContainers {
			if !podutil.IsRestartableInitContainer(&container) {
				continue
			}

			pRes.Containers = append(pRes.Containers, p.getContainerResources(logger, pod, &container))
		}

		for _, container := range pod.Spec.Containers {
			pRes.Containers = append(pRes.Containers, p.getContainerResources(logger, pod, &container))
		}
		podResources[i] = &pRes
	}

	response := &podresourcesv1.ListPodResourcesResponse{
		PodResources: podResources,
	}
	return response, nil
}

// GetAllocatableResources 返回服务器已知的全部资源，
// 更接近"容量"而非当前空闲量。
func (p *v1PodResourcesServer) GetAllocatableResources(ctx context.Context, req *podresourcesv1.AllocatableResourcesRequest) (*podresourcesv1.AllocatableResourcesResponse, error) {
	logger := klog.FromContext(ctx)
	metrics.PodResourcesEndpointRequestsTotalCount.WithLabelValues("v1").Inc()
	metrics.PodResourcesEndpointRequestsGetAllocatableCount.WithLabelValues("v1").Inc()

	// 汇总设备、CPU 与内存的可分配总量。
	response := &podresourcesv1.AllocatableResourcesResponse{
		Devices: p.devicesProvider.GetAllocatableDevices(logger),
		CpuIds:  p.cpusProvider.GetAllocatableCPUs(),
		Memory:  p.memoryProvider.GetAllocatableMemory(logger),
	}

	return response, nil
}

// Get 返回指定 Pod 已分配的资源信息。
func (p *v1PodResourcesServer) Get(ctx context.Context, req *podresourcesv1.GetPodResourcesRequest) (*podresourcesv1.GetPodResourcesResponse, error) {
	metrics.PodResourcesEndpointRequestsTotalCount.WithLabelValues("v1").Inc()
	metrics.PodResourcesEndpointRequestsGetCount.WithLabelValues("v1").Inc()

	logger := klog.FromContext(ctx)

	// Pod 不存在时记录错误指标并返回错误。
	pod, exist := p.podsProvider.GetPodByName(req.PodNamespace, req.PodName)
	if !exist {
		metrics.PodResourcesEndpointErrorsGetCount.WithLabelValues("v1").Inc()
		return nil, fmt.Errorf("pod %s in namespace %s not found", req.PodName, req.PodNamespace)
	}

	// 构造 Pod 级资源信息；启用 PodLevelResourceManagers 时补充 Pod 级 CPU/内存。
	podResources := &podresourcesv1.PodResources{
		Name:       pod.Name,
		Namespace:  pod.Namespace,
		Containers: make([]*podresourcesv1.ContainerResources, 0, len(pod.Spec.Containers)),
	}
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResourceManagers) {
		podResources.CpuIds = p.cpusProvider.GetPodCPUs(string(pod.UID))
		podResources.Memory = p.memoryProvider.GetPodMemory(logger, string(pod.UID))
	}

	podResources.Containers = make([]*podresourcesv1.ContainerResources, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	for _, container := range pod.Spec.InitContainers {
		if !podutil.IsRestartableInitContainer(&container) {
			continue
		}

		podResources.Containers = append(podResources.Containers, p.getContainerResources(logger, pod, &container))
	}

	for _, container := range pod.Spec.Containers {
		podResources.Containers = append(podResources.Containers, p.getContainerResources(logger, pod, &container))
	}

	response := &podresourcesv1.GetPodResourcesResponse{
		PodResources: podResources,
	}
	return response, nil
}

// getContainerResources 汇总单个容器的设备、CPU、内存与动态资源分配。
func (p *v1PodResourcesServer) getContainerResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) *podresourcesv1.ContainerResources {
	containerResources := &podresourcesv1.ContainerResources{
		Name:             container.Name,
		Devices:          p.devicesProvider.GetDevices(string(pod.UID), container.Name),
		CpuIds:           p.cpusProvider.GetCPUs(pod, container),
		Memory:           p.memoryProvider.GetMemory(logger, pod, container),
		DynamicResources: p.dynamicResourcesProvider.GetDynamicResources(logger, pod, container),
	}
	return containerResources
}
