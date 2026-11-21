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

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/metrics"

	v1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

// v1alpha1PodResourcesServer 实现 PodResourcesListerServer 的 v1alpha1 版本。
type v1alpha1PodResourcesServer struct {
	// podsProvider 提供节点上的 Pod 列表。
	podsProvider PodsProvider
	// devicesProvider 提供已分配设备信息。
	devicesProvider DevicesProvider
	v1alpha1.UnsafePodResourcesListerServer
}

// NewV1alpha1PodResourcesServer 创建 v1alpha1 的 PodResourcesListerServer，
// Pod 列表来自 PodsProvider，设备信息来自 DevicesProvider。
func NewV1alpha1PodResourcesServer(providers PodResourcesProviders) v1alpha1.PodResourcesListerServer {
	return &v1alpha1PodResourcesServer{
		podsProvider:    providers.Pods,
		devicesProvider: providers.Devices,
	}
}

// v1DevicesToAlphaV1 把 v1 容器设备转换为 v1alpha1 格式。
func v1DevicesToAlphaV1(alphaDevs []*v1.ContainerDevices) []*v1alpha1.ContainerDevices {
	var devs []*v1alpha1.ContainerDevices
	for _, alphaDev := range alphaDevs {
		dev := v1alpha1.ContainerDevices{
			ResourceName: alphaDev.ResourceName,
			DeviceIds:    alphaDev.DeviceIds,
		}
		devs = append(devs, &dev)
	}

	return devs
}

// List 返回节点上各 Pod 已分配的资源信息。
func (p *v1alpha1PodResourcesServer) List(ctx context.Context, req *v1alpha1.ListPodResourcesRequest) (*v1alpha1.ListPodResourcesResponse, error) {
	logger := klog.FromContext(ctx)
	// 记录 v1alpha1 端点的请求数。
	metrics.PodResourcesEndpointRequestsTotalCount.WithLabelValues("v1alpha1").Inc()
	pods := p.podsProvider.GetPods()
	podResources := make([]*v1alpha1.PodResources, len(pods))
	// 刷新已分配设备信息，确保返回最新结果。
	p.devicesProvider.UpdateAllocatedDevices(logger)

	// 逐个 Pod、逐个容器组装资源信息。
	for i, pod := range pods {
		pRes := v1alpha1.PodResources{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			Containers: make([]*v1alpha1.ContainerResources, len(pod.Spec.Containers)),
		}

		for j, container := range pod.Spec.Containers {
			pRes.Containers[j] = &v1alpha1.ContainerResources{
				Name:    container.Name,
				Devices: v1DevicesToAlphaV1(p.devicesProvider.GetDevices(string(pod.UID), container.Name)),
			}
		}
		podResources[i] = &pRes
	}

	return &v1alpha1.ListPodResourcesResponse{
		PodResources: podResources,
	}, nil
}
