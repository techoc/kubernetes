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

package endpointslice

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// ControllerUpdateTimeout 是 Pod 或 Service 变更反映到
	// 其 EndpointSlices 的最大预期等待时间。（与
	// e2eservice.ServiceEndpointsTimeout 相同，但这里复制一份以避免 import 循环。）
	ControllerUpdateTimeout = 2 * time.Minute
)

type EndpointSliceConditionFunc func(ctx context.Context, endpointSlices []discoveryv1.EndpointSlice) (done bool, err error)

// WaitForEndpointSlices 是 EndpointSlice 专用的包装器，围绕
// wait.PollUntilContextTimeout，用 serviceName 的 EndpointSlices
// 列表轮询 conditionFunc。
func WaitForEndpointSlices(ctx context.Context, cs clientset.Interface, namespace, serviceName string, interval, timeout time.Duration, conditionFunc EndpointSliceConditionFunc) error {
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		esList, err := cs.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", discoveryv1.LabelServiceName, serviceName)})
		if err != nil {
			framework.Logf("Unexpected error trying to get EndpointSlices for %s/%s: %v", namespace, serviceName, err)
			return false, nil
		}
		return conditionFunc(ctx, esList.Items)
	})
}

// WaitForEndpointCount 等待（最多 ControllerUpdateTimeout）指定服务的
// EndpointSlice 至少有一个，且所有 EndpointSlice 的 `Endpoints` 数组
// 总长度为 expectNum。（简单服务等价于等待恰好有 expectNum 个
// pod 支撑该服务，但双栈服务或带命名端口的服务中，
// `Endpoints` 与 pod 可能不是一一对应。另注意等待 0 个 endpoint 表示
// "等待 Service 存在但没有 endpoint"，而不是"等待 Service
// 被删除"。）
func WaitForEndpointCount(ctx context.Context, cs clientset.Interface, namespace, serviceName string, expectNum int) error {
	framework.Logf("Waiting for amount of service %s/%s endpoints to be %d", namespace, serviceName, expectNum)
	return WaitForEndpointSlices(ctx, cs, namespace, serviceName, time.Second, ControllerUpdateTimeout, func(ctx context.Context, endpointSlices []discoveryv1.EndpointSlice) (bool, error) {
		if len(endpointSlices) == 0 {
			framework.Logf("Waiting for at least 1 EndpointSlice to exist")
			return false, nil
		}

		// 同一个地址可能出现在多个 EndpointSlice 上
		addresses := sets.Set[string]{}
		for _, epSlice := range endpointSlices {
			for _, ep := range epSlice.Endpoints {
				if len(ep.Addresses) > 0 {
					addresses.Insert(ep.Addresses[0])
				}
			}
		}

		if addresses.Len() != expectNum {
			framework.Logf("Unexpected number of Endpoints on Slices, got %d, expected %d", addresses.Len(), expectNum)
			return false, nil
		}
		return true, nil
	})
}

// WaitForEndpointPods 等待（最多 ControllerUpdateTimeout）指定服务至少有一个
// EndpointSlice，且所有 slice 的 `Endpoints` 集合恰好覆盖
// expectPods 中的 pod。（注意双栈服务上它可能提前返回，
// 因为第一个 EndpointSlice 通常已覆盖所有 pod。）
func WaitForEndpointPods(ctx context.Context, cs clientset.Interface, namespace, serviceName string, expectPods ...string) error {
	framework.Logf("Waiting for service %s/%s to have endpoints pointing to %v", namespace, serviceName, expectPods)
	expectPodSet := sets.New(expectPods...)
	return WaitForEndpointSlices(ctx, cs, namespace, serviceName, time.Second, ControllerUpdateTimeout, func(ctx context.Context, endpointSlices []discoveryv1.EndpointSlice) (bool, error) {
		if len(endpointSlices) == 0 {
			framework.Logf("Waiting for at least 1 EndpointSlice to exist")
			return false, nil
		}

		podSet := sets.Set[string]{}
		for _, epSlice := range endpointSlices {
			for _, ep := range epSlice.Endpoints {
				if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" && ep.TargetRef.Namespace == namespace {
					podSet.Insert(ep.TargetRef.Name)
				}
			}
		}

		if !podSet.Equal(expectPodSet) {
			framework.Logf("Unexpected endpoints on slices, missing: %v, extra: %v", sets.List(expectPodSet.Difference(podSet)), sets.List(podSet.Difference(expectPodSet)))
			return false, nil
		}
		return true, nil
	})
}

// PortMapping 包含 WaitForEndpointPorts 使用的数据
type PortMapping struct {
	// Name 是 ServicePort / EndpointPort 的名称
	Name string
	// Protocol 是协议（为空时默认 TCP）
	Protocol v1.Protocol
	// Target 是 endpoint Pod 的名称
	Target string
	// TargetPort 是 Target 上的端口
	TargetPort int32
}

// WaitForEndpointPorts 等待（最多 ControllerUpdateTimeout）指定服务至少有一个
// EndpointSlice，且所有 slice 的 `Endpoints` 集合恰好覆盖
// portMappings。（注意双栈服务上它可能提前返回，
// 因为单一地址族的
// EndpointSlice 通常已覆盖所有端口。）
func WaitForEndpointPorts(ctx context.Context, cs clientset.Interface, namespace, service string, portMappings []PortMapping) error {
	framework.Logf("Waiting for service %s/%s to have endpoints for ports %v", namespace, service, portMappings)

	for i := range portMappings {
		if portMappings[i].Protocol == "" {
			portMappings[i].Protocol = v1.ProtocolTCP
		}
	}
	expectMappings := sets.New(portMappings...)

	return WaitForEndpointSlices(ctx, cs, namespace, service, time.Second, ControllerUpdateTimeout, func(ctx context.Context, endpointSlices []discoveryv1.EndpointSlice) (bool, error) {
		if len(endpointSlices) == 0 {
			framework.Logf("Waiting for at least 1 EndpointSlice to exist")
			return false, nil
		}

		gotMappings := sets.New[PortMapping]()
		for _, epSlice := range endpointSlices {
			for _, endpoint := range epSlice.Endpoints {
				for _, port := range epSlice.Ports {
					gotMappings.Insert(PortMapping{
						Name:       *port.Name,
						Protocol:   *port.Protocol,
						Target:     endpoint.TargetRef.Name,
						TargetPort: *port.Port,
					})
				}
			}
		}

		if !gotMappings.Equal(expectMappings) {
			framework.Logf("Unexpected port mappings on slices, missing: %v, extra: %v", expectMappings.Difference(gotMappings).UnsortedList(), gotMappings.Difference(expectMappings).UnsortedList())
			return false, nil
		}

		return true, nil
	})
}
