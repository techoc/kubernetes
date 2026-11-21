/*
Copyright 2019 The Kubernetes Authors.

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

package reconcilers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryclient "k8s.io/client-go/kubernetes/typed/discovery/v1"
	utilnet "k8s.io/utils/net"
)

// EndpointsAdapter 提供读写 Endpoints 与 EndpointSlice 的简单接口。
// 注意：这是不完整的适配器，仅适用于本包——这里的 Endpoints 总是端口一致、单个 subset 且地址较少，
// 从而保持 1:1 映射。更复杂的 Endpoints 会拆成多个 EndpointSlice，显著增加复杂度。
type EndpointsAdapter struct {
	endpointClient      corev1client.EndpointsGetter
	endpointSliceClient discoveryclient.EndpointSlicesGetter
}

// NewEndpointsAdapter 返回一个新的 EndpointsAdapter。
func NewEndpointsAdapter(endpointClient corev1client.EndpointsGetter, endpointSliceClient discoveryclient.EndpointSlicesGetter) EndpointsAdapter {
	return EndpointsAdapter{
		endpointClient:      endpointClient,
		endpointSliceClient: endpointSliceClient,
	}
}

// Get 根据 namespace 与 name 读取 Endpoints 对象。
func (adapter *EndpointsAdapter) Get(namespace, name string, getOpts metav1.GetOptions) (*corev1.Endpoints, error) {
	return adapter.endpointClient.Endpoints(namespace).Get(context.TODO(), name, getOpts)
}

// Create 创建 Endpoints 对象及对应的 EndpointSlice。
func (adapter *EndpointsAdapter) Create(namespace string, endpoints *corev1.Endpoints) (*corev1.Endpoints, error) {
	endpoints, err := adapter.endpointClient.Endpoints(namespace).Create(context.TODO(), endpoints, metav1.CreateOptions{})
	if err == nil {
		err = adapter.EnsureEndpointSliceFromEndpoints(namespace, endpoints)
	}
	return endpoints, err
}

// Update 更新 Endpoints 对象及对应的 EndpointSlice。
func (adapter *EndpointsAdapter) Update(namespace string, endpoints *corev1.Endpoints) (*corev1.Endpoints, error) {
	endpoints, err := adapter.endpointClient.Endpoints(namespace).Update(context.TODO(), endpoints, metav1.UpdateOptions{})
	if err == nil {
		err = adapter.EnsureEndpointSliceFromEndpoints(namespace, endpoints)
	}
	return endpoints, err
}

// EnsureEndpointSliceFromEndpoints 确保 EndpointSlice 与 Endpoints 一致，必要时创建或更新。
func (adapter *EndpointsAdapter) EnsureEndpointSliceFromEndpoints(namespace string, endpoints *corev1.Endpoints) error {
	endpointSlice := endpointSliceFromEndpoints(endpoints)
	currentEndpointSlice, err := adapter.endpointSliceClient.EndpointSlices(namespace).Get(context.TODO(), endpointSlice.Name, metav1.GetOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			if _, err = adapter.endpointSliceClient.EndpointSlices(namespace).Create(context.TODO(), endpointSlice, metav1.CreateOptions{}); errors.IsAlreadyExists(err) {
				err = nil
			}
		}
		return err
	}

	// 地址类型从 IP 过渡到 IPv4 时需要重建 EndpointSlice。
	if currentEndpointSlice.AddressType != endpointSlice.AddressType {
		err = adapter.endpointSliceClient.EndpointSlices(namespace).Delete(context.TODO(), endpointSlice.Name, metav1.DeleteOptions{})
		if err != nil {
			return err
		}
		_, err = adapter.endpointSliceClient.EndpointSlices(namespace).Create(context.TODO(), endpointSlice, metav1.CreateOptions{})
		return err
	}

	if apiequality.Semantic.DeepEqual(currentEndpointSlice.Endpoints, endpointSlice.Endpoints) &&
		apiequality.Semantic.DeepEqual(currentEndpointSlice.Ports, endpointSlice.Ports) &&
		apiequality.Semantic.DeepEqual(currentEndpointSlice.Labels, endpointSlice.Labels) {
		return nil
	}

	_, err = adapter.endpointSliceClient.EndpointSlices(namespace).Update(context.TODO(), endpointSlice, metav1.UpdateOptions{})
	return err
}

// endpointSliceFromEndpoints 根据 Endpoints 生成 EndpointSlice。
func endpointSliceFromEndpoints(endpoints *corev1.Endpoints) *discovery.EndpointSlice {
	endpointSlice := &discovery.EndpointSlice{}
	endpointSlice.Name = endpoints.Name
	endpointSlice.Namespace = endpoints.Namespace
	endpointSlice.Labels = map[string]string{discovery.LabelServiceName: endpoints.Name}

	// TODO: 在此处（及 EndpointsAdapter 其余部分）支持双栈。
	endpointSlice.AddressType = discovery.AddressTypeIPv4

	if len(endpoints.Subsets) > 0 {
		subset := endpoints.Subsets[0]
		for i := range subset.Ports {
			endpointSlice.Ports = append(endpointSlice.Ports, discovery.EndpointPort{
				Port:     &subset.Ports[i].Port,
				Name:     &subset.Ports[i].Name,
				Protocol: &subset.Ports[i].Protocol,
			})
		}

		if allAddressesIPv6(append(subset.Addresses, subset.NotReadyAddresses...)) {
			endpointSlice.AddressType = discovery.AddressTypeIPv6
		}

		endpointSlice.Endpoints = append(endpointSlice.Endpoints, getEndpointsFromAddresses(subset.Addresses, endpointSlice.AddressType, true)...)
		endpointSlice.Endpoints = append(endpointSlice.Endpoints, getEndpointsFromAddresses(subset.NotReadyAddresses, endpointSlice.AddressType, false)...)
	}

	return endpointSlice
}

// getEndpointsFromAddresses 返回与给定地址类型匹配的地址对应的 Endpoint 列表。
func getEndpointsFromAddresses(addresses []corev1.EndpointAddress, addressType discovery.AddressType, ready bool) []discovery.Endpoint {
	endpoints := []discovery.Endpoint{}
	isIPv6AddressType := addressType == discovery.AddressTypeIPv6

	for _, address := range addresses {
		if utilnet.IsIPv6String(address.IP) == isIPv6AddressType {
			endpoints = append(endpoints, endpointFromAddress(address, ready))
		}
	}

	return endpoints
}

// endpointFromAddress 根据 EndpointAddress 生成 Endpoint。
func endpointFromAddress(address corev1.EndpointAddress, ready bool) discovery.Endpoint {
	ep := discovery.Endpoint{
		Addresses:  []string{address.IP},
		Conditions: discovery.EndpointConditions{Ready: &ready},
		TargetRef:  address.TargetRef,
	}

	if address.NodeName != nil {
		ep.NodeName = address.NodeName
	}

	return ep
}

// allAddressesIPv6 判断所有地址是否均为 IPv6。
func allAddressesIPv6(addresses []corev1.EndpointAddress) bool {
	if len(addresses) == 0 {
		return false
	}

	for _, address := range addresses {
		if !utilnet.IsIPv6String(address.IP) {
			return false
		}
	}

	return true
}

// setSkipMirrorTrue 设置 skip-mirror 标签为 true，返回是否发生了变更。
func setSkipMirrorTrue(e *corev1.Endpoints) bool {
	skipMirrorVal, ok := e.Labels[discovery.LabelSkipMirror]
	if !ok || skipMirrorVal != "true" {
		if e.Labels == nil {
			e.Labels = map[string]string{}
		}
		e.Labels[discovery.LabelSkipMirror] = "true"
		return true
	}
	return false
}
