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

package endpointslice

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// TestNewEndpointSlice 测试 newEndpointSlice 根据 Service 与 endpointMeta 构造 EndpointSlice 的正确性。
// 输入覆盖无标签、带标签、无头、多标签、试图劫持保留标签、带注解的 Service；
// 断言生成 slice 的标签、GenerateName、OwnerReferences、Namespace、Ports、AddressType 与空 Endpoints。
func TestNewEndpointSlice(t *testing.T) {
	ipAddressType := discovery.AddressTypeIPv4
	portName := "foo"
	endpointMeta := endpointMeta{
		ports: []discovery.EndpointPort{{
			Name:     &portName,
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		addressType: ipAddressType,
	}
	service := v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test"},
		Spec: v1.ServiceSpec{
			ClusterIP: "1.1.1.1",
			Ports:     []v1.ServicePort{{Port: 80}},
			Selector:  map[string]string{"foo": "bar"},
		},
	}

	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	ownerRef := metav1.NewControllerRef(&service, gvk)

	// 表驱动用例：name 为用例名，updateSvc 用于定制基础 Service，expectedSlice 为期望生成的 EndpointSlice。
	testCases := []struct {
		name          string
		updateSvc     func(svc v1.Service) v1.Service // given basic valid services, each test case can customize them
		expectedSlice *discovery.EndpointSlice
	}{
		{
			name: "Service without labels",
			updateSvc: func(svc v1.Service) v1.Service {
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			name: "Service with labels",
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Labels = labels
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						"foo":                      "bar",
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			name: "Headless Service with labels",
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Labels = labels
				svc.Spec.ClusterIP = v1.ClusterIPNone
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						v1.IsHeadlessService:       "",
						"foo":                      "bar",
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			name: "Service with multiple labels",
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar", "foo2": "bar2"}
				svc.Labels = labels
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						"foo":                      "bar",
						"foo2":                     "bar2",
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		// 安全组：Service 试图覆盖保留标签时应被忽略，仍使用真实 Service 名与控制器名。
		{
			name: "Evil service hijacking endpoint slices labels",
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{
					discovery.LabelServiceName: "bad",
					discovery.LabelManagedBy:   "actor",
					"foo":                      "bar",
				}
				svc.Labels = labels
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						"foo":                      "bar",
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			name: "Service with annotations",
			updateSvc: func(svc v1.Service) v1.Service {
				annotations := map[string]string{"foo": "bar"}
				svc.Annotations = annotations
				return svc
			},
			expectedSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
					},
					GenerateName:    fmt.Sprintf("%s-", service.Name),
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
					Namespace:       service.Namespace,
				},
				Ports:       endpointMeta.ports,
				AddressType: endpointMeta.addressType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 使用测试上下文提供的 logger。
			logger, _ := ktesting.NewTestContext(t)
			// 按用例定制基础 Service。
			svc := tc.updateSvc(service)
			// 调用被测函数生成 EndpointSlice。
			generatedSlice := newEndpointSlice(logger, &svc, &endpointMeta, controllerName)
			// 断言生成结果与期望的 slice 完全一致。
			assert.EqualValues(t, tc.expectedSlice, generatedSlice)
		})
	}

}

// TestPodToEndpoint 测试 podToEndpoint 在各种 Pod 状态下生成 Endpoint 的正确性。
// 输入涵盖就绪/未就绪、终止中、多 IP、带节点标签、带 hostname，以及 publishNotReadyAddresses 场景；
// 断言地址列表以及 Ready/Serving/Terminating 条件、NodeName、Zone、Hostname、TargetRef 均符合预期。
func TestPodToEndpoint(t *testing.T) {
	ns := "test"
	svc, _ := newServiceAndEndpointMeta("foo", ns)
	svcPublishNotReady, _ := newServiceAndEndpointMeta("publishnotready", ns)
	svcPublishNotReady.Spec.PublishNotReadyAddresses = true

	readyPod := newPod(1, ns, true, 1, false)
	readyTerminatingPod := newPod(1, ns, true, 1, true)
	readyPodHostname := newPod(1, ns, true, 1, false)
	readyPodHostname.Spec.Subdomain = svc.Name
	readyPodHostname.Spec.Hostname = "example-hostname"

	unreadyPod := newPod(1, ns, false, 1, false)
	unreadyTerminatingPod := newPod(1, ns, false, 1, true)
	multiIPPod := newPod(1, ns, true, 1, false)
	multiIPPod.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}, {IP: "1234::5678:0000:0000:9abc:def0"}}

	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: readyPod.Spec.NodeName,
			Labels: map[string]string{
				"topology.kubernetes.io/zone":   "us-central1-a",
				"topology.kubernetes.io/region": "us-central1",
			},
		},
	}

	// 表驱动用例：name 为用例名，pod/node/svc 为输入，expectedEndpoint 为期望的 Endpoint，
	// publishNotReadyAddresses 标记是否开启发布未就绪地址。
	testCases := []struct {
		name                     string
		pod                      *v1.Pod
		node                     *v1.Node
		svc                      *v1.Service
		expectedEndpoint         discovery.Endpoint
		publishNotReadyAddresses bool
	}{
		{
			name: "Ready pod",
			pod:  readyPod,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		{
			name: "Ready pod + publishNotReadyAddresses",
			pod:  readyPod,
			svc:  &svcPublishNotReady,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		{
			name: "Unready pod",
			pod:  unreadyPod,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(false),
					Serving:     ptr.To(false),
					Terminating: ptr.To(false),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		{
			name: "Unready pod + publishNotReadyAddresses",
			pod:  unreadyPod,
			svc:  &svcPublishNotReady,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(false),
					Terminating: ptr.To(false),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		// 拓扑组：验证从 Node 标签写入 Zone，以及按地址类型筛选多 IP 地址。
		{
			name: "Ready pod + node labels",
			pod:  readyPod,
			node: node1,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		{
			name: "Multi IP Ready pod + node labels",
			pod:  multiIPPod,
			node: node1,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.4"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		// 主机名组：Pod subdomain 与 Service 匹配时应写入 Hostname。
		{
			name: "Ready pod + hostname",
			pod:  readyPodHostname,
			node: node1,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Hostname: &readyPodHostname.Spec.Hostname,
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPodHostname.Name,
					UID:       readyPodHostname.UID,
				},
			},
		},
		{
			name: "Ready pod",
			pod:  readyPod,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		// 终止组：即便仍在 serving，终止中的 Pod 也不应被标记为 Ready（除非开启发布未就绪地址）。
		{
			name: "Ready terminating pod",
			pod:  readyTerminatingPod,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(false),
					Serving:     ptr.To(true),
					Terminating: ptr.To(true),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
		{
			name: "Not ready terminating pod",
			pod:  unreadyTerminatingPod,
			svc:  &svc,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.5"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(false),
					Serving:     ptr.To(false),
					Terminating: ptr.To(true),
				},
				NodeName: ptr.To("node-1"),
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns,
					Name:      readyPod.Name,
					UID:       readyPod.UID,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 按 IPv4 地址类型调用被测函数生成 Endpoint。
			endpoint := podToEndpoint(testCase.pod, testCase.node, testCase.svc, discovery.AddressTypeIPv4)
			// 深比较生成的 Endpoint 与期望值，不一致则报错。
			if !reflect.DeepEqual(testCase.expectedEndpoint, endpoint) {
				t.Errorf("Expected endpoint: %+v, got: %+v", testCase.expectedEndpoint, endpoint)
			}
		})
	}
}

// TestServiceControllerKey 测试 ServiceControllerKey 从 EndpointSlice 推导 Service controller key 的行为。
// 输入包括 nil slice、缺少 service-name 标签的空 slice、以及带命名空间与标签的合法 slice；
// 断言返回的错误与 key（如 "ns/svc"）符合预期。
func TestServiceControllerKey(t *testing.T) {
	// 表驱动用例：endpointSlice 为输入，expectedKey 为期望的 key，expectedErr 为期望的错误。
	testCases := map[string]struct {
		endpointSlice *discovery.EndpointSlice
		expectedKey   string
		expectedErr   error
	}{
		"nil EndpointSlice": {
			endpointSlice: nil,
			expectedKey:   "",
			expectedErr:   fmt.Errorf("nil EndpointSlice passed to ServiceControllerKey()"),
		},
		"empty EndpointSlice": {
			endpointSlice: &discovery.EndpointSlice{},
			expectedKey:   "",
			expectedErr:   fmt.Errorf("EndpointSlice missing kubernetes.io/service-name label"),
		},
		"valid EndpointSlice": {
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Labels: map[string]string{
						discovery.LabelServiceName: "svc",
					},
				},
			},
			expectedKey: "ns/svc",
			expectedErr: nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 调用被测函数，得到推导出的 key 与可能的错误。
			actualKey, actualErr := ServiceControllerKey(tc.endpointSlice)
			// 断言错误与期望一致（含 nil 的情况）。
			if !reflect.DeepEqual(actualErr, tc.expectedErr) {
				t.Errorf("Expected %s, got %s", tc.expectedErr, actualErr)
			}
			// 断言 key 与期望一致。
			if actualKey != tc.expectedKey {
				t.Errorf("Expected %s, got %s", tc.expectedKey, actualKey)
			}
		})
	}
}

// TestGetEndpointPorts 测试 getEndpointPorts 从 Service 与 Pod 生成 EndpointPort 列表的正确性。
// 输入覆盖 AppProtocol、命名端口、可重启 sidecar init 容器端口，以及普通容器与 sidecar 同名端口的竞争场景；
// 断言端口数量与各 EndpointPort 的 Name/Port/Protocol/AppProtocol 均符合预期。
func TestGetEndpointPorts(t *testing.T) {
	// sidecar init 容器所需的 Always 重启策略。
	restartPolicyAlways := v1.ContainerRestartPolicyAlways

	// 表驱动用例：service/pod 为输入，expectedPorts 为期望的 EndpointPort 列表。
	testCases := map[string]struct {
		service       *v1.Service
		pod           *v1.Pod
		expectedPorts []*discovery.EndpointPort
	}{
		"service with AppProtocol on one port": {
			service: &v1.Service{
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{{
						Name:        "http",
						Port:        80,
						TargetPort:  intstr.FromInt32(80),
						Protocol:    v1.ProtocolTCP,
						AppProtocol: ptr.To("example.com/custom-protocol"),
					}},
				},
			},
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Ports: []v1.ContainerPort{},
					}},
				},
			},
			expectedPorts: []*discovery.EndpointPort{{
				Name:        ptr.To("http"),
				Port:        ptr.To[int32](80),
				Protocol:    ptr.To(v1.ProtocolTCP),
				AppProtocol: ptr.To("example.com/custom-protocol"),
			}},
		},
		"service with named port and AppProtocol on one port": {
			service: &v1.Service{
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{{
						Name:       "http",
						Port:       80,
						TargetPort: intstr.FromInt32(80),
						Protocol:   v1.ProtocolTCP,
					}, {
						Name:        "https",
						Protocol:    v1.ProtocolTCP,
						TargetPort:  intstr.FromString("https"),
						AppProtocol: ptr.To("https"),
					}},
				},
			},
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Ports: []v1.ContainerPort{{
							Name:          "https",
							ContainerPort: int32(443),
							Protocol:      v1.ProtocolTCP,
						}},
					}},
				},
			},
			expectedPorts: []*discovery.EndpointPort{{
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}, {
				Name:        ptr.To("https"),
				Port:        ptr.To[int32](443),
				Protocol:    ptr.To(v1.ProtocolTCP),
				AppProtocol: ptr.To("https"),
			}},
		},
		// sidecar 组：命名端口可在 restartPolicy=Always 的 init 容器中解析，普通容器优先。
		"service with named port for restartable init container": {
			service: &v1.Service{
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{{
						Name:       "http-sidecar",
						Port:       8080,
						TargetPort: intstr.FromInt32(8080),
						Protocol:   v1.ProtocolTCP,
					}, {
						Name:       "http",
						Port:       8090,
						TargetPort: intstr.FromString("http"),
						Protocol:   v1.ProtocolTCP,
					}},
				},
			},
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{{
						Ports: []v1.ContainerPort{{
							Name:          "http-sidecar",
							ContainerPort: int32(8080),
							Protocol:      v1.ProtocolTCP,
						}},
						RestartPolicy: &restartPolicyAlways,
					}},
					Containers: []v1.Container{{
						Ports: []v1.ContainerPort{{
							Name:          "http",
							ContainerPort: int32(8090),
							Protocol:      v1.ProtocolTCP,
						}},
					}},
				},
			},
			expectedPorts: []*discovery.EndpointPort{{
				Name:     ptr.To("http-sidecar"),
				Port:     ptr.To[int32](8080),
				Protocol: ptr.To(v1.ProtocolTCP),
			}, {
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](8090),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		},
		"service with same named port for regular and restartable init container": {
			service: &v1.Service{
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: intstr.FromString("http"),
							Protocol:   v1.ProtocolTCP,
						}},
				},
			},
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{{
						Ports: []v1.ContainerPort{{
							Name:          "http",
							ContainerPort: int32(8080),
							Protocol:      v1.ProtocolTCP,
						}},
						RestartPolicy: &restartPolicyAlways,
					}},
					Containers: []v1.Container{{
						Ports: []v1.ContainerPort{{
							Name:          "http",
							ContainerPort: int32(8090),
							Protocol:      v1.ProtocolTCP,
						}},
					}},
				},
			},
			expectedPorts: []*discovery.EndpointPort{{
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](8090),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 使用测试上下文提供的 logger 调用被测函数。
			logger, _ := ktesting.NewTestContext(t)
			actualPorts := getEndpointPorts(logger, tc.service, tc.pod)

			// 先断言端口数量一致，数量不符直接终止当前用例。
			if len(actualPorts) != len(tc.expectedPorts) {
				t.Fatalf("Expected %d ports, got %d", len(tc.expectedPorts), len(actualPorts))
			}

			// 再逐个深比较端口内容。
			for i, actualPort := range actualPorts {
				if !reflect.DeepEqual(&actualPort, tc.expectedPorts[i]) {
					t.Errorf("Expected port: %+v, got %+v", tc.expectedPorts[i], &actualPort)
				}
			}
		})
	}
}

// TestSetEndpointSliceLabels 测试 setEndpointSliceLabels 计算 slice 标签及是否变化的正确性。
// 输入覆盖空/非空 slice、有/无标签、无头与 ClusterIP Service、保留标签冲突及恶意劫持场景；
// 断言返回的标签集合符合预期，且 updated 标记准确反映非保留标签是否变化。
func TestSetEndpointSliceLabels(t *testing.T) {

	service := v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test"},
		Spec: v1.ServiceSpec{
			Ports:     []v1.ServicePort{{Port: 80}},
			Selector:  map[string]string{"foo": "bar"},
			ClusterIP: "1.1.1.1",
		},
	}

	// 表驱动用例：name 为用例名，epSlice 为现有 slice，updateSvc 用于定制 Service，
	// expectedLabels/expectedUpdate 分别为期望标签与是否需要更新。
	testCases := []struct {
		name           string
		epSlice        *discovery.EndpointSlice
		updateSvc      func(svc v1.Service) v1.Service // given basic valid services, each test case can customize them
		expectedLabels map[string]string
		expectedUpdate bool
	}{
		{
			name:    "Service without labels and empty endpoint slice",
			epSlice: &discovery.EndpointSlice{},
			updateSvc: func(svc v1.Service) v1.Service {
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name:    "Headless service with labels and empty endpoint slice",
			epSlice: &discovery.EndpointSlice{},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Spec.ClusterIP = v1.ClusterIPNone
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				v1.IsHeadlessService:       "",
				"foo":                      "bar",
			},
			expectedUpdate: true,
		},
		{
			name:    "Headless service without labels and empty endpoint slice",
			epSlice: &discovery.EndpointSlice{},
			updateSvc: func(svc v1.Service) v1.Service {
				svc.Spec.ClusterIP = v1.ClusterIPNone
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				v1.IsHeadlessService:       "",
			},
			expectedUpdate: false,
		},
		{
			name:    "Non Headless service with Headless label and empty endpoint slice",
			epSlice: &discovery.EndpointSlice{},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{v1.IsHeadlessService: ""}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name: "Headless Service change to ClusterIP Service with headless label",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						v1.IsHeadlessService:       "",
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{v1.IsHeadlessService: ""}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name: "Headless Service change to ClusterIP Service",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						v1.IsHeadlessService:       "",
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name: "Headless service and endpoint slice with same labels",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						"foo":                      "bar",
					},
				},
			}, updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Spec.ClusterIP = v1.ClusterIPNone
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				v1.IsHeadlessService:       "",
				"foo":                      "bar",
			},
			expectedUpdate: false,
		},
		{
			name:    "Service with labels and empty endpoint slice",
			epSlice: &discovery.EndpointSlice{},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				"foo":                      "bar",
			},
			expectedUpdate: true,
		},
		{
			name: "Slice with labels and service without labels",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						"foo":                      "bar",
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: true,
		},
		{
			name: "Slice with headless label and service with ClusterIP",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
						v1.IsHeadlessService:       "",
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name: "Slice with reserved labels and service with labels",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{"foo": "bar"}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				"foo":                      "bar",
			},
			expectedUpdate: true,
		},
		// 安全组：Service 试图写入保留标签时被忽略，不会误报需要更新。
		{
			name: "Evil service trying to hijack slice labels only well-known slice labels",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{
					discovery.LabelServiceName: "bad",
					discovery.LabelManagedBy:   "actor",
					v1.IsHeadlessService:       "invalid",
				}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
			},
			expectedUpdate: false,
		},
		{
			name: "Evil service trying to hijack slice labels with updates",
			epSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: service.Name,
						discovery.LabelManagedBy:   controllerName,
					},
				},
			},
			updateSvc: func(svc v1.Service) v1.Service {
				labels := map[string]string{
					discovery.LabelServiceName: "bad",
					discovery.LabelManagedBy:   "actor",
					"foo":                      "bar",
				}
				svc.Labels = labels
				return svc
			},
			expectedLabels: map[string]string{
				discovery.LabelServiceName: service.Name,
				discovery.LabelManagedBy:   controllerName,
				"foo":                      "bar",
			},
			expectedUpdate: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 使用测试上下文提供的 logger，并按用例定制 Service。
			logger, _ := ktesting.NewTestContext(t)
			svc := tc.updateSvc(service)
			// 调用被测函数，得到新标签与是否需要更新。
			labels, updated := setEndpointSliceLabels(logger, tc.epSlice, &svc, controllerName)
			// 断言是否需要更新的标记与期望一致。
			assert.EqualValues(t, tc.expectedUpdate, updated)
			// 断言计算出的标签集合与期望一致。
			assert.EqualValues(t, tc.expectedLabels, labels)
		})
	}

}

// Test helpers
// 测试辅助函数

// newPod 创建测试用 Pod：按 n 命名并生成 IP（1.2.3.<4+n>），
// 依据 ready 设置 PodReady 条件，依据 terminating 决定是否设置删除时间戳。
func newPod(n int, namespace string, ready bool, nPorts int, terminating bool) *v1.Pod {
	// 根据 ready 参数决定 PodReady 条件的状态。
	status := v1.ConditionTrue
	if !ready {
		status = v1.ConditionFalse
	}

	// 终止中（terminating）的 Pod 设置删除时间戳，否则为 nil。
	var deletionTimestamp *metav1.Time
	if terminating {
		deletionTimestamp = &metav1.Time{
			Time: time.Now(),
		}
	}

	// 构造 Pod：包含基础容器、节点名、Pod IP 与就绪条件。
	p := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              fmt.Sprintf("pod%d", n),
			Labels:            map[string]string{"foo": "bar"},
			DeletionTimestamp: deletionTimestamp,
			ResourceVersion:   fmt.Sprint(n),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "container-1",
			}},
			NodeName: "node-1",
		},
		Status: v1.PodStatus{
			PodIP: fmt.Sprintf("1.2.3.%d", 4+n),
			PodIPs: []v1.PodIP{{
				IP: fmt.Sprintf("1.2.3.%d", 4+n),
			}},
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: status,
				},
			},
		},
	}

	return p
}

// newClientset 创建带预置 reactor 的 fake clientset：
// create 时为 GenerateName 补随机后缀并置 Generation=1，update 时递增 Generation。
func newClientset() *fake.Clientset {
	client := fake.NewSimpleClientset()

	// create reactor：模拟 apiserver 根据 GenerateName 生成真实名称。
	client.PrependReactor("create", "endpointslices", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		endpointSlice := action.(k8stesting.CreateAction).GetObject().(*discovery.EndpointSlice)

		if endpointSlice.ObjectMeta.GenerateName != "" {
			endpointSlice.ObjectMeta.Name = fmt.Sprintf("%s-%s", endpointSlice.ObjectMeta.GenerateName, rand.String(8))
			endpointSlice.ObjectMeta.GenerateName = ""
		}
		endpointSlice.Generation = 1

		return false, endpointSlice, nil
	}))
	// update reactor：模拟 apiserver 更新对象时递增 Generation。
	client.PrependReactor("update", "endpointslices", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		endpointSlice := action.(k8stesting.CreateAction).GetObject().(*discovery.EndpointSlice)
		endpointSlice.Generation++
		return false, endpointSlice, nil
	}))

	return client
}

// newServiceAndEndpointMeta 创建测试用 Service 与对应的 endpointMeta：
// Service 使用数字 targetPort 80 与 IPv4 地址族，两者端口协议均为 TCP。
func newServiceAndEndpointMeta(name, namespace string) (v1.Service, endpointMeta) {
	// 端口 80 的 IntOrString 形式，供 targetPort 与 endpointMeta 共用。
	portNameIntStr := intstr.IntOrString{
		Type:   intstr.Int,
		IntVal: 80,
	}

	// 构造带 UID、端口、selector 与 IPFamilies 的基础 Service。
	svc := v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(namespace + "-" + name),
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{{
				TargetPort: portNameIntStr,
				Protocol:   v1.ProtocolTCP,
				Name:       name,
			}},
			Selector:   map[string]string{"foo": "bar"},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	}

	// 构造与 Service 匹配的 endpointMeta：IPv4 地址类型与对应的端口定义。
	endpointMeta := endpointMeta{
		addressType: discovery.AddressTypeIPv4,
		ports: []discovery.EndpointPort{{
			Name:     &name,
			Port:     &portNameIntStr.IntVal,
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
	}

	return svc, endpointMeta
}

// newEmptyEndpointSlice 创建一个空的 EndpointSlice：名称由 Service 名与序号 n 组成，
// 带指向 Service 的 OwnerReference，端口与地址类型取自 endpointMeta。
func newEmptyEndpointSlice(n int, namespace string, endpointMeta endpointMeta, svc v1.Service) *discovery.EndpointSlice {
	// 构造指向 Service 的控制器 OwnerReference。
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	ownerRef := metav1.NewControllerRef(&svc, gvk)

	// 返回端点列表为空的 slice。
	return &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-%d", svc.Name, n),
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{*ownerRef},
		},
		Ports:       endpointMeta.ports,
		AddressType: endpointMeta.addressType,
		Endpoints:   []discovery.Endpoint{},
	}
}

// TestSupportedServiceAddressType 测试 getAddressTypesForService 推断 Service 地址类型的行为。
// 输入覆盖 IPv4/IPv6/双栈 Service、IPFamilies 为空的升级场景、以及有无 selector 的 headless Service；
// 断言返回的地址类型集合数量与内容均符合预期。
func TestSupportedServiceAddressType(t *testing.T) {
	// 表驱动用例：name 为用例名，service 为输入，expectedAddressTypes 为期望的地址类型集合。
	testCases := []struct {
		name                 string
		service              v1.Service
		expectedAddressTypes []discovery.AddressType
	}{
		{
			name:                 "v4 service with no ip families (cluster upgrade)",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv4},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "10.0.0.10",
					IPFamilies: nil,
				},
			},
		},
		{
			name:                 "v6 service with no ip families (cluster upgrade)",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv6},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "2000::1",
					IPFamilies: nil,
				},
			},
		},
		// 正常推断组：IPFamilies 已填写时直接按协议族推断（含 v4/v6 及双栈顺序无关）。
		{
			name:                 "v4 service",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv4},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
		},
		{
			name:                 "v6 services",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv6},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
		},
		{
			name:                 "v4,v6 service",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv4, discovery.AddressTypeIPv6},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
				},
			},
		},
		{
			name:                 "v6,v4 service",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv6, discovery.AddressTypeIPv4},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
				},
			},
		},
		// headless 回退组：无 IPFamilies 时默认按双栈处理，避免误删 slice 中断流量。
		{
			name:                 "headless with no selector and no families (old api-server)",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv6, discovery.AddressTypeIPv4},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: nil,
				},
			},
		},
		{
			name:                 "headless with selector and no families (old api-server)",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv6, discovery.AddressTypeIPv4},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: nil,
				},
			},
		},

		{
			name:                 "headless with no selector with families",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv4, discovery.AddressTypeIPv6},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
				},
			},
		},
		{
			name:                 "headless with selector with families",
			expectedAddressTypes: []discovery.AddressType{discovery.AddressTypeIPv4, discovery.AddressTypeIPv6},
			service: v1.Service{
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 使用测试上下文提供的 logger 调用被测函数。
			logger, _ := ktesting.NewTestContext(t)
			addressTypes := getAddressTypesForService(logger, &testCase.service)
			// 先断言地址类型数量一致。
			if len(addressTypes) != len(testCase.expectedAddressTypes) {
				t.Fatalf("expected count address types %v got %v", len(testCase.expectedAddressTypes), len(addressTypes))
			}

			// compare
			// 逐个确认期望的地址类型确实出现在结果集合中。
			for _, expectedAddressType := range testCase.expectedAddressTypes {
				// 在结果集合中查找当前期望的地址类型。
				found := false
				for key := range addressTypes {
					if key == expectedAddressType {
						found = true
						break

					}
				}
				if !found {
					t.Fatalf("expected address type %v was not found in the result", expectedAddressType)
				}
			}
		})
	}
}

// Test_hintsEnabled 测试 hintsEnabled 根据注解判断是否启用拓扑感知提示的行为。
// 输入覆盖空注解、无关注解、旧/新注解取值为 enabled、aUto、auto、Auto、disabled 等情形，
// 以及新旧注解同时存在时旧注解优先的场景；断言返回值与期望一致。
func Test_hintsEnabled(t *testing.T) {
	// 表驱动用例：name 为用例名，annotations 为输入注解，expectEnabled 为期望的启用状态。
	testCases := []struct {
		name          string
		annotations   map[string]string
		expectEnabled bool
	}{{
		name:          "empty annotations",
		expectEnabled: false,
	}, {
		name:          "different annotations",
		annotations:   map[string]string{"topology-hints": "enabled"},
		expectEnabled: false,
	}, {
		// 旧注解组：只有 "auto"/"Auto" 视为启用，其余取值均不启用。
		name:          "hints annotation == enabled",
		annotations:   map[string]string{v1.DeprecatedAnnotationTopologyAwareHints: "enabled"},
		expectEnabled: false,
	}, {
		name:          "hints annotation == aUto",
		annotations:   map[string]string{v1.DeprecatedAnnotationTopologyAwareHints: "aUto"},
		expectEnabled: false,
	}, {
		name:          "hints annotation == auto",
		annotations:   map[string]string{v1.DeprecatedAnnotationTopologyAwareHints: "auto"},
		expectEnabled: true,
	}, {
		name:          "hints annotation == Auto",
		annotations:   map[string]string{v1.DeprecatedAnnotationTopologyAwareHints: "Auto"},
		expectEnabled: true,
	}, {
		name:          "hints annotation == disabled",
		annotations:   map[string]string{v1.DeprecatedAnnotationTopologyAwareHints: "disabled"},
		expectEnabled: false,
	}, {
		// 新注解组：AnnotationTopologyMode 的行为与旧注解一致。
		name:          "mode annotation == enabled",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "enabled"},
		expectEnabled: false,
	}, {
		name:          "mode annotation == aUto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "aUto"},
		expectEnabled: false,
	}, {
		name:          "mode annotation == auto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "auto"},
		expectEnabled: true,
	}, {
		name:          "mode annotation == Auto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "Auto"},
		expectEnabled: true,
	}, {
		name:          "mode annotation == disabled",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "disabled"},
		expectEnabled: false,
	}, {
		name:          "mode annotation == enabled",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "enabled"},
		expectEnabled: false,
	}, {
		name:          "mode annotation == aUto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "aUto"},
		expectEnabled: false,
	}, {
		name:          "mode annotation == auto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "auto"},
		expectEnabled: true,
	}, {
		name:          "mode annotation == Auto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "Auto"},
		expectEnabled: true,
	}, {
		name:          "mode annotation == disabled",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "disabled"},
		expectEnabled: false,
	}, {
		// 优先级组：新旧注解同时存在时，以旧注解（hints）为准。
		name:          "mode annotation == disabled, hints annotation == auto",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "disabled", v1.DeprecatedAnnotationTopologyAwareHints: "auto"},
		expectEnabled: true,
	}, {
		name:          "mode annotation == auto, hints annotation == disabled",
		annotations:   map[string]string{v1.AnnotationTopologyMode: "auto", v1.DeprecatedAnnotationTopologyAwareHints: "disabled"},
		expectEnabled: false,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 调用被测函数判断是否启用拓扑提示。
			actualEnabled := hintsEnabled(tc.annotations)
			// 断言实际启用状态与期望一致。
			if actualEnabled != tc.expectEnabled {
				t.Errorf("Expected %t, got %t", tc.expectEnabled, actualEnabled)
			}
		})
	}
}

// TestFindPort 测试 FindPort 根据 ServicePort 的 targetPort 在 Pod 中定位容器端口的行为。
// 输入覆盖数字 targetPort、字符串命名端口、协议不匹配、sidecar init 容器端口，
// 以及应用容器与 sidecar 同名端口竞争等场景；断言返回端口值及解析是否成功。
func TestFindPort(t *testing.T) {
	// sidecar init 容器所需的 Always 重启策略。
	restartAlways := v1.ContainerRestartPolicyAlways
	// 表驱动用例：name 为用例名，containers/initContainers 为 Pod 中的容器，
	// port 为 targetPort，expected 为期望端口，pass 表示是否应成功解析。
	testCases := []struct {
		name           string
		containers     []v1.Container
		initContainers []v1.Container
		port           intstr.IntOrString
		expected       int
		pass           bool
	}{{
		// 数字端口组：targetPort 为数字时直接返回，无需查看容器。
		name:       "valid int, no ports",
		containers: []v1.Container{{}},
		port:       intstr.FromInt32(93),
		expected:   93,
		pass:       true,
	}, {
		name: "valid int, with ports",
		containers: []v1.Container{{Ports: []v1.ContainerPort{{
			Name:          "",
			ContainerPort: 11,
			Protocol:      "TCP",
		}, {
			Name:          "p",
			ContainerPort: 22,
			Protocol:      "TCP",
		}}}},
		port:     intstr.FromInt32(93),
		expected: 93,
		pass:     true,
	}, {
		// 命名端口组：按名称与协议在普通容器中匹配。
		name:       "valid str, no ports",
		containers: []v1.Container{{}},
		port:       intstr.FromString("p"),
		expected:   0,
		pass:       false,
	}, {
		name: "valid str, one ctr with ports",
		containers: []v1.Container{{Ports: []v1.ContainerPort{{
			Name:          "",
			ContainerPort: 11,
			Protocol:      "UDP",
		}, {
			Name:          "p",
			ContainerPort: 22,
			Protocol:      "TCP",
		}, {
			Name:          "q",
			ContainerPort: 33,
			Protocol:      "TCP",
		}}}},
		port:     intstr.FromString("q"),
		expected: 33,
		pass:     true,
	}, {
		name: "valid str, two ctr with ports",
		containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
			Name:          "",
			ContainerPort: 11,
			Protocol:      "UDP",
		}, {
			Name:          "p",
			ContainerPort: 22,
			Protocol:      "TCP",
		}, {
			Name:          "q",
			ContainerPort: 33,
			Protocol:      "TCP",
		}}}},
		port:     intstr.FromString("q"),
		expected: 33,
		pass:     true,
	}, {
		name: "valid str, two ctr with same port",
		containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
			Name:          "",
			ContainerPort: 11,
			Protocol:      "UDP",
		}, {
			Name:          "p",
			ContainerPort: 22,
			Protocol:      "TCP",
		}, {
			Name:          "q",
			ContainerPort: 22,
			Protocol:      "TCP",
		}}}},
		port:     intstr.FromString("q"),
		expected: 22,
		pass:     true,
	}, {
		name: "valid str, invalid protocol",
		containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
			Name:          "a",
			ContainerPort: 11,
			Protocol:      "snmp",
		},
		}}},
		port:     intstr.FromString("a"),
		expected: 0,
		pass:     false,
	}, {
		name: "valid hostPort",
		containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
			Name:          "a",
			ContainerPort: 11,
			HostPort:      81,
			Protocol:      "TCP",
		},
		}}},
		port:     intstr.FromString("a"),
		expected: 11,
		pass:     true,
	},
		{
			name: "invalid hostPort",
			containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
				Name:          "a",
				ContainerPort: 11,
				HostPort:      -1,
				Protocol:      "TCP",
			},
			}}},
			port:     intstr.FromString("a"),
			expected: 11,
			pass:     true,
			// this should fail but passes.
		},
		{
			name: "invalid ContainerPort",
			containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
				Name:          "a",
				ContainerPort: -1,
				Protocol:      "TCP",
			},
			}}},
			port:     intstr.FromString("a"),
			expected: -1,
			pass:     true,
			// this should fail but passes
		},
		{
			name: "HostIP Address",
			containers: []v1.Container{{}, {Ports: []v1.ContainerPort{{
				Name:          "a",
				ContainerPort: 11,
				HostIP:        "192.168.1.1",
				Protocol:      "TCP",
			},
			}}},
			port:     intstr.FromString("a"),
			expected: 11,
			pass:     true,
		},
		{
			// sidecar 组：restartPolicy=Always 的 init 容器命名端口可被解析，普通容器优先。
			name: "Sidecar initContainer named port",
			initContainers: []v1.Container{{
				RestartPolicy: &restartAlways,
				Ports: []v1.ContainerPort{{
					Name:          "a",
					ContainerPort: 80,
					HostPort:      -1,
					Protocol:      "TCP",
				}},
			}},
			port:     intstr.FromString("a"),
			expected: 80,
			pass:     true,
		},
		{
			name: "Invalid(restartPolicy != Always) initContainer named port",
			initContainers: []v1.Container{{
				Ports: []v1.ContainerPort{{
					Name:          "a",
					ContainerPort: 80,
					HostPort:      -1,
					Protocol:      "TCP",
				}},
			}},
			port:     intstr.FromString("a"),
			expected: 0,
			pass:     false,
		},
		{
			name: "App and sidecar containers have the same named port, first app container port will be used",
			initContainers: []v1.Container{{
				RestartPolicy: &restartAlways,
				Ports: []v1.ContainerPort{{
					Name:          "a",
					ContainerPort: 80,
					HostPort:      -1,
					Protocol:      "TCP",
				}},
			}},
			containers: []v1.Container{{
				Ports: []v1.ContainerPort{{
					Name:          "a",
					ContainerPort: 81,
					HostPort:      -1,
					Protocol:      "TCP",
				}},
			}, {
				Ports: []v1.ContainerPort{{
					Name:          "a",
					ContainerPort: 82,
					HostPort:      -1,
					Protocol:      "TCP",
				}},
			}},
			port:     intstr.FromString("a"),
			expected: 81,
			pass:     true,
		},
	}

	for _, tc := range testCases {
		// 用用例中的容器构造 Pod 与 ServicePort，调用被测函数解析端口。
		port, err := FindPort(&v1.Pod{Spec: v1.PodSpec{Containers: tc.containers, InitContainers: tc.initContainers}},
			&v1.ServicePort{Protocol: "TCP", TargetPort: tc.port})
		// 本应成功却报错则失败。
		if err != nil && tc.pass {
			t.Errorf("unexpected error for %s: %v", tc.name, err)
		}
		// 本应失败却成功解析则失败。
		if err == nil && !tc.pass {
			t.Errorf("unexpected non-error for %s: %d", tc.name, port)
		}
		// 断言解析出的端口值与期望一致。
		if port != tc.expected {
			t.Errorf("wrong result for %s: expected %d, got %d", tc.name, tc.expected, port)
		}
	}
}
