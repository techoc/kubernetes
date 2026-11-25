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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
)

// noEndpoints 是测试中使用的空 Endpoints 对象。
var noEndpoints = &corev1.Endpoints{}

// TestEndpointsAdapterGet 验证 Get 能读取 Endpoints。
func TestEndpointsAdapterGet(t *testing.T) {
	endpoints1, epSlice1 := generateEndpointsAndSlice("foo", "testing", []int{80, 443}, []string{"10.1.2.3", "10.1.2.4"})

	testCases := map[string]struct {
		expectedError     error
		expectedEndpoints *corev1.Endpoints
		initialState      []runtime.Object
		namespaceParam    string
		nameParam         string
	}{
		"single-existing-endpoints": {
			expectedError:     nil,
			expectedEndpoints: endpoints1,
			initialState:      []runtime.Object{endpoints1, epSlice1},
			namespaceParam:    "testing",
			nameParam:         "foo",
		},
		"endpoints exists, endpointslice does not": {
			expectedError:     nil,
			expectedEndpoints: endpoints1,
			initialState:      []runtime.Object{endpoints1},
			namespaceParam:    "testing",
			nameParam:         "foo",
		},
		"endpointslice exists, endpoints does not": {
			expectedError:     errors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, "foo"),
			expectedEndpoints: noEndpoints,
			initialState:      []runtime.Object{epSlice1},
			namespaceParam:    "testing",
			nameParam:         "foo",
		},
		"wrong-namespace": {
			expectedError:     errors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, "foo"),
			expectedEndpoints: noEndpoints,
			initialState:      []runtime.Object{endpoints1, epSlice1},
			namespaceParam:    "foo",
			nameParam:         "foo",
		},
		"wrong-name": {
			expectedError:     errors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, "bar"),
			expectedEndpoints: noEndpoints,
			initialState:      []runtime.Object{endpoints1, epSlice1},
			namespaceParam:    "testing",
			nameParam:         "bar",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(testCase.initialState...)
			epAdapter := NewEndpointsAdapter(client.CoreV1(), client.DiscoveryV1())

			endpoints, err := epAdapter.Get(testCase.namespaceParam, testCase.nameParam, metav1.GetOptions{})

			if !apiequality.Semantic.DeepEqual(testCase.expectedError, err) {
				t.Errorf("Expected error: %v, got: %v", testCase.expectedError, err)
			}

			if !apiequality.Semantic.DeepEqual(endpoints, testCase.expectedEndpoints) {
				t.Errorf("Expected endpoints: %v, got: %v", testCase.expectedEndpoints, endpoints)
			}
		})
	}
}

// TestEndpointsAdapterCreate 验证 Create 同时创建 Endpoints 与 EndpointSlice。
func TestEndpointsAdapterCreate(t *testing.T) {
	endpoints1, epSlice1 := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.3", "10.1.2.4"})

	// 即使 Endpoints 包含 IPv6 地址，也不应将其
	// 纳入对应的 IPv4 EndpointSlice。
	endpoints2, _ := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.5", "10.1.2.6", "1234::5678:0000:0000:9abc:def0"})
	_, epSlice2 := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.5", "10.1.2.6"})

	// 确保仅含 IPv6 地址的 Endpoints 生成
	// 地址类型为 IPv6 的 EndpointSlice。
	endpoints3, epSlice3 := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"1234::5678:0000:0000:9abc:def0"})
	epSlice3.AddressType = discovery.AddressTypeIPv6

	testCases := map[string]struct {
		expectedError  error
		expectedResult *corev1.Endpoints
		expectCreate   []runtime.Object
		expectUpdate   []runtime.Object
		initialState   []runtime.Object
		namespaceParam string
		endpointsParam *corev1.Endpoints
	}{
		"single-endpoint": {
			expectedError:  nil,
			expectedResult: endpoints1,
			expectCreate:   []runtime.Object{endpoints1, epSlice1},
			initialState:   []runtime.Object{},
			namespaceParam: endpoints1.Namespace,
			endpointsParam: endpoints1,
		},
		"single-endpoint-partial-ipv6": {
			expectedError:  nil,
			expectedResult: endpoints2,
			expectCreate:   []runtime.Object{endpoints2, epSlice2},
			initialState:   []runtime.Object{},
			namespaceParam: endpoints2.Namespace,
			endpointsParam: endpoints2,
		},
		"single-endpoint-full-ipv6": {
			expectedError:  nil,
			expectedResult: endpoints3,
			expectCreate:   []runtime.Object{endpoints3, epSlice3},
			initialState:   []runtime.Object{},
			namespaceParam: endpoints3.Namespace,
			endpointsParam: endpoints3,
		},
		"existing-endpoints": {
			expectedError:  errors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "endpoints"}, "foo"),
			expectedResult: noEndpoints,
			initialState:   []runtime.Object{endpoints1, epSlice1},
			namespaceParam: endpoints1.Namespace,
			endpointsParam: endpoints1,

			// 期望尝试创建但同时失败。
			expectCreate: []runtime.Object{endpoints1},
		},
		"existing-endpointslice-incorrect": {
			// 需要创建 Endpoints 且 EndpointSlice 已存在时不应报错。
			expectedError:  nil,
			expectedResult: endpoints1,
			expectCreate:   []runtime.Object{endpoints1},
			initialState:   []runtime.Object{epSlice1},
			namespaceParam: endpoints1.Namespace,
			endpointsParam: endpoints1,
		},
		"existing-endpointslice-correct": {
			// 需要创建 Endpoints 且 EndpointSlice 已存在时也不应报错。
			expectedError:  nil,
			expectedResult: endpoints2,
			expectCreate:   []runtime.Object{endpoints2},
			expectUpdate:   []runtime.Object{epSlice2},
			initialState:   []runtime.Object{epSlice1},
			namespaceParam: endpoints2.Namespace,
			endpointsParam: endpoints2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(testCase.initialState...)
			epAdapter := NewEndpointsAdapter(client.CoreV1(), client.DiscoveryV1())

			endpoints, err := epAdapter.Create(testCase.namespaceParam, testCase.endpointsParam)

			if !apiequality.Semantic.DeepEqual(testCase.expectedError, err) {
				t.Errorf("Expected error: %v, got: %v", testCase.expectedError, err)
			}

			if !apiequality.Semantic.DeepEqual(endpoints, testCase.expectedResult) {
				t.Errorf("Expected endpoints: %v, got: %v", testCase.expectedResult, endpoints)
			}

			err = verifyCreatesAndUpdates(client, testCase.expectCreate, testCase.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}
		})
	}
}

// TestEndpointsAdapterUpdate 验证 Update 同步更新 Endpoints 与 EndpointSlice。
func TestEndpointsAdapterUpdate(t *testing.T) {
	endpoints1, epSlice1 := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.3", "10.1.2.4"})
	endpoints2, epSlice2 := generateEndpointsAndSlice("foo", "testing", []int{80, 443}, []string{"10.1.2.3", "10.1.2.4", "10.1.2.5"})
	endpoints3, _ := generateEndpointsAndSlice("bar", "testing", []int{80, 443}, []string{"10.1.2.3", "10.1.2.4", "10.1.2.5"})

	// 确保已弃用 IP 地址类型的 EndpointSlice 被替换为
	// IPv4 地址类型的版本。
	endpoints4, _ := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.7", "10.1.2.8"})
	_, epSlice4IP := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.7", "10.1.2.8"})
	// "IP" 是已弃用的地址类型，需确保被正确处理。
	epSlice4IP.AddressType = discovery.AddressType("IP")
	_, epSlice4IPv4 := generateEndpointsAndSlice("foo", "testing", []int{80}, []string{"10.1.2.7", "10.1.2.8"})

	testCases := map[string]struct {
		expectedError  error
		expectedResult *corev1.Endpoints
		expectCreate   []runtime.Object
		expectUpdate   []runtime.Object
		initialState   []runtime.Object
		namespaceParam string
		endpointsParam *corev1.Endpoints
	}{
		"single-existing-endpoints-no-change": {
			expectedError:  nil,
			expectedResult: endpoints1,
			initialState:   []runtime.Object{endpoints1, epSlice1},
			namespaceParam: "testing",
			endpointsParam: endpoints1,

			// 即使没有变化，仍期望调用 Update()，
			// 因为该单测总是调用 Update()。
			expectUpdate: []runtime.Object{endpoints1},
		},
		"existing-endpointslice-replaced-with-updated-ipv4-address-type": {
			expectedError:  nil,
			expectedResult: endpoints4,
			initialState:   []runtime.Object{endpoints4, epSlice4IP},
			namespaceParam: "testing",
			endpointsParam: endpoints4,

			// AddressType 变化时会先删后建 EndpointSlice，
			// 因此体现在 expectCreate 而非 expectUpdate 中。
			expectUpdate: []runtime.Object{endpoints4},
			expectCreate: []runtime.Object{epSlice4IPv4},
		},
		"add-ports-and-ips": {
			expectedError:  nil,
			expectedResult: endpoints2,
			expectUpdate:   []runtime.Object{endpoints2, epSlice2},
			initialState:   []runtime.Object{endpoints1, epSlice1},
			namespaceParam: "testing",
			endpointsParam: endpoints2,
		},
		"endpoints-correct-endpointslice-wrong": {
			expectedError:  nil,
			expectedResult: endpoints2,
			expectUpdate:   []runtime.Object{endpoints2, epSlice2},
			initialState:   []runtime.Object{endpoints2, epSlice1},
			namespaceParam: "testing",
			endpointsParam: endpoints2,
		},
		"endpointslice-correct-endpoints-wrong": {
			expectedError:  nil,
			expectedResult: endpoints2,
			expectUpdate:   []runtime.Object{endpoints2},
			initialState:   []runtime.Object{endpoints1, epSlice2},
			namespaceParam: "testing",
			endpointsParam: endpoints2,
		},
		"wrong-endpoints": {
			expectedError:  errors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, "bar"),
			expectedResult: noEndpoints,
			expectUpdate:   []runtime.Object{endpoints3},
			initialState:   []runtime.Object{endpoints1, epSlice1},
			namespaceParam: "testing",
			endpointsParam: endpoints3,
		},
		"missing-endpoints": {
			expectedError:  errors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, "bar"),
			expectedResult: noEndpoints,
			initialState:   []runtime.Object{endpoints1, epSlice1},
			namespaceParam: "testing",
			endpointsParam: endpoints3,

			// 期望尝试更新但同时失败。
			expectUpdate: []runtime.Object{endpoints3},
		},
		"missing-endpointslice": {
			// 需要更新 Endpoints 且 EndpointSlice 不存在时不应报错。
			expectedError:  nil,
			expectedResult: endpoints1,
			expectUpdate:   []runtime.Object{endpoints1},
			expectCreate:   []runtime.Object{epSlice1},
			initialState:   []runtime.Object{endpoints2},
			namespaceParam: "testing",
			endpointsParam: endpoints1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(testCase.initialState...)
			epAdapter := NewEndpointsAdapter(client.CoreV1(), client.DiscoveryV1())

			endpoints, err := epAdapter.Update(testCase.namespaceParam, testCase.endpointsParam)

			if !apiequality.Semantic.DeepEqual(testCase.expectedError, err) {
				t.Errorf("Expected error: %v, got: %v", testCase.expectedError, err)
			}

			if !apiequality.Semantic.DeepEqual(endpoints, testCase.expectedResult) {
				t.Errorf("Expected endpoints: %v, got: %v", testCase.expectedResult, endpoints)
			}

			err = verifyCreatesAndUpdates(client, testCase.expectCreate, testCase.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}
		})
	}
}

// generateEndpointsAndSlice 构造配套的 Endpoints 与 EndpointSlice 测试数据。
func generateEndpointsAndSlice(name, namespace string, ports []int, addresses []string) (*corev1.Endpoints, *discovery.EndpointSlice) {
	trueBool := true

	epSlice := &discovery.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: namespace},
		AddressType: discovery.AddressTypeIPv4,
	}
	epSlice.Labels = map[string]string{discovery.LabelServiceName: name}
	subset := corev1.EndpointSubset{}

	for i, port := range ports {
		endpointPort := corev1.EndpointPort{
			Name:     fmt.Sprintf("port-%d", i),
			Port:     int32(port),
			Protocol: corev1.ProtocolTCP,
		}
		subset.Ports = append(subset.Ports, endpointPort)
		epSlice.Ports = append(epSlice.Ports, discovery.EndpointPort{
			Name:     &endpointPort.Name,
			Port:     &endpointPort.Port,
			Protocol: &endpointPort.Protocol,
		})
	}

	for i, address := range addresses {
		endpointAddress := corev1.EndpointAddress{
			IP: address,
			TargetRef: &corev1.ObjectReference{
				Kind: "Pod",
				Name: fmt.Sprintf("pod-%d", i),
			},
		}

		subset.Addresses = append(subset.Addresses, endpointAddress)

		epSlice.Endpoints = append(epSlice.Endpoints, discovery.Endpoint{
			Addresses:  []string{endpointAddress.IP},
			TargetRef:  endpointAddress.TargetRef,
			Conditions: discovery.EndpointConditions{Ready: &trueBool},
		})
	}

	return &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				discovery.LabelSkipMirror: "true",
			},
		},
		Subsets: []corev1.EndpointSubset{subset},
	}, epSlice
}

// TestEndpointManagerEnsureEndpointSliceFromEndpoints 验证 EndpointSlice 按需创建或更新。
func TestEndpointManagerEnsureEndpointSliceFromEndpoints(t *testing.T) {
	endpoints1, epSlice1 := generateEndpointsAndSlice("foo", "testing", []int{80, 443}, []string{"10.1.2.3", "10.1.2.4"})
	endpoints2, epSlice2 := generateEndpointsAndSlice("foo", "testing", []int{80, 443}, []string{"10.1.2.3", "10.1.2.4", "10.1.2.5"})

	testCases := map[string]struct {
		expectedError         error
		expectedEndpointSlice *discovery.EndpointSlice
		initialState          []runtime.Object
		namespaceParam        string
		endpointsParam        *corev1.Endpoints
	}{
		"existing-endpointslice-no-change": {
			expectedError:         nil,
			expectedEndpointSlice: epSlice1,
			initialState:          []runtime.Object{epSlice1},
			namespaceParam:        "testing",
			endpointsParam:        endpoints1,
		},
		"existing-endpointslice-change": {
			expectedError:         nil,
			expectedEndpointSlice: epSlice2,
			initialState:          []runtime.Object{epSlice1},
			namespaceParam:        "testing",
			endpointsParam:        endpoints2,
		},
		"missing-endpointslice": {
			expectedError:         nil,
			expectedEndpointSlice: epSlice1,
			initialState:          []runtime.Object{},
			namespaceParam:        "testing",
			endpointsParam:        endpoints1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(testCase.initialState...)
			epAdapter := NewEndpointsAdapter(client.CoreV1(), client.DiscoveryV1())

			err := epAdapter.EnsureEndpointSliceFromEndpoints(testCase.namespaceParam, testCase.endpointsParam)
			if !apiequality.Semantic.DeepEqual(testCase.expectedError, err) {
				t.Errorf("Expected error: %v, got: %v", testCase.expectedError, err)
			}

			endpointSlice, err := client.DiscoveryV1().EndpointSlices(testCase.namespaceParam).Get(context.TODO(), testCase.endpointsParam.Name, metav1.GetOptions{})
			if err != nil && !errors.IsNotFound(err) {
				t.Fatalf("Error getting Endpoint Slice: %v", err)
			}

			if !apiequality.Semantic.DeepEqual(endpointSlice, testCase.expectedEndpointSlice) {
				t.Errorf("Expected Endpoint Slice: %v, got: %v", testCase.expectedEndpointSlice, endpointSlice)
			}
		})
	}
}
