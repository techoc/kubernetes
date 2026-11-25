/*
Copyright 2020 The Kubernetes Authors.

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

package endpointslicemirroring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

// TestNewEndpointSlice 验证 newEndpointSlice 能从 Endpoints 构造出带正确标签、注解、
// 属主引用和端口信息的 EndpointSlice，且不会修改作为输入源的 Endpoints 对象。
func TestNewEndpointSlice(t *testing.T) {
	portName := "foo"
	protocol := v1.ProtocolTCP

	ports := []discovery.EndpointPort{{Name: &portName, Protocol: &protocol}}
	addrType := discovery.AddressTypeIPv4
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Endpoints"}

	endpoints := v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "test",
		},
		Subsets: []v1.EndpointSubset{{
			Ports: []v1.EndpointPort{{Port: 80}},
		}},
	}
	ownerRef := metav1.NewControllerRef(&endpoints, gvk)

	testCases := []struct {
		name          string
		tweakEndpoint func(ep *v1.Endpoints)
		expectedSlice discovery.EndpointSlice
	}{
		{
			name: "create slice from endpoints",
			tweakEndpoint: func(ep *v1.Endpoints) {
			},
			expectedSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: endpoints.Name,
						discovery.LabelManagedBy:   ControllerName,
					},
					Annotations:     map[string]string{},
					GenerateName:    fmt.Sprintf("%s-", endpoints.Name),
					Namespace:       endpoints.Namespace,
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				Ports:       ports,
				AddressType: addrType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			// 自定义注解必须原样透传到 EndpointSlice，否则用户通过 Endpoints 携带的元数据会丢失。
			name: "create slice from endpoints with annotations",
			tweakEndpoint: func(ep *v1.Endpoints) {
				annotations := map[string]string{"foo": "bar"}
				ep.Annotations = annotations
			},
			expectedSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						discovery.LabelServiceName: endpoints.Name,
						discovery.LabelManagedBy:   ControllerName,
					},
					Annotations:     map[string]string{"foo": "bar"},
					GenerateName:    fmt.Sprintf("%s-", endpoints.Name),
					Namespace:       endpoints.Namespace,
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				Ports:       ports,
				AddressType: addrType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			// 自定义标签会被保留，但控制器专用标签仍由控制器生成，避免用户覆盖。
			name: "create slice from endpoints with labels",
			tweakEndpoint: func(ep *v1.Endpoints) {
				labels := map[string]string{"foo": "bar"}
				ep.Labels = labels
			},
			expectedSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo":                      "bar",
						discovery.LabelServiceName: endpoints.Name,
						discovery.LabelManagedBy:   ControllerName,
					},
					Annotations:     map[string]string{},
					GenerateName:    fmt.Sprintf("%s-", endpoints.Name),
					Namespace:       endpoints.Namespace,
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				Ports:       ports,
				AddressType: addrType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			name: "create slice from endpoints with labels and annotations",
			tweakEndpoint: func(ep *v1.Endpoints) {
				labels := map[string]string{"foo": "bar"}
				ep.Labels = labels
				annotations := map[string]string{"foo2": "bar2"}
				ep.Annotations = annotations
			},
			expectedSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo":                      "bar",
						discovery.LabelServiceName: endpoints.Name,
						discovery.LabelManagedBy:   ControllerName,
					},
					Annotations:     map[string]string{"foo2": "bar2"},
					GenerateName:    fmt.Sprintf("%s-", endpoints.Name),
					Namespace:       endpoints.Namespace,
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				Ports:       ports,
				AddressType: addrType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
		{
			// EndpointsLastChangeTriggerTime 是内部追踪注解，不应暴露到 EndpointSlice 上。
			name: "create slice from endpoints with labels and annotations triggertime",
			tweakEndpoint: func(ep *v1.Endpoints) {
				labels := map[string]string{"foo": "bar"}
				ep.Labels = labels
				annotations := map[string]string{
					"foo2":                            "bar2",
					v1.EndpointsLastChangeTriggerTime: "date",
				}
				ep.Annotations = annotations
			},
			expectedSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo":                      "bar",
						discovery.LabelServiceName: endpoints.Name,
						discovery.LabelManagedBy:   ControllerName,
					},
					Annotations:     map[string]string{"foo2": "bar2"},
					GenerateName:    fmt.Sprintf("%s-", endpoints.Name),
					Namespace:       endpoints.Namespace,
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				Ports:       ports,
				AddressType: addrType,
				Endpoints:   []discovery.Endpoint{},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 深拷贝后再修改，保证原对象不被污染，下面才能检查输入未被改动。
			ep := endpoints.DeepCopy()
			tc.tweakEndpoint(ep)
			generatedSlice := newEndpointSlice(ep, ports, addrType, "")
			// 逐字段比较整个结果，防止遗漏标签或属主引用等关键字段。
			assert.EqualValues(t, tc.expectedSlice, *generatedSlice)
			// 构造过程若就地修改输入标签，会影响同一 Endpoints 上的其他切片生成。
			if len(endpoints.Labels) > 1 {
				t.Errorf("Expected Endpoints labels to not be modified, got %+v", endpoints.Labels)
			}
		})
	}
}

// TestAddressToEndpoint 验证地址转换会保留 IP、主机名、节点名和 TargetRef，
// 正确设置 Ready 条件，并依据 IP 家族推导出地址类型。
func TestAddressToEndpoint(t *testing.T) {
	// 简单场景，启用特性门控。
	epAddress := v1.EndpointAddress{
		IP:       "10.1.2.3",
		Hostname: "foo",
		NodeName: ptr.To("node-abc"),
		TargetRef: &v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "default",
			Name:       "foo",
		},
	}
	ready := true
	expectedEndpoint := discovery.Endpoint{
		Addresses: []string{"10.1.2.3"},
		Hostname:  ptr.To("foo"),
		Conditions: discovery.EndpointConditions{
			Ready: ptr.To(true),
		},
		TargetRef: &v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "default",
			Name:       "foo",
		},
		NodeName: ptr.To("node-abc"),
	}

	ep, addrType := addressToEndpoint(epAddress, ready)
	// 逐字段比对，确保 TargetRef 等容易被遗漏的字段也被正确复制。
	assert.EqualValues(t, expectedEndpoint, *ep)
	// 地址类型决定 EndpointSlice 的 AddressType，错误的推导会导致切片不可用。
	assert.EqualValues(t, discovery.AddressTypeIPv4, *addrType)
}

// 测试辅助函数

// newClientset 创建带 EndpointSlice 反应器的 fake 客户端。
// fake 客户端不会自动处理 GenerateName，也不会递增 Generation，
// 因此这里手动模拟这两个行为，使镜像控制器的更新流程可被测试。
func newClientset() *fake.Clientset {
	client := fake.NewSimpleClientset()

	client.PrependReactor("create", "endpointslices", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		endpointSlice := action.(k8stesting.CreateAction).GetObject().(*discovery.EndpointSlice)

		if endpointSlice.ObjectMeta.GenerateName != "" {
			endpointSlice.ObjectMeta.Name = fmt.Sprintf("%s-%s", endpointSlice.ObjectMeta.GenerateName, rand.String(8))
			endpointSlice.ObjectMeta.GenerateName = ""
		}
		endpointSlice.ObjectMeta.Generation = 1

		return false, endpointSlice, nil
	}))
	client.PrependReactor("update", "endpointslices", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		endpointSlice := action.(k8stesting.CreateAction).GetObject().(*discovery.EndpointSlice)
		endpointSlice.ObjectMeta.Generation++
		return false, endpointSlice, nil
	}))

	return client
}
