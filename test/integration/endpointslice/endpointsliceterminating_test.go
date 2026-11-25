/*
Copyright 2021 The Kubernetes Authors.

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
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/endpointslice"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// TestEndpointSliceTerminating 测试 terminating endpoint 会被包含，并且
// ready、serving 和 terminating 条件被正确设置。
func TestEndpointSliceTerminating(t *testing.T) {
	testcases := []struct {
		name              string
		podStatus         corev1.PodStatus
		expectedEndpoints []discovery.Endpoint
	}{
		{
			name: "ready terminating pods",
			podStatus: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
				PodIP: "10.0.0.1",
				PodIPs: []corev1.PodIP{
					{
						IP: "10.0.0.1",
					},
				},
			},
			expectedEndpoints: []discovery.Endpoint{
				{
					Addresses: []string{"10.0.0.1"},
					Conditions: discovery.EndpointConditions{
						Ready:       ptr.To(false),
						Serving:     ptr.To(true),
						Terminating: ptr.To(true),
					},
				},
			},
		},
		{
			name: "not ready terminating pods",
			podStatus: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
				PodIP: "10.0.0.1",
				PodIPs: []corev1.PodIP{
					{
						IP: "10.0.0.1",
					},
				},
			},
			expectedEndpoints: []discovery.Endpoint{
				{
					Addresses: []string{"10.0.0.1"},
					Conditions: discovery.EndpointConditions{
						Ready:       ptr.To(false),
						Serving:     ptr.To(false),
						Terminating: ptr.To(true),
					},
				},
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
			server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
			defer server.TearDownFn()

			client, err := clientset.NewForConfig(server.ClientConfig)
			if err != nil {
				t.Fatalf("Error creating clientset: %v", err)
			}

			resyncPeriod := 12 * time.Hour
			informers := informers.NewSharedInformerFactory(client, resyncPeriod)

			tCtx := ktesting.Init(t)
			epsController := endpointslice.NewController(
				tCtx,
				informers.Core().V1().Pods(),
				informers.Core().V1().Services(),
				informers.Core().V1().Nodes(),
				informers.Discovery().V1().EndpointSlices(),
				int32(100),
				client,
				1*time.Second)

			// 启动 informer 和 controller
			informers.Start(tCtx.Done())
			go epsController.Run(tCtx, 1)

			// 创建 namespace
			ns := framework.CreateNamespaceOrDie(client, "test-endpoints-terminating", t)
			defer framework.DeleteNamespaceOrDie(client, ns, t)

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "fake-node",
				},
			}

			_, err = client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create test node: %v", err)
			}

			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: ns.Name,
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: corev1.ServiceSpec{
					Selector: map[string]string{
						"foo": "bar",
					},
					Ports: []corev1.ServicePort{
						{Name: "port-443", Port: 443, Protocol: "TCP", TargetPort: intstr.FromInt32(443)},
					},
				},
			}

			_, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), svc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create test Service: %v", err)
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "fake-node",
					Containers: []corev1.Container{
						{
							Name:  "fakename",
							Image: "fakeimage",
							Ports: []corev1.ContainerPort{
								{
									Name:          "port-443",
									ContainerPort: 443,
								},
							},
						},
					},
				},
			}

			pod, err = client.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create test ready pod: %v", err)
			}

			pod.Status = testcase.podStatus
			_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to update status for test ready pod: %v", err)
			}

			// 首先检查 endpoint 是否被包含，测试中应始终有 1 个初始 endpoint
			err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
				esList, err := client.DiscoveryV1().EndpointSlices(ns.Name).List(context.TODO(), metav1.ListOptions{
					LabelSelector: discovery.LabelServiceName + "=" + svc.Name,
				})

				if err != nil {
					return false, err
				}

				if len(esList.Items) == 0 {
					return false, nil
				}

				numEndpoints := 0
				for _, slice := range esList.Items {
					numEndpoints += len(slice.Endpoints)
				}

				if numEndpoints > 0 {
					return true, nil
				}

				return false, nil
			})
			if err != nil {
				t.Errorf("Error waiting for endpoint slices: %v", err)
			}

			// 删除 Pod 并检查 EndpointSlice 的条件
			err = client.CoreV1().Pods(ns.Name).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("Failed to delete pod in terminating state: %v", err)
			}

			// 验证终止 endpoint 后 EndpointSlice 中会出现预期的 endpoint。
			// 此处使用更严格的超时时间，以便捕获移除已终止 endpoint 所耗时间的回归。
			var endpoints []discovery.Endpoint
			err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
				esList, err := client.DiscoveryV1().EndpointSlices(ns.Name).List(context.TODO(), metav1.ListOptions{
					LabelSelector: discovery.LabelServiceName + "=" + svc.Name,
				})

				if err != nil {
					return false, err
				}

				if len(esList.Items) == 0 {
					return false, nil
				}

				endpoints = esList.Items[0].Endpoints
				if len(endpoints) == 0 && len(testcase.expectedEndpoints) == 0 {
					return true, nil
				}

				if len(endpoints) != len(testcase.expectedEndpoints) {
					return false, nil
				}

				if !reflect.DeepEqual(endpoints[0].Addresses, testcase.expectedEndpoints[0].Addresses) {
					return false, nil
				}

				if !reflect.DeepEqual(endpoints[0].Conditions, testcase.expectedEndpoints[0].Conditions) {
					return false, nil
				}

				return true, nil
			})
			if err != nil {
				t.Logf("actual endpoints: %v", endpoints)
				t.Logf("expected endpoints: %v", testcase.expectedEndpoints)
				t.Errorf("unexpected endpoints: %v", err)
			}
		})
	}
}
