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

package dualstack

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controller/endpoint"
	"k8s.io/kubernetes/pkg/controller/endpointslice"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestDualStackEndpoints(t *testing.T) {
	// 创建 IPv4IPv6 双栈控制平面
	serviceCIDR := "10.0.0.0/16"
	secondaryServiceCIDR := "2001:db8:1::/112"
	labelMap := func() map[string]string {
		return map[string]string{"foo": "bar"}
	}

	tCtx := ktesting.Init(t)

	client, _, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.ServiceClusterIPRanges = fmt.Sprintf("%s,%s", serviceCIDR, secondaryServiceCIDR)
			// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
		},
	})
	defer tearDownFn()

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("Creating kubernetes service timed out")
	}

	resyncPeriod := 0 * time.Hour
	informers := informers.NewSharedInformerFactory(client, resyncPeriod)

	// 创建伪造节点
	testNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fakenode",
		},
		Spec: v1.NodeSpec{Unschedulable: false},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{
					Type:              v1.NodeReady,
					Status:            v1.ConditionTrue,
					Reason:            "schedulable condition",
					LastHeartbeatTime: metav1.Time{Time: time.Now()},
				},
			},
		},
	}
	if _, err := client.CoreV1().Nodes().Create(tCtx, testNode, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", testNode.Name, err)
	}

	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		1*time.Second)

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
	// 只使用一个 worker，使更新串行化
	go epController.Run(tCtx, 1)
	go epsController.Run(tCtx, 1)

	var testcases = []struct {
		name           string
		serviceType    v1.ServiceType
		ipFamilies     []v1.IPFamily
		ipFamilyPolicy v1.IPFamilyPolicy
	}{
		{
			name:           "Service IPv4 Only",
			serviceType:    v1.ServiceTypeClusterIP,
			ipFamilies:     []v1.IPFamily{v1.IPv4Protocol},
			ipFamilyPolicy: v1.IPFamilyPolicySingleStack,
		},
		{
			name:           "Service IPv6 Only",
			serviceType:    v1.ServiceTypeClusterIP,
			ipFamilies:     []v1.IPFamily{v1.IPv6Protocol},
			ipFamilyPolicy: v1.IPFamilyPolicySingleStack,
		},
		{
			name:           "Service IPv6 IPv4 Dual Stack",
			serviceType:    v1.ServiceTypeClusterIP,
			ipFamilies:     []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
			ipFamilyPolicy: v1.IPFamilyPolicyRequireDualStack,
		},
		{
			name:           "Service IPv4 IPv6 Dual Stack",
			serviceType:    v1.ServiceTypeClusterIP,
			ipFamilies:     []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
			ipFamilyPolicy: v1.IPFamilyPolicyRequireDualStack,
		},
	}

	for i, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ns := framework.CreateNamespaceOrDie(client, fmt.Sprintf("test-endpointslice-dualstack-%d", i), t)
			defer framework.DeleteNamespaceOrDie(client, ns, t)

			// 创建一个带标签的 Pod
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: ns.Name,
					Labels:    labelMap(),
				},
				Spec: v1.PodSpec{
					NodeName: "fakenode",
					Containers: []v1.Container{
						{
							Name:  "fake-name",
							Image: "fakeimage",
						},
					},
				},
			}

			createdPod, err := client.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
			}

			// 设置 Pod IP
			podIPbyFamily := map[v1.IPFamily]string{v1.IPv4Protocol: "1.1.1.1", v1.IPv6Protocol: "2001:db2::65"}
			createdPod.Status = v1.PodStatus{
				Phase:  v1.PodRunning,
				PodIPs: []v1.PodIP{{IP: podIPbyFamily[v1.IPv4Protocol]}, {IP: podIPbyFamily[v1.IPv6Protocol]}},
			}
			_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, createdPod, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
			}

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("svc-test-%d", i), // 每个测试使用不同的 Service
					Namespace: ns.Name,
					Labels:    labelMap(),
				},
				Spec: v1.ServiceSpec{
					Type:           v1.ServiceTypeClusterIP,
					IPFamilies:     tc.ipFamilies,
					IPFamilyPolicy: &tc.ipFamilyPolicy,
					Selector:       labelMap(),
					Ports: []v1.ServicePort{
						{
							Name:       fmt.Sprintf("port-test-%d", i),
							Port:       443,
							TargetPort: intstr.IntOrString{IntVal: 443},
							Protocol:   "TCP",
						},
					},
				},
			}

			// 创建 Service
			_, err = client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Error creating service: %v", err)
			}

			// 等待 Endpoints 创建完成
			// 旧版 Endpoints 不支持双栈，
			// 使用第一个 IP 地址族的地址
			if err := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
				e, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
				if err != nil {
					t.Logf("Error fetching endpoints: %v", err)
					return false, nil
				}
				// 检查 Endpoints 地址是否与 Service 第一个 IPFamily 的 Pod IP 匹配
				// 由于这是集成测试，PodIP 并非处于 "ready" 状态
				if len(e.Subsets) > 0 && len(e.Subsets[0].NotReadyAddresses) > 0 {
					if e.Subsets[0].NotReadyAddresses[0].IP == podIPbyFamily[tc.ipFamilies[0]] {
						return true, nil
					}
					t.Logf("Endpoint address %s does not match PodIP %s ", e.Subsets[0].Addresses[0].IP, podIPbyFamily[tc.ipFamilies[0]])
				}
				t.Logf("Endpoint does not contain addresses: %s", e.Name)
				return false, nil
			}); err != nil {
				t.Fatalf("Endpoints not found: %v", err)
			}

			// 等待 EndpointSlice 创建完成
			err = wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
				lSelector := discovery.LabelServiceName + "=" + svc.Name
				esList, err := client.DiscoveryV1().EndpointSlices(ns.Name).List(tCtx, metav1.ListOptions{LabelSelector: lSelector})
				if err != nil {
					t.Logf("Error listing EndpointSlices: %v", err)
					return false, nil
				}
				// 每个 ipFamily 必须有一个 EndpointSlice
				if len(esList.Items) != len(tc.ipFamilies) {
					t.Logf("Waiting for EndpointSlice to be created %v", esList)
					return false, nil
				}
				// 每个 IP 地址族必须有一个 endpoint 地址
				for _, ipFamily := range tc.ipFamilies {
					found := false
					for _, slice := range esList.Items {
						// 检查 endpoint 地址是否与 Pod IP 匹配
						if len(slice.Endpoints) > 0 && len(slice.Endpoints[0].Addresses) > 0 {
							if string(ipFamily) == string(slice.AddressType) &&
								slice.Endpoints[0].Addresses[0] == podIPbyFamily[ipFamily] {
								found = true
								break
							}
						}
						t.Logf("Waiting endpoint slice to contain addresses")
					}
					if !found {
						t.Logf("Endpoint slices does not contain PodIP %s", podIPbyFamily[ipFamily])
						return false, nil
					}
				}
				return true, nil
			})
			if err != nil {
				t.Fatalf("Error waiting for endpoint slices: %v", err)
			}
		})
	}
}
