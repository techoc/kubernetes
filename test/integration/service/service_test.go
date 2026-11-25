/*
Copyright 2022 The Kubernetes Authors.

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

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/endpointslice"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/utils/ptr"
)

// Test_ExternalNameServiceStopsDefaultingInternalTrafficPolicy 测试当 Type 为 ExternalName 时，
// Service 不再对 internalTrafficPolicy 字段执行默认值填充。保留该测试是出于历史原因：
// 旧版本会对 internalTrafficPolicy 字段填充默认值。新版本停止填充该字段，并在读取时丢弃它，
// 但出于兼容性考虑，我们仍然接受该字段。
func Test_ExternalNameServiceStopsDefaultingInternalTrafficPolicy(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-external-name-drops-internal-traffic-policy", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: "foo.bar.com",
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}

	service, err = client.CoreV1().Services(ns.Name).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}
}

// Test_ExternalNameServiceDropsInternalTrafficPolicy 测试 Service 在创建时接受 internalTrafficPolicy 字段，
// 但在读取时丢弃该字段。保留该测试是出于历史原因：旧版本会对 internalTrafficPolicy 字段填充默认值。
// 新版本停止填充该字段，并在读取时丢弃它，但出于兼容性考虑，我们仍然接受该字段。
func Test_ExternalNameServiceDropsInternalTrafficPolicy(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-external-name-drops-internal-traffic-policy", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	internalTrafficPolicy := corev1.ServiceInternalTrafficPolicyCluster
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeExternalName,
			ExternalName:          "foo.bar.com",
			InternalTrafficPolicy: &internalTrafficPolicy,
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}

	service, err = client.CoreV1().Services(ns.Name).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}
}

// Test_ConvertingToExternalNameServiceDropsInternalTrafficPolicy 测试将 Service 转换为 Type=ExternalName
// 会导致 internalTrafficPolicy 字段被丢弃。保留该测试是出于历史原因：旧版本会对 internalTrafficPolicy
// 字段填充默认值。新版本停止填充该字段，并在读取时丢弃它，但出于兼容性考虑，我们仍然接受该字段。
func Test_ConvertingToExternalNameServiceDropsInternalTrafficPolicy(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-external-name-drops-internal-traffic-policy", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
			Selector: map[string]string{
				"foo": "bar",
			},
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	if *service.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster {
		t.Error("service internalTrafficPolicy was not set for clusterIP Service")
	}

	newService := service.DeepCopy()
	newService.Spec.Type = corev1.ServiceTypeExternalName
	newService.Spec.ExternalName = "foo.bar.com"

	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), newService, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("error updating service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}

	service, err = client.CoreV1().Services(ns.Name).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting service: %v", err)
	}

	if service.Spec.InternalTrafficPolicy != nil {
		t.Errorf("service internalTrafficPolicy should be droppped but is set: %v", service.Spec.InternalTrafficPolicy)
	}
}

// Test_RemovingExternalIPsFromClusterIPServiceDropsExternalTrafficPolicy 测试从 ClusterIP Service 中
// 移除 externalIPs 会导致 externalTrafficPolicy 字段被丢弃。
func Test_RemovingExternalIPsFromClusterIPServiceDropsExternalTrafficPolicy(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-removing-external-ips-drops-external-traffic-policy", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
			Selector: map[string]string{
				"foo": "bar",
			},
			ExternalIPs: []string{"1.1.1.1"},
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyCluster {
		t.Error("service externalTrafficPolicy was not set for clusterIP Service with externalIPs")
	}

	// 移除 externalIPs 后，externalTrafficPolicy 应该被丢弃。
	newService := service.DeepCopy()
	newService.Spec.ExternalIPs = []string{}

	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), newService, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("error updating service: %v", err)
	}

	if service.Spec.ExternalTrafficPolicy != "" {
		t.Errorf("service externalTrafficPolicy should be droppped but is set: %v", service.Spec.ExternalTrafficPolicy)
	}

	service, err = client.CoreV1().Services(ns.Name).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting service: %v", err)
	}

	if service.Spec.ExternalTrafficPolicy != "" {
		t.Errorf("service externalTrafficPolicy should be droppped but is set: %v", service.Spec.ExternalTrafficPolicy)
	}

	// 再次添加 externalIPs 后，externalTrafficPolicy 应该被设置。
	newService = service.DeepCopy()
	newService.Spec.ExternalIPs = []string{"1.1.1.1"}

	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), newService, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("error updating service: %v", err)
	}

	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyCluster {
		t.Error("service externalTrafficPolicy was not set for clusterIP Service with externalIPs")
	}

	service, err = client.CoreV1().Services(ns.Name).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting service: %v", err)
	}

	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyCluster {
		t.Error("service externalTrafficPolicy was not set for clusterIP Service with externalIPs")
	}
}

func assertEndpointSliceHints(t *testing.T, ctx context.Context, client *clientset.Clientset, namespace, serviceName string, expectZoneHints, expectNodeHints bool) {
	t.Helper()
	logsBuffer := &bytes.Buffer{}

	endpointSlicesHaveExpectedHints := func(ctx context.Context) (bool, error) {
		slices, err := client.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", discoveryv1.LabelServiceName, serviceName)})
		if err != nil {
			fmt.Fprintf(logsBuffer, "failed to list EndpointSlices for service %q: %v\n", serviceName, err)
			return false, nil
		}
		if slices == nil || len(slices.Items) == 0 {
			fmt.Fprintf(logsBuffer, "no EndpointSlices returned for service %q\n", serviceName)
			return false, nil
		}
		fmt.Fprintf(logsBuffer, "EndpointSlices=\n%v\n", format.Object(slices, 1 /* indent one level */))

		for _, slice := range slices.Items {
			for _, endpoint := range slice.Endpoints {
				var ip, zone, nodeName string
				if len(endpoint.Addresses) > 0 {
					ip = endpoint.Addresses[0]
				}
				if endpoint.Zone != nil {
					zone = *endpoint.Zone
				}
				if endpoint.NodeName != nil {
					nodeName = *endpoint.NodeName
				}
				if endpoint.Hints == nil {
					if expectZoneHints || expectNodeHints {
						fmt.Fprintf(logsBuffer, "endpoint with ip %v has no hints, expectZoneHints=%v expectNodeHints=%v\n", ip, expectZoneHints, expectNodeHints)
						return false, nil
					}
					continue
				}
				if expectZoneHints {
					if len(endpoint.Hints.ForZones) != 1 || endpoint.Hints.ForZones[0].Name != zone {
						fmt.Fprintf(logsBuffer, "endpoint with ip %v does not have the correct hint, want hint for zone %q\n", ip, zone)
						return false, nil
					}
				} else if len(endpoint.Hints.ForZones) != 0 {
					fmt.Fprintf(logsBuffer, "endpoint with ip %v has unexpected hint for zone %q\n", ip, endpoint.Hints.ForZones[0].Name)
					return false, nil
				}
				if expectNodeHints {
					if len(endpoint.Hints.ForNodes) != 1 || endpoint.Hints.ForNodes[0].Name != nodeName {
						fmt.Fprintf(logsBuffer, "endpoint with ip %v does not have the correct hint, want hint for node %q\n", ip, nodeName)
						return false, nil
					}
				} else if len(endpoint.Hints.ForNodes) != 0 {
					fmt.Fprintf(logsBuffer, "endpoint with ip %v has unexpected hint for node %q\n", ip, endpoint.Hints.ForNodes[0].Name)
					return false, nil
				}
			}
		}
		return true, nil
	}

	err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, endpointSlicesHaveExpectedHints)
	if err != nil {
		t.Logf("logsBuffer=\n%v", logsBuffer)
		t.Fatalf("Error waiting for EndpointSlices to have expected hints: %v", err)
	}
}

// 测试涉及 Service spec 中 `trafficDistribution` 字段的状态转换。
func Test_TransitionsForTrafficDistribution(t *testing.T) {

	////////////////////////////////////////////////////////////////////////////
	// 设置各类组件，例如 kube-apiserver 和 EndpointSlice controller。
	////////////////////////////////////////////////////////////////////////////

	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	resyncPeriod := 12 * time.Hour
	informers := informers.NewSharedInformerFactory(client, resyncPeriod)

	ctx := ktesting.Init(t)
	defer ctx.Cancel("test has completed")
	epsController := endpointslice.NewController(
		ctx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Nodes(),
		informers.Discovery().V1().EndpointSlices(),
		int32(100),
		client,
		1*time.Second,
	)

	informers.Start(ctx.Done())
	go epsController.Run(ctx, 1)

	////////////////////////////////////////////////////////////////////////////
	// 创建 namespace、Node、该 Node 上的 Pod，以及一个暴露该 Pod 的 Service。
	////////////////////////////////////////////////////////////////////////////

	ns := framework.CreateNamespaceOrDie(client, "test-service-traffic-distribution", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fake-node",
			Labels: map[string]string{
				corev1.LabelTopologyZone: "fake-zone-1",
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: ns.GetName(),
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: node.GetName(),
			Containers: []corev1.Container{
				{
					Name:  "fake-name",
					Image: "fake-image",
					Ports: []corev1.ContainerPort{
						{
							Name:          "port-443",
							ContainerPort: 443,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
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
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: ns.GetName(),
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

	_, err = client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}
	_, err = client.CoreV1().Pods(ns.Name).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test ready pod: %v", err)
	}
	_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status for test pod to Ready: %v", err)
	}
	_, err = client.CoreV1().Services(ns.Name).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test service: %v", err)
	}

	////////////////////////////////////////////////////////////////////////////
	// 断言在既没有 `trafficDistribution` 字段，也没有
	// service.kubernetes.io/topology-mode=Auto 注解的情况下，
	// EndpointSlice 中不存在 zone hint。
	////////////////////////////////////////////////////////////////////////////

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), false, false)

	////////////////////////////////////////////////////////////////////////////
	// 通过设置 `trafficDistribution: PreferClose` 字段来更新 Service。
	//
	// 断言相应的 EndpointSlice 获得同 zone 的 hint。
	////////////////////////////////////////////////////////////////////////////

	trafficDist := corev1.ServiceTrafficDistributionPreferClose
	svc.Spec.TrafficDistribution = &trafficDist
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test service with 'trafficDistribution: PreferClose': %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), true, false)

	////////////////////////////////////////////////////////////////////////////
	// 使用 service.kubernetes.io/topology-mode=Auto 注解更新 Service。
	//
	// 断言 service.kubernetes.io/topology-mode=Auto 生效后，
	// Service 对应的 EndpointSlice 不再有 hint，因为
	// 只有一个 Service Pod 时 topology 注解不起作用。
	////////////////////////////////////////////////////////////////////////////

	svc.Annotations = map[string]string{corev1.AnnotationTopologyMode: "Auto"}
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test service with 'service.kubernetes.io/topology-mode=Auto' annotation: %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), false, false)

	////////////////////////////////////////////////////////////////////////////
	// 从 Service 中移除 service.kubernetes.io/topology-mode=Auto 注解。
	//
	// 断言由于存在 `trafficDistribution: PreferClose` 字段，
	// Service 对应的 EndpointSlice 再次具有正确的同 zone hint。
	////////////////////////////////////////////////////////////////////////////

	svc.Annotations = map[string]string{}
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to remove annotation 'service.kubernetes.io/topology-mode=Auto' from service: %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), true, false)

	////////////////////////////////////////////////////////////////////////////
	// 从 Service 中移除 `trafficDistribution: PreferClose` 字段。
	//
	// 断言 Service 对应的 EndpointSlice 再次没有 zone hint。
	////////////////////////////////////////////////////////////////////////////

	svc.Spec.TrafficDistribution = nil
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to remove annotation 'service.kubernetes.io/topology-mode=Auto' from service: %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), false, false)

	////////////////////////////////////////////////////////////////////////////
	// 通过设置 `trafficDistribution: PreferSameZone` 来更新 Service。
	//
	// 断言相应的 EndpointSlice 获得同 zone 的 hint。
	////////////////////////////////////////////////////////////////////////////

	trafficDist = corev1.ServiceTrafficDistributionPreferSameZone
	svc.Spec.TrafficDistribution = &trafficDist
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test service with 'trafficDistribution: PreferSameZone': %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), true, false)

	////////////////////////////////////////////////////////////////////////////
	// 将 `trafficDistribution` 改为 `PreferSameNode`。
	//
	// 断言相应的 EndpointSlice 同时具有同 zone 和同 Node 的 hint。
	////////////////////////////////////////////////////////////////////////////

	trafficDist = corev1.ServiceTrafficDistributionPreferSameNode
	svc.Spec.TrafficDistribution = &trafficDist
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test service with 'trafficDistribution: PreferSameNode': %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), true, true)

	////////////////////////////////////////////////////////////////////////////
	// 从 Service 中移除 `trafficDistribution` 字段。
	//
	// 断言 Service 对应的 EndpointSlice 再次没有 zone hint。
	////////////////////////////////////////////////////////////////////////////

	svc.Spec.TrafficDistribution = nil
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to remove annotation 'service.kubernetes.io/topology-mode=Auto' from service: %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), false, false)

	////////////////////////////////////////////////////////////////////////////
	// 更新 Node 使其不再具有 zone label，并重新启用 `PreferSameNode`。
	//
	// 断言相应的 EndpointSlice 具有同 Node 的 hint，但没有同 zone 的 hint。
	////////////////////////////////////////////////////////////////////////////

	delete(node.Labels, corev1.LabelTopologyZone)
	_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test node with no zone label: %v", err)
	}

	trafficDist = corev1.ServiceTrafficDistributionPreferSameNode
	svc.Spec.TrafficDistribution = &trafficDist
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update test service with 'trafficDistribution: PreferSameNode': %v", err)
	}

	assertEndpointSliceHints(t, ctx, client, ns.GetName(), svc.GetName(), false, true)
}

func Test_ServiceClusterIPSelector(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-external-name-drops-internal-traffic-policy", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建 headless Service。
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-headless",
			Namespace: ns.Name,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
			Selector: map[string]string{
				"foo": "bar",
			},
		},
	}

	_, err = client.CoreV1().Services(ns.Name).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	// 只监听非 headless Service 的 informer。
	kubeInformers := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermNotEqualSelector("spec.clusterIP", corev1.ClusterIPNone).String()
	}))

	serviceInformer := kubeInformers.Core().V1().Services().Informer()
	serviceLister := kubeInformers.Core().V1().Services().Lister()
	serviceHasSynced := serviceInformer.HasSynced
	if _, err = serviceInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				svc := obj.(*corev1.Service)
				t.Logf("Added Service %#v", svc)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldSvc := oldObj.(*corev1.Service)
				newSvc := newObj.(*corev1.Service)
				t.Logf("Updated Service %#v to %#v", oldSvc, newSvc)
			},
			DeleteFunc: func(obj interface{}) {
				svc := obj.(*corev1.Service)
				t.Logf("Deleted Service %#v", svc)
			},
		},
	); err != nil {
		t.Fatalf("Error adding service informer handler: %v", err)
	}
	kubeInformers.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), serviceHasSynced)
	svcs, err := serviceLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Error listing services: %v", err)
	}
	// 预期只存在 kubernetes.default Service。
	if len(svcs) != 1 || svcs[0].Name != "kubernetes" {
		t.Fatalf("expected 1 services, got %d", len(svcs))
	}

	// 创建一个带 ClusterIP 的新 Service。
	service2 := service.DeepCopy()
	service2.Spec.ClusterIP = ""
	service2.Name = "test-clusterip"
	_, err = client.CoreV1().Services(ns.Name).Create(ctx, service2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		svc, err := serviceLister.Services(service2.Namespace).Get(service2.Name)
		if svc == nil || err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error waiting for test service test-clusterip: %v", err)
	}

	// 修改 Service 以移除 ClusterIP；理论上 ClusterIP 是不可变的，但……
	service.Spec.ExternalName = "test"
	service.Spec.Type = corev1.ServiceTypeExternalName
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		svc, err := serviceLister.Services(service.Namespace).Get(service.Name)
		if svc == nil || err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error waiting for test service without ClusterIP: %v", err)
	}

	// 修改 Service 以重新获得 ClusterIP。
	service.Spec.ExternalName = ""
	service.Spec.ClusterIP = ""
	service.Spec.Type = corev1.ServiceTypeClusterIP
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		svc, err := serviceLister.Services(service.Namespace).Get(service.Name)
		if svc == nil || err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error waiting for test service with ClusterIP: %v", err)
	}
}

// 复现 https://github.com/kubernetes/kubernetes/issues/123853
func Test_ServiceWatchUntil(t *testing.T) {
	svcReadyTimeout := 30 * time.Second

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-watchuntil", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	testSvcName := "test-service-" + utilrand.String(5)
	testSvcLabels := map[string]string{"test-service-static": "true"}
	testSvcLabelsFlat := "test-service-static=true"

	w := &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = testSvcLabelsFlat
			return client.CoreV1().Services(ns.Name).Watch(ctx, options)
		},
	}

	svcList, err := client.CoreV1().Services("").List(ctx, metav1.ListOptions{LabelSelector: testSvcLabelsFlat})
	if err != nil {
		t.Fatalf("failed to list Services: %v", err)
	}
	// 创建 Service。
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   testSvcName,
			Labels: testSvcLabels,
		},
		Spec: corev1.ServiceSpec{
			Type: "LoadBalancer",
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Protocol:   corev1.ProtocolTCP,
				Port:       int32(80),
				TargetPort: intstr.FromInt32(80),
			}},
			LoadBalancerClass: ptr.To[string]("example.com/internal-vip"),
		},
	}
	_, err = client.CoreV1().Services(ns.Name).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	ctxUntil, cancel := context.WithTimeout(ctx, svcReadyTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if svc, ok := event.Object.(*corev1.Service); ok {
			found := svc.ObjectMeta.Name == service.ObjectMeta.Name &&
				svc.ObjectMeta.Namespace == ns.Name &&
				svc.Labels["test-service-static"] == "true"
			if !found {
				t.Logf("observed Service %v in namespace %v with labels: %v & ports %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Spec.Ports)
				return false, nil
			}
			t.Logf("Found Service %v in namespace %v with labels: %v & ports %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Spec.Ports)
			return found, nil
		}
		t.Logf("Observed event: %+v", event.Object)
		return false, nil
	})
	if err != nil {
		t.Fatalf("Error service not found: %v", err)
	}

	t.Log("patching the ServiceStatus")
	lbStatus := corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{{IP: "203.0.113.1"}},
	}
	lbStatusJSON, err := json.Marshal(lbStatus)
	if err != nil {
		t.Fatalf("Error marshalling status: %v", err)
	}
	_, err = client.CoreV1().Services(ns.Name).Patch(ctx, testSvcName, types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"patchedstatus":"true"}},"status":{"loadBalancer":`+string(lbStatusJSON)+`}}`),
		metav1.PatchOptions{}, "status")
	if err != nil {
		t.Fatalf("Could not patch service status: %v", err)
	}

	t.Log("watching for the Service to be patched")
	ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
	defer cancel()

	_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if svc, ok := event.Object.(*corev1.Service); ok {
			found := svc.ObjectMeta.Name == service.ObjectMeta.Name &&
				svc.ObjectMeta.Namespace == ns.Name &&
				svc.Annotations["patchedstatus"] == "true"
			if !found {
				t.Logf("observed Service %v in namespace %v with annotations: %v & LoadBalancer: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
				return false, nil
			}
			t.Logf("Found Service %v in namespace %v with annotations: %v & LoadBalancer: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
			return found, nil
		}
		t.Logf("Observed event: %+v", event.Object)
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to locate Service %v in namespace %v", service.ObjectMeta.Name, ns)
	}
	t.Logf("Service %s has service status patched", testSvcName)

	t.Log("updating the ServiceStatus")

	var statusToUpdate, updatedStatus *corev1.Service
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		statusToUpdate, err = client.CoreV1().Services(ns.Name).Get(ctx, testSvcName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		statusToUpdate.Status.Conditions = append(statusToUpdate.Status.Conditions, metav1.Condition{
			Type:    "StatusUpdate",
			Status:  metav1.ConditionTrue,
			Reason:  "E2E",
			Message: "Set from e2e test",
		})

		updatedStatus, err = client.CoreV1().Services(ns.Name).UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("\n\n Failed to UpdateStatus. %v\n\n", err)
	}
	t.Logf("updatedStatus.Conditions: %#v", updatedStatus.Status.Conditions)

	t.Log("watching for the Service to be updated")
	ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if svc, ok := event.Object.(*corev1.Service); ok {
			found := svc.ObjectMeta.Name == service.ObjectMeta.Name &&
				svc.ObjectMeta.Namespace == ns.Name &&
				svc.Annotations["patchedstatus"] == "true"
			if !found {
				t.Logf("Observed Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
				return false, nil
			}
			for _, cond := range svc.Status.Conditions {
				if cond.Type == "StatusUpdate" &&
					cond.Reason == "E2E" &&
					cond.Message == "Set from e2e test" {
					t.Logf("Found Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.Conditions)
					return found, nil
				} else {
					t.Logf("Observed Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
					return false, nil
				}
			}
		}
		t.Logf("Observed event: %+v", event.Object)
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to locate Service %v in namespace %v", service.ObjectMeta.Name, ns)
	}
	t.Logf("Service %s has service status updated", testSvcName)

	t.Log("patching the service")
	servicePatchPayload, err := json.Marshal(corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"test-service": "patched",
			},
		},
	})

	_, err = client.CoreV1().Services(ns.Name).Patch(ctx, testSvcName, types.StrategicMergePatchType, []byte(servicePatchPayload), metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("failed to patch service. %v", err)
	}

	t.Log("watching for the Service to be patched")
	ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if svc, ok := event.Object.(*corev1.Service); ok {
			found := svc.ObjectMeta.Name == service.ObjectMeta.Name &&
				svc.ObjectMeta.Namespace == ns.Name &&
				svc.Labels["test-service"] == "patched"
			if !found {
				t.Logf("observed Service %v in namespace %v with labels: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels)
				return false, nil
			}
			t.Logf("Found Service %v in namespace %v with labels: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels)
			return found, nil
		}
		t.Logf("Observed event: %+v", event.Object)
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to locate Service %v in namespace %v", service.ObjectMeta.Name, ns)
	}

	t.Logf("Service %s patched", testSvcName)

	t.Log("deleting the service")
	err = client.CoreV1().Services(ns.Name).Delete(ctx, testSvcName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete the Service. %v", err)
	}

	t.Log("watching for the Service to be deleted")
	ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		switch event.Type {
		case watch.Deleted:
			if svc, ok := event.Object.(*corev1.Service); ok {
				found := svc.ObjectMeta.Name == service.ObjectMeta.Name &&
					svc.ObjectMeta.Namespace == ns.Name &&
					svc.Labels["test-service-static"] == "true"
				if !found {
					t.Logf("observed Service %v in namespace %v with labels: %v & annotations: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Annotations)
					return false, nil
				}
				t.Logf("Found Service %v in namespace %v with labels: %v & annotations: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Annotations)
				return found, nil
			}
		default:
			t.Logf("Observed event: %+v", event.Type)
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to delete Service %v in namespace %v", service.ObjectMeta.Name, ns)
	}
	t.Logf("Service %s deleted", testSvcName)
}

func Test_ServiceValidation_FeatureGateEnableDisable(t *testing.T) {

	////////////////////////////////////////////////////////////////////////////
	// 在启用 RelaxedServiceNameValidation feature gate 的情况下
	// 启动 kube-apiserver。
	////////////////////////////////////////////////////////////////////////////

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RelaxedServiceNameValidation, true)

	sharedEtcd := framework.SharedEtcd()
	server1 := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), sharedEtcd)

	client1, err := clientset.NewForConfig(server1.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	////////////////////////////////////////////////////////////////////////////
	// 创建名称分别以数字和字母开头的 Service。
	//
	// 断言在 feature gate 启用时这些 Service 均能创建成功。
	////////////////////////////////////////////////////////////////////////////

	ns := framework.CreateNamespaceOrDie(client1, "test-service-traffic-distribution", t)
	makeService := func(serviceName string) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: ns.GetName(),
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 443}},
			},
		}
	}

	// 预期成功，因为 feature gate 已启用。
	_, err = client1.CoreV1().Services(ns.Name).Create(t.Context(), makeService("test-service-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test service: %v", err)
	}

	// 预期成功，因为 feature gate 已启用。
	_, err = client1.CoreV1().Services(ns.Name).Create(t.Context(), makeService("9-test-service-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Successfully created service, but shouldn't have: %v", err)
	}

	////////////////////////////////////////////////////////////////////////////
	// 在禁用 RelaxedServiceNameValidation feature gate 的情况下
	// 重启 kube-apiserver。
	//
	// 断言这些 Service 只能按照旧的校验规则创建。
	////////////////////////////////////////////////////////////////////////////

	server1.TearDownFn()
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.34"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RelaxedServiceNameValidation, false)

	server2 := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), sharedEtcd)
	client2, err := clientset.NewForConfig(server2.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	// 预期成功，因为 feature gate 已禁用。
	_, err = client2.CoreV1().Services(ns.Name).Create(t.Context(), makeService("test-service-2"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test service: %v", err)
	}

	// 预期失败，因为 feature gate 已禁用，而该名称需要宽松校验。
	_, err = client2.CoreV1().Services(ns.Name).Create(t.Context(), makeService("9-test-service-2"), metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("Successfully created service, but shouldn't have: %v", err)
	}

	////////////////////////////////////////////////////////////////////////////
	// 断言在 feature gate 被禁用之前创建的 Service 仍然可以成功 patch，
	// 即使该操作需要宽松校验。
	////////////////////////////////////////////////////////////////////////////

	// 预期成功，因为该 Service 是在 feature gate 被禁用之前创建的。
	patch := []byte(`{"spec":{"selector":{"foo":"baz"}}}`)
	_, err = client2.CoreV1().Services(ns.Name).Patch(t.Context(), "9-test-service-1", types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("Failed to patch selector of service '9-test-service-1': %v", err)
	}

	server2.TearDownFn()
}
