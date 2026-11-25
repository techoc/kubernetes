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

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	servicecontroller "k8s.io/cloud-provider/controllers/service"
	fakecloud "k8s.io/cloud-provider/fake"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// Test_ServiceLoadBalancerAllocateNodePorts 验证 spec.allocateLoadBalancerNodePorts=false 的
// Service 不会分配 node port。
func Test_ServiceLoadBalancerDisableAllocateNodePorts(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-allocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(false),
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

	if serviceHasNodePorts(service) {
		t.Error("found node ports when none was expected")
	}
}

// Test_ServiceUpdateLoadBalancerAllocateNodePorts 验证 Service 从 ClusterIP 更新为 LoadBalancer
// 且 spec.allocateLoadBalancerNodePorts=false 时不会分配 node port。
func Test_ServiceUpdateLoadBalancerDisableAllocateNodePorts(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-allocate-node-ports", t)
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

	if serviceHasNodePorts(service) {
		t.Error("found node ports when none was expected")
	}

	service.Spec.Type = corev1.ServiceTypeLoadBalancer
	service.Spec.AllocateLoadBalancerNodePorts = ptr.To(false)
	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if serviceHasNodePorts(service) {
		t.Error("found node ports when none was expected")
	}
}

// Test_ServiceLoadBalancerSwitchToDeallocatedNodePorts 验证把 Service 切换为
// spec.allocateLoadBalancerNodePorts=false 不会回收已有的 node port。
func Test_ServiceLoadBalancerEnableThenDisableAllocatedNodePorts(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-deallocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(true),
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

	if !serviceHasNodePorts(service) {
		t.Error("expected node ports but found none")
	}

	service.Spec.AllocateLoadBalancerNodePorts = ptr.To(false)
	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if !serviceHasNodePorts(service) {
		t.Error("node ports were unexpectedly deallocated")
	}
}

// Test_ServiceLoadBalancerDisableAllocatedNodePort 验证把 Service 切换为
// spec.allocateLoadBalancerNodePorts=false 时可以回收已有的 node port。
func Test_ServiceLoadBalancerDisableAllocatedNodePort(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-deallocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(true),
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

	if !serviceHasNodePorts(service) {
		t.Error("expected node ports but found none")
	}

	service.Spec.AllocateLoadBalancerNodePorts = ptr.To(false)
	service.Spec.Ports[0].NodePort = 0
	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if serviceHasNodePorts(service) {
		t.Error("node ports were expected to be deallocated")
	}
}

// Test_ServiceLoadBalancerDisableAllocatedNodePorts 验证把 Service 切换为
// spec.allocateLoadBalancerNodePorts=false 时可以回收其中一个已有的 node port。
func Test_ServiceLoadBalancerDisableAllocatedNodePorts(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-deallocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(true),
			Ports: []corev1.ServicePort{{
				Name: "np-1",
				Port: int32(80),
			}, {
				Name: "np-2",
				Port: int32(81),
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

	if !serviceHasNodePorts(service) {
		t.Error("expected node ports but found none")
	}

	service.Spec.AllocateLoadBalancerNodePorts = ptr.To(false)
	service.Spec.Ports[0].NodePort = 0
	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if service.Spec.Ports[0].NodePort != 0 {
		t.Error("node ports[0] was expected to be deallocated")
	}
	if service.Spec.Ports[1].NodePort == 0 {
		t.Error("node ports was not expected to be deallocated")
	}
}

// Test_ServiceLoadBalancerDisableAllocatedNodePortsByPatch 验证通过 patch 把 Service 切换为
// spec.allocateLoadBalancerNodePorts=false 时可以回收其中一个已有的 node port。
func Test_ServiceLoadBalancerDisableAllocatedNodePortsByPatch(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-deallocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(true),
			Ports: []corev1.ServicePort{{
				Name: "np-1",
				Port: int32(80),
			}, {
				Name: "np-2",
				Port: int32(81),
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

	if !serviceHasNodePorts(service) {
		t.Error("expected node ports but found none")
	}

	clone := service.DeepCopy()
	clone.Spec.AllocateLoadBalancerNodePorts = ptr.To(false)
	clone.Spec.Ports[0].NodePort = 0

	oldData, err := json.Marshal(service)
	if err != nil {
		t.Fatalf("Error marshalling test service: %v", err)
	}
	newData, err := json.Marshal(clone)
	if err != nil {
		t.Fatalf("Error marshalling test service: %v", err)
	}
	patch, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, corev1.Service{})
	if err != nil {
		t.Fatalf("Error creating patch: %v", err)
	}

	service, err = client.CoreV1().Services(ns.Name).Patch(context.TODO(), service.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if service.Spec.Ports[0].NodePort != 0 {
		t.Error("node ports[0] was expected to be deallocated")
	}
	if service.Spec.Ports[1].NodePort == 0 {
		t.Error("node ports was not expected to be deallocated")
	}
}

// Test_ServiceLoadBalancerDisableThenEnableAllocatedNodePorts 验证把 Service 的
// spec.allocateLoadBalancerNodePorts 从 false 切回 true 时会分配新的 node port。
func Test_ServiceLoadBalancerDisableThenEnableAllocatedNodePorts(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-reallocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			AllocateLoadBalancerNodePorts: ptr.To(false),
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

	if serviceHasNodePorts(service) {
		t.Error("not expected node ports but found one")
	}

	service.Spec.AllocateLoadBalancerNodePorts = ptr.To(true)
	service, err = client.CoreV1().Services(ns.Name).Update(context.TODO(), service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating test service: %v", err)
	}

	if !serviceHasNodePorts(service) {
		t.Error("expected node ports but found none")
	}
}

func serviceHasNodePorts(svc *corev1.Service) bool {
	for _, port := range svc.Spec.Ports {
		if port.NodePort > 0 {
			return true
		}
	}

	return false
}

// Test_ServiceLoadBalancerEnableLoadBalancerClass 验证 LoadBalancer 类型的 Service 设置了
// spec.LoadBalancerClass 后，cloud provider 不应创建默认 load balancer。
func Test_ServiceLoadBalancerEnableLoadBalancerClass(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-load-balancer-class", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	controller, cloud, informer := newServiceController(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informer.Start(ctx.Done())
	go controller.Run(ctx, 1, controllersmetrics.NewControllerManagerMetrics("loadbalancer-test"))

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-load-balancer-class",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
			LoadBalancerClass: ptr.To("test.com/test"),
		},
	}

	_, err = client.CoreV1().Services(ns.Name).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	time.Sleep(5 * time.Second) // 等待 service controller 完成 reconcile
	if len(cloud.Calls) > 0 {
		t.Errorf("Unexpected cloud provider calls: %v", cloud.Calls)
	}
}

// Test_SetLoadBalancerClassThenUpdateLoadBalancerClass 验证 LoadBalancer 类型的 Service 设置
// spec.LoadBalancerClass 后，只要 Service 类型仍是 LoadBalancer，该字段就不可修改。
func Test_SetLoadBalancerClassThenUpdateLoadBalancerClass(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-immutable-load-balancer-class", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	controller, cloud, informer := newServiceController(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informer.Start(ctx.Done())
	go controller.Run(ctx, 1, controllersmetrics.NewControllerManagerMetrics("loadbalancer-test"))

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-load-balancer-class",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
			LoadBalancerClass: ptr.To("test.com/test"),
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	service.Spec.LoadBalancerClass = ptr.To("test.com/update")
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, service, metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("Error: updating test service load balancer class should throw error, field is immutable")
	}

	time.Sleep(5 * time.Second) // 等待 service controller 完成 reconcile
	if len(cloud.Calls) > 0 {
		t.Errorf("Unexpected cloud provider calls: %v", cloud.Calls)
	}
}

// Test_UpdateLoadBalancerWithLoadBalancerClass 验证 LoadBalancer 类型的 Service 从
// 未设置 loadBalancerClass 更新为设置该字段时，更新应被拒绝。
func Test_UpdateLoadBalancerWithLoadBalancerClass(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-update-load-balancer-class", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	controller, cloud, informer := newServiceController(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informer.Start(ctx.Done())
	go controller.Run(ctx, 1, controllersmetrics.NewControllerManagerMetrics("loadbalancer-test"))

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-update-load-balancer-class",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	service.Spec.LoadBalancerClass = ptr.To("test.com/test")
	_, err = client.CoreV1().Services(ns.Name).Update(ctx, service, metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("Error: updating test service load balancer class should throw error, field is immutable")
	}

	time.Sleep(5 * time.Second) // 等待 service controller 完成 reconcile
	if len(cloud.Calls) == 0 {
		t.Errorf("expected cloud provider calls to create load balancer")
	}
}

// Test_ServiceLoadBalancerMixedProtocolSetup 验证可以创建包含多种 protocol 的 LoadBalancer Service。
func Test_ServiceLoadBalancerMixedProtocolSetup(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-mixed-protocols", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	controller, cloud, informer := newServiceController(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informer.Start(ctx.Done())
	go controller.Run(ctx, 1, controllersmetrics.NewControllerManagerMetrics("loadbalancer-test"))

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{
					Name:     "tcpport",
					Port:     int32(53),
					Protocol: corev1.ProtocolTCP,
				},
				{
					Name:     "udpport",
					Port:     int32(53),
					Protocol: corev1.ProtocolUDP,
				},
			},
		},
	}

	_, err = client.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	time.Sleep(5 * time.Second) // 等待 service controller 完成 reconcile
	if len(cloud.Calls) == 0 {
		t.Errorf("expected cloud provider calls to create load balancer")
	}
}

func newServiceController(t *testing.T, client *clientset.Clientset) (*servicecontroller.Controller, *fakecloud.Cloud, informers.SharedInformerFactory) {
	cloud := &fakecloud.Cloud{}
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	serviceInformer := informerFactory.Core().V1().Services()
	nodeInformer := informerFactory.Core().V1().Nodes()

	controller, err := servicecontroller.New(cloud,
		client,
		serviceInformer,
		nodeInformer,
		"test-cluster",
		utilfeature.DefaultFeatureGate)
	if err != nil {
		t.Fatalf("Error creating service controller: %v", err)
	}
	cloud.ClearCalls() // 忽略 init() 中产生的 cloud 调用
	return controller, cloud, informerFactory
}

// Test_ServiceLoadBalancerIPMode 验证 cloud provider 是否正确更新了 ipMode 字段。
func Test_ServiceLoadBalancerIPMode(t *testing.T) {
	baseService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-update-load-balancer-ip-mode",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{
				Port: int32(80),
			}},
		},
	}

	testCases := []struct {
		setIPMode       *corev1.LoadBalancerIPMode
		externalIP      string
		expectedIngress corev1.LoadBalancerIngress
	}{
		{
			setIPMode:  nil,
			externalIP: "1.2.3.4",
			expectedIngress: corev1.LoadBalancerIngress{
				IP:     "1.2.3.4",
				IPMode: ptr.To(corev1.LoadBalancerIPModeVIP),
				Ports:  []corev1.PortStatus{{Port: 80, Protocol: corev1.ProtocolTCP}},
			},
		},
		{
			setIPMode:  ptr.To(corev1.LoadBalancerIPModeVIP),
			externalIP: "1.2.3.4",
			expectedIngress: corev1.LoadBalancerIngress{
				IP:     "1.2.3.4",
				IPMode: ptr.To(corev1.LoadBalancerIPModeVIP),
				Ports:  []corev1.PortStatus{{Port: 80, Protocol: corev1.ProtocolTCP}},
			},
		},
		{
			setIPMode:  ptr.To(corev1.LoadBalancerIPModeProxy),
			externalIP: "1.2.3.4",
			expectedIngress: corev1.LoadBalancerIngress{
				IP:     "1.2.3.4",
				IPMode: ptr.To(corev1.LoadBalancerIPModeProxy),
				Ports:  []corev1.PortStatus{{Port: 80, Protocol: corev1.ProtocolTCP}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
			defer server.TearDownFn()

			client, err := clientset.NewForConfig(server.ClientConfig)
			if err != nil {
				t.Fatalf("Error creating clientset: %v", err)
			}

			ns := framework.CreateNamespaceOrDie(client, "test-service-update-load-balancer-ip-mode", t)
			defer framework.DeleteNamespaceOrDie(client, ns, t)

			controller, cloud, informer := newServiceController(t, client)
			cloud.ExternalIP = net.ParseIPSloppy(tc.externalIP)
			cloud.BalancerIPMode = tc.expectedIngress.IPMode

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			informer.Start(ctx.Done())
			go controller.Run(ctx, 1, controllersmetrics.NewControllerManagerMetrics("loadbalancer-test"))

			service, err := client.CoreV1().Services(ns.Name).Create(ctx, baseService, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Error creating test service: %v", err)
			}

			err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 10*time.Second, true, func(_ context.Context) (done bool, err error) {
				service, err = client.CoreV1().Services(ns.Name).Get(ctx, service.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Error getting test service: %v", err)
				}
				if len(service.Status.LoadBalancer.Ingress) != 1 {
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				t.Fatalf("unexpected load balancer status")
			}

			ingress := service.Status.LoadBalancer.Ingress[0]
			if !apiequality.Semantic.DeepEqual(&ingress, &tc.expectedIngress) {
				t.Errorf("expected Ingress %v, got %v",
					tc.expectedIngress, ingress)
				if ingress.IPMode != nil && tc.expectedIngress.IPMode != nil {
					t.Logf("IPMode %v expected %v", *ingress.IPMode, *tc.expectedIngress.IPMode)
				}
			}

			// 修改 Service，验证 status 被保留
			newService := service.DeepCopy()
			newService.Spec.Ports[0].Port = 443
			service, err = client.CoreV1().Services(ns.Name).Update(ctx, newService, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Error updating test service: %v", err)
			}

			expectedIngress := tc.expectedIngress
			expectedIngress.Ports[0].Port = 443
			err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 10*time.Second, true, func(_ context.Context) (done bool, err error) {
				service, err = client.CoreV1().Services(ns.Name).Get(ctx, service.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Error getting test service: %v", err)
				}
				if len(service.Status.LoadBalancer.Ingress) != 1 {
					return false, nil
				}
				ingress = service.Status.LoadBalancer.Ingress[0]
				if !apiequality.Semantic.DeepEqual(&ingress, &expectedIngress) {
					t.Logf("Ingress %v Expected %v", ingress, expectedIngress)
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				t.Fatalf("unexpected load balancer status")
			}
		})
	}
}
