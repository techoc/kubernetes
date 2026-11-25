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

package endpoints

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/endpoint"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
)

func TestEndpointUpdates(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	informers := informers.NewSharedInformerFactory(client, 0)

	tCtx := ktesting.Init(t)
	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		0)

	// 启动 informer 和 controller
	informers.Start(tCtx.Done())
	go epController.Run(tCtx, 1)

	// 创建 namespace
	ns := framework.CreateNamespaceOrDie(client, "test-endpoints-updates", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建带有 label 的 Pod
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
	createdPod.Status = v1.PodStatus{
		Phase:  v1.PodRunning,
		PodIPs: []v1.PodIP{{IP: "1.1.1.1"}, {IP: "2001:db8::"}},
	}
	_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, createdPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
	}

	// 创建与该 Pod 关联的 Service
	svc := newService(ns.Name, "foo1")
	svc1, err := client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create service %s: %v", svc.Name, err)
	}

	// 获取新建 Endpoints 的 ResourceVersion
	var resVersion string
	if err := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err != nil {
			t.Logf("error fetching endpoints: %v", err)
			return false, nil
		}
		resVersion = endpoints.ObjectMeta.ResourceVersion
		return true, nil
	}); err != nil {
		t.Fatalf("endpoints not found: %v", err)
	}

	// 强制 endpoint controller 重新计算
	svc1.SetAnnotations(map[string]string{"foo": "bar"})
	_, err = client.CoreV1().Services(ns.Name).Update(tCtx, svc1, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update service %s: %v", svc1.Name, err)
	}

	// 创建一个新 Service 并等待它被处理，
	// 这样我们可以确保在断言之前，原 Service 的 Endpoints
	// 已被重新计算，因为 endpoint controller 中只有 1 个 worker。
	svc2 := newService(ns.Name, "foo2")
	_, err = client.CoreV1().Services(ns.Name).Create(tCtx, svc2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create service %s: %v", svc.Name, err)
	}

	if err := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		_, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc2.Name, metav1.GetOptions{})
		if err != nil {
			t.Logf("error fetching endpoints: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("endpoints not found: %v", err)
	}

	// 由于没有任何变化，endpoint controller 不应更新为原 Service 创建的
	// Endpoints，其 resource version 必须保持不变。
	endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching endpoints: %v", err)
	}
	if resVersion != endpoints.ObjectMeta.ResourceVersion {
		t.Fatalf("endpoints resource version does not match, expected %s received %s", resVersion, endpoints.ObjectMeta.ResourceVersion)
	}

}

// 针对 https://issues.k8s.io/125638 的回归测试
func TestEndpointWithMultiplePodUpdates(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	informers := informers.NewSharedInformerFactory(client, 0)

	tCtx := ktesting.Init(t)
	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		0)

	// 并行处理 10 个 Service，以提高出现 informer 缓存过期的概率。
	concurrency := 10
	// 启动 informer 和 controller
	informers.Start(tCtx.Done())
	go epController.Run(tCtx, concurrency)

	// 创建 namespace
	ns := framework.CreateNamespaceOrDie(client, "test-endpoints-updates", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建带有 label 的 Pod
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

	pod, err = client.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
	}

	// 设置 Pod 状态
	pod.Status = v1.PodStatus{
		Phase:      v1.PodRunning,
		Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
		PodIPs:     []v1.PodIP{{IP: "1.1.1.1"}},
	}
	pod, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
	}

	var services []*v1.Service
	// 创建与该 Pod 关联的多个 Service
	for i := range concurrency {
		svc := newService(ns.Name, fmt.Sprintf("foo%d", i))
		_, err = client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create service %s: %v", svc.Name, err)
		}
		services = append(services, svc)
	}

	for _, service := range services {
		// 确保新的 Endpoints 已创建。
		if err := wait.PollUntilContextTimeout(tCtx, 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
			_, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, service.Name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Fatalf("endpoints not found: %v", err)
		}
	}

	// 更新 Pod 状态并立即恢复。Endpoints 最终应与 Pod 状态保持一致。
	pod.Status.Conditions[0].Status = v1.ConditionFalse
	pod, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update pod %s to not ready: %v", pod.Name, err)
	}

	pod.Status.Conditions[0].Status = v1.ConditionTrue
	pod, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update pod %s to ready: %v", pod.Name, err)
	}

	// 有些 worker 可能会更新 Endpoints 两次（Ready->NotReady->Ready），而另一些 worker
	// 如果很快收到第二次 Pod 更新，可能完全不更新 Endpoints。因此不能依靠 Endpoints 的
	// resource version 来判断 controller 是否已处理 Pod 更新。改为等待 1 秒，假设这段时间
	// 足以让 worker 至少处理一次 Endpoints。
	time.Sleep(1 * time.Second)
	expectedEndpointAddresses := []v1.EndpointAddress{
		{
			IP:       pod.Status.PodIP,
			NodeName: &pod.Spec.NodeName,
			TargetRef: &v1.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			},
		},
	}
	for _, service := range services {
		var endpoints *v1.Endpoints
		if err := wait.PollUntilContextTimeout(tCtx, 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
			endpoints, err = client.CoreV1().Endpoints(ns.Name).Get(tCtx, service.Name, metav1.GetOptions{})
			if err != nil {
				t.Logf("Error fetching endpoints: %v", err)
				return false, nil
			}
			if len(endpoints.Subsets) == 0 {
				return false, nil
			}
			if !reflect.DeepEqual(expectedEndpointAddresses, endpoints.Subsets[0].Addresses) {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Fatalf("Expected endpoints %v to contain ready endpoint addresses %v", endpoints, expectedEndpointAddresses)
		}
	}
}

// TestExternalNameToClusterIPTransition 测试 ExternalName 类型的 Service
// 不会有 Endpoints，而在转换为 ClusterIP 后，Service 会获得 Endpoints，
// 且不带 headless label。
func TestExternalNameToClusterIPTransition(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	informers := informers.NewSharedInformerFactory(client, 0)

	tCtx := ktesting.Init(t)
	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		0)

	// 启动 informer 和 controller
	informers.Start(tCtx.Done())
	go epController.Run(tCtx, 1)

	// 创建 namespace
	ns := framework.CreateNamespaceOrDie(client, "test-endpoints-updates", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建带有 label 的 Pod
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
	createdPod.Status = v1.PodStatus{
		Phase:  v1.PodRunning,
		PodIPs: []v1.PodIP{{IP: "1.1.1.1"}, {IP: "2001:db8::"}},
	}
	_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, createdPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
	}

	// 创建与该 Pod 关联的 ExternalName Service
	svc := newExternalNameService(ns.Name, "foo1")
	svc1, err := client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create service %s: %v", svc.Name, err)
	}

	err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err == nil {
			t.Errorf("expected no endpoints for externalName service, got: %v", endpoints)
			return true, nil
		}
		return false, nil
	})
	if err == nil {
		t.Errorf("expected error waiting for endpoints")
	}

	// 将 Service 更新为 ClusterIP 类型，并验证 Endpoints 已创建
	svc1.Spec.Type = v1.ServiceTypeClusterIP
	_, err = client.CoreV1().Services(ns.Name).Update(tCtx, svc1, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update service %s: %v", svc1.Name, err)
	}

	if err := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		ep, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc1.Name, metav1.GetOptions{})
		if err != nil {
			t.Logf("no endpoints found, error: %v", err)
			return false, nil
		}
		t.Logf("endpoint %s was successfully created", svc1.Name)
		if _, ok := ep.Labels[v1.IsHeadlessService]; ok {
			t.Errorf("ClusterIP endpoint should not have headless label, got: %v", ep)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("endpoints not found: %v", err)
	}
}

// TestEndpointWithTerminatingPod 测试处于 terminating 状态的 Pod 不会包含在 Endpoints 中。
// 此能力仅在较新的 EndpointSlice API 中提供，且没有计划为 Endpoints 加入该能力。
// 如果将来决定在 Endpoints 中包含 terminating endpoint，可以移除该测试，
// 但在此之前，该测试确保我们不会意外改变这一行为。
func TestEndpointWithTerminatingPod(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	informers := informers.NewSharedInformerFactory(client, 0)

	tCtx := ktesting.Init(t)
	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		0)

	// 启动 informer 和 controller
	informers.Start(tCtx.Done())
	go epController.Run(tCtx, 1)

	// 创建 namespace
	ns := framework.CreateNamespaceOrDie(client, "test-endpoints-terminating", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建带有 label 的 Pod
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "test-pod",
			Labels: labelMap(),
		},
		Spec: v1.PodSpec{
			NodeName: "fake-node",
			Containers: []v1.Container{
				{
					Name:  "fakename",
					Image: "fakeimage",
					Ports: []v1.ContainerPort{
						{
							Name:          "port-443",
							ContainerPort: 443,
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
			PodIP: "10.0.0.1",
			PodIPs: []v1.PodIP{
				{
					IP: "10.0.0.1",
				},
			},
		},
	}

	createdPod, err := client.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
	}

	createdPod.Status = pod.Status
	_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, createdPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
	}

	// 创建与该 Pod 关联的 Service
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: ns.Name,
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"foo": "bar",
			},
			Ports: []v1.ServicePort{
				{Name: "port-443", Port: 443, Protocol: "TCP", TargetPort: intstr.FromInt32(443)},
			},
		},
	}
	_, err = client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create service %s: %v", svc.Name, err)
	}

	// 轮询等待之前创建的 Service 对应的 Endpoints 出现
	if err := wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}

		numEndpoints := 0
		for _, subset := range endpoints.Subsets {
			numEndpoints += len(subset.Addresses)
		}

		if numEndpoints == 0 {
			return false, nil
		}

		return true, nil
	}); err != nil {
		t.Fatalf("endpoints not found: %v", err)
	}

	err = client.CoreV1().Pods(ns.Name).Delete(tCtx, pod.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("error deleting test pod: %v", err)
	}

	// 轮询等待被删除 Pod 的 endpoint 不再出现在 Endpoints 中。
	if err := wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		// 确保刚刚删除的 Pod 仍然存在但带有 deletion timestamp。如果 Pod 不存在，
		// 则应判定测试失败，因为此时它不再针对 terminating Pod 进行验证。
		pod, err := client.CoreV1().Pods(ns.Name).Get(tCtx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("expected Pod %q to exist with deletion timestamp but was not found: %v", pod.Name, err)
		}
		if err != nil {
			return false, nil
		}

		if pod.DeletionTimestamp == nil {
			return false, errors.New("pod did not have deletion timestamp set")
		}

		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}

		numEndpoints := 0
		for _, subset := range endpoints.Subsets {
			numEndpoints += len(subset.Addresses)
		}

		if numEndpoints > 0 {
			return false, nil
		}

		return true, nil
	}); err != nil {
		t.Fatalf("error checking for no endpoints with terminating pods: %v", err)
	}
}

func labelMap() map[string]string {
	return map[string]string{"foo": "bar"}
}

// newService 返回一个带有 selector 并暴露端口的 Service
func newService(namespace, name string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labelMap(),
		},
		Spec: v1.ServiceSpec{
			Selector: labelMap(),
			Ports: []v1.ServicePort{
				{Name: "port-1338", Port: 1338, Protocol: "TCP", TargetPort: intstr.FromInt32(1338)},
				{Name: "port-1337", Port: 1337, Protocol: "TCP", TargetPort: intstr.FromInt32(1337)},
			},
		},
	}
}

// newExternalNameService 返回一个带有 selector 并暴露端口的 ExternalName Service
func newExternalNameService(namespace, name string) *v1.Service {
	svc := newService(namespace, name)
	svc.Spec.Type = v1.ServiceTypeExternalName
	svc.Spec.ExternalName = "google.com"
	return svc
}

func TestEndpointTruncate(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client, err := clientset.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	informers := informers.NewSharedInformerFactory(client, 0)

	tCtx := ktesting.Init(t)
	epController := endpoint.NewEndpointController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().Services(),
		informers.Core().V1().Endpoints(),
		client,
		0)

	// 启动 informer 和 controller
	informers.Start(tCtx.Done())
	go epController.Run(tCtx, 1)

	// 创建 namespace
	ns := framework.CreateNamespaceOrDie(client, "test-endpoints-truncate", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建带有 label 的 Pod
	basePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "test-pod",
			Labels: labelMap(),
		},
		Spec: v1.PodSpec{
			NodeName: "fake-node",
			Containers: []v1.Container{
				{
					Name:  "fakename",
					Image: "fakeimage",
					Ports: []v1.ContainerPort{
						{
							Name:          "port-443",
							ContainerPort: 443,
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
			PodIP: "10.0.0.1",
			PodIPs: []v1.PodIP{
				{
					IP: "10.0.0.1",
				},
			},
		},
	}

	// 创建 1001 个 Pod，以达到设置为 1000 的 Endpoints 最大容量
	allPodNames := sets.New[string]()
	baseIP := netutils.BigForIP(netutils.ParseIPSloppy("10.0.0.1"))
	for i := range 1001 {
		pod := basePod.DeepCopy()
		pod.Name = fmt.Sprintf("%s-%d", basePod.Name, i)
		allPodNames.Insert(pod.Name)
		podIP := netutils.AddIPOffset(baseIP, i).String()
		pod.Status.PodIP = podIP
		pod.Status.PodIPs[0] = v1.PodIP{IP: podIP}
		createdPod, err := client.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
		}

		createdPod.Status = pod.Status
		_, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, createdPod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
		}
	}

	// 创建与该 Pod 关联的 Service
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: ns.Name,
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"foo": "bar",
			},
			Ports: []v1.ServicePort{
				{Name: "port-443", Port: 443, Protocol: "TCP", TargetPort: intstr.FromInt32(443)},
			},
		},
	}
	_, err = client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create service %s: %v", svc.Name, err)
	}

	var truncatedPodName string
	// 轮询等待之前创建的 Service 对应的 Endpoints 出现
	if err := wait.PollUntilContextTimeout(tCtx, 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
		podNames := sets.New[string]()
		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}

		for _, subset := range endpoints.Subsets {
			for _, address := range subset.Addresses {
				podNames.Insert(address.TargetRef.Name)
			}
		}

		if podNames.Len() != 1000 {
			return false, nil
		}

		truncated, ok := endpoints.Annotations[v1.EndpointsOverCapacity]
		if !ok || truncated != "truncated" {
			return false, nil
		}
		// 只有 1 个 Pod 被截断。
		truncatedPodName, _ = allPodNames.Difference(podNames).PopAny()
		return true, nil
	}); err != nil {
		t.Fatalf("endpoints not found: %v", err)
	}

	// 多次更新被截断的 Pod，使 endpoint controller 重新同步该 Service。
	truncatedPod, err := client.CoreV1().Pods(ns.Name).Get(tCtx, truncatedPodName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get pod %s: %v", truncatedPodName, err)
	}
	for range 10 {
		truncatedPod.Status.Conditions[0].Status = v1.ConditionFalse
		truncatedPod, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, truncatedPod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status of pod %s: %v", truncatedPod.Name, err)
		}
		truncatedPod.Status.Conditions[0].Status = v1.ConditionTrue
		truncatedPod, err = client.CoreV1().Pods(ns.Name).UpdateStatus(tCtx, truncatedPod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status of pod %s: %v", truncatedPod.Name, err)
		}
	}

	// 删除 501 个 Pod
	for i := 500; i < 1001; i++ {
		podName := fmt.Sprintf("%s-%d", basePod.Name, i)
		err = client.CoreV1().Pods(ns.Name).Delete(tCtx, podName, metav1.DeleteOptions{})
		if err != nil {
			t.Fatalf("error deleting test pod: %v", err)
		}
	}

	// 轮询等待被删除 Pod 的 endpoint 不再出现在 Endpoints 中。
	if err := wait.PollUntilContextTimeout(tCtx, 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
		endpoints, err := client.CoreV1().Endpoints(ns.Name).Get(tCtx, svc.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}

		numEndpoints := 0
		for _, subset := range endpoints.Subsets {
			numEndpoints += len(subset.Addresses)
		}

		if numEndpoints != 500 {
			return false, nil
		}

		truncated, ok := endpoints.Annotations[v1.EndpointsOverCapacity]
		if ok || truncated == "truncated" {
			return false, nil
		}

		return true, nil
	}); err != nil {
		t.Fatalf("error checking for no endpoints with terminating pods: %v", err)
	}
}
