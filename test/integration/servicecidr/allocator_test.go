/*
Copyright 2023 The Kubernetes Authors.

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

package servicecidr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller/servicecidrs"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestServiceAllocation(t *testing.T) {
	// 创建一个 IPv4 单栈 control-plane
	serviceCIDR := "192.168.0.0/29"
	var testcases = []struct {
		name                 string
		ipAllocatorGate      bool
		disableDualWriteGate bool
	}{
		{
			name:                 "Bitmap allocator",
			ipAllocatorGate:      false,
			disableDualWriteGate: false,
		},
		{
			name:                 "IP allocator and dual write",
			ipAllocatorGate:      true,
			disableDualWriteGate: false,
		},
		{
			name:                 "IP allocator only",
			ipAllocatorGate:      true,
			disableDualWriteGate: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			etcdOptions := framework.SharedEtcd()
			apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
			s1 := kubeapiservertesting.StartTestServerOrDie(t,
				apiServerOptions,
				[]string{
					"--service-cluster-ip-range=" + serviceCIDR,
					"--advertise-address=10.0.0.2",
					"--disable-admission-plugins=ServiceAccount",
					"--emulated-version=1.33",
					fmt.Sprintf("--feature-gates=%s=%v,%s=%v", features.MultiCIDRServiceAllocator, tc.ipAllocatorGate, features.DisableAllocatorDualWrite, tc.disableDualWriteGate),
				},
				etcdOptions)
			defer s1.TearDownFn()

			client, err := clientset.NewForConfig(s1.ClientConfig)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			svc := func(i int) *v1.Service {
				return &v1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("svc-%v", i),
					},
					Spec: v1.ServiceSpec{
						Type: v1.ServiceTypeClusterIP,
						Ports: []v1.ServicePort{
							{Port: 80},
						},
					},
				}
			}

			// 等待默认的 "kubernetes" service 创建完成。
			if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 15*time.Second, true, func(context.Context) (bool, error) {
				_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(context.TODO(), "kubernetes", metav1.GetOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
				return !apierrors.IsNotFound(err), nil
			}); err != nil {
				t.Fatalf("creating kubernetes service timed out: %v", err)
			}

			// 再创建 5 个 service 以占满所有 IP
			for i := range 5 {
				if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc(i), metav1.CreateOptions{}); err != nil {
					t.Error(err)
				}
			}

			// 再创建一个 service。由于集群 IP 已耗尽，该操作会失败
			if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc(8), metav1.CreateOptions{}); err != nil {
				if !strings.Contains(err.Error(), "range is full") {
					t.Errorf("unexpected error text: %v", err)
				}
			} else {
				svcs, err := client.CoreV1().Services(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
				if err != nil {
					t.Fatalf("unexpected success, and error getting the services: %v", err)
				}
				allIPs := []string{}
				for _, s := range svcs.Items {
					allIPs = append(allIPs, s.Spec.ClusterIP)
				}
				t.Fatalf("unexpected creation success. The following IPs exist: %#v. It should only be possible to allocate 2 IP addresses in this cluster.\n\n%#v", allIPs, svcs)
			}

			// 删除第一个 service。
			if err := client.CoreV1().Services(metav1.NamespaceDefault).Delete(context.TODO(), svc(1).ObjectMeta.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}

			// ipallocator 有一个 informer，它需要等待删除事件传播后
			// 才能重新分配该 IP。这次创建第二个 service 应该成功，
			// 假设 informer 最多需要 2 秒来赶上进度。
			err = wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
				_, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(ctx, svc(8), metav1.CreateOptions{})
				return err == nil, nil
			})
			if err != nil {
				t.Fatalf("unexpected creation failure: %v", err)
			}
		})
	}
}

func TestServiceAllocIPAddressLargeCIDR(t *testing.T) {
	// 创建一个具有大地址范围的 IPv6 单栈 control-plane
	serviceCIDR := "2001:db8::/64"
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s1 := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=" + serviceCIDR,
			"--advertise-address=2001:db8::10",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s1.TearDownFn()

	client, err := clientset.NewForConfig(s1.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	svc := func(i int) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("svc-%v", i),
			},
			Spec: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{Port: 80},
				},
			},
		}
	}

	// Wait until the default "kubernetes" service is created.
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	// 创建 5 个随机 service 并检查它们是否关联了 IP
	for i := range 5 {
		svc, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc(i), metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
		}
		_, err = client.NetworkingV1().IPAddresses().Get(tCtx, svc.Spec.ClusterIP, metav1.GetOptions{})
		if err != nil {
			t.Error(err)
		}
	}

	// 在地址范围末尾创建一个 service，以验证整个范围内都可分配，
	// 因为创建 2^64 个 service 并不现实
	lastSvc := svc(8)
	lastSvc.Spec.ClusterIP = "2001:db8::ffff:ffff:ffff:ffff"
	if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, lastSvc, metav1.CreateOptions{}); err != nil {
		t.Errorf("unexpected error text: %v", err)
	}

	_, err = client.NetworkingV1().IPAddresses().Get(context.TODO(), lastSvc.Spec.ClusterIP, metav1.GetOptions{})
	if err != nil {
		t.Error(err)
	}

}

func TestMigrateService(t *testing.T) {
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/24",
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()
	serviceName := "test-old-service"
	namespace := "old-service-ns"
	// 创建一个 service 并将其存入 etcd
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:              serviceName,
			Namespace:         namespace,
			CreationTimestamp: metav1.Now(),
			UID:               "08675309-9376-9376-9376-086753099999",
		},
		Spec: v1.ServiceSpec{
			ClusterIP: "10.0.0.11",
			Ports: []v1.ServicePort{
				{
					Name: "test-port",
					Port: 81,
				},
			},
		},
	}
	svcJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), svc)
	if err != nil {
		t.Fatalf("Failed creating service JSON: %v", err)
	}
	key := "/" + etcdOptions.Prefix + "/services/specs/" + namespace + "/" + serviceName
	if _, err := s.EtcdClient.Put(context.Background(), key, string(svcJSON)); err != nil {
		t.Error(err)
	}
	t.Logf("Service stored in etcd %v", string(svcJSON))

	kubeclient, err := clientset.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ns := framework.CreateNamespaceOrDie(kubeclient, namespace, t)
	defer framework.DeleteNamespaceOrDie(kubeclient, ns, t)

	// TODO: 弄清为什么无法通过 List 获取该 Service，只有通过
	// 更新 Service 触发事件后才能获取到。
	_, err = kubeclient.CoreV1().Services(namespace).Update(context.Background(), svc, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err)
	}

	err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		// 修复循环必须创建关联的 IP address
		_, err = kubeclient.NetworkingV1().IPAddresses().Get(context.TODO(), svc.Spec.ClusterIP, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Error(err)
	}

}

// TestSkewedAllocatorsRollback 先用新分配器创建一个 apiserver，
// 再启动一个使用 bitmap 分配器的旧 apiserver。
func TestSkewedAllocatorsRollback(t *testing.T) {
	// TODO(#134606): Fix or remove this test.
	t.Skip("Temporarily disabled: see http://issues.k8s.io/134606")

	svc := func(i int) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("svc-%v", i),
			},
			Spec: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{Port: 80},
				},
			},
		}
	}

	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	// s1 使用 IPAddress 分配器
	s1 := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/24",
			"--disable-admission-plugins=ServiceAccount"},
		etcdOptions)
	defer s1.TearDownFn()

	kubeclient1, err := clientset.NewForConfig(s1.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 创建 5 个随机 service 并检查它们是否关联了 IP
	for i := range 5 {
		service, err := kubeclient1.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc(i), metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = kubeclient1.NetworkingV1().IPAddresses().Get(context.TODO(), service.Spec.ClusterIP, metav1.GetOptions{})
		if err != nil {
			t.Error(err)
		}
	}

	// s2 使用 bitmap 分配器
	s2 := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/24",
			"--disable-admission-plugins=ServiceAccount",
			"--emulated-version=1.33",
			fmt.Sprintf("--feature-gates=%s=false,%s=false", features.MultiCIDRServiceAllocator, features.DisableAllocatorDualWrite)},
		etcdOptions)
	defer s2.TearDownFn()

	kubeclient2, err := clientset.NewForConfig(s2.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 创建 5 个随机 service 并检查它们是否关联了 IP
	for i := 5; i < 10; i++ {
		service, err := kubeclient2.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc(i), metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
		}

		err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
			// 修复循环必须创建关联的 IP address
			_, err = kubeclient1.NetworkingV1().IPAddresses().Get(context.TODO(), service.Spec.ClusterIP, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Error(err)
		}

	}

}

// TestSkewAllocatorsRollout 测试两个不同的 apiserver（一个启用了
// feature gate，另一个未启用）不会将同一个 IP
// 分配给两个不同的 Service
func TestSkewAllocatorsRollout(t *testing.T) {
	svc := func(name string, ip string) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1.ServiceSpec{
				Type:      v1.ServiceTypeClusterIP,
				ClusterIP: ip,
				Ports: []v1.ServicePort{
					{Port: 80},
				},
			},
		}
	}

	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	// 这里的顺序很重要，因为 apiserver 的分配器逻辑需要对
	// Allocator 接口进行类型转换，才能传入 Service 引用。

	// oldServer 使用 bitmap 分配器
	oldServer := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16",
			"--disable-admission-plugins=ServiceAccount",
			"--emulated-version=1.33",
			fmt.Sprintf("--feature-gates=%s=false,%s=false", features.MultiCIDRServiceAllocator, features.DisableAllocatorDualWrite)},
		etcdOptions)
	defer oldServer.TearDownFn()

	kubeclientOld, err := clientset.NewForConfig(oldServer.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// s1 使用 IPAddress 分配器
	newServer := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16",
			"--disable-admission-plugins=ServiceAccount",
			"--emulated-version=1.33",
			fmt.Sprintf("--feature-gates=%s=true,%s=false", features.MultiCIDRServiceAllocator, features.DisableAllocatorDualWrite)},
		etcdOptions)
	defer newServer.TearDownFn()

	kubeclientNew, err := clientset.NewForConfig(newServer.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	namespace := "test-ns"
	ns := framework.CreateNamespaceOrDie(kubeclientNew, namespace, t)
	defer framework.DeleteNamespaceOrDie(kubeclientNew, ns, t)

	// 在每个 apiserver 上并行创建两个使用相同 ClusterIP 的 Service，重复 N 次。
	concurrency := 100
	var errorsOld, errorsNew atomic.Uint64

	var wg sync.WaitGroup
	for i := 5; i < concurrency+5; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		service1 := svc(fmt.Sprintf("svc-%d-new", i), ip)
		service2 := svc(fmt.Sprintf("svc-%d-old", i), ip)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := kubeclientNew.CoreV1().Services(namespace).Create(context.TODO(), service1, metav1.CreateOptions{})
			if err != nil {
				t.Logf("Service %s with ip %s result: %v", service1.Name, service1.Spec.ClusterIP, err)
				errorsNew.Add(1)
			}
		}()

		go func() {
			defer wg.Done()
			_, err := kubeclientOld.CoreV1().Services(namespace).Create(context.TODO(), service2, metav1.CreateOptions{})
			if err != nil {
				t.Logf("Service %s with ip %s result: %v", service2.Name, service2.Spec.ClusterIP, err)
				errorsOld.Add(1)
			}
		}()
	}

	wg.Wait()

	errorsTotal := errorsOld.Load() + errorsNew.Load()
	t.Logf("errors received, old allocator %d new allocator %d", errorsOld.Load(), errorsNew.Load())
	if errorsTotal != uint64(concurrency) {
		t.Fatalf("expected %d Services creation to have failed, got %d", concurrency, errorsTotal)
	}

	// Service 需要一些时间才会可用，
	servicesList := []v1.Service{}
	err = wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
		svcs, err := kubeclientNew.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		if len(svcs.Items) != concurrency {
			t.Logf("expected %d Services to exist, got  %d", concurrency, len(svcs.Items))
			return false, nil
		}
		servicesList = svcs.Items
		return true, nil
	})
	if err != nil {
		t.Fatalf("No expected Services objects created: %v", err)
	}

	// repairip 循环需要一些时间才能创建相应的 IPAddress 对象，
	// ClusterIP 通过 bitmap 进行同步。
	err = wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
		ips, err := kubeclientNew.NetworkingV1().IPAddresses().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		// 也把 kubernetes.default service 计算在内
		if len(ips.Items) != concurrency+1 {
			t.Logf("expected %d IPAddresses to exist, got %d: %v", concurrency+1, len(ips.Items), ips.Items)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("No expected IPAddress objects created: %v", err)
	}

	allIPs := map[string]string{}
	for _, s := range servicesList {
		if svc, ok := allIPs[s.Spec.ClusterIP]; ok {
			t.Fatalf("duplicate IP %s for Services %s and %s", s.Spec.ClusterIP, svc, s.Name)
		} else {
			allIPs[s.Spec.ClusterIP] = s.Name
		}
	}

	// 检查所有 IPAddress 对象都已创建
	for i := 5; i < concurrency+5; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		err = wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 10*time.Second, true, func(context.Context) (bool, error) {
			// 修复循环必须创建关联的 IP address
			_, err = kubeclientNew.NetworkingV1().IPAddresses().Get(context.Background(), ip, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("No expected IPAddress objects created: %v", err)
		}
	}
}

func TestFlagsIPAllocator(t *testing.T) {
	svc := func(i int) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("svc-%v", i),
			},
			Spec: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{Port: 80},
				},
			},
		}
	}

	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	// s1 使用 IPAddress 分配器
	s1 := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions,
		[]string{"--service-cluster-ip-range=10.0.0.0/24"},
		etcdOptions)
	defer s1.TearDownFn()

	kubeclient1, err := clientset.NewForConfig(s1.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 创建 5 个随机 service 并检查它们是否关联了 IP
	for i := range 5 {
		service, err := kubeclient1.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc(i), metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = kubeclient1.NetworkingV1().IPAddresses().Get(context.TODO(), service.Spec.ClusterIP, metav1.GetOptions{})
		if err != nil {
			t.Error(err)
		}
	}

}

// 针对 https://issues.k8s.io/135333 的回归测试
func TestInvalidService(t *testing.T) {
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/24",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := clientset.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// ServiceCIDR 控制器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resyncPeriod := 12 * time.Hour
	informerFactory := informers.NewSharedInformerFactory(client, resyncPeriod)
	go servicecidrs.NewController(
		ctx,
		informerFactory.Networking().V1().ServiceCIDRs(),
		informerFactory.Networking().V1().IPAddresses(),
		client,
	).Run(ctx, 5)
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	// 添加一个新的 service CIDR，以便创建新的 IP。
	cidr := makeServiceCIDR("test2", "10.168.0.0/24", "")
	if _, err := client.NetworkingV1().ServiceCIDRs().Create(context.Background(), cidr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	// 等待 ServiceCIDR 就绪
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(context.TODO(), cidr.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("waiting for service cidr ready condition %v", err)
	}

	// 没有名称的 service 会一直无法分配 IP address，
	// 因为它试图用无效的引用创建 IPAddress 对象。
	// 最终请求超时，在此期间会导致 CPU 使用率升高。
	svc := &v1.Service{
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{Port: 80},
			},
		},
	}

	// 分配过程中请求不应超时，而应返回无效错误。
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 由于该 service 缺少名称，操作将失败
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(ctx, svc, metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("unexpected error reason: %v", err)
	}

}
