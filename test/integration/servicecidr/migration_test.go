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
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/servicecidrs"
	"k8s.io/kubernetes/pkg/controlplane/controller/defaultservicecidr"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestMigrateServiceCIDR 验证迁移集群默认 ServiceCIDR（含已有 kubernetes.default Service）所需的步骤：
// 1. 以 --service-cluster-ip-range 192.168.0.0/29 启动 apiserver
// 2. 创建 Service 占用该 CIDR 中的部分地址
// 3. 新建一个临时 ServiceCIDR 10.168.0.0/24 作为迁移目标
// 4. 删除默认 ServiceCIDR，使分配器不再使用它（因 finalizer 且仍有 IP，它会处于 pending 状态）
// 5. 重建 Service，分配器应从临时 ServiceCIDR 中分配
// 6. 用新的 ServiceCIDR 参数启动新 apiserver 并关闭旧实例
// 7. 删除 kubernetes.default Service，新 apiserver 会在新 ServiceCIDR 中重建它
func TestMigrateServiceCIDR(t *testing.T) {
	tCtx := ktesting.Init(t)

	cidr1 := "192.168.0.0/29"
	cidr2 := "10.168.0.0/24"

	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s1 := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=" + cidr1,
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)

	client1, err := clientset.NewForConfig(s1.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client1, "test-migrate-service-cidr", t)

	resyncPeriod := 12 * time.Hour
	informers1 := informers.NewSharedInformerFactory(client1, resyncPeriod)
	// 启动 ServiceCIDR controller
	go servicecidrs.NewController(
		tCtx,
		informers1.Networking().V1().ServiceCIDRs(),
		informers1.Networking().V1().IPAddresses(),
		client1,
	).Run(tCtx, 5)
	informers1.Start(tCtx.Done())
	informers1.WaitForCacheSync(tCtx.Done())

	// 默认 ServiceCIDR 应有 finalizer，且 Ready condition 为 true
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		cidr, err := client1.NetworkingV1().ServiceCIDRs().Get(context.TODO(), defaultservicecidr.DefaultServiceCIDRName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if len(cidr.Finalizers) == 0 {
			return false, nil
		}

		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("waiting for default service cidr ready condition set to false: %v", err)
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

	// 创建 2 个 Service，CIDR 中还剩 3 个空闲地址
	for i := range 2 {
		if _, err := client1.CoreV1().Services(ns.Name).Create(context.TODO(), svc(i), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// 新建一个 ServiceCIDR，作为 apiserver 迁移目标
	if _, err := client1.NetworkingV1().ServiceCIDRs().Create(context.Background(), makeServiceCIDR("migration-cidr", cidr2, ""), metav1.CreateOptions{}); err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}

	// 等待 ServiceCIDR 就绪
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		cidr, err := client1.NetworkingV1().ServiceCIDRs().Get(context.TODO(), "migration-cidr", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("waiting for default service cidr ready condition set to false: %v", err)
	}

	// 删除默认 ServiceCIDR，使其不再用于 IP 分配
	if err := client1.NetworkingV1().ServiceCIDRs().Delete(context.Background(), defaultservicecidr.DefaultServiceCIDRName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}

	// 默认 ServiceCIDR 应处于 pending 删除状态，且 Ready condition 为 false
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		cidr, err := client1.NetworkingV1().ServiceCIDRs().Get(context.TODO(), defaultservicecidr.DefaultServiceCIDRName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		for _, condition := range cidr.Status.Conditions {
			if condition.Type == networkingv1.ServiceCIDRConditionReady {
				return condition.Status == metav1.ConditionFalse, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("waiting for default service cidr ready condition set to false: %v", err)
	}

	// 迁移 Service：删除原有 Service，不指定 ClusterIP 重建，让分配器重新分配
	services, err := client1.CoreV1().Services("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range services.Items {
		// 跳过 default Service：它由 apiserver 管理，我们希望由使用新 ServiceCIDR 的新 apiserver 接管
		if svc.Name == "kubernetes" {
			continue
		}

		if err := client1.CoreV1().Services(svc.Namespace).Delete(context.Background(), svc.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		t.Logf("Deleted Service with IP %s", svc.Spec.ClusterIP)

		// 清除必要字段以便重建 Service
		svc.ResourceVersion = ""
		svc.Spec.ClusterIP = ""
		svc.Spec.ClusterIPs = nil
		svc.Status = v1.ServiceStatus{}
		svc, err := client1.CoreV1().Services(svc.Namespace).Create(context.Background(), &svc, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("got unexpected error: %v", err)
		}
		t.Logf("Created Service with IP %s", svc.Spec.ClusterIP)
		if !cidrContainsIP(cidr2, svc.Spec.ClusterIP) {
			t.Fatalf("Service expected to have an ip in range 10.168.0.0/24, got %s", svc.Spec.ClusterIP)
		}
	}

	// 用新地址范围启动第二个 apiserver，并运行新的 ServiceCIDR controller
	s2 := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=" + cidr2,
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s2.TearDownFn()

	client2, err := clientset.NewForConfig(s2.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer framework.DeleteNamespaceOrDie(client2, ns, t)

	// 将 controller 切换到新 apiserver
	tCtx.Cancel("tearing down ServiceCIDR controller 1")
	s1.TearDownFn()

	// 启动 ServiceCIDR controller
	tCtx2 := ktesting.Init(t)
	defer tCtx2.Cancel("tearing down ServiceCIDR controller 2")
	informers2 := informers.NewSharedInformerFactory(client2, resyncPeriod)
	go servicecidrs.NewController(
		tCtx2,
		informers2.Networking().V1().ServiceCIDRs(),
		informers2.Networking().V1().IPAddresses(),
		client2,
	).Run(tCtx2, 5)
	informers2.Start(tCtx2.Done())
	informers2.WaitForCacheSync(tCtx.Done())

	// 删除 kubernetes.default Service，以便旧的 DefaultServiceCIDR 被删除、新 apiserver 接管
	if err := client2.CoreV1().Services(metav1.NamespaceDefault).Delete(context.Background(), "kubernetes", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	// 默认 ServiceCIDR 应已更新为新的那个
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		cidr, err := client2.NetworkingV1().ServiceCIDRs().Get(context.TODO(), defaultservicecidr.DefaultServiceCIDRName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}

		if len(cidr.Spec.CIDRs) == 0 {
			t.Logf("No CIDR available")
			return false, nil
		}

		if cidr.Spec.CIDRs[0] != cidr2 {
			t.Logf("CIDR expected %s got %s", cidr2, cidr.Spec.CIDRs[0])
			return false, nil
		}

		if len(cidr.Finalizers) == 0 {
			t.Logf("Expected finalizer to be set")
			return false, nil
		}

		for _, condition := range cidr.Status.Conditions {
			if condition.Type == networkingv1.ServiceCIDRConditionReady {
				t.Logf("Expected Condition %s to be %s", condition.Status, metav1.ConditionTrue)
				return condition.Status == metav1.ConditionTrue, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("waiting for default service cidr ready condition set to true: %v", err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		svc, err := client2.CoreV1().Services(metav1.NamespaceDefault).Get(context.TODO(), "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}

		if svc.Spec.ClusterIP != "10.168.0.1" {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("waiting for default service kubernetes.default to be migrated: %v", err)
	}

	// 默认 ServiceCIDR 已覆盖该范围，临时 ServiceCIDR 现在可以删除
	if err := client2.NetworkingV1().ServiceCIDRs().Delete(context.Background(), "migration-cidr", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}

	// 等待 ServiceCIDR 被删除
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		_, err := client2.NetworkingV1().ServiceCIDRs().Get(context.TODO(), "migration-cidr", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("waiting for the migration service cidr to be deleted: %v", err)
	}

}

// TestServiceCIDRMigrationScenarios 覆盖 ServiceCIDR 的各种迁移路径。
func TestServiceCIDRMigrationScenarios(t *testing.T) {
	ipv4CIDRSmall := "10.0.0.0/29" // 6 个 IP
	ipv4CIDRBig := "10.1.0.0/16"
	ipv6CIDRSmall := "2001:db8:1::/125" // 6 个 IP
	ipv6CIDRBig := "2001:db8:2::/112"

	testCases := []struct {
		name                          string
		initialCIDRs                  []string
		migratedCIDRs                 []string
		preMigrationSvcName           string
		postMigrationSvcName          string
		expectedPostMigrationSvcError bool // 切换主 IP 族但保留旧分配器
		expectInconsistentState       bool // flags 中配置的新 ServiceCIDR 未生效
	}{
		// --- 无变化 ---
		{
			name:                 "IPv4 -> IPv4 (no change)",
			initialCIDRs:         []string{ipv4CIDRSmall},
			migratedCIDRs:        []string{ipv4CIDRSmall},
			preMigrationSvcName:  "svc-pre-v4-v4",
			postMigrationSvcName: "svc-post-v4-v4",
		},
		{
			name:                 "IPv6 -> IPv6 (no change)",
			initialCIDRs:         []string{ipv6CIDRSmall},
			migratedCIDRs:        []string{ipv6CIDRSmall},
			preMigrationSvcName:  "svc-pre-v6-v6",
			postMigrationSvcName: "svc-post-v6-v6",
		},
		{
			name:                 "IPv4,IPv6 -> IPv4,IPv6 (no change)",
			initialCIDRs:         []string{ipv4CIDRSmall, ipv6CIDRSmall},
			migratedCIDRs:        []string{ipv4CIDRSmall, ipv6CIDRSmall},
			preMigrationSvcName:  "svc-pre-v4v6-v4v6",
			postMigrationSvcName: "svc-post-v4v6-v4v6",
		},
		{
			name:                 "IPv6,IPv4 -> IPv6,IPv4 (no change)",
			initialCIDRs:         []string{ipv6CIDRSmall, ipv4CIDRSmall},
			migratedCIDRs:        []string{ipv6CIDRSmall, ipv4CIDRSmall},
			preMigrationSvcName:  "svc-pre-v6v4-v6v4",
			postMigrationSvcName: "svc-post-v6v4-v6v4",
		},
		// --- 有效升级 ---
		{
			name:                 "IPv4 -> IPv4,IPv6 (upgrade)",
			initialCIDRs:         []string{ipv4CIDRSmall},
			migratedCIDRs:        []string{ipv4CIDRSmall, ipv6CIDRBig},
			preMigrationSvcName:  "svc-pre-v4-v4v6",
			postMigrationSvcName: "svc-post-v4-v4v6",
		},
		{
			name:                 "IPv6 -> IPv6,IPv4 (upgrade)",
			initialCIDRs:         []string{ipv6CIDRSmall},
			migratedCIDRs:        []string{ipv6CIDRSmall, ipv4CIDRBig},
			preMigrationSvcName:  "svc-pre-v6-v6v4",
			postMigrationSvcName: "svc-post-v6-v6v4",
		},
		// --- 非法迁移（需人工介入） ---
		{
			name:                          "IPv4,IPv6 -> IPv6,IPv4 (change primary)",
			initialCIDRs:                  []string{ipv4CIDRSmall, ipv6CIDRSmall},
			migratedCIDRs:                 []string{ipv6CIDRSmall, ipv4CIDRSmall},
			preMigrationSvcName:           "svc-pre-v4v6-v6v4",
			postMigrationSvcName:          "svc-post-v4v6-v6v4",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                          "IPv6,IPv4 -> IPv4,IPv6 (change primary)",
			initialCIDRs:                  []string{ipv6CIDRSmall, ipv4CIDRSmall},
			migratedCIDRs:                 []string{ipv4CIDRSmall, ipv6CIDRSmall},
			preMigrationSvcName:           "svc-pre-v6v4-v4v6",
			postMigrationSvcName:          "svc-post-v6v4-v4v6",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                    "IPv4,IPv6 -> IPv4 (downgrade)",
			initialCIDRs:            []string{ipv4CIDRSmall, ipv6CIDRSmall},
			migratedCIDRs:           []string{ipv4CIDRSmall},
			preMigrationSvcName:     "svc-pre-v4v6-v4",
			postMigrationSvcName:    "svc-post-v4v6-v4",
			expectInconsistentState: true,
		},
		{
			name:                          "IPv4,IPv6 -> IPv6 (downgrade)",
			initialCIDRs:                  []string{ipv4CIDRSmall, ipv6CIDRSmall},
			migratedCIDRs:                 []string{ipv6CIDRSmall},
			preMigrationSvcName:           "svc-pre-v4v6-v6",
			postMigrationSvcName:          "svc-post-v4v6-v6",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                          "IPv4 -> IPv6 (change family)",
			initialCIDRs:                  []string{ipv4CIDRSmall},
			migratedCIDRs:                 []string{ipv6CIDRSmall},
			preMigrationSvcName:           "svc-pre-v4-v6",
			postMigrationSvcName:          "svc-post-v4-v6",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                          "IPv6 -> IPv4 (change family)",
			initialCIDRs:                  []string{ipv6CIDRSmall},
			migratedCIDRs:                 []string{ipv4CIDRSmall},
			preMigrationSvcName:           "svc-pre-v6-v4",
			postMigrationSvcName:          "svc-post-v6-v4",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                          "IPv4 -> IPv6,IPv4 (upgrade, change primary)",
			initialCIDRs:                  []string{ipv4CIDRSmall},
			migratedCIDRs:                 []string{ipv6CIDRBig, ipv4CIDRSmall}, // 升级时切换主 IP 族
			preMigrationSvcName:           "svc-pre-v4-v6v4",
			postMigrationSvcName:          "svc-post-v4-v6v4",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
		{
			name:                          "IPv6 -> IPv4,IPv6 (upgrade, change primary)",
			initialCIDRs:                  []string{ipv6CIDRSmall},
			migratedCIDRs:                 []string{ipv4CIDRBig, ipv6CIDRSmall}, // 升级时切换主 IP 族
			preMigrationSvcName:           "svc-pre-v6-v4v6",
			postMigrationSvcName:          "svc-post-v6-v4v6",
			expectedPostMigrationSvcError: true,
			expectInconsistentState:       true,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			etcdOptions := framework.SharedEtcd()
			apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
			resyncPeriod := 12 * time.Hour

			// --- 初始配置 ---
			initialFlags := []string{
				"--service-cluster-ip-range=" + strings.Join(tc.initialCIDRs, ","),
				"--advertise-address=" + strings.Split(tc.initialCIDRs[0], "/")[0], // advertise 地址必须与集群主 IP 族一致
				"--disable-admission-plugins=ServiceAccount",
			}
			t.Logf("Starting API server with CIDRs: %v", tc.initialCIDRs)
			s1 := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions, initialFlags, etcdOptions)
			client1, err := clientset.NewForConfig(s1.ClientConfig)
			if err != nil {
				s1.TearDownFn()
				t.Fatalf("Failed to create client for initial server: %v", err)
			}

			ns := framework.CreateNamespaceOrDie(client1, fmt.Sprintf("migrate-%d", i), t)

			informers1 := informers.NewSharedInformerFactory(client1, resyncPeriod)
			controllerCtx1, cancelController1 := context.WithCancel(tCtx)
			go servicecidrs.NewController(
				controllerCtx1,
				informers1.Networking().V1().ServiceCIDRs(),
				informers1.Networking().V1().IPAddresses(),
				client1,
			).Run(controllerCtx1, 5)
			informers1.Start(controllerCtx1.Done())
			informers1.WaitForCacheSync(controllerCtx1.Done())

			// 等待默认 ServiceCIDR 就绪
			if err := waitForServiceCIDRState(tCtx, client1, tc.initialCIDRs, true); err != nil {
				s1.TearDownFn()
				cancelController1()
				t.Fatalf("Initial default ServiceCIDR did not become ready: %v", err)
			}

			// 创建迁移前的 Service
			preSvc, err := client1.CoreV1().Services(ns.Name).Create(tCtx, makeService(tc.preMigrationSvcName), metav1.CreateOptions{})
			if err != nil {
				s1.TearDownFn()
				cancelController1()
				t.Fatalf("Failed to create pre-migration service: %v", err)
			}
			t.Logf("Pre-migration service %s created with ClusterIPs: %v", preSvc.Name, preSvc.Spec.ClusterIPs)

			// 基本校验迁移前 Service 的 IP
			if len(preSvc.Spec.ClusterIPs) == 0 {
				s1.TearDownFn()
				cancelController1()
				t.Fatalf("Pre-migration service %s has no ClusterIPs", preSvc.Name)
			}
			if !cidrContainsIP(tc.initialCIDRs[0], preSvc.Spec.ClusterIPs[0]) {
				s1.TearDownFn()
				cancelController1()
				t.Fatalf("Pre-migration service %s primary IP %s not in expected range %s", preSvc.Name, preSvc.Spec.ClusterIPs[0], tc.initialCIDRs[0])
			}

			// --- 迁移 ---
			t.Logf("Shutting down initial API server and controller")
			cancelController1()
			s1.TearDownFn()

			t.Logf("Starting migrated API server with CIDRs: %v", tc.migratedCIDRs)
			migratedFlags := []string{
				"--service-cluster-ip-range=" + strings.Join(tc.migratedCIDRs, ","),
				"--advertise-address=" + strings.Split(tc.migratedCIDRs[0], "/")[0], // advertise 地址必须与集群配置的主 IP 族一致
				"--disable-admission-plugins=ServiceAccount",
			}
			s2 := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions, migratedFlags, etcdOptions)
			defer s2.TearDownFn() // 即使测试失败也确保清理

			client2, err := clientset.NewForConfig(s2.ClientConfig)
			if err != nil {
				t.Fatalf("Failed to create client for migrated server: %v", err)
			}
			defer framework.DeleteNamespaceOrDie(client2, ns, t)

			informers2 := informers.NewSharedInformerFactory(client2, resyncPeriod)
			controllerCtx2, cancelController2 := context.WithCancel(tCtx)
			defer cancelController2() // 确保 controller context 被取消
			go servicecidrs.NewController(
				controllerCtx2,
				informers2.Networking().V1().ServiceCIDRs(),
				informers2.Networking().V1().IPAddresses(),
				client2,
			).Run(controllerCtx2, 5)
			informers2.Start(controllerCtx2.Done())
			informers2.WaitForCacheSync(controllerCtx2.Done())

			// 等待默认 ServiceCIDR 反映迁移后的状态；若处于不一致状态，预期保留原有 CIDR。
			expectedCIDRs := tc.migratedCIDRs
			if tc.expectInconsistentState {
				expectedCIDRs = tc.initialCIDRs
			}
			if err := waitForServiceCIDRState(tCtx, client2, expectedCIDRs, true); err != nil {
				t.Fatalf("Migrated default ServiceCIDR did not reach expected state : %v", err)
			}

			// --- 迁移后校验 ---

			// 校验迁移前的 Service 仍存在且 IP 未变
			preSvcMigrated, err := client2.CoreV1().Services(ns.Name).Get(tCtx, tc.preMigrationSvcName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Failed to get pre-migration service after migration: %v", err)
			}
			if !reflect.DeepEqual(preSvcMigrated.Spec.ClusterIPs, preSvc.Spec.ClusterIPs) {
				t.Errorf("Pre-migration service %s ClusterIPs changed after migration. Before: %v, After: %v",
					preSvcMigrated.Name, preSvc.Spec.ClusterIPs, preSvcMigrated.Spec.ClusterIPs)
			}
			// 创建迁移后的 Service
			postSvc, err := client2.CoreV1().Services(ns.Name).Create(tCtx, makeService(tc.postMigrationSvcName), metav1.CreateOptions{})
			if err != nil && !tc.expectedPostMigrationSvcError {
				t.Fatalf("Failed to create post-migration service: %v", err)
			} else if err == nil && tc.expectedPostMigrationSvcError {
				return
			}

			t.Logf("Post-migration service %s created with ClusterIPs: %v, Families: %v", postSvc.Name, postSvc.Spec.ClusterIPs, postSvc.Spec.IPFamilies)
			// 检查 IP 是否落在迁移后的 CIDR 范围内
			if len(postSvc.Spec.ClusterIPs) > 0 && !cidrContainsIP(expectedCIDRs[0], postSvc.Spec.ClusterIPs[0]) {
				t.Errorf("Post-migration service %s primary IP %s not in expected range %s", postSvc.Name, postSvc.Spec.ClusterIPs[0], expectedCIDRs[0])
			}
		})
	}
}

// waitForServiceCIDRState 等待指定的 ServiceCIDR 出现、CIDR 列表符合预期且 Ready condition 达到指定状态。
func waitForServiceCIDRState(ctx context.Context, client clientset.Interface, expectedCIDRs []string, expectReady bool) error {
	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	return wait.PollUntilContextCancel(pollCtx, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, defaultservicecidr.DefaultServiceCIDRName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, fmt.Errorf("default ServiceCIDR must exist")
			}
			return false, nil
		}

		// 检查 CIDR 是否匹配
		if !reflect.DeepEqual(cidr.Spec.CIDRs, expectedCIDRs) {
			klog.Infof("Waiting for ServiceCIDR %s CIDRs to match %v, current: %v", defaultservicecidr.DefaultServiceCIDRName, expectedCIDRs, cidr.Spec.CIDRs)
			return false, nil
		}

		// 检查 Ready condition
		isReady := false
		foundReadyCondition := false
		for _, condition := range cidr.Status.Conditions {
			if condition.Type == networkingv1.ServiceCIDRConditionReady {
				foundReadyCondition = true
				isReady = condition.Status == metav1.ConditionTrue
				break
			}
		}

		if !foundReadyCondition && expectReady {
			klog.Infof("Waiting for ServiceCIDR %s Ready condition to be set...", defaultservicecidr.DefaultServiceCIDRName)
			return false, nil // 尚未出现 Ready condition
		}

		if isReady != expectReady {
			klog.Infof("Waiting for ServiceCIDR %s Ready condition to be %v, current: %v", defaultservicecidr.DefaultServiceCIDRName, expectReady, isReady)
			return false, nil // Ready condition 与预期不符
		}

		klog.Infof("ServiceCIDR %s reached desired state (Ready: %v, CIDRs: %v)", defaultservicecidr.DefaultServiceCIDRName, expectReady, expectedCIDRs)
		return true, nil // 所有条件均满足
	})
}
