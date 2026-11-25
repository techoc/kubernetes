/*
Copyright 2025 The Kubernetes Authors.

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
	"testing"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/test/integration/framework"
)

// populateEtcdForRepairTest 向 etcd 写入 namespace、service 和 ServiceCIDR，
// 模拟从 v1.32 升级到 v1.33 时需要修复的存量集群。
func populateEtcdForRepairTest(t *testing.T, etcdOptions *storagebackend.Config, apiServerOptions *kubeapiservertesting.TestServerInstanceOptions, numNamespaces int) string {
	t.Logf("Populating etcd with %d namespaces and services (simulating v1.32 cluster)", numNamespaces)

	// 启动临时 server 只是为了拿到 etcd client。
	tempServer := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/24",
			"--advertise-address=10.1.1.1",
		},
		etcdOptions)

	// 直接向 etcd 写入测试 namespace。
	namespace := "test-repair-race"
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              namespace,
			CreationTimestamp: metav1.Now(),
			UID:               types.UID("test-namespace-uid"),
		},
	}
	nsJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), ns)
	if err != nil {
		t.Fatalf("Failed to encode namespace: %v", err)
	}
	nsKey := "/" + etcdOptions.Prefix + "/namespaces/" + namespace
	if _, err := tempServer.EtcdClient.Put(context.Background(), nsKey, string(nsJSON)); err != nil {
		t.Fatalf("Failed to store namespace in etcd: %v", err)
	}
	t.Logf("Created namespace %s in etcd", namespace)

	// 直接写入大量 namespace 模拟大集群，使 namespace informer 启动时同步耗时显著增加。
	for i := range numNamespaces {
		ns := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("bulk-ns-%d", i),
				CreationTimestamp: metav1.Now(),
				UID:               types.UID(fmt.Sprintf("bulk-ns-uid-%d", i)),
			},
		}
		nsJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), ns)
		if err != nil {
			t.Fatalf("Failed to encode namespace: %v", err)
		}
		nsKey := "/" + etcdOptions.Prefix + "/namespaces/" + ns.Name
		if _, err := tempServer.EtcdClient.Put(context.Background(), nsKey, string(nsJSON)); err != nil {
			t.Fatalf("Failed to store namespace in etcd: %v", err)
		}
		if numNamespaces > 1000 && i%1000 == 0 && i > 0 {
			t.Logf("Created %d/%d bulk namespaces in etcd", i, numNamespaces)
		}
	}
	t.Logf("Created %d additional namespaces in etcd to simulate large cluster", numNamespaces)

	// 直接写入 service 模拟 v1.32 存量数据，这些 service 需要 repair controller 创建对应的 IPAddress。
	numServices := 5
	for i := 1; i <= numServices; i++ {
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("test-service-%d", i),
				Namespace:         namespace,
				CreationTimestamp: metav1.Now(),
				UID:               types.UID(fmt.Sprintf("test-svc-uid-%d", i)),
			},
			Spec: v1.ServiceSpec{
				Type:      v1.ServiceTypeClusterIP,
				ClusterIP: fmt.Sprintf("10.0.0.%d", i+10),
				Ports: []v1.ServicePort{
					{
						Name:     "http",
						Port:     80,
						Protocol: v1.ProtocolTCP,
					},
				},
			},
		}
		svcJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), svc)
		if err != nil {
			t.Fatalf("Failed to encode service: %v", err)
		}
		svcKey := "/" + etcdOptions.Prefix + "/services/specs/" + namespace + "/" + svc.Name
		if _, err := tempServer.EtcdClient.Put(context.Background(), svcKey, string(svcJSON)); err != nil {
			t.Fatalf("Failed to store service in etcd: %v", err)
		}
		t.Logf("Created service %s with ClusterIP %s in etcd", svc.Name, svc.Spec.ClusterIP)
	}

	// repair controller 运行依赖 ServiceCIDR，需先写入。
	serviceCIDR := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kubernetes",
		},
		Spec: networkingv1.ServiceCIDRSpec{
			CIDRs: []string{"10.0.0.0/24"},
		},
	}
	serviceCIDRJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(networkingv1.SchemeGroupVersion), serviceCIDR)
	if err != nil {
		t.Fatalf("Failed to encode ServiceCIDR: %v", err)
	}
	serviceCIDRKey := "/" + etcdOptions.Prefix + "/servicecidrs/" + serviceCIDR.Name
	if _, err := tempServer.EtcdClient.Put(context.Background(), serviceCIDRKey, string(serviceCIDRJSON)); err != nil {
		t.Fatalf("Failed to store ServiceCIDR in etcd: %v", err)
	}
	t.Logf("Created ServiceCIDR in etcd")

	// 关闭临时 server。
	tempServer.TearDownFn()
	t.Logf("Etcd population complete: %d namespaces, %d services, and ServiceCIDR", numNamespaces+1, numServices)

	return namespace
}

// TestServiceIPRepairRaceCondition 用真实的 namespace 规模复现竞态问题。
//
// 该测试复现 https://github.com/kubernetes/kubernetes/issues/136288 中的竞态，
// 使用真实数量的 namespace（330000），不引入人为延迟。
//
// 模拟的完整场景：
//  1. 集群从 v1.32 升级到 v1.33，开启 MultiCIDRServiceAllocator
//  2. etcd 中存在需要创建 IPAddress 的存量 Service（修复场景）
//  3. etcd 中有大量 namespace（330000），namespace informer 自然需要较长时间同步
//  4. apiserver 启动时，PostStartHook 中的 repair controller 尝试创建 IPAddress
//  5. 由于 namespace informer 尚未同步，NamespaceLifecycle 等 admission 插件以 "not yet ready" 拒绝请求
//  6. PostStartHook 失败，apiserver 无法就绪
//
// 测试耗时很长（填充 etcd 约 30 分钟），但能基于真实负载复现问题。
func TestServiceIPRepairRaceCondition(t *testing.T) {
	t.Skip("reproducer for https://issues.k8s.io/136288")

	// 准备 etcd。
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()

	// 阶段 1：直接写入 etcd，模拟 v1.32 存量集群。
	// 绕过 apiserver 直接写 etcd 可加快准备速度；
	// 使用 330000 个 namespace 以真实负载（无人为延迟）复现问题。
	t.Logf("Phase 1: Populating etcd with namespaces and services (simulating v1.32 cluster)")
	populateEtcdForRepairTest(t, etcdOptions, apiServerOptions, 330000)

	// 阶段 2：开启 MultiCIDRServiceAllocator 重启 apiserver，模拟 v1.32 -> v1.33 升级。
	t.Logf("Phase 2: Starting apiserver with MultiCIDRServiceAllocator enabled (simulating v1.33 upgrade)")

	// repair controller 在 PostStartHook 中创建 IPAddress；namespace 过多导致 informer 同步缓慢，
	// 依赖该 informer 的 admission 插件会以 "not yet ready" 拒绝请求。
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--runtime-config=networking.k8s.io/v1beta1=true",
			"--service-cluster-ip-range=10.0.0.0/24",
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
			// 开启详细日志，便于查看 repair controller 的行为。
			"-v=5",
		},
		etcdOptions)
	defer testServer.TearDownFn()
}
