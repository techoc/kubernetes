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
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/servicecidrs"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

func TestServiceAllocNewServiceCIDR(t *testing.T) {
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/29",
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
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

	// /29 = 6 个 service，kubernetes.default 占用第一个地址
	// 再创建 5 个 service 以占满所有 IP
	for i := range 5 {
		if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(context.Background(), makeService(fmt.Sprintf("service-%d", i)), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// 再创建一个 service。由于集群 IP 已耗尽，该操作会失败
	if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(context.Background(), makeService("fail"), metav1.CreateOptions{}); err != nil {
		if !strings.Contains(err.Error(), "range is full") {
			t.Fatalf("unexpected error text: %v", err)
		}
	} else {
		svcs, err := client.CoreV1().Services(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error getting the services: %v", err)
		}
		allIPs := []string{}
		for _, s := range svcs.Items {
			allIPs = append(allIPs, s.Spec.ClusterIP)
		}
		t.Fatalf("unexpected creation success. The following IPs exist: %#v. It should only be possible to allocate 6 IP addresses in this cluster.\n\n%#v", allIPs, svcs)
	}

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
		t.Fatalf("waiting for default service cidr ready condition set to false: %v", err)
	}
	// 这次创建更多 Service 应该会成功
	for i := 10; i < 150; i++ {
		if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(context.Background(), makeService(fmt.Sprintf("service-%d", i)), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

// 当没有孤立 IP，或现有 IP 包含在另一个未被删除的 ServiceCIDR 中时，
// 该 ServiceCIDR 可以被删除。
// 测试以 "192.168.0.0/29" 范围启动 apiserver
// 创建 Service 以填满该范围
// 新建一个与 apiserver 中定义的范围相同的 ServiceCIDR cidr1
// 删除 cidr1 对象会成功，因为其范围被 apiserver flag 创建的默认 ServiceCIDR 覆盖
// 新建一个范围与 cidr1 不同的 cidr2
// 新建一个 service，由于此时 "192.168.0.0/29" 已满，它会从该范围获取 IPAddress
// 新建一个包含 cidr2 的 cidr3
// 删除 cidr2，因为其范围被 cidr3 覆盖
// 尝试删除 cidr3 会被阻止，因为还存在一个 IPAddress
// 删除那个阻碍删除的 IPAddress 所属的 Service
// 此时 cidr3 必须已不存在
func TestServiceCIDRDeletion(t *testing.T) {
	cidr1 := "192.168.0.0/29" // 与默认值相同
	cidr2 := "10.0.0.0/24"    // 新范围
	cidr3 := "10.0.0.0/16"    // 包含 cidr2

	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=" + cidr1,
			"--advertise-address=172.16.1.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-cidr-deletion", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

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

	// /29 = 6 个 service，kubernetes.default 占用第一个地址
	// 再创建 5 个 service 以占满所有 IP
	for i := range 5 {
		if _, err := client.CoreV1().Services(ns.Name).Create(context.Background(), makeService(fmt.Sprintf("service-%d", i)), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// 新建一个与默认 ServiceCIDR 范围重叠的 ServiceCIDR
	_, err = client.NetworkingV1().ServiceCIDRs().Create(ctx, makeServiceCIDR("cidr1", cidr1, ""), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 等待其就绪。
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr1", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("cidr1 is not ready")
	}
	// 尽管该 ServiceCIDR 包含 IP 地址，我们也应该能删除它，
	// 因为它与默认的 ServiceCIDR 范围重叠
	err = client.NetworkingV1().ServiceCIDRs().Delete(ctx, "cidr1", metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		_, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr1", metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("cidr1 has not been deleted")
	}

	// 添加一个使用新范围的 ServiceCIDR
	_, err = client.NetworkingV1().ServiceCIDRs().Create(ctx, makeServiceCIDR("cidr2", cidr2, ""), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 等待分配器处理新的 ServiceCIDR
	// 等待其就绪。
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr2", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("cidr2 is not ready")
	}
	// 新建一个 Service，使其从新范围中获取 IP 地址
	svc, err := client.CoreV1().Services(ns.Name).Create(context.Background(), makeService("new-cidr-service"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if !cidrContainsIP(cidr2, svc.Spec.ClusterIP) {
		t.Fatalf("Service %s expected to have an IP on range %s, got %s", svc.Name, cidr2, svc.Spec.ClusterIP)
	}

	// 添加一个新的 ServiceCIDR，其范围与现有 range 重叠
	_, err = client.NetworkingV1().ServiceCIDRs().Create(ctx, makeServiceCIDR("cidr3", cidr3, ""), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 等待其就绪。
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr3", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return isServiceCIDRReady(cidr), nil
	}); err != nil {
		t.Fatalf("cidr3 is not ready")
	}
	// 尽管 ServiceCIDR2 包含 IP 地址，我们也应该能删除它，
	// 因为它被包含在 ServiceCIDR3 中
	err = client.NetworkingV1().ServiceCIDRs().Delete(ctx, "cidr2", metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		_, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr2", metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("cidr2 has not been deleted")
	}

	// 在 IPAddress 被移除之前，无法删除 serviceCIDR3
	err = client.NetworkingV1().ServiceCIDRs().Delete(ctx, "cidr3", metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 等待其变为未就绪状态。
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		cidr, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr3", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, condition := range cidr.Status.Conditions {
			if condition.Type == networkingv1.ServiceCIDRConditionReady {
				return condition.Status == metav1.ConditionStatus(metav1.ConditionFalse), nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("cidr3 is ready")
	}

	// 删除阻碍删除操作的 service
	if err := client.CoreV1().Services(ns.Name).Delete(context.Background(), "new-cidr-service", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	// cidr3 必须已不存在
	if err := wait.PollUntilContextTimeout(context.Background(), 250*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		_, err := client.NetworkingV1().ServiceCIDRs().Get(ctx, "cidr3", metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("cidr3 has not been deleted")
	}
}

func makeServiceCIDR(name, primary, secondary string) *networkingv1.ServiceCIDR {
	serviceCIDR := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: networkingv1.ServiceCIDRSpec{},
	}
	serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, primary)
	if secondary != "" {
		serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, secondary)
	}
	return serviceCIDR
}

func makeService(name string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{Port: 80},
			},
		},
	}
}

// 当 ServiceCIDRConditionReady 为 true 时返回 true
func isServiceCIDRReady(serviceCIDR *networkingv1.ServiceCIDR) bool {
	if serviceCIDR == nil {
		return false
	}

	for _, condition := range serviceCIDR.Status.Conditions {
		if condition.Type == networkingv1.ServiceCIDRConditionReady {
			return condition.Status == metav1.ConditionStatus(metav1.ConditionTrue)
		}
	}

	return false
}

func cidrContainsIP(cidr, ip string) bool {
	prefix := netip.MustParsePrefix(cidr)
	address := netip.MustParseAddr(ip)
	return prefix.Contains(address)
}

// TestValidationAdmissionPolicyServiceCIDR 测试 ValidatingAdmissionPolicy
// 可以阻止 apiserver 以外的用户更新默认 ServiceCIDR，
// 并且 apiserver 在启动时可以更新它。
func TestValidationAdmissionPolicyServiceCIDR(t *testing.T) {
	// 该测试会重启 apiserver，因此不能并行运行。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdOptions := framework.SharedEtcd()
	// 1. 以单栈方式启动一个 apiserver。
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--runtime-config=networking.k8s.io/v1=true",
			"--service-cluster-ip-range=192.168.0.0/24",
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
			"--enable-admission-plugins=ValidatingAdmissionPolicy",
		},
		etcdOptions)

	clientset, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		s.TearDownFn()
		t.Fatalf("Failed to create clientset: %v", err)
	}

	// 伪造用户
	newCfg := *s.ClientConfig
	newCfg.Impersonate = rest.ImpersonationConfig{
		UserName: "fake-admin",
		Groups:   []string{"system:authenticated"},
	}
	kc, err := kubernetes.NewForConfig(&newCfg)
	if err != nil {
		t.Fatalf("Unexpected error creating kubernetes client impersonating %q", newCfg.Impersonate.UserName)
	}

	// 2. 安装一个 validationadmissionpolicy，只允许 apiserver 用户
	//    将默认 servicecidr 更新为双栈
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "deny-non-apiserver-servicecidr-updates"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: ptr.To(admissionregistrationv1.Fail),
			MatchConstraints: &admissionregistrationv1.MatchResources{
				ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
					{
						RuleWithOperations: admissionregistrationv1.RuleWithOperations{
							Operations: []admissionregistrationv1.OperationType{"CREATE", "UPDATE"},
							Rule:       admissionregistrationv1.Rule{APIGroups: []string{"networking.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"servicecidrs"}},
						},
					},
				},
			},
			Validations: []admissionregistrationv1.Validation{{
				// 只允许 apiserver 创建或更新默认的 kubernetes ServiceCIDR
				Expression: "request.userInfo.username == 'system:apiserver' && 'system:masters' in request.userInfo.groups",
				Message:    "only apiserver can update and create servicecidrs",
			}, {
				Expression: "object.metadata.name == 'kubernetes'",
				Message:    "only allow changes on the default servicecidr",
			}},
		},
	}
	policy, err = clientset.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{})
	if err != nil {
		s.TearDownFn()
		t.Fatalf("Failed to create policy: %v", err)
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "deny-non-apiserver-servicecidr-updates-binding"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{
				admissionregistrationv1.Deny,
			},
			MatchResources: &admissionregistrationv1.MatchResources{
				NamespaceSelector: &metav1.LabelSelector{}, // 匹配集群范围的资源
			},
		},
	}
	_, err = clientset.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		s.TearDownFn()
		t.Fatalf("Failed to create binding: %v", err)
	}

	// 等待 policy 和 binding 生效。
	time.Sleep(2 * time.Second)

	// 3. 尝试从测试中把 defaultservicecidr 更新为双栈，并检查其失败
	defaultCIDR, err := clientset.NetworkingV1().ServiceCIDRs().Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		s.TearDownFn()
		t.Fatalf("Failed to get default ServiceCIDR: %v", err)
	}

	defaultCIDR.Spec.CIDRs = append(defaultCIDR.Spec.CIDRs, "2001:db8::/112")
	_, err = kc.NetworkingV1().ServiceCIDRs().Update(ctx, defaultCIDR, metav1.UpdateOptions{})
	if err == nil {
		s.TearDownFn()
		t.Fatal("Expected an error updating default ServiceCIDR but got none")
	}
	if !strings.Contains(err.Error(), "only apiserver can update and create servicecidrs") {
		s.TearDownFn()
		t.Fatalf("Expected error to contain 'only apiserver can update and create servicecidrs', but got: %v", err)
	}

	// 4. 添加一个新的 ServiceCIDR
	_, err = clientset.NetworkingV1().ServiceCIDRs().Create(ctx, makeServiceCIDR("cidrnew", "192.168.0.0/28", ""), metav1.CreateOptions{})
	if err == nil {
		s.TearDownFn()
		t.Fatal("Expected an error creating a new ServiceCIDR but got none")
	}
	if !strings.Contains(err.Error(), "only allow changes on the default servicecidr") {
		s.TearDownFn()
		t.Fatalf("Expected error to contain 'only allow changes on the default servicecidr', but got: %v", err)
	}

	// 5. 停止当前 apiserver，并启动一个在 flag 中带有双栈 CIDR 的新 apiserver
	s.TearDownFn()
	apiServerOptionsDual := kubeapiservertesting.NewDefaultTestServerOptions()
	sDual := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptionsDual,
		[]string{
			"--runtime-config=networking.k8s.io/v1=true",
			"--service-cluster-ip-range=192.168.0.0/24,2001:db8::/112",
			"--advertise-address=10.1.1.1",
			"--disable-admission-plugins=ServiceAccount",
			"--enable-admission-plugins=ValidatingAdmissionPolicy",
		},
		etcdOptions)
	defer sDual.TearDownFn()

	clientsetDual, err := kubernetes.NewForConfig(sDual.ClientConfig)
	if err != nil {
		t.Fatalf("Failed to create clientset for dual-stack server: %v", err)
	}

	// 6. 验证默认 service CIDR 已被更新
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		cidr, err := clientsetDual.NetworkingV1().ServiceCIDRs().Get(ctx, "kubernetes", metav1.GetOptions{})
		if err != nil {
			// API server 可能尚未完全就绪，因此出错时重试。
			t.Logf("failed to get service cidr, retrying: %v", err)
			return false, nil
		}
		if len(cidr.Spec.CIDRs) == 2 {
			return true, nil
		}
		t.Logf("service cidr not updated yet, have: %v", cidr.Spec.CIDRs)
		return false, nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for default ServiceCIDR to be updated to dual-stack: %v", err)
	}

	updatedDefaultCIDR, err := clientsetDual.NetworkingV1().ServiceCIDRs().Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get updated default ServiceCIDR: %v", err)
	}

	expectedCIDRs := []string{"192.168.0.0/24", "2001:db8::/112"}
	sort.Strings(updatedDefaultCIDR.Spec.CIDRs)
	sort.Strings(expectedCIDRs)
	if !reflect.DeepEqual(updatedDefaultCIDR.Spec.CIDRs, expectedCIDRs) {
		t.Errorf("Expected ServiceCIDR to be %v, but got %v", expectedCIDRs, updatedDefaultCIDR.Spec.CIDRs)
	}
}
