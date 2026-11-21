/*
Copyright 2016 The Kubernetes Authors.

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

package tests

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetfake "k8s.io/client-go/kubernetes/fake"

	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// TestFakeClientSetFiltering 验证 fake clientset 会按命名空间过滤资源：
// 带命名空间的 List/Get 只能返回该命名空间下的对象，跨命名空间的 Get 应失败，
// 而使用 NamespaceAll 时则返回全部对象。
func TestFakeClientSetFiltering(t *testing.T) {
	// 预置两个命名空间下的 Pod 与 ServiceAccount，作为过滤行为的测试数据。
	tc := clientsetfake.NewSimpleClientset(
		testPod("nsA", "pod-1"),
		testPod("nsB", "pod-2"),
		testSA("nsA", "sa-1"),
		testSA("nsA", "sa-2"),
		testSA("nsB", "sa-1"),
		testSA("nsB", "sa-2"),
		testSA("nsB", "sa-3"),
	)

	// 在 nsA 下应只得到 2 个 ServiceAccount，逐项再次确认命名空间，
	// 防止 fake 实现返回数量正确但内容串命名空间。
	saList1, err := tc.CoreV1().ServiceAccounts("nsA").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("ServiceAccounts.List: %s", err)
	}
	if actual, expected := len(saList1.Items), 2; expected != actual {
		t.Fatalf("Expected %d records to match, got %d", expected, actual)
	}
	for _, sa := range saList1.Items {
		if sa.Namespace != "nsA" {
			t.Fatalf("Expected namespace %q; got %q", "nsA", sa.Namespace)
		}
	}

	// nsB 下有 3 个 ServiceAccount，验证不同命名空间的列表互不影响。
	saList2, err := tc.CoreV1().ServiceAccounts("nsB").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("ServiceAccounts.List: %s", err)
	}
	if actual, expected := len(saList2.Items), 3; expected != actual {
		t.Fatalf("Expected %d records to match, got %d", expected, actual)
	}
	for _, sa := range saList2.Items {
		if sa.Namespace != "nsB" {
			t.Fatalf("Expected namespace %q; got %q", "nsA", sa.Namespace)
		}
	}

	pod1, err := tc.CoreV1().Pods("nsA").Get(context.TODO(), "pod-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Pods.Get: %s", err)
	}
	if pod1 == nil {
		t.Fatalf("Expected to find pod nsA/pod-1 but it wasn't found")
	}
	if pod1.Namespace != "nsA" || pod1.Name != "pod-1" {
		t.Fatalf("Expected to find pod nsA/pod-1t, got %s/%s", pod1.Namespace, pod1.Name)
	}

	// 同名 Pod 只存在于 nsA，从 nsB 查询必须返回错误，
	// 这说明 fake 客户端的 Get 会同时考虑名称与请求命名空间。
	wrongPod, err := tc.CoreV1().Pods("nsB").Get(context.TODO(), "pod-1", metav1.GetOptions{})
	if err == nil {
		t.Fatalf("Pods.Get: expected nsB/pod-1 not to match, but it matched %s/%s", wrongPod.Namespace, wrongPod.Name)
	}

	allPods, err := tc.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Pods.List: %s", err)
	}
	// NamespaceAll 相当于取消命名空间过滤，应看到全部 2 个 Pod。
	if actual, expected := len(allPods.Items), 2; expected != actual {
		t.Fatalf("Expected %d pods to match, got %d", expected, actual)
	}

	allSAs, err := tc.CoreV1().ServiceAccounts(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("ServiceAccounts.List: %s", err)
	}
	// 同理，全局列表应包含两个命名空间共 5 个 ServiceAccount。
	if actual, expected := len(allSAs.Items), 5; expected != actual {
		t.Fatalf("Expected %d service accounts to match, got %d", expected, actual)
	}
}

// TestFakeClientsetInheritsNamespace 验证 fake clientset 会为对象补全命名空间：
// 当创建的对象未指定 Namespace 时，fake 客户端会用请求路径中的命名空间填充它，
// 同时拒绝对象命名空间与请求命名空间不一致的写入。
func TestFakeClientsetInheritsNamespace(t *testing.T) {
	tc := clientsetfake.NewSimpleClientset(
		testNamespace("nsA"),
		testPod("nsA", "pod-1"),
	)

	_, err := tc.CoreV1().Namespaces().Create(context.TODO(), testNamespace("nsB"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Namespaces.Create: %s", err)
	}

	allNS, err := tc.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Namespaces.List: %s", err)
	}
	if actual, expected := len(allNS.Items), 2; expected != actual {
		t.Fatalf("Expected %d namespaces to match, got %d", expected, actual)
	}

	// 创建时对象未填 Namespace，fake 客户端应从请求路径继承 "nsB"，
	// 因此后续用 nsB 查询应能命中。
	_, err = tc.CoreV1().Pods("nsB").Create(context.TODO(), testPod("", "pod-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Pods.Create nsB/pod-1: %s", err)
	}

	podB1, err := tc.CoreV1().Pods("nsB").Get(context.TODO(), "pod-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Pods.Get nsB/pod-1: %s", err)
	}
	if podB1 == nil {
		t.Fatalf("Expected to find pod nsB/pod-1 but it wasn't found")
	}
	if podB1.Namespace != "nsB" || podB1.Name != "pod-1" {
		t.Fatalf("Expected to find pod nsB/pod-1t, got %s/%s", podB1.Namespace, podB1.Name)
	}

	// 未填 Namespace 的对象在 nsA 下与预置的 pod-1 冲突，应报 AlreadyExists。
	_, err = tc.CoreV1().Pods("nsA").Create(context.TODO(), testPod("", "pod-1"), metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("Expected Pods.Create to fail with already exists error")
	}

	// Update 同样允许继承命名空间，因为对象已存在，更新操作应成功。
	_, err = tc.CoreV1().Pods("nsA").Update(context.TODO(), testPod("", "pod-1"), metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Pods.Update nsA/pod-1: %s", err)
	}

	// 对象显式声明 nsB 却向 nsA 发起 Create，命名空间冲突必须被拒绝。
	_, err = tc.CoreV1().Pods("nsA").Create(context.TODO(), testPod("nsB", "pod-2"), metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("Expected Pods.Create to fail with bad request from namespace mismtach")
	}
	// 错误信息须同时给出请求与对象的命名空间，便于定位到底是哪一侧不一致。
	if err.Error() != `request namespace does not match object namespace, request: "nsA" object: "nsB"` {
		t.Fatalf("Expected Pods.Create error to provide object and request namespaces, got %q", err)
	}

	// pod-3 并不存在，Update 不应隐式创建它，应返回 NotFound。
	_, err = tc.CoreV1().Pods("nsA").Update(context.TODO(), testPod("", "pod-3"), metav1.UpdateOptions{})
	if err == nil {
		t.Fatalf("Expected Pods.Update nsA/pod-3 to fail with not found error")
	}
}

// testSA 构造一个仅含命名空间与名称的 ServiceAccount，作为测试夹具。
func testSA(ns, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
	}
}

// testPod 构造一个仅含命名空间与名称的 Pod，作为测试夹具。
func testPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
	}
}

// testNamespace 构造一个仅含名称的 Namespace，作为测试夹具。
func testNamespace(ns string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}
}
