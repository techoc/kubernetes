/*
Copyright 2015 The Kubernetes Authors.

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

package deletion

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	restclient "k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// TestFinalized 验证 finalized 的判定：存在 finalizer 时未终结，清空后视为已终结。
func TestFinalized(t *testing.T) {
	testNamespace := &v1.Namespace{
		Spec: v1.NamespaceSpec{
			Finalizers: []v1.FinalizerName{"a", "b"},
		},
	}
	if finalized(testNamespace) {
		t.Errorf("Unexpected result, namespace is not finalized")
	}
	testNamespace.Spec.Finalizers = []v1.FinalizerName{}
	if !finalized(testNamespace) {
		t.Errorf("Expected object to be finalized")
	}
}

// TestFinalizeNamespaceFunc 验证 finalizeNamespace 仅移除 kubernetes finalizer，
// 保留其他 finalizer，并通过 finalize 子资源提交更新。
func TestFinalizeNamespaceFunc(t *testing.T) {
	mockClient := &fake.Clientset{}
	testNamespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test",
			ResourceVersion: "1",
		},
		Spec: v1.NamespaceSpec{
			Finalizers: []v1.FinalizerName{"kubernetes", "other"},
		},
	}
	d := namespacedResourcesDeleter{
		nsClient:       mockClient.CoreV1().Namespaces(),
		finalizerToken: v1.FinalizerKubernetes,
	}
	d.finalizeNamespace(context.Background(), testNamespace)
	actions := mockClient.Actions()
	if len(actions) != 1 {
		t.Errorf("Expected 1 mock client action, but got %v", len(actions))
	}
	if !actions[0].Matches("create", "namespaces") || actions[0].GetSubresource() != "finalize" {
		t.Errorf("Expected finalize-namespace action %v", actions[0])
	}
	finalizers := actions[0].(core.CreateAction).GetObject().(*v1.Namespace).Spec.Finalizers
	if len(finalizers) != 1 {
		t.Errorf("There should be a single finalizer remaining")
	}
	if string(finalizers[0]) != "other" {
		t.Errorf("Unexpected finalizer value, %v", finalizers[0])
	}
}

// testSyncNamespaceThatIsTerminating 是终止中 Namespace 删除流程的共享测试驱动：
// 按不同场景构造 Namespace 与 API 发现错误，触发 Delete 后校验
// kube/metadata 客户端产生的请求集合与 Namespace 状态条件。
func testSyncNamespaceThatIsTerminating(t *testing.T, versions *metav1.APIVersions) {
	now := metav1.Now()
	namespaceName := "test"
	testNamespacePendingFinalize := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              namespaceName,
			ResourceVersion:   "1",
			DeletionTimestamp: &now,
		},
		Spec: v1.NamespaceSpec{
			Finalizers: []v1.FinalizerName{"kubernetes"},
		},
		Status: v1.NamespaceStatus{
			Phase: v1.NamespaceTerminating,
		},
	}
	testNamespaceFinalizeComplete := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              namespaceName,
			ResourceVersion:   "1",
			DeletionTimestamp: &now,
		},
		Spec: v1.NamespaceSpec{},
		Status: v1.NamespaceStatus{
			Phase: v1.NamespaceTerminating,
		},
	}

	// 批量删除资源时默认会先 GET 集合再 DELETE 集合，据此预构造期望的请求集合。
	metadataClientActionSet := sets.NewString()
	resources := testResources()
	groupVersionResources, _ := discovery.GroupVersionResources(resources)
	for groupVersionResource := range groupVersionResources {
		urlPath := path.Join([]string{
			dynamic.LegacyAPIPathResolverFunc(schema.GroupVersionKind{Group: groupVersionResource.Group, Version: groupVersionResource.Version}),
			groupVersionResource.Group,
			groupVersionResource.Version,
			"namespaces",
			namespaceName,
			groupVersionResource.Resource,
		}...)
		metadataClientActionSet.Insert((&fakeAction{method: "GET", path: urlPath}).String())
		metadataClientActionSet.Insert((&fakeAction{method: "DELETE", path: urlPath}).String())
	}

	scenarios := map[string]struct {
		testNamespace           *v1.Namespace
		kubeClientActionSet     sets.String
		metadataClientActionSet sets.String
		gvrError                error
		expectErrorOnDelete     error
		expectStatus            *v1.NamespaceStatus
	}{
		"pending-finalize": {
			testNamespace: testNamespacePendingFinalize,
			kubeClientActionSet: sets.NewString(
				strings.Join([]string{"get", "namespaces", ""}, "-"),
				strings.Join([]string{"create", "namespaces", "finalize"}, "-"),
				strings.Join([]string{"list", "pods", ""}, "-"),
				strings.Join([]string{"update", "namespaces", "status"}, "-"),
			),
			metadataClientActionSet: metadataClientActionSet,
		},
		"complete-finalize": {
			testNamespace: testNamespaceFinalizeComplete,
			kubeClientActionSet: sets.NewString(
				strings.Join([]string{"get", "namespaces", ""}, "-"),
			),
			metadataClientActionSet: sets.NewString(),
		},
		"groupVersionResourceErr": {
			testNamespace: testNamespaceFinalizeComplete,
			kubeClientActionSet: sets.NewString(
				strings.Join([]string{"get", "namespaces", ""}, "-"),
			),
			metadataClientActionSet: sets.NewString(),
			gvrError:                fmt.Errorf("test error"),
		},
		"groupVersionResourceErr-finalize": {
			testNamespace: testNamespacePendingFinalize,
			kubeClientActionSet: sets.NewString(
				strings.Join([]string{"get", "namespaces", ""}, "-"),
				strings.Join([]string{"list", "pods", ""}, "-"),
				strings.Join([]string{"update", "namespaces", "status"}, "-"),
			),
			metadataClientActionSet: metadataClientActionSet,
			gvrError:                fmt.Errorf("test error"),
			expectErrorOnDelete:     fmt.Errorf("test error"),
			expectStatus: &v1.NamespaceStatus{
				Phase: v1.NamespaceTerminating,
				Conditions: []v1.NamespaceCondition{
					{Type: v1.NamespaceDeletionDiscoveryFailure},
				},
			},
		},
	}

	for scenario, testInput := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			testHandler := &fakeActionHandler{statusCode: 200}
			srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
			defer srv.Close()

			mockClient := fake.NewSimpleClientset(testInput.testNamespace)
			metadataClient, err := metadata.NewForConfig(clientConfig)
			if err != nil {
				t.Fatal(err)
			}

			fn := func() ([]*metav1.APIResourceList, error) {
				return resources, testInput.gvrError
			}
			_, ctx := ktesting.NewTestContext(t)
			d := NewNamespacedResourcesDeleter(ctx, mockClient.CoreV1().Namespaces(), metadataClient, mockClient.CoreV1(), fn, v1.FinalizerKubernetes)
			if err := d.Delete(ctx, testInput.testNamespace.Name); !matchErrors(err, testInput.expectErrorOnDelete) {
				t.Errorf("expected error %q when syncing namespace, got %q, %v", testInput.expectErrorOnDelete, err, testInput.expectErrorOnDelete == err)
			}

			// 校验 kube client 产生的请求集合。
			actionSet := sets.NewString()
			for _, action := range mockClient.Actions() {
				actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
			}
			if !actionSet.Equal(testInput.kubeClientActionSet) {
				t.Errorf("mock client expected actions:\n%v\n but got:\n%v\nDifference:\n%v",
					testInput.kubeClientActionSet, actionSet, testInput.kubeClientActionSet.Difference(actionSet))
			}

			// 校验 metadata client 产生的请求集合。
			actionSet = sets.NewString()
			for _, action := range testHandler.actions {
				actionSet.Insert(action.String())
			}
			if !actionSet.Equal(testInput.metadataClientActionSet) {
				t.Errorf(" metadata client expected actions:\n%v\n but got:\n%v\nDifference:\n%v",
					testInput.metadataClientActionSet, actionSet, testInput.metadataClientActionSet.Difference(actionSet))
			}

			// 校验失败场景下写入 Namespace 的状态条件。
			if testInput.expectStatus != nil {
				obj, err := mockClient.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, testInput.testNamespace.Namespace, testInput.testNamespace.Name)
				if err != nil {
					t.Fatalf("Unexpected error in getting the namespace: %v", err)
				}
				ns, ok := obj.(*v1.Namespace)
				if !ok {
					t.Fatalf("Expected a namespace but received %v", obj)
				}
				if ns.Status.Phase != testInput.expectStatus.Phase {
					t.Fatalf("Expected namespace status phase %v but received %v", testInput.expectStatus.Phase, ns.Status.Phase)
				}
				for _, expCondition := range testInput.expectStatus.Conditions {
					nsCondition := getCondition(ns.Status.Conditions, expCondition.Type)
					if nsCondition == nil {
						t.Fatalf("Missing namespace status condition %v", expCondition.Type)
					}
				}
			}
		})
	}
}

// TestRetryOnConflictError 验证 retryOnConflictError 在遇到冲突时会重试，
// 直到操作成功（本用例中第二次即成功）。
func TestRetryOnConflictError(t *testing.T) {
	mockClient := &fake.Clientset{}
	numTries := 0
	retryOnce := func(ctx context.Context, namespace *v1.Namespace) (*v1.Namespace, error) {
		numTries++
		if numTries <= 1 {
			return namespace, errors.NewConflict(api.Resource("namespaces"), namespace.Name, fmt.Errorf("ERROR"))
		}
		return namespace, nil
	}
	namespace := &v1.Namespace{}
	d := namespacedResourcesDeleter{
		nsClient: mockClient.CoreV1().Namespaces(),
	}
	_, err := d.retryOnConflictError(context.Background(), namespace, retryOnce)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if numTries != 2 {
		t.Errorf("Expected %v, but got %v", 2, numTries)
	}
}

// TestSyncNamespaceThatIsTerminatingNonExperimental 用空 API 版本列表驱动终止中 Namespace 的删除测试。
func TestSyncNamespaceThatIsTerminatingNonExperimental(t *testing.T) {
	testSyncNamespaceThatIsTerminating(t, &metav1.APIVersions{})
}

// TestSyncNamespaceThatIsTerminatingV1 用含 apps/v1 的 API 版本列表驱动终止中 Namespace 的删除测试。
func TestSyncNamespaceThatIsTerminatingV1(t *testing.T) {
	testSyncNamespaceThatIsTerminating(t, &metav1.APIVersions{Versions: []string{"apps/v1"}})
}

// TestSyncNamespaceThatIsActive 验证对处于 Active 状态的 Namespace，Delete 只读取一次对象，
// 不会触发任何删除动作。
func TestSyncNamespaceThatIsActive(t *testing.T) {
	mockClient := &fake.Clientset{}
	testNamespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test",
			ResourceVersion: "1",
		},
		Spec: v1.NamespaceSpec{
			Finalizers: []v1.FinalizerName{"kubernetes"},
		},
		Status: v1.NamespaceStatus{
			Phase: v1.NamespaceActive,
		},
	}
	fn := func() ([]*metav1.APIResourceList, error) {
		return testResources(), nil
	}
	_, ctx := ktesting.NewTestContext(t)
	d := NewNamespacedResourcesDeleter(ctx, mockClient.CoreV1().Namespaces(), nil, mockClient.CoreV1(),
		fn, v1.FinalizerKubernetes)
	err := d.Delete(ctx, testNamespace.Name)
	if err != nil {
		t.Errorf("Unexpected error when synching namespace %v", err)
	}
	if len(mockClient.Actions()) != 1 {
		t.Errorf("Expected only one action from controller, but got: %d %v", len(mockClient.Actions()), mockClient.Actions())
	}
	action := mockClient.Actions()[0]
	if !action.Matches("get", "namespaces") {
		t.Errorf("Expected get namespaces, got: %v", action)
	}
}

// matchErrors 比较两个 error 是否匹配；本测试中按错误消息比较即可满足需要。
func matchErrors(e1, e2 error) bool {
	if e1 == nil && e2 == nil {
		return true
	}
	if e1 != nil && e2 != nil {
		return e1.Error() == e2.Error()
	}
	return false
}

// testServerAndClientConfig 启动一个测试 HTTP 服务并返回可连接到它的客户端配置。
func testServerAndClientConfig(handler func(http.ResponseWriter, *http.Request)) (*httptest.Server, *restclient.Config) {
	srv := httptest.NewServer(http.HandlerFunc(handler))
	config := &restclient.Config{
		Host: srv.URL,
	}
	return srv, config
}

// fakeAction 记录一次请求的 method 与 path，便于在测试中比对客户端发出的请求。
type fakeAction struct {
	method string
	path   string
}

// String 以 method=path 的形式返回请求信息，便于放入集合做比较。
func (f *fakeAction) String() string {
	return strings.Join([]string{f.method, f.path}, "=")
}

// fakeActionHandler 记录接收到的所有 fakeAction 的 HTTP 处理器。
type fakeActionHandler struct {
	// statusCode 是该处理器对所有请求返回的 HTTP 状态码。
	statusCode int

	lock    sync.Mutex
	actions []fakeAction
}

// ServeHTTP 记录收到的请求信息，并始终返回预设的状态码与空列表响应。
func (f *fakeActionHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.actions = append(f.actions, fakeAction{method: request.Method, path: request.URL.Path})
	response.Header().Set("Content-Type", runtime.ContentTypeJSON)
	response.WriteHeader(f.statusCode)
	response.Write([]byte("{\"apiVersion\": \"v1\", \"kind\": \"List\",\"items\":null}"))
}

// testResources 返回测试用的资源发现信息，覆盖多个 API 组，供 namespace 控制器测试使用。
func testResources() []*metav1.APIResourceList {
	results := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{
					Name:       "pods",
					Namespaced: true,
					Kind:       "Pod",
					Verbs:      []string{"get", "list", "delete", "deletecollection", "create", "update"},
				},
				{
					Name:       "services",
					Namespaced: true,
					Kind:       "Service",
					Verbs:      []string{"get", "list", "delete", "deletecollection", "create", "update"},
				},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{
					Name:       "deployments",
					Namespaced: true,
					Kind:       "Deployment",
					Verbs:      []string{"get", "list", "delete", "deletecollection", "create", "update"},
				},
			},
		},
	}
	return results
}

// TestDeleteEncounters404 验证删除过程中遇到 NotFound 时的回退路径：
// delete-collection 返回 404 后改用 list 再 list 确认资源已清空；
// 并验证不同 Namespace 之间不会相互干扰。
func TestDeleteEncounters404(t *testing.T) {
	now := metav1.Now()
	ns1 := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns1", ResourceVersion: "1", DeletionTimestamp: &now},
		Spec:       v1.NamespaceSpec{Finalizers: []v1.FinalizerName{"kubernetes"}},
		Status:     v1.NamespaceStatus{Phase: v1.NamespaceActive},
	}
	ns2 := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns2", ResourceVersion: "1", DeletionTimestamp: &now},
		Spec:       v1.NamespaceSpec{Finalizers: []v1.FinalizerName{"kubernetes"}},
		Status:     v1.NamespaceStatus{Phase: v1.NamespaceActive},
	}
	mockClient := fake.NewSimpleClientset(ns1, ns2)

	ns1FlakesNotFound := func(action core.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetNamespace() == "ns1" {
			// 模拟处理 ns1 时 flakes 资源不存在，触发 NotFound 回退路径。
			return true, nil, errors.NewNotFound(schema.GroupResource{}, "")
		}
		return false, nil, nil
	}
	mockMetadataClient := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	mockMetadataClient.PrependReactor("delete-collection", "flakes", ns1FlakesNotFound)
	mockMetadataClient.PrependReactor("list", "flakes", ns1FlakesNotFound)

	resourcesFn := func() ([]*metav1.APIResourceList, error) {
		return []*metav1.APIResourceList{{
			GroupVersion: "example.com/v1",
			APIResources: []metav1.APIResource{{Name: "flakes", Namespaced: true, Kind: "Flake", Verbs: []string{"get", "list", "delete", "deletecollection", "create", "update"}}},
		}}, nil
	}
	_, ctx := ktesting.NewTestContext(t)
	d := NewNamespacedResourcesDeleter(ctx, mockClient.CoreV1().Namespaces(), mockMetadataClient, mockClient.CoreV1(), resourcesFn, v1.FinalizerKubernetes)

	// 删除 ns1，flakes 资源会返回 NotFound 错误。
	mockMetadataClient.ClearActions()
	if err := d.Delete(ctx, ns1.Name); err != nil {
		t.Fatal(err)
	}
	if len(mockMetadataClient.Actions()) != 3 ||
		!mockMetadataClient.Actions()[0].Matches("delete-collection", "flakes") ||
		!mockMetadataClient.Actions()[1].Matches("list", "flakes") ||
		!mockMetadataClient.Actions()[2].Matches("list", "flakes") {
		for _, action := range mockMetadataClient.Actions() {
			t.Log("ns1", action)
		}
		t.Error("ns1: expected delete-collection -> fallback to list -> list to verify 0 items")
	}

	// 删除 ns2，验证其回退路径不受 ns1 影响。
	mockMetadataClient.ClearActions()
	if err := d.Delete(ctx, ns2.Name); err != nil {
		t.Fatal(err)
	}
	if len(mockMetadataClient.Actions()) != 2 ||
		!mockMetadataClient.Actions()[0].Matches("delete-collection", "flakes") ||
		!mockMetadataClient.Actions()[1].Matches("list", "flakes") {
		for _, action := range mockMetadataClient.Actions() {
			t.Log("ns2", action)
		}
		t.Error("ns2: expected delete-collection -> list to verify 0 items")
	}
}
