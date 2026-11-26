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

package lifecycle

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	kubeadmission "k8s.io/apiserver/pkg/admission/initializer"
	informers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// newHandlerForTest 返回一个为测试配置好的 handler。
// 使用真实时钟，适用于不关心 forceLiveLookupTTL 的用例。
func newHandlerForTest(c clientset.Interface) (*Lifecycle, informers.SharedInformerFactory, error) {
	return newHandlerForTestWithClock(c, clock.RealClock{})
}

// newHandlerForTestWithClock 返回一个为测试配置好、可指定时钟的 handler。
// 可注入假时钟，便于测试强制实时查询的有效期；初始化流程与生产一致，
// 以便 ValidateInitialization 能发现缺少依赖的问题。
func newHandlerForTestWithClock(c clientset.Interface, cacheClock clock.Clock) (*Lifecycle, informers.SharedInformerFactory, error) {
	f := informers.NewSharedInformerFactory(c, 5*time.Minute)
	handler, err := newLifecycleWithClock(sets.New[string](metav1.NamespaceDefault, metav1.NamespaceSystem), cacheClock)
	if err != nil {
		return nil, f, err
	}
	pluginInitializer := kubeadmission.New(c, nil, f, nil, nil, nil, nil, nil)
	pluginInitializer.Initialize(handler)
	err = admission.ValidateInitialization(handler)
	return handler, f, err
}

// newMockClientForTest 创建一个 mock 客户端，按指定的 phase 返回给定那批命名空间的对象。
// 只拦截 list：插件的 lister 由 informer 填充，其余请求走默认的 fake 行为。
func newMockClientForTest(namespaces map[string]v1.NamespacePhase) *fake.Clientset {
	mockClient := &fake.Clientset{}
	mockClient.AddReactor("list", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		namespaceList := &v1.NamespaceList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: fmt.Sprintf("%d", len(namespaces)),
			},
		}
		index := 0
		for name, phase := range namespaces {
			namespaceList.Items = append(namespaceList.Items, v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:            name,
					ResourceVersion: fmt.Sprintf("%d", index),
				},
				Status: v1.NamespaceStatus{
					Phase: phase,
				},
			})
			index++
		}
		return true, namespaceList, nil
	})
	return mockClient
}

// newPod 返回指定命名空间下的一个新 Pod。
// 只带最小字段，避免无关字段影响准入判断。
func newPod(namespace string) v1.Pod {
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "123", Namespace: namespace},
		Spec: v1.PodSpec{
			Volumes:    []v1.Volume{{Name: "vol"}},
			Containers: []v1.Container{{Name: "ctr", Image: "image"}},
		},
	}
}

// TestAccessReviewCheckOnMissingNamespace 验证对 LocalSubjectAccessReview 这类
// 非命名空间资源的准入请求，在命名空间不存在时也不会被拒绝：
// 授权检查本身需要能在目标命名空间缺失时运行，才能给出有意义的鉴权结果。
func TestAccessReviewCheckOnMissingNamespace(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest(map[string]v1.NamespacePhase{})
	mockClient.AddReactor("get", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("nope, out of luck")
	})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory.Start(stopCh)

	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "authorization.k8s.io", Version: "v1", Kind: "LocalSubjectAccesReview"}, namespace, "", schema.GroupVersionResource{Group: "authorization.k8s.io", Version: "v1", Resource: "localsubjectaccessreviews"}, "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Error(err)
	}
}

// TestAdmissionNamespaceDoesNotExist 验证命名空间不存在时 Pod 不会被准入。
// 同时覆盖了“更新/删除不受命名空间缺失影响”的兼容性约束：
// 已存在于存储中的对象必须仍可被清理或更新。
func TestAdmissionNamespaceDoesNotExist(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest(map[string]v1.NamespacePhase{})
	mockClient.AddReactor("get", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("nope, out of luck")
	})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		actions := ""
		for _, action := range mockClient.Actions() {
			actions = actions + action.GetVerb() + ":" + action.GetResource().Resource + ":" + action.GetSubresource() + ", "
		}
		t.Errorf("expected error returned from admission handler: %v", actions)
	}

	// 验证在该命名空间中创建资源会报错
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected error rejecting creates in a namespace when it is missing")
	}

	// 验证无旧对象时，该命名空间下的更新操作可以放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error rejecting updates in a namespace when it is missing: %v", err)
	}

	// 验证该命名空间下的删除操作可以放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error returned from admission handler: %v", err)
	}
}

// TestAdmissionNamespaceFullyDeleted 验证命名空间从存储中彻底删除（不只是 Terminating）后，
// Create 被拒绝而 Update 放行。对象可能比命名空间活得更久，跨命名空间列举的控制器
// 仍需能更新这些残留对象。对象是否真实存在由存储层最终校验；按 Update 处理时重跑
// Create 准入链是安全的，因为存储层会以 Create 操作再次执行准入。
//
// 命名空间被彻底删除后，残留对象仍需可更新/删除，因此只有 Create 被拒绝；
// 存储层会负责对象是否存在的最终校验。
func TestAdmissionNamespaceFullyDeleted(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest(map[string]v1.NamespacePhase{})
	mockClient.AddReactor("get", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, errors.NewNotFound(v1.Resource("namespaces"), namespace)
	})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)

	// 验证创建被拒绝
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected error rejecting creates in a fully deleted namespace")
	}

	// 验证更新被放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error rejecting updates in a fully deleted namespace: %v", err)
	}

	// 验证删除被放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error returned from admission handler: %v", err)
	}
}

// TestAdmissionNamespaceActive 验证命名空间处于 Active 时资源会被准入。
// 活跃命名空间下，Create 应当无任何错误地通过。
func TestAdmissionNamespaceActive(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest(map[string]v1.NamespacePhase{
		namespace: v1.NamespaceActive,
	})

	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
}

// TestAdmissionNamespaceTerminating 验证命名空间处于 Terminating 时不会创建资源。
// 终止中的命名空间：Create 被拒绝且带 NamespaceTerminatingCause，Update/Delete 放行；
// 另外 default 与 kube-system 这类 immortal 命名空间永远不能被删除。
func TestAdmissionNamespaceTerminating(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest(map[string]v1.NamespacePhase{
		namespace: v1.NamespaceTerminating,
	})

	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	// 验证在该命名空间中创建资源会报错
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected error rejecting creates in a namespace when it is terminating")
	}
	expectedCause := metav1.StatusCause{
		Type:    v1.NamespaceTerminatingCause,
		Message: fmt.Sprintf("namespace %s is being terminated", namespace),
		Field:   "metadata.namespace",
	}
	if cause, ok := errors.StatusCause(err, v1.NamespaceTerminatingCause); !ok || !reflect.DeepEqual(expectedCause, cause) {
		t.Errorf("Expected status cause indicating the namespace is terminating: %t %s", ok, cmp.Diff(expectedCause, cause))
	}

	// 验证该命名空间下的更新操作可以放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error returned from admission handler: %v", err)
	}

	// 验证该命名空间下的删除操作可以放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error returned from admission handler: %v", err)
	}

	// 验证 default 命名空间永不允许删除
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Namespace").GroupKind().WithVersion("version"), "", metav1.NamespaceDefault, v1.Resource("namespaces").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error that this namespace can never be deleted")
	}

	// 验证非 default 命名空间允许删除
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Namespace").GroupKind().WithVersion("version"), "", "other", v1.Resource("namespaces").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Did not expect an error %v", err)
	}
}

// TestAdmissionNamespaceForceLiveLookup 验证删除命名空间之后会执行实时（绕过缓存）查询。
// 删除命名空间后，informer 缓存可能仍显示 Active，因此需要在
// forceLiveLookupTTL 窗口内绕过缓存直接查询 API Server；窗口结束后恢复走缓存，
// 避免每个请求都产生一次额外 API 调用。
func TestAdmissionNamespaceForceLiveLookup(t *testing.T) {
	namespace := "test"
	getCalls := int64(0)
	phases := map[string]v1.NamespacePhase{namespace: v1.NamespaceActive}
	mockClient := newMockClientForTest(phases)
	mockClient.AddReactor("get", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		getCalls++
		return true, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}, Status: v1.NamespaceStatus{Phase: phases[namespace]}}, nil
	})

	fakeClock := testingclock.NewFakeClock(time.Now())

	handler, informerFactory, err := newHandlerForTestWithClock(mockClient, fakeClock)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	// 验证该命名空间下的创建操作被放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error rejecting creates in an active namespace")
	}
	if getCalls != 0 {
		t.Errorf("Expected no live lookups of the namespace, got %d", getCalls)
	}
	getCalls = 0

	// 验证命名空间删除可以放行
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, v1.SchemeGroupVersion.WithKind("Namespace").GroupKind().WithVersion("version"), namespace, namespace, v1.Resource("namespaces").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Expected namespace deletion to be allowed")
	}
	if getCalls != 0 {
		t.Errorf("Expected no live lookups of the namespace, got %d", getCalls)
	}
	getCalls = 0

	// 模拟 phase 发生变化。
	// 只改 reactor 中的 phase，不更新 informer 缓存，模拟“缓存滞后于真实状态”。
	phases[namespace] = v1.NamespaceTerminating

	// 验证在该命名空间中创建资源会报错
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected error rejecting creates in a namespace right after deleting it")
	}
	if getCalls != 1 {
		t.Errorf("Expected a live lookup of the namespace at t=0, got %d", getCalls)
	}
	getCalls = 0

	// 验证在 forceLiveLookupTTL 窗口内仍强制走实时查询
	fakeClock.Step(forceLiveLookupTTL)

	// 验证在该命名空间中创建资源会报错
	err = handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected error rejecting creates in a namespace right after deleting it")
	}
	if getCalls != 1 {
		t.Errorf("Expected a live lookup of the namespace at t=forceLiveLookupTTL, got %d", getCalls)
	}
	getCalls = 0

	// 验证窗口到期后实时查询失效
	fakeClock.Step(time.Millisecond)

	// 验证超时后该命名空间下的创建不再强制走实时查询
	handler.Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, v1.SchemeGroupVersion.WithKind("Pod").GroupKind().WithVersion("version"), pod.Namespace, pod.Name, v1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if getCalls != 0 {
		t.Errorf("Expected no live lookup of the namespace at t=forceLiveLookupTTL+1ms, got %d", getCalls)
	}
	getCalls = 0
}
