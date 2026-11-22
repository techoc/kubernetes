/*
Copyright 2014 The Kubernetes Authors.

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

package autoprovision

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	admissiontesting "k8s.io/apiserver/pkg/admission/testing"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// newHandlerForTest 创建测试用的准入控制器，并完成初始化校验。
func newHandlerForTest(c clientset.Interface) (admission.MutationInterface, informers.SharedInformerFactory, error) {
	f := informers.NewSharedInformerFactory(c, 5*time.Minute)
	handler := NewProvision()
	pluginInitializer := genericadmissioninitializer.New(c, nil, f, nil, nil, nil, nil, nil)
	pluginInitializer.Initialize(handler)
	err := admission.ValidateInitialization(handler)
	return handler, f, err
}

// newMockClientForTest 创建 mock 客户端，其 namespace 列表由参数指定。
func newMockClientForTest(namespaces []string) *fake.Clientset {
	mockClient := &fake.Clientset{}
	mockClient.AddReactor("list", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		namespaceList := &corev1.NamespaceList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: fmt.Sprintf("%d", len(namespaces)),
			},
		}
		for i, ns := range namespaces {
			namespaceList.Items = append(namespaceList.Items, corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:            ns,
					ResourceVersion: fmt.Sprintf("%d", i),
				},
			})
		}
		return true, namespaceList, nil
	})
	return mockClient
}

// newPod 创建指定命名空间下的测试 Pod。
func newPod(namespace string) api.Pod {
	return api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "123", Namespace: namespace},
		Spec: api.PodSpec{
			Volumes:    []api.Volume{{Name: "vol"}},
			Containers: []api.Container{{Name: "ctr", Image: "image"}},
		},
	}
}

// hasCreateNamespaceAction 判断 mock 客户端是否收到创建命名空间的请求。
func hasCreateNamespaceAction(mockClient *fake.Clientset) bool {
	for _, action := range mockClient.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "namespaces" {
			return true
		}
	}
	return false
}

// TestAdmission 验证命名空间不存在时会触发创建。
func TestAdmission(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = admissiontesting.WithReinvocationTesting(t, handler).Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
	if !hasCreateNamespaceAction(mockClient) {
		t.Errorf("expected create namespace action")
	}
}

// TestAdmissionNamespaceExists 验证命名空间已存在时不会发起创建请求。
func TestAdmissionNamespaceExists(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{namespace})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = admissiontesting.WithReinvocationTesting(t, handler).Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
	if hasCreateNamespaceAction(mockClient) {
		t.Errorf("unexpected create namespace action")
	}
}

// TestAdmissionDryRun 验证 dry-run 请求不会真正创建命名空间。
func TestAdmissionDryRun(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = admissiontesting.WithReinvocationTesting(t, handler).Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, true, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
	if hasCreateNamespaceAction(mockClient) {
		t.Errorf("unexpected create namespace action")
	}
}

// TestIgnoreAdmission 验证非创建请求会被忽略。
func TestIgnoreAdmission(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)
	chainHandler := admissiontesting.WithReinvocationTesting(t, admission.NewChainHandler(handler))

	pod := newPod(namespace)
	err = admissiontesting.WithReinvocationTesting(t, chainHandler).Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
	if hasCreateNamespaceAction(mockClient) {
		t.Errorf("unexpected create namespace action")
	}
}

// TestAdmissionWithLatentCache 验证缓存滞后导致 AlreadyExists 时仍能正常完成准入。
func TestAdmissionWithLatentCache(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{})
	mockClient.AddReactor("create", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, errors.NewAlreadyExists(api.Resource("namespaces"), namespace)
	})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = admissiontesting.WithReinvocationTesting(t, handler).Admit(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}

	if !hasCreateNamespaceAction(mockClient) {
		t.Errorf("expected create namespace action")
	}
}
