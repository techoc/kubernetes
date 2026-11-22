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

package exists

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	informers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// newHandlerForTest 返回测试用的、已注入初始化器的准入插件。
func newHandlerForTest(c kubernetes.Interface) (admission.ValidationInterface, informers.SharedInformerFactory, error) {
	f := informers.NewSharedInformerFactory(c, 5*time.Minute)
	handler := NewExists()
	pluginInitializer := genericadmissioninitializer.New(c, nil, f, nil, nil, nil, nil, nil)
	pluginInitializer.Initialize(handler)
	err := admission.ValidateInitialization(handler)
	return handler, f, err
}

// newMockClientForTest 创建模拟客户端，list 操作返回指定的命名空间列表。
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

// newPod 构造指定命名空间下的测试 Pod。
func newPod(namespace string) api.Pod {
	return api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "123", Namespace: namespace},
		Spec: api.PodSpec{
			Volumes:    []api.Volume{{Name: "vol"}},
			Containers: []api.Container{{Name: "ctr", Image: "image"}},
		},
	}
}

// TestAdmissionNamespaceExists 验证命名空间存在时 Pod 可以准入。
// TestAdmissionNamespaceExists 验证命名空间存在时 Pod 可以准入。
func TestAdmissionNamespaceExists(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{namespace})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error returned from admission handler")
	}
}

// TestAdmissionNamespaceDoesNotExist 验证命名空间不存在时 Pod 准入被拒绝。
// TestAdmissionNamespaceDoesNotExist 验证命名空间不存在时 Pod 准入被拒绝。
func TestAdmissionNamespaceDoesNotExist(t *testing.T) {
	namespace := "test"
	mockClient := newMockClientForTest([]string{})
	mockClient.AddReactor("get", "namespaces", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("nope, out of luck")
	})
	handler, informerFactory, err := newHandlerForTest(mockClient)
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	informerFactory.Start(wait.NeverStop)

	pod := newPod(namespace)
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name, api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		actions := ""
		for _, action := range mockClient.Actions() {
			actions = actions + action.GetVerb() + ":" + action.GetResource().Resource + ":" + action.GetSubresource() + ", "
		}
		t.Errorf("expected error returned from admission handler: %v", actions)
	}
}
