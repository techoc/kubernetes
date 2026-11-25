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

package apiserver

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/test/integration/authutil"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestPodSubresourceAuth 测试 pod 子资源的合成鉴权检查是否正常工作。
func TestPodSubresourceAuth(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, clientConfig, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.Authorization.Modes = []string{"RBAC"}
		},
	})
	defer tearDownFn()

	adminConfig := rest.CopyConfig(clientConfig)
	adminClientset, err := kubernetes.NewForConfig(adminConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ns := "test-pod-subresource-auth"
	if _, err := adminClientset.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if _, err := adminClientset.CoreV1().ServiceAccounts(ns).Create(context.TODO(), sa, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "test-image",
				},
			},
		},
	}
	if _, err := adminClientset.CoreV1().Pods(ns).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// 只有 'get' 权限的用户
	podGetterUsername := "pod-getter"
	podGetterRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-getter-role"},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods/exec", "pods/attach", "pods/portforward"},
				Verbs:     []string{"get"},
			},
		},
	}
	if _, err := adminClientset.RbacV1().Roles(ns).Create(context.TODO(), podGetterRole, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	podGetterRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-getter-binding"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: podGetterUsername}},
		RoleRef:    rbacv1.RoleRef{Kind: "Role", Name: "pod-getter-role"},
	}
	if _, err := adminClientset.RbacV1().RoleBindings(ns).Create(context.TODO(), podGetterRoleBinding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	podGetterConfig := rest.CopyConfig(clientConfig)
	podGetterConfig.Impersonate = rest.ImpersonationConfig{UserName: podGetterUsername}
	podGetterClient, err := kubernetes.NewForConfig(podGetterConfig)
	if err != nil {
		t.Fatal(err)
	}

	// 对 pods 子资源具有 'get' 和 'create' 权限的用户。
	podCreatorUsername := "pod-creator"
	podCreatorRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-creator-role"},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods/exec", "pods/attach", "pods/portforward"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/exec", "pods/attach", "pods/portforward"},
				Verbs:     []string{"create"},
			},
		},
	}
	if _, err := adminClientset.RbacV1().Roles(ns).Create(context.TODO(), podCreatorRole, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	podCreatorRoleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-creator-binding"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: podCreatorUsername}},
		RoleRef:    rbacv1.RoleRef{Kind: "Role", Name: "pod-creator-role"},
	}
	if _, err := adminClientset.RbacV1().RoleBindings(ns).Create(context.TODO(), podCreatorRoleBinding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	podCreatorConfig := rest.CopyConfig(clientConfig)
	podCreatorConfig.Impersonate = rest.ImpersonationConfig{UserName: podCreatorUsername}
	podCreatorClient, err := kubernetes.NewForConfig(podCreatorConfig)
	if err != nil {
		t.Fatal(err)
	}

	// 通过确认 create 请求被允许，等待权限生效
	waitContext, cancel := context.WithTimeout(context.TODO(), wait.ForeverTestTimeout)
	defer cancel()
	authutil.WaitForNamedAuthorizationUpdate(t, waitContext, adminClientset.AuthorizationV1(), podCreatorUsername, ns, "create", "", schema.GroupResource{Group: "", Resource: "pods/exec"}, true)

	subresources := []string{"exec", "attach", "portforward"}
	for _, subresource := range subresources {
		t.Run(fmt.Sprintf("subresource=%s", subresource), func(t *testing.T) {
			// 只有 'get' 权限的用户应被拒绝。
			// 使用 GET 方法，因为 WebSocket 升级请求使用的就是该方法。
			err := podGetterClient.CoreV1().RESTClient().Get().
				Namespace(ns).
				Resource("pods").
				Name("test-pod").
				SubResource(subresource).
				Do(context.TODO()).
				Error()
			if !errors.IsForbidden(err) {
				t.Errorf("expected forbidden error for user with only 'get' permissions, but got: %v", err)
			}

			// 具有 'get' 和 'create' 权限的用户应被允许。
			// 使用 GET 方法，因为 WebSocket 升级请求使用的就是该方法。
			err = podCreatorClient.CoreV1().RESTClient().Get().
				Namespace(ns).
				Resource("pods").
				Name("test-pod").
				SubResource(subresource).
				Do(context.TODO()).
				Error()
			// 没有 "Forbidden" 即表示成功；集成测试服务器
			// 并未为该 pod 运行真实的 Kubelet，因此代理/流式连接
			// 在鉴权成功之后最终会失败
			// （因此返回 "Bad Request" 错误）。
			if err != nil && !errors.IsBadRequest(err) {
				t.Errorf("expected nil error for user with 'create' permissions, but got: %v", err)
			}
		})
	}
}
