/*
Copyright 2020 The Kubernetes Authors.

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

package certificates

import (
	"context"
	"testing"

	certv1 "k8s.io/api/certificates/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/authutil"
	"k8s.io/kubernetes/test/integration/framework"
)

// TestCSRSignerNameApprovalPlugin 验证 CSR approval admission 插件是否正确要求用户对指定 signer 具有审批权限。
func TestCSRSignerNameApprovalPlugin(t *testing.T) {
	tests := map[string]struct {
		allowedSignerName string
		signerName        string
		error             string
	}{
		"should admit when a user has permission for the exact signerName": {
			allowedSignerName: "example.com/something",
			signerName:        "example.com/something",
		},
		"should admit when a user has permission for the wildcard-suffixed signerName": {
			allowedSignerName: "example.com/*",
			signerName:        "example.com/something",
		},
		"should deny if a user does not have permission for the given signerName": {
			allowedSignerName: "example.com/not-something",
			signerName:        "example.com/something",
			error:             `certificatesigningrequests.certificates.k8s.io "csr" is forbidden: user not permitted to approve requests with signerName "example.com/something"`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// 用默认配置启动 apiserver。
			s := kubeapiservertesting.StartTestServerOrDie(t, kubeapiservertesting.NewDefaultTestServerOptions(), []string{"--authorization-mode=RBAC"}, framework.SharedEtcd())
			defer s.TearDownFn()
			client := clientset.NewForConfigOrDie(s.ClientConfig)

			// 授予 test-user 审批指定 signerName 的 CertificateSigningRequest 的权限。
			const username = "test-user"
			grantUserPermissionToApproveFor(t, client, username, test.allowedSignerName)
			// 创建用于尝试审批的 CSR。
			csr := createTestingCSR(t, client.CertificatesV1().CertificateSigningRequests(), "csr", test.signerName, "")

			// 再建一个模拟 test-user 的客户端用于测试。
			testuserConfig := restclient.CopyConfig(s.ClientConfig)
			testuserConfig.Impersonate = restclient.ImpersonationConfig{UserName: username}
			testuserClient := clientset.NewForConfigOrDie(testuserConfig)

			// 尝试更新 Approved 条件。
			csr.Status.Conditions = append(csr.Status.Conditions, certv1.CertificateSigningRequestCondition{
				Type:    certv1.CertificateApproved,
				Status:  v1.ConditionTrue,
				Reason:  "AutoApproved",
				Message: "Approved during integration test",
			})
			_, err := testuserClient.CertificatesV1().CertificateSigningRequests().UpdateApproval(context.TODO(), csr.Name, csr, metav1.UpdateOptions{})
			if err != nil && test.error != err.Error() {
				t.Errorf("expected error %q but got: %v", test.error, err)
			}
			if err == nil && test.error != "" {
				t.Errorf("expected to get an error %q but got none", test.error)
			}
		})
	}
}

func grantUserPermissionToApproveFor(t *testing.T, client clientset.Interface, username string, signerNames ...string) {
	resourceName := "signername-" + username
	cr := buildApprovalClusterRoleForSigners(resourceName, signerNames...)
	crb := buildClusterRoleBindingForUser(resourceName, username, cr.Name)
	if _, err := client.RbacV1().ClusterRoles().Create(context.TODO(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("unable to create test fixture RBAC rules: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Create(context.TODO(), crb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("unable to create test fixture RBAC rules: %v", err)
	}
	approveRule := cr.Rules[0]
	updateRule := cr.Rules[1]
	authutil.WaitForNamedAuthorizationUpdate(t, context.TODO(), client.AuthorizationV1(), username, "", approveRule.Verbs[0], approveRule.ResourceNames[0], schema.GroupResource{Group: approveRule.APIGroups[0], Resource: approveRule.Resources[0]}, true)
	authutil.WaitForNamedAuthorizationUpdate(t, context.TODO(), client.AuthorizationV1(), username, "", updateRule.Verbs[0], "", schema.GroupResource{Group: updateRule.APIGroups[0], Resource: updateRule.Resources[0]}, true)
}

func buildApprovalClusterRoleForSigners(name string, signerNames ...string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Rules: []rbacv1.PolicyRule{
			// 审批指定 signerName 的 CSR，需要对相应 signer 具备 approve 权限。
			{
				Verbs:         []string{"approve"},
				APIGroups:     []string{certv1.SchemeGroupVersion.Group},
				Resources:     []string{"signers"},
				ResourceNames: signerNames,
			},
			{
				Verbs:     []string{"update"},
				APIGroups: []string{certv1.SchemeGroupVersion.Group},
				Resources: []string{"certificatesigningrequests/approval"},
			},
		},
	}
}

func buildClusterRoleBindingForUser(name, username, clusterRoleName string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind: rbacv1.UserKind,
				Name: username,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.SchemeGroupVersion.Group,
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
		},
	}
}
