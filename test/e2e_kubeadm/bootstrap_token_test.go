/*
Copyright 2019 The Kubernetes Authors.

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

package kubeadm

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	bootstrapTokensGroup                             = "system:bootstrappers:kubeadm:default-node-token"
	bootstrapTokensAllowPostCSRClusterRoleBinding    = "kubeadm:kubelet-bootstrap"
	bootstrapTokensAllowPostCSRClusterRoleName       = "system:node-bootstrapper"
	bootstrapTokensCSRAutoApprovalClusterRoleBinding = "kubeadm:node-autoapprove-bootstrap"
	bootstrapTokensCSRAutoApprovalClusterRoleName    = "system:certificates.k8s.io:certificatesigningrequests:nodeclient"
)

// 定义容器，包含所有用于校验 kubeadm 创建 bootstrap token、
// system:bootstrappers:kubeadm:default-node-token 组以及所有相关 RBAC 规则是否就位的测试规格。

var _ = Describe("bootstrap token", func() {

	// 获取 k8s test framework 实例
	f := framework.NewDefaultFramework("bootstrap token")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 该容器中的测试不应在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行速度
	f.SkipNamespaceCreation = true

	ginkgo.It("should exist and be properly configured", func(ctx context.Context) {
		secrets, err := f.ClientSet.CoreV1().
			Secrets(kubeSystemNamespace).
			List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "error reading Secrets")

		tokenNum := 0
		for _, s := range secrets.Items {
			// 检查额外的 group 和 token 用法，确保至少存在一个 token
			if s.Type == corev1.SecretTypeBootstrapToken && string(s.Data[bootstrapapi.BootstrapTokenExtraGroupsKey]) == bootstrapTokensGroup {
				usageBootstrapAuthentication := string(s.Data[bootstrapapi.BootstrapTokenUsageAuthentication])
				usageBootstrapSigning := string(s.Data[bootstrapapi.BootstrapTokenUsageSigningKey])
				gomega.Expect(usageBootstrapAuthentication).Should(gomega.Equal("true"), "the bootstrap token should be able to be used for authentication")
				gomega.Expect(usageBootstrapSigning).Should(gomega.Equal("true"), "the bootstrap token should be able to be used for signing")
				tokenNum++
			}
		}
		gomega.Expect(tokenNum).Should(gomega.BeNumerically(">", 0), "At least one bootstrap token should exist")
	})

	ginkgo.It("should be allowed to post CSR for kubelet certificates on joining nodes", func(ctx context.Context) {
		ExpectClusterRoleBindingWithSubjectAndRole(f.ClientSet,
			bootstrapTokensAllowPostCSRClusterRoleBinding,
			rbacv1.GroupKind, bootstrapTokensGroup,
			bootstrapTokensAllowPostCSRClusterRoleName,
		)
	})

	ginkgo.It("should be allowed to auto approve CSR for kubelet certificates on joining nodes", func(ctx context.Context) {
		ExpectClusterRoleBindingWithSubjectAndRole(f.ClientSet,
			bootstrapTokensCSRAutoApprovalClusterRoleBinding,
			rbacv1.GroupKind, bootstrapTokensGroup,
			bootstrapTokensCSRAutoApprovalClusterRoleName,
		)
	})
})
