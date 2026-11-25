/*
Copyright 2017 The Kubernetes Authors.

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

package node

import (
	"fmt"

	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
)

// AllowBootstrapTokensToPostCSRs 创建 RBAC 规则，使节点引导令牌能够提交 CSR。
func AllowBootstrapTokensToPostCSRs(client clientset.Interface) error {
	fmt.Println("[bootstrap-token] Configured RBAC rules to allow Node Bootstrap tokens to post CSRs in order for nodes to get long term certificate credentials")

	return apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.NodeKubeletBootstrap,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.NodeBootstrapperClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: constants.NodeBootstrapTokenAuthGroup,
			},
		},
	})
}

// AllowBootstrapTokensToGetNodes 创建 RBAC 规则，允许节点引导令牌列出节点。
func AllowBootstrapTokensToGetNodes(client clientset.Interface) error {
	fmt.Println("[bootstrap-token] Configured RBAC rules to allow Node Bootstrap tokens to get nodes")

	if err := apiclient.CreateOrUpdate(client.RbacV1().ClusterRoles(), &rbac.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.GetNodesClusterRoleName,
		},
		Rules: []rbac.PolicyRule{
			{
				Verbs:     []string{"get"},
				APIGroups: []string{""},
				Resources: []string{"nodes"},
			},
		},
	}); err != nil {
		return err
	}

	return apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.GetNodesClusterRoleName,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.GetNodesClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: constants.NodeBootstrapTokenAuthGroup,
			},
		},
	})
}

// AutoApproveNodeBootstrapTokens 创建 RBAC 规则，使 csrapprover 控制器自动批准节点引导令牌的 CSR。
func AutoApproveNodeBootstrapTokens(client clientset.Interface) error {
	fmt.Println("[bootstrap-token] Configured RBAC rules to allow the csrapprover controller automatically approve CSRs from a Node Bootstrap Token")

	// 尽管如此，仍始终创建此 kubeadm 专用绑定。
	return apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.NodeAutoApproveBootstrapClusterRoleBinding,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.CSRAutoApprovalClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: constants.NodeBootstrapTokenAuthGroup,
			},
		},
	})
}

// AutoApproveNodeCertificateRotation 创建 RBAC 规则，使 csrapprover 控制器自动批准节点证书轮换 CSR。
func AutoApproveNodeCertificateRotation(client clientset.Interface) error {
	fmt.Println("[bootstrap-token] Configured RBAC rules to allow certificate rotation for all node client certificates in the cluster")

	return apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.NodeAutoApproveCertificateRotationClusterRoleBinding,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.NodeSelfCSRAutoApprovalClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: constants.NodesGroup,
			},
		},
	})
}

// AllowAPIServerToAccessKubeletAPI 创建 RBAC 规则，允许 API 服务器的 kubelet 客户端访问 kubelet API。
func AllowAPIServerToAccessKubeletAPI(client clientset.Interface) error {
	fmt.Println("[bootstrap-token] Configured RBAC rules to allow the API server kubelet client certificate to access the kubelet API")

	return apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.KubeletAPIAdminClusterRoleBindingName,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.KubeletAPIAdminClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.UserKind,
				Name: constants.APIServerKubeletClientCertCommonName,
			},
		},
	})
}
