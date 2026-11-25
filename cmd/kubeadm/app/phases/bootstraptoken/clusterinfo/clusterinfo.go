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

package clusterinfo

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// BootstrapSignerClusterRoleName 设置可访问 kube-public 命名空间中 ConfigMap 的 ClusterRole 名称。
	BootstrapSignerClusterRoleName = "kubeadm:bootstrap-signer-clusterinfo"
)

// CreateBootstrapConfigMapIfNotExists 在 kube-public ConfigMap 不存在时创建它。
func CreateBootstrapConfigMapIfNotExists(client clientset.Interface, kubeconfig *clientcmdapi.Config) error {

	fmt.Printf("[bootstrap-token] Creating the %q ConfigMap in the %q namespace\n", bootstrapapi.ConfigMapClusterInfo, metav1.NamespacePublic)

	// 克隆 kubeconfig，避免原对象被修改。
	adminConfig := kubeconfig.DeepCopy()
	if err := clientcmdapi.FlattenConfig(adminConfig); err != nil {
		return err
	}

	if adminConfig.Contexts[adminConfig.CurrentContext] == nil {
		return errors.New("invalid kubeconfig")
	}

	adminCluster := adminConfig.Contexts[adminConfig.CurrentContext].Cluster
	// 将 admin.conf 中的集群信息复制到引导 kubeconfig，包括 CA 证书和服务器 URL。
	klog.V(1).Infoln("[bootstrap-token] copying the cluster from admin.conf to the bootstrap kubeconfig")
	bootstrapConfig := &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"": adminConfig.Clusters[adminCluster],
		},
	}
	bootstrapBytes, err := clientcmd.Write(*bootstrapConfig)
	if err != nil {
		return err
	}

	// 在 kube-public 命名空间中创建或更新 ConfigMap。
	klog.V(1).Infoln("[bootstrap-token] creating/updating ConfigMap in kube-public namespace")
	return apiclient.CreateOrUpdate(client.CoreV1().ConfigMaps(metav1.NamespacePublic), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bootstrapapi.ConfigMapClusterInfo,
			Namespace: metav1.NamespacePublic,
		},
		Data: map[string]string{
			bootstrapapi.KubeConfigKey: string(bootstrapBytes),
		},
	})
}

// CreateClusterInfoRBACRules 创建 RBAC 规则，允许未认证用户访问 kube-public 命名空间中的 cluster-info ConfigMap。
func CreateClusterInfoRBACRules(client clientset.Interface) error {
	klog.V(1).Infoln("creating the RBAC rules for exposing the cluster-info ConfigMap in the kube-public namespace")
	err := apiclient.CreateOrUpdate(client.RbacV1().Roles(metav1.NamespacePublic), &rbac.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BootstrapSignerClusterRoleName,
			Namespace: metav1.NamespacePublic,
		},
		Rules: []rbac.PolicyRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{bootstrapapi.ConfigMapClusterInfo},
			},
		},
	})
	if err != nil {
		return err
	}

	return apiclient.CreateOrUpdate(client.RbacV1().RoleBindings(metav1.NamespacePublic), &rbac.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BootstrapSignerClusterRoleName,
			Namespace: metav1.NamespacePublic,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "Role",
			Name:     BootstrapSignerClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.UserKind,
				Name: user.Anonymous,
			},
		},
	})
}
