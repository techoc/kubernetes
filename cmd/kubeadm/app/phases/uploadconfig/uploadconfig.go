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

package uploadconfig

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
)

const (
	// NodesKubeadmConfigClusterRoleName 设置 ClusterRole 的名称，该角色允许引导令牌
	// 在节点引导或发现期间以及节点升级期间访问 kubeadm-config ConfigMap。
	NodesKubeadmConfigClusterRoleName = "kubeadm:nodes-kubeadm-config"
)

// UploadConfiguration 保存 InitConfiguration，供后续升级等场景参考。
func UploadConfiguration(cfg *kubeadmapi.InitConfiguration, client clientset.Interface) error {
	fmt.Printf("[upload-config] Storing the configuration used in ConfigMap %q in the %q Namespace\n", kubeadmconstants.KubeadmConfigConfigMap, metav1.NamespaceSystem)

	// 准备用于上传的 ClusterConfiguration。
	// 各组件将配置保存在各自的 ConfigMap 中，然后重置 .ComponentConfig 结构体。
	// 为避免修改 cfg 本身，先通过 .DeepCopy 创建副本。
	clusterConfigurationToUpload := cfg.ClusterConfiguration.DeepCopy()
	clusterConfigurationToUpload.ComponentConfigs = kubeadmapi.ComponentConfigMap{}

	// 必要时将已解析的 Kubernetes 版本恢复为 CI Kubernetes 版本。
	if len(clusterConfigurationToUpload.CIKubernetesVersion) > 0 {
		clusterConfigurationToUpload.KubernetesVersion = clusterConfigurationToUpload.CIKubernetesVersion
	}

	// 将 ClusterConfiguration 序列化为 YAML。
	clusterConfigurationYaml, err := configutil.MarshalKubeadmConfigObject(clusterConfigurationToUpload, kubeadmapiv1.SchemeGroupVersion)
	if err != nil {
		return err
	}

	err = apiclient.CreateOrMutate(client.CoreV1().ConfigMaps(metav1.NamespaceSystem), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeadmconstants.KubeadmConfigConfigMap,
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			kubeadmconstants.ClusterConfigurationConfigMapKey: string(clusterConfigurationYaml),
		},
	}, func(cm *v1.ConfigMap) error {
		// Upgrade 会以修改后的 KubernetesVersion 调用 UploadConfiguration，以反映新的 Kubernetes 版本。
		// 此时将执行修改路径。
		cm.Data[kubeadmconstants.ClusterConfigurationConfigMapKey] = string(clusterConfigurationYaml)
		return nil
	})
	if err != nil {
		return err
	}

	// 确保 NodesKubeadmConfigClusterRoleName 存在。
	err = apiclient.CreateOrUpdate(client.RbacV1().Roles(metav1.NamespaceSystem), &rbac.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NodesKubeadmConfigClusterRoleName,
			Namespace: metav1.NamespaceSystem,
		},
		Rules: []rbac.PolicyRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{kubeadmconstants.KubeadmConfigConfigMap},
			},
		},
	})
	if err != nil {
		return err
	}

	// 将 NodesKubeadmConfigClusterRoleName 绑定到 system:bootstrappers:kubeadm:default-node-token 组中的
	// 所有引导令牌，以及所有节点。
	return apiclient.CreateOrUpdate(client.RbacV1().RoleBindings(metav1.NamespaceSystem), &rbac.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NodesKubeadmConfigClusterRoleName,
			Namespace: metav1.NamespaceSystem,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "Role",
			Name:     NodesKubeadmConfigClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: kubeadmconstants.NodeBootstrapTokenAuthGroup,
			},
			{
				Kind: rbac.GroupKind,
				Name: kubeadmconstants.NodesGroup,
			},
		},
	})
}
