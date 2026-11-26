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

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	kubeletConfigConfigMapKey = "kubelet"
)

var (
	kubeletConfigConfigMapName   string
	kubeletConfigRoleName        string
	kubeletConfigRoleBindingName string
)

// 定义容器，包含所有用于校验 kubeadm 创建 kubelet-config ConfigMap、
// 其配置正确以及所有相关 RBAC 规则就位的测试规格。

var _ = Describe("kubelet-config ConfigMap", func() {

	// 获取 k8s test framework 实例
	f := framework.NewDefaultFramework("kubelet-config")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 该容器中的测试不应在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行速度
	f.SkipNamespaceCreation = true

	// kubelet-config map 名以 kubernetesVersion 为后缀，因此
	// 测试前必须从 kubeadm-config ConfigMap 获取它
	ginkgo.BeforeEach(func() {
		// 如果已知 kubelet-config map 名则直接退出
		if kubeletConfigConfigMapName != "" {
			return
		}

		kubeletConfigConfigMapName = "kubelet-config"
		kubeletConfigRoleName = "kubeadm:kubelet-config"
		kubeletConfigRoleBindingName = kubeletConfigRoleName
	})

	ginkgo.It("should exist and be properly configured", func(ctx context.Context) {
		cm := GetConfigMap(f.ClientSet, kubeSystemNamespace, kubeletConfigConfigMapName)
		gomega.Expect(cm.Data).To(gomega.HaveKey(kubeletConfigConfigMapKey))
	})

	ginkgo.It("should have related Role and RoleBinding", func(ctx context.Context) {
		ExpectRole(f.ClientSet, kubeSystemNamespace, kubeletConfigRoleName)
		ExpectRoleBinding(f.ClientSet, kubeSystemNamespace, kubeletConfigRoleBindingName)
	})

	ginkgo.It("should be accessible for bootstrap tokens", func(ctx context.Context) {
		ExpectSubjectHasAccessToResource(f.ClientSet,
			rbacv1.GroupKind, bootstrapTokensGroup,
			kubeadmConfigConfigMapResource,
		)
	})

	ginkgo.It("should be accessible for nodes", func(ctx context.Context) {
		ExpectSubjectHasAccessToResource(f.ClientSet,
			rbacv1.GroupKind, nodesGroup,
			kubeadmConfigConfigMapResource,
		)
	})
})
