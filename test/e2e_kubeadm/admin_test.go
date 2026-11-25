/*
Copyright 2023 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/version"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
)

const (
	kubeadmClusterAdminsGroupAndCRB = "kubeadm:cluster-admins"
	clusterAdminClusterRole         = "cluster-admin"
)

// 为所有旨在验证管理员
// 是否拥有 "cluster-admin" 级别权限的测试定义一个容器。
var _ = Describe("admin", func() {

	// 获取 k8s 测试框架实例
	f := framework.NewDefaultFramework("admin")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 此容器中的测试预期不会在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行
	f.SkipNamespaceCreation = true

	ginkgo.It("kubeadm:cluster-admins CRB must exist and be binding the cluster-admin ClusterRole"+
		" to the kubeadm:cluster-admins Group", func(ctx context.Context) {

		// 使用 ClusterConfiguration.kubernetesVersion 决定是否查找 CRB。
		// 它在 kubeadm v1.29-pre 中加入，但 kubeadm 与 control plane 之间
		// 最多支持 N-1 的版本偏差，因此只要 kubernetesVersion 与
		// kubeadm 版本匹配就没问题，否则跳过此测试。
		m := getClusterConfiguration(f.ClientSet)
		const verKey = "kubernetesVersion"
		verInterface, exists := m[verKey]
		if !exists {
			framework.Failf("the kubeadm ConfigMap %s is missing a %s key in %s",
				kubeadmConfigName,
				verKey,
				kubeadmConfigClusterConfigurationConfigMapKey)
		}

		// 解析版本字符串
		verStr, ok := verInterface.(string)
		if !ok {
			framework.Failf("cannot cast %s to string", verKey)
		}
		ver, err := version.ParseSemantic(verStr)
		if err != nil {
			framework.Failf("could not parse the %s key: %v", verKey, err)
		}

		// 如果版本早于该特性加入时的版本，则跳过此测试
		minVer := version.MustParseSemantic("v1.29.0-alpha.2.188+05076de57fc49f")
		if !ver.AtLeast(minVer) {
			e2eskipper.Skipf("Skipping because version %s is older than the minimum version %s",
				ver,
				minVer)
		}

		// 期望 CRB 存在
		ExpectClusterRoleBindingWithSubjectAndRole(f.ClientSet,
			kubeadmClusterAdminsGroupAndCRB,
			rbacv1.GroupKind, kubeadmClusterAdminsGroupAndCRB,
			clusterAdminClusterRole,
		)
	})
})
