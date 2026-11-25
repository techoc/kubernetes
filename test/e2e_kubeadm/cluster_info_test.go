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

	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	clusterInfoConfigMapName   = "cluster-info"
	clusterInfoRoleName        = "kubeadm:bootstrap-signer-clusterinfo"
	clusterInfoRoleBindingName = clusterInfoRoleName
)

var (
	clusterInfoConfigMapResource = &authv1.ResourceAttributes{
		Namespace: kubePublicNamespace,
		Name:      clusterInfoConfigMapName,
		Resource:  "configmaps",
		Verb:      "get",
	}
)

// 为所有旨在验证以下内容的测试规格定义一个容器：
// kubeadm 创建了 cluster-info ConfigMap，其配置正确，
// 且所有相关 RBAC 规则都已就位
var _ = Describe("cluster-info ConfigMap", func() {

	// 获取 k8s 测试框架实例
	f := framework.NewDefaultFramework("cluster-info")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 此容器中的测试预期不会在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行
	f.SkipNamespaceCreation = true

	ginkgo.It("should exist and be properly configured", func(ctx context.Context) {
		// Nb. 从技术上讲，这是 bootstrap-token 阶段的一部分
		cm := GetConfigMap(f.ClientSet, kubePublicNamespace, clusterInfoConfigMapName)

		gomega.Expect(cm.Data).To(gomega.HaveKey(gomega.HavePrefix(bootstrapapi.JWSSignatureKeyPrefix)))
		gomega.Expect(cm.Data).To(gomega.HaveKey(bootstrapapi.KubeConfigKey))

		//TODO: What else? server?
	})

	ginkgo.It("should have related Role and RoleBinding", func(ctx context.Context) {
		// Nb. 从技术上讲，这是 bootstrap-token 阶段的一部分
		ExpectRole(f.ClientSet, kubePublicNamespace, clusterInfoRoleName)
		ExpectRoleBinding(f.ClientSet, kubePublicNamespace, clusterInfoRoleBindingName)
	})

	ginkgo.It("should be accessible for anonymous", func(ctx context.Context) {
		ExpectSubjectHasAccessToResource(f.ClientSet,
			rbacv1.UserKind, anonymousUser,
			clusterInfoConfigMapResource,
		)
	})
})
