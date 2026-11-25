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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	nodesGroup                                 = "system:nodes"
	nodesCertificateRotationClusterRoleName    = "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient"
	nodesCertificateRotationClusterRoleBinding = "kubeadm:node-autoapprove-certificate-rotation"
	nodesCRISocketAnnotation                   = "kubeadm.alpha.kubernetes.io/cri-socket"
)

// 定义容器，容纳所有用于验证 kubeadm 是否按预期配置节点和 system:nodes 组的测试用例
var _ = Describe("nodes", func() {

	// 获取 k8s 测试框架实例
	f := framework.NewDefaultFramework("nodes")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 该容器中的测试不会在集群里创建对象，
	// 因此禁用 namespace 创建以加快执行速度
	f.SkipNamespaceCreation = true

	ginkgo.It("should have CRI annotation", func(ctx context.Context) {
		nodes, err := f.ClientSet.CoreV1().Nodes().
			List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "error reading nodes")

		nodeLocalCRISocketEnabled := true
		cc := getClusterConfiguration(f.ClientSet)
		if _, ok := cc["featureGates"]; ok {
			fgCC := cc["featureGates"].(map[interface{}]interface{})
			if fg, ok := fgCC["NodeLocalCRISocket"]; ok {
				nodeLocalCRISocketEnabled = fg.(bool)
			}
		}

		// 检查节点是否带有 CRI socket 注解，且注解值以 URL scheme 为前缀
		for _, node := range nodes.Items {
			if !nodeLocalCRISocketEnabled {
				gomega.Expect(node.Annotations).To(gomega.HaveKey(nodesCRISocketAnnotation))
				gomega.Expect(node.Annotations[nodesCRISocketAnnotation]).To(gomega.HavePrefix("unix://"))
			}
		}
	})

	ginkgo.It("should be allowed to rotate CSR", func(ctx context.Context) {
		// 注意：严格来说，这是 bootstrap-token 阶段的一部分
		ExpectClusterRoleBindingWithSubjectAndRole(f.ClientSet,
			nodesCertificateRotationClusterRoleBinding,
			rbacv1.GroupKind, nodesGroup,
			nodesCertificateRotationClusterRoleName,
		)
	})
})
