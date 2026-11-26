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

	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	dnsService = "kube-dns"

	coreDNSServiceAccountName = "coredns"
	coreDNSConfigMap          = "coredns"
	coreDNSConfigMapKey       = "Corefile"
	coreDNSRoleName           = "system:coredns"
	coreDNSRoleBindingName    = coreDNSRoleName
	coreDNSDeploymentName     = "coredns"
)

// 为所有旨在验证 kubeadm 是否按预期配置 DNS addon 的
// 测试定义 container
var _ = Describe("DNS addon", func() {

	// 获取 k8s test framework 的实例
	f := framework.NewDefaultFramework("DNS")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 这个 container 中的测试不应在集群中创建新对象，
	// 所以禁用 namespace 创建以加快执行
	f.SkipNamespaceCreation = true

	ginkgo.Context("CoreDNS", func() {
		ginkgo.Context("CoreDNS ServiceAccount", func() {
			ginkgo.It("should exist", func(ctx context.Context) {
				ExpectServiceAccount(f.ClientSet, kubeSystemNamespace, coreDNSServiceAccountName)
			})

			ginkgo.It("should have related ClusterRole and ClusterRoleBinding", func(ctx context.Context) {
				ExpectClusterRole(f.ClientSet, coreDNSRoleName)
				ExpectClusterRoleBinding(f.ClientSet, coreDNSRoleBindingName)
			})
		})

		ginkgo.Context("CoreDNS ConfigMap", func() {
			ginkgo.It("should exist and be properly configured", func(ctx context.Context) {
				cm := GetConfigMap(f.ClientSet, kubeSystemNamespace, coreDNSConfigMap)
				gomega.Expect(cm.Data).To(gomega.HaveKey(coreDNSConfigMapKey))
			})
		})

		ginkgo.Context("CoreDNS Deployment", func() {
			ginkgo.It("should exist and be properly configured", func(ctx context.Context) {
				d := GetDeployment(f.ClientSet, kubeSystemNamespace, coreDNSDeploymentName)
				gomega.Expect(d.Spec.Template.Spec.ServiceAccountName).To(gomega.Equal(coreDNSServiceAccountName))
			})
		})
	})

	ginkgo.Context("DNS Service", func() {
		ginkgo.It("should exist", func(ctx context.Context) {
			ExpectService(f.ClientSet, kubeSystemNamespace, dnsService)
		})
	})
})
