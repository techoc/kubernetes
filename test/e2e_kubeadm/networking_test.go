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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	admissionapi "k8s.io/pod-security-admission/api"
	netutils "k8s.io/utils/net"

	"github.com/onsi/ginkgo/v2"
)

var (
	dualStack                    bool
	podSubnetInKubeadmConfig     bool
	serviceSubnetInKubeadmConfig bool
)

// 为所有旨在验证以下内容的测试规格定义一个容器：
// 验证 kubeadm 按预期配置了网络。
// 如需跳过此测试，请使用 SKIP=setup-networking
var _ = Describe("networking [setup-networking]", func() {

	// 获取 k8s 测试框架实例
	f := framework.NewDefaultFramework("networking")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 此容器中的测试预期不会在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行
	f.SkipNamespaceCreation = true

	// 测试要么是单栈（IPv4 或 IPv6 地址），要么是双栈
	// 我们可以通过查看 kubeadm config 中
	// networking.podSubnet 指定的 IP 数量来判断
	ginkgo.BeforeEach(func() {
		// 从 kubeadm kubeadm-config ConfigMap 中以无类型 map 形式获取 ClusterConfiguration
		cc := getClusterConfiguration(f.ClientSet)
		// 提取 networking.podSubnet
		// 请注意，此测试假定用户不会通过
		// extraArgs 选项修改网络配置。
		if _, ok := cc["networking"]; ok {
			netCC := cc["networking"].(map[interface{}]interface{})
			if ps, ok := netCC["podSubnet"]; ok {
				// 如果未指定 podSubnet，将跳过 podSubnet 相关用例。
				// 注意 kubeadm 目前不会为 PodSubnet 应用默认值，因此跳过。
				podSubnetInKubeadmConfig = true
				cidrs := strings.Split(ps.(string), ",")
				if len(cidrs) > 1 {
					dualStack = true
				}
			}
			if _, ok := netCC["serviceSubnet"]; ok {
				// 如果未指定 serviceSubnet，将跳过 serviceSubnet 相关用例。
				serviceSubnetInKubeadmConfig = true
			}
		}
	})

	ginkgo.Context("single-stack", func() {
		ginkgo.Context("podSubnet", func() {
			ginkgo.It("should be properly configured if specified in kubeadm-config", func(ctx context.Context) {
				if dualStack {
					e2eskipper.Skipf("Skipping because cluster is dual-stack")
				}
				if !podSubnetInKubeadmConfig {
					e2eskipper.Skipf("Skipping because podSubnet was not specified in kubeadm-config")
				}
				cc := getClusterConfiguration(f.ClientSet)
				if _, ok := cc["networking"]; ok {
					netCC := cc["networking"].(map[interface{}]interface{})
					if ps, ok := netCC["podSubnet"]; ok {
						// 检查分配给节点的 pod CIDR 是否在 kubeadm-config 的 podCIDR 范围内。
						nodes, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
						framework.ExpectNoError(err, "error listing nodes")
						for _, node := range nodes.Items {
							if !subnetWithinSubnet(ps.(string), node.Spec.PodCIDR) {
								framework.Failf("failed due to node(%v) IP %v not inside configured pod subnet: %s", node.Name, node.Spec.PodCIDR, ps)
							}
						}
					}
				}
			})
		})
		ginkgo.Context("serviceSubnet", func() {
			ginkgo.It("should be properly configured if specified in kubeadm-config", func(ctx context.Context) {
				if dualStack {
					e2eskipper.Skipf("Skipping because cluster is dual-stack")
				}
				if !serviceSubnetInKubeadmConfig {
					e2eskipper.Skipf("Skipping because serviceSubnet was not specified in kubeadm-config")
				}
				cc := getClusterConfiguration(f.ClientSet)
				if _, ok := cc["networking"]; ok {
					netCC := cc["networking"].(map[interface{}]interface{})
					if ss, ok := netCC["serviceSubnet"]; ok {
						// 获取默认命名空间中的 kubernetes service。
						// 检查分配的 service CIDR 是否在 serviceSubnet 范围内。
						svc, err := f.ClientSet.CoreV1().Services("default").Get(ctx, "kubernetes", metav1.GetOptions{})
						framework.ExpectNoError(err, "error getting Service %q from namespace %q", "kubernetes", "default")
						if !ipWithinSubnet(ss.(string), svc.Spec.ClusterIP) {
							framework.Failf("failed due to service(%v) cluster-IP %v not inside configured service subnet: %s", svc.Name, svc.Spec.ClusterIP, ss)
						}
					}
				}
			})
		})
	})
	ginkgo.Context("dual-stack", func() {
		ginkgo.Context("podSubnet", func() {
			ginkgo.It("should be properly configured if specified in kubeadm-config", func(ctx context.Context) {
				if !dualStack {
					e2eskipper.Skipf("Skipping because cluster is not dual-stack")
				}
				if !podSubnetInKubeadmConfig {
					e2eskipper.Skipf("Skipping because podSubnet was not specified in kubeadm-config")
				}
				cc := getClusterConfiguration(f.ClientSet)
				if _, ok := cc["networking"]; ok {
					netCC := cc["networking"].(map[interface{}]interface{})
					if ps, ok := netCC["podSubnet"]; ok {
						nodes, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
						framework.ExpectNoError(err, "error listing nodes")
						// 检查分配给节点的 pod CIDR 是否在 kubeadm-config 的 podCIDR 范围内。
						var found bool
						configCIDRs := strings.Split(ps.(string), ",")
						for _, node := range nodes.Items {
							for _, nCIDR := range node.Spec.PodCIDRs {
								found = false
								for _, cCIDR := range configCIDRs {
									if subnetWithinSubnet(cCIDR, nCIDR) {
										found = true
										break
									}
								}
								if !found {
									framework.Failf("failed due to the PodCIDRs (%v) of Node %q not being inside the configuration podSubnet CIDR %q", node.Spec.PodCIDRs, node.Name, configCIDRs)
								}
							}
						}
					}
				}
			})
		})
	})
})

// 如果 IP（targetIP）落在参考子网（refIPNet）内，ipWithinSubnet 返回 true
func ipWithinSubnet(refIPNet, targetIP string) bool {
	_, rNet, _ := netutils.ParseCIDRSloppy(refIPNet)
	tIP := netutils.ParseIPSloppy(targetIP)
	return rNet.Contains(tIP)
}

// 如果子网（targetNet）落在参考子网（refIPNet）内，subnetWithinSubnet 返回 true
func subnetWithinSubnet(refIPNet, targetNet string) bool {
	_, rNet, _ := netutils.ParseCIDRSloppy(refIPNet)
	tNet, _, _ := netutils.ParseCIDRSloppy(targetNet)
	return rNet.Contains(tNet)
}
