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

package windows

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = sigDescribe(feature.Windows, "DNS", skipUnlessWindows(func() {

	ginkgo.BeforeEach(func() {
		e2eskipper.SkipUnlessNodeOSDistroIs("windows")
	})

	f := framework.NewDefaultFramework("dns")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	ginkgo.It("should support configurable pod DNS servers", func(ctx context.Context) {

		ginkgo.By("Getting the IP address of the internal Kubernetes service")

		svc, err := f.ClientSet.CoreV1().Services("kube-system").Get(ctx, "kube-dns", metav1.GetOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Preparing a test DNS service with injected DNS names...")
		// 默认 service IP 因集群而异，但始终存在，是很好的 DNS 测试目标
		testInjectedIP := svc.Spec.ClusterIP
		testSearchPath := "default.svc.cluster.local"

		ginkgo.By("Creating a windows pod with dnsPolicy=None and customized dnsConfig...")
		testPod := e2epod.NewAgnhostPod(f.Namespace.Name, "e2e-dns-utils", nil, nil, nil)
		testPod.Spec.DNSPolicy = v1.DNSNone
		testPod.Spec.DNSConfig = &v1.PodDNSConfig{
			Nameservers: []string{testInjectedIP, "1.1.1.1"},
			Searches:    []string{testSearchPath},
		}
		testPod.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		testPod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testPod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("confirming that the pod has a windows label")
		gomega.Expect(testPod.Spec.NodeSelector).To(gomega.HaveKeyWithValue("kubernetes.io/os", "windows"), "pod.spec.nodeSelector")

		framework.Logf("Created pod %v", testPod)
		defer func() {
			framework.Logf("Deleting pod %s...", testPod.Name)
			if err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, testPod.Name, *metav1.NewDeleteOptions(0)); err != nil {
				framework.Failf("Failed to delete pod %s: %v", testPod.Name, err)
			}
		}()
		framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, testPod.Name, f.Namespace.Name), "failed to wait for pod %s to be running", testPod.Name)

		// 这不是最好的"测试"，但是很好的诊断手段，真正的测试见后面的用例。
		ginkgo.By("Calling ipconfig to get debugging info for this pod's DNS and confirm that a dns server 1.1.1.1 can be injected, along with ")
		cmd := []string{"ipconfig", "/all"}
		stdout, _, err := e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
			Command:       cmd,
			Namespace:     f.Namespace.Name,
			PodName:       testPod.Name,
			ContainerName: "agnhost-container",
			CaptureStdout: true,
			CaptureStderr: true,
		})
		framework.ExpectNoError(err)
		framework.Logf("ipconfig /all:\n%s", stdout)

		if !strings.Contains(stdout, "1.1.1.1") {
			framework.Failf("One of the custom DNS options 1.1.1.1, not found in ipconfig /all")
		}

		// 我们已经验证了 DNS 配置被注入……现在确认 curl '错误' 端点会失败，即
		// 设置阴性对照，在运行最终测试之前执行……

		ginkgo.By("Verifying that curl queries FAIL for wrong URLs")

		// 下面的测试用 curl，因为 nslookup 似乎没有正确使用 ndots
		// 理想情况下应使用 powershell 原生的 ResolveDns，但（截至 k8s 1.20）agnhost 镜像中
		// TODO @jayunit100 把 ResolveHost 加入 agnhost 镜像

		cmd = []string{"curl.exe", "-k", "https://kubernetezzzzzzzz:443"}
		stdout, _, err = e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
			Command:       cmd,
			Namespace:     f.Namespace.Name,
			PodName:       testPod.Name,
			ContainerName: "agnhost-container",
			CaptureStdout: true,
			CaptureStderr: true,
		})
		if err == nil {
			framework.Logf("Warning: Somehow the curl command succeeded... The output was \n %v", stdout)
			framework.Failf("Expected a bogus URL query to fail - something is wrong with this test harness, cannot proceed.")
		}

		ginkgo.By("Verifying that injected dns records for 'kubernetes' resolve to the valid ip address")
		cmd = []string{"curl.exe", "-k", "https://kubernetes:443"}
		stdout, _, err = e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
			Command:       cmd,
			Namespace:     f.Namespace.Name,
			PodName:       testPod.Name,
			ContainerName: "agnhost-container",
			CaptureStdout: true,
			CaptureStderr: true,
		})
		framework.Logf("Result of curling the kubernetes service... (Failure ok, only testing for the sake of DNS resolution) %v ... error = %v", stdout, err)

		// 如果主机无法解析，curl 返回错误，否则返回成功结果。
		if err != nil {
			framework.ExpectNoError(err)
		}

		// TODO: 为其他 DNSPolicy 添加更多测试用例。
	})
}))
