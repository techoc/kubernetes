/*
Copyright 2018 The Kubernetes Authors.

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

// 本测试只验证 kubelet 能否正确地将 GMSA 凭据规格传递给运行时。
// 相比覆盖完整 GMSA 流程但需要繁重环境搭建的 gmsa_full，这个测试轻量得多。

package windows

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = sigDescribe(feature.Windows, "GMSA Kubelet", framework.WithSlow(), skipUnlessWindows(func() {
	f := framework.NewDefaultFramework("gmsa-kubelet-test-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("kubelet GMSA support", func() {
		ginkgo.Context("when creating a pod with correct GMSA credential specs", func() {
			ginkgo.It("passes the credential specs down to the Pod's containers", func(ctx context.Context) {
				defer ginkgo.GinkgoRecover()

				podName := "with-correct-gmsa-specs"

				container1Name := "container1"
				podDomain := "acme.com"

				container2Name := "container2"
				container2Domain := "contoso.org"

				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: podName,
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  container1Name,
								Image: imageutils.GetE2EImage(imageutils.BusyBox),
								Command: []string{
									"powershell.exe",
									"-Command",
									"sleep -Seconds 600",
								},
							},
							{
								Name:  container2Name,
								Image: imageutils.GetE2EImage(imageutils.BusyBox),
								Command: []string{
									"powershell.exe",
									"-Command",
									"sleep -Seconds 600",
								},
								SecurityContext: &v1.SecurityContext{
									WindowsOptions: &v1.WindowsSecurityContextOptions{
										GMSACredentialSpec: generateDummyCredSpecs(container2Domain),
									},
								},
							},
						},
						SecurityContext: &v1.PodSecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								GMSACredentialSpec: generateDummyCredSpecs(podDomain),
							},
						},
					},
				}

				ginkgo.By("creating a pod with correct GMSA specs")
				e2epod.NewPodClient(f).CreateSync(ctx, pod)

				ginkgo.By("checking the domain reported by nltest in the containers")
				namespaceOption := fmt.Sprintf("--namespace=%s", f.Namespace.Name)
				for containerName, domain := range map[string]string{
					container1Name: podDomain,
					container2Name: container2Domain,
				} {
					var (
						output string
						err    error
					)

					containerOption := fmt.Sprintf("--container=%s", containerName)
					// 即使是无效凭据，`nltest /PARENTDOMAIN` 也会返回 AD 域，这已足够满足这里的验证目的。
					// 使用 Eventually 是因为 powershell 容器即使被 docker 报告为运行中，
					// 响应请求也往往偏慢。
					gomega.Eventually(ctx, func() error {
						output, err = e2ekubectl.RunKubectl(f.Namespace.Name, "exec", namespaceOption, podName, containerOption, "--", "nltest", "/PARENTDOMAIN")
						if err != nil {
							return err
						}
						return nil
					}, 1*time.Minute, 1*time.Second).Should(gomega.Succeed())

					if !strings.HasPrefix(output, domain) {
						framework.Failf("Expected %q to start with %q", output, domain)
					}

					expectedSubstr := "The command completed successfully"
					if !strings.Contains(output, expectedSubstr) {
						framework.Failf("Expected %q to contain %q", output, expectedSubstr)
					}
				}

				// 若这是 e2e_node 测试，还可以检查用于向 Docker 传递凭据规格的注册表项
				// 是否已清理；但目前 e2e_node 测试尚不支持 Windows。等其支持后应迁移该测试。
			})
		})
	})
}))

func generateDummyCredSpecs(domain string) *string {
	shortName := strings.ToUpper(strings.Split(domain, ".")[0])

	credSpecs := fmt.Sprintf(`{
       "ActiveDirectoryConfig":{
          "GroupManagedServiceAccounts":[
             {
                "Name":"WebApplication",
                "Scope":"%s"
             },
             {
                "Name":"WebApplication",
                "Scope":"%s"
             }
          ]
       },
       "CmsPlugins":[
          "ActiveDirectory"
       ],
       "DomainJoinConfig":{
          "DnsName":"%s",
          "DnsTreeName":"%s",
          "Guid":"244818ae-87ca-4fcd-92ec-e79e5252348a",
          "MachineAccountName":"WebApplication",
          "NetBiosName":"%s",
          "Sid":"S-1-5-21-2126729477-2524175714-3194792973"
       }
    }`, shortName, domain, domain, domain, shortName)

	return &credSpecs
}
