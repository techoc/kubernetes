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

// 本测试确保整个 GMSA 流程按预期工作。
// 但正确运行它需要相当繁重的准备，尤其是对所运行的集群有如下假设：
//  * 存在一个带 agentpool=windowsgmsa 标签的 Windows worker 节点。
//  * 该节点已加入一个可正常工作的 Active Directory 域。
//  * 已在该 AD 域中创建 GMSA 账户，并在同一 worker 上安装。
//  * 已使用
//    https://github.com/kubernetes-sigs/windows-gmsa/blob/master/scripts/GenerateCredentialSpecResource.ps1
//    根据该 GMSA 账户的 credential spec 生成一个有效的 k8s manifest 文件，
//    其中包含单个 CRD 定义，类型为 GMSACredentialSpec、名称为 gmsa-e2e；
//    并且该 manifest 文件已写入同一 worker 节点的 C:\gmsa\gmsa-cred-spec-gmsa-e2e.yml。
//  * API 已启用 MutatingAdmissionWebhook 和 ValidatingAdmissionWebhook admission controller。
//  * 集群中至少有一个可承载工作负载的 Linux 节点，可以是 master，也可以是其他 Linux 节点，
//    用于运行 webhook 的 Pod。
//  * 为运行 "can read and write file to remote folder" 测试用例，需要在该 AD 域中
//    创建一个文件夹（如 "write_test"）并共享给该 GMSA 账户。
// 我们每日运行 e2e 测试的 AKS 集群在搭建时会通过 AKS 扩展满足以上全部假设，
// 但这也使得在该特定环境之外运行此测试非常困难。

package windows

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// gmsaFullNodeLabel 是我们期望在至少一个节点上找到的标签，
	// 该节点应满足上述全部条件。
	gmsaFullNodeLabel = "agentpool=windowsgmsa"

	// gmsaCrdManifestPath 是该节点上 GMSA cred spec manifest 文件的预期位置，见上文说明。
	gmsaCrdManifestPath = `C:\gmsa\gmsa-cred-spec-gmsa-e2e.yml`

	// gmsaCustomResourceName 是 gmsaCrdManifestPath 中定义的 GMSA custom resource 的预期名称。
	gmsaCustomResourceName = "gmsa-e2e"

	// gmsaWebhookDeployScriptURL 是 GMSA webhook 部署脚本的 URL。
	gmsaWebhookDeployScriptURL = "https://raw.githubusercontent.com/kubernetes-sigs/windows-gmsa/master/admission-webhook/deploy/deploy-gmsa-webhook.sh"

	// nltest /query 命令的输出中应包含此内容。
	expectedQueryOutput = "The command completed successfully"

	// 预期域名。
	gmsaDomain = "k8sgmsa.lan"

	// 预期域中用于文件写入测试的共享文件夹。
	gmsaSharedFolder = "write_test"
)

var _ = sigDescribe(feature.Windows, "GMSA Full", framework.WithSerial(), framework.WithSlow(), skipUnlessWindows(func() {
	f := framework.NewDefaultFramework("gmsa-full-test-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("GMSA support", func() {
		ginkgo.It("works end to end", func(ctx context.Context) {
			defer ginkgo.GinkgoRecover()

			ginkgo.By("finding the worker node that fulfills this test's assumptions")
			nodes := findPreconfiguredGmsaNodes(ctx, f.ClientSet)
			if len(nodes) != 1 {
				e2eskipper.Skipf("Expected to find exactly one node with the %q label, found %d", gmsaFullNodeLabel, len(nodes))
			}
			node := nodes[0]

			ginkgo.By("retrieving the contents of the GMSACredentialSpec custom resource manifest from the node")
			crdManifestContents := retrieveCRDManifestFileContents(ctx, f, node)

			ginkgo.By("deploying the GMSA webhook")
			err := deployGmsaWebhook(ctx, f)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating the GMSA custom resource")
			err = createGmsaCustomResource(f.Namespace.Name, crdManifestContents)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating an RBAC role to grant use access to that GMSA resource")
			rbacRoleName, err := createRBACRoleForGmsa(ctx, f)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating a service account")
			serviceAccountName := createServiceAccount(ctx, f)

			ginkgo.By("binding the RBAC role to the service account")
			bindRBACRoleToServiceAccount(ctx, f, serviceAccountName, rbacRoleName)

			ginkgo.By("creating a pod using the GMSA cred spec")
			podName := createPodWithGmsa(ctx, f, serviceAccountName)

			// nltest /QUERY 只有在已配置 GMSA 身份，且成功联系 AD controller
			// 并通过其认证时才会成功返回。
			ginkgo.By("checking that nltest /QUERY returns successfully")
			var output string
			gomega.Eventually(ctx, func() error {
				output, err = runKubectlExecInNamespace(f.Namespace.Name, podName, "--", "nltest", "/QUERY")
				if err != nil {
					return fmt.Errorf("unable to run command in container via exec: %w", err)
				}

				if !isValidOutput(output) {
					// 尝试运行 reset 命令修复安全通道。
					// https://kubernetes.io/docs/tasks/configure-pod-container/configure-gmsa/#troubleshooting
					output, err = runKubectlExecInNamespace(f.Namespace.Name, podName, "--", "nltest", fmt.Sprintf("/sc_reset:%s", gmsaDomain))
					if err != nil {
						return fmt.Errorf("unable to run command in container via exec: %w", err)
					}
					return fmt.Errorf("failed to connect to domain; tried resetting the domain, output:\n%v", string(output))
				}
				return nil
			}, 1*time.Minute, 1*time.Second).Should(gomega.Succeed())
		})

		ginkgo.It("can read and write file to remote SMB folder", func(ctx context.Context) {
			defer ginkgo.GinkgoRecover()

			ginkgo.By("finding the worker node that fulfills this test's assumptions")
			nodes := findPreconfiguredGmsaNodes(ctx, f.ClientSet)
			if len(nodes) != 1 {
				e2eskipper.Skipf("Expected to find exactly one node with the %q label, found %d", gmsaFullNodeLabel, len(nodes))
			}
			node := nodes[0]

			ginkgo.By("retrieving the contents of the GMSACredentialSpec custom resource manifest from the node")
			crdManifestContents := retrieveCRDManifestFileContents(ctx, f, node)

			ginkgo.By("deploying the GMSA webhook")
			err := deployGmsaWebhook(ctx, f)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating the GMSA custom resource")
			err = createGmsaCustomResource(f.Namespace.Name, crdManifestContents)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating an RBAC role to grant use access to that GMSA resource")
			rbacRoleName, err := createRBACRoleForGmsa(ctx, f)
			if err != nil {
				framework.Fail(err.Error())
			}

			ginkgo.By("creating a service account")
			serviceAccountName := createServiceAccount(ctx, f)

			ginkgo.By("binding the RBAC role to the service account")
			bindRBACRoleToServiceAccount(ctx, f, serviceAccountName, rbacRoleName)

			ginkgo.By("creating a pod using the GMSA cred spec")
			podName := createPodWithGmsa(ctx, f, serviceAccountName)

			ginkgo.By("getting the ip of GMSA domain")
			gmsaDomainIP := getGmsaDomainIP(f, podName)

			ginkgo.By("checking that file can be read and write from the remote folder successfully")
			filePath := fmt.Sprintf("\\\\%s\\%s\\write-test-%s.txt", gmsaDomainIP, gmsaSharedFolder, string(uuid.NewUUID())[0:4])

			gomega.Eventually(ctx, func() error {
				// filePath 指向远程文件夹，不要改变其格式。
				_, _ = runKubectlExecInNamespace(f.Namespace.Name, podName, "--", "powershell.exe", "-Command", "echo 'This is a test file.' > "+filePath)
				_, err := runKubectlExecInNamespace(f.Namespace.Name, podName, "powershell.exe", "--", "cat", filePath)
				if err != nil {
					return err
				}
				return nil
			}, 1*time.Minute, 1*time.Second).Should(gomega.Succeed())

		})
	})
}))

func isValidOutput(output string) bool {
	return strings.Contains(output, expectedQueryOutput) &&
		!strings.Contains(output, "ERROR_NO_LOGON_SERVERS") &&
		!strings.Contains(output, "RPC_S_SERVER_UNAVAILABLE")
}

// findPreconfiguredGmsaNodes 查找带有 gmsaFullNodeLabel 标签的节点。
func findPreconfiguredGmsaNodes(ctx context.Context, c clientset.Interface) []v1.Node {
	nodeOpts := metav1.ListOptions{
		LabelSelector: gmsaFullNodeLabel,
	}
	nodes, err := c.CoreV1().Nodes().List(ctx, nodeOpts)
	if err != nil {
		framework.Failf("Unable to list nodes: %v", err)
	}
	return nodes.Items
}

// retrieveCRDManifestFileContents 获取节点上 gmsaCrdManifestPath 处的文件内容。
// 做法是在带有 gmsaFullNodeLabel 标签的节点上调度一个 Pod，挂载该文件所在目录，
// 然后 exec 进入该 Pod 读取文件内容。
func retrieveCRDManifestFileContents(ctx context.Context, f *framework.Framework, node v1.Node) string {
	podName := "retrieve-gmsa-crd-contents"
	// 测试自身运行在 Linux 机器上，因此这里不能使用 filepath.Dir。
	splitPath := strings.Split(gmsaCrdManifestPath, `\`)
	dirPath := strings.Join(splitPath[:len(splitPath)-1], `\`)
	volumeName := "retrieve-gmsa-crd-contents-volume"

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			NodeSelector: node.Labels,
			Containers: []v1.Container{
				{
					Name:  podName,
					Image: imageutils.GetPauseImageName(),
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: dirPath,
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: dirPath,
						},
					},
				},
			},
		},
	}
	e2epod.NewPodClient(f).CreateSync(ctx, pod)

	output, err := runKubectlExecInNamespace(f.Namespace.Name, podName, "--", "cmd", "/S", "/C", fmt.Sprintf("type %s", gmsaCrdManifestPath))
	if err != nil {
		framework.Failf("failed to retrieve the contents of %q on node %q: %v", gmsaCrdManifestPath, node.Name, err)
	}

	// 将 Windows 换行转换为 Linux 换行。
	return strings.ReplaceAll(output, "\r\n", "\n")
}

// deployGmsaWebhook 部署 GMSA webhook，返回一个在测试结束后调用的清理函数，
// 用于删除它创建的临时文件和 API 资源。
func deployGmsaWebhook(ctx context.Context, f *framework.Framework) error {
	deployerName := "webhook-deployer"
	deployerNamespace := f.Namespace.Name
	webHookName := "gmsa-webhook"
	webHookNamespace := deployerNamespace + "-webhook"

	// 无论部署是否成功，都尽力清理。
	ginkgo.DeferCleanup(func() {
		framework.Logf("Best effort clean up of the webhook:\n")
		stdout, err := e2ekubectl.RunKubectl("", "delete", "CustomResourceDefinition", "gmsacredentialspecs.windows.k8s.io")
		framework.Logf("stdout:%s\nerror:%s", stdout, err)

		stdout, err = e2ekubectl.RunKubectl("", "delete", "CertificateSigningRequest", fmt.Sprintf("%s.%s", webHookName, webHookNamespace))
		framework.Logf("stdout:%s\nerror:%s", stdout, err)

		stdout, err = runKubectlExecInNamespace(deployerNamespace, deployerName, "--", "kubectl", "delete", "-f", "/manifests.yml")
		framework.Logf("stdout:%s\nerror:%s", stdout, err)
	})

	// 确保 deployer 有权限批准 certificatesigningrequest 以安装 webhook。
	s := createServiceAccount(ctx, f)
	bindClusterRBACRoleToServiceAccount(ctx, f, s, "cluster-admin")

	installSteps := []string{
		"echo \"@community http://dl-cdn.alpinelinux.org/alpine/edge/community/\" >> /etc/apk/repositories",
		"&& apk add kubectl@community gettext openssl",
		"&& apk add --update coreutils",
		fmt.Sprintf("&& curl %s > gmsa.sh", gmsaWebhookDeployScriptURL),
		"&& chmod +x gmsa.sh",
		fmt.Sprintf("&& ./gmsa.sh --file %s --name %s --namespace %s --certs-dir %s --tolerate-master", "/manifests.yml", webHookName, webHookNamespace, "certs"),
		"&& /agnhost pause",
	}
	installCommand := strings.Join(installSteps, " ")

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployerName,
			Namespace: deployerNamespace,
		},
		Spec: v1.PodSpec{
			ServiceAccountName: s,
			NodeSelector: map[string]string{
				"kubernetes.io/os": "linux",
			},
			Containers: []v1.Container{
				{
					Name:    deployerName,
					Image:   imageutils.GetE2EImage(imageutils.Agnhost),
					Command: []string{"bash", "-c"},
					Args:    []string{installCommand},
				},
			},
			Tolerations: []v1.Toleration{
				{
					Operator: v1.TolerationOpExists,
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
		},
	}
	e2epod.NewPodClient(f).CreateSync(ctx, pod)

	// 等待 webhook Deployment 就绪。deployer Pod 需要几秒钟来初始化并创建资源。
	err := waitForDeployment(func() (*appsv1.Deployment, error) {
		return f.ClientSet.AppsV1().Deployments(webHookNamespace).Get(ctx, webHookName, metav1.GetOptions{})
	}, 10*time.Second, f.Timeouts.PodStart)
	if err == nil {
		framework.Logf("GMSA webhook successfully deployed")
	} else {
		err = fmt.Errorf("GMSA webhook did not become ready: %w", err)
	}

	// 输出 deployer 日志。
	logs, _ := e2epod.GetPodLogs(ctx, f.ClientSet, deployerNamespace, deployerName, deployerName)
	framework.Logf("GMSA deployment logs:\n%s", logs)

	return err
}

// createGmsaCustomResource 根据从 worker 节点获取的 manifest 文件内容创建 GMSA API 对象。
// 它返回一个清理函数，测试结束后删除创建的临时文件和 API 对象。
func createGmsaCustomResource(ns string, crdManifestContents string) error {
	tempFile, err := os.CreateTemp("", "")
	if err != nil {
		return fmt.Errorf("unable to create temp file: %w", err)
	}
	defer tempFile.Close()

	ginkgo.DeferCleanup(func() {
		e2ekubectl.RunKubectl(ns, "delete", "--filename", tempFile.Name())
		os.Remove(tempFile.Name())
	})

	_, err = tempFile.WriteString(crdManifestContents)
	if err != nil {
		err = fmt.Errorf("unable to write GMSA contents to %q: %w", tempFile.Name(), err)
		return err
	}

	output, err := e2ekubectl.RunKubectl(ns, "apply", "--filename", tempFile.Name())
	if err != nil {
		err = fmt.Errorf("unable to create custom resource, output:\n%s: %w", output, err)
	}

	return err
}

// createRBACRoleForGmsa 创建一个 RBAC ClusterRole，授予对测试 credential spec 的 use 权限。
// 返回角色名，以及测试结束后删除它的函数。
func createRBACRoleForGmsa(ctx context.Context, f *framework.Framework) (string, error) {
	roleName := f.Namespace.Name + "-rbac-role"

	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{"windows.k8s.io"},
				Resources:     []string{"gmsacredentialspecs"},
				Verbs:         []string{"use"},
				ResourceNames: []string{gmsaCustomResourceName},
			},
		},
	}

	ginkgo.DeferCleanup(framework.IgnoreNotFound(f.ClientSet.RbacV1().ClusterRoles().Delete), roleName, metav1.DeleteOptions{})
	_, err := f.ClientSet.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{})
	if err != nil {
		err = fmt.Errorf("unable to create RBAC cluster role %q: %w", roleName, err)
	}

	return roleName, err
}

// createServiceAccount 创建一个 ServiceAccount 并返回其名称。
func createServiceAccount(ctx context.Context, f *framework.Framework) string {
	accountName := f.Namespace.Name + "-sa-" + string(uuid.NewUUID())
	account := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      accountName,
			Namespace: f.Namespace.Name,
		},
	}
	if _, err := f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name).Create(ctx, account, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create service account %q: %v", accountName, err)
	}
	return accountName
}

// bindRBACRoleToServiceAccount 将给定的 RBAC ClusterRole 绑定到给定的 ServiceAccount。
func bindRBACRoleToServiceAccount(ctx context.Context, f *framework.Framework, serviceAccountName, rbacRoleName string) {
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.Namespace.Name + "-rbac-binding",
			Namespace: f.Namespace.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: f.Namespace.Name,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     rbacRoleName,
		},
	}
	_, err := f.ClientSet.RbacV1().RoleBindings(f.Namespace.Name).Create(ctx, binding, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func bindClusterRBACRoleToServiceAccount(ctx context.Context, f *framework.Framework, serviceAccountName, rbacRoleName string) {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.Namespace.Name + "-rbac-binding",
			Namespace: f.Namespace.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: f.Namespace.Name,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     rbacRoleName,
		},
	}
	_, err := f.ClientSet.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

// createPodWithGmsa 使用测试 GMSA cred spec 创建一个 Pod，并返回其名称。
func createPodWithGmsa(ctx context.Context, f *framework.Framework, serviceAccountName string) string {
	podName := "pod-with-gmsa"
	credSpecName := gmsaCustomResourceName

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			ServiceAccountName: serviceAccountName,
			Containers: []v1.Container{
				{
					Name:  podName,
					Image: imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{
						"powershell.exe",
						"-Command",
						"sleep -Seconds 600",
					},
				},
			},
			SecurityContext: &v1.PodSecurityContext{
				WindowsOptions: &v1.WindowsSecurityContextOptions{
					GMSACredentialSpecName: &credSpecName,
				},
			},
		},
	}
	e2epod.NewPodClient(f).CreateSync(ctx, pod)

	return podName
}

func runKubectlExecInNamespace(namespace string, args ...string) (string, error) {
	namespaceOption := fmt.Sprintf("--namespace=%s", namespace)
	return e2ekubectl.RunKubectl(namespace, append([]string{"exec", namespaceOption}, args...)...)
}

func getGmsaDomainIP(f *framework.Framework, podName string) string {
	output, _ := runKubectlExecInNamespace(f.Namespace.Name, podName, "powershell.exe", "--", "nslookup", gmsaDomain)
	re := regexp.MustCompile(`(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}`)
	idx := strings.Index(output, gmsaDomain)

	submatchall := re.FindAllString(output[idx:], -1)
	if len(submatchall) < 1 {
		framework.Logf("fail to get the ip of the gmsa domain")
		return ""
	}
	return submatchall[0]
}
