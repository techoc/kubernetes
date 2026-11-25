/*
Copyright 2014 The Kubernetes Authors.

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

package node

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/environment"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Sysctls [LinuxOnly]", framework.WithNodeConformance(), func() {

	ginkgo.BeforeEach(func() {
		// Windows 不支持 sysctl。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
	})

	f := framework.NewDefaultFramework("sysctl")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var podClient *e2epod.PodClient

	testPod := func() *v1.Pod {
		podName := "sysctl-" + string(uuid.NewUUID())
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Annotations: map[string]string{},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "test-container",
						Image: imageutils.GetE2EImage(imageutils.BusyBox),
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		return &pod
	}

	ginkgo.BeforeEach(func() {
		podClient = e2epod.NewPodClient(f)
	})

	/*
	  Release: v1.21
	  Testname: Sysctl, test sysctls
	  Description: 创建设置了 kernel.shm_rmid_forced sysctl 的 Pod。kernel.shm_rmid_forced 必须设为 1
	  [LinuxOnly]: 该测试标记为 LinuxOnly，因为 Windows 不支持 sysctls
	  [Environment:NotInUserNS]: 该测试在 UserNS 中会失败（符合预期）：`open /proc/sys/kernel/shm_rmid_forced: permission denied`
	*/
	framework.ConformanceIt("should support sysctls", environment.NotInUserNS, func(ctx context.Context) {
		pod := testPod()
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			Sysctls: []v1.Sysctl{
				{
					Name:  "kernel.shm_rmid_forced",
					Value: "1",
				},
			},
		}
		pod.Spec.Containers[0].Command = []string{"/bin/sysctl", "kernel.shm_rmid_forced"}

		ginkgo.By("Creating a pod with the kernel.shm_rmid_forced sysctl")
		pod = podClient.Create(ctx, pod)

		ginkgo.By("Watching for error events or started pod")
		// 监听事件而非等待 pod 终止：kubelet 会删除没有运行中容器的失败 pod，
		// 这里可能读到已被删除的 pod，从而产生竞态。
		ev, err := e2epod.NewPodClient(f).WaitForErrorEventOrSuccess(ctx, pod)
		framework.ExpectNoError(err)
		gomega.Expect(ev).To(gomega.BeNil())

		ginkgo.By("Waiting for pod completion")
		err = e2epod.WaitForPodNoLongerRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		framework.ExpectNoError(err)
		pod, err = podClient.Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Checking that the pod succeeded")
		gomega.Expect(pod.Status.Phase).To(gomega.Equal(v1.PodSucceeded))

		ginkgo.By("Getting logs from the pod")
		log, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		framework.ExpectNoError(err)

		ginkgo.By("Checking that the sysctl is actually updated")
		gomega.Expect(log).To(gomega.ContainSubstring("kernel.shm_rmid_forced = 1"))
	})

	/*
	  Release: v1.21
	  Testname: Sysctls, reject invalid sysctls
	  Description: 创建带一个有效、两个无效 sysctl 的 Pod。Pod 不应应用无效的 sysctl。
	  [LinuxOnly]: 该测试标记为 LinuxOnly，因为 Windows 不支持 sysctls
	*/
	framework.ConformanceIt("should reject invalid sysctls", func(ctx context.Context) {
		pod := testPod()
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			Sysctls: []v1.Sysctl{
				// 安全参数
				{
					Name:  "foo-",
					Value: "bar",
				},
				{
					Name:  "kernel.shmmax",
					Value: "100000000",
				},
				{
					Name:  "safe-and-unsafe",
					Value: "100000000",
				},
				{
					Name:  "bar..",
					Value: "42",
				},
			},
		}

		ginkgo.By("Creating a pod with one valid and two invalid sysctls")
		client := f.ClientSet.CoreV1().Pods(f.Namespace.Name)
		_, err := client.Create(ctx, pod, metav1.CreateOptions{})

		gomega.Expect(err).To(gomega.MatchError(gomega.SatisfyAll(
			gomega.ContainSubstring(`Invalid value: "foo-"`),
			gomega.ContainSubstring(`Invalid value: "bar.."`),
			gomega.Not(gomega.ContainSubstring(`safe-and-unsafe`)),
			gomega.Not(gomega.ContainSubstring("kernel.shmmax")),
		)))
	})

	// 创建设置了不安全 sysctl kernel.msgmax 的 Pod。
	ginkgo.It("should not launch unsafe, but not explicitly enabled sysctls on the node", func(ctx context.Context) {
		pod := testPod()
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			Sysctls: []v1.Sysctl{
				{
					Name:  "kernel.msgmax",
					Value: "10000000000",
				},
			},
		}

		ginkgo.By("Creating a pod with an ignorelisted, but not allowlisted sysctl on the node")
		pod = podClient.Create(ctx, pod)

		ginkgo.By("Wait for pod failed reason")
		// 监听 pod 失败原因，而非等待 pod 终止
		err := e2epod.WaitForPodFailedReason(ctx, f.ClientSet, pod, "SysctlForbidden", f.Timeouts.PodStart)
		framework.ExpectNoError(err)
	})

	/*
	  Release: v1.23
	  Testname: Sysctl, test sysctls supports slashes
	  Description: 创建设置了 kernel/shm_rmid_forced sysctl 的 Pod。支持用斜杠作为 sysctl 分隔符，'/' 可替代 '.'
	  [LinuxOnly]: 该测试标记为 LinuxOnly，因为 Windows 不支持 sysctls
	  [Environment:NotInUserNS]: 该测试在 UserNS 中会失败（符合预期）：`open /proc/sys/kernel/shm_rmid_forced: permission denied`
	*/
	f.It("should support sysctls with slashes as separator", environment.NotInUserNS, func(ctx context.Context) {
		pod := testPod()
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			Sysctls: []v1.Sysctl{
				{
					Name:  "kernel/shm_rmid_forced",
					Value: "1",
				},
			},
		}
		pod.Spec.Containers[0].Command = []string{"/bin/sysctl", "kernel/shm_rmid_forced"}

		ginkgo.By("Creating a pod with the kernel/shm_rmid_forced sysctl")
		pod = podClient.Create(ctx, pod)

		ginkgo.By("Watching for error events or started pod")
		// 监听事件而非等待 pod 终止：kubelet 会删除没有运行中容器的失败 pod，
		// 这里可能读到已被删除的 pod，从而产生竞态。
		ev, err := e2epod.NewPodClient(f).WaitForErrorEventOrSuccess(ctx, pod)
		framework.ExpectNoError(err)
		gomega.Expect(ev).To(gomega.BeNil())

		ginkgo.By("Waiting for pod completion")
		err = e2epod.WaitForPodNoLongerRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		framework.ExpectNoError(err)
		pod, err = podClient.Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Checking that the pod succeeded")
		gomega.Expect(pod.Status.Phase).To(gomega.Equal(v1.PodSucceeded))

		ginkgo.By("Getting logs from the pod")
		log, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		framework.ExpectNoError(err)

		ginkgo.By("Checking that the sysctl is actually updated")
		// 注意 sysctl 变量名中 "/" 和 "." 都可用作分隔符。
		// "kernel.shm_rmid_forced=1" 与 "kernel/shm_rmid_forced=1" 等价。
		// 在 Linux 上执行 "/bin/sysctl kernel/shm_rmid_forced" 命令，
		// 显示结果为 "kernel.shm_rmid_forced=1"。
		// 因此检查 pod 日志时要找的子串是
		// "kernel.shm_rmid_forced=1"，而不是 "kernel/shm_rmid_forced=1"。
		gomega.Expect(log).To(gomega.ContainSubstring("kernel.shm_rmid_forced = 1"))
	})
})
