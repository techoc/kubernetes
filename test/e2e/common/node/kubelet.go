/*
Copyright 2016 The Kubernetes Authors.

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
	"bytes"
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Kubelet", func() {
	f := framework.NewDefaultFramework("kubelet-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient
	ginkgo.BeforeEach(func() {
		podClient = e2epod.NewPodClient(f)
	})
	ginkgo.Context("when scheduling a busybox command in a pod", func() {
		podName := "busybox-scheduling-" + string(uuid.NewUUID())

		/*
			Release: v1.13
			Testname: Kubelet, log output, default
			Description: 默认情况下，pod 中进程的标准输出和标准错误 MUST 被送入 pod 日志。
		*/
		framework.ConformanceIt("should print the output to logs", f.WithNodeConformance(), func(ctx context.Context) {
			podClient.CreateSync(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					// Pod 预期会退出，因此不重启
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Name:    podName,
							Command: []string{"sh", "-c", "echo 'Hello World' ; sleep 240"},
						},
					},
				},
			})
			gomega.Eventually(ctx, func() string {
				sinceTime := metav1.NewTime(time.Now().Add(time.Duration(-1 * time.Hour)))
				rc, err := podClient.GetLogs(podName, &v1.PodLogOptions{SinceTime: &sinceTime}).Stream(ctx)
				if err != nil {
					return ""
				}
				defer rc.Close()
				buf := new(bytes.Buffer)
				buf.ReadFrom(rc)
				return buf.String()
			}, time.Minute, time.Second*4).Should(gomega.Equal("Hello World\n"))
		})
	})
	ginkgo.Context("when scheduling a busybox command that always fails in a pod", func() {
		var podName string

		ginkgo.BeforeEach(func(ctx context.Context) {
			podName = "bin-false" + string(uuid.NewUUID())
			podClient.Create(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					// Pod 预期会退出，因此不重启
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Name:    podName,
							Command: []string{"/bin/false"},
						},
					},
				},
			})
		})

		/*
			Release: v1.13
			Testname: Kubelet, failed pod, terminated reason
			Description: 创建一个处于 terminated 状态的 Pod。Pod MUST 只有一个容器。该容器 MUST 处于 terminated 状态且 MUST 有终止原因。
		*/
		framework.ConformanceIt("should have an terminated reason", f.WithNodeConformance(), func(ctx context.Context) {
			gomega.Eventually(ctx, func() error {
				podData, err := podClient.Get(ctx, podName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if len(podData.Status.ContainerStatuses) != 1 {
					return fmt.Errorf("expected only one container in the pod %q", podName)
				}
				contTerminatedState := podData.Status.ContainerStatuses[0].State.Terminated
				if contTerminatedState == nil {
					return fmt.Errorf("expected state to be terminated. Got pod status: %+v", podData.Status)
				}
				if contTerminatedState.ExitCode == 0 || contTerminatedState.Reason == "" {
					return fmt.Errorf("expected non-zero exitCode and non-empty terminated state reason. Got exitCode: %+v and terminated state reason: %+v", contTerminatedState.ExitCode, contTerminatedState.Reason)
				}
				return nil
			}, framework.PodStartTimeout, time.Second*4).Should(gomega.BeNil())
		})

		/*
			Release: v1.13
			Testname: Kubelet, failed pod, delete
			Description: 创建一个处于 terminated 状态的 Pod。这个已终止的 Pod MUST 可以被删除。
		*/
		framework.ConformanceIt("should be possible to delete", f.WithNodeConformance(), func(ctx context.Context) {
			err := podClient.Delete(ctx, podName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "deleting Pod")
		})
	})
	ginkgo.Context("when scheduling an agnhost Pod with hostAliases", func() {
		podName := "agnhost-host-aliases" + string(uuid.NewUUID())

		/*
			Release: v1.13
			Testname: Kubelet, hostAliases
			Description: 创建一个带 hostAliases 的 Pod，其中容器命令会输出 /etc/hosts 条目。Pod 日志中 MUST 包含与指定 hostAliases 对应的 /etc/hosts 条目。
		*/
		framework.ConformanceIt("should write entries to /etc/hosts", f.WithNodeConformance(), func(ctx context.Context) {
			pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, nil, nil, nil, "etc-hosts")
			// Pod 预期会退出，因此不重启
			pod.Spec.RestartPolicy = v1.RestartPolicyNever
			pod.Spec.HostAliases = []v1.HostAlias{
				{
					IP:        "123.45.67.89",
					Hostnames: []string{"foo", "bar"},
				},
			}

			pod = podClient.Create(ctx, pod)
			ginkgo.By("Waiting for pod completion")
			err := e2epod.WaitForPodNoLongerRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)

			rc, err := podClient.GetLogs(podName, &v1.PodLogOptions{}).Stream(ctx)
			framework.ExpectNoError(err)
			defer rc.Close()
			buf := new(bytes.Buffer)
			buf.ReadFrom(rc)
			hostsFileContent := buf.String()

			errMsg := fmt.Sprintf("expected hosts file to contain entries from HostAliases. Got:\n%+v", hostsFileContent)
			gomega.Expect(hostsFileContent).To(gomega.ContainSubstring("123.45.67.89\tfoo\tbar"), errMsg)
		})
	})
	ginkgo.Context("when scheduling a read only busybox container", func() {
		podName := "busybox-readonly-fs" + string(uuid.NewUUID())

		/*
			Release: v1.13
			Testname: Kubelet, pod with read only root file system
			Description: 创建一个 security context 中 ReadOnlyRootFileSystem 设为 true 的 Pod。Pod 尝试向根目录下的 /file 写入，写操作 MUST 按预期失败。
			该测试标记为 LinuxOnly，因为 Windows 不支持创建只读访问的容器。
		*/
		framework.ConformanceIt("should not write to root filesystem [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
			isReadOnly := true
			podClient.CreateSync(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					// Pod 预期会退出，因此不重启
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Name:    podName,
							Command: []string{"/bin/sh", "-c", "echo test > /file; sleep 240"},
							SecurityContext: &v1.SecurityContext{
								ReadOnlyRootFilesystem: &isReadOnly,
							},
						},
					},
				},
			})
			gomega.Eventually(ctx, func() string {
				rc, err := podClient.GetLogs(podName, &v1.PodLogOptions{}).Stream(ctx)
				if err != nil {
					return ""
				}
				defer rc.Close()
				buf := new(bytes.Buffer)
				buf.ReadFrom(rc)
				return buf.String()
			}, time.Minute, time.Second*4).Should(gomega.Equal("/bin/sh: can't create /file: Read-only file system\n"))
		})
	})
})

var _ = SIGDescribe("Kubelet with pods in a privileged namespace", func() {
	f := framework.NewDefaultFramework("kubelet-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var podClient *e2epod.PodClient
	ginkgo.BeforeEach(func() {
		podClient = e2epod.NewPodClient(f)
	})
	ginkgo.Context("when scheduling an agnhost Pod with hostAliases and hostNetwork", func() {
		podName := "agnhost-host-aliases" + string(uuid.NewUUID())
		/*
			Release: v1.30
			Testname: Kubelet, hostAliases, hostNetwork
			Description: 创建一个启用 hostNetwork、带 hostAliases 的 Pod，其中容器命令会输出 /etc/hosts 条目。Pod 日志中 MUST 包含与指定 hostAliases 对应的 /etc/hosts 条目。
		*/
		framework.It("should write entries to /etc/hosts when hostNetwork is enabled", f.WithNodeConformance(), func(ctx context.Context) {
			pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, nil, nil, nil, "etc-hosts")
			pod.Spec.HostNetwork = true
			// Pod 预期会退出，因此不重启
			pod.Spec.RestartPolicy = v1.RestartPolicyNever
			pod.Spec.HostAliases = []v1.HostAlias{
				{
					IP:        "123.45.67.89",
					Hostnames: []string{"foo", "bar"},
				},
			}

			pod = podClient.Create(ctx, pod)
			ginkgo.By("Waiting for pod completion")
			err := e2epod.WaitForPodNoLongerRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)

			rc, err := podClient.GetLogs(podName, &v1.PodLogOptions{}).Stream(ctx)
			framework.ExpectNoError(err)
			defer rc.Close() // nolint:errcheck
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(rc)
			framework.ExpectNoError(err)
			hostsFileContent := buf.String()

			errMsg := fmt.Sprintf("expected hosts file to contain entries from HostAliases. Got:\n%+v", hostsFileContent)
			gomega.Expect(hostsFileContent).To(gomega.ContainSubstring("123.45.67.89\tfoo\tbar"), errMsg)
		})
	})
})
