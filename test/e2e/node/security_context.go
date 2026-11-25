/*
Copyright 2015 The Kubernetes Authors.

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

/* 此测试检查在 Pod 或容器级别指定的 SecurityContext 参数是否按预期工作。当未使用 'SecurityContextDeny' admission controller 时无法运行这些测试，因此默认跳过。 */

package node

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// SeccompProcStatusField 是 /proc/$PID/status 中引用 seccomp filter 类型的字段。
const SeccompProcStatusField = "Seccomp:"

// ProcSelfStatusPath 是 /proc/self/status 的路径。
const ProcSelfStatusPath = "/proc/self/status"

func scTestPod(hostIPC bool, hostPID bool) *v1.Pod {
	podName := "security-context-" + string(uuid.NewUUID())
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Labels:      map[string]string{"name": podName},
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			HostIPC:         hostIPC,
			HostPID:         hostPID,
			SecurityContext: &v1.PodSecurityContext{},
			Containers: []v1.Container{
				{
					Name:  "test-container",
					Image: imageutils.GetE2EImage(imageutils.BusyBox),
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	return pod
}

var _ = SIGDescribe("Security Context", func() {
	f := framework.NewDefaultFramework("security-context")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.It("should support pod.Spec.SecurityContext.SupplementalGroups [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		pod.Spec.Containers[0].Command = []string{"id", "-G"}
		pod.Spec.SecurityContext.SupplementalGroups = []int64{1234, 5678}
		groups := []string{"1234", "5678"}
		e2eoutput.TestContainerOutput(ctx, f, "pod.Spec.SecurityContext.SupplementalGroups", pod, 0, groups)
	})

	ginkgo.When("if the container's primary UID belongs to some groups in the image [LinuxOnly]", func() {
		ginkgo.It("should add pod.Spec.SecurityContext.SupplementalGroups to them [LinuxOnly] in resultant supplementary groups for the container processes", func(ctx context.Context) {
			uidInImage := int64(1000)
			gidDefinedInImage := int64(50000)
			supplementalGroup := int64(60000)
			agnhost := imageutils.GetConfig(imageutils.Agnhost)
			pod := scTestPod(false, false)
			pod.Spec.Containers[0].Image = agnhost.GetE2EImage()
			pod.Spec.Containers[0].Command = []string{"id", "-G"}
			pod.Spec.SecurityContext.SupplementalGroups = []int64{int64(supplementalGroup)}
			pod.Spec.SecurityContext.RunAsUser = &uidInImage

			// 在指定的镜像（agnhost E2E 镜像）中，
			// - 定义了 user-defined-in-image（uid=1000）
			// - user-defined-in-image 属于 group-defined-in-image（gid=50000）
			// 因此，容器进程的最终 supplementary group 应为
			// - 1000：自身
			// - 50000：在自身（uid=1000）的容器镜像中预定义的组
			// - 60000: SupplementalGroups
			// $ id -G
			// 1000 50000 60000
			e2eoutput.TestContainerOutput(
				ctx,
				f,
				"pod.Spec.SecurityContext.SupplementalGroups with pre-defined-group in the image",
				pod, 0,
				[]string{fmt.Sprintf("%d %d %d", uidInImage, gidDefinedInImage, supplementalGroup)},
			)
		})
	})

	ginkgo.It("should support pod.Spec.SecurityContext.RunAsUser [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		userID := int64(1001)
		pod.Spec.SecurityContext.RunAsUser = &userID
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "id"}

		e2eoutput.TestContainerOutput(ctx, f, "pod.Spec.SecurityContext.RunAsUser", pod, 0, []string{
			fmt.Sprintf("uid=%v", userID),
			fmt.Sprintf("gid=%v", 0),
		})
	})

	/*
		Release: v1.21
		Testname: Security Context, test RunAsGroup at pod level
		Description: 通过在 pod 级别传入 uid 1001 和 gid 2002，创建带 runAsUser 和 runAsGroup 选项的容器。Pod MUST 处于 Succeeded 阶段。
		[LinuxOnly]: 由于 Windows 不支持以 UID / GID 运行，此测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should support pod.Spec.SecurityContext.RunAsUser And pod.Spec.SecurityContext.RunAsGroup [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		userID := int64(1001)
		groupID := int64(2002)
		pod.Spec.SecurityContext.RunAsUser = &userID
		pod.Spec.SecurityContext.RunAsGroup = &groupID
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "id"}

		e2eoutput.TestContainerOutput(ctx, f, "pod.Spec.SecurityContext.RunAsUser", pod, 0, []string{
			fmt.Sprintf("uid=%v", userID),
			fmt.Sprintf("gid=%v", groupID),
		})
	})

	ginkgo.It("should support container.SecurityContext.RunAsUser [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		userID := int64(1001)
		overrideUserID := int64(1002)
		pod.Spec.SecurityContext.RunAsUser = &userID
		pod.Spec.Containers[0].SecurityContext = new(v1.SecurityContext)
		pod.Spec.Containers[0].SecurityContext.RunAsUser = &overrideUserID
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "id"}

		e2eoutput.TestContainerOutput(ctx, f, "pod.Spec.SecurityContext.RunAsUser", pod, 0, []string{
			fmt.Sprintf("uid=%v", overrideUserID),
			fmt.Sprintf("gid=%v", 0),
		})
	})

	/*
		Release: v1.21
		Testname: Security Context, test RunAsGroup at container level
		Description: 通过在容器级别传入 uid 1001 和 gid 2002，创建带 runAsUser 和 runAsGroup 选项的容器。Pod MUST 处于 Succeeded 阶段。
		[LinuxOnly]: 由于 Windows 不支持以 UID / GID 运行，此测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should support container.SecurityContext.RunAsUser And container.SecurityContext.RunAsGroup [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		userID := int64(1001)
		groupID := int64(2001)
		overrideUserID := int64(1002)
		overrideGroupID := int64(2002)
		pod.Spec.SecurityContext.RunAsUser = &userID
		pod.Spec.SecurityContext.RunAsGroup = &groupID
		pod.Spec.Containers[0].SecurityContext = new(v1.SecurityContext)
		pod.Spec.Containers[0].SecurityContext.RunAsUser = &overrideUserID
		pod.Spec.Containers[0].SecurityContext.RunAsGroup = &overrideGroupID
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "id"}

		e2eoutput.TestContainerOutput(ctx, f, "pod.Spec.SecurityContext.RunAsUser", pod, 0, []string{
			fmt.Sprintf("uid=%v", overrideUserID),
			fmt.Sprintf("gid=%v", overrideGroupID),
		})
	})

	f.It("should support volume SELinux relabeling", f.WithFlaky(), f.WithLabel("LinuxOnly"), func(ctx context.Context) {
		testPodSELinuxLabeling(ctx, f, false, false)
	})

	f.It("should support volume SELinux relabeling when using hostIPC", f.WithFlaky(), f.WithLabel("LinuxOnly"), func(ctx context.Context) {
		testPodSELinuxLabeling(ctx, f, true, false)
	})

	f.It("should support volume SELinux relabeling when using hostPID", f.WithFlaky(), f.WithLabel("LinuxOnly"), func(ctx context.Context) {
		testPodSELinuxLabeling(ctx, f, false, true)
	})

	ginkgo.It("should support seccomp unconfined on the container [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		pod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{SeccompProfile: &v1.SeccompProfile{Type: v1.SeccompProfileTypeUnconfined}}
		pod.Spec.SecurityContext = &v1.PodSecurityContext{SeccompProfile: &v1.SeccompProfile{Type: v1.SeccompProfileTypeRuntimeDefault}}
		pod.Spec.Containers[0].Command = []string{"grep", SeccompProcStatusField, ProcSelfStatusPath}
		e2eoutput.TestContainerOutput(ctx, f, "seccomp unconfined container", pod, 0, []string{"0"}) // 禁用 seccomp
	})

	ginkgo.It("should support seccomp unconfined on the pod [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		pod.Spec.SecurityContext = &v1.PodSecurityContext{SeccompProfile: &v1.SeccompProfile{Type: v1.SeccompProfileTypeUnconfined}}
		pod.Spec.Containers[0].Command = []string{"grep", SeccompProcStatusField, ProcSelfStatusPath}
		e2eoutput.TestContainerOutput(ctx, f, "seccomp unconfined pod", pod, 0, []string{"0"}) // 禁用 seccomp
	})

	ginkgo.It("should support seccomp runtime/default [LinuxOnly]", func(ctx context.Context) {
		pod := scTestPod(false, false)
		pod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{SeccompProfile: &v1.SeccompProfile{Type: v1.SeccompProfileTypeRuntimeDefault}}
		pod.Spec.Containers[0].Command = []string{"grep", SeccompProcStatusField, ProcSelfStatusPath}
		e2eoutput.TestContainerOutput(ctx, f, "seccomp runtime/default", pod, 0, []string{"2"}) // seccomp 已过滤
	})
})

func testPodSELinuxLabeling(ctx context.Context, f *framework.Framework, hostIPC bool, hostPID bool) {
	// 使用 empty_dir volume 写入和读取文件
	// 使用带 MCS 标签 s0:c0,c1 的 Pod
	pod := scTestPod(hostIPC, hostPID)
	volumeName := "test-volume"
	mountPath := "/mounted_volume"
	pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			Name:      volumeName,
			MountPath: mountPath,
		},
	}
	pod.Spec.Volumes = []v1.Volume{
		{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{
					Medium: v1.StorageMediumDefault,
				},
			},
		},
	}
	pod.Spec.SecurityContext.SELinuxOptions = &v1.SELinuxOptions{
		Level: "s0:c0,c1",
	}
	pod.Spec.Containers[0].Command = []string{"sleep", "6000"}

	client := f.ClientSet.CoreV1().Pods(f.Namespace.Name)
	pod, err := client.Create(ctx, pod, metav1.CreateOptions{})

	framework.ExpectNoError(err, "Error creating pod %v", pod)
	framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

	testContent := "hello"
	testFilePath := mountPath + "/TEST"
	tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, f.Namespace.Name)
	err = tk.WriteFileViaContainer(pod.Name, pod.Spec.Containers[0].Name, testFilePath, testContent)
	framework.ExpectNoError(err)
	content, err := tk.ReadFileViaContainer(pod.Name, pod.Spec.Containers[0].Name, testFilePath)
	framework.ExpectNoError(err)
	gomega.Expect(content).To(gomega.ContainSubstring(testContent))

	foundPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)

	// 确认第二个 Pod 使用带相同 MCS 标签的 host_path
	// 时能访问该文件
	volumeHostPath := fmt.Sprintf("%s/pods/%s/volumes/kubernetes.io~empty-dir/%s", framework.TestContext.KubeletRootDir, foundPod.UID, volumeName)
	ginkgo.By(fmt.Sprintf("confirming a container with the same label can read the file under --kubelet-root-dir=%s", framework.TestContext.KubeletRootDir))
	pod = scTestPod(hostIPC, hostPID)
	pod.Spec.NodeName = foundPod.Spec.NodeName
	volumeMounts := []v1.VolumeMount{
		{
			Name:      volumeName,
			MountPath: mountPath,
		},
	}
	volumes := []v1.Volume{
		{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: volumeHostPath,
				},
			},
		},
	}
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].Command = []string{"cat", testFilePath}
	pod.Spec.SecurityContext.SELinuxOptions = &v1.SELinuxOptions{
		Level: "s0:c0,c1",
	}
	e2eoutput.TestContainerOutput(ctx, f, "Pod with same MCS label reading test file", pod, 0, []string{testContent})

	// 确认带不同 MCS 标签的同一 Pod
	// 无法访问该 volume
	ginkgo.By("confirming a container with a different MCS label is unable to read the file")
	pod = scTestPod(hostIPC, hostPID)
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Containers[0].Command = []string{"sleep", "6000"}
	pod.Spec.SecurityContext.SELinuxOptions = &v1.SELinuxOptions{
		Level: "s0:c2,c3",
	}
	_, err = client.Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Error creating pod %v", pod)

	err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
	framework.ExpectNoError(err, "Error waiting for pod to run %v", pod)

	// 为此，SELinux 必须处于 enforcing 模式，先检查一下
	isEnforced, err := tk.ReadFileViaContainer(pod.Name, "test-container", "/sys/fs/selinux/enforce")
	if err == nil && isEnforced == "1" {
		_, err = tk.ReadFileViaContainer(pod.Name, "test-container", testFilePath)
		gomega.Expect(err).To(gomega.HaveOccurred(), "expecting SELinux to not let the container with different MCS label to read the file")
	}
}
