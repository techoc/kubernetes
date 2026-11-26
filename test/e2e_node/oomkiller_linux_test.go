/*
Copyright 2022 The Kubernetes Authors.

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

package e2enode

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	libcontainercgroups "github.com/opencontainers/cgroups"
	"k8s.io/utils/ptr"
)

type testCase struct {
	name                    string
	podSpec                 *v1.Pod
	oomTargetContainerName  string
	enableSingleProcessKill *bool
	expectPodRunning        bool
}

// KubeReservedMemory 是为 K8s 组件预留的节点内存容量的默认比例
const KubeReservedMemory = 0.35

var _ = SIGDescribe("OOMKiller for pod using more memory than node allocatable [LinuxOnly]", framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("nodeallocatable-oomkiller-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	testCases := []testCase{
		{
			name:                   "single process container without resource limits",
			oomTargetContainerName: "oomkill-nodeallocatable-container",
			podSpec: getOOMTargetPod("oomkill-nodeallocatable-pod", "oomkill-nodeallocatable-container",
				getOOMTargetContainerWithoutLimit),
		},
	}

	for _, testCase := range testCases {
		runOomKillerTest(f, testCase, KubeReservedMemory)
	}
})

var _ = SIGDescribe("OOMKiller [LinuxOnly]", framework.WithNodeConformance(), framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("oomkiller-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	testCases := []testCase{
		{
			name:                   "single process container",
			oomTargetContainerName: "oomkill-single-target-container",
			podSpec: getOOMTargetPod("oomkill-target-pod", "oomkill-single-target-container",
				getOOMTargetContainer),
		},
		{
			name:                   "init container",
			oomTargetContainerName: "oomkill-target-init-container",
			podSpec: getInitContainerOOMTargetPod("initcontinar-oomkill-target-pod", "oomkill-target-init-container",
				getOOMTargetContainer),
		},
	}

	// 如果使用 cgroup v2，我们为容器 cgroup 设置 memory.oom.group=1，这样其中任何进程
	// 被 OOM 杀死时，容器内所有进程都会被 OOM 杀死
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		testCases = append(testCases, testCase{
			name:                   "multi process container",
			oomTargetContainerName: "oomkill-multi-target-container",
			podSpec: getOOMTargetPod("oomkill-target-pod", "oomkill-multi-target-container",
				getOOMTargetContainerMultiProcess),
			enableSingleProcessKill: nil,
		})

		testCases = append(testCases, testCase{
			name:                   "multi process container (single process kill enabled)",
			oomTargetContainerName: "oomkill-multi-target-container",
			podSpec: getOOMTargetPod("oomkill-target-pod", "oomkill-multi-target-container",
				getOOMTargetContainerMultiProcess),
			enableSingleProcessKill: ptr.To(true),
			expectPodRunning:        true,
		})

		testCases = append(testCases, testCase{
			name:                   "multi process container (single process kill disabled)",
			oomTargetContainerName: "oomkill-multi-target-container",
			podSpec: getOOMTargetPod("oomkill-target-pod", "oomkill-multi-target-container",
				getOOMTargetContainerMultiProcess),
			enableSingleProcessKill: ptr.To(false),
		})
	}
	for _, tc := range testCases {
		runOomKillerTest(f, tc, 0)
	}
})

func runOomKillerTest(f *framework.Framework, testCase testCase, kubeReservedMemory float64) {
	ginkgo.Context(testCase.name, func() {
		// 在 KubeletConfig 中更新 KubeReservedMemory。
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			if kubeReservedMemory > 0 {
				if initialConfig.KubeReserved == nil {
					initialConfig.KubeReserved = map[string]string{}
				}
				// 当节点可分配内存等于节点容量时，观察到系统 OOM 和 cgroup OOM 之间存在竞态。因此为
				// K8s 组件预留一部分节点内存容量，使节点可分配内存小于节点容量，
				// 从而在 cgroup 级别而非系统级别观察 OOM 杀死。
				//
				initialConfig.KubeReserved["memory"] = fmt.Sprintf("%d", int(kubeReservedMemory*getLocalNode(context.TODO(), f).Status.Capacity.Memory().AsApproximateFloat64()))
			}

			initialConfig.SingleProcessOOMKill = testCase.enableSingleProcessKill
		})

		ginkgo.BeforeEach(func() {
			// 运行测试前预防性检查 kubelet 是否健康。
			waitForKubeletToStart(context.TODO(), f)

			ginkgo.By("setting up the pod to be used in the test")
			e2epod.NewPodClient(f).Create(context.TODO(), testCase.podSpec)
		})

		if testCase.expectPodRunning {
			ginkgo.It("The containers should not be OOMKilled", func() {
				err := e2epod.WaitForPodsRunning(context.TODO(), f.ClientSet, f.Namespace.Name, 1, framework.PodStartTimeout)
				framework.ExpectNoError(err, "Failed waiting for pod to be running state, %s/%s", f.Namespace.Name, testCase.podSpec.Name)

				gomega.Consistently(context.TODO(), func(ctx context.Context) error {
					pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, testCase.podSpec.Name, metav1.GetOptions{})
					if err != nil {
						return fmt.Errorf("expected the pod %s to exist: %w", pod.Name, err)
					}
					phase := pod.Status.Phase
					if phase != v1.PodRunning && phase != v1.PodSucceeded {
						return fmt.Errorf("pod %s: unexpected status %s, expected status: %s or %s", pod.Name, pod.Status.Phase, v1.PodRunning, v1.PodSucceeded)
					}
					return nil
				}, 10*time.Second, f.Timeouts.Poll).Should(gomega.BeNil())
			})
		} else {
			ginkgo.It("The containers terminated by OOM killer should have the reason set to OOMKilled", func() {
				ginkgo.By("Waiting for the pod to be failed")
				err := e2epod.WaitForPodTerminatedInNamespace(context.TODO(), f.ClientSet, testCase.podSpec.Name, "", f.Namespace.Name)
				framework.ExpectNoError(err, "Failed waiting for pod to terminate, %s/%s", f.Namespace.Name, testCase.podSpec.Name)

				ginkgo.By("Fetching the latest pod status")
				pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(context.TODO(), testCase.podSpec.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "Failed to get the recent pod object for name: %q", pod.Name)

				ginkgo.By("Verifying the OOM target container has the expected reason")
				verifyReasonForOOMKilledContainer(pod, testCase.oomTargetContainerName)
			})
		}

		ginkgo.AfterEach(func() {
			ginkgo.By(fmt.Sprintf("deleting pod: %s", testCase.podSpec.Name))
			e2epod.NewPodClient(f).DeleteSync(context.TODO(), testCase.podSpec.Name, metav1.DeleteOptions{}, framework.PodDeleteTimeout)
		})
	})
}

func verifyReasonForOOMKilledContainer(pod *v1.Pod, oomTargetContainerName string) {
	container := e2epod.FindContainerStatusInPod(pod, oomTargetContainerName)
	if container == nil {
		framework.Failf("OOM target pod %q, container %q does not have the expected state terminated", pod.Name, oomTargetContainerName)
	}
	if container.State.Terminated == nil {
		framework.Failf("OOM target pod %q, container %q is not in the terminated state", pod.Name, container.Name)
	}
	gomega.Expect(container.State.Terminated.ExitCode).To(gomega.Equal(int32(137)),
		"pod: %q, container: %q has unexpected exitCode: %q", pod.Name, container.Name, container.State.Terminated.ExitCode)

	// This check is currently causing tests to flake on containerd & crio, https://github.com/kubernetes/kubernetes/issues/119600
	// 所以如果知道 reason 检查会失败，就跳过它。
	// TODO: Remove this once https://github.com/containerd/containerd/issues/8893 is resolved
	if container.State.Terminated.Reason == "OOMKilled" {
		gomega.Expect(container.State.Terminated.Reason).To(gomega.Equal("OOMKilled"),
			"pod: %q, container: %q has unexpected reason: %q", pod.Name, container.Name, container.State.Terminated.Reason)
	}

}

func getOOMTargetPod(podName string, ctnName string, createContainer func(name string) v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			PriorityClassName: "system-node-critical",
			RestartPolicy:     v1.RestartPolicyNever,
			Containers: []v1.Container{
				createContainer(ctnName),
			},
		},
	}
}

func getInitContainerOOMTargetPod(podName string, ctnName string, createContainer func(name string) v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			InitContainers: []v1.Container{
				createContainer(ctnName),
			},
			Containers: []v1.Container{
				{
					Name:  "busybox",
					Image: busyboxImage,
				},
			},
		},
	}
}

// getOOMTargetContainer 返回一个只有单个进程的容器，它尝试分配超过
// 容器内存限制允许的内存。
func getOOMTargetContainer(name string) v1.Container {
	return v1.Container{
		Name:  name,
		Image: busyboxImage,
		Command: []string{
			"sh",
			"-c",
			// 用 dd 工具尝试在一个块中分配 20M，超过限制
			"sleep 5 && dd if=/dev/zero of=/dev/null bs=20M",
		},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("15Mi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("15Mi"),
			},
		},
		SecurityContext: &v1.SecurityContext{
			SeccompProfile: &v1.SeccompProfile{
				Type: v1.SeccompProfileTypeRuntimeDefault,
			},
			AllowPrivilegeEscalation: ptr.To(false),
			RunAsUser:                ptr.To[int64](999),
			RunAsGroup:               ptr.To[int64](999),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &v1.Capabilities{Drop: []v1.Capability{"ALL"}},
		},
	}
}

// getOOMTargetContainerMultiProcess 返回一个有两个进程的容器，其中一个尝试分配更多
// 内存，超过容器内存限制，另一个进程只是睡眠。
func getOOMTargetContainerMultiProcess(name string) v1.Container {
	return v1.Container{
		Name:  name,
		Image: busyboxImage,
		Command: []string{
			"sh",
			"-c",
			// 用 dd 工具尝试在一个块中分配 20M，超过限制
			"sleep 5 && dd if=/dev/zero of=/dev/null bs=20M & sleep 86400",
		},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("15Mi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("15Mi"),
			},
		},
		SecurityContext: &v1.SecurityContext{
			SeccompProfile: &v1.SeccompProfile{
				Type: v1.SeccompProfileTypeRuntimeDefault,
			},
			AllowPrivilegeEscalation: ptr.To(false),
			RunAsUser:                ptr.To[int64](999),
			RunAsGroup:               ptr.To[int64](999),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &v1.Capabilities{Drop: []v1.Capability{"ALL"}},
		},
	}
}

// getOOMTargetContainerWithoutLimit 返回一个只有单个进程的容器，它尝试分配超过
// 节点可分配量的内存，且没有设置资源限制。
func getOOMTargetContainerWithoutLimit(name string) v1.Container {
	return v1.Container{
		Name:  name,
		Image: busyboxImage,
		Command: []string{
			"sh",
			"-c",
			// 用 dd 工具尝试分配一个超过节点可分配量的大内存块
			"sleep 5 && dd if=/dev/zero of=/dev/null iflag=fullblock count=10 bs=1024G",
		},
		SecurityContext: &v1.SecurityContext{
			SeccompProfile: &v1.SeccompProfile{
				Type: v1.SeccompProfileTypeRuntimeDefault,
			},
			AllowPrivilegeEscalation: ptr.To(false),
			RunAsUser:                ptr.To[int64](999),
			RunAsGroup:               ptr.To[int64](999),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &v1.Capabilities{Drop: []v1.Capability{"ALL"}},
		},
	}
}
