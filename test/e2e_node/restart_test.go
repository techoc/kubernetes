//go:build linux

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

package e2enode

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"
)

type podCondition func(pod *v1.Pod) (bool, error)

// waitForPodsCondition 在超时时长内等待 `podCount` 个 Pod 匹配特定 pod condition。
// 如果超时，返回当前运行的 Pod 列表。
func waitForPodsCondition(ctx context.Context, f *framework.Framework, podCount int, timeout time.Duration, condition podCondition) (runningPods []*v1.Pod) {
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(10 * time.Second) {
		podList, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Logf("Failed to list pods on node: %v", err)
			continue
		}

		runningPods = []*v1.Pod{}
		for i := range podList.Items {
			pod := podList.Items[i]
			if r, err := condition(&pod); err != nil || !r {
				continue
			}
			runningPods = append(runningPods, &pod)
		}
		framework.Logf("Running pod count %d", len(runningPods))
		if len(runningPods) >= podCount {
			break
		}
	}
	return runningPods
}

var _ = SIGDescribe("Restart", framework.WithSerial(), framework.WithSlow(), framework.WithDisruptive(), func() {
	const (
		// 让节点饱和。并非所有这些 Pod 都必须进入
		// Running/Ready，因为我们不知道测试节点的内核数，
		// 也不知道是否应用了默认限制。关键是
		// 不能有任何容器进入 terminated。选择 100 是因为
		// 这是每个节点的最大 Pod 数。
		podCount            = 100
		podCreationInterval = 100 * time.Millisecond
		recoverTimeout      = 5 * time.Minute
		startTimeout        = 3 * time.Minute
		// restartCount 的取值确保即使使用 minPods，也会耗尽
		// 一个 /24 网段的默认分配。
		minPods      = 50
		restartCount = 6
	)

	f := framework.NewDefaultFramework("restart-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	ginkgo.Context("Container Runtime", func() {
		ginkgo.Context("Network", func() {
			ginkgo.It("should recover from ip leak", func(ctx context.Context) {
				pods := newTestPods(podCount, false, imageutils.GetPauseImageName(), "restart-container-runtime-test")
				ginkgo.By(fmt.Sprintf("Trying to create %d pods on node", len(pods)))
				createBatchPodWithRateControl(ctx, f, pods, podCreationInterval)
				ginkgo.DeferCleanup(deletePodsSync, f, pods)

				// 给节点一些时间稳定，假定在 startTimeout 内进入 RunningReady 的 Pod
				// 适合该节点，且节点现已饱和。
				runningPods := waitForPodsCondition(ctx, f, podCount, startTimeout, testutils.PodRunningReadyOrSucceeded)
				if len(runningPods) < minPods {
					framework.Failf("Failed to start %d pods, cannot test that restarting container runtime doesn't leak IPs", minPods)
				}

				for i := range restartCount {
					ginkgo.By(fmt.Sprintf("Killing container runtime iteration %d", i))
					// 等待容器运行时进入运行态
					var pid int
					gomega.Eventually(ctx, func() error {
						runtimePids, err := getPidsForProcess(framework.TestContext.ContainerRuntimeProcessName, framework.TestContext.ContainerRuntimePidFile)
						if err != nil {
							return err
						}
						if len(runtimePids) != 1 {
							return fmt.Errorf("unexpected container runtime pid list: %+v", runtimePids)
						}
						// 确保容器运行时正在运行，从 pid 文件获取的 pid 可能并未运行。
						pid = runtimePids[0]
						if _, err := exec.Command("sudo", "ps", "-p", fmt.Sprintf("%d", pid)).CombinedOutput(); err != nil {
							return err
						}
						return nil
					}, 1*time.Minute, 2*time.Second).Should(gomega.BeNil())
					if stdout, err := exec.Command("sudo", "kill", "-SIGKILL", fmt.Sprintf("%d", pid)).CombinedOutput(); err != nil {
						framework.Failf("Failed to kill container runtime (pid=%d): %v, stdout: %q", pid, err, string(stdout))
					}
					// 假定容器运行时会由 systemd/supervisord 等重启。
					time.Sleep(20 * time.Second)
				}

				ginkgo.By("Checking currently Running/Ready pods")
				postRestartRunningPods := waitForPodsCondition(ctx, f, len(runningPods), recoverTimeout, testutils.PodRunningReadyOrSucceeded)
				if len(postRestartRunningPods) == 0 {
					framework.Failf("Failed to start *any* pods after container runtime restart, this might indicate an IP leak")
				}
				ginkgo.By("Confirm no containers have terminated")
				for _, pod := range postRestartRunningPods {
					if c := testutils.TerminatedContainers(pod); len(c) != 0 {
						framework.Failf("Pod %q has failed containers %+v after container runtime restart, this might indicate an IP leak", pod.Name, c)
					}
				}
				ginkgo.By(fmt.Sprintf("Container runtime restart test passed with %d pods", len(postRestartRunningPods)))
			})
		})
	})
	ginkgo.Context("Kubelet", func() {
		ginkgo.It("should correctly account for terminated pods after restart", func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			cpus := node.Status.Allocatable[v1.ResourceCPU]
			numCpus := int((&cpus).Value())
			if numCpus < 1 {
				e2eskipper.Skipf("insufficient CPU available for kubelet restart test")
			}
			if numCpus > 18 {
				// 950m * 19 = 1805 CPUs -> 不足以阻止另一个 950m Pod 的调度
				e2eskipper.Skipf("test will return false positives on a machine with >18 cores")
			}

			// 创建与可分配 CPU 数量相同的 restartNever Pod
			// 节点；如果它们未被正确计入 terminated，
			// 之后将占满所有节点容量
			podCountRestartNever := numCpus
			ginkgo.By(fmt.Sprintf("creating %d RestartNever pods on node", podCountRestartNever))
			restartNeverPods := newTestPods(podCountRestartNever, false, imageutils.GetE2EImage(imageutils.BusyBox), "restart-kubelet-test")
			for _, pod := range restartNeverPods {
				pod.Spec.RestartPolicy = "Never"
				pod.Spec.Containers[0].Command = []string{"echo", "hi"}
				pod.Spec.Containers[0].Resources.Limits = v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("950m"), // 为其他工作负载留一点空间
				}
			}
			createBatchPodWithRateControl(ctx, f, restartNeverPods, podCreationInterval)
			ginkgo.DeferCleanup(deletePodsSync, f, restartNeverPods)
			completedPods := waitForPodsCondition(ctx, f, podCountRestartNever, startTimeout, testutils.PodSucceeded)

			if len(completedPods) < podCountRestartNever {
				framework.Failf("Failed to run sufficient restartNever pods, got %d but expected %d", len(completedPods), podCountRestartNever)
			}

			podCountRestartAlways := (numCpus / 2) + 1
			ginkgo.By(fmt.Sprintf("creating %d RestartAlways pods on node", podCountRestartAlways))
			restartAlwaysPods := newTestPods(podCountRestartAlways, false, imageutils.GetPauseImageName(), "restart-kubelet-test")
			for _, pod := range restartAlwaysPods {
				pod.Spec.Containers[0].Resources.Limits = v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				}
			}
			createBatchPodWithRateControl(ctx, f, restartAlwaysPods, podCreationInterval)
			ginkgo.DeferCleanup(deletePodsSync, f, restartAlwaysPods)

			numAllPods := podCountRestartNever + podCountRestartAlways
			allPods := waitForPodsCondition(ctx, f, numAllPods, startTimeout, testutils.PodRunningReadyOrSucceeded)
			if len(allPods) < numAllPods {
				framework.Failf("Failed to run sufficient restartAlways pods, got %d but expected %d", len(allPods), numAllPods)
			}

			ginkgo.By("killing and restarting kubelet")
			// 我们希望杀死 kubelet，而不是优雅重启
			restartKubelet := mustStopKubelet(ctx, f)
			restartKubelet(ctx)

			// 如果此测试正常工作，每个 Pod 都会正常
			// 退出。但如果计数失效，重启后调度的 Pod
			// 可能认为这些旧 Pod 仍在消耗 CPU，从而
			// 导致 OutOfCpu 错误。
			ginkgo.By("verifying restartNever pods succeed and restartAlways pods stay running")
			for start := time.Now(); time.Since(start) < startTimeout && ctx.Err() == nil; time.Sleep(10 * time.Second) {
				postRestartRunningPods := waitForPodsCondition(ctx, f, numAllPods, recoverTimeout, testutils.PodRunningReadyOrSucceeded)
				if len(postRestartRunningPods) < numAllPods {
					framework.Failf("less pods are running after node restart, got %d but expected %d", len(postRestartRunningPods), numAllPods)
				}
			}
		})
		// 针对 https://issues.k8s.io/116925 的回归测试
		ginkgo.It("should delete pods which are marked as terminal and have a deletion timestamp set after restart", func(ctx context.Context) {
			podName := "terminal-restart-pod" + string(uuid.NewUUID())
			gracePeriod := int64(30)
			podSpec := e2epod.MustMixinRestrictedPodSecurity(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &gracePeriod,
					RestartPolicy:                 v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:    podName,
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"sh", "-c"},
							Args: []string{`
							sleep 9999999 &
							PID=$!

							_term () {
							   kill $PID
							   echo "Caught SIGTERM!"
							}

							trap _term SIGTERM
							touch /tmp/trap-marker

							wait $PID
							trap - TERM

							# Wait for the long running sleep to exit
							wait $PID

							exit 0
							`,
							},
							ReadinessProbe: &v1.Probe{
								PeriodSeconds: 1,
								ProbeHandler: v1.ProbeHandler{
									Exec: &v1.ExecAction{
										Command: []string{"/bin/sh", "-c", "cat /tmp/trap-marker"},
									},
								},
							},
						},
					},
				},
			})
			ginkgo.By(fmt.Sprintf("Creating a pod (%v/%v) with restart policy: %v", f.Namespace.Name, podName, podSpec.Spec.RestartPolicy))
			pod := e2epod.NewPodClient(f).Create(ctx, podSpec)

			ginkgo.By(fmt.Sprintf("Waiting for the pod (%v/%v) to be running, and with the SIGTERM trap registered", f.Namespace.Name, pod.Name))
			err := e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name, f.Timeouts.PodStart)
			framework.ExpectNoError(err, "Failed to await for the pod to be running: (%v/%v)", f.Namespace.Name, pod.Name)

			w := &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return f.ClientSet.CoreV1().Pods(f.Namespace.Name).Watch(ctx, options)
				},
			}

			podsList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err, "Failed to list pods in namespace: %s", f.Namespace.Name)

			ginkgo.By(fmt.Sprintf("Deleting the pod (%v/%v) to set a deletion timestamp", pod.Namespace, pod.Name))
			err = e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
			framework.ExpectNoError(err, "Failed to delete the pod: %q", pod.Name)

			ctxUntil, cancel := context.WithTimeout(ctx, f.Timeouts.PodStart)
			defer cancel()

			ginkgo.By(fmt.Sprintf("Started watch for pod (%v/%v) to enter succeeded phase", pod.Namespace, pod.Name))
			_, err = watchtools.Until(ctxUntil, podsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
				if pod, ok := event.Object.(*v1.Pod); ok {
					found := pod.ObjectMeta.Name == podName &&
						pod.ObjectMeta.Namespace == f.Namespace.Name &&
						pod.Status.Phase == v1.PodSucceeded
					if !found {
						ginkgo.By(fmt.Sprintf("Observed Pod (%s/%s) in phase %v", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name, pod.Status.Phase))
						return false, nil
					}
					ginkgo.By(fmt.Sprintf("Found Pod (%s/%s) in phase %v", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name, pod.Status.Phase))
					return found, nil
				}
				ginkgo.By(fmt.Sprintf("Observed event: %+v", event.Object))
				return false, nil
			})
			ginkgo.By("Ended watch for pod entering succeeded phase")
			framework.ExpectNoError(err, "failed to see event that pod (%s/%s) enter succeeded phase: %v", pod.Namespace, pod.Name, err)

			// Pod 一进入 succeeded 阶段（由上面的 watch 检测到），就杀死 kubelet。
			// 这有些竞争，但目标是在 kubelet 从 API server 删除 Pod 之前停止它，以复现 https://issues.k8s.io/116925
			ginkgo.By("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			ginkgo.By("Restarting the kubelet")
			restartKubelet(ctx)

			// 等待 Kubelet 就绪。
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				nodes, err := e2enode.TotalReady(ctx, f.ClientSet)
				framework.ExpectNoError(err)
				return nodes == 1
			}, time.Minute, f.Timeouts.Poll).Should(gomega.BeTrueBecause("expected kubelet to be in ready state"))

			ginkgo.By(fmt.Sprintf("After the kubelet is restarted, verify the pod (%s/%s) is deleted by kubelet", pod.Namespace, pod.Name))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, pod.Name, pod.Namespace)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())
		})
		// 针对 https://issues.k8s.io/118472 的回归测试
		ginkgo.It("should force-delete non-admissible pods created and deleted during kubelet restart", func(ctx context.Context) {
			podName := "rejected-deleted-pod" + string(uuid.NewUUID())
			gracePeriod := int64(30)
			nodeName := getNodeName(ctx, f)
			podSpec := e2epod.MustMixinRestrictedPodSecurity(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: f.Namespace.Name,
				},
				Spec: v1.PodSpec{
					NodeName: nodeName,
					NodeSelector: map[string]string{
						"this-label": "does-not-exist-on-any-nodes",
					},
					TerminationGracePeriodSeconds: &gracePeriod,
					RestartPolicy:                 v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:  podName,
							Image: imageutils.GetPauseImageName(),
						},
					},
				},
			})
			ginkgo.By("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			// 创建绑定到该节点的 Pod。由于 kubelet 停机，
			// 它将保持在 Pending 阶段。
			ginkgo.By(fmt.Sprintf("Creating a pod (%v/%v)", f.Namespace.Name, podName))
			pod := e2epod.NewPodClient(f).Create(ctx, podSpec)

			ginkgo.By(fmt.Sprintf("Deleting the pod (%v/%v) to set a deletion timestamp", pod.Namespace, pod.Name))
			err := e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
			framework.ExpectNoError(err, "Failed to delete the pod: %q", pod.Name)

			// 重启 Kubelet，使其继续执行删除
			ginkgo.By("Starting the kubelet")
			restartKubelet(ctx)

			ginkgo.By(fmt.Sprintf("After the kubelet is restarted, verify the pod (%v/%v) is deleted by kubelet", pod.Namespace, pod.Name))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, pod.Name, pod.Namespace)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())
		})
		// 针对 https://issues.k8s.io/118472 扩展场景的回归测试
		ginkgo.It("should force-delete non-admissible pods that was admitted and running before kubelet restart", func(ctx context.Context) {
			nodeLabelKey := "custom-label-key-required"
			nodeLabelValueRequired := "custom-label-value-required-for-admission"
			podName := "rejected-deleted-run" + string(uuid.NewUUID())
			gracePeriod := int64(30)
			nodeName := getNodeName(ctx, f)
			pod := e2epod.MustMixinRestrictedPodSecurity(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: f.Namespace.Name,
				},
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						nodeLabelKey: nodeLabelValueRequired,
					},
					NodeName:                      nodeName,
					TerminationGracePeriodSeconds: &gracePeriod,
					RestartPolicy:                 v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:  podName,
							Image: imageutils.GetPauseImageName(),
						},
					},
				},
			})

			ginkgo.By(fmt.Sprintf("Adding node label for node (%v) to allow admission of pod (%v/%v)", nodeName, f.Namespace.Name, podName))
			e2enode.AddOrUpdateLabelOnNode(f.ClientSet, nodeName, nodeLabelKey, nodeLabelValueRequired)
			ginkgo.DeferCleanup(func() { e2enode.RemoveLabelOffNode(f.ClientSet, nodeName, nodeLabelKey) })

			// 创建绑定到该节点的 Pod。它会启动，但 kubelet 重启后会被拒绝。
			ginkgo.By(fmt.Sprintf("Creating a pod (%v/%v)", f.Namespace.Name, podName))
			pod = e2epod.NewPodClient(f).Create(ctx, pod)

			ginkgo.By(fmt.Sprintf("Waiting for the pod (%v/%v) to be running", f.Namespace.Name, pod.Name))
			err := e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err, "Failed to await for the pod to be running: (%v/%v)", f.Namespace.Name, pod.Name)

			ginkgo.By("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			ginkgo.By(fmt.Sprintf("Deleting the pod (%v/%v) to set a deletion timestamp", pod.Namespace, pod.Name))
			err = e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
			framework.ExpectNoError(err, "Failed to delete the pod: %q", pod.Name)

			ginkgo.By(fmt.Sprintf("Removing node label for node (%v) to ensure the pod (%v/%v) is rejected after kubelet restart", nodeName, f.Namespace.Name, podName))
			e2enode.RemoveLabelOffNode(f.ClientSet, nodeName, nodeLabelKey)

			// 重启 Kubelet，使其继续执行删除
			ginkgo.By("Restarting the kubelet")
			restartKubelet(ctx)

			// 等待 Kubelet 就绪。
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				nodes, err := e2enode.TotalReady(ctx, f.ClientSet)
				framework.ExpectNoError(err)
				return nodes == 1
			}, time.Minute, f.Timeouts.Poll).Should(gomega.BeTrueBecause("expected kubelet to be in ready state"))

			ginkgo.By(fmt.Sprintf("Once Kubelet is restarted, verify the pod (%v/%v) is deleted by kubelet", pod.Namespace, pod.Name))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, pod.Name, pod.Namespace)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())
		})
	})

})
