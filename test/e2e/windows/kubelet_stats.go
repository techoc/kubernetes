/*
Copyright 2020 The Kubernetes Authors.

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
	"fmt"
	"k8s.io/klog/v2"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = sigDescribe(feature.Windows, "Kubelet-Stats", framework.WithSerial(), skipUnlessWindows(func() {
	f := framework.NewDefaultFramework("kubelet-stats-test-windows-serial")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("Kubelet stats collection for Windows nodes", func() {

		ginkgo.Context("when running up to 10 pods", func() {
			// 10 秒是 metrics-server 和 kube-prometheus 的默认抓取超时
			ginkgo.It("should return within 10 seconds", func(ctx context.Context) {

				ginkgo.By("Selecting a Windows node")
				targetNode, err := findWindowsNode(ctx, f)
				framework.ExpectNoError(err, "Error finding Windows node")
				framework.Logf("Using node: %v", targetNode.Name)

				// 根据集群准入链为每个 Pod 注入的额外开销（Pod Overhead，KEP-688）调整 Pod 数量；
				// 开销为 0 时这是空操作。
				const baselineNumPods = 10
				const minNumPods = 3
				numPods := baselineNumPods
				overhead, err := detectPodOverheadMemory(ctx, f.ClientSet, f.Namespace.Name)
				framework.ExpectNoError(err, "detecting pod overhead memory")
				if overhead > 0 {
					// 使用准入视角的核算（allocatable - 已准入请求之和，含开销），
					// 而不是 /stats/summary 的 Memory.AvailableBytes：后者是 working-set 视角，
					// 当上一个 [Serial] 测试还有 Terminating Pod 时可能与准入结果不一致。
					allocatable := targetNode.Status.Allocatable.Memory().Value()
					waitForNodeMemoryToSettle(ctx, f.ClientSet, targetNode.Name, overhead+windowsTestMemorySafetyBuffer)
					existing := sumExistingPodMemoryReservation(ctx, f.ClientSet, targetNode.Name)
					maxPods := (allocatable - existing - windowsTestMemorySafetyBuffer) / overhead
					if maxPods < int64(numPods) {
						numPods = int(maxPods)
					}
					// 节点没有容量调度最低要求的 Pod 数量时失败。这表明测试集群配置有误
					//（allocatable 太小、现有预留意外过大，
					// 或 Pod 开销高于集群规划值）；应显式报错，
					// 而不是静默跳过测试。
					//
					if numPods < minNumPods {
						framework.Failf("Node %s has insufficient memory capacity to schedule %d pods: allocatable=%d, existing-reservation=%d, per-pod-overhead=%d, safety-buffer=%d, max-pods=%d. Right-size the test cluster, lower per-pod overhead, or reduce existing reservation.",
							targetNode.Name, minNumPods, allocatable, existing, overhead, windowsTestMemorySafetyBuffer, maxPods)
					}
					framework.Logf("Adjusted pod count to %d (baseline=%d, overhead=%d bytes, allocatable=%d bytes, existing-reservation=%d bytes, safety-buffer=%d bytes)",
						numPods, baselineNumPods, overhead, allocatable, existing, windowsTestMemorySafetyBuffer)
				}

				ginkgo.By(fmt.Sprintf("Scheduling %d pods", numPods))
				powershellImage := imageutils.GetConfig(imageutils.BusyBox)
				pods := newKubeletStatsTestPods(numPods, powershellImage, targetNode.Name)
				e2epod.NewPodClient(f).CreateBatch(ctx, pods)

				ginkgo.By(fmt.Sprintf("Waiting up to 3 minutes for %d pods to be running", numPods))
				timeout := 3 * time.Minute
				err = e2epod.WaitForPodsRunningReady(ctx, f.ClientSet, f.Namespace.Name, numPods, timeout)
				framework.ExpectNoError(err)

				ginkgo.By("Getting kubelet stats 5 times and checking average duration")
				iterations := 5
				var totalDurationMs int64

				for range iterations {
					start := time.Now()
					nodeStats, err := e2ekubelet.GetStatsSummary(ctx, f.ClientSet, targetNode.Name)
					duration := time.Since(start)
					totalDurationMs += duration.Milliseconds()

					framework.ExpectNoError(err, "Error getting kubelet stats")

					// 对本次测试命名空间中 Pod 的统计信息做基本健全性检查
					statsChecked := 0
					for _, podStats := range nodeStats.Pods {
						if podStats.PodRef.Namespace != f.Namespace.Name {
							continue
						}
						statsChecked = statsChecked + 1

						if *podStats.CPU.UsageCoreNanoSeconds <= 0 {
							framework.Failf("Pod %s/%s stats report cpu usage equal to %v but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *podStats.CPU.UsageCoreNanoSeconds)
						}
						if *podStats.Memory.WorkingSetBytes <= 0 {
							framework.Failf("Pod %s/%s stats report bytes for memory working set equal to %v but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *podStats.Memory.WorkingSetBytes)
						}

						for _, containerStats := range podStats.Containers {
							if containerStats.Logs == nil {
								framework.Failf("Pod %s/%s stats should have container log stats, but it is nil", podStats.PodRef.Namespace, podStats.PodRef.Name)
							}
							if *containerStats.Logs.AvailableBytes <= 0 {
								framework.Failf("Pod %s/%s container log stats report %v bytes for AvailableBytes, but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *containerStats.Logs.AvailableBytes)
							}
						}
					}
					gomega.Expect(statsChecked).To(gomega.Equal(numPods), fmt.Sprintf("Should find stats for %d pods in kubelet stats", numPods))

					time.Sleep(5 * time.Second)
				}

				avgDurationMs := totalDurationMs / int64(iterations)

				durationMatch := avgDurationMs <= time.Duration(10*time.Second).Milliseconds()
				framework.Logf("Getting kubelet stats for node %v took an average of %v milliseconds over %v iterations", targetNode.Name, avgDurationMs, iterations)
				if !durationMatch {
					framework.Failf("Collecting kubelet stats took %v seconds but it should not take longer than 10 seconds", durationMatch)
				}
			})
		})
	})
}))

var _ = sigDescribe(feature.Windows, "Kubelet-Stats", skipUnlessWindows(func() {
	f := framework.NewDefaultFramework("kubelet-stats-test-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("Kubelet stats collection for Windows nodes", func() {

		ginkgo.Context("when windows is booted", func() {
			ginkgo.It("should return bootid within 10 seconds", func(ctx context.Context) {
				ginkgo.By("Selecting a Windows node")
				targetNode, err := findWindowsNode(ctx, f)
				framework.ExpectNoError(err, "Error finding Windows node")
				framework.Logf("Using node: %v", targetNode.Name)

				ginkgo.By("Getting bootid")
				if len(targetNode.Status.NodeInfo.BootID) == 0 {
					framework.Failf("expected bootId in kubelet stats, got none")
				}
			})
		})

		ginkgo.Context("when running 3 pods", func() {
			// 10 秒是 metrics-server 和 kube-prometheus 的默认抓取超时
			ginkgo.It("should return within 10 seconds", func(ctx context.Context) {

				ginkgo.By("Selecting a Windows node")
				targetNode, err := findWindowsNode(ctx, f)
				framework.ExpectNoError(err, "Error finding Windows node")
				framework.Logf("Using node: %v", targetNode.Name)

				// 计入开销后节点若没有足够容量运行所需 Pod 数则失败。测试集群配置有误时应显式报错，
				// 而不是静默跳过。集群不注入开销时这是空操作。
				// 而不是静默跳过。集群不注入开销时这是空操作。
				const numPods = 3
				overhead, err := detectPodOverheadMemory(ctx, f.ClientSet, f.Namespace.Name)
				framework.ExpectNoError(err, "detecting pod overhead memory")
				if overhead > 0 {
					allocatable := targetNode.Status.Allocatable.Memory().Value()
					needed := int64(numPods)*overhead + windowsTestMemorySafetyBuffer
					waitForNodeMemoryToSettle(ctx, f.ClientSet, targetNode.Name, needed)
					existing := sumExistingPodMemoryReservation(ctx, f.ClientSet, targetNode.Name)
					free := allocatable - existing - windowsTestMemorySafetyBuffer
					if free < int64(numPods)*overhead {
						framework.Failf("Node %s has insufficient memory capacity to schedule %d pods: allocatable=%d, existing-reservation=%d, per-pod-overhead=%d, safety-buffer=%d, free=%d (need %d). Right-size the test cluster, lower per-pod overhead, or reduce existing reservation.",
							targetNode.Name, numPods, allocatable, existing, overhead, windowsTestMemorySafetyBuffer, free, int64(numPods)*overhead)
					}
				}

				ginkgo.By("Scheduling 3 pods")
				powershellImage := imageutils.GetConfig(imageutils.BusyBox)
				pods := newKubeletStatsTestPods(3, powershellImage, targetNode.Name)
				e2epod.NewPodClient(f).CreateBatch(ctx, pods)

				ginkgo.By("Waiting up to 3 minutes for pods to be running")
				timeout := 3 * time.Minute
				err = e2epod.WaitForPodsRunningReady(ctx, f.ClientSet, f.Namespace.Name, 3, timeout)
				framework.ExpectNoError(err)

				ginkgo.By("Getting kubelet stats 1 time")
				iterations := 1
				var totalDurationMs int64

				for range iterations {
					start := time.Now()
					nodeStats, err := e2ekubelet.GetStatsSummary(ctx, f.ClientSet, targetNode.Name)
					duration := time.Since(start)
					totalDurationMs += duration.Milliseconds()

					framework.ExpectNoError(err, "Error getting kubelet stats")

					// 对本次测试命名空间中 Pod 的统计信息做基本健全性检查
					statsChecked := 0
					for _, podStats := range nodeStats.Pods {
						if podStats.PodRef.Namespace != f.Namespace.Name {
							continue
						}
						statsChecked = statsChecked + 1

						if *podStats.CPU.UsageCoreNanoSeconds <= 0 {
							framework.Failf("Pod %s/%s stats report cpu usage equal to %v but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *podStats.CPU.UsageCoreNanoSeconds)
						}
						if *podStats.Memory.WorkingSetBytes <= 0 {
							framework.Failf("Pod %s/%s stats report bytes for memory working set equal to %v but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *podStats.Memory.WorkingSetBytes)
						}

						for _, containerStats := range podStats.Containers {
							if containerStats.Logs == nil {
								framework.Failf("Pod %s/%s stats should have container log stats, but it is nil", podStats.PodRef.Namespace, podStats.PodRef.Name)
							}
							if *containerStats.Logs.AvailableBytes <= 0 {
								framework.Failf("Pod %s/%s container log stats report %v bytes for AvailableBytes, but it should be greater than 0", podStats.PodRef.Namespace, podStats.PodRef.Name, *containerStats.Logs.AvailableBytes)
							}
						}
					}
					gomega.Expect(statsChecked).To(gomega.Equal(3), "Should find stats for 3 pods in kubelet stats")

					time.Sleep(5 * time.Second)
				}

				avgDurationMs := totalDurationMs / int64(iterations)

				durationMatch := avgDurationMs <= time.Duration(10*time.Second).Milliseconds()
				framework.Logf("Getting kubelet stats for node %v took an average of %v milliseconds over %v iterations", targetNode.Name, avgDurationMs, iterations)
				if !durationMatch {
					framework.Failf("Collecting kubelet stats took %v seconds but it should not take longer than 10 seconds", durationMatch)
				}
			})
		})
	})
}))

// findWindowsNode 查找 Ready 且可调度的 Windows 节点
func findWindowsNode(ctx context.Context, f *framework.Framework) (v1.Node, error) {
	logger := klog.FromContext(ctx)
	selector := labels.Set{"kubernetes.io/os": "windows"}.AsSelector()
	nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector.String()})

	if err != nil {
		return v1.Node{}, err
	}

	var targetNode v1.Node
	foundNode := false
	for _, n := range nodeList.Items {
		if e2enode.IsNodeReady(logger, &n) && e2enode.IsNodeSchedulable(logger, &n) {
			targetNode = n
			foundNode = true
			break
		}
	}

	if !foundNode {
		e2eskipper.Skipf("Could not find and ready and schedulable Windows nodes")
	}

	return targetNode, nil
}

// findWindowsNodes 查找所有 Ready 且可调度的 Windows 节点
func findWindowsNodes(ctx context.Context, f *framework.Framework) ([]v1.Node, error) {
	logger := klog.FromContext(ctx)
	selector := labels.Set{"kubernetes.io/os": "windows"}.AsSelector()
	nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector.String()})

	if err != nil {
		return nil, err
	}

	var targetNodes []v1.Node
	for _, n := range nodeList.Items {
		if e2enode.IsNodeReady(logger, &n) && e2enode.IsNodeSchedulable(logger, &n) {
			targetNodes = append(targetNodes, n)
		}
	}

	return targetNodes, nil
}

// newKubeletStatsTestPods 为测试创建一组 Pod spec。
func newKubeletStatsTestPods(numPods int, image imageutils.Config, nodeName string) []*v1.Pod {
	var pods []*v1.Pod

	for i := range numPods {
		podName := "statscollectiontest-" + string(uuid.NewUUID())
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-%d", podName, i),
				Labels: map[string]string{
					"name":    podName,
					"testapp": "stats-collection",
				},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Image: image.GetE2EImage(),
						Name:  "stat-container",
						Command: []string{
							"powershell.exe",
							"-Command",
							"sleep -Seconds 600",
						},
					},
				},
				InitContainers: []v1.Container{
					{
						Image: image.GetE2EImage(),
						Name:  "init-container",
						Command: []string{
							"powershell.exe",
							"-Command",
							"sleep -Seconds 1",
						},
					},
				},
				NodeName: nodeName,
			},
		}

		pods = append(pods, &pod)
	}

	return pods
}
