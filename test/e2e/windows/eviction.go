/*
Copyright 2024 The Kubernetes Authors.

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
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// 节点内存压力 taint 可能需要 10-15 秒才出现在节点上，
	// 所以等待 45 秒让 taint 出现，以便 e2e 测试用例能捕获
	// 它，并等待 taint 被移除，让其他串行/慢速测试能
	// 在同一节点上运行。
	waitForNodeMemoryPressureTaintDelayDuration = 45 * time.Second

	// eviction pod namespace 基础名称
	evictionPodNamespaceBaseName = "eviction-test-windows"
)

var _ = sigDescribe(feature.Windows, "Eviction", framework.WithSerial(), framework.WithSlow(), framework.WithDisruptive(), func() {
	ginkgo.BeforeEach(func() {
		e2eskipper.SkipUnlessNodeOSDistroIs("windows")
	})

	f := framework.NewDefaultFramework(evictionPodNamespaceBaseName)
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 这个测试先找到一个开启内存压力硬驱逐的 Windows 节点。
	// 然后调度一个请求并消耗 500Mi 内存的 pod，再调度
	// 另一个消耗节点剩余内存的 pod。
	// 然后验证第二个 pod 被驱逐，且节点在驱逐后
	// 重新变为可调度。
	ginkgo.It("should evict a pod when a node experiences memory pressure", func(ctx context.Context) {
		framework.Logf("Looking for a Windows node with memory-pressure eviction enabled")
		selector := labels.Set{"kubernetes.io/os": "windows"}.AsSelector()
		nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: selector.String(),
		})
		framework.ExpectNoError(err)

		var node *v1.Node
		var nodeMem nodeMemory
		for _, n := range nodeList.Items {
			// Due to a known issue (https://github.com/projectcalico/calico/issues/6974),
			// Windows 节点重启后 pod 可能变得无法删除。因此，
			// eviction manager 可能把这些 pod 列为驱逐目标，但无法删除它们。
			// TODO: 上游问题修复后移除这个 workaround。
			if n.Labels["test/reboot-used"] == "true" {
				framework.Logf("Skipping node %s because it was used for reboot test", n.Name)
				continue
			}

			nm := getNodeMemory(ctx, f, n)
			if nm.hardEviction.Value() != 0 {
				framework.Logf("Using node %s", n.Name)
				node = &n
				nodeMem = nm
				break
			}
		}
		//lint:ignore SA5011 If node == nil we are already skipping test.
		if node == nil {
			e2eskipper.Skipf("No Windows nodes with hard memory-pressure eviction found")
		}

		framework.Logf("Node %q capacity: %v Mi", node.Name, nodeMem.capacity.Value()/(1024*1024))
		framework.Logf("Node %q hard eviction threshold: %v Mi", node.Name, nodeMem.hardEviction.Value()/(1024*1024))
		framework.Logf("Available memory before eviction: %v Mi", (nodeMem.capacity.Value()-nodeMem.hardEviction.Value())/(1024*1024))

		err = waitForMemoryPressureTaintRemoval(ctx, f, node.Name, 10*time.Minute)
		framework.ExpectNoError(err, "Timed out waiting for memory-pressure taint to be removed from node %q", node.Name)

		ginkgo.DeferCleanup(f.DeleteNamespace, f.Namespace.Name)

		ginkgo.By("Scheduling a pod that requests and consumes 500Mi of Memory")

		pod1 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod1",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "pod1",
						Image: imageutils.GetE2EImage(imageutils.ResourceConsumer),
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: *resource.NewQuantity(500*1024*1024, resource.BinarySI),
							},
						},
						Command: []string{
							"/bin/testlimit.exe",
							"-accepteula",
							"-d",
							"100Mb",
							"-e",
							"5",
							"20000s",
							"-c",
							"5"},
					},
				},
				NodeSelector: map[string]string{
					"kubernetes.io/os": "windows",
				},
				NodeName: node.Name,
			},
		}
		pod1, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod1)
		framework.ExpectNoError(err)

		ginkgo.By("Scheduling another pod will consume the rest of the node's memory")
		chunks := int((nodeMem.capacity.Value()-nodeMem.hardEviction.Value())/(300*1024*1024) + 3)
		framework.Logf("Pod2 will request approximately %v Mi total memory (%d chunks × 300Mi)", chunks*300, chunks)

		pod2 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod2",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "pod2",
						Image: imageutils.GetE2EImage(imageutils.ResourceConsumer),
						Command: []string{
							"/bin/testlimit.exe",
							"-accepteula",
							"-d",
							"300Mb",
							"-e",
							"1",
							"20000s",
							"-c",
							strconv.Itoa(chunks)},
					},
				},
				NodeSelector: map[string]string{
					"kubernetes.io/os": "windows",
				},
				NodeName: node.Name,
			},
		}
		pod2, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By(fmt.Sprintf("Waiting for pod2 running on node %q, in namespace %q", node.Name, f.Namespace.Name))
		err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod2)
		framework.ExpectNoError(err)

		framework.Logf("Waiting for pod2 to be evicted")

		gomega.Eventually(ctx, func() error {
			// 获取更新后的节点信息
			node, err = f.ClientSet.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node: %w", err)
			}

			// 记录节点内存压力 condition 和 taint
			for _, cond := range node.Status.Conditions {
				if cond.Type == v1.NodeMemoryPressure {
					framework.Logf("Node condition: MemoryPressure = %v (Reason: %s, Message: %s)",
						cond.Status, cond.Reason, cond.Message)
				}
			}
			for _, taint := range node.Spec.Taints {
				framework.Logf("Node %q has taint %q (Effect: %q)", node.Name, taint.Key, taint.Effect)
			}

			// 检查 namespace 中的驱逐事件
			events, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list events: %w", err)
			}

			evicted := false
			for _, e := range events.Items {
				if e.Reason == "Evicted" {
					if e.InvolvedObject.Name == pod2.Name {
						framework.Logf("Eviction event for pod2: %q", e.Message)
						evicted = true
					} else {
						framework.Logf("Eviction event for other pod %q: %q", e.InvolvedObject.Name, e.Message)
					}
				}
				if e.InvolvedObject.Name == pod2.Name {
					framework.Logf("Event for pod2: Type=%s, Reason=%s, Message=%q", e.Type, e.Reason, e.Message)
				}
			}

			if evicted {
				return nil
			}
			return fmt.Errorf("pod2 not evicted yet; still waiting")
		}).WithTimeout(10*time.Minute).WithPolling(10*time.Second).Should(gomega.Succeed(), "pod2 should eventually be evicted")

		ginkgo.By("Waiting for node.kubernetes.io/memory-pressure taint to be removed")
		// 确保 e2e test framework 捕获到内存压力 taint
		time.Sleep(waitForNodeMemoryPressureTaintDelayDuration)
		// 等待 node.kubernetes.io/memory-pressure=NoSchedule 被移除，让其他测试能运行
		err = e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 10*time.Minute)
		framework.ExpectNoError(err)
	})
})

func waitForMemoryPressureTaintRemoval(ctx context.Context, f *framework.Framework, nodeName string, timeout time.Duration) error {
	framework.Logf("Waiting for memory-pressure taint to be removed from node %q", nodeName)
	return wait.PollUntilContextTimeout(ctx, 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Failed to get node %q: %v", nodeName, err)
			return false, err
		}

		// 记录节点 condition
		for _, cond := range node.Status.Conditions {
			if cond.Type == v1.NodeMemoryPressure {
				framework.Logf("Node condition: MemoryPressure = %v (reason: %s, message: %s)", cond.Status, cond.Reason, cond.Message)
			}
		}

		// 记录 taint
		hasTaint := false
		for _, taint := range node.Spec.Taints {
			if taint.Key == v1.TaintNodeMemoryPressure && taint.Effect == v1.TaintEffectNoSchedule {
				framework.Logf("Node %q still has memory-pressure taint (Effect: %s, TimeAdded: %v)", nodeName, taint.Effect, taint.TimeAdded)
				hasTaint = true
			}
		}

		// 记录节点上的所有 pod
		podList, err := f.ClientSet.CoreV1().Pods("").List(ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
		})
		if err != nil {
			framework.Logf("Failed to list pods on node %q: %v", nodeName, err)
		} else {
			for _, pod := range podList.Items {
				framework.Logf("Pod %q in ns %q phase: %s", pod.Name, pod.Namespace, pod.Status.Phase)
			}
		}

		if !hasTaint {
			framework.Logf("Memory-pressure taint has been removed from node %q", nodeName)
			return true, nil
		}
		return false, nil
	})
}
