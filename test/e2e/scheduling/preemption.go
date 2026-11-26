/*
Copyright 2017 The Kubernetes Authors.

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

package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2ereplicaset "k8s.io/kubernetes/test/e2e/framework/replicaset"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

type priorityPair struct {
	name  string
	value int32
}

var testExtendedResource = v1.ResourceName("scheduling.k8s.io/foo")

const (
	testFinalizer = "example.com/test-finalizer"
)

var _ = SIGDescribe("SchedulerPreemption", framework.WithSerial(), func() {
	var cs clientset.Interface
	var nodeList *v1.NodeList
	var ns string
	f := framework.NewDefaultFramework("sched-preemption")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	lowPriority, mediumPriority, highPriority := int32(1), int32(100), int32(1000)
	lowPriorityClassName := f.BaseName + "-low-priority"
	mediumPriorityClassName := f.BaseName + "-medium-priority"
	highPriorityClassName := f.BaseName + "-high-priority"
	priorityPairs := []priorityPair{
		{name: lowPriorityClassName, value: lowPriority},
		{name: mediumPriorityClassName, value: mediumPriority},
		{name: highPriorityClassName, value: highPriority},
	}

	ginkgo.AfterEach(func(ctx context.Context) {
		for _, pair := range priorityPairs {
			_ = cs.SchedulingV1().PriorityClasses().Delete(ctx, pair.name, *metav1.NewDeleteOptions(0))
		}
		for _, node := range nodeList.Items {
			e2enode.RemoveExtendedResource(ctx, cs, node.Name, testExtendedResource)
		}
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		cs = f.ClientSet
		ns = f.Namespace.Name
		nodeList = &v1.NodeList{}
		var err error
		for _, pair := range priorityPairs {
			_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: pair.name}, Value: pair.value}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				framework.Failf("expected 'alreadyExists' as error, got instead: %v", err)
			}
		}

		e2enode.WaitForTotalHealthy(ctx, cs, time.Minute)
		nodeList, err = e2enode.GetReadySchedulableNodes(ctx, cs)
		if err != nil {
			framework.Logf("Unexpected error occurred: %v", err)
		}
		framework.ExpectNoErrorWithOffset(0, err)
		for _, n := range nodeList.Items {
			workerNodes.Insert(n.Name)
		}

		err = framework.CheckTestingNSDeletedExcept(ctx, cs, ns)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.19
		测试名称：Scheduler, Basic Preemption
		描述：创建高优先级 pod 后，如果找不到资源充足的
		节点，scheduler 必须抢占低优先级 pod，并
		调度高优先级 pod。
	*/
	framework.ConformanceIt("validates basic preemption works", func(ctx context.Context) {
		var podRes v1.ResourceList

		// 每个节点创建两个消耗大量节点资源的 pod。
		ginkgo.By("Create pods that use 4/5 of node resources.")
		pods := make([]*v1.Pod, 0, 2*len(nodeList.Items))
		// 在集群中创建 pod。
		// 其中一个优先级低，作为被抢占的受害者。
		for i, node := range nodeList.Items {
			// 更新每个节点，通告 3 个可用的扩展资源
			e2enode.AddExtendedResource(ctx, cs, node.Name, testExtendedResource, resource.MustParse("5"))

			for j := range 2 {
				// 为受害者 pod 请求可用资源中的 2 个
				podRes = v1.ResourceList{}
				podRes[testExtendedResource] = resource.MustParse("2")

				// 让第一个 pod 为低优先级，其余为中等优先级。
				priorityName := mediumPriorityClassName
				if len(pods) == 0 {
					priorityName = lowPriorityClassName
				}
				pausePod := createPausePod(ctx, f, pausePodConfig{
					Name:              fmt.Sprintf("pod%d-%d-%v", i, j, priorityName),
					PriorityClassName: priorityName,
					Resources: &v1.ResourceRequirements{
						Requests: podRes,
						Limits:   podRes,
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{node.Name}},
										},
									},
								},
							},
						},
					},
				})
				pods = append(pods, pausePod)
				framework.Logf("Created pod: %v", pausePod.Name)
			}
		}
		if len(pods) < 2 {
			framework.Failf("We need at least two pods to be created but " +
				"all nodes are already heavily utilized, so preemption tests cannot be run")
		}
		ginkgo.By("Wait for pods to be scheduled.")
		for _, pod := range pods {
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod))
		}

		// 把 pod request 设为第一个 pod 的资源（应该是低优先级 pod）
		podRes = pods[0].Spec.Containers[0].Resources.Requests

		ginkgo.By("Run a high priority pod that has same requirements as that of lower priority pod")
		// 创建高优先级 pod，确认它与低优先级 pod 调度到同一节点。
		runPausePod(ctx, f, pausePodConfig{
			Name:              "preemptor-pod",
			PriorityClassName: highPriorityClassName,
			Resources: &v1.ResourceRequirements{
				Requests: podRes,
				Limits:   podRes,
			},
		})

		preemptedPod, err := cs.CoreV1().Pods(pods[0].Namespace).Get(ctx, pods[0].Name, metav1.GetOptions{})
		podPreempted := (err != nil && apierrors.IsNotFound(err)) ||
			(err == nil && preemptedPod.DeletionTimestamp != nil)
		if !podPreempted {
			framework.Failf("expected pod to be preempted, instead got pod %+v and error %v", preemptedPod, err)
		}
		for i := 1; i < len(pods); i++ {
			livePod, err := cs.CoreV1().Pods(pods[i].Namespace).Get(ctx, pods[i].Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(livePod.DeletionTimestamp).To(gomega.BeNil())
		}
	})

	/*
		发布版本：v1.19
		测试名称：Scheduler, Preemption for critical pod
		描述：创建 critical pod 后，如果找不到资源充足的
		节点，scheduler 必须抢占低优先级 pod 来
		调度 critical pod。
	*/
	framework.ConformanceIt("validates lower priority pod preemption by critical pod", func(ctx context.Context) {
		var podRes v1.ResourceList

		ginkgo.By("Create pods that use 4/5 of node resources.")
		pods := make([]*v1.Pod, 0, len(nodeList.Items))
		for i, node := range nodeList.Items {
			// 更新每个节点，通告 3 个可用的扩展资源
			e2enode.AddExtendedResource(ctx, cs, node.Name, testExtendedResource, resource.MustParse("5"))

			for j := range 2 {
				// 为受害者 pod 请求可用资源中的 2 个
				podRes = v1.ResourceList{}
				podRes[testExtendedResource] = resource.MustParse("2")

				// 让第一个 pod 为低优先级，其余为中等优先级。
				priorityName := mediumPriorityClassName
				if len(pods) == 0 {
					priorityName = lowPriorityClassName
				}
				pausePod := createPausePod(ctx, f, pausePodConfig{
					Name:              fmt.Sprintf("pod%d-%d-%v", i, j, priorityName),
					PriorityClassName: priorityName,
					Resources: &v1.ResourceRequirements{
						Requests: podRes,
						Limits:   podRes,
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{node.Name}},
										},
									},
								},
							},
						},
					},
				})
				pods = append(pods, pausePod)
				framework.Logf("Created pod: %v", pausePod.Name)
			}
		}
		if len(pods) < 2 {
			framework.Failf("We need at least two pods to be created but " +
				"all nodes are already heavily utilized, so preemption tests cannot be run")
		}
		ginkgo.By("Wait for pods to be scheduled.")
		for _, pod := range pods {
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod))
		}

		// 我们希望这个 pod 被抢占
		podRes = pods[0].Spec.Containers[0].Resources.Requests
		ginkgo.By("Run a critical pod that use same resources as that of a lower priority pod")
		// 创建 critical pod，确认它被调度。
		defer func() {
			// 清理 critical pod
			// 始终执行清理，确保 pod 被正确清理。
			err := f.ClientSet.CoreV1().Pods(metav1.NamespaceSystem).Delete(ctx, "critical-pod", *metav1.NewDeleteOptions(0))
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Failf("Error cleanup pod `%s/%s`: %v", metav1.NamespaceSystem, "critical-pod", err)
			}
		}()
		runPausePod(ctx, f, pausePodConfig{
			Name:              "critical-pod",
			Namespace:         metav1.NamespaceSystem,
			PriorityClassName: scheduling.SystemClusterCritical,
			Resources: &v1.ResourceRequirements{
				Requests: podRes,
				Limits:   podRes,
			},
		})

		defer func() {
			// 清理 critical pod
			err := f.ClientSet.CoreV1().Pods(metav1.NamespaceSystem).Delete(ctx, "critical-pod", *metav1.NewDeleteOptions(0))
			framework.ExpectNoError(err)
		}()
		// 确保最低优先级的 pod 被删除。
		preemptedPod, err := cs.CoreV1().Pods(pods[0].Namespace).Get(ctx, pods[0].Name, metav1.GetOptions{})
		podPreempted := (err != nil && apierrors.IsNotFound(err)) ||
			(err == nil && preemptedPod.DeletionTimestamp != nil)
		for i := 1; i < len(pods); i++ {
			livePod, err := cs.CoreV1().Pods(pods[i].Namespace).Get(ctx, pods[i].Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(livePod.DeletionTimestamp).To(gomega.BeNil())
		}

		if !podPreempted {
			framework.Failf("expected pod to be preempted, instead got pod %+v and error %v", preemptedPod, err)
		}
	})

	/*
		发布版本：v1.32
		测试名称：Scheduler runs the preemption with various priority classes expectedly
		描述：当具有不同 priority class 的 pod 参与抢占时，
		scheduler 必须优先处理 priority class 更高的 Pod。
	*/
	framework.It("validates various priority Pods preempt expectedly with the async preemption", feature.SchedulerAsyncPreemption, framework.WithFeatureGate(features.SchedulerAsyncPreemption), func(ctx context.Context) {
		var podRes v1.ResourceList
		// 每个节点创建 10 个 pod，占满节点所有资源。
		ginkgo.By("Create 10 low-priority pods on each node.")
		nodeListLen := len(nodeList.Items)
		lowPriorityPods := make([]*v1.Pod, 0, 10*nodeListLen)
		// 在集群中创建 pod。
		for i, node := range nodeList.Items {
			// 更新每个节点，通告 3 个可用的扩展资源
			e2enode.AddExtendedResource(ctx, cs, node.Name, testExtendedResource, resource.MustParse("10"))

			// 每个节点创建 10 个低优先级 pod，占满节点 10/10 的资源。
			for j := range 10 {
				// 为受害者 pod 请求可用资源中的 1 个
				podRes = v1.ResourceList{}
				podRes[testExtendedResource] = resource.MustParse("1")
				pausePod := createPausePod(ctx, f, pausePodConfig{
					Name:              fmt.Sprintf("pod%d-%d-%v", i, j, lowPriorityClassName),
					PriorityClassName: lowPriorityClassName,
					Resources: &v1.ResourceRequirements{
						Requests: podRes,
						Limits:   podRes,
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{node.Name}},
										},
									},
								},
							},
						},
					},
					// 这个受害者 pod 会被高优先级 pod 抢占。
					// 但删除会被设置了
					// TerminationGracePeriodSeconds 的 preStop hook 阻塞。
					//
					// 需要 preStop hook 加 TerminationGracePeriodSeconds 来阻止中等优先级 Pod
					// 被调度而不是高优先级 Pod，
					// 这取决于 scheduler 何时发现我们创建的所有高优先级 Pod。
					TerminationGracePeriodSeconds: ptr.To[int64](80),
					PreStopHookSleepSeconds:       ptr.To[int64](79),
				})
				lowPriorityPods = append(lowPriorityPods, pausePod)
				framework.Logf("Created pod: %v", pausePod.Name)
			}
		}

		ginkgo.By("Wait for lower priority pods to be scheduled.")
		for _, pod := range lowPriorityPods {
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod))
		}

		highPriorityPods := make([]*v1.Pod, 0, 5*nodeListLen)
		mediumPriorityPods := make([]*v1.Pod, 0, 10*nodeListLen)

		ginkgo.By("Run medium priority pods that have same requirements as that of lower priority pod")
		for i := range nodeList.Items {
			// 先创建中等优先级 pod
			// 以确认 scheduler 最终优先处理高优先级 pod，忽略中等优先级 pod。
			for j := range 10 {
				// 每个节点有 5 个 pod 无法调度
				// 因为节点只有 10 个资源，高优先级 pod 会用掉 5 个。
				p := createPausePod(ctx, f, pausePodConfig{
					Name:              fmt.Sprintf("pod%d-%d-%v", i, j, mediumPriorityClassName),
					PriorityClassName: mediumPriorityClassName,
					Resources: &v1.ResourceRequirements{
						// 把 pod request 设为低优先级 pod 的资源
						Requests: lowPriorityPods[0].Spec.Containers[0].Resources.Requests,
						Limits:   lowPriorityPods[0].Spec.Containers[0].Resources.Requests,
					},
				})
				mediumPriorityPods = append(mediumPriorityPods, p)
			}
		}

		// 所有低优先级 Pod 都应成为抢占目标。
		// 这些 Pod 带 finalizer，因此此时不应被删除。
		ginkgo.By("Check all low priority pods to be about to preempted.")
		for _, pod := range lowPriorityPods {
			framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, time.Second, framework.PodStartTimeout, false, func(ctx context.Context) (bool, error) {
				preemptedPod, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				return preemptedPod.DeletionTimestamp != nil, nil
			}))
		}

		ginkgo.By("Run high priority pods that have same requirements as that of lower priority pod")
		for i := range nodeList.Items {
			for j := range 5 {
				p := createPausePod(ctx, f, pausePodConfig{
					Name:              fmt.Sprintf("pod%d-%d-%v", i, j, highPriorityClassName),
					PriorityClassName: highPriorityClassName,
					Resources: &v1.ResourceRequirements{
						// 把 pod request 设为低优先级 pod 的资源
						Requests: lowPriorityPods[0].Spec.Containers[0].Resources.Requests,
						Limits:   lowPriorityPods[0].Spec.Containers[0].Resources.Requests,
					},
				})
				highPriorityPods = append(highPriorityPods, p)
			}
		}

		// 移除低优先级 Pod 后，所有高优先级 Pod 都应可调度。
		ginkgo.By("Wait for high priority pods to be ready for the preemption.")
		for _, pod := range highPriorityPods {
			framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, time.Second, framework.PodStartTimeout, false, func(ctx context.Context) (bool, error) {
				highPod, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				return highPod.Status.NominatedNodeName != "", nil
			}))
		}

		ginkgo.By("Delete all low priority pods to proceed the preemption faster.")
		for _, pod := range lowPriorityPods {
			err := cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Logf("Deleting %v pod failed: %v", pod.Name, err)
			}
		}

		ginkgo.By("Wait for high priority pods to be scheduled.")
		for _, pod := range highPriorityPods {
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod))
		}

		ginkgo.By(fmt.Sprintf("Wait for %v medium priority pods to be scheduled.", 5*nodeListLen))
		framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, time.Second, framework.PodStartTimeout, false, func(ctx context.Context) (bool, error) {
			scheduled := 0
			for _, pod := range mediumPriorityPods {
				medPod, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				if medPod.Spec.NodeName != "" {
					scheduled++
				}
			}
			if scheduled > 5*nodeListLen {
				return false, fmt.Errorf("expected %v medium priority pods to be scheduled, but got %d", 5*nodeListLen, scheduled)
			}

			return scheduled == 5*nodeListLen, nil
		}))
	})

	/*
		发布版本：v1.31
		测试名称：Verify the DisruptionTarget condition is added to the preempted pod
		描述：
		1. 运行一个带 finalizer 的低优先级 pod，消耗 1/1 的节点资源
		2. 调度一个同样消耗 1/1 节点资源的更高优先级 pod
		3. 查看低优先级 pod 是否被抢占并带有 pod disruption condition
		4. 移除 finalizer，让 pod 能被 GC 删除
	*/
	framework.ConformanceIt("validates pod disruption condition is added to the preempted pod", func(ctx context.Context) {
		podRes := v1.ResourceList{testExtendedResource: resource.MustParse("1")}

		ginkgo.By("Select a node to run the lower and higher priority pods")
		gomega.Expect(nodeList.Items).ToNot(gomega.BeEmpty(), "We need at least one node for the test to run")
		node := nodeList.Items[0]
		e2enode.AddExtendedResource(ctx, cs, node.Name, testExtendedResource, resource.MustParse("1"))

		// 准备 node affinity，确保低优先级和高优先级 pod 调度到同一节点
		testNodeAffinity := v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchFields: []v1.NodeSelectorRequirement{
								{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{node.Name}},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Create a low priority pod that consumes 1/1 of node resources")
		victimPod := createPausePod(ctx, f, pausePodConfig{
			Name:              "victim-pod",
			PriorityClassName: lowPriorityClassName,
			Resources: &v1.ResourceRequirements{
				Requests: podRes,
				Limits:   podRes,
			},
			Finalizers: []string{testFinalizer},
			Affinity:   &testNodeAffinity,
		})
		framework.Logf("Created pod: %v", victimPod.Name)

		ginkgo.By("Wait for the victim pod to be scheduled")
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, victimPod))

		// 移除 finalizer，让受害者 pod 能被 GC
		defer e2epod.NewPodClient(f).RemoveFinalizer(ctx, victimPod.Name, testFinalizer)

		ginkgo.By("Create a high priority pod to trigger preemption of the lower priority pod")
		preemptorPod := createPausePod(ctx, f, pausePodConfig{
			Name:              "preemptor-pod",
			PriorityClassName: highPriorityClassName,
			Resources: &v1.ResourceRequirements{
				Requests: podRes,
				Limits:   podRes,
			},
			Affinity: &testNodeAffinity,
		})
		framework.Logf("Created pod: %v", preemptorPod.Name)

		ginkgo.By("Waiting for the victim pod to be terminating")
		err := e2epod.WaitForPodTerminatingInNamespaceTimeout(ctx, f.ClientSet, victimPod.Name, victimPod.Namespace, framework.PodDeleteTimeout)
		framework.ExpectNoError(err)

		ginkgo.By("Verifying the pod has the pod disruption condition")
		e2epod.VerifyPodHasConditionWithType(ctx, f, victimPod, v1.DisruptionTarget)
	})

	ginkgo.Context("PodTopologySpread Preemption", func() {
		var nodeNames []string
		var nodes []*v1.Node
		topologyKey := "kubernetes.io/e2e-pts-preemption"
		var fakeRes v1.ResourceName = "example.com/fakePTSRes"

		ginkgo.BeforeEach(func(ctx context.Context) {
			if len(nodeList.Items) < 2 {
				ginkgo.Skip("At least 2 nodes are required to run the test")
			}
			ginkgo.By("Trying to get 2 available nodes which can run pod")
			nodeNames = Get2NodesThatCanRunPod(ctx, f)
			ginkgo.By(fmt.Sprintf("Apply dedicated topologyKey %v for this test on the 2 nodes.", topologyKey))
			for _, nodeName := range nodeNames {
				e2enode.AddOrUpdateLabelOnNode(cs, nodeName, topologyKey, nodeName)

				node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
				framework.ExpectNoError(err)
				// 用 fake resource 更新 Node API 对象
				ginkgo.By(fmt.Sprintf("Apply 10 fake resource to node %v.", node.Name))
				e2enode.AddExtendedResource(ctx, cs, node.Name, fakeRes, resource.MustParse("10"))
				nodes = append(nodes, node)
			}
		})
		ginkgo.AfterEach(func(ctx context.Context) {
			for _, nodeName := range nodeNames {
				e2enode.RemoveLabelOffNode(cs, nodeName, topologyKey)
			}
			for _, node := range nodes {
				e2enode.RemoveExtendedResource(ctx, cs, node.Name, fakeRes)
			}
		})

		ginkgo.It("validates proper pods are preempted", func(ctx context.Context) {
			podLabel := "e2e-pts-preemption"
			nodeAffinity := &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      topologyKey,
										Operator: v1.NodeSelectorOpIn,
										Values:   nodeNames,
									},
								},
							},
						},
					},
				},
			}
			highPodCfg := pausePodConfig{
				Name:              "high",
				Namespace:         ns,
				Labels:            map[string]string{podLabel: ""},
				PriorityClassName: highPriorityClassName,
				Affinity:          nodeAffinity,
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{fakeRes: resource.MustParse("9")},
					Limits:   v1.ResourceList{fakeRes: resource.MustParse("9")},
				},
			}
			lowPodCfg := pausePodConfig{
				Namespace:         ns,
				Labels:            map[string]string{podLabel: ""},
				PriorityClassName: lowPriorityClassName,
				Affinity:          nodeAffinity,
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{fakeRes: resource.MustParse("3")},
					Limits:   v1.ResourceList{fakeRes: resource.MustParse("3")},
				},
			}

			ginkgo.By("Create 1 High Pod and 3 Low Pods to occupy 9/10 of fake resources on both nodes.")
			// 准备 1 个高优先级 Pod 和 3 个低优先级 Pod
			runPausePod(ctx, f, highPodCfg)
			for i := 1; i <= 3; i++ {
				lowPodCfg.Name = fmt.Sprintf("low-%v", i)
				runPausePod(ctx, f, lowPodCfg)
			}

			ginkgo.By("Create 1 Medium Pod with TopologySpreadConstraints")
			mediumPodCfg := pausePodConfig{
				Name:              "medium",
				Namespace:         ns,
				Labels:            map[string]string{podLabel: ""},
				PriorityClassName: mediumPriorityClassName,
				Affinity:          nodeAffinity,
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{fakeRes: resource.MustParse("3")},
					Limits:   v1.ResourceList{fakeRes: resource.MustParse("3")},
				},
				TopologySpreadConstraints: []v1.TopologySpreadConstraint{
					{
						MaxSkew:           1,
						TopologyKey:       topologyKey,
						WhenUnsatisfiable: v1.DoNotSchedule,
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      podLabel,
									Operator: metav1.LabelSelectorOpExists,
								},
							},
						},
					},
				},
			}
			// 为满足 resource.requests，中等优先级 Pod 只需抢占一个低优先级 pod。
			// 但那样 Pod 分布会变成 [<high>, <medium, low, low>]，不满足
			// pod topology spread constraints。因此需要再抢占一个低优先级 pod
			// 让 Pod 分布变成 [<high>, <medium, low>]。
			runPausePod(ctx, f, mediumPodCfg)

			ginkgo.By("Verify there are 3 Pods left in this namespace")
			wantPods := sets.New("high", "medium", "low")

			// 等待 pod 数量稳定。注意 `medium` pod 在
			// 第二个低优先级 pod 被标记为 terminating 后就可以调度。
			pods, err := e2epod.WaitForNumberOfPods(ctx, cs, ns, 3, framework.PollShortTimeout)
			framework.ExpectNoError(err)

			for _, pod := range pods.Items {
				// 去掉低优先级 pod 的序号索引。
				podName := strings.Split(pod.Name, "-")[0]
				if wantPods.Has(podName) {
					ginkgo.By(fmt.Sprintf("Pod %q is as expected to be running.", pod.Name))
					wantPods.Delete(podName)
				} else {
					framework.Failf("Pod %q conflicted with expected PodSet %v", podName, wantPods)
				}
			}
		})
	})

	ginkgo.Context("PreemptionExecutionPath", func() {
		// 构造一个 fakecpu 以便设置到 Node 对象的 status 中，
		// 否则更新 CPU/Memory 等后，kubelet 会把值改回去
		var fakecpu v1.ResourceName = "example.com/fakecpu"
		var cs clientset.Interface
		var node *v1.Node
		var ns, nodeHostNameLabel string
		f := framework.NewDefaultFramework("sched-preemption-path")
		f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

		priorityPairs := make([]priorityPair, 0)

		ginkgo.AfterEach(func(ctx context.Context) {
			// 测试失败时打印附加信息
			if ginkgo.CurrentSpecReport().Failed() {
				// 列举已有的 PriorityClass。
				priorityList, err := cs.SchedulingV1().PriorityClasses().List(ctx, metav1.ListOptions{})
				if err != nil {
					framework.Logf("Unable to list PriorityClasses: %v", err)
				} else {
					framework.Logf("List existing PriorityClasses:")
					for _, p := range priorityList.Items {
						framework.Logf("%v/%v created at %v", p.Name, p.Value, p.CreationTimestamp)
					}
				}
			}

			if node != nil {
				e2enode.RemoveExtendedResource(ctx, cs, node.Name, fakecpu)
			}
			for _, pair := range priorityPairs {
				_ = cs.SchedulingV1().PriorityClasses().Delete(ctx, pair.name, *metav1.NewDeleteOptions(0))
			}
		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			cs = f.ClientSet
			ns = f.Namespace.Name

			// 找一个可用节点
			ginkgo.By("Finding an available node")
			nodeName := GetNodeThatCanRunPod(ctx, f)
			framework.Logf("found a healthy node: %s", nodeName)

			// 获取 node API 对象
			var err error
			node, err = cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil {
				framework.Failf("error getting node %q: %v", nodeName, err)
			}
			var ok bool
			nodeHostNameLabel, ok = node.GetObjectMeta().GetLabels()["kubernetes.io/hostname"]
			if !ok {
				framework.Failf("error getting kubernetes.io/hostname label on node %s", nodeName)
			}

			// 用 fake resource 更新 Node API 对象
			e2enode.AddExtendedResource(ctx, cs, node.Name, fakecpu, resource.MustParse("1000"))

			// 创建四个 PriorityClass：p1、p2、p3、p4
			for i := 1; i <= 4; i++ {
				priorityName := fmt.Sprintf("p%d", i)
				priorityVal := int32(i)
				priorityPairs = append(priorityPairs, priorityPair{name: priorityName, value: priorityVal})
				_, err := cs.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: priorityName}, Value: priorityVal}, metav1.CreateOptions{})
				if err != nil {
					framework.Logf("Failed to create priority '%v/%v'. Reason: %v. Msg: %v", priorityName, priorityVal, apierrors.ReasonForError(err), err)
				}
				if err != nil && !apierrors.IsAlreadyExists(err) {
					framework.Failf("expected 'alreadyExists' as error, got instead: %v", err)
				}
			}
		})

		/*
			发布版本：v1.19
			测试名称：Pod preemption verification
			描述：ReplicaSet 中四个不同优先级的 Pod 在给定 CPU 限制下必须能启动。优先级 1 - 3 的 Pod 必须先启动，然后是优先级 4 的 Pod。带 Replicas 的 ReplicaSet 必须包含预期数量的副本。
		*/
		framework.ConformanceIt("runs ReplicaSets to verify preemption running path", func(ctx context.Context) {
			var podNamesSeen [3]atomic.Int32

			_, podController := cache.NewInformer(
				&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						obj, err := f.ClientSet.CoreV1().Pods(ns).List(ctx, options)
						return runtime.Object(obj), err
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return f.ClientSet.CoreV1().Pods(ns).Watch(ctx, options)
					},
				},
				&v1.Pod{},
				0,
				cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						if pod, ok := obj.(*v1.Pod); ok {
							switch {
							case strings.HasPrefix(pod.Name, "rs-pod1"):
								podNamesSeen[0].Add(1)
							case strings.HasPrefix(pod.Name, "rs-pod2"):
								podNamesSeen[1].Add(1)
							case strings.HasPrefix(pod.Name, "rs-pod3"):
								podNamesSeen[2].Add(1)
							}
						}
					},
				},
			)

			go podController.Run(ctx.Done())

			// 准备三个 ReplicaSet
			rsConfs := []pauseRSConfig{
				{
					Replicas: int32(1),
					PodConfig: pausePodConfig{
						Name:              "pod1",
						Namespace:         ns,
						Labels:            map[string]string{"name": "pod1"},
						PriorityClassName: "p1",
						NodeSelector:      map[string]string{"kubernetes.io/hostname": nodeHostNameLabel},
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{fakecpu: resource.MustParse("200")},
							Limits:   v1.ResourceList{fakecpu: resource.MustParse("200")},
						},
					},
				},
				{
					Replicas: int32(1),
					PodConfig: pausePodConfig{
						Name:              "pod2",
						Namespace:         ns,
						Labels:            map[string]string{"name": "pod2"},
						PriorityClassName: "p2",
						NodeSelector:      map[string]string{"kubernetes.io/hostname": nodeHostNameLabel},
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{fakecpu: resource.MustParse("300")},
							Limits:   v1.ResourceList{fakecpu: resource.MustParse("300")},
						},
					},
				},
				{
					Replicas: int32(1),
					PodConfig: pausePodConfig{
						Name:              "pod3",
						Namespace:         ns,
						Labels:            map[string]string{"name": "pod3"},
						PriorityClassName: "p3",
						NodeSelector:      map[string]string{"kubernetes.io/hostname": nodeHostNameLabel},
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{fakecpu: resource.MustParse("450")},
							Limits:   v1.ResourceList{fakecpu: resource.MustParse("450")},
						},
					},
				},
			}
			// 创建 ReplicaSet{1,2,3} 占用 950/1000 的 fake resource
			for i := range rsConfs {
				runPauseRS(ctx, f, rsConfs[i])
			}

			framework.Logf(
				"pods created so far: [%d %d %d]",
				podNamesSeen[0].Load(),
				podNamesSeen[1].Load(),
				podNamesSeen[2].Load(),
			)
			framework.Logf("length of pods created so far: %v", len(podNamesSeen))

			// 创建一个 Preemptor Pod
			preemptorPodConf := pausePodConfig{
				Name:              "pod4",
				Namespace:         ns,
				Labels:            map[string]string{"name": "pod4"},
				PriorityClassName: "p4",
				NodeSelector:      map[string]string{"kubernetes.io/hostname": nodeHostNameLabel},
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{fakecpu: resource.MustParse("500")},
					Limits:   v1.ResourceList{fakecpu: resource.MustParse("500")},
				},
			}
			preemptorPod := createPod(ctx, f, preemptorPodConf)
			waitForPreemptingWithTimeout(ctx, f, preemptorPod, framework.PodGetTimeout)

			framework.Logf(
				"pods created so far: [%d %d %d]",
				podNamesSeen[0].Load(),
				podNamesSeen[1].Load(),
				podNamesSeen[2].Load(),
			)

			// 统计 ReplicaSet{1,2,3} 的 pod 数：
			// - 如果超过预期副本数，说明 pod 被过度抢占
			// - 如果少于预期副本数，说明 pod 抢占不足
			// "*2" 表示 ReplicaSet{1,2} 的 pod 预期各只被抢占一次。
			expectedRSPods := []int32{1 * 2, 1 * 2, 1}
			err := wait.PollUntilContextTimeout(ctx, framework.Poll, framework.PollShortTimeout, false, func(ctx context.Context) (bool, error) {
				for i := range len(podNamesSeen) {
					got := podNamesSeen[i].Load()
					if got < expectedRSPods[i] {
						framework.Logf("waiting for rs%d to observe %d pod creations, got %d", i+1, expectedRSPods[i], got)
						return false, nil
					} else if got > expectedRSPods[i] {
						return false, fmt.Errorf("rs%d had more than %d pods created: %d", i+1, expectedRSPods[i], got)
					}
				}
				return true, nil
			})
			if err != nil {
				framework.Logf(
					"pods created so far: [%d %d %d]",
					podNamesSeen[0].Load(),
					podNamesSeen[1].Load(),
					podNamesSeen[2].Load(),
				)
				framework.Failf("failed pod observation expectations: %v", err)
			}

			// 如果逻辑走到这里，应做最后检查，确保一段时间内
			// 状态稳定；否则 pod 可能被过度抢占。
			time.Sleep(5 * time.Second)
			for i := range len(podNamesSeen) {
				got := podNamesSeen[i].Load()
				if got < expectedRSPods[i] {
					framework.Failf("pods of ReplicaSet%d have been under-preempted: expect %v pod names, but got %d", i+1, expectedRSPods[i], got)
				} else if got > expectedRSPods[i] {
					framework.Failf("pods of ReplicaSet%d have been over-preempted: expect %v pod names, but got %d", i+1, expectedRSPods[i], got)
				}
			}
		})
	})

	ginkgo.Context("PriorityClass endpoints", func() {
		var cs clientset.Interface
		f := framework.NewDefaultFramework("sched-preemption-path")
		f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
		testUUID := uuid.New().String()
		var pcs []*schedulingv1.PriorityClass

		ginkgo.BeforeEach(func(ctx context.Context) {
			cs = f.ClientSet
			// 创建 2 个 PriorityClass：p1、p2。
			for i := 1; i <= 2; i++ {
				name, val := fmt.Sprintf("p%d", i), int32(i)
				pc, err := cs.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"e2e": testUUID}}, Value: val}, metav1.CreateOptions{})
				if err != nil {
					framework.Logf("Failed to create priority '%v/%v'. Reason: %v. Msg: %v", name, val, apierrors.ReasonForError(err), err)
				}
				if err != nil && !apierrors.IsAlreadyExists(err) {
					framework.Failf("expected 'alreadyExists' as error, got instead: %v", err)
				}
				pcs = append(pcs, pc)
			}
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			// 测试失败时打印附加信息。
			if ginkgo.CurrentSpecReport().Failed() {
				// 列举已有的 PriorityClass。
				priorityList, err := cs.SchedulingV1().PriorityClasses().List(ctx, metav1.ListOptions{})
				if err != nil {
					framework.Logf("Unable to list PriorityClasses: %v", err)
				} else {
					framework.Logf("List existing PriorityClasses:")
					for _, p := range priorityList.Items {
						framework.Logf("%v/%v created at %v", p.Name, p.Value, p.CreationTimestamp)
					}
				}
			}

			// 对创建的 PriorityClass 执行集合删除。
			err := cs.SchedulingV1().PriorityClasses().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: fmt.Sprintf("e2e=%v", testUUID)})
			framework.ExpectNoError(err)
		})

		/*发布版本：v1.20
		测试名称：Scheduler, Verify PriorityClass endpoints
		描述：验证 PriorityClass endpoint 可被列举。patch 或 update 任何可变字段
		都必须成功。patch 或 update 任何不可变字段
		都必须失败。

		*/
		framework.ConformanceIt("verify PriorityClass endpoints can be operated with different HTTP methods", func(ctx context.Context) {
			// 1. 对不可变字段 Patch/Update 会失败。
			pcCopy := pcs[0].DeepCopy()
			pcCopy.Value = pcCopy.Value * 10
			err := patchPriorityClass(ctx, cs, pcs[0], pcCopy)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expect a patch error on an immutable field")
			framework.Logf("%v", err)

			pcCopy = pcs[1].DeepCopy()
			pcCopy.Value = pcCopy.Value * 10
			_, err = cs.SchedulingV1().PriorityClasses().Update(ctx, pcCopy, metav1.UpdateOptions{})
			gomega.Expect(err).To(gomega.HaveOccurred(), "expect an update error on an immutable field")
			framework.Logf("%v", err)

			// 2. 对可变字段 Patch/Update 会成功。
			newDesc := "updated description"
			pcCopy = pcs[0].DeepCopy()
			pcCopy.Description = newDesc
			err = patchPriorityClass(ctx, cs, pcs[0], pcCopy)
			framework.ExpectNoError(err)

			pcCopy = pcs[1].DeepCopy()
			pcCopy.Description = newDesc
			_, err = cs.SchedulingV1().PriorityClasses().Update(ctx, pcCopy, metav1.UpdateOptions{})
			framework.ExpectNoError(err)

			// 3. 列举已有的 PriorityClass。
			_, err = cs.SchedulingV1().PriorityClasses().List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)

			// 4. 验证更新后 PriorityClass 的字段。
			for _, pc := range pcs {
				livePC, err := cs.SchedulingV1().PriorityClasses().Get(ctx, pc.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)
				gomega.Expect(livePC.Value).To(gomega.Equal(pc.Value))
				gomega.Expect(livePC.Description).To(gomega.Equal(newDesc))
				gomega.Expect(livePC).To(apimachineryutils.HaveValidResourceVersion())
				gomega.Expect(resourceversion.CompareResourceVersion(pc.ResourceVersion, livePC.ResourceVersion)).To(gomega.BeNumerically("==", -1), "changed object should have a larger resource version")
			}
		})
	})
})

type pauseRSConfig struct {
	Replicas  int32
	PodConfig pausePodConfig
}

func initPauseRS(f *framework.Framework, conf pauseRSConfig) *appsv1.ReplicaSet {
	pausePod := initPausePod(f, conf.PodConfig)
	pauseRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs-" + pausePod.Name,
			Namespace: pausePod.Namespace,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &conf.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: pausePod.Labels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: pausePod.ObjectMeta.Labels},
				Spec:       pausePod.Spec,
			},
		},
	}
	return pauseRS
}

func createPauseRS(ctx context.Context, f *framework.Framework, conf pauseRSConfig) *appsv1.ReplicaSet {
	namespace := conf.PodConfig.Namespace
	if len(namespace) == 0 {
		namespace = f.Namespace.Name
	}
	rs, err := f.ClientSet.AppsV1().ReplicaSets(namespace).Create(ctx, initPauseRS(f, conf), metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return rs
}

func runPauseRS(ctx context.Context, f *framework.Framework, conf pauseRSConfig) *appsv1.ReplicaSet {
	rs := createPauseRS(ctx, f, conf)
	framework.ExpectNoError(e2ereplicaset.WaitForReplicaSetTargetAvailableReplicasWithTimeout(ctx, f.ClientSet, rs, conf.Replicas, framework.PodGetTimeout))
	return rs
}

func createPod(ctx context.Context, f *framework.Framework, conf pausePodConfig) *v1.Pod {
	namespace := conf.Namespace
	if len(namespace) == 0 {
		namespace = f.Namespace.Name
	}
	pod, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, initPausePod(f, conf), metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return pod
}

// waitForPreemptingWithTimeout 验证 'pod' 是否在 'timeout' 内进行抢占，具体检查
// 抢占者 'pod' 的 'spec.NodeName' 字段是否已被设置。
func waitForPreemptingWithTimeout(ctx context.Context, f *framework.Framework, pod *v1.Pod, timeout time.Duration) {
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, false, func(ctx context.Context) (bool, error) {
		pod, err := f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if len(pod.Spec.NodeName) > 0 {
			return true, nil
		}
		return false, err
	})
	framework.ExpectNoError(err, "pod %v/%v failed to preempt other pods", pod.Namespace, pod.Name)
}

func patchPriorityClass(ctx context.Context, cs clientset.Interface, old, new *schedulingv1.PriorityClass) error {
	oldData, err := json.Marshal(old)
	if err != nil {
		return err
	}

	newData, err := json.Marshal(new)
	if err != nil {
		return err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &schedulingv1.PriorityClass{})
	if err != nil {
		return fmt.Errorf("failed to create merge patch for PriorityClass %q: %w", old.Name, err)
	}
	_, err = cs.SchedulingV1().PriorityClasses().Patch(ctx, old.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}
