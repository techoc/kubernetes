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

package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	clientset "k8s.io/client-go/kubernetes"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2eruntimeclass "k8s.io/kubernetes/test/e2e/framework/node/runtimeclass"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	maxNumberOfPods int64 = 10
	defaultTimeout        = 3 * time.Minute
)

var localStorageVersion = utilversion.MustParseSemantic("v1.8.0-beta.0")

// 在 BeforeEach 中填充的变量，之后不再修改
var workerNodes = sets.Set[string]{}

type pausePodConfig struct {
	Name                              string
	Namespace                         string
	Finalizers                        []string
	Affinity                          *v1.Affinity
	Annotations, Labels, NodeSelector map[string]string
	Resources                         *v1.ResourceRequirements
	RuntimeClassHandler               *string
	Tolerations                       []v1.Toleration
	NodeName                          string
	Ports                             []v1.ContainerPort
	OwnerReferences                   []metav1.OwnerReference
	PriorityClassName                 string
	DeletionGracePeriodSeconds        *int64
	TopologySpreadConstraints         []v1.TopologySpreadConstraint
	SchedulingGates                   []v1.PodSchedulingGate
	TerminationGracePeriodSeconds     *int64
	PreStopHookSleepSeconds           *int64
}

var _ = SIGDescribe("SchedulerPredicates", framework.WithSerial(), func() {
	var cs clientset.Interface
	var nodeList *v1.NodeList
	var RCName string
	var ns string
	f := framework.NewDefaultFramework("sched-pred")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.AfterEach(func(ctx context.Context) {
		rc, err := cs.CoreV1().ReplicationControllers(ns).Get(ctx, RCName, metav1.GetOptions{})
		if err == nil && *(rc.Spec.Replicas) != 0 {
			ginkgo.By("Cleaning up the replication controller")
			err := e2erc.DeleteRCAndWaitForGC(ctx, f.ClientSet, ns, RCName)
			framework.ExpectNoError(err)
		}
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		cs = f.ClientSet
		ns = f.Namespace.Name
		nodeList = &v1.NodeList{}
		var err error

		e2enode.AllNodesReady(ctx, cs, time.Minute)

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

		for _, node := range nodeList.Items {
			framework.Logf("\nLogging pods the apiserver thinks is on node %v before test", node.Name)
			printAllPodsOnNode(ctx, cs, node.Name)
		}

	})

	// 这个测试验证我们不允许以这样的方式调度 pod：pod 的本地临时存储资源 requests 之和大于机器容量。
	// 它假设集群 add-on pod 保持稳定，且不能与任何其他触及 Node 或 Pod 的测试并行运行。
	// 因为我们需要精确控制集群中运行的内容。
	f.It("validates local ephemeral storage resource limits of pods that are allowed to run", func(ctx context.Context) {

		e2eskipper.SkipUnlessServerVersionGTE(localStorageVersion, f.ClientSet.Discovery())

		nodeMaxAllocatable := int64(0)

		nodeToAllocatableMap := make(map[string]int64)
		for _, node := range nodeList.Items {
			allocatable, found := node.Status.Allocatable[v1.ResourceEphemeralStorage]
			if !found {
				framework.Failf("node.Status.Allocatable %v does not contain entry %v", node.Status.Allocatable, v1.ResourceEphemeralStorage)
			}
			nodeToAllocatableMap[node.Name] = allocatable.Value()
			if nodeMaxAllocatable < allocatable.Value() {
				nodeMaxAllocatable = allocatable.Value()
			}
		}
		WaitForStableCluster(cs, workerNodes)

		pods, err := cs.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		for _, pod := range pods.Items {
			_, found := nodeToAllocatableMap[pod.Spec.NodeName]
			if found && pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed {
				framework.Logf("Pod %v requesting local ephemeral resource =%v on Node %v", pod.Name, getRequestedStorageEphemeralStorage(pod), pod.Spec.NodeName)
				nodeToAllocatableMap[pod.Spec.NodeName] -= getRequestedStorageEphemeralStorage(pod)
			}
		}

		var podsNeededForSaturation int
		ephemeralStoragePerPod := nodeMaxAllocatable / maxNumberOfPods

		framework.Logf("Using pod capacity: %v", ephemeralStoragePerPod)
		for name, leftAllocatable := range nodeToAllocatableMap {
			framework.Logf("Node: %v has local ephemeral resource allocatable: %v", name, leftAllocatable)
			podsNeededForSaturation += (int)(leftAllocatable / ephemeralStoragePerPod)
		}

		ginkgo.By(fmt.Sprintf("Starting additional %v Pods to fully saturate the cluster local ephemeral resource and trying to start another one", podsNeededForSaturation))

		// 由于 pod 随机分布在各节点上，
		// 很容易出现所有节点都饱和、
		// 不需要创建更多 pod 的情况。
		// StartPods 至少需要复制一个 pod。
		if podsNeededForSaturation > 0 {
			framework.ExpectNoError(testutils.StartPods(cs, podsNeededForSaturation, ns, "overcommit",
				*initPausePod(f, pausePodConfig{
					Name:   "",
					Labels: map[string]string{"name": ""},
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStoragePerPod, "DecimalSI"),
						},
						Requests: v1.ResourceList{
							v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStoragePerPod, "DecimalSI"),
						},
					},
				}), true, framework.Logf))
		}
		podName := "additional-pod"
		conf := pausePodConfig{
			Name:   podName,
			Labels: map[string]string{"name": "additional"},
			Resources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStoragePerPod, "DecimalSI"),
				},
				Requests: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStoragePerPod, "DecimalSI"),
				},
			},
		}
		WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podName, false)
		verifyResult(ctx, cs, podsNeededForSaturation, 1, ns)
	})

	// 这个测试验证我们不允许以这样的方式调度 pod：limits 之和
	// 加上相关 overhead 大于机器容量。
	// 它假设集群 add-on pod 保持稳定，且不能与
	// 任何其他触及 Node 或 Pod 的测试并行运行。
	// 因此我们需要精确控制集群中运行的内容。
	// 测试场景：
	// 1. 找到系统上第一个就绪的节点，为其添加一个 fake 资源
	// 2. 创建一个 affinity 到该节点、使用 70% fake 资源的 pod。
	// 3. 等待该 pod 被调度。
	// 4. 再创建一个 affinity 到该节点的 pod，需要 20% 的 fake 资源，且
	//    overhead 设为 fake 资源的 25%。
	// 5. 确保这个额外的 pod 未被调度。

	ginkgo.Context("validates pod overhead is considered along with resource limits of pods that are allowed to run", func() {
		var testNodeName string
		var handler string
		var beardsecond v1.ResourceName = "example.com/beardsecond"

		ginkgo.BeforeEach(func(ctx context.Context) {
			WaitForStableCluster(cs, workerNodes)
			ginkgo.By("Add RuntimeClass and fake resource")

			// 找一个能运行 pod 的节点：
			testNodeName = GetNodeThatCanRunPod(ctx, f)

			// 获取 node 对象：
			node, err := cs.CoreV1().Nodes().Get(ctx, testNodeName, metav1.GetOptions{})
			framework.ExpectNoError(err, "unable to get node object for node %v", testNodeName)

			// 用 fake 资源更新 Node API 对象
			nodeCopy := node.DeepCopy()
			nodeCopy.ResourceVersion = "0"

			nodeCopy.Status.Capacity[beardsecond] = resource.MustParse("1000")
			_, err = cs.CoreV1().Nodes().UpdateStatus(ctx, nodeCopy, metav1.UpdateOptions{})
			framework.ExpectNoError(err, "unable to apply fake resource to %v", testNodeName)

			// 注册一个 runtimeClass，overhead 设为可用 beard-seconds 的 25%
			handler = e2eruntimeclass.PreconfiguredRuntimeClassHandler

			rc := &nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: handler},
				Handler:    handler,
				Overhead: &nodev1.Overhead{
					PodFixed: v1.ResourceList{
						beardsecond: resource.MustParse("250"),
					},
				},
			}
			_, err = cs.NodeV1().RuntimeClasses().Create(ctx, rc, metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create RuntimeClass resource")
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			ginkgo.By("Remove fake resource and RuntimeClass")
			// 移除 fake 资源：
			if testNodeName != "" {
				// 获取 node 对象：
				node, err := cs.CoreV1().Nodes().Get(ctx, testNodeName, metav1.GetOptions{})
				framework.ExpectNoError(err, "unable to get node object for node %v", testNodeName)

				nodeCopy := node.DeepCopy()
				// 强制更新
				nodeCopy.ResourceVersion = "0"
				delete(nodeCopy.Status.Capacity, beardsecond)
				_, err = cs.CoreV1().Nodes().UpdateStatus(ctx, nodeCopy, metav1.UpdateOptions{})
				framework.ExpectNoError(err, "unable to update node %v", testNodeName)
			}

			// 移除 RuntimeClass
			_ = cs.NodeV1().RuntimeClasses().Delete(ctx, e2eruntimeclass.PreconfiguredRuntimeClassHandler, metav1.DeleteOptions{})
		})

		ginkgo.It("verify pod overhead is accounted for", func(ctx context.Context) {
			if testNodeName == "" {
				framework.Fail("unable to find a node which can run a pod")
			}

			ginkgo.By("Starting Pod to consume most of the node's resource.")

			// 创建一个需要 70% 可用 beard-seconds 的 pod。
			fillerPod := createPausePod(ctx, f, pausePodConfig{
				Name: "filler-pod-" + string(uuid.NewUUID()),
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{beardsecond: resource.MustParse("700")},
					Limits:   v1.ResourceList{beardsecond: resource.MustParse("700")},
				},
			})

			// 等待 filler pod 被调度。
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, fillerPod))

			ginkgo.By("Creating another pod that requires unavailable amount of resources.")
			// 再创建一个需要 20% 可用 beard-seconds 的 pod，但使用
			// 定义了额外 25% pod overhead 的 RuntimeClass。
			// 这个 pod 应保持 pending，因为节点中至少 70% 的 beard-second
			// 已被占用。
			podName := "additional-pod" + string(uuid.NewUUID())
			conf := pausePodConfig{
				RuntimeClassHandler: &handler,
				Name:                podName,
				Labels:              map[string]string{"name": "additional"},
				Resources: &v1.ResourceRequirements{
					Limits: v1.ResourceList{beardsecond: resource.MustParse("200")},
				},
			}

			WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podName, false)
			verifyResult(ctx, cs, 1, 1, ns)
		})
	})

	// 这个测试验证我们不允许以这样的方式调度 pod：pod 的
	// 资源 requests 之和大于机器容量。
	// 它假设集群 add-on pod 保持稳定，且不能与
	// 任何其他触及 Node 或 Pod 的测试并行运行。
	// 因为我们需要精确控制集群中运行的内容。
	// 测试场景：
	// 1. 找出每个节点上的 CPU 资源量。
	// 2. 为每个节点创建一个 affinity 到它、使用 70% 节点 CPU 的 pod。
	// 3. 等待这些 pod 被调度。
	// 4. 再创建一个不 affinity 到任何节点、需要最大节点 CPU 50% 的 pod。
	// 5. 确保这个额外的 pod 未被调度。
	/*
		发布版本：v1.9
		测试名称：Scheduler, resource limits
		描述：如果资源 requests 超过机器容量，Pod 调度必须失败。
	*/
	framework.ConformanceIt("validates resource limits of pods that are allowed to run", func(ctx context.Context) {
		WaitForStableCluster(cs, workerNodes)
		nodeMaxAllocatable := int64(0)
		nodeToAllocatableMap := make(map[string]int64)
		for _, node := range nodeList.Items {
			nodeReady := false
			for _, condition := range node.Status.Conditions {
				if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
					nodeReady = true
					break
				}
			}
			if !nodeReady {
				continue
			}
			// 给每个节点打上 node label
			e2enode.AddOrUpdateLabelOnNode(cs, node.Name, "node", node.Name)
			e2enode.ExpectNodeHasLabel(ctx, cs, node.Name, "node", node.Name)
			// 找出可分配的 CPU 量。
			allocatable, found := node.Status.Allocatable[v1.ResourceCPU]
			if !found {
				framework.Failf("node.Status.Allocatable %v does not contain entry %v", node.Status.Allocatable, v1.ResourceCPU)
			}
			nodeToAllocatableMap[node.Name] = allocatable.MilliValue()
			if nodeMaxAllocatable < allocatable.MilliValue() {
				nodeMaxAllocatable = allocatable.MilliValue()
			}
		}
		// 本测试后清理添加的标签。
		defer func() {
			for nodeName := range nodeToAllocatableMap {
				e2enode.RemoveLabelOffNode(cs, nodeName, "node")
			}
		}()

		pods, err := cs.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		for _, pod := range pods.Items {
			_, found := nodeToAllocatableMap[pod.Spec.NodeName]
			if found && pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed {
				framework.Logf("Pod %v requesting resource cpu=%vm on Node %v", pod.Name, getRequestedCPU(pod), pod.Spec.NodeName)
				nodeToAllocatableMap[pod.Spec.NodeName] -= getRequestedCPU(pod)
			}
		}

		ginkgo.By("Starting Pods to consume most of the cluster CPU.")
		// 每个节点创建一个需要节点剩余 CPU 70% 的 pod。
		fillerPods := []*v1.Pod{}
		for nodeName, cpu := range nodeToAllocatableMap {
			requestedCPU := cpu * 7 / 10
			framework.Logf("Creating a pod which consumes cpu=%vm on Node %v", requestedCPU, nodeName)
			fillerPods = append(fillerPods, createPausePod(ctx, f, pausePodConfig{
				Name: "filler-pod-" + string(uuid.NewUUID()),
				Resources: &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(requestedCPU, "DecimalSI"),
					},
					Requests: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(requestedCPU, "DecimalSI"),
					},
				},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "node",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{nodeName},
										},
									},
								},
							},
						},
					},
				},
			}))
		}
		// 等待 filler pod 被调度。
		for _, pod := range fillerPods {
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod))
		}
		ginkgo.By("Creating another pod that requires unavailable amount of CPU.")
		// 再创建一个需要最大节点 CPU 资源 50% 的 pod。
		// 这个 pod 应保持 pending，因为集群中其他节点至少 70% 的 CPU
		// 已被占用。
		podName := "additional-pod"
		conf := pausePodConfig{
			Name:   podName,
			Labels: map[string]string{"name": "additional"},
			Resources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(nodeMaxAllocatable*5/10, "DecimalSI"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(nodeMaxAllocatable*5/10, "DecimalSI"),
				},
			},
		}
		WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podName, false)
		verifyResult(ctx, cs, len(fillerPods), 1, ns)
	})

	// 测试节点没有任何 label，因此带非空 Selector 的 Pod
	// 应无法调度。
	/*
		发布版本：v1.9
		测试名称：Scheduler, node selector not matching
		描述：创建一个 NodeSelector 设为与集群中节点都不匹配的值的 Pod。由于没有符合条件的节点，该 Pod 不得被调度。
	*/
	framework.ConformanceIt("validates that NodeSelector is respected if not matching", func(ctx context.Context) {
		ginkgo.By("Trying to schedule Pod with nonempty NodeSelector.")
		podName := "restricted-pod"

		WaitForStableCluster(cs, workerNodes)

		conf := pausePodConfig{
			Name:   podName,
			Labels: map[string]string{"name": "restricted"},
			NodeSelector: map[string]string{
				"label": "nonempty",
			},
		}

		WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podName, false)
		verifyResult(ctx, cs, 0, 1, ns)
	})

	/*
		发布版本：v1.9
		测试名称：Scheduler, node selector matching
		描述：在节点上创建标签 {k: v}，然后创建一个 NodeSelector 设为 {k: v} 的 Pod。检查 Pod 是否被调度。NodeSelector 匹配时，Pod 必须调度到该节点上。
	*/
	framework.ConformanceIt("validates that NodeSelector is respected if matching", func(ctx context.Context) {
		nodeName := GetNodeThatCanRunPod(ctx, f)

		ginkgo.By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(uuid.NewUUID()))
		v := "42"
		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, k, v)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, k, v)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, k)

		ginkgo.By("Trying to relaunch the pod, now with labels.")
		labelPodName := "with-labels"
		createPausePod(ctx, f, pausePodConfig{
			Name: labelPodName,
			NodeSelector: map[string]string{
				k: v,
			},
		})

		// 检查 pod 是否被调度。我们特意不检查
		// pod 是否在运行，因为那会与
		// kubelet 和 scheduler 产生竞态：scheduler 可能已经调度了 pod，
		// 而 kubelet 还不知道它的新 label。这样
		// kubelet 会拒绝启动该 pod。
		framework.ExpectNoError(e2epod.WaitForPodNotPending(ctx, cs, ns, labelPodName))
		labelPod, err := cs.CoreV1().Pods(ns).Get(ctx, labelPodName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(labelPod.Spec.NodeName).To(gomega.Equal(nodeName))
	})

	// 测试节点没有任何 label，因此带非 nil 的
	// NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution 的 Pod 应无法调度。
	ginkgo.It("validates that NodeAffinity is respected if not matching", func(ctx context.Context) {
		ginkgo.By("Trying to schedule Pod with nonempty NodeSelector.")
		podName := "restricted-pod"

		WaitForStableCluster(cs, workerNodes)

		conf := pausePodConfig{
			Name: podName,
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "foo",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"bar", "value2"},
									},
								},
							}, {
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "diffkey",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"wrong", "value2"},
									},
								},
							},
						},
					},
				},
			},
			Labels: map[string]string{"name": "restricted"},
		}
		WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podName, false)
		verifyResult(ctx, cs, 0, 1, ns)
	})

	// 与 NodeSelector 测试步骤相同，
	// 但在 Pod.Spec.Affinity 中指定 Affinity，而不是 NodeSelector。
	ginkgo.It("validates that required NodeAffinity setting is respected if matching", func(ctx context.Context) {
		nodeName := GetNodeThatCanRunPod(ctx, f)

		ginkgo.By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(uuid.NewUUID()))
		v := "42"
		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, k, v)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, k, v)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, k)

		ginkgo.By("Trying to relaunch the pod, now with labels.")
		labelPodName := "with-labels"
		createPausePod(ctx, f, pausePodConfig{
			Name: labelPodName,
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      k,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{v},
									},
								},
							},
						},
					},
				},
			},
		})

		// 检查 pod 是否被调度。我们特意不检查
		// pod 是否在运行，因为那会与
		// kubelet 和 scheduler 产生竞态：scheduler 可能已经调度了 pod，
		// 而 kubelet 还不知道它的新 label。这样
		// kubelet 会拒绝启动该 pod。
		framework.ExpectNoError(e2epod.WaitForPodNotPending(ctx, cs, ns, labelPodName))
		labelPod, err := cs.CoreV1().Pods(ns).Get(ctx, labelPodName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(labelPod.Spec.NodeName).To(gomega.Equal(nodeName))
	})

	// 1. 运行一个 pod 找到一个可用节点，然后删除该 pod
	// 2. 用随机 taint 给节点打污点
	// 3. 尝试用能容忍节点 taint 的 tolerations 重新启动 pod，
	// 并把 pod 的 nodeName 指定为第 1 步找到的节点名
	ginkgo.It("validates that taints-tolerations is respected if matching", func(ctx context.Context) {
		nodeName := getNodeThatCanRunPodWithoutToleration(ctx, f)

		ginkgo.By("Trying to apply a random taint on the found node.")
		testTaint := v1.Taint{
			Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-%s", string(uuid.NewUUID())),
			Value:  "testing-taint-value",
			Effect: v1.TaintEffectNoSchedule,
		}
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		ginkgo.By("Trying to apply a random label on the found node.")
		labelKey := fmt.Sprintf("kubernetes.io/e2e-label-key-%s", string(uuid.NewUUID()))
		labelValue := "testing-label-value"
		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, labelKey, labelValue)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, labelKey, labelValue)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, labelKey)

		ginkgo.By("Trying to relaunch the pod, now with tolerations.")
		tolerationPodName := "with-tolerations"
		createPausePod(ctx, f, pausePodConfig{
			Name:         tolerationPodName,
			Tolerations:  []v1.Toleration{{Key: testTaint.Key, Value: testTaint.Value, Effect: testTaint.Effect}},
			NodeSelector: map[string]string{labelKey: labelValue},
		})

		// 检查 pod 是否被调度。我们特意不检查
		// pod 是否在运行，因为那会与
		// kubelet 和 scheduler 产生竞态：scheduler 可能已经调度了 pod，
		// 而 kubelet 还不知道它的新 taint。这样
		// kubelet 会拒绝启动该 pod。
		framework.ExpectNoError(e2epod.WaitForPodNotPending(ctx, cs, ns, tolerationPodName))
		deployedPod, err := cs.CoreV1().Pods(ns).Get(ctx, tolerationPodName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(deployedPod.Spec.NodeName).To(gomega.Equal(nodeName))
	})

	// 1. 运行一个 pod 找到一个可用节点，然后删除该 pod
	// 2. 用随机 taint 给节点打污点
	// 3. 尝试不带任何 tolerations 重新启动 pod，
	// 并把 pod 的 nodeName 指定为第 1 步找到的节点名
	ginkgo.It("validates that taints-tolerations is respected if not matching", func(ctx context.Context) {
		nodeName := getNodeThatCanRunPodWithoutToleration(ctx, f)

		ginkgo.By("Trying to apply a random taint on the found node.")
		testTaint := v1.Taint{
			Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-%s", string(uuid.NewUUID())),
			Value:  "testing-taint-value",
			Effect: v1.TaintEffectNoSchedule,
		}
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		ginkgo.By("Trying to apply a random label on the found node.")
		labelKey := fmt.Sprintf("kubernetes.io/e2e-label-key-%s", string(uuid.NewUUID()))
		labelValue := "testing-label-value"
		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, labelKey, labelValue)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, labelKey, labelValue)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, labelKey)

		ginkgo.By("Trying to relaunch the pod, still no tolerations.")
		podNameNoTolerations := "still-no-tolerations"
		conf := pausePodConfig{
			Name:         podNameNoTolerations,
			NodeSelector: map[string]string{labelKey: labelValue},
		}

		WaitForSchedulerAfterAction(ctx, f, createPausePodAction(f, conf), ns, podNameNoTolerations, false)
		verifyResult(ctx, cs, 0, 1, ns)

		ginkgo.By("Removing taint off the node")
		WaitForSchedulerAfterAction(ctx, f, removeTaintFromNodeAction(cs, nodeName, testTaint), ns, podNameNoTolerations, true)
		verifyResult(ctx, cs, 1, 0, ns)
	})

	ginkgo.It("validates that there is no conflict between pods with same hostPort but different hostIP and protocol", func(ctx context.Context) {

		nodeName := GetNodeThatCanRunPod(ctx, f)
		localhost := "127.0.0.1"
		if framework.TestContext.ClusterIsIPv6() {
			localhost = "::1"
		}
		hostIP := getNodeHostIP(ctx, f, nodeName)

		// 用 nodeSelector 确保测试 pod 被分配到同一节点，以明确验证是否存在冲突
		ginkgo.By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(uuid.NewUUID()))
		v := "90"

		nodeSelector := make(map[string]string)
		nodeSelector[k] = v

		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, k, v)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, k, v)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, k)

		port := int32(54321)
		ginkgo.By(fmt.Sprintf("Trying to create a pod(pod1) with hostport %v and hostIP %s and expect scheduled", port, localhost))
		createHostPortPodOnNode(ctx, f, "pod1", ns, localhost, port, v1.ProtocolTCP, nodeSelector, true)

		ginkgo.By(fmt.Sprintf("Trying to create another pod(pod2) with hostport %v but hostIP %s on the node which pod1 resides and expect scheduled", port, hostIP))
		createHostPortPodOnNode(ctx, f, "pod2", ns, hostIP, port, v1.ProtocolTCP, nodeSelector, true)

		ginkgo.By(fmt.Sprintf("Trying to create a third pod(pod3) with hostport %v, hostIP %s but use UDP protocol on the node which pod2 resides", port, hostIP))
		createHostPortPodOnNode(ctx, f, "pod3", ns, hostIP, port, v1.ProtocolUDP, nodeSelector, true)

	})

	/*
		发布版本：v1.16
		测试名称：Scheduling, HostPort and Protocol match, HostIPs different but one is default HostIP (0.0.0.0)
		描述：HostPort 和 Protocol 相同但 HostIP 不同的 Pod，如果其中一个 IP 是默认 HostIP 0.0.0.0（代表主机上所有 IP），
		则不得调度到同一节点。
	*/
	framework.ConformanceIt("validates that there exists conflict between pods with same hostPort and protocol but one using 0.0.0.0 hostIP", func(ctx context.Context) {
		nodeName := GetNodeThatCanRunPod(ctx, f)
		hostIP := getNodeHostIP(ctx, f, nodeName)
		// 用 nodeSelector 确保测试 pod 被分配到同一节点，以明确验证是否存在冲突
		ginkgo.By("Trying to apply a random label on the found node.")
		k := fmt.Sprintf("kubernetes.io/e2e-%s", string(uuid.NewUUID()))
		v := "95"

		nodeSelector := make(map[string]string)
		nodeSelector[k] = v

		e2enode.AddOrUpdateLabelOnNode(cs, nodeName, k, v)
		e2enode.ExpectNodeHasLabel(ctx, cs, nodeName, k, v)
		defer e2enode.RemoveLabelOffNode(cs, nodeName, k)

		port := int32(54322)
		ginkgo.By(fmt.Sprintf("Trying to create a pod(pod4) with hostport %v and hostIP 0.0.0.0(empty string here) and expect scheduled", port))
		createHostPortPodOnNode(ctx, f, "pod4", ns, "", port, v1.ProtocolTCP, nodeSelector, true)

		ginkgo.By(fmt.Sprintf("Trying to create another pod(pod5) with hostport %v but hostIP %s on the node which pod4 resides and expect not scheduled", port, hostIP))
		createHostPortPodOnNode(ctx, f, "pod5", ns, hostIP, port, v1.ProtocolTCP, nodeSelector, false)
	})

	ginkgo.Context("PodTopologySpread Filtering", func() {
		var nodeNames []string
		topologyKey := "kubernetes.io/e2e-pts-filter"

		ginkgo.BeforeEach(func(ctx context.Context) {
			if len(nodeList.Items) < 2 {
				ginkgo.Skip("At least 2 nodes are required to run the test")
			}
			ginkgo.By("Trying to get 2 available nodes which can run pod")
			nodeNames = Get2NodesThatCanRunPod(ctx, f)
			ginkgo.By(fmt.Sprintf("Apply dedicated topologyKey %v for this test on the 2 nodes.", topologyKey))
			for _, nodeName := range nodeNames {
				e2enode.AddOrUpdateLabelOnNode(cs, nodeName, topologyKey, nodeName)
			}
		})
		ginkgo.AfterEach(func() {
			for _, nodeName := range nodeNames {
				e2enode.RemoveLabelOffNode(cs, nodeName, topologyKey)
			}
		})

		ginkgo.It("validates 4 pods with MaxSkew=1 are evenly distributed into 2 nodes", func(ctx context.Context) {
			podLabel := "e2e-pts-filter"
			replicas := 4
			rsConfig := pauseRSConfig{
				Replicas: int32(replicas),
				PodConfig: pausePodConfig{
					Name:      podLabel,
					Namespace: ns,
					Labels:    map[string]string{podLabel: ""},
					Affinity: &v1.Affinity{
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
				},
			}
			runPauseRS(ctx, f, rsConfig)
			podList, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			numInNode1, numInNode2 := 0, 0
			for _, pod := range podList.Items {
				if pod.Spec.NodeName == nodeNames[0] {
					numInNode1++
				} else if pod.Spec.NodeName == nodeNames[1] {
					numInNode2++
				}
			}
			expected := replicas / len(nodeNames)
			gomega.Expect(numInNode1).To(gomega.Equal(expected), fmt.Sprintf("Pods are not distributed as expected on node %q", nodeNames[0]))
			gomega.Expect(numInNode2).To(gomega.Equal(expected), fmt.Sprintf("Pods are not distributed as expected on node %q", nodeNames[1]))
		})
	})

	ginkgo.It("validates Pods with non-empty schedulingGates are blocked on scheduling", func(ctx context.Context) {
		podLabel := "e2e-scheduling-gates"
		replicas := 3
		ginkgo.By(fmt.Sprintf("Creating a ReplicaSet with replicas=%v, carrying scheduling gates [foo bar]", replicas))
		rsConfig := pauseRSConfig{
			Replicas: int32(replicas),
			PodConfig: pausePodConfig{
				Name:      podLabel,
				Namespace: ns,
				Labels:    map[string]string{podLabel: ""},
				SchedulingGates: []v1.PodSchedulingGate{
					{Name: "foo"},
					{Name: "bar"},
				},
			},
		}
		createPauseRS(ctx, f, rsConfig)

		ginkgo.By("Expect all pods stay in pending state")
		podList, err := e2epod.WaitForNumberOfPods(ctx, cs, ns, replicas, time.Minute)
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitForPodsSchedulingGated(ctx, cs, ns, replicas, time.Minute))

		ginkgo.By("Remove one scheduling gate")
		want := []v1.PodSchedulingGate{{Name: "bar"}}
		var pods []*v1.Pod
		for _, pod := range podList.Items {
			clone := pod.DeepCopy()
			clone.Spec.SchedulingGates = want
			live, err := patchPod(cs, &pod, clone)
			framework.ExpectNoError(err)
			pods = append(pods, live)
		}

		ginkgo.By("Expect all pods carry one scheduling gate and are still in pending state")
		framework.ExpectNoError(e2epod.WaitForPodsWithSchedulingGates(ctx, cs, ns, replicas, time.Minute, want))
		framework.ExpectNoError(e2epod.WaitForPodsSchedulingGated(ctx, cs, ns, replicas, time.Minute))

		ginkgo.By("Remove the remaining scheduling gates")
		for _, pod := range pods {
			clone := pod.DeepCopy()
			clone.Spec.SchedulingGates = nil
			_, err := patchPod(cs, pod, clone)
			framework.ExpectNoError(err)
		}

		ginkgo.By("Expect all pods are scheduled and running")
		framework.ExpectNoError(e2epod.WaitForPodsRunning(ctx, cs, ns, replicas, time.Minute))
	})

	// Regression test for an extended scenario for https://issues.k8s.io/123465
	ginkgo.It("when PVC has node-affinity to non-existent/illegal nodes, the pod should be scheduled normally if suitable nodes exist", func(ctx context.Context) {
		nodeName := GetNodeThatCanRunPod(ctx, f)
		nonExistentNodeName1 := string(uuid.NewUUID())
		nonExistentNodeName2 := string(uuid.NewUUID())
		hostLabel := "kubernetes.io/hostname"
		localPath := "/tmp"
		podName := "bind-pv-with-non-existent-nodes"
		pvcName := "pvc-" + string(uuid.NewUUID())
		_, pvc, err := e2epv.CreatePVPVC(ctx, cs, f.Timeouts, e2epv.PersistentVolumeConfig{
			PVSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{
					Path: localPath,
				},
			},
			Prebind: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns},
			},
			NodeAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      hostLabel,
									Operator: v1.NodeSelectorOpIn,
									// 把不存在的节点加入列表
									Values: []string{nodeName, nonExistentNodeName1, nonExistentNodeName2},
								},
							},
						},
					},
				},
			},
		}, e2epv.PersistentVolumeClaimConfig{
			Name: pvcName,
		}, ns, true)
		framework.ExpectNoError(err)
		bindPvPod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "pause",
						Image: imageutils.GetE2EImage(imageutils.Pause),
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "data",
								MountPath: "/tmp",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "data",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc.Name,
							},
						},
					},
				},
			},
		}
		_, err = f.ClientSet.CoreV1().Pods(ns).Create(ctx, bindPvPod, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitForPodNotPending(ctx, f.ClientSet, ns, podName))
	})
})

func patchPod(cs clientset.Interface, old, new *v1.Pod) (*v1.Pod, error) {
	oldData, err := json.Marshal(old)
	if err != nil {
		return nil, err
	}

	newData, err := json.Marshal(new)
	if err != nil {
		return nil, err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &v1.Pod{})
	if err != nil {
		return nil, fmt.Errorf("failed to create merge patch for Pod %q: %w", old.Name, err)
	}
	return cs.CoreV1().Pods(new.Namespace).Patch(context.TODO(), new.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
}

// printAllPodsOnNode 把所有 kubelet pod 的状态输出到日志。
func printAllPodsOnNode(ctx context.Context, c clientset.Interface, nodeName string) {
	podList, err := c.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	if err != nil {
		framework.Logf("Unable to retrieve pods for node %v: %v", nodeName, err)
		return
	}
	for _, p := range podList.Items {
		framework.Logf("%v from %v started at %v (%d container statuses recorded)", p.Name, p.Namespace, p.Status.StartTime, len(p.Status.ContainerStatuses))
		for _, c := range p.Status.ContainerStatuses {
			framework.Logf("\tContainer %v ready: %v, restart count %v",
				c.Name, c.Ready, c.RestartCount)
		}
	}
}

func initPausePod(f *framework.Framework, conf pausePodConfig) *v1.Pod {
	var gracePeriod = int64(1)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            conf.Name,
			Namespace:       conf.Namespace,
			Labels:          map[string]string{},
			Annotations:     map[string]string{},
			OwnerReferences: conf.OwnerReferences,
			Finalizers:      conf.Finalizers,
		},
		Spec: v1.PodSpec{
			SecurityContext:           e2epod.GetRestrictedPodSecurityContext(),
			NodeSelector:              conf.NodeSelector,
			Affinity:                  conf.Affinity,
			TopologySpreadConstraints: conf.TopologySpreadConstraints,
			RuntimeClassName:          conf.RuntimeClassHandler,
			Containers: []v1.Container{
				{
					Name:            conf.Name,
					Image:           imageutils.GetPauseImageName(),
					Ports:           conf.Ports,
					SecurityContext: e2epod.GetRestrictedContainerSecurityContext(),
				},
			},
			Tolerations:                   conf.Tolerations,
			PriorityClassName:             conf.PriorityClassName,
			TerminationGracePeriodSeconds: &gracePeriod,
			SchedulingGates:               conf.SchedulingGates,
		},
	}
	for key, value := range conf.Labels {
		pod.ObjectMeta.Labels[key] = value
	}
	for key, value := range conf.Annotations {
		pod.ObjectMeta.Annotations[key] = value
	}
	// TODO: 设置 Pod 的 nodeAffinity 而非设置 .spec.nodeName 可以规避
	// Preemption e2e 偶发失败（#88441），但应深入调查以找到根因。
	if len(conf.NodeName) != 0 {
		e2epod.SetNodeAffinity(&pod.Spec, conf.NodeName)
	}
	if conf.Resources != nil {
		pod.Spec.Containers[0].Resources = *conf.Resources
	}
	if conf.DeletionGracePeriodSeconds != nil {
		pod.ObjectMeta.DeletionGracePeriodSeconds = conf.DeletionGracePeriodSeconds
	}
	if conf.TerminationGracePeriodSeconds != nil {
		pod.Spec.TerminationGracePeriodSeconds = conf.TerminationGracePeriodSeconds
	}
	if conf.PreStopHookSleepSeconds != nil {
		pod.Spec.Containers[0].Lifecycle = &v1.Lifecycle{
			PreStop: &v1.LifecycleHandler{
				Sleep: &v1.SleepAction{
					Seconds: *conf.PreStopHookSleepSeconds,
				},
			},
		}
	}
	return pod
}

func createPausePod(ctx context.Context, f *framework.Framework, conf pausePodConfig) *v1.Pod {
	namespace := conf.Namespace
	if len(namespace) == 0 {
		namespace = f.Namespace.Name
	}
	pod, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, initPausePod(f, conf), metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return pod
}

func runPausePod(ctx context.Context, f *framework.Framework, conf pausePodConfig) *v1.Pod {
	pod := createPausePod(ctx, f, conf)
	framework.ExpectNoError(e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStartShort))
	pod, err := f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, conf.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return pod
}

func runPodAndGetNodeName(ctx context.Context, f *framework.Framework, conf pausePodConfig) string {
	// 启动一个 pod 来找到能启动 pod 的节点。我们特意不
	// 直接取节点列表并选第一个。取决于
	// 集群和 scheduler，"普通" pod 可能无法
	// 调度到该节点上。
	pod := runPausePod(ctx, f, conf)

	ginkgo.By("Explicitly delete pod here to free the resource it takes.")
	err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
	framework.ExpectNoError(err)

	return pod.Spec.NodeName
}

func getRequestedCPU(pod v1.Pod) int64 {
	var result int64
	for _, container := range pod.Spec.Containers {
		result += container.Resources.Requests.Cpu().MilliValue()
	}
	return result
}

func getRequestedStorageEphemeralStorage(pod v1.Pod) int64 {
	var result int64
	for _, container := range pod.Spec.Containers {
		result += container.Resources.Requests.StorageEphemeral().Value()
	}
	return result
}

// removeTaintFromNodeAction 返回一个闭包，调用时从
// 给定节点移除给定 taint。
func removeTaintFromNodeAction(cs clientset.Interface, nodeName string, testTaint v1.Taint) Action {
	return func(ctx context.Context) error {
		e2enode.RemoveTaintOffNode(ctx, cs, nodeName, testTaint)
		return nil
	}
}

// createPausePodAction 返回一个闭包，调用时创建 pause pod。
func createPausePodAction(f *framework.Framework, conf pausePodConfig) Action {
	return func(ctx context.Context) error {
		_, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, initPausePod(f, conf), metav1.CreateOptions{})
		return err
	}
}

// WaitForSchedulerAfterAction 执行提供的操作，然后等待
// scheduler 对给定 pod 采取行动。
func WaitForSchedulerAfterAction(ctx context.Context, f *framework.Framework, action Action, ns, podName string, expectSuccess bool) {
	predicate := scheduleFailureEvent(podName)
	if expectSuccess {
		predicate = scheduleSuccessEvent(ns, podName, "" /* any node */)
	}
	observed, err := observeEventAfterAction(ctx, f.ClientSet, f.Namespace.Name, predicate, action)
	framework.ExpectNoError(err)
	if expectSuccess && !observed {
		framework.Failf("Did not observe success event after performing the supplied action for pod %v", podName)
	}
	if !expectSuccess && !observed {
		framework.Failf("Did not observe failed event after performing the supplied action for pod %v", podName)
	}
}

// TODO: 等我们能运行 PodAffinity 测试时，升级其中的调用
func verifyResult(ctx context.Context, c clientset.Interface, expectedScheduled int, expectedNotScheduled int, ns string) {
	allPods, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	scheduledPods, notScheduledPods := GetPodsScheduled(workerNodes, allPods)

	gomega.Expect(notScheduledPods).To(gomega.HaveLen(expectedNotScheduled), fmt.Sprintf("Not scheduled Pods: %#v", notScheduledPods))
	gomega.Expect(scheduledPods).To(gomega.HaveLen(expectedScheduled), fmt.Sprintf("Scheduled Pods: %#v", scheduledPods))
}

// GetNodeThatCanRunPod 尝试启动一个不带 label 的 pod，以找到能运行它的节点
func GetNodeThatCanRunPod(ctx context.Context, f *framework.Framework) string {
	ginkgo.By("Trying to launch a pod without a label to get a node which can launch it.")
	return runPodAndGetNodeName(ctx, f, pausePodConfig{Name: "without-label"})
}

// Get2NodesThatCanRunPod 返回一个长度为 2 的切片，其中的节点可以运行 pod。
func Get2NodesThatCanRunPod(ctx context.Context, f *framework.Framework) []string {
	firstNode := GetNodeThatCanRunPod(ctx, f)
	ginkgo.By("Trying to launch a pod without a label to get a node which can launch it.")
	pod := pausePodConfig{
		Name: "without-label",
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchFields: []v1.NodeSelectorRequirement{
								{Key: "metadata.name", Operator: v1.NodeSelectorOpNotIn, Values: []string{firstNode}},
							},
						},
					},
				},
			},
		},
	}
	secondNode := runPodAndGetNodeName(ctx, f, pod)
	return []string{firstNode, secondNode}
}

func getNodeThatCanRunPodWithoutToleration(ctx context.Context, f *framework.Framework) string {
	ginkgo.By("Trying to launch a pod without a toleration to get a node which can launch it.")
	return runPodAndGetNodeName(ctx, f, pausePodConfig{Name: "without-toleration"})
}

// CreateHostPortPods 创建 host port 为 4321 的 RC
func CreateHostPortPods(ctx context.Context, f *framework.Framework, id string, replicas int, expectRunning bool) {
	ginkgo.By("Running RC which reserves host port")
	config := &testutils.RCConfig{
		Client:    f.ClientSet,
		Name:      id,
		Namespace: f.Namespace.Name,
		Timeout:   defaultTimeout,
		Image:     imageutils.GetPauseImageName(),
		Replicas:  replicas,
		HostPorts: map[string]int{"port1": 4321},
	}
	err := e2erc.RunRC(ctx, *config)
	if expectRunning {
		framework.ExpectNoError(err)
	}
}

// CreateNodeSelectorPods 创建 host port 为 4321 且定义 node selector 的 RC
func CreateNodeSelectorPods(ctx context.Context, f *framework.Framework, id string, replicas int, nodeSelector map[string]string, expectRunning bool) error {
	ginkgo.By("Running RC which reserves host port and defines node selector")

	config := &testutils.RCConfig{
		Client:       f.ClientSet,
		Name:         id,
		Namespace:    f.Namespace.Name,
		Timeout:      defaultTimeout,
		Image:        imageutils.GetPauseImageName(),
		Replicas:     replicas,
		HostPorts:    map[string]int{"port1": 4321},
		NodeSelector: nodeSelector,
	}
	err := e2erc.RunRC(ctx, *config)
	if expectRunning {
		return err
	}
	return nil
}

// 根据 nodeSelector 在指定节点上创建使用 hostport 的 pod
// 它在暴露的端口上启动一个 http server
func createHostPortPodOnNode(ctx context.Context, f *framework.Framework, podName, ns, hostIP string, port int32, protocol v1.Protocol, nodeSelector map[string]string, expectScheduled bool) {
	hostPortPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "agnhost",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args:  []string{"netexec", "--http-port=8080", "--udp-port=8080"},
					Ports: []v1.ContainerPort{
						{
							HostPort:      port,
							ContainerPort: 8080,
							Protocol:      protocol,
							HostIP:        hostIP,
						},
					},
					ReadinessProbe: &v1.Probe{
						ProbeHandler: v1.ProbeHandler{
							HTTPGet: &v1.HTTPGetAction{
								Path: "/hostname",
								Port: intstr.IntOrString{
									IntVal: int32(8080),
								},
								Scheme: v1.URISchemeHTTP,
							},
						},
					},
				},
			},
			NodeSelector: nodeSelector,
		},
	}
	_, err := f.ClientSet.CoreV1().Pods(ns).Create(ctx, hostPortPod, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	err = e2epod.WaitForPodNotPending(ctx, f.ClientSet, ns, podName)
	if expectScheduled {
		framework.ExpectNoError(err)
	}
}

// GetPodsScheduled 返回 worker 节点上当前已调度和未调度的 Pod 数量。
func GetPodsScheduled(workerNodes sets.Set[string], pods *v1.PodList) (scheduledPods, notScheduledPods []v1.Pod) {
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != "" && workerNodes.Has(pod.Spec.NodeName) {
			_, scheduledCondition := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
			if scheduledCondition == nil {
				framework.Failf("Did not find 'scheduled' condition for pod %+v", podName)
			}
			if scheduledCondition.Status != v1.ConditionTrue {
				framework.Failf("PodStatus isn't 'true' for pod %+v", podName)
			}
			scheduledPods = append(scheduledPods, pod)
		} else if pod.Spec.NodeName == "" {
			notScheduledPods = append(notScheduledPods, pod)
		}
	}
	return
}

// getNodeHostIP 返回节点上匹配主 Cluster IP 地址族的第一个内部 IP
func getNodeHostIP(ctx context.Context, f *framework.Framework, nodeName string) string {
	// 获取节点的内部 HostIP
	family := v1.IPv4Protocol
	if framework.TestContext.ClusterIsIPv6() {
		family = v1.IPv6Protocol
	}
	node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	ips := e2enode.GetAddressesByTypeAndFamily(node, v1.NodeInternalIP, family)
	if len(ips) == 0 {
		ips = e2enode.GetAddressesByTypeAndFamily(node, v1.NodeExternalIP, family)
	}
	if len(ips) == 0 {
		framework.Failf("No address by family (%s) on node (%s) found.",
			family, nodeName)
	}
	return ips[0]
}
