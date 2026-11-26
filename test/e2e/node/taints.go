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

package node

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

const (
	testFinalizer = "example.com/test-finalizer"
)

func getTestTaint() v1.Taint {
	now := metav1.Now()
	return v1.Taint{
		Key:       "kubernetes.io/e2e-evict-taint-key",
		Value:     "evictTaintVal",
		Effect:    v1.TaintEffectNoExecute,
		TimeAdded: &now,
	}
}

// 为本测试创建一个默认 Pod，参数表示 Pod 是否应对本测试使用的 Taint 具有
// toleration。
func createPodForTaintsTest(hasToleration bool, tolerationSeconds int, podName, podLabel, ns string) *v1.Pod {
	grace := int64(1)
	if !hasToleration {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:                       podName,
				Namespace:                  ns,
				Labels:                     map[string]string{"group": podLabel},
				DeletionGracePeriodSeconds: &grace,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "pause",
						Image: imageutils.GetE2EImage(imageutils.Pause),
					},
				},
			},
		}
	}
	if tolerationSeconds <= 0 {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:                       podName,
				Namespace:                  ns,
				Labels:                     map[string]string{"group": podLabel},
				DeletionGracePeriodSeconds: &grace,
				// 默认 - 永久容忍
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "pause",
						Image: imageutils.GetE2EImage(imageutils.Pause),
					},
				},
				Tolerations: []v1.Toleration{{Key: "kubernetes.io/e2e-evict-taint-key", Value: "evictTaintVal", Effect: v1.TaintEffectNoExecute}},
			},
		}
	}
	ts := int64(tolerationSeconds)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       podName,
			Namespace:                  ns,
			Labels:                     map[string]string{"group": podLabel},
			DeletionGracePeriodSeconds: &grace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "pause",
					Image: imageutils.GetE2EImage(imageutils.Pause),
				},
			},
			// 默认 - 永久容忍
			Tolerations: []v1.Toleration{{Key: "kubernetes.io/e2e-evict-taint-key", Value: "evictTaintVal", Effect: v1.TaintEffectNoExecute, TolerationSeconds: &ts}},
		},
	}
}

// 创建并启动一个 controller（informer），监听给定命名空间中指定名称的 Pod 更新。每次看到删除事件，
// 就往 observedDeletion channel 放入一个新结构体。
func createTestController(ctx context.Context, cs clientset.Interface, observedDeletions chan string, podLabel, ns string) {
	_, controller := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"group": podLabel}).String()
				obj, err := cs.CoreV1().Pods(ns).List(ctx, options)
				return runtime.Object(obj), err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"group": podLabel}).String()
				return cs.CoreV1().Pods(ns).Watch(ctx, options)
			},
		},
		&v1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: func(oldObj interface{}) {
				if delPod, ok := oldObj.(*v1.Pod); ok {
					observedDeletions <- delPod.Name
				} else {
					observedDeletions <- ""
				}
			},
		},
	)
	framework.Logf("Starting informer...")
	go controller.Run(ctx.Done())
}

const (
	kubeletPodDeletionDelaySeconds = 60
	additionalWaitPerDeleteSeconds = 5
)

// 测试 NoExecuteTaintManager 的行为，包含以下场景：
// - 不容忍的 Pod 被从带 taint 的节点驱逐，
// - 容忍的 Pod 不被从带 taint 的节点驱逐，
// - 短时间容忍的 Pod 延迟从带 taint 的节点驱逐，
// - taint 移除后短时间容忍的 Pod 不被驱逐。
var _ = SIGDescribe("NoExecuteTaintManager Single Pod", framework.WithSerial(), func() {
	var cs clientset.Interface
	var ns string
	f := framework.NewDefaultFramework("taint-single-pod")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.BeforeEach(func(ctx context.Context) {
		cs = f.ClientSet
		ns = f.Namespace.Name

		e2enode.WaitForTotalHealthy(ctx, cs, time.Minute)

		err := framework.CheckTestingNSDeletedExcept(ctx, cs, ns)
		framework.ExpectNoError(err)
	})

	// 1. 运行一个 Pod
	// 2. 给运行该 Pod 的节点打上 no-execute taint
	// 3. 观察 Pod 是否被驱逐
	ginkgo.It("evicts pods from tainted nodes", func(ctx context.Context) {
		podName := "taint-eviction-1"
		pod := createPodForTaintsTest(false, 0, podName, podName, ns)
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podName, ns)

		ginkgo.By("Starting pod...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod is running on %v. Tainting Node", nodeName)

		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		// 稍等片刻
		ginkgo.By("Waiting for Pod to be deleted")
		timeoutChannel := time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+additionalWaitPerDeleteSeconds) * time.Second).C
		select {
		case <-timeoutChannel:
			framework.Failf("Failed to evict Pod")
		case <-observedDeletions:
			framework.Logf("Noticed Pod eviction. Test successful")
		}
	})

	// 1. 运行一个带 toleration 的 Pod
	// 2. 给运行该 Pod 的节点打上 no-execute taint
	// 3. 观察 Pod 是否不被驱逐
	ginkgo.It("doesn't evict pod with tolerations from tainted nodes", func(ctx context.Context) {
		podName := "taint-eviction-2"
		pod := createPodForTaintsTest(true, 0, podName, podName, ns)
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podName, ns)

		ginkgo.By("Starting pod...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod is running on %v. Tainting Node", nodeName)

		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		// 稍等片刻
		ginkgo.By("Waiting for Pod to be deleted")
		timeoutChannel := time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+additionalWaitPerDeleteSeconds) * time.Second).C
		select {
		case <-timeoutChannel:
			framework.Logf("Pod wasn't evicted. Test successful")
		case <-observedDeletions:
			framework.Failf("Pod was evicted despite toleration")
		}
	})

	// 1. 运行一个带有限 toleration 的 Pod
	// 2. 给运行该 Pod 的节点打上 no-execute taint
	// 3. 观察 toleration 到期前 Pod 是否不被驱逐
	// 4. 观察 toleration 到期后 Pod 是否被驱逐
	ginkgo.It("eventually evict pod with finite tolerations from tainted nodes", func(ctx context.Context) {
		podName := "taint-eviction-3"
		pod := createPodForTaintsTest(true, kubeletPodDeletionDelaySeconds+2*additionalWaitPerDeleteSeconds, podName, podName, ns)
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podName, ns)

		ginkgo.By("Starting pod...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod is running on %v. Tainting Node", nodeName)

		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		// 稍等片刻
		ginkgo.By("Waiting to see if a Pod won't be deleted")
		timeoutChannel := time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+additionalWaitPerDeleteSeconds) * time.Second).C
		select {
		case <-timeoutChannel:
			framework.Logf("Pod wasn't evicted")
		case <-observedDeletions:
			framework.Failf("Pod was evicted despite toleration")
			return
		}
		ginkgo.By("Waiting for Pod to be deleted")
		timeoutChannel = time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+additionalWaitPerDeleteSeconds) * time.Second).C
		select {
		case <-timeoutChannel:
			framework.Failf("Pod wasn't evicted")
		case <-observedDeletions:
			framework.Logf("Pod was evicted after toleration time run out. Test successful")
			return
		}
	})

	/*
		Release: v1.16
		Testname: Taint, Pod Eviction on taint removal
		Description: 带 toleration timeout 的 Pod 调度到带 taint 的节点后，如果在 toleration 到期前 taint 被移除，该 Pod 不得被
		驱逐。
	*/
	framework.ConformanceIt("removing taint cancels eviction", f.WithDisruptive(), func(ctx context.Context) {
		podName := "taint-eviction-4"
		pod := createPodForTaintsTest(true, 2*additionalWaitPerDeleteSeconds, podName, podName, ns)
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podName, ns)

		// 1. 运行一个短时间 toleration 的 Pod
		ginkgo.By("Starting pod...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod is running on %v. Tainting Node", nodeName)

		// 2. 给运行该 Pod 的节点打上 no-execute taint
		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		taintRemoved := false
		ginkgo.DeferCleanup(func(ctx context.Context) {
			if !taintRemoved {
				e2enode.RemoveTaintOffNode(ctx, cs, nodeName, testTaint)
			}
		})

		// 3. 等待一段时间
		ginkgo.By("Waiting short time to make sure Pod is queued for deletion")
		timeoutChannel := time.NewTimer(additionalWaitPerDeleteSeconds).C
		select {
		case <-timeoutChannel:
			framework.Logf("Pod wasn't evicted. Proceeding")
		case <-observedDeletions:
			framework.Failf("Pod was evicted despite toleration")
			return
		}

		// 4. 移除 taint
		framework.Logf("Removing taint from Node")
		e2enode.RemoveTaintOffNode(ctx, cs, nodeName, testTaint)
		taintRemoved = true

		// 5. 观察 Pod 是否不被驱逐。
		ginkgo.By("Waiting some time to make sure that toleration time passed.")
		timeoutChannel = time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+3*additionalWaitPerDeleteSeconds) * time.Second).C
		select {
		case <-timeoutChannel:
			framework.Logf("Pod wasn't evicted. Test successful")
		case <-observedDeletions:
			framework.Failf("Pod was evicted despite toleration")
		}
	})

	// 1. 运行一个带 finalizer 的 Pod
	// 2. 给运行该 Pod 的节点打上 no-execute taint
	// 3. 观察 Pod 是否被驱逐并带有 pod disruption 条件
	// 4. 移除 finalizer，使 Pod 能被 GC 删除
	ginkgo.It("pods evicted from tainted nodes have pod disruption condition", func(ctx context.Context) {
		podName := "taint-eviction-pod-disruption"
		pod := createPodForTaintsTest(false, 0, podName, podName, ns)
		pod.Finalizers = append(pod.Finalizers, testFinalizer)

		ginkgo.By("Starting pod...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod is running on %v. Tainting Node", nodeName)

		ginkgo.DeferCleanup(e2epod.NewPodClient(f).RemoveFinalizer, pod.Name, testFinalizer)

		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		ginkgo.By("Waiting for Pod to be terminating")
		timeout := time.Duration(kubeletPodDeletionDelaySeconds+3*additionalWaitPerDeleteSeconds) * time.Second
		err = e2epod.WaitForPodTerminatingInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, timeout)
		framework.ExpectNoError(err)

		ginkgo.By("Verifying the pod has the pod disruption condition")
		e2epod.VerifyPodHasConditionWithType(ctx, f, pod, v1.DisruptionTarget)
	})
})

var _ = SIGDescribe("NoExecuteTaintManager Multiple Pods", framework.WithSerial(), func() {
	var cs clientset.Interface
	var ns string
	f := framework.NewDefaultFramework("taint-multiple-pods")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.BeforeEach(func(ctx context.Context) {
		cs = f.ClientSet
		ns = f.Namespace.Name

		e2enode.WaitForTotalHealthy(ctx, cs, time.Minute)

		err := framework.CheckTestingNSDeletedExcept(ctx, cs, ns)
		framework.ExpectNoError(err)
	})

	// 1. 运行两个 Pod：一个带 toleration，一个不带
	// 2. 给运行这些 Pod 的节点打上 no-execute taint
	// 3. 观察不带 toleration 的 Pod 被驱逐，带 toleration 的 Pod 保留
	ginkgo.It("only evicts pods without tolerations from tainted nodes", func(ctx context.Context) {
		podGroup := "taint-eviction-a"
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podGroup, ns)

		pod1 := createPodForTaintsTest(false, 0, podGroup+"1", podGroup, ns)
		pod2 := createPodForTaintsTest(true, 0, podGroup+"2", podGroup, ns)

		ginkgo.By("Starting pods...")
		nodeName1, err := testutils.RunPodAndGetNodeName(ctx, cs, pod1, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod1 is running on %v. Tainting Node", nodeName1)
		nodeName2, err := testutils.RunPodAndGetNodeName(ctx, cs, pod2, 2*time.Minute)
		framework.ExpectNoError(err)
		framework.Logf("Pod2 is running on %v. Tainting Node", nodeName2)

		ginkgo.By("Trying to apply a taint on the Nodes")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName1, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName1, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName1, testTaint)
		if nodeName2 != nodeName1 {
			e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName2, testTaint)
			e2enode.ExpectNodeHasTaint(ctx, cs, nodeName2, &testTaint)
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName2, testTaint)
		}

		// 稍等片刻
		ginkgo.By("Waiting for Pod1 to be deleted")
		timeoutChannel := time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+additionalWaitPerDeleteSeconds) * time.Second).C
		var evicted int
		for {
			select {
			case <-timeoutChannel:
				if evicted == 0 {
					framework.Failf("Failed to evict Pod1.")
				} else if evicted == 2 {
					framework.Failf("Pod1 is evicted. But unexpected Pod2 also get evicted.")
				}
				return
			case podName := <-observedDeletions:
				evicted++
				if podName == podGroup+"1" {
					framework.Logf("Noticed Pod %q gets evicted.", podName)
				} else if podName == podGroup+"2" {
					framework.Failf("Unexpected Pod %q gets evicted.", podName)
					return
				}
			}
		}
	})

	/*
		Release: v1.16
		Testname: Pod Eviction, Toleration limits
		Description: 在带 tolerationSeconds 的多 Pod 场景中，Pod 必须按
		toleration 时间限制被驱逐。
	*/
	framework.ConformanceIt("evicts pods with minTolerationSeconds", f.WithDisruptive(), func(ctx context.Context) {
		podGroup := "taint-eviction-b"
		observedDeletions := make(chan string, 100)
		createTestController(ctx, cs, observedDeletions, podGroup, ns)

		// 1. 运行两个都带 toleration 的 Pod：一个 tolerationSeconds=5，另一个 25
		pod1 := createPodForTaintsTest(true, additionalWaitPerDeleteSeconds, podGroup+"1", podGroup, ns)
		pod2 := createPodForTaintsTest(true, 5*additionalWaitPerDeleteSeconds, podGroup+"2", podGroup, ns)

		ginkgo.By("Starting pods...")
		nodeName, err := testutils.RunPodAndGetNodeName(ctx, cs, pod1, 2*time.Minute)
		framework.ExpectNoError(err)
		node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		nodeHostNameLabel, ok := node.GetObjectMeta().GetLabels()["kubernetes.io/hostname"]
		if !ok {
			framework.Failf("error getting kubernetes.io/hostname label on node %s", nodeName)
		}
		framework.ExpectNoError(err)
		framework.Logf("Pod1 is running on %v. Tainting Node", nodeName)
		// 确保 pod2 与 pod1 落在同一节点
		pod2.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": nodeHostNameLabel}
		_, err = testutils.RunPodAndGetNodeName(ctx, cs, pod2, 2*time.Minute)
		framework.ExpectNoError(err)
		// 驱逐发生前等待 Pod 进入 running 状态
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod1))
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, cs, pod2))
		framework.Logf("Pod2 is running on %v. Tainting Node", nodeName)

		// 2. 给运行这些 Pod 的节点打上 no-execute taint
		ginkgo.By("Trying to apply a taint on the Node")
		testTaint := getTestTaint()
		e2enode.AddOrUpdateTaintOnNode(ctx, cs, nodeName, testTaint)
		e2enode.ExpectNodeHasTaint(ctx, cs, nodeName, &testTaint)
		ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, cs, nodeName, testTaint)

		// 3. 等待观察两个 Pod 是否在 [5, 25] 秒内被驱逐
		ginkgo.By("Waiting for Pod1 and Pod2 to be deleted")
		// 在 Windows 主机上，Pod 删除耗时更长，
		// 因此使用更大的超时比较稳妥
		timeoutChannel := time.NewTimer(time.Duration(kubeletPodDeletionDelaySeconds+10*additionalWaitPerDeleteSeconds) * time.Second).C
		var evicted int
		for evicted != 2 {
			select {
			case <-timeoutChannel:
				framework.Failf("Failed to evict all Pods. %d pod(s) is not evicted.", 2-evicted)
				return
			case podName := <-observedDeletions:
				framework.Logf("Noticed Pod %q gets evicted.", podName)
				evicted++
			}
		}
	})
})
