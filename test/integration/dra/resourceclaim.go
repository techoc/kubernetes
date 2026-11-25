/*
Copyright 2025 The Kubernetes Authors.

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

package dra

import (
	"sync"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

// testShareResourceClaim 源自 PR https://github.com/kubernetes/kubernetes/pull/133214 从 E2E 移除的
// "sharing a claim sequentially" 测试：创建一个 ResourceClaim，然后调度超过并发上限数量的 Pod 使用它。
// 在 E2E 中调度如此多的 Pod 很麻烦，集成测试里环境可控则无此问题。
func testShareResourceClaimSequentially(tCtx ktesting.TContext) {
	// 部分 E2E helper 仍直接使用 Gomega，TODO 改为使用 TContext；
	// 在此之前串行运行以让 Gomega 正常工作。
	gomega.RegisterTestingT(tCtx)
	tCtx.Cleanup(func() {
		gomega.RegisterFailHandler(nil)
	})

	tCtx = tCtx.WithNamespace(createTestNamespace(tCtx, nil))
	startScheduler(tCtx)
	startClaimController(tCtx)

	nodes := drautils.NewNodesNow(tCtx, 1, 8)
	driver := drautils.NewDriverInstance(tCtx)
	driver.WithRealNodes = false
	driver.WithKubelet = false
	driver.Run(tCtx, "/no/kubelet/root", nodes, drautils.NetworkResources(1, false)(nodes))
	b := drautils.NewBuilderNow(tCtx, driver)

	var objects []klog.KMetadata
	claim := b.ExternalClaim()
	objects = append(objects, claim)

	// 本测试原先验证同一时刻只有一个 Pod 使用 claim。
	// 移除 "not sharable" 特性并提高最大消费者数后，它变成了压力测试。
	numMaxPods := resourceapi.ResourceClaimReservedForMaxSize
	tCtx.Logf("Creating %d pods sharing the same claim", numMaxPods)
	pods := make([]*v1.Pod, numMaxPods)
	for i := range numMaxPods {
		pod := b.PodExternal(claim.Name)
		pods[i] = pod
		objects = append(objects, pod)
	}
	b.Create(tCtx, objects...)

	podStartTimeout := 5 * time.Minute * time.Duration(numMaxPods)
	ensureDuration := time.Minute // 检查时间不宜过长，精度差一些可以接受。
	podIsScheduled := func() gomega.OmegaMatcher {
		return gomega.HaveField("Spec.NodeName", gomega.Not(gomega.BeEmpty()))
	}
	podIsPending := func() gomega.OmegaMatcher {
		return gomega.And(
			gomega.HaveField("Status.Phase", gomega.Equal(v1.PodPending)),
			gomega.HaveField("Status.Conditions", gomega.ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   gomega.Equal(v1.PodScheduled),
				"Status": gomega.Equal(v1.ConditionFalse),
				// 当前调度器将创建过多 Pod 视为错误，因此这里应匹配 "SchedulerError"。
				// 也许它本不该是错误？
				// "Reason": gomega.Equal(v1.PodReasonUnschedulable),
			}))),
		)
	}
	assertPodScheduledEventually := func(tCtx ktesting.TContext, pod *v1.Pod) {
		tCtx.Helper()
		tCtx.AssertEventually(tCtx.Client().CoreV1().Pods(pod.Namespace).Get).
			WithArguments(pod.Name, metav1.GetOptions{}).
			WithTimeout(podStartTimeout).
			WithPolling(10*time.Second).
			Should(podIsScheduled(), "Pod %s should get scheduled.", pod.Name)
	}

	assertPodPendingEventually := func(tCtx ktesting.TContext, pod *v1.Pod) {
		tCtx.Helper()
		tCtx.AssertEventually(tCtx.Client().CoreV1().Pods(pod.Namespace).Get).
			WithArguments(pod.Name, metav1.GetOptions{}).
			WithTimeout(ensureDuration).
			WithPolling(10*time.Second).
			Should(podIsPending(), "Pod %s should remain pending.", pod.Name)
	}

	// 无需关心具体顺序，只要所有 Pod 最终都被调度即可。
	tCtx.Logf("Waiting for %d pods to be scheduled", numMaxPods)
	var wg sync.WaitGroup
	for i := range numMaxPods {
		wg.Go(func() {
			assertPodScheduledEventually(tCtx, pods[i])
		})
	}
	wg.Wait()
	if tCtx.Failed() {
		return
	}

	// TODO (?): 检查调度相关指标并加速 Pod 调度。
	// 目前 Pod 会进入 backoff，导致最初 256 个 Pod 的调度远慢于理论值。

	numMorePods := 10
	tCtx.Logf("Creating %d additional pods for the same claim", numMorePods)
	morePods := make([]*v1.Pod, numMorePods)
	objects = nil
	for i := range numMorePods {
		pod := b.PodExternal(claim.Name)
		morePods[i] = pod
		objects = append(objects, pod)
	}
	b.Create(tCtx, objects...)

	// 受 ReservedFor 上限限制，新增的 Pod 都无法运行。
	tCtx.Logf("Check for %s that the additional pods don't get scheduled", ensureDuration)
	for i := range numMorePods {
		wg.Go(func() {
			assertPodPendingEventually(tCtx, morePods[i])
		})
	}
	wg.Wait()
	if tCtx.Failed() {
		return
	}

	// 必须强制删除已调度的 Pod（没有 kubelet！），否则新 Pod 无法使用该 claim。
	tCtx.Logf("Deleting the initial %d pods", numMaxPods)
	for i := range numMaxPods {
		wg.Go(func() {
			if !tCtx.AssertNoError(tCtx.Client().CoreV1().Pods(pods[i].Namespace).Delete(tCtx, pods[i].Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})) {
				return
			}
			// 删除应当几乎（或者说完全？）瞬时完成。
			tCtx.AssertNoError(e2epod.WaitForPodNotFoundInNamespace(tCtx, tCtx.Client(), pods[i].Name, pods[i].Namespace, time.Second))
		})
	}
	wg.Wait()
	if tCtx.Failed() {
		return
	}

	// 现在这些 Pod 也应该最终被调度了……
	tCtx.Logf("Waiting for the additional %d pods to be scheduled", numMorePods)
	for i := range numMorePods {
		wg.Go(func() {
			assertPodScheduledEventually(tCtx, morePods[i])
		})
	}
	wg.Wait()
}
