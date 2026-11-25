/*
Copyright 2019 The Kubernetes Authors.

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

package pod

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gcustom"
	"github.com/onsi/gomega/types"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	apitypes "k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	testutils "k8s.io/kubernetes/test/utils"
	"k8s.io/kubernetes/test/utils/ktesting/format"
)

const (
	// defaultPodDeletionTimeout 是删除 Pod 的默认超时。
	defaultPodDeletionTimeout = 3 * time.Minute

	// podListTimeout 是等待 Pod 可被列出多长时间。
	podListTimeout = time.Minute

	podRespondingTimeout = 15 * time.Minute

	// Pod 被调度到节点上需要多长时间
	podScheduledBeforeTimeout = podListTimeout + (20 * time.Second)

	// podStartTimeout 是等待 Pod 启动的时间。
	podStartTimeout = 5 * time.Minute

	// singleCallTimeout 是尝试单个 API 调用（如 'get' 或 'list'）的时长，用于防止
	// 瞬态失败导致测试失败。
	singleCallTimeout = 5 * time.Minute

	// 由于 volume 挂载/卸载延迟，某些 Pod 就绪可能需要更长时间。
	slowPodStartTimeout = 15 * time.Minute
)

type podCondition func(pod *v1.Pod) (bool, error)

// BeRunningNoRetries 验证 Pod 开始运行。当 Pod 进入其他永久阶段时，
// 视为永久失败。
func BeRunningNoRetries() types.GomegaMatcher {
	return gomega.And(
		// 这个额外的 matcher 检查最终的错误 condition。
		gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
			switch pod.Status.Phase {
			case v1.PodFailed, v1.PodSucceeded:
				return false, gomega.StopTrying(fmt.Sprintf("Expected pod to reach phase %q, got final phase %q instead:\n%s", v1.PodRunning, pod.Status.Phase, format.Object(pod, 1)))
			default:
				return true, nil
			}
		}),
		BeInPhase(v1.PodRunning),
	)
}

// BeRunningReadyNoRetries 验证 Pod 开始运行且具有 status 为 true 的
// ready condition。当 Pod 进入其他永久阶段时，视为永久失败。
// 视为永久失败。
func BeRunningReadyNoRetries() types.GomegaMatcher {
	return gomega.And(
		// 这个额外的 matcher 检查最终的错误 condition。
		gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
			switch pod.Status.Phase {
			case v1.PodFailed, v1.PodSucceeded:
				return false, gomega.StopTrying(fmt.Sprintf("Expected pod to reach phase %q, got final phase %q instead:\n%s", v1.PodRunning, pod.Status.Phase, format.Object(pod, 1)))
			default:
				return true, nil
			}
		}),
		BeInPhase(v1.PodRunning),
		gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
			return podutils.IsPodReady(pod), nil
		}).WithMessage("Expected pod to have a ready condition of status true"),
	)
}

// BeInPhase 在 pod.status.phase 为预期阶段时匹配。
func BeInPhase(phase v1.PodPhase) types.GomegaMatcher {
	// 一个简单的实现是：
	// return gomega.HaveField("Status.Phase", phase)
	//
	// 但这会产生相当笼统的
	//     Value for field 'Status.Phase' failed to satisfy matcher.
	// 失败消息，且不显示 Pod。用自定义 matcher 可以做得更好。
	// 用自定义 matcher 可以做得更好。

	return gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
		return pod.Status.Phase == phase, nil
	}).WithTemplate("Expected Pod {{.To}} be in {{format .Data}}\nGot instead:\n{{.FormattedActual}}").WithTemplateData(phase)
}

// WaitForAlmostAllReady 最多等待 timeout，直到满足以下条件：
// 1. 命名空间 ns 中至少 minPods 个 Pod 处于 Running 和 Ready
// 2. 命名空间 ns 中所有 Pod 要么 Ready 要么 Succeeded
// 3. 命名空间 ns 中属于 ReplicaSet 或 ReplicationController 的所有 Pod 都 Ready
//
// 超时后，如果处于 Pending 阶段的 Pod 数量
// 超过 allowedNotReadyPods，则返回错误。
//
// 一般建议尽可能使用 WaitForPodsRunningReady 而不是此函数，
// 因为其行为更直观。与 WaitForPodsRunningReady 类似，
// 该函数每次迭代都会请求 Pod 列表，适用于
// Pod 集合可能变化的场景，例如集群启动期间。
//
// 如果 minPods 或 allowedNotReadyPods 为 -1，此方法立即返回，
// 不等待。
func WaitForAlmostAllPodsReady(ctx context.Context, c clientset.Interface, ns string, minPods, allowedNotReadyPods int, timeout time.Duration) error {
	if minPods == -1 || allowedNotReadyPods == -1 {
		return nil
	}

	// 每次迭代都会重新获取 Pod、replication controller 和 replica
	// set 的列表，因为启动期间会有更多 Pod 上线，
	// 我们希望确保它们也被检查。
	//
	// 该结构体在轮询期间填充，然后检查；超时时
	// 会包含在失败消息中。
	type state struct {
		ReplicationControllers []v1.ReplicationController
		ReplicaSets            []appsv1.ReplicaSet
		Pods                   []v1.Pod
	}

	nOk := 0
	badPods := []v1.Pod{}
	otherPods := []v1.Pod{}
	succeededPods := []string{}

	err := framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*state, error) {

		rcList, err := c.CoreV1().ReplicationControllers(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing replication controllers in namespace %s: %w", ns, err)
		}
		rsList, err := c.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing replication sets in namespace %s: %w", ns, err)
		}
		podList, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing pods in namespace %s: %w", ns, err)
		}
		return &state{
			ReplicationControllers: rcList.Items,
			ReplicaSets:            rsList.Items,
			Pods:                   podList.Items,
		}, nil
	})).WithTimeout(timeout).Should(framework.MakeMatcher(func(s *state) (func() string, error) {
		replicas, replicaOk := int32(0), int32(0)
		for _, rc := range s.ReplicationControllers {
			replicas += *rc.Spec.Replicas
			replicaOk += rc.Status.ReadyReplicas
		}
		for _, rs := range s.ReplicaSets {
			replicas += *rs.Spec.Replicas
			replicaOk += rs.Status.ReadyReplicas
		}

		nOk = 0
		badPods = []v1.Pod{}
		otherPods = []v1.Pod{}
		succeededPods = []string{}
		for _, pod := range s.Pods {
			res, err := testutils.PodRunningReady(&pod)
			switch {
			case res && err == nil:
				nOk++
			case pod.Status.Phase == v1.PodSucceeded:
				// 等待这个 Pod 没有意义
				succeededPods = append(succeededPods, pod.Name)
			case pod.Status.Phase == v1.PodFailed:
				// 忽略由某个控制器管理的失败 Pod
				if metav1.GetControllerOf(&pod) == nil {
					badPods = append(badPods, pod)
				}
			default:
				otherPods = append(otherPods, pod)
			}
		}
		done := replicaOk == replicas && nOk >= minPods && (len(badPods)+len(otherPods)) == 0
		if done {
			return nil, nil
		}

		// 延迟格式化失败消息。
		return func() string {
			var buffer strings.Builder
			buffer.WriteString(fmt.Sprintf("Expected all pods (need at least %d) in namespace %q to be running and ready (except for %d).\n", minPods, ns, allowedNotReadyPods))
			buffer.WriteString(fmt.Sprintf("%d / %d pods were running and ready.\n", nOk, len(s.Pods)))
			buffer.WriteString(fmt.Sprintf("Expected %d pod replicas, %d are Running and Ready.\n", replicas, replicaOk))
			if len(succeededPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that completed successfully:\n%s", format.Object(succeededPods, 1)))
			}
			if len(badPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that failed and were not controlled by some controller:\n%s", format.Object(badPods, 1)))
			}
			if len(otherPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that were neither completed nor running:\n%s", format.Object(otherPods, 1)))
			}
			return buffer.String()
		}, nil
	}))

	// 错误可能不是致命的。
	if len(otherPods) <= allowedNotReadyPods {
		return nil
	}
	return err
}

// WaitForPodsRunningReady 最多等待 timeout，直到满足以下条件：
//  1. 命名空间 ns 中至少 minPods 个 Pod 处于 Running 和 Ready
//  2. 命名空间 ns 中没有不属于任何控制器的 Failed Pod，也没有 Pending Pod
//
// 如果超时前任一条件未满足，则返回错误。
//
// 它与其他 "等待 Pod" 函数的不同之处在于
// 它每次迭代都会请求 Pod 列表。这在
// 集群启动时很有用，因为等待期间 Pod 数量会增加。
// 所有处于 SUCCESS 状态的 Pod 不计入。
func WaitForPodsRunningReady(ctx context.Context, c clientset.Interface, ns string, minPods int, timeout time.Duration) error {

	return framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) ([]v1.Pod, error) {

		podList, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing pods in namespace %s: %w", ns, err)
		}
		return podList.Items, nil
	})).WithTimeout(timeout).Should(framework.MakeMatcher(func(pods []v1.Pod) (func() string, error) {

		nOk := 0
		badPods := []v1.Pod{}
		otherPods := []v1.Pod{}
		succeededPods := []string{}

		for _, pod := range pods {
			res, err := testutils.PodRunningReady(&pod)
			switch {
			case res && err == nil:
				nOk++
			case pod.Status.Phase == v1.PodSucceeded:
				// 忽略已成功的 Pod
				succeededPods = append(succeededPods, pod.Name)
			case pod.Status.Phase == v1.PodFailed:
				// 忽略由某个控制器管理的失败 Pod
				if metav1.GetControllerOf(&pod) == nil {
					badPods = append(badPods, pod)
				}
			default:
				otherPods = append(otherPods, pod)
			}
		}
		if nOk >= minPods && len(badPods)+len(otherPods) == 0 {
			return nil, nil
		}

		// 延迟格式化失败消息。
		return func() string {
			var buffer strings.Builder
			buffer.WriteString(fmt.Sprintf("Expected all pods (need at least %d) in namespace %q to be running and ready \n", minPods, ns))
			buffer.WriteString(fmt.Sprintf("%d / %d pods were running and ready.\n", nOk, len(pods)))
			if len(succeededPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that completed successfully:\n%s", format.Object(succeededPods, 1)))
			}
			if len(badPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that failed and were not controlled by some controller:\n%s", format.Object(badPods, 1)))
			}
			if len(otherPods) > 0 {
				buffer.WriteString(fmt.Sprintf("Pods that were neither completed nor running:\n%s", format.Object(otherPods, 1)))
			}
			return buffer.String()
		}, nil
	}))

}

// WaitForPodCondition 等待 Pod 匹配给定的 condition。
// condition 回调可用 gomega.StopTrying 提前中止。
func WaitForPodCondition(ctx context.Context, c clientset.Interface, ns, podName, conditionDesc string, timeout time.Duration, condition podCondition) error {
	return framework.Gomega().
		Eventually(ctx, framework.RetryNotFound(framework.GetObject(c.CoreV1().Pods(ns).Get, podName, metav1.GetOptions{}))).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
			done, err := condition(pod)
			if err != nil {
				return nil, err
			}
			if done {
				return nil, nil
			}
			return func() string {
				return fmt.Sprintf("expected pod to be %s, got instead:\n%s", conditionDesc, format.Object(pod, 1))
			}, nil
		}))
}

// WaitForPodObservedGeneration 等待 Pod 具有指定的 observed generation。
func WaitForPodObservedGeneration(ctx context.Context, c clientset.Interface, ns, podName string, expectedGeneration int64, timeout time.Duration) error {
	return framework.Gomega().
		Eventually(ctx, framework.RetryNotFound(framework.GetObject(c.CoreV1().Pods(ns).Get, podName, metav1.GetOptions{}))).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
			if pod.Status.ObservedGeneration == expectedGeneration {
				return nil, nil
			}
			return func() string {
				return fmt.Sprintf("expected pod generation to be %d, got %d instead:\n", expectedGeneration, pod.Status.ObservedGeneration)
			}, nil
		}))
}

// WaitForPodConditionObservedGeneration 等待 Pod condition 具有指定的 observed generation。
func WaitForPodConditionObservedGeneration(ctx context.Context, c clientset.Interface, ns, podName string, conditionType v1.PodConditionType, expectedGeneration int64, timeout time.Duration) error {
	return framework.Gomega().
		Eventually(ctx, framework.RetryNotFound(framework.GetObject(c.CoreV1().Pods(ns).Get, podName, metav1.GetOptions{}))).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == conditionType {
					if condition.ObservedGeneration == expectedGeneration {
						return nil, nil
					} else {
						return func() string {
							return fmt.Sprintf("expected condition %s generation to be %d, got %d instead:\n", conditionType, expectedGeneration, condition.ObservedGeneration)
						}, nil
					}
				}
			}

			return func() string {
				return fmt.Sprintf("could not find condition %s:\n", conditionType)
			}, nil
		}))
}

// Range 决定必须存在多少项以及多少项必须匹配某个
// condition。小于等于 0 的值会被忽略。
// TODO（?）：移至 test/e2e/framework/range
type Range struct {
	// MinMatching 必须小于等于实际匹配项数，或小于等于 0。
	MinMatching int
	// MaxMatching 必须大于等于实际匹配项数，或小于等于 0。
	// 要检查"没有匹配项"，请设置 NonMatching。
	MaxMatching int
	// NoneMatching 表示不能有任何项匹配。
	NoneMatching bool
	// AllMatching 表示所有项都必须匹配。
	AllMatching bool
	// MinFound 必须小于等于现有项数，或小于等于 0。
	MinFound int
}

// Min 返回必须存在多少项。
func (r Range) Min() int {
	min := r.MinMatching
	if min < r.MinFound {
		min = r.MinFound
	}
	return min
}

// WaitForPods 等待指定命名空间中的 Pod 匹配给定的
// condition。必须存在多少 Pod、多少 Pod 必须匹配 condition
// 由 range 参数决定。condition 回调可使用
// gomega.StopTrying(...).Now() 提前中止。condition 描述
// 将用于 "expected pods to <description>"。
func WaitForPods(ctx context.Context, c clientset.Interface, ns string, opts metav1.ListOptions, r Range, timeout time.Duration, conditionDesc string, condition func(*v1.Pod) bool) (*v1.PodList, error) {
	var finalPods *v1.PodList
	minPods := r.Min()
	match := func(pods *v1.PodList) (func() string, error) {
		finalPods = pods

		if len(pods.Items) < minPods {
			return func() string {
				return fmt.Sprintf("expected at least %d pods, only got %d", minPods, len(pods.Items))
			}, nil
		}

		var nonMatchingPods, matchingPods []v1.Pod
		for _, pod := range pods.Items {
			if condition(&pod) {
				matchingPods = append(matchingPods, pod)
			} else {
				nonMatchingPods = append(nonMatchingPods, pod)
			}
		}
		matching := len(pods.Items) - len(nonMatchingPods)
		if matching < r.MinMatching && r.MinMatching > 0 {
			return func() string {
				return fmt.Sprintf("expected at least %d pods to %s, %d out of %d were not:\n%s",
					r.MinMatching, conditionDesc, len(nonMatchingPods), len(pods.Items),
					format.Object(nonMatchingPods, 1))
			}, nil
		}
		if len(nonMatchingPods) > 0 && r.AllMatching {
			return func() string {
				return fmt.Sprintf("expected all pods to %s, %d out of %d were not:\n%s",
					conditionDesc, len(nonMatchingPods), len(pods.Items),
					format.Object(nonMatchingPods, 1))
			}, nil
		}
		if matching > r.MaxMatching && r.MaxMatching > 0 {
			return func() string {
				return fmt.Sprintf("expected at most %d pods to %s, %d out of %d were:\n%s",
					r.MinMatching, conditionDesc, len(matchingPods), len(pods.Items),
					format.Object(matchingPods, 1))
			}, nil
		}
		if matching > 0 && r.NoneMatching {
			return func() string {
				return fmt.Sprintf("expected no pods to %s, %d out of %d were:\n%s",
					conditionDesc, len(matchingPods), len(pods.Items),
					format.Object(matchingPods, 1))
			}, nil
		}
		return nil, nil
	}

	err := framework.Gomega().
		Eventually(ctx, framework.ListObjects(c.CoreV1().Pods(ns).List, opts)).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(match))
	return finalPods, err
}

// RunningReady 检查 Pod p 的 phase 是否为 running，且具有 status 为 true 的
// ready condition。
func RunningReady(p *v1.Pod) bool {
	return p.Status.Phase == v1.PodRunning && podutils.IsPodReady(p)
}

// WaitForPodsRunning 在给定的 `timeout` 内等待，判断指定 `ns` 中是否有一定数量的 Pod 正在运行。
func WaitForPodsRunning(ctx context.Context, c clientset.Interface, ns string, num int, timeout time.Duration) error {
	_, err := WaitForPods(ctx, c, ns, metav1.ListOptions{}, Range{MinMatching: num, MaxMatching: num}, timeout,
		"be running and ready", func(pod *v1.Pod) bool {
			ready, _ := testutils.PodRunningReady(pod)
			return ready
		})
	return err
}

// WaitForPodsSchedulingGated 在给定的 `timeout` 内等待，判断指定 `ns` 中是否有一定数量的 Pod 保持在 scheduling gated 状态。
func WaitForPodsSchedulingGated(ctx context.Context, c clientset.Interface, ns string, num int, timeout time.Duration) error {
	_, err := WaitForPods(ctx, c, ns, metav1.ListOptions{}, Range{MinMatching: num, MaxMatching: num}, timeout,
		"be in scheduling gated state", func(pod *v1.Pod) bool {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == v1.PodScheduled && condition.Reason == v1.PodReasonSchedulingGated {
					return true
				}
			}
			return false
		})
	return err
}

// WaitForPodsWithSchedulingGates 在给定的 `timeout` 内等待，判断指定 `ns` 中是否有一定数量的 Pod
// 匹配给定的 `schedulingGates` 并保持在 scheduling gated 状态。
func WaitForPodsWithSchedulingGates(ctx context.Context, c clientset.Interface, ns string, num int, timeout time.Duration, schedulingGates []v1.PodSchedulingGate) error {
	_, err := WaitForPods(ctx, c, ns, metav1.ListOptions{}, Range{MinMatching: num, MaxMatching: num}, timeout,
		"have certain scheduling gates", func(pod *v1.Pod) bool {
			return reflect.DeepEqual(pod.Spec.SchedulingGates, schedulingGates)
		})
	return err
}

// 如果 Pod 在 podStartTimeout 之后过久才终止，WaitForPodTerminatedInNamespace 返回错误；
// 如果 Pod 的 Get API 返回错误（IsNotFound 或其他），或 Pod 失败（因而没有
// 终止）且原因不符合预期，也会返回错误。通常用于测试传入的 Pod 是否完全
// 终止（reason==""），但也可用于检测 Pod 是否*没有*按
// 给定的原因终止。
func WaitForPodTerminatedInNamespace(ctx context.Context, c clientset.Interface, podName, reason, namespace string) error {
	return WaitForPodTerminatedInNamespaceTimeout(ctx, c, podName, reason, namespace, podStartTimeout)
}

// 如果 Pod 在给定超时后过久才终止，WaitForPodTerminatedInNamespaceTimeout 返回错误；
// 如果 Pod 的 Get API 返回错误，或 Pod 失败且原因不符合预期，
// 也会返回错误。
// 终止）且原因不符合预期。通常用于测试
// 但也可用于检测 Pod 是否没有按给定原因终止。
// 但也可用于检测 Pod 是否没有按给定原因终止。
func WaitForPodTerminatedInNamespaceTimeout(ctx context.Context, c clientset.Interface, podName, reason, namespace string, timeout time.Duration) error {
	return WaitForPodCondition(ctx, c, namespace, podName, fmt.Sprintf("terminated with reason %s", reason), timeout, func(pod *v1.Pod) (bool, error) {
		// 只考虑 Failed 的 Pod。成功的 Pod 会被删除，并在
		// waitForPodCondition 的 Get 调用中检测为 `IsNotFound`。
		if pod.Status.Phase == v1.PodFailed {
			if pod.Status.Reason == reason { // 提前结束 waitForPodCondition 的循环
				return true, nil
			}
			return true, fmt.Errorf("Expected pod %q in namespace %q to be terminated with reason %q, got reason: %q", podName, namespace, reason, pod.Status.Reason)
		}
		return false, nil
	})
}

// WaitForPodTerminatingInNamespaceTimeout 在 Pod 正在终止时返回，若超时后仍未终止则返回错误。
func WaitForPodTerminatingInNamespaceTimeout(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return WaitForPodCondition(ctx, c, namespace, podName, "is terminating", timeout, func(pod *v1.Pod) (bool, error) {
		if pod.DeletionTimestamp != nil {
			return true, nil
		}
		return false, nil
	})
}

// 如果 Pod 进入 success 状态，WaitForPodSuccessInNamespaceTimeout 返回 nil；若失败或运行过久则返回错误。
func WaitForPodSuccessInNamespaceTimeout(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return WaitForPodCondition(ctx, c, namespace, podName, fmt.Sprintf("%s or %s", v1.PodSucceeded, v1.PodFailed), timeout, func(pod *v1.Pod) (bool, error) {
		if pod.DeletionTimestamp == nil && pod.Spec.RestartPolicy == v1.RestartPolicyAlways {
			return true, gomega.StopTrying(fmt.Sprintf("pod %q will never terminate with a succeeded state since its restart policy is Always", podName))
		}
		switch pod.Status.Phase {
		case v1.PodSucceeded:
			ginkgo.By("Saw pod success")
			return true, nil
		case v1.PodFailed:
			return true, gomega.StopTrying(fmt.Sprintf("pod %q failed with status: \n%s", podName, format.Object(pod.Status, 1)))
		default:
			return false, nil
		}
	})
}

// 如果 Pod 过久才进入 Pending 状态，
// 且 condition Status 变为 Unschedulable，WaitForPodNameUnschedulableInNamespace 返回错误；
// 如果 Get API 返回错误，或 Pod 因意外原因失败，也返回错误。
// 通常用于测试传入的 Pod 处于 Pending 且 Unschedulable。
func WaitForPodNameUnschedulableInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string) error {
	return WaitForPodCondition(ctx, c, namespace, podName, v1.PodReasonUnschedulable, podStartTimeout, func(pod *v1.Pod) (bool, error) {
		// 只考虑 Failed 的 Pod。成功的 Pod 会被删除，并在
		// waitForPodCondition 的 Get 调用中检测为 `IsNotFound`。
		if pod.Status.Phase == v1.PodPending {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse && cond.Reason == v1.PodReasonUnschedulable {
					return true, nil
				}
			}
		}
		if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			return true, fmt.Errorf("Expected pod %q in namespace %q to be in phase Pending, but got phase: %v", podName, namespace, pod.Status.Phase)
		}
		return false, nil
	})
}

// WaitForPodNameRunningInNamespace 等待默认时长（PodStartTimeout），直到指定 Pod 进入运行态。
// 如果先超时或 Pod 进入失败状态，则返回错误。
func WaitForPodNameRunningInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string) error {
	return WaitTimeoutForPodRunningInNamespace(ctx, c, podName, namespace, podStartTimeout)
}

// WaitForPodRunningInNamespaceSlow 等待更长时间（slowPodStartTimeout），直到指定 Pod 进入运行态。
// Watching 对象更改时会使用 resourceVersion，它表明我们从何时开始
// 关心 Pod 的更改。如果先超时或 Pod 进入失败状态，则返回错误。
func WaitForPodRunningInNamespaceSlow(ctx context.Context, c clientset.Interface, podName, namespace string) error {
	return WaitTimeoutForPodRunningInNamespace(ctx, c, podName, namespace, slowPodStartTimeout)
}

// WaitTimeoutForPodRunningInNamespace 等待给定的超时时长，直到指定 Pod 进入运行态。
// 调用此函数时 Pod 不必已存在，且预期 Pod 成功或失败后不会被重建。
// 调用时 Pod 不必已存在，且预期其成功或失败后不会被重建。
func WaitTimeoutForPodRunningInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return framework.Gomega().Eventually(ctx, framework.RetryNotFound(framework.GetObject(c.CoreV1().Pods(namespace).Get, podName, metav1.GetOptions{}))).
		WithTimeout(timeout).
		Should(BeRunningNoRetries())
}

// WaitTimeoutForPodRunningReadyInNamespace 等待给定的超时时长，直到指定 Pod 进入运行态
// 且具有 status 为 true 的 ready condition。
// 调用此函数时 Pod 不必已存在，且预期 Pod 成功或失败后不会被重建。
// 调用时 Pod 不必已存在，且预期其成功或失败后不会被重建。
func WaitTimeoutForPodRunningReadyInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return framework.Gomega().Eventually(ctx, framework.RetryNotFound(framework.GetObject(c.CoreV1().Pods(namespace).Get, podName, metav1.GetOptions{}))).
		WithTimeout(timeout).
		Should(BeRunningReadyNoRetries())
}

// WaitForPodRunningInNamespace 等待默认时长（podStartTimeout），直到指定 Pod 进入运行态。
// 如果先超时或 Pod 进入失败状态，则返回错误。
func WaitForPodRunningInNamespace(ctx context.Context, c clientset.Interface, pod *v1.Pod) error {
	if pod.Status.Phase == v1.PodRunning {
		return nil
	}
	return WaitTimeoutForPodRunningInNamespace(ctx, c, pod.Name, pod.Namespace, podStartTimeout)
}

// WaitTimeoutForPodNoLongerRunningInNamespace 等待给定的超时时长，直到指定 Pod 停止。
func WaitTimeoutForPodNoLongerRunningInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return WaitForPodCondition(ctx, c, namespace, podName, "completed", timeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodFailed, v1.PodSucceeded:
			return true, nil
		}
		return false, nil
	})
}

// WaitForPodNoLongerRunningInNamespace 等待默认时长（defaultPodDeletionTimeout），直到指定 Pod 停止运行。
// 如果先超时，则返回错误。
func WaitForPodNoLongerRunningInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string) error {
	return WaitTimeoutForPodNoLongerRunningInNamespace(ctx, c, podName, namespace, defaultPodDeletionTimeout)
}

// WaitTimeoutForPodReadyInNamespace 等待给定的超时时长，直到
// 指定 Pod 就绪并运行。
func WaitTimeoutForPodReadyInNamespace(ctx context.Context, c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return WaitForPodCondition(ctx, c, namespace, podName, "running and ready", timeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodFailed, v1.PodSucceeded:
			return false, gomega.StopTrying(fmt.Sprintf("The phase of Pod %s is %s which is unexpected.", pod.Name, pod.Status.Phase))
		case v1.PodRunning:
			return podutils.IsPodReady(pod), nil
		}
		return false, nil
	})
}

// 如果 Pod 过久才退出 pending 状态，WaitForPodNotPending 返回错误。
// Watching 对象更改时会使用 resourceVersion，它表明我们从何时开始
// 关心 Pod 的更改。
func WaitForPodNotPending(ctx context.Context, c clientset.Interface, ns, podName string) error {
	return WaitForPodCondition(ctx, c, ns, podName, "not pending", podStartTimeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodPending:
			return false, nil
		default:
			return true, nil
		}
	})
}

// 如果 Pod 进入 success 状态，WaitForPodSuccessInNamespace 返回 nil；若失败或直到 podStartupTimeout 仍未成功则返回错误。
func WaitForPodSuccessInNamespace(ctx context.Context, c clientset.Interface, podName string, namespace string) error {
	return WaitForPodSuccessInNamespaceTimeout(ctx, c, podName, namespace, podStartTimeout)
}

// 如果 Pod 过久才完全终止，WaitForPodNotFoundInNamespace 返回错误。
// 与 `waitForPodTerminatedInNamespace` 不同，Pod 的 Phase 和 Reason 会被忽略。如果 Pod 的 Get
// API 返回 IsNotFound，则等待停止并返回 nil。如果 Get API 返回
// "not found" 以外的最终错误，则返回该错误并停止等待。
func WaitForPodNotFoundInNamespace(ctx context.Context, c clientset.Interface, podName, ns string, timeout time.Duration) error {
	err := framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*v1.Pod, error) {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return pod, err
	})).WithTimeout(timeout).Should(gomega.BeNil())
	if err != nil {
		return fmt.Errorf("expected pod to not be found: %w", err)
	}
	return nil
}

// WaitForPodsResponding 等待 Pod 响应。
func WaitForPodsResponding(ctx context.Context, c clientset.Interface, ns string, controllerName string, selector labels.Selector, wantName bool, timeout time.Duration, pods *v1.PodList) error {
	if timeout == 0 {
		timeout = podRespondingTimeout
	}
	ginkgo.By("trying to dial each unique pod")
	options := metav1.ListOptions{LabelSelector: selector.String()}

	type response struct {
		podName  string
		response string
	}

	get := func(ctx context.Context) ([]response, error) {
		currentPods, err := c.CoreV1().Pods(ns).List(ctx, options)
		if err != nil {
			return nil, fmt.Errorf("list pods: %w", err)
		}

		var responses []response
		for _, pod := range pods.Items {
			// 检查副本列表保持不变，否则就有问题了。
			if !isElementOf(pod.UID, currentPods) {
				return nil, gomega.StopTrying(fmt.Sprintf("Pod with UID %s is no longer a member of the replica set. Must have been restarted for some reason.\nCurrent replica set:\n%s", pod.UID, format.Object(currentPods, 1)))
			}

			ctxUntil, cancel := context.WithTimeout(ctx, singleCallTimeout)
			defer cancel()

			body, err := c.CoreV1().RESTClient().Get().
				Namespace(ns).
				Resource("pods").
				SubResource("proxy").
				Name(string(pod.Name)).
				Do(ctxUntil).
				Raw()

			if err != nil {
				// 这里可能遇到错误，因为 Pod 就绪与 apiserver proxy 之间存在竞争，
				// 或者因为临时故障。错误会被包装以供 framework.HandleRetry 处理。
				// Gomega+Ginkgo 会负责记录日志。
				return nil, fmt.Errorf("controller %s: failed to Get from replica pod %s:\n%w\nPod status:\n%s",
					controllerName, pod.Name,
					err, format.Object(pod.Status, 1))
			}
			responses = append(responses, response{podName: pod.Name, response: string(body)})
		}
		return responses, nil
	}

	match := func(responses []response) (func() string, error) {
		// 响应检查器期望 Pod 的名称，除非 !respondName，
		// 此时只检查响应是否非空。
		var unexpected []response
		for _, response := range responses {
			if wantName {
				if response.response != response.podName {
					unexpected = append(unexpected, response)
				}
			} else {
				if len(response.response) == 0 {
					unexpected = append(unexpected, response)
				}
			}
		}
		if len(unexpected) > 0 {
			return func() string {
				what := "some response"
				if wantName {
					what = "the pod's own name as response"
				}
				return fmt.Sprintf("Wanted %s, but the following pods replied with something else:\n%s", what, format.Object(unexpected, 1))
			}, nil
		}
		return nil, nil
	}

	err := framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(match))
	if err != nil {
		return fmt.Errorf("checking pod responses: %w", err)
	}
	return nil
}

func isElementOf(podUID apitypes.UID, pods *v1.PodList) bool {
	for _, pod := range pods.Items {
		if pod.UID == podUID {
			return true
		}
	}
	return false
}

// WaitForNumberOfPods 最多等待 timeout，以确保命名空间 `ns` 中
// 恰好有 `num` 个 Pod。
// 返回匹配的 Pod 或超时错误。
func WaitForNumberOfPods(ctx context.Context, c clientset.Interface, ns string, num int, timeout time.Duration) (pods *v1.PodList, err error) {
	return WaitForPods(ctx, c, ns, metav1.ListOptions{}, Range{MinMatching: num, MaxMatching: num}, podScheduledBeforeTimeout, "exist", func(pod *v1.Pod) bool {
		return true
	})
}

// WaitForPodsWithLabelScheduled 等待所有匹配的 Pod 都被调度，且至少存在一个
// 匹配的 Pod。返回匹配 Pod 的列表。
func WaitForPodsWithLabelScheduled(ctx context.Context, c clientset.Interface, ns string, label labels.Selector) (pods *v1.PodList, err error) {
	opts := metav1.ListOptions{LabelSelector: label.String()}
	return WaitForPods(ctx, c, ns, opts, Range{MinFound: 1, AllMatching: true}, podScheduledBeforeTimeout, "be scheduled", func(pod *v1.Pod) bool {
		return pod.Spec.NodeName != ""
	})
}

// WaitForPodsWithLabel 最多等待 podListTimeout，以获取带特定标签的 Pod
func WaitForPodsWithLabel(ctx context.Context, c clientset.Interface, ns string, label labels.Selector) (*v1.PodList, error) {
	opts := metav1.ListOptions{LabelSelector: label.String()}
	return WaitForPods(ctx, c, ns, opts, Range{MinFound: 1}, podListTimeout, "exist", func(pod *v1.Pod) bool {
		return true
	})
}

// WaitForPodsWithLabelRunningReady 等待恰好指定数量的匹配 Pod 进入运行态并就绪。
// 返回匹配 Pod 的列表。
func WaitForPodsWithLabelRunningReady(ctx context.Context, c clientset.Interface, ns string, label labels.Selector, num int, timeout time.Duration) (pods *v1.PodList, err error) {
	opts := metav1.ListOptions{LabelSelector: label.String()}
	return WaitForPods(ctx, c, ns, opts, Range{MinFound: num, AllMatching: true}, timeout, "be running and ready", RunningReady)
}

// WaitForNRestartablePods 尝试用 ps 列出正在重启的 Pod，直到找到预期数量，
// 若在超时前找到则返回它们的名称。
func WaitForNRestartablePods(ctx context.Context, ps *testutils.PodStore, expect int, timeout time.Duration) ([]string, error) {
	var pods []*v1.Pod

	get := func(ctx context.Context) ([]*v1.Pod, error) {
		return ps.List(), nil
	}

	match := func(allPods []*v1.Pod) (func() string, error) {
		pods = FilterNonRestartablePods(allPods)
		if len(pods) != expect {
			return func() string {
				return fmt.Sprintf("expected to find non-restartable %d pods, but found %d:\n%s", expect, len(pods), format.Object(pods, 1))
			}, nil
		}
		return nil, nil
	}

	err := framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(match))
	if err != nil {
		return nil, err
	}

	podNames := make([]string, len(pods))
	for i, p := range pods {
		podNames[i] = p.Name
	}
	return podNames, nil
}

// WaitForPodContainerToFail 等待给定 Pod 的容器因指定原因失败，特别是由于
// 无效的容器配置。此时容器将保持 waiting 状态，并带有特定的
// reason，应与给定的原因匹配。
func WaitForPodContainerToFail(ctx context.Context, c clientset.Interface, namespace, podName string, containerIndex int, reason string, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("container %d failed with reason %s", containerIndex, reason)
	return WaitForPodCondition(ctx, c, namespace, podName, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodPending:
			if len(pod.Status.ContainerStatuses) == 0 {
				return false, nil
			}
			containerStatus := pod.Status.ContainerStatuses[containerIndex]
			if containerStatus.State.Waiting != nil && containerStatus.State.Waiting.Reason == reason {
				return true, nil
			}
			return false, nil
		case v1.PodFailed, v1.PodRunning, v1.PodSucceeded:
			return false, fmt.Errorf("pod was expected to be pending, but it is in the state: %s", pod.Status.Phase)
		}
		return false, nil
	})
}

// WaitForPodScheduled 等待 Pod 被调度，即设置了 .spec.nodeName
func WaitForPodScheduled(ctx context.Context, c clientset.Interface, namespace, podName string) error {
	return WaitForPodCondition(ctx, c, namespace, podName, "pod is scheduled", podScheduledBeforeTimeout, func(pod *v1.Pod) (bool, error) {
		return pod.Spec.NodeName != "", nil
	})
}

// WaitForPodContainerStarted 等待给定 Pod 的容器在 startupProbe 成功后启动。
func WaitForPodContainerStarted(ctx context.Context, c clientset.Interface, namespace, podName string, containerIndex int, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("container %d started", containerIndex)
	return WaitForPodCondition(ctx, c, namespace, podName, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		if containerIndex >= len(pod.Status.ContainerStatuses) {
			return false, nil
		}
		containerStatus := pod.Status.ContainerStatuses[containerIndex]
		return *containerStatus.Started, nil
	})
}

// WaitForPodInitContainerStarted 等待给定 Pod 的 init container 启动。
func WaitForPodInitContainerStarted(ctx context.Context, c clientset.Interface, namespace, podName string, initContainerIndex int, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("init container %d started", initContainerIndex)
	return WaitForPodCondition(ctx, c, namespace, podName, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		if initContainerIndex >= len(pod.Status.InitContainerStatuses) {
			return false, nil
		}
		initContainerStatus := pod.Status.InitContainerStatuses[initContainerIndex]
		return *initContainerStatus.Started, nil
	})
}

// WaitForPodFailedReason 等待 status 中出现 Pod 失败原因，例如 "SysctlForbidden"。
func WaitForPodFailedReason(ctx context.Context, c clientset.Interface, pod *v1.Pod, reason string, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("failed with reason %s", reason)
	return WaitForPodCondition(ctx, c, pod.Namespace, pod.Name, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodSucceeded:
			return true, errors.New("pod succeeded unexpectedly")
		case v1.PodFailed:
			if pod.Status.Reason == reason {
				return true, nil
			} else {
				return true, fmt.Errorf("pod failed with reason %s", pod.Status.Reason)
			}
		}
		return false, nil
	})
}

// WaitForContainerRunning 等待给定 Pod 的容器进入 running 状态
func WaitForContainerRunning(ctx context.Context, c clientset.Interface, namespace, podName, containerName string, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("container %s running", containerName)
	return WaitForPodCondition(ctx, c, namespace, podName, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		for _, statuses := range [][]v1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses, pod.Status.EphemeralContainerStatuses} {
			for _, cs := range statuses {
				if cs.Name == containerName {
					return cs.State.Running != nil, nil
				}
			}
		}
		return false, nil
	})
}

// WaitForContainerTerminated 等待给定 Pod 的容器进入 terminated 状态
func WaitForContainerTerminated(ctx context.Context, c clientset.Interface, namespace, podName, containerName string, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("container %s terminated", containerName)
	return WaitForPodCondition(ctx, c, namespace, podName, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		for _, statuses := range [][]v1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses, pod.Status.EphemeralContainerStatuses} {
			for _, cs := range statuses {
				if cs.Name == containerName {
					return cs.State.Terminated != nil, nil
				}
			}
		}
		return false, nil
	})
}
