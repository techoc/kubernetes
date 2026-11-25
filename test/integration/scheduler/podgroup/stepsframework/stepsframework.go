/*
Copyright The Kubernetes Authors.

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

package podgroup

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/backend/queue"
	testutils "k8s.io/kubernetes/test/integration/util"
)

type WaitForAnyPodsScheduled struct {
	Pods             []*v1.Pod
	NumScheduled     int
	NumUnschedulable int
}

type PodGroupConditionCheck struct {
	PodGroupName    string
	ConditionStatus metav1.ConditionStatus
	Reason          string
}

type VerifyAssignments struct {
	Pods  []string
	Nodes sets.Set[string]
}

type VerifyAssignedInOneDomain struct {
	Pods        []string
	TopologyKey string
}

type VerifyPodsSchedulingAttempts struct {
	PodNames     []string
	PodGroupName string
	Attempts     int
}

type UpdatePod struct {
	PodName  string
	ModifyFn func(*v1.Pod)
}

// Step 让测试更易读：将测试写成一串步骤，每步是对集群的一次操作。
// 每个 Step 都应有 Name 用于标识，框架只执行该步骤中遇到的第一个操作。
//
// 例如，下面只会创建 PodGroup，而不会创建 Workload：
//
//	Step {
//		Name: "Create and wait for workload to be ready",
//		CreateWorkloads: []*schedulingapi.Workload{
//			st.MakeWorkload().Name("workload").
//				PodGroupTemplate(st.MakePodGroupTemplate().Name("t1").MinCount(3).Obj()).
//				Obj(),
//		},
//		CreatePodGroup: gangPodGroup,
//	}
//
// 定义好所有步骤后，可用 RunSteps 遍历执行这些步骤来运行测试。
//
// 例如：
//
// ns := testCtx.NS.Name
//
//	steps := []Step{
//			{
//				Name:        "Create Nodes",
//				CreateNodes: []*v1.Node{node},
//			},
//			{
//				Name:            "Create workloads",
//				CreateWorkloads: []*schedulingapi.Workload{workload, otherWorkload},
//			},
//			{
//				Name:           "Create the PodGroup object",
//				CreatePodGroup: gangPodGroup,
//			},
//			{
//				Name:                   "Verify PodGroup created",
//				WaitForPodGroupCreated: "pg1",
//			},
//			{
//				Name:       "Create all pods belonging to the gang",
//				CreatePods: []*v1.Pod{p1, p2, p3},
//			},
//			{
//				Name:                 "Verify all gang pods are scheduled successfully",
//				WaitForPodsScheduled: []string{"p1", "p2", "p3"},
//			},
//			{
//				Name: "Verify PodGroup condition is set to Scheduled",
//				WaitForPodGroupCondition: &stepsframework.PodGroupConditionCheck{
//					PodGroupName:    "pg1",
//					ConditionStatus: metav1.ConditionTrue,
//					Reason:          "Scheduled",
//				},
//			},
//		}
//
// runSteps(tCtx, ns, steps)
type Step struct {
	// 步骤名，用于标识步骤，每个 Step 都应设置。
	// 用于在测试输出中描述该步骤。
	Name string
	// 在集群中创建节点。
	CreateNodes []*v1.Node
	// 创建 PodGroup 并等待其就绪。
	CreatePodGroup *schedulingapi.PodGroup
	// 更新已有 PodGroup 并等待变更传播完成。
	UpdatePodGroup *schedulingapi.PodGroup
	// 按名称删除 PodGroup 并等待删除生效。
	DeletePodGroup string
	// 创建 CompositePodGroup 并等待其就绪。
	CreateCompositePodGroup *schedulingv1alpha3.CompositePodGroup
	// 更新已有 CompositePodGroup 并等待变更传播完成。
	UpdateCompositePodGroup *schedulingv1alpha3.CompositePodGroup
	// 在集群中创建 Pod。
	CreatePods []*v1.Pod
	// 创建 Pod 并确保它们按指定顺序进入调度队列。
	CreatePodsInOrder []*v1.Pod
	// 在集群中创建 Workload。
	CreateWorkloads []*schedulingapi.Workload
	// 从集群中删除 Pod。
	DeletePods []string
	// 修改 Pod 的任意字段。
	UpdatePod *UpdatePod
	// 等待 Pod 出现在 ActiveQ 中。
	WaitForPodsInActiveQ []string
	// 等待 Pod 进入 unschedulableEntities。
	WaitForPodsInUnschedulableEntities []string
	// 等待 Pod 进入 incompletePodGroupPods。
	WaitForPodsInIncompletePodGroupPods []string
	// 等待 Pod 变为 unschedulable。
	WaitForPodsUnschedulable []string
	// 等待 Pod 出现调度错误状态。
	WaitForPodsSchedulingError []string
	// 等待 Pod 完成调度。
	WaitForPodsScheduled []string
	// 等待 Pod 被移除。
	WaitForPodsRemoved []string
	// 等待 PodGroup 中任意 Pod 完成调度。
	WaitForAnyPodsScheduled *WaitForAnyPodsScheduled
	// 等待 PodGroup 出现指定 condition。
	WaitForPodGroupCondition *PodGroupConditionCheck
	// 验证 Pod 被分配到正确的节点。
	VerifyAssignments *VerifyAssignments
	// 验证 Pod 被分配到同一拓扑域内的节点。
	VerifyAssignedInOneDomain *VerifyAssignedInOneDomain
	// 检查 Pod 的调度尝试次数。
	VerifyPodSchedulingAttempts *VerifyPodsSchedulingAttempts
	// 手动执行一次调度。
	// 仅在测试未启动调度 goroutine（testCtx.Scheduler.Run）时使用。
	RunScheduleOne bool
}

func podInUnschedulableEntities(queue queue.SchedulingQueue, podName string) bool {
	unschedPods := queue.UnschedulablePods()
	for _, pod := range unschedPods {
		if pod.Name == podName {
			return true
		}
	}
	return false
}

func podSchedulingAttempted(cs kubernetes.Interface, ns, name string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
		return cond != nil, nil
	}
}

func podInIncompletePodGroupPods(queue queue.SchedulingQueue, podName string) bool {
	incompletePods := queue.IncompletePodGroupPodsPods()
	for _, pod := range incompletePods {
		if pod.Name == podName {
			return true
		}
	}
	return false
}

func podGroupHasScheduledCondition(cs kubernetes.Interface, ns, name string, status metav1.ConditionStatus, reason string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pg, err := cs.SchedulingV1beta1().PodGroups(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for _, c := range pg.Status.Conditions {
			if c.Type == schedulingapi.PodGroupInitiallyScheduled &&
				c.Status == status && c.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	}
}

func createNodes(testCtx *testutils.TestContext, nodes []*v1.Node) error {
	cs := testCtx.ClientSet
	for _, node := range nodes {
		n := node.DeepCopy()
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, n, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create node %s: %w", n.Name, err)
		}
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			func(_ context.Context) (bool, error) {
				_, err := testCtx.Scheduler.Cache.GetNode(n.Name)
				return err == nil, nil
			},
		)
		if err != nil {
			return fmt.Errorf("failed to wait for node %s to be in the scheduler cache: %w", n.Name, err)
		}
	}
	return nil
}

func createPods(testCtx *testutils.TestContext, ns string, pods []*v1.Pod, preserveOrder bool) error {
	cs := testCtx.ClientSet
	for _, pod := range pods {
		p := pod.DeepCopy()
		p.Namespace = ns
		if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, p, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create pod %s: %w", p.Name, err)
		}
		if preserveOrder {
			podSchedulingAttemptedFn := podSchedulingAttempted(cs, ns, p.Name)
			err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false, func(ctx context.Context) (bool, error) {
				_, ok := testCtx.Scheduler.SchedulingQueue.GetPod(p.Name, p.Namespace, p.Spec.SchedulingGroup)
				if ok {
					return true, nil
				}
				// 两次轮询之间 Pod 可能已入队并完成一次调度尝试。
				// PodScheduled condition（无论取值）都能证明调度器已观察到该 Pod。
				return podSchedulingAttemptedFn(ctx)
			})
			if err != nil {
				return fmt.Errorf("failed to ensure queueing order of pod %s: %w", p.Name, err)
			}
		}
	}
	return nil
}

func createCompositePodGroup(testCtx *testutils.TestContext, ns string, cpg *schedulingv1alpha3.CompositePodGroup) error {
	cs := testCtx.ClientSet
	cpgCopy := cpg.DeepCopy()
	cpgCopy.Namespace = ns
	if _, err := cs.SchedulingV1alpha3().CompositePodGroups(ns).Create(testCtx.Ctx, cpgCopy, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create composite pod group %s: %w", cpgCopy.Name, err)
	}
	err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(_ context.Context) (bool, error) {
			_, err := testCtx.InformerFactory.Scheduling().V1alpha3().CompositePodGroups().Lister().CompositePodGroups(ns).Get(cpgCopy.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for composite pod group %s to be discoverable by scheduler: %w", cpgCopy.Name, err)
	}
	return nil
}

func updateCompositePodGroup(testCtx *testutils.TestContext, ns string, cpg *schedulingv1alpha3.CompositePodGroup) error {
	cs := testCtx.ClientSet
	cpgCopy := cpg.DeepCopy()
	cpgCopy.Namespace = ns

	existing, err := cs.SchedulingV1alpha3().CompositePodGroups(ns).Get(testCtx.Ctx, cpgCopy.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get existing composite pod group %s for update: %w", cpgCopy.Name, err)
	}
	cpgCopy.ResourceVersion = existing.ResourceVersion

	if _, err := cs.SchedulingV1alpha3().CompositePodGroups(ns).Update(testCtx.Ctx, cpgCopy, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update composite pod group %s: %w", cpgCopy.Name, err)
	}
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(_ context.Context) (bool, error) {
			listerCPG, err := testCtx.InformerFactory.Scheduling().V1alpha3().CompositePodGroups().Lister().CompositePodGroups(ns).Get(cpgCopy.Name)
			if err != nil {
				return false, err
			}
			if apiequality.Semantic.DeepEqual(listerCPG.Spec.SchedulingPolicy, cpgCopy.Spec.SchedulingPolicy) {
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for composite pod group %s update to be discoverable by scheduler: %w", cpgCopy.Name, err)
	}
	return nil
}

func createPodGroup(testCtx *testutils.TestContext, ns string, pg *schedulingapi.PodGroup) error {
	cs := testCtx.ClientSet
	pgCopy := pg.DeepCopy()
	pgCopy.Namespace = ns
	if _, err := cs.SchedulingV1beta1().PodGroups(ns).Create(testCtx.Ctx, pgCopy, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create pod group %s: %w", pgCopy.Name, err)
	}
	err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(_ context.Context) (bool, error) {
			_, err := testCtx.InformerFactory.Scheduling().V1beta1().PodGroups().Lister().PodGroups(ns).Get(pgCopy.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for pod group %s to be discoverable by scheduler: %w", pgCopy.Name, err)
	}
	return nil
}

func updatePodGroup(testCtx *testutils.TestContext, ns string, pg *schedulingapi.PodGroup) error {
	cs := testCtx.ClientSet
	pgCopy := pg.DeepCopy()
	pgCopy.Namespace = ns

	existing, err := cs.SchedulingV1beta1().PodGroups(ns).Get(testCtx.Ctx, pgCopy.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get existing pod group %s for update: %w", pgCopy.Name, err)
	}
	pgCopy.ResourceVersion = existing.ResourceVersion

	if _, err := cs.SchedulingV1beta1().PodGroups(ns).Update(testCtx.Ctx, pgCopy, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update pod group %s: %w", pgCopy.Name, err)
	}
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(_ context.Context) (bool, error) {
			listerPG, err := testCtx.InformerFactory.Scheduling().V1beta1().PodGroups().Lister().PodGroups(ns).Get(pgCopy.Name)
			if err != nil {
				return false, err
			}
			if apiequality.Semantic.DeepEqual(listerPG.Spec.SchedulingPolicy, pgCopy.Spec.SchedulingPolicy) {
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for pod group %s update to be discoverable by scheduler: %w", pgCopy.Name, err)
	}
	return nil
}

func deletePodGroup(testCtx *testutils.TestContext, ns string, pgName string) error {
	cs := testCtx.ClientSet

	pg, err := cs.SchedulingV1beta1().PodGroups(ns).Get(testCtx.Ctx, pgName, metav1.GetOptions{})
	if err == nil && len(pg.Finalizers) > 0 {
		pg.Finalizers = nil
		if _, err = cs.SchedulingV1beta1().PodGroups(ns).Update(testCtx.Ctx, pg, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to clear finalizers of pod group %s: %w", pgName, err)
		}
	}
	if err := cs.SchedulingV1beta1().PodGroups(ns).Delete(testCtx.Ctx, pgName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("failed to delete pod group %s: %w", pgName, err)
	}
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(_ context.Context) (bool, error) {
			_, err := testCtx.InformerFactory.Scheduling().V1beta1().PodGroups().Lister().PodGroups(ns).Get(pgName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for pod group %s deletion to propagate: %w", pgName, err)
	}
	return nil
}

func createWorkloads(testCtx *testutils.TestContext, ns string, wls []*schedulingapi.Workload) error {
	cs := testCtx.ClientSet
	for _, wl := range wls {
		wlCopy := wl.DeepCopy()
		wlCopy.Namespace = ns
		if _, err := cs.SchedulingV1beta1().Workloads(ns).Create(testCtx.Ctx, wlCopy, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create workload %s: %w", wlCopy.Name, err)
		}
	}
	return nil
}

func deletePods(testCtx *testutils.TestContext, ns string, podNames []string) error {
	cs := testCtx.ClientSet
	for _, podName := range podNames {
		if err := cs.CoreV1().Pods(ns).Delete(testCtx.Ctx, podName, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("failed to delete pod %s: %w", podName, err)
		}
	}
	return nil
}

func updatePod(testCtx *testutils.TestContext, ns string, update *UpdatePod) error {
	cs := testCtx.ClientSet
	p, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, update.PodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get pod %s for update: %w", update.PodName, err)
	}
	update.ModifyFn(p)
	_, err = cs.CoreV1().Pods(ns).Update(testCtx.Ctx, p, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update pod %s: %w", update.PodName, err)
	}
	return nil
}

func waitForPodsInActiveQ(testCtx *testutils.TestContext, podNames []string) error {
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false, func(ctx context.Context) (bool, error) {
			for _, p := range testCtx.Scheduler.SchedulingQueue.PodsInActiveQ() {
				if p.Name == podName {
					return true, nil
				}
			}
			return false, nil
		})
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be in active queue: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsInUnschedulableEntities(testCtx *testutils.TestContext, ns string, podNames []string) error {
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			func(_ context.Context) (bool, error) {
				return podInUnschedulableEntities(testCtx.Scheduler.SchedulingQueue, podName), nil
			},
		)
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be in unschedulable entities: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsInIncompletePodGroupPods(testCtx *testutils.TestContext, ns string, podNames []string) error {
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			func(_ context.Context) (bool, error) {
				return podInIncompletePodGroupPods(testCtx.Scheduler.SchedulingQueue, podName), nil
			},
		)
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be in incompletePodGroupPods: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsUnschedulable(testCtx *testutils.TestContext, ns string, podNames []string) error {
	cs := testCtx.ClientSet
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			testutils.PodUnschedulable(cs, ns, podName))
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be unschedulable: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsSchedulingError(testCtx *testutils.TestContext, ns string, podNames []string) error {
	cs := testCtx.ClientSet
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			testutils.PodSchedulingError(cs, ns, podName))
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to get scheduling error: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsScheduled(testCtx *testutils.TestContext, ns string, podNames []string) error {
	cs := testCtx.ClientSet
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			testutils.PodScheduled(cs, ns, podName))
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be scheduled: %w", podName, err)
		}
	}
	return nil
}

func waitForPodsRemoved(testCtx *testutils.TestContext, ns string, podNames []string) error {
	cs := testCtx.ClientSet
	for _, podName := range podNames {
		err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
			testutils.PodDeleted(testCtx.Ctx, cs, ns, podName))
		if err != nil {
			return fmt.Errorf("failed to wait for pod %s to be removed: %w", podName, err)
		}
	}
	return nil
}

func waitForAnyPodsScheduled(testCtx *testutils.TestContext, ns string, waitAny *WaitForAnyPodsScheduled) error {
	cs := testCtx.ClientSet
	err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		func(ctx context.Context) (bool, error) {
			scheduledCount := 0
			unschedulableCount := 0
			for _, pod := range waitAny.Pods {
				if ok, err := testutils.PodScheduled(cs, ns, pod.Name)(ctx); err != nil {
					return false, err
				} else if ok {
					scheduledCount++
					continue
				}
				if ok, err := testutils.PodUnschedulable(cs, ns, pod.Name)(ctx); err != nil {
					return false, err
				} else if ok {
					unschedulableCount++
				}
			}
			return scheduledCount == waitAny.NumScheduled && unschedulableCount == waitAny.NumUnschedulable, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to wait for pods to be scheduled or unschedulable: %w", err)
	}
	return nil
}

func waitForPodGroupCondition(testCtx *testutils.TestContext, ns string, check *PodGroupConditionCheck) error {
	cs := testCtx.ClientSet
	err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
		podGroupHasScheduledCondition(cs, ns, check.PodGroupName, check.ConditionStatus, check.Reason))
	if err != nil {
		return fmt.Errorf("failed to wait for PodGroup %s condition (status=%s, reason=%s): %w",
			check.PodGroupName, check.ConditionStatus, check.Reason, err)
	}
	return nil
}

func verifyAssignments(testCtx *testutils.TestContext, ns string, verify *VerifyAssignments) error {
	cs := testCtx.ClientSet
	for _, podName := range verify.Pods {
		assignedPod, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve assigned pod %s: %w", podName, err)
		}
		nodeName := assignedPod.Spec.NodeName
		if nodeName == "" {
			return fmt.Errorf("pod %s is not assigned", podName)
		}
		if !verify.Nodes.Has(nodeName) {
			return fmt.Errorf("wanted pod %s scheduled on node within %v but got assignment to %s", podName, verify.Nodes, nodeName)
		}
	}
	return nil
}

func verifyAssignedInOneDomain(testCtx *testutils.TestContext, ns string, verify *VerifyAssignedInOneDomain) error {
	cs := testCtx.ClientSet
	expectedDomain := ""
	for _, podName := range verify.Pods {
		assignedPod, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve assigned pod %s: %w", podName, err)
		}
		nodeName := assignedPod.Spec.NodeName
		if nodeName == "" {
			return fmt.Errorf("pod %s is not assigned", podName)
		}
		node, err := cs.CoreV1().Nodes().Get(testCtx.Ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve node %s: %w", nodeName, err)
		}
		domain := node.Labels[verify.TopologyKey]
		if domain == "" {
			return fmt.Errorf("invalid domain value \"\" in node %s", nodeName)
		}
		if expectedDomain == "" {
			expectedDomain = domain
		} else if expectedDomain != domain {
			return fmt.Errorf("pod %s assigned to a different domain. Expected %s but got %s", podName, expectedDomain, domain)
		}
	}
	return nil
}

func verifyPodSchedulingAttempts(testCtx *testutils.TestContext, ns string, check *VerifyPodsSchedulingAttempts) error {
	podGroupName := check.PodGroupName
	for _, podName := range check.PodNames {
		pInfo, ok := testCtx.Scheduler.SchedulingQueue.GetPod(podName, ns, &v1.PodSchedulingGroup{
			PodGroupName: &podGroupName,
		})
		if !ok {
			return fmt.Errorf("pod %s not found in scheduling queue", podName)
		}
		if pInfo.Attempts != check.Attempts {
			return fmt.Errorf("expected pod %s scheduling attempts to be %d, but got %d", podName, check.Attempts, pInfo.Attempts)
		}
	}
	return nil
}

func runScheduleOne(testCtx *testutils.TestContext) {
	testCtx.Scheduler.ScheduleOne(testCtx.Ctx)
}

// RunSteps 按顺序执行各步骤，每步只执行遇到的第一个操作。
// 步骤中没有操作会返回错误；任一步骤出错时立即停止并返回该错误。
func RunSteps(testCtx *testutils.TestContext, t *testing.T, ns string, steps []Step) error {
	for i, step := range steps {
		if step.Name == "" {
			return fmt.Errorf("step name cannot be empty")
		}
		t.Log("Executing step", "step", i, "name", step.Name)
		var err error
		switch {
		case step.CreateNodes != nil:
			err = createNodes(testCtx, step.CreateNodes)
		case step.CreatePods != nil:
			err = createPods(testCtx, ns, step.CreatePods, false)
		case step.CreatePodsInOrder != nil:
			err = createPods(testCtx, ns, step.CreatePodsInOrder, true)
		case step.CreateCompositePodGroup != nil:
			err = createCompositePodGroup(testCtx, ns, step.CreateCompositePodGroup)
		case step.UpdateCompositePodGroup != nil:
			err = updateCompositePodGroup(testCtx, ns, step.UpdateCompositePodGroup)
		case step.CreatePodGroup != nil:
			err = createPodGroup(testCtx, ns, step.CreatePodGroup)
		case step.UpdatePodGroup != nil:
			err = updatePodGroup(testCtx, ns, step.UpdatePodGroup)
		case step.DeletePodGroup != "":
			err = deletePodGroup(testCtx, ns, step.DeletePodGroup)
		case step.CreateWorkloads != nil:
			err = createWorkloads(testCtx, ns, step.CreateWorkloads)
		case step.DeletePods != nil:
			err = deletePods(testCtx, ns, step.DeletePods)
		case step.UpdatePod != nil:
			err = updatePod(testCtx, ns, step.UpdatePod)
		case step.WaitForPodsInActiveQ != nil:
			err = waitForPodsInActiveQ(testCtx, step.WaitForPodsInActiveQ)
		case step.WaitForPodsInUnschedulableEntities != nil:
			err = waitForPodsInUnschedulableEntities(testCtx, ns, step.WaitForPodsInUnschedulableEntities)
		case step.WaitForPodsInIncompletePodGroupPods != nil:
			err = waitForPodsInIncompletePodGroupPods(testCtx, ns, step.WaitForPodsInIncompletePodGroupPods)
		case step.WaitForPodsUnschedulable != nil:
			err = waitForPodsUnschedulable(testCtx, ns, step.WaitForPodsUnschedulable)
		case step.WaitForPodsSchedulingError != nil:
			err = waitForPodsSchedulingError(testCtx, ns, step.WaitForPodsSchedulingError)
		case step.WaitForPodsScheduled != nil:
			err = waitForPodsScheduled(testCtx, ns, step.WaitForPodsScheduled)
		case step.WaitForPodsRemoved != nil:
			err = waitForPodsRemoved(testCtx, ns, step.WaitForPodsRemoved)
		case step.WaitForAnyPodsScheduled != nil:
			err = waitForAnyPodsScheduled(testCtx, ns, step.WaitForAnyPodsScheduled)
		case step.WaitForPodGroupCondition != nil:
			err = waitForPodGroupCondition(testCtx, ns, step.WaitForPodGroupCondition)
		case step.VerifyAssignments != nil:
			err = verifyAssignments(testCtx, ns, step.VerifyAssignments)
		case step.VerifyAssignedInOneDomain != nil:
			err = verifyAssignedInOneDomain(testCtx, ns, step.VerifyAssignedInOneDomain)
		case step.VerifyPodSchedulingAttempts != nil:
			err = verifyPodSchedulingAttempts(testCtx, ns, step.VerifyPodSchedulingAttempts)
		case step.RunScheduleOne:
			runScheduleOne(testCtx)
		default:
			err = fmt.Errorf("no operation specified for step %d (%s)", i, step.Name)
		}
		if err != nil {
			return fmt.Errorf("step %d (%s) failed: %w", i, step.Name, err)
		}
	}
	return nil
}
