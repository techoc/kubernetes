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

package deployment

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/deployment"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/kubernetes/pkg/controller/replicaset"
	"k8s.io/kubernetes/test/integration/framework"
	testutil "k8s.io/kubernetes/test/utils"
	"k8s.io/utils/ptr"
)

const (
	pollInterval = 100 * time.Millisecond
	pollTimeout  = 60 * time.Second

	fakeContainerName = "fake-name"
	fakeImage         = "fakeimage"
)

var pauseFn = func(update *apps.Deployment) {
	update.Spec.Paused = true
}

var resumeFn = func(update *apps.Deployment) {
	update.Spec.Paused = false
}

type deploymentTester struct {
	t          *testing.T
	c          clientset.Interface
	deployment *apps.Deployment
}

func testLabels() map[string]string {
	return map[string]string{"name": "test"}
}

// newDeployment 返回一个使用 RollingUpdate 策略、镜像为假镜像的 Deployment
func newDeployment(name, ns string, replicas int32) *apps.Deployment {
	return &apps.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Spec: apps.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: testLabels()},
			Strategy: apps.DeploymentStrategy{
				Type:          apps.RollingUpdateDeploymentStrategyType,
				RollingUpdate: new(apps.RollingUpdateDeployment),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: testLabels(),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  fakeContainerName,
							Image: fakeImage,
						},
					},
				},
			},
		},
	}
}

// dcSetup 准备 Deployment 集成测试所需的各项设施，包括 control plane、apiserver、informer 和 clientset
func dcSetup(ctx context.Context, t *testing.T) (kubeapiservertesting.TearDownFunc, *replicaset.ReplicaSetController, *deployment.DeploymentController, informers.SharedInformerFactory, clientset.Interface) {
	// 禁用 ServiceAccount 准入插件，因为没有运行 serviceaccount 控制器。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())

	config := restclient.CopyConfig(server.ClientConfig)
	clientSet, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("error in create clientset: %v", err)
	}
	resyncPeriod := 12 * time.Hour
	informers := informers.NewSharedInformerFactory(clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "deployment-informers")), resyncPeriod)

	dc, err := deployment.NewDeploymentController(
		ctx,
		informers.Apps().V1().Deployments(),
		informers.Apps().V1().ReplicaSets(),
		informers.Core().V1().Pods(),
		clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "deployment-controller")),
	)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}
	rm := replicaset.NewReplicaSetController(
		ctx,
		informers.Apps().V1().ReplicaSets(),
		informers.Core().V1().Pods(),
		clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "replicaset-controller")),
		replicaset.BurstReplicas,
	)
	return server.TearDownFn, rm, dc, informers, clientSet
}

// dcSimpleSetup 准备 Deployment 集成测试所需的各项设施，包括 control plane、apiserver
// 和 clientset，但不包括控制器和 informer
func dcSimpleSetup(t *testing.T) (kubeapiservertesting.TearDownFunc, clientset.Interface) {
	// 禁用 ServiceAccount 准入插件，因为没有运行 serviceaccount 控制器。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())

	config := restclient.CopyConfig(server.ClientConfig)
	clientSet, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("error in create clientset: %v", err)
	}
	return server.TearDownFn, clientSet
}

// runControllersAndInformers 启动 RS 控制器、Deployment 控制器和 informer
func runControllersAndInformers(t *testing.T, rm *replicaset.ReplicaSetController, dc *deployment.DeploymentController, informers informers.SharedInformerFactory) func() {
	ctx, cancelFn := context.WithCancel(context.Background())
	informers.Start(ctx.Done())
	go rm.Run(ctx, 5)
	go dc.Run(ctx, 5)
	return cancelFn
}

// addPodConditionReady 将给定 Pod 的状态设置为在给定时间就绪
func addPodConditionReady(pod *v1.Pod, time metav1.Time) {
	pod.Status = v1.PodStatus{
		Phase: v1.PodRunning,
		Conditions: []v1.PodCondition{
			{
				Type:               v1.PodReady,
				Status:             v1.ConditionTrue,
				LastTransitionTime: time,
			},
		},
	}
}

func (d *deploymentTester) waitForDeploymentRevisionAndImage(revision, image string) error {
	if err := testutil.WaitForDeploymentRevisionAndImage(d.c, d.deployment.Namespace, d.deployment.Name, revision, image, d.t.Logf, pollInterval, pollTimeout); err != nil {
		return fmt.Errorf("failed to wait for Deployment revision %s: %v", d.deployment.Name, err)
	}
	return nil
}

func markPodReady(c clientset.Interface, ns string, pod *v1.Pod) error {
	addPodConditionReady(pod, metav1.Now())
	_, err := c.CoreV1().Pods(ns).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
	return err
}

// markUpdatedPodsReady 手动将更新后的 Deployment Pod 标记为就绪，
// 直到 Deployment 完成
func (d *deploymentTester) markUpdatedPodsReady(wg *sync.WaitGroup) {
	defer wg.Done()

	ns := d.deployment.Namespace
	err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		// Deployment 完成即结束
		if completed, err := d.deploymentComplete(); err != nil {
			return false, err
		} else if completed {
			return true, nil
		}
		// 否则，将剩余的 Pod 标记为就绪
		pods, err := d.listUpdatedPods()
		if err != nil {
			d.t.Log(err)
			return false, nil
		}
		d.t.Logf("%d/%d of deployment pods are created", len(pods), *d.deployment.Spec.Replicas)
		for i := range pods {
			pod := pods[i]
			if podutil.IsPodReady(&pod) {
				continue
			}
			if err = markPodReady(d.c, ns, &pod); err != nil {
				d.t.Logf("failed to update Deployment pod %s, will retry later: %v", pod.Name, err)
			}
		}
		return false, nil
	})
	if err != nil {
		d.t.Errorf("failed to mark updated Deployment pods to ready: %v", err)
	}
}

// removeTerminatedPods 手动删除已终止的 Deployment Pod，
// 直到 Deployment 完成
func (d *deploymentTester) removeTerminatedPods(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		// Deployment 完成即结束
		if completed, err := d.deploymentComplete(); err != nil {
			return false, err
		} else if completed {
			return true, nil
		}
		// 否则，将剩余的 Pod 标记为就绪
		rsList, err := deploymentutil.ListReplicaSets(d.deployment, deploymentutil.RsListFromClient(d.c.AppsV1()))
		if err != nil {
			d.t.Log(err)
			return false, nil
		}
		for _, rs := range rsList {
			if err := d.removeRSPods(ctx, rs, math.MaxInt, true, 0); err != nil {
				d.t.Log(err)
				return false, nil
			}
		}
		return false, nil
	})
	if err != nil {
		d.t.Errorf("failed to remove terminated Deployment pods: %v", err)
	}
}

func (d *deploymentTester) deploymentComplete() (bool, error) {
	latest, err := d.c.AppsV1().Deployments(d.deployment.Namespace).Get(context.TODO(), d.deployment.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return deploymentutil.DeploymentComplete(d.deployment, &latest.Status), nil
}

// 等待 Deployment 完成，并检查滚动更新策略在任何时刻都没有被破坏。
// 滚动更新期间不应违反滚动更新策略。
func (d *deploymentTester) waitForDeploymentCompleteAndCheckRolling() error {
	return testutil.WaitForDeploymentCompleteAndCheckRolling(d.c, d.deployment, d.t.Logf, pollInterval, pollTimeout)
}

// 等待 Deployment 完成，但不检查滚动更新策略是否被破坏。
// 滚动更新策略仅在滚动更新期间适用，其他情况下（例如扩缩容后不久或 Deployment 刚创建时）
// 可以被违反。
func (d *deploymentTester) waitForDeploymentComplete() error {
	return testutil.WaitForDeploymentComplete(d.c, d.deployment, d.t.Logf, pollInterval, pollTimeout)
}

// waitForDeploymentCompleteAndCheckRollingAndMarkPodsReady 等待 Deployment 完成，
// 同时将更新后的 Deployment Pod 标记为就绪。
// 使用严格检查确保滚动更新策略在任何时刻都不被违反。
func (d *deploymentTester) waitForDeploymentCompleteAndCheckRollingAndMarkPodsReady() error {
	var wg sync.WaitGroup

	// 在单独的 goroutine 中手动将更新后的 Deployment Pod 标记为就绪
	wg.Add(1)
	go d.markUpdatedPodsReady(&wg)
	// 在所有返回路径上都等待 goroutine 结束。
	defer wg.Wait()

	// 在 Deployment Pod 逐渐就绪的同时，等待 Deployment 状态完成
	err := d.waitForDeploymentCompleteAndCheckRolling()
	if err != nil {
		return fmt.Errorf("failed to wait for Deployment %s to complete: %v", d.deployment.Name, err)
	}

	return nil
}

// waitForDeploymentCompleteAndMarkPodsReady 等待 Deployment 完成，
// 同时将更新后的 Deployment Pod 标记为就绪。
func (d *deploymentTester) waitForDeploymentCompleteAndMarkPodsReady() error {
	var wg sync.WaitGroup

	// 在单独的 goroutine 中手动将更新后的 Deployment Pod 标记为就绪
	wg.Add(1)
	go d.markUpdatedPodsReady(&wg)

	// 在 Deployment Pod 逐渐就绪的同时，使用宽松检查等待 Deployment 状态完成
	err := d.waitForDeploymentComplete()
	if err != nil {
		return fmt.Errorf("failed to wait for Deployment status %s: %v", d.deployment.Name, err)
	}

	// 等待 goroutine 结束
	wg.Wait()

	return nil
}

// waitForDeploymentCompleteAndMarkPodsReadyAndRemoveTerminating 等待 Deployment 完成，
// 同时将更新后的 Deployment Pod 标记为就绪，并删除已终止的 Pod。
func (d *deploymentTester) waitForDeploymentCompleteAndMarkPodsReadyAndRemoveTerminated(ctx context.Context) error {
	var wg sync.WaitGroup

	// 在单独的 goroutine 中手动将更新后的 Deployment Pod 标记为就绪
	wg.Add(1)
	go d.markUpdatedPodsReady(&wg)

	wg.Add(1)
	go d.removeTerminatedPods(ctx, &wg)

	// 在 Deployment Pod 逐渐就绪的同时，使用宽松检查等待 Deployment 状态完成
	err := d.waitForDeploymentComplete()
	if err != nil {
		return fmt.Errorf("failed to wait for Deployment status %s: %w", d.deployment.Name, err)
	}

	// 等待 goroutine 结束
	wg.Wait()

	return nil
}

func (d *deploymentTester) updateDeployment(applyUpdate testutil.UpdateDeploymentFunc) (*apps.Deployment, error) {
	return testutil.UpdateDeploymentWithRetries(d.c, d.deployment.Namespace, d.deployment.Name, applyUpdate, d.t.Logf, pollInterval, pollTimeout)
}

func (d *deploymentTester) waitForObservedDeployment(desiredGeneration int64) error {
	if err := testutil.WaitForObservedDeployment(d.c, d.deployment.Namespace, d.deployment.Name, desiredGeneration); err != nil {
		return fmt.Errorf("failed waiting for ObservedGeneration of deployment %s to become %d: %v", d.deployment.Name, desiredGeneration, err)
	}
	return nil
}

func (d *deploymentTester) getNewReplicaSet() (*apps.ReplicaSet, error) {
	deployment, err := d.c.AppsV1().Deployments(d.deployment.Namespace).Get(context.TODO(), d.deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed retrieving deployment %s: %v", d.deployment.Name, err)
	}
	rs, err := testutil.GetNewReplicaSet(deployment, d.c)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving new replicaset of deployment %s: %v", d.deployment.Name, err)
	}
	return rs, nil
}

func (d *deploymentTester) expectNoNewReplicaSet() error {
	rs, err := d.getNewReplicaSet()
	if err != nil {
		return err
	}
	if rs != nil {
		return fmt.Errorf("expected deployment %s not to create a new replicaset, got %v", d.deployment.Name, rs)
	}
	return nil
}

func (d *deploymentTester) expectNewReplicaSet() (*apps.ReplicaSet, error) {
	rs, err := d.getNewReplicaSet()
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, fmt.Errorf("expected deployment %s to create a new replicaset, got nil", d.deployment.Name)
	}
	return rs, nil
}

func (d *deploymentTester) updateReplicaSet(name string, applyUpdate testutil.UpdateReplicaSetFunc) (*apps.ReplicaSet, error) {
	return testutil.UpdateReplicaSetWithRetries(d.c, d.deployment.Namespace, name, applyUpdate, d.t.Logf, pollInterval, pollTimeout)
}

func (d *deploymentTester) waitForDeploymentUpdatedReplicasGTE(minUpdatedReplicas int32) error {
	return testutil.WaitForDeploymentUpdatedReplicasGTE(d.c, d.deployment.Namespace, d.deployment.Name, minUpdatedReplicas, d.deployment.Generation, pollInterval, pollTimeout)
}

func (d *deploymentTester) waitForDeploymentWithCondition(reason string, condType apps.DeploymentConditionType) error {
	return testutil.WaitForDeploymentWithCondition(d.c, d.deployment.Namespace, d.deployment.Name, reason, condType, d.t.Logf, pollInterval, pollTimeout)
}

func (d *deploymentTester) listUpdatedPods() ([]v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(d.deployment.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse deployment selector: %v", err)
	}
	pods, err := d.c.CoreV1().Pods(d.deployment.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("failed to list deployment pods, will retry later: %v", err)
	}
	newRS, err := d.getNewReplicaSet()
	if err != nil {
		return nil, fmt.Errorf("failed to get new replicaset of deployment %q: %v", d.deployment.Name, err)
	}
	if newRS == nil {
		return nil, fmt.Errorf("unable to find new replicaset of deployment %q", d.deployment.Name)
	}

	var ownedPods []v1.Pod
	for _, pod := range pods.Items {
		rs := metav1.GetControllerOf(&pod)
		if rs.UID == newRS.UID {
			ownedPods = append(ownedPods, pod)
		}
	}
	return ownedPods, nil
}

func (d *deploymentTester) waitRSStable(replicaset *apps.ReplicaSet) error {
	return testutil.WaitRSStable(d.t, d.c, replicaset, pollInterval, pollTimeout)
}

func (d *deploymentTester) scaleDeployment(newReplicas int32) error {
	var err error
	d.deployment, err = d.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = &newReplicas
	})
	if err != nil {
		return fmt.Errorf("failed updating deployment %q: %v", d.deployment.Name, err)
	}

	if err := d.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		return err
	}

	rs, err := d.expectNewReplicaSet()
	if err != nil {
		return err
	}
	if *rs.Spec.Replicas != newReplicas {
		return fmt.Errorf("expected new replicaset replicas = %d, got %d", newReplicas, *rs.Spec.Replicas)
	}
	return nil
}

// waitForReadyReplicas 等待就绪副本数等于期望副本数。
func (d *deploymentTester) waitForReadyReplicas() error {
	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		deployment, err := d.c.AppsV1().Deployments(d.deployment.Namespace).Get(context.TODO(), d.deployment.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get deployment %q: %v", d.deployment.Name, err)
		}
		return deployment.Status.ReadyReplicas == *deployment.Spec.Replicas, nil
	}); err != nil {
		return fmt.Errorf("failed to wait for .readyReplicas to equal .replicas: %v", err)
	}
	return nil
}

// markUpdatedPodsReadyWithoutComplete 将更新后的 Deployment Pod 标记为就绪，但不等待 Deployment 完成。
func (d *deploymentTester) markUpdatedPodsReadyWithoutComplete() error {
	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		pods, err := d.listUpdatedPods()
		if err != nil {
			return false, err
		}
		for i := range pods {
			pod := pods[i]
			if podutil.IsPodReady(&pod) {
				continue
			}
			if err = markPodReady(d.c, d.deployment.Namespace, &pod); err != nil {
				d.t.Logf("failed to update Deployment pod %q, will retry later: %v", pod.Name, err)
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("failed to mark all updated pods as ready: %v", err)
	}
	return nil
}

// waitForReadyReplicas 等待就绪副本数等于期望副本数。
func (d *deploymentTester) waitForDeploymentStatusReplicasFields(ctx context.Context, replicas, updatedReplicas, readyReplicas, availableReplicas, unavailableReplicas int32, terminatingReplicas *int32) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(_ context.Context) (bool, error) {
		// 不将 ctx 传给 checkDeploymentStatusReplicasFields（Deployments().Get），以便始终获得最新的错误，而非超时错误
		lastErr = d.checkDeploymentStatusReplicasFields(replicas, updatedReplicas, readyReplicas, availableReplicas, unavailableReplicas, terminatingReplicas)
		return lastErr == nil, nil
	})
	if err != nil {
		return fmt.Errorf("%w: %w", lastErr, err)
	}
	return nil
}

// 校验 DeploymentStatus 中所有副本数相关字段均为期望值。
// 一旦发现字段不匹配，立即返回错误。
func (d *deploymentTester) checkDeploymentStatusReplicasFields(replicas, updatedReplicas, readyReplicas, availableReplicas, unavailableReplicas int32, terminatingReplicas *int32) error {
	deployment, err := d.c.AppsV1().Deployments(d.deployment.Namespace).Get(context.TODO(), d.deployment.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment %q: %v", d.deployment.Name, err)
	}
	if deployment.Status.Replicas != replicas {
		return fmt.Errorf("unexpected .replicas: expect %d, got %d", replicas, deployment.Status.Replicas)
	}
	if deployment.Status.UpdatedReplicas != updatedReplicas {
		return fmt.Errorf("unexpected .updatedReplicas: expect %d, got %d", updatedReplicas, deployment.Status.UpdatedReplicas)
	}
	if deployment.Status.ReadyReplicas != readyReplicas {
		return fmt.Errorf("unexpected .readyReplicas: expect %d, got %d", readyReplicas, deployment.Status.ReadyReplicas)
	}
	if deployment.Status.AvailableReplicas != availableReplicas {
		return fmt.Errorf("unexpected .availableReplicas: expect %d, got %d", availableReplicas, deployment.Status.AvailableReplicas)
	}
	if deployment.Status.UnavailableReplicas != unavailableReplicas {
		return fmt.Errorf("unexpected .unavailableReplicas: expect %d, got %d", unavailableReplicas, deployment.Status.UnavailableReplicas)
	}
	if !ptr.Equal(deployment.Status.TerminatingReplicas, terminatingReplicas) {
		return fmt.Errorf("unexpected .terminatingReplicas: expect %v, got %v", ptr.Deref(terminatingReplicas, -1), ptr.Deref(deployment.Status.TerminatingReplicas, -1))

	}
	return nil
}

func (d *deploymentTester) removeRSPods(ctx context.Context, replicaset *apps.ReplicaSet, count int, targetOnlyTerminating bool, gracePeriodSeconds int64) error {
	selector, err := metav1.LabelSelectorAsSelector(replicaset.Spec.Selector)
	if err != nil {
		return fmt.Errorf("could not parse a selector for a replica set %q: %w", replicaset.Name, err)
	}
	pods, err := d.c.CoreV1().Pods(replicaset.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return fmt.Errorf("failed to get pods for a replica set %q: %w", replicaset.Name, err)
	}

	for i, pod := range pods.Items {
		if i >= count {
			break
		}
		if targetOnlyTerminating && pod.DeletionTimestamp == nil {
			continue
		}
		err := d.c.CoreV1().Pods(replicaset.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To(gracePeriodSeconds)})
		if err != nil {
			return fmt.Errorf("failed to delete pod %q for a replica set %q: %w", pod.Name, replicaset.Name, err)
		}
	}

	return nil
}
