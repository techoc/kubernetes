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
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/retry"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	testutil "k8s.io/kubernetes/test/utils"
	"k8s.io/utils/ptr"
)

func TestNewDeployment(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()
	name := "test-new-deployment"

	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	replicas := int32(20)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}
	tester.deployment.Spec.MinReadySeconds = 4

	tester.deployment.Annotations = map[string]string{"test": "should-copy-to-replica-set", v1.LastAppliedConfigAnnotation: "should-not-copy-to-replica-set"}
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %s: %v", tester.deployment.Name, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边手动标记 Pod ready，一边等待 Deployment 完成；刚创建时 rolling update 约束可能被短暂违反，故用软检查。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 检查新 ReplicaSet 的 annotation。
	newRS, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}
	if newRS.Annotations["test"] != "should-copy-to-replica-set" {
		t.Errorf("expected new ReplicaSet annotations copied from Deployment %s, got: %v", tester.deployment.Name, newRS.Annotations)
	}
	if newRS.Annotations[v1.LastAppliedConfigAnnotation] != "" {
		t.Errorf("expected new ReplicaSet last-applied annotation not copied from Deployment %s", tester.deployment.Name)
	}

	// 新 ReplicaSet 的 selector、label 和 template label 都应带 pod-template-hash。
	rsHash, err := checkRSHashLabels(newRS)
	if err != nil {
		t.Error(err)
	}

	// Deployment 选中的所有 Pod 都应带 pod-template-hash label。
	selector, err := metav1.LabelSelectorAsSelector(tester.deployment.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to parse deployment %s selector: %v", name, err)
	}
	pods, err := c.CoreV1().Pods(ns.Name).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		t.Fatalf("failed to list pods of deployment %s: %v", name, err)
	}
	if len(pods.Items) != int(replicas) {
		t.Errorf("expected %d pods, got %d pods", replicas, len(pods.Items))
	}
	podHash, err := checkPodsHashLabel(pods)
	if err != nil {
		t.Error(err)
	}
	if rsHash != podHash {
		t.Errorf("found mismatching pod-template-hash value: rs hash = %s whereas pod hash = %s", rsHash, podHash)
	}
}

// TestDeploymentRollingUpdate 验证 Deployment 支持滚动发布、回滚和 roll over。
func TestDeploymentRollingUpdate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-rolling-update-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	replicas := int32(20)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}
	tester.deployment.Spec.MinReadySeconds = 4
	quarter := intstr.FromString("25%")
	tester.deployment.Spec.Strategy.RollingUpdate = &apps.RollingUpdateDeployment{
		MaxUnavailable: &quarter,
		MaxSurge:       &quarter,
	}

	// 创建 Deployment。
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %s: %v", tester.deployment.Name, err)
	}
	oriImage := tester.deployment.Spec.Template.Spec.Containers[0].Image
	if err := tester.waitForDeploymentRevisionAndImage("1", oriImage); err != nil {
		t.Fatal(err)
	}
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 1. 发布新镜像。
	image := "new-image"
	if oriImage == image {
		t.Fatalf("bad test setup, deployment %s roll out with the same image", tester.deployment.Name)
	}
	imageFn := func(update *apps.Deployment) {
		update.Spec.Template.Spec.Containers[0].Image = image
	}
	tester.deployment, err = tester.updateDeployment(imageFn)
	if err != nil {
		t.Fatalf("failed to update deployment %s: %v", tester.deployment.Name, err)
	}
	if err := tester.waitForDeploymentRevisionAndImage("2", image); err != nil {
		t.Fatal(err)
	}
	if err := tester.waitForDeploymentCompleteAndCheckRollingAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 2. 上一次滚动更新尚未完成时就再次 roll over。
	image = "dont-finish"
	imageFn = func(update *apps.Deployment) {
		update.Spec.Template.Spec.Containers[0].Image = image
	}
	tester.deployment, err = tester.updateDeployment(imageFn)
	if err != nil {
		t.Fatalf("failed to update deployment %s: %v", tester.deployment.Name, err)
	}
	if err := tester.waitForDeploymentRevisionAndImage("3", image); err != nil {
		t.Fatal(err)
	}

	// 不标记 Pod ready，让滚动发布保持进行中，此时再触发一次新发布。
	image = "rollover"
	imageFn = func(update *apps.Deployment) {
		update.Spec.Template.Spec.Containers[0].Image = image
	}
	tester.deployment, err = tester.updateDeployment(imageFn)
	if err != nil {
		t.Fatalf("failed to update deployment %s: %v", tester.deployment.Name, err)
	}
	if err := tester.waitForDeploymentRevisionAndImage("4", image); err != nil {
		t.Fatal(err)
	}
	if err := tester.waitForDeploymentCompleteAndCheckRollingAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}
	_, allOldRSs, err := testutil.GetOldReplicaSets(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed retrieving old replicasets of deployment %s: %v", tester.deployment.Name, err)
	}
	for _, oldRS := range allOldRSs {
		if *oldRS.Spec.Replicas != 0 {
			t.Errorf("expected old replicaset %s of deployment %s to have 0 replica, got %d", oldRS.Name, tester.deployment.Name, *oldRS.Spec.Replicas)
		}
	}
}

// 除 apps/v1beta1 和 extensions/v1beta1 外，所有 API 版本的 selector 均不可变。
func TestDeploymentSelectorImmutability(t *testing.T) {
	closeFn, c := dcSimpleSetup(t)
	defer closeFn()

	name := "test-deployment-selector-immutability"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, int32(20))}
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create apps/v1 deployment %s: %v", tester.deployment.Name, err)
	}

	// 验证 apps/v1 的 selector 不可变。
	deploymentAppsV1, err := c.AppsV1().Deployments(ns.Name).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get apps/v1 deployment %s: %v", name, err)
	}
	newSelectorLabels := map[string]string{"name_apps_v1": "test_apps_v1"}
	deploymentAppsV1.Spec.Selector.MatchLabels = newSelectorLabels
	deploymentAppsV1.Spec.Template.Labels = newSelectorLabels
	_, err = c.AppsV1().Deployments(ns.Name).Update(context.TODO(), deploymentAppsV1, metav1.UpdateOptions{})
	if err == nil {
		t.Fatalf("failed to provide validation error when changing immutable selector when updating apps/v1 deployment %s", deploymentAppsV1.Name)
	}
	expectedErrType := "Invalid value"
	expectedErrDetail := "field is immutable"
	if !strings.Contains(err.Error(), expectedErrType) || !strings.Contains(err.Error(), expectedErrDetail) {
		t.Errorf("error message does not match, expected type: %s, expected detail: %s, got: %s", expectedErrType, expectedErrDetail, err.Error())
	}
}

// TestPausedDeployment 验证暂停的 Deployment 不会启动新的 rollout。
func TestPausedDeployment(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-paused-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}
	tester.deployment.Spec.Paused = true
	tgps := int64(1)
	tester.deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = &tgps

	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %s: %v", tester.deployment.Name, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 验证暂停状态下不会创建新的 ReplicaSet。
	if err := tester.expectNoNewReplicaSet(); err != nil {
		t.Fatal(err)
	}

	// 恢复 Deployment。
	tester.deployment, err = tester.updateDeployment(resumeFn)
	if err != nil {
		t.Fatalf("failed to resume deployment %s: %v", tester.deployment.Name, err)
	}

	// 等待 controller 感知恢复操作。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边手动标记 Pod ready，一边等待 Deployment 完成；刚创建时 rolling update 约束可能被短暂违反，故用软检查。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 应创建新的 ReplicaSet。
	if _, err := tester.expectNewReplicaSet(); err != nil {
		t.Fatal(err)
	}

	// 暂停 Deployment。
	// 暂停状态下不应触发新的 rollout。
	tester.deployment, err = tester.updateDeployment(pauseFn)
	if err != nil {
		t.Fatalf("failed to pause deployment %s: %v", tester.deployment.Name, err)
	}

	// 等待 controller 感知暂停操作。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 更新 Deployment template。
	newTGPS := int64(0)
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Template.Spec.TerminationGracePeriodSeconds = &newTGPS
	})
	if err != nil {
		t.Fatalf("failed updating template of deployment %s: %v", tester.deployment.Name, err)
	}

	// 等待 controller 感知 rollout。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 验证暂停状态下不会创建新的 ReplicaSet。
	if err := tester.expectNoNewReplicaSet(); err != nil {
		t.Fatal(err)
	}

	_, allOldRs, err := testutil.GetOldReplicaSets(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed retrieving old replicasets of deployment %s: %v", tester.deployment.Name, err)
	}
	if len(allOldRs) != 1 {
		t.Errorf("expected an old replica set, got %v", allOldRs)
	}
	if *allOldRs[0].Spec.Template.Spec.TerminationGracePeriodSeconds == newTGPS {
		t.Errorf("TerminationGracePeriodSeconds on the replica set should be %d, got %d", tgps, newTGPS)
	}
}

// TestScalePausedDeployment 验证暂停的 Deployment 仍可扩缩容。
func TestScalePausedDeployment(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-scale-paused-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}
	tgps := int64(1)
	tester.deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = &tgps

	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %s: %v", tester.deployment.Name, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边手动标记 Pod ready，一边等待 Deployment 完成；刚创建时 rolling update 约束可能被短暂违反，故用软检查。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 应创建新的 ReplicaSet。
	if _, err := tester.expectNewReplicaSet(); err != nil {
		t.Fatal(err)
	}

	// 暂停 Deployment。
	tester.deployment, err = tester.updateDeployment(pauseFn)
	if err != nil {
		t.Fatalf("failed to pause deployment %s: %v", tester.deployment.Name, err)
	}

	// 等待 controller 感知扩缩容操作。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 扩缩容暂停状态的 Deployment。
	newReplicas := int32(10)
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = &newReplicas
	})
	if err != nil {
		t.Fatalf("failed updating deployment %s: %v", tester.deployment.Name, err)
	}

	// 等待 controller 感知扩缩容操作。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 验证新 ReplicaSet 已完成扩缩容。
	rs, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}
	if *rs.Spec.Replicas != newReplicas {
		t.Errorf("expected new replicaset replicas = %d, got %d", newReplicas, *rs.Spec.Replicas)
	}

	// 一边手动标记 Pod ready，一边等待 Deployment 完成；刚扩缩容时 rolling update 约束可能被短暂违反，故用软检查。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}
}

// TestDeploymentHashCollision 验证 hash 碰撞不应阻塞 Deployment 的 rollout。
func TestDeploymentHashCollision(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-hash-collision-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}

	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %s: %v", tester.deployment.Name, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 模拟一次 hash 碰撞。
	newRS, err := testutil.GetNewReplicaSet(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed getting new replicaset of deployment %s: %v", tester.deployment.Name, err)
	}
	if newRS == nil {
		t.Fatalf("unable to find new replicaset of deployment %s", tester.deployment.Name)
	}
	_, err = tester.updateReplicaSet(newRS.Name, func(update *apps.ReplicaSet) {
		*update.Spec.Template.Spec.TerminationGracePeriodSeconds = int64(5)
	})
	if err != nil {
		t.Fatalf("failed updating replicaset %s template: %v", newRS.Name, err)
	}

	// 期望 Deployment 的 collision 计数器递增。
	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		d, err := c.AppsV1().Deployments(ns.Name).Get(context.TODO(), tester.deployment.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return d.Status.CollisionCount != nil && *d.Status.CollisionCount == int32(1), nil
	}); err != nil {
		t.Fatalf("Failed to increment collision counter for deployment %q: %v", tester.deployment.Name, err)
	}

	// 期望创建新的 ReplicaSet。
	if err := tester.waitForDeploymentRevisionAndImage("2", fakeImage); err != nil {
		t.Fatal(err)
	}
}

func checkRSHashLabels(rs *apps.ReplicaSet) (string, error) {
	hash := rs.Labels[apps.DefaultDeploymentUniqueLabelKey]
	selectorHash := rs.Spec.Selector.MatchLabels[apps.DefaultDeploymentUniqueLabelKey]
	templateLabelHash := rs.Spec.Template.Labels[apps.DefaultDeploymentUniqueLabelKey]

	if hash != selectorHash || selectorHash != templateLabelHash {
		return "", fmt.Errorf("mismatching hash value found in replicaset %s: %#v", rs.Name, rs)
	}
	if len(hash) == 0 {
		return "", fmt.Errorf("unexpected replicaset %s missing required pod-template-hash labels", rs.Name)
	}

	if !strings.HasSuffix(rs.Name, hash) {
		return "", fmt.Errorf("unexpected replicaset %s name suffix doesn't match hash %s", rs.Name, hash)
	}

	return hash, nil
}

func checkPodsHashLabel(pods *v1.PodList) (string, error) {
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods given")
	}
	var hash string
	for _, pod := range pods.Items {
		podHash := pod.Labels[apps.DefaultDeploymentUniqueLabelKey]
		if len(podHash) == 0 {
			return "", fmt.Errorf("found pod %s missing pod-template-hash label: %#v", pod.Name, pods)
		}
		// 保存第一个有效的 hash。
		if len(hash) == 0 {
			hash = podHash
		}
		if podHash != hash {
			return "", fmt.Errorf("found pod %s with mismatching pod-template-hash value %s: %#v", pod.Name, podHash, pods)
		}
	}
	return hash, nil
}

// TestFailedDeployment 验证 Deployment 在规定时间内无法推进时会进入超时条件。
func TestFailedDeployment(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-failed-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "progress-check"
	replicas := int32(1)
	three := int32(3)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	tester.deployment.Spec.ProgressDeadlineSeconds = &three
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	if err = tester.waitForDeploymentUpdatedReplicasGTE(replicas); err != nil {
		t.Fatal(err)
	}

	// Pod 未标记 Ready，超过 progressDeadlineSeconds 后 Deployment 应进入超时条件。
	if err = tester.waitForDeploymentWithCondition(deploymentutil.TimedOutReason, apps.DeploymentProgressing); err != nil {
		t.Fatal(err)
	}

	// 手动标记 Pod Ready，等待 Deployment 完成。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatalf("deployment %q fails to have its status becoming valid: %v", deploymentName, err)
	}

	// 等待 Deployment 进入完成条件。
	if err = tester.waitForDeploymentWithCondition(deploymentutil.NewRSAvailableReason, apps.DeploymentProgressing); err != nil {
		t.Fatal(err)
	}
}

func TestOverlappingDeployments(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-overlapping-deployments"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	replicas := int32(1)
	firstDeploymentName := "first-deployment"
	secondDeploymentName := "second-deployment"
	testers := []*deploymentTester{
		{t: t, c: c, deployment: newDeployment(firstDeploymentName, ns.Name, replicas)},
		{t: t, c: c, deployment: newDeployment(secondDeploymentName, ns.Name, replicas)},
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 创建两个 selector 重叠的 Deployment。
	var err error
	var rss []*apps.ReplicaSet
	for _, tester := range testers {
		tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
		dname := tester.deployment.Name
		if err != nil {
			t.Fatalf("failed to create deployment %q: %v", dname, err)
		}
		// 等待 Deployment 更新到 revision 1。
		if err = tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
			t.Fatalf("failed to update deployment %q to revision 1: %v", dname, err)
		}
		// 一边手动标记 Pod ready，一边等待 Deployment 完成。
		if err = tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
			t.Fatalf("deployment %q failed to complete: %v", dname, err)
		}
		// 获取 Deployment 对应的 ReplicaSet。
		newRS, err := tester.getNewReplicaSet()
		if err != nil {
			t.Fatalf("failed to get new replicaset of deployment %q: %v", dname, err)
		}
		if newRS == nil {
			t.Fatalf("unable to find new replicaset of deployment %q", dname)
		}
		// 保存 ReplicaSet 供后续步骤使用。
		rss = append(rss, newRS)
	}

	// 两个 Deployment 应独立推进，各自的 ReplicaSet 不应相同。
	if rss[0].UID == rss[1].UID {
		t.Fatalf("overlapping deployments should not share the same replicaset")
	}

	// 只将第一个 Deployment 扩容 1 个副本。
	newReplicas := replicas + 1
	testers[0].deployment, err = testers[0].updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = &newReplicas
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", firstDeploymentName, err)
	}

	// 确认扩缩容后 Deployment 完成。
	if err := testers[0].waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatalf("deployment %q failed to complete after scaling: %v", firstDeploymentName, err)
	}

	// 验证两个 Deployment 的 ReplicaSet 副本数均已更新。
	for i, tester := range testers {
		rs, err := c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), rss[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get replicaset %q: %v", rss[i].Name, err)
		}
		if *rs.Spec.Replicas != *tester.deployment.Spec.Replicas {
			t.Errorf("expected replicaset %q of deployment %q has %d replicas, but found %d replicas", rs.Name, firstDeploymentName, *tester.deployment.Spec.Replicas, *rs.Spec.Replicas)
		}
	}
}

// TestScaledRolloutDeployment 验证同时更新副本数和 template 时不应阻塞 rollout。
func TestScaledRolloutDeployment(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-scaled-rollout-deployment"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 创建 rolling update 策略的 Deployment，maxSurge=3、maxUnavailable=2。
	var err error
	replicas := int32(10)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(name, ns.Name, replicas)}
	tester.deployment.Spec.Strategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt32(3))
	tester.deployment.Spec.Strategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(2))
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", name, err)
	}
	if err = tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}
	if err = tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatalf("deployment %q failed to complete: %v", name, err)
	}

	// 启动新 rollout 前记录当前 ReplicaSet。
	firstRS, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}

	// 换成另一个新镜像但不标记 Pod ready，使新 ReplicaSet 暂时无法完成。
	fakeImage2 := "fakeimage2"
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Template.Spec.Containers[0].Image = fakeImage2
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", name, err)
	}
	if err = tester.waitForDeploymentRevisionAndImage("2", fakeImage2); err != nil {
		t.Fatal(err)
	}

	// 验证第 2 次 rollout 后仍满足最小可用副本数。
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get deployment %q: %v", name, err)
	}
	minAvailableReplicas := deploymentutil.MinAvailable(tester.deployment)
	if tester.deployment.Status.AvailableReplicas < minAvailableReplicas {
		t.Fatalf("deployment %q does not have minimum number of available replicas after 2nd rollout", name)
	}

	// 等待第 1 次 rollout 的旧 ReplicaSet 达到期望副本数。
	firstRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), firstRS.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get replicaset %q: %v", firstRS.Name, err)
	}
	if err = tester.waitRSStable(firstRS); err != nil {
		t.Fatal(err)
	}

	// 等待第 2 次 rollout 的新 ReplicaSet 达到期望副本数。
	secondRS, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}
	if err = tester.waitRSStable(secondRS); err != nil {
		t.Fatal(err)
	}

	// 扩容 Deployment 的同时再次更换镜像，这次标记所有 Pod ready。
	newReplicas := int32(20)
	fakeImage3 := "fakeimage3"
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = &newReplicas
		update.Spec.Template.Spec.Containers[0].Image = fakeImage3
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", name, err)
	}
	if err = tester.waitForDeploymentRevisionAndImage("3", fakeImage3); err != nil {
		t.Fatal(err)
	}
	if err = tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatalf("deployment %q failed to complete: %v", name, err)
	}

	// 验证第 3 次 rollout 后每个 ReplicaSet 的 desiredReplicas annotation 正确。
	thirdRS, err := testutil.GetNewReplicaSet(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed getting new revision 3 replicaset for deployment %q: %v", name, err)
	}
	rss := []*apps.ReplicaSet{firstRS, secondRS, thirdRS}
	for _, curRS := range rss {
		curRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), curRS.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get replicaset when checking desired replicas annotation: %v", err)
		}
		desired, ok := deploymentutil.GetDesiredReplicasAnnotation(logger, curRS)
		if !ok {
			t.Fatalf("failed to retrieve desiredReplicas annotation for replicaset %q", curRS.Name)
		}
		if desired != *(tester.deployment.Spec.Replicas) {
			t.Fatalf("unexpected desiredReplicas annotation for replicaset %q: expected %d, got %d", curRS.Name, *(tester.deployment.Spec.Replicas), desired)
		}
	}

	// 换成另一个新镜像但不标记 Pod ready，使新 ReplicaSet 暂时无法完成。
	fakeImage4 := "fakeimage4"
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Template.Spec.Containers[0].Image = fakeImage4
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", name, err)
	}
	if err = tester.waitForDeploymentRevisionAndImage("4", fakeImage4); err != nil {
		t.Fatal(err)
	}

	// 验证第 4 次 rollout 后仍满足最小可用副本数。
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get deployment %q: %v", name, err)
	}
	minAvailableReplicas = deploymentutil.MinAvailable(tester.deployment)
	if tester.deployment.Status.AvailableReplicas < minAvailableReplicas {
		t.Fatalf("deployment %q does not have minimum number of available replicas after 4th rollout", name)
	}

	// 等待第 3 次 rollout 的旧 ReplicaSet 达到期望副本数。
	thirdRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), thirdRS.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get replicaset %q: %v", thirdRS.Name, err)
	}
	if err = tester.waitRSStable(thirdRS); err != nil {
		t.Fatal(err)
	}

	// 等待第 4 次 rollout 的新 ReplicaSet 达到期望副本数。
	fourthRS, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}
	if err = tester.waitRSStable(fourthRS); err != nil {
		t.Fatal(err)
	}

	// 缩容 Deployment 的同时再次更换镜像，这次标记所有 Pod ready。
	newReplicas = int32(5)
	fakeImage5 := "fakeimage5"
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = &newReplicas
		update.Spec.Template.Spec.Containers[0].Image = fakeImage5
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", name, err)
	}
	if err = tester.waitForDeploymentRevisionAndImage("5", fakeImage5); err != nil {
		t.Fatal(err)
	}
	if err = tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatalf("deployment %q failed to complete: %v", name, err)
	}

	// 验证第 5 次 rollout 后每个 ReplicaSet 的 desiredReplicas annotation 正确。
	fifthRS, err := testutil.GetNewReplicaSet(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed getting new revision 5 replicaset for deployment %q: %v", name, err)
	}
	rss = []*apps.ReplicaSet{thirdRS, fourthRS, fifthRS}
	for _, curRS := range rss {
		curRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), curRS.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get replicaset when checking desired replicas annotation: %v", err)
		}
		desired, ok := deploymentutil.GetDesiredReplicasAnnotation(logger, curRS)
		if !ok {
			t.Fatalf("failed to retrieve desiredReplicas annotation for replicaset %q", curRS.Name)
		}
		if desired != *(tester.deployment.Spec.Replicas) {
			t.Fatalf("unexpected desiredReplicas annotation for replicaset %q: expected %d, got %d", curRS.Name, *(tester.deployment.Spec.Replicas), desired)
		}
	}
}

func TestSpecReplicasChange(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-spec-replicas-change"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	tester.deployment.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
	tester.deployment.Spec.Strategy.RollingUpdate = nil
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 扩缩容 Deployment，验证其 ReplicaSet 的 .spec.replicas 同步更新。
	if err = tester.scaleDeployment(2); err != nil {
		t.Fatal(err)
	}
	if err = tester.scaleDeployment(0); err != nil {
		t.Fatal(err)
	}
	if err = tester.scaleDeployment(1); err != nil {
		t.Fatal(err)
	}

	// 只修改 template annotation，验证 .spec.replicas 不变时 status 仍会更新。
	var oldGeneration int64
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		oldGeneration = update.Generation
		update.Spec.RevisionHistoryLimit = ptr.To[int32](4)
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", tester.deployment.Name, err)
	}

	savedGeneration := tester.deployment.Generation
	if savedGeneration == oldGeneration {
		t.Fatalf("Failed to verify .Generation has incremented for deployment %q", deploymentName)
	}
	if err = tester.waitForObservedDeployment(savedGeneration); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentAvailableCondition(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, true)

	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-deployment-available-condition"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(10)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	// 设置一个较大的 minReadySeconds。
	tester.deployment.Spec.MinReadySeconds = 3600
	// progressDeadlineSeconds 必须大于 minReadySeconds。
	tester.deployment.Spec.ProgressDeadlineSeconds = ptr.To[int32](7200)
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 controller 观察到 Deployment 且 updated replica 数达到指定值。
	if err = tester.waitForDeploymentUpdatedReplicasGTE(replicas); err != nil {
		t.Fatal(err)
	}

	// Pod 未标记 ready，等待 Deployment 出现 MinimumReplicasUnavailable reason。
	if err = tester.waitForDeploymentWithCondition(deploymentutil.MinimumReplicasUnavailable, apps.DeploymentAvailable); err != nil {
		t.Fatal(err)
	}

	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(10, 10, 0, 0, 10, ptr.To[int32](0)); err != nil {
		t.Fatal(err)
	}

	// 标记 Pod ready，但不等待 Deployment 完成。
	if err = tester.markUpdatedPodsReadyWithoutComplete(); err != nil {
		t.Fatal(err)
	}

	// 等待 ready replica 数与期望副本数一致。
	if err = tester.waitForReadyReplicas(); err != nil {
		t.Fatal(err)
	}

	// minReadySeconds 期间内 Deployment 应仍保留 MinimumReplicasUnavailable reason。
	if err = tester.waitForDeploymentWithCondition(deploymentutil.MinimumReplicasUnavailable, apps.DeploymentAvailable); err != nil {
		t.Fatal(err)
	}

	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(10, 10, 10, 0, 10, ptr.To[int32](0)); err != nil {
		t.Fatal(err)
	}

	// 将 minReadySeconds 调小。
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.MinReadySeconds = 1
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
	}

	// 等待 Deployment 感知 minReadySeconds 的变化。
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	// 经过 minReadySeconds 后应转为 MinimumReplicasAvailable reason。
	if err = tester.waitForDeploymentWithCondition(deploymentutil.MinimumReplicasAvailable, apps.DeploymentAvailable); err != nil {
		t.Fatal(err)
	}

	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(10, 10, 10, 10, 0, ptr.To[int32](0)); err != nil {
		t.Fatal(err)
	}
}

// testRSControllerRefPatch 等待 Deployment 自动修复 ReplicaSet 上错误的 ControllerRef。
func testRSControllerRefPatch(t *testing.T, tester *deploymentTester, rs *apps.ReplicaSet, ownerReference *metav1.OwnerReference, expectedOwnerReferenceNum int) {
	ns := rs.Namespace
	rsClient := tester.c.AppsV1().ReplicaSets(ns)
	rs, err := tester.updateReplicaSet(rs.Name, func(update *apps.ReplicaSet) {
		update.OwnerReferences = []metav1.OwnerReference{*ownerReference}
	})
	if err != nil {
		t.Fatalf("failed to update replicaset %q: %v", rs.Name, err)
	}

	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		newRS, err := rsClient.Get(context.TODO(), rs.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return metav1.GetControllerOf(newRS) != nil, nil
	}); err != nil {
		t.Fatalf("failed to wait for controllerRef of the replicaset %q to become nil: %v", rs.Name, err)
	}

	newRS, err := rsClient.Get(context.TODO(), rs.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to obtain replicaset %q: %v", rs.Name, err)
	}
	controllerRef := metav1.GetControllerOf(newRS)
	if controllerRef.UID != tester.deployment.UID {
		t.Fatalf("controllerRef of replicaset %q has a different UID: Expected %v, got %v", newRS.Name, tester.deployment.UID, controllerRef.UID)
	}
	ownerReferenceNum := len(newRS.GetOwnerReferences())
	if ownerReferenceNum != expectedOwnerReferenceNum {
		t.Fatalf("unexpected number of owner references for replicaset %q: Expected %d, got %d", newRS.Name, expectedOwnerReferenceNum, ownerReferenceNum)
	}
}

func TestGeneralReplicaSetAdoption(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-general-replicaset-adoption"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边标记 Pod ready，一边等待 Deployment 完成。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 获取 Deployment 对应的 ReplicaSet。
	rs, err := testutil.GetNewReplicaSet(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed to get replicaset of deployment %q: %v", deploymentName, err)
	}
	if rs == nil {
		t.Fatalf("unable to find replicaset of deployment %q", deploymentName)
	}

	// RS 唯一的 OwnerReference 指向其他类型对象（如 StatefulSet）且 Controller=false 时，
	// Deployment 应补一条指向自身、Controller=true 的 OwnerReference（ControllerRef）。
	var falseVar = false
	ownerReference := metav1.OwnerReference{UID: uuid.NewUUID(), APIVersion: "apps/v1", Kind: "StatefulSet", Name: deploymentName, Controller: &falseVar}
	testRSControllerRefPatch(t, tester, rs, &ownerReference, 2)

	// RS 唯一的 OwnerReference 已指向该 Deployment 但 Controller=false 时，应将其改为 true。
	ownerReference = metav1.OwnerReference{UID: tester.deployment.UID, APIVersion: "apps/v1", Kind: "Deployment", Name: deploymentName, Controller: &falseVar}
	testRSControllerRefPatch(t, tester, rs, &ownerReference, 1)
}

func testScalingUsingScaleSubresource(t *testing.T, tester *deploymentTester, replicas int32) {
	ns := tester.deployment.Namespace
	deploymentName := tester.deployment.Name
	deploymentClient := tester.c.AppsV1().Deployments(ns)
	deployment, err := deploymentClient.Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain deployment %q: %v", deploymentName, err)
	}
	scale, err := tester.c.AppsV1().Deployments(ns).GetScale(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain scale subresource for deployment %q: %v", deploymentName, err)
	}
	if scale.Spec.Replicas != *deployment.Spec.Replicas {
		t.Fatalf("Scale subresource for deployment %q does not match .Spec.Replicas: expected %d, got %d", deploymentName, *deployment.Spec.Replicas, scale.Spec.Replicas)
	}

	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		scale, err := tester.c.AppsV1().Deployments(ns).GetScale(context.TODO(), deploymentName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		scale.Spec.Replicas = replicas
		_, err = tester.c.AppsV1().Deployments(ns).UpdateScale(context.TODO(), deploymentName, scale, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("Failed to set .Spec.Replicas of scale subresource for deployment %q: %v", deploymentName, err)
	}

	deployment, err = deploymentClient.Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain deployment %q: %v", deploymentName, err)
	}
	if *deployment.Spec.Replicas != replicas {
		t.Fatalf(".Spec.Replicas of deployment %q does not match its scale subresource: expected %d, got %d", deploymentName, replicas, *deployment.Spec.Replicas)
	}
}

func TestDeploymentScaleSubresource(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-deployment-scale-subresource"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(2)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边标记 Pod ready，一边等待 Deployment 完成。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// 通过 scale 子资源将 Deployment 扩到 3。
	testScalingUsingScaleSubresource(t, tester, 3)
	// 通过 scale 子资源将 Deployment 缩到 0。
	testScalingUsingScaleSubresource(t, tester, 0)
}

// TestReplicaSetOrphaningAndAdoptionWhenLabelsChange 验证 ReplicaSet 的 label 不再匹配 Deployment selector 时会被 orphan，
// 并间接验证即使 PodTemplateSpec 未变化，新建 ReplicaSet 仍会触发名称碰撞规避。
// https://github.com/kubernetes/kubernetes/pull/59212#discussion_r166465113
func TestReplicaSetOrphaningAndAdoptionWhenLabelsChange(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-replicaset-orphaning-and-adoption-when-labels-change"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(1)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 等待 Deployment 更新到 revision 1。
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}

	// 一边标记 Pod ready，一边等待 Deployment 完成。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}

	// Orphaning：RS label 不再匹配时，Deployment 应移除其 OwnerReference。

	// 获取 Deployment 对应的 ReplicaSet。
	rs, err := testutil.GetNewReplicaSet(tester.deployment, c)
	if err != nil {
		t.Fatalf("failed to get replicaset of deployment %q: %v", deploymentName, err)
	}
	if rs == nil {
		t.Fatalf("unable to find replicaset of deployment %q", deploymentName)
	}

	// 验证 ReplicaSet 的 controllerRef 存在且指向该 Deployment。
	controllerRef := metav1.GetControllerOf(rs)
	if controllerRef == nil {
		t.Fatalf("controllerRef of replicaset %q is nil", rs.Name)
	}
	if controllerRef.UID != tester.deployment.UID {
		t.Fatalf("controllerRef of replicaset %q has a different UID: Expected %v, got %v", rs.Name, tester.deployment.UID, controllerRef.UID)
	}

	// 修改 ReplicaSet 的 label，使其不再匹配 Deployment。
	labelMap := map[string]string{"new-name": "new-test"}
	rs, err = tester.updateReplicaSet(rs.Name, func(update *apps.ReplicaSet) {
		update.Labels = labelMap
	})
	if err != nil {
		t.Fatalf("failed to update replicaset %q: %v", rs.Name, err)
	}

	// 等待 ReplicaSet 的 controllerRef 被清空。
	rsClient := tester.c.AppsV1().ReplicaSets(ns.Name)
	if err = wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		rs, err = rsClient.Get(context.TODO(), rs.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return metav1.GetControllerOf(rs) == nil, nil
	}); err != nil {
		t.Fatalf("failed to wait for controllerRef of replicaset %q to become nil: %v", rs.Name, err)
	}

	// 等待 Deployment 创建新 ReplicaSet；由于 RS 名称是确定性生成的，会触发碰撞规避，
	// 新 RS 将使用不同 hash 以保证名称唯一。
	var newRS *apps.ReplicaSet
	if err = wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		newRS, err = testutil.GetNewReplicaSet(tester.deployment, c)
		if err != nil {
			return false, fmt.Errorf("failed to get new replicaset of deployment %q after orphaning: %v", deploymentName, err)
		}
		return newRS != nil, nil
	}); err != nil {
		t.Fatalf("failed to wait for deployment %q to create a new replicaset after orphaning: %v", deploymentName, err)
	}
	if newRS.UID == rs.UID {
		t.Fatalf("expect deployment %q to create a new replicaset different from the orphaned one, but it isn't", deploymentName)
	}

	// Adoption：RS label 重新匹配时，Deployment 应为其添加 controllerRef。

	// 修改旧 ReplicaSet 的 label 使其重新匹配 Deployment。
	rs, err = tester.updateReplicaSet(rs.Name, func(update *apps.ReplicaSet) {
		update.Labels = testLabels()
	})
	if err != nil {
		t.Fatalf("failed to update replicaset %q: %v", rs.Name, err)
	}

	// 等待 Deployment 重新纳管旧 ReplicaSet。
	if err = wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		rs, err := rsClient.Get(context.TODO(), rs.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		controllerRef = metav1.GetControllerOf(rs)
		return controllerRef != nil && controllerRef.UID == tester.deployment.UID, nil
	}); err != nil {
		t.Fatalf("failed waiting for replicaset adoption by deployment %q to complete: %v", deploymentName, err)
	}
}

func TestTerminatingReplicasDeploymentStatus(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, false)

	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()

	name := "test-terminating-replica-status"
	ns := framework.CreateNamespaceOrDie(c, name, t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	deploymentName := "deployment"
	replicas := int32(6)
	tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
	tester.deployment.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
	tester.deployment.Spec.Strategy.RollingUpdate = nil
	tester.deployment.Spec.Template.Spec.NodeName = "fake-node"
	tester.deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(300))

	var err error
	tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
	}

	// 启动 informer 和 controller。
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	// 一边标记 Pod ready，一边等待 Deployment 完成。
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}
	// feature gate 关闭时不应更新 terminating replica 数。
	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(6, 6, 6, 6, 0, nil); err != nil {
		t.Fatal(err)
	}

	// 缩容 Deployment。
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = ptr.To(int32(4))
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
	}
	// 等待 ready replica 数与期望副本数一致。
	if err = tester.waitForReadyReplicas(); err != nil {
		t.Fatal(err)
	}
	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(4, 4, 4, 4, 0, nil); err != nil {
		t.Fatal(err)
	}

	// feature gate 开启时应更新 terminating replica 数。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, true)
	// 缩容 Deployment。
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = ptr.To(int32(3))
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
	}
	// 等待 ready replica 数与期望副本数一致。
	if err = tester.waitForReadyReplicas(); err != nil {
		t.Fatal(err)
	}
	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(3, 3, 3, 3, 0, ptr.To[int32](3)); err != nil {
		t.Fatal(err)
	}

	// feature gate 关闭时不应更新 terminating replica 数。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, false)
	// 缩容 Deployment。
	tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
		update.Spec.Replicas = ptr.To(int32(2))
	})
	if err != nil {
		t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
	}
	// 等待 ready replica 数与期望副本数一致。
	if err = tester.waitForReadyReplicas(); err != nil {
		t.Fatal(err)
	}
	// 验证 DeploymentStatus 各 replicas 字段的数值符合预期。
	if err = tester.checkDeploymentStatusReplicasFields(2, 2, 2, 2, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRecreateDeploymentForPodReplacement(t *testing.T) {
	tests := []struct {
		name                                                   string
		enableDeploymentReplicaSetTerminatingReplicas          bool
		expectedReplicasAfterOldRSScaleDown                    int32
		expectedTerminatingReplicasAfterOldRSScaleDown         *int32
		expectedReplicasAfterNewRS                             int32
		expectedTerminatingReplicasAfterNewRS                  *int32
		expectedReplicasAfterInFlightPodTermination            int32
		expectedTerminatingReplicasAfterInFlightPodTermination *int32
		expectedReplicasAfterInFlightScaleUp                   int32
		expectedTerminatingReplicasAfterInFlightScaleUp        *int32
		expectedReplicasAfterInFlightScaleDown                 int32
		expectedTerminatingReplicasAfterInFlightScaleDown      *int32
		expectedReplicasForDeploymentComplete                  int32
		expectedTerminatingReplicasForDeploymentComplete       *int32
	}{
		{
			name: "recreate should wait for terminating pods to complete in a new rollout with DeploymentReplicaSetTerminatingReplicas=false",
			enableDeploymentReplicaSetTerminatingReplicas: false,

			expectedReplicasAfterOldRSScaleDown:                    0,
			expectedTerminatingReplicasAfterOldRSScaleDown:         nil, // terminating counting disabled for all expectedTerminating
			expectedReplicasAfterNewRS:                             6,
			expectedTerminatingReplicasAfterNewRS:                  nil,
			expectedReplicasAfterInFlightPodTermination:            6, // 1 pod terminated
			expectedTerminatingReplicasAfterInFlightPodTermination: nil,
			expectedReplicasAfterInFlightScaleUp:                   7, // +1 scale up
			expectedTerminatingReplicasAfterInFlightScaleUp:        nil,
			expectedReplicasAfterInFlightScaleDown:                 5, // -2 scale down
			expectedTerminatingReplicasAfterInFlightScaleDown:      nil,
			expectedReplicasForDeploymentComplete:                  5,
			expectedTerminatingReplicasForDeploymentComplete:       nil,
		},
		{
			name: "recreate should wait for terminating pods to complete in a new rollout with DeploymentReplicaSetTerminatingReplicas=true",
			enableDeploymentReplicaSetTerminatingReplicas: true,

			expectedReplicasAfterOldRSScaleDown:                    0,
			expectedTerminatingReplicasAfterOldRSScaleDown:         ptr.To[int32](6),
			expectedReplicasAfterNewRS:                             6,
			expectedTerminatingReplicasAfterNewRS:                  ptr.To[int32](0),
			expectedReplicasAfterInFlightPodTermination:            6, // 1 pod terminated
			expectedTerminatingReplicasAfterInFlightPodTermination: ptr.To[int32](1),
			expectedReplicasAfterInFlightScaleUp:                   7, // +1 scale up
			expectedTerminatingReplicasAfterInFlightScaleUp:        ptr.To[int32](1),
			expectedReplicasAfterInFlightScaleDown:                 5, // -2 scale down
			expectedTerminatingReplicasAfterInFlightScaleDown:      ptr.To[int32](3),
			expectedReplicasForDeploymentComplete:                  5,
			expectedTerminatingReplicasForDeploymentComplete:       ptr.To[int32](3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, test.enableDeploymentReplicaSetTerminatingReplicas)

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			closeFn, rm, dc, informers, c := dcSetup(ctx, t)
			defer closeFn()

			name := "test-recreate-deployment-pod-replacement"
			ns := framework.CreateNamespaceOrDie(c, name, t)
			defer framework.DeleteNamespaceOrDie(c, ns, t)

			// 启动 informer 和 controller。
			stopControllers := runControllersAndInformers(t, rm, dc, informers)
			defer stopControllers()

			deploymentName := "deployment"
			replicas := int32(6)
			tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
			tester.deployment.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
			tester.deployment.Spec.Strategy.RollingUpdate = nil
			tester.deployment.Spec.Template.Spec.NodeName = "fake-node"
			tester.deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(300))

			var err error
			tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
			}

			// 一边标记 Pod ready，一边等待 Deployment 完成。
			if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
				t.Fatal(err)
			}
			// 启动新 rollout 前记录当前 ReplicaSet。
			firstRS, err := tester.expectNewReplicaSet()
			if err != nil {
				t.Fatal(err)
			}

			// 触发新的 rollout。
			tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
				update.Spec.Template.Spec.Containers[0].Env = append(update.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "deploy2", Value: "true"})
			})
			if err != nil {
				t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
			}

			// 先等待第 1 次 rollout 的旧 ReplicaSet 缩到 0。
			firstRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), firstRS.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get replicaset %q: %v", firstRS.Name, err)
			}
			firstRS.Spec.Replicas = ptr.To[int32](0)
			if err = tester.waitRSStable(firstRS); err != nil {
				t.Fatal(err)
			}

			// 缩容阶段结束后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas := test.expectedReplicasAfterOldRSScaleDown
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasAfterOldRSScaleDown); err != nil {
				t.Fatal(err)
			}

			// 验证旧 Pod 终止前新 rollout 不会创建新 ReplicaSet。
			if err := tester.expectNoNewReplicaSet(); err != nil {
				t.Fatal(err)
			}
			// 删除 terminating Pod，跳过旧 RS 的优雅终止流程。
			if err := tester.removeRSPods(ctx, firstRS, math.MaxInt, true, 0); err != nil {
				t.Fatal(err)
			}

			// 新 ReplicaSet 创建后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasAfterNewRS
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasAfterNewRS); err != nil {
				t.Fatal(err)
			}
			// 验证新 rollout 创建了新的 ReplicaSet。
			secondRS, err := tester.expectNewReplicaSet()
			if err != nil {
				t.Fatal(err)
			}

			// 开始终止 1 个 Pod。
			err = tester.removeRSPods(ctx, secondRS, 1, false, 300)
			if err != nil {
				t.Fatal(err)
			}
			// Pod 意外终止后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasAfterInFlightPodTermination
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasAfterInFlightPodTermination); err != nil {
				t.Fatal(err)
			}

			// 在 rollout 过程中扩容。
			tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
				update.Spec.Replicas = ptr.To[int32](7)
			})
			if err != nil {
				t.Fatalf("failed to update deployment %q: %v", deploymentName, err)
			}

			// rollout 期间扩容后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasAfterInFlightScaleUp
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasAfterInFlightScaleUp); err != nil {
				t.Fatal(err)
			}

			// 在 rollout 过程中缩容。
			tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
				update.Spec.Replicas = ptr.To[int32](5)
			})
			if err != nil {
				t.Fatalf("failed to update/scale deployment %q: %v", deploymentName, err)
			}

			// rollout 期间缩容后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasAfterInFlightScaleDown
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasAfterInFlightScaleDown); err != nil {
				t.Fatal(err)
			}

			// Deployment 完成前，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasForDeploymentComplete
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, 0, 0, expectedReplicas, test.expectedTerminatingReplicasForDeploymentComplete); err != nil {
				t.Fatal(err)
			}

			// 一边标记 Pod ready，一边等待新 Deployment 完成。
			if err = tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
				t.Fatal(err)
			}
			// Deployment 完成后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasForDeploymentComplete
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, expectedReplicas, expectedReplicas, 0, test.expectedTerminatingReplicasForDeploymentComplete); err != nil {
				t.Fatal(err)
			}

			// 删除所有 terminating Pod（若有），跳过旧 RS 的优雅终止流程。
			if err := tester.removeRSPods(ctx, firstRS, math.MaxInt, true, 0); err != nil {
				t.Fatal(err)
			}
			// 删除 terminating Pod，跳过新 RS 的优雅终止流程。
			if err := tester.removeRSPods(ctx, secondRS, math.MaxInt, true, 0); err != nil {
				t.Fatal(err)
			}

			// Deployment 完成且旧 Pod 终止后，验证 DeploymentStatus 各 replicas 字段符合预期。
			expectedReplicas = test.expectedReplicasForDeploymentComplete
			var expectedFinalTerminatingReplicas *int32
			if test.enableDeploymentReplicaSetTerminatingReplicas {
				expectedFinalTerminatingReplicas = ptr.To[int32](0)
			}
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicas, expectedReplicas, expectedReplicas, expectedReplicas, 0, expectedFinalTerminatingReplicas); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRollingUpdateAndProportionalScalingForDeploymentPodReplacement(t *testing.T) {
	tests := []struct {
		name                                          string
		enableDeploymentReplicaSetTerminatingReplicas bool
		terminatingReplicasFirstRS                    int32
		terminatingReplicasSecondRS                   int32

		expectedFirstRSReplicasDuringNewRollout          int32
		expectedSecondRSReplicasDuringNewRollout         int32
		expectedTerminatingReplicasDuringNewRollout      *int32
		expectedFirstRSReplicasAfterInFlightScaleUp      int32
		expectedSecondRSReplicasAfterInFlightScaleUp     int32
		expectedTerminatingReplicasDuringInFlightScaleUp *int32
		expectedFirstRSAnnotationsAfterInFlightScaleUp   map[string]string
		expectedSecondRSAnnotationsAfterInFlightScaleUp  map[string]string
	}{
		// 初始规模为 100 副本，maxSurge 为 20。
		{
			name: "rolling update should not wait for terminating pods with DeploymentReplicaSetTerminatingReplicas=false",
			enableDeploymentReplicaSetTerminatingReplicas: false,

			expectedFirstRSReplicasDuringNewRollout:          100,
			expectedSecondRSReplicasDuringNewRollout:         20,
			expectedTerminatingReplicasDuringNewRollout:      nil,
			expectedFirstRSReplicasAfterInFlightScaleUp:      117,
			expectedSecondRSReplicasAfterInFlightScaleUp:     23,
			expectedTerminatingReplicasDuringInFlightScaleUp: nil,
			expectedFirstRSAnnotationsAfterInFlightScaleUp: map[string]string{
				deploymentutil.DesiredReplicasAnnotation: "120",
				deploymentutil.MaxReplicasAnnotation:     "140",
				deploymentutil.RevisionAnnotation:        "1",
			},
			expectedSecondRSAnnotationsAfterInFlightScaleUp: map[string]string{
				deploymentutil.DesiredReplicasAnnotation: "120",
				deploymentutil.MaxReplicasAnnotation:     "140",
				deploymentutil.RevisionAnnotation:        "2",
			},
		},
		{
			name: "rolling update and scaling should not wait for terminating pods with DeploymentReplicaSetTerminatingReplicas=true",
			enableDeploymentReplicaSetTerminatingReplicas: true,
			terminatingReplicasFirstRS:                    15,
			terminatingReplicasSecondRS:                   1,

			expectedFirstRSReplicasDuringNewRollout:          100,
			expectedSecondRSReplicasDuringNewRollout:         20,
			expectedTerminatingReplicasDuringNewRollout:      ptr.To[int32](15),
			expectedFirstRSReplicasAfterInFlightScaleUp:      117,
			expectedSecondRSReplicasAfterInFlightScaleUp:     23,
			expectedTerminatingReplicasDuringInFlightScaleUp: ptr.To[int32](16),
			expectedFirstRSAnnotationsAfterInFlightScaleUp: map[string]string{
				deploymentutil.DesiredReplicasAnnotation: "120",
				deploymentutil.MaxReplicasAnnotation:     "140",
				deploymentutil.RevisionAnnotation:        "1",
			},
			expectedSecondRSAnnotationsAfterInFlightScaleUp: map[string]string{
				deploymentutil.DesiredReplicasAnnotation: "120",
				deploymentutil.MaxReplicasAnnotation:     "140",
				deploymentutil.RevisionAnnotation:        "2",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, test.enableDeploymentReplicaSetTerminatingReplicas)

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			closeFn, rm, dc, informers, c := dcSetup(ctx, t)
			defer closeFn()

			name := "test-proportional-scaling"
			ns := framework.CreateNamespaceOrDie(c, name, t)
			defer framework.DeleteNamespaceOrDie(c, ns, t)

			// 启动 informer 和 controller。
			stopControllers := runControllersAndInformers(t, rm, dc, informers)
			defer stopControllers()

			deploymentName := "deployment"
			replicas := int32(100)
			maxSurge := int32(20)
			tester := &deploymentTester{t: t, c: c, deployment: newDeployment(deploymentName, ns.Name, replicas)}
			tester.deployment.Spec.Strategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt32(maxSurge))
			tester.deployment.Spec.Strategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(0))
			tester.deployment.Spec.Template.Spec.NodeName = "fake-node"
			tester.deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(300))

			var err error
			tester.deployment, err = c.AppsV1().Deployments(ns.Name).Create(context.TODO(), tester.deployment, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create deployment %q: %v", deploymentName, err)
			}

			// 一边标记 Pod ready，一边等待 Deployment 完成。
			if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
				t.Fatal(err)
			}
			// 启动新 rollout 前记录当前 ReplicaSet。
			firstRS, err := tester.expectNewReplicaSet()
			if err != nil {
				t.Fatal(err)
			}

			// 终止一部分副本。
			err = tester.removeRSPods(ctx, firstRS, int(test.terminatingReplicasFirstRS), false, 300)
			if err != nil {
				t.Fatal(err)
			}
			// 一边标记 Pod ready，一边等待 Deployment 完成。
			if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
				t.Fatal(err)
			}

			// 触发新的 rollout。
			tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
				update.Spec.Template.Spec.Containers[0].Env = append(update.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "deploy2", Value: "true"})
			})
			if err != nil {
				t.Fatalf("failed updating deployment %q: %v", deploymentName, err)
			}
			expectedReplicasDuringNewRollout := test.expectedFirstRSReplicasDuringNewRollout + test.expectedSecondRSReplicasDuringNewRollout
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicasDuringNewRollout, test.expectedSecondRSReplicasDuringNewRollout, test.expectedFirstRSReplicasDuringNewRollout, test.expectedFirstRSReplicasDuringNewRollout, test.expectedSecondRSReplicasDuringNewRollout, test.expectedTerminatingReplicasDuringNewRollout); err != nil {
				t.Fatal(err)
			}

			// 验证新 rollout 创建了新的 ReplicaSet。
			secondRS, err := tester.expectNewReplicaSet()
			if err != nil {
				t.Fatal(err)
			}

			// 再终止一部分副本。
			err = tester.removeRSPods(ctx, secondRS, int(test.terminatingReplicasSecondRS), false, 300)
			if err != nil {
				t.Fatal(err)
			}
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicasDuringNewRollout, test.expectedSecondRSReplicasDuringNewRollout, test.expectedFirstRSReplicasDuringNewRollout, test.expectedFirstRSReplicasDuringNewRollout, test.expectedSecondRSReplicasDuringNewRollout, test.expectedTerminatingReplicasDuringInFlightScaleUp); err != nil {
				t.Fatal(err)
			}

			// 在 rollout 过程中扩容。
			tester.deployment, err = tester.updateDeployment(func(update *apps.Deployment) {
				update.Spec.Replicas = ptr.To[int32](120)
			})
			if err != nil {
				t.Fatalf("failed to update/scale deployment %q: %v", deploymentName, err)
			}
			expectedReplicasDuringInFlightScaleUp := test.expectedFirstRSReplicasAfterInFlightScaleUp + test.expectedSecondRSReplicasAfterInFlightScaleUp
			expectedSurgeReplicas := expectedReplicasDuringInFlightScaleUp - test.expectedFirstRSReplicasDuringNewRollout
			if err = tester.waitForDeploymentStatusReplicasFields(ctx, expectedReplicasDuringInFlightScaleUp, test.expectedSecondRSReplicasAfterInFlightScaleUp, test.expectedFirstRSReplicasDuringNewRollout, test.expectedFirstRSReplicasDuringNewRollout, expectedSurgeReplicas, test.expectedTerminatingReplicasDuringInFlightScaleUp); err != nil {
				t.Fatal(err)
			}

			// 检查所有 ReplicaSet 的 Pod 数和 annotation。
			firstRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), firstRS.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get replicaset %q: %v", firstRS.Name, err)
			}
			if *(firstRS.Spec.Replicas) != test.expectedFirstRSReplicasAfterInFlightScaleUp {
				t.Fatalf("unexpected first RS .spec.replicas: expect %d, got %d", test.expectedFirstRSReplicasAfterInFlightScaleUp, *(firstRS.Spec.Replicas))
			}
			if !reflect.DeepEqual(test.expectedFirstRSAnnotationsAfterInFlightScaleUp, firstRS.Annotations) {
				t.Fatalf("unexpected %q replica set annotations: %s", firstRS.Name, cmp.Diff(test.expectedFirstRSAnnotationsAfterInFlightScaleUp, firstRS.Annotations))
			}

			secondRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), secondRS.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get replicaset %q: %v", secondRS.Name, err)
			}
			if *(secondRS.Spec.Replicas) != test.expectedSecondRSReplicasAfterInFlightScaleUp {
				t.Fatalf("unexpected second RS .spec.replicas: expect %d, got %d", test.expectedSecondRSReplicasAfterInFlightScaleUp, *(secondRS.Spec.Replicas))
			}
			if !reflect.DeepEqual(test.expectedSecondRSAnnotationsAfterInFlightScaleUp, secondRS.Annotations) {
				t.Fatalf("unexpected %q replica set annotations: %s", secondRS.Name, cmp.Diff(test.expectedSecondRSAnnotationsAfterInFlightScaleUp, secondRS.Annotations))
			}

			// 一边标记 Pod ready 并清理已终止 Pod，一边等待 Deployment 完成。
			if err := tester.waitForDeploymentCompleteAndMarkPodsReadyAndRemoveTerminated(ctx); err != nil {
				t.Fatal(err)
			}

			// 最终所有 ReplicaSet 的 annotation 都应更新到最新。
			rss := []*apps.ReplicaSet{firstRS, secondRS}
			for idx, curRS := range rss {
				curRS, err = c.AppsV1().ReplicaSets(ns.Name).Get(context.TODO(), curRS.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("failed to get replicaset when checking desired replicas annotation: %v", err)
				}
				expectedFinalAnnotations := map[string]string{
					deploymentutil.DesiredReplicasAnnotation: "120",
					deploymentutil.MaxReplicasAnnotation:     "140",
					deploymentutil.RevisionAnnotation:        fmt.Sprintf("%d", idx+1),
				}
				if !reflect.DeepEqual(expectedFinalAnnotations, curRS.Annotations) {
					t.Fatalf("unexpected %q replica set annotations: %s", curRS.Name, cmp.Diff(expectedFinalAnnotations, curRS.Annotations))
				}
			}
		})
	}
}
