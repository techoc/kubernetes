/*
Copyright 2016 The Kubernetes Authors.

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

package statefulset

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/utils/lru"
	"k8s.io/utils/ptr"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/pkg/controller/statefulset/metrics"
	"k8s.io/kubernetes/pkg/features"
	testingclock "k8s.io/utils/clock/testing"
)

// invariantFunc 定义测试用的校验函数：在每个控制器操作后验证 StatefulSet、Pod 与 PVC 的不变量。
type invariantFunc func(set *apps.StatefulSet, om *fakeObjectManager) error

// setupController 用给定的 fake 客户端构造 StatefulSet 控制器及其 fake 依赖。
// 返回对象管理器、状态更新器与控制器接口，供用例直接驱动协调循环。
func setupController(client clientset.Interface) (*fakeObjectManager, *fakeStatefulSetStatusUpdater, StatefulSetControlInterface) {
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	om := newFakeObjectManager(informerFactory)
	spc := NewStatefulPodControlFromManager(om, &noopRecorder{})
	ssu := newFakeStatefulSetStatusUpdater(informerFactory.Apps().V1().StatefulSets())
	ssc := NewDefaultStatefulSetControl(spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()))

	// 这里没有启动 informer：测试直接操作本地缓存索引，也不需要等待客户端状态同步。
	// 若启动 informer，反而可能因客户端对象未被更新而意外删除缓存中的对象，破坏测试。
	//
	// TODO: 更好的做法或许是重写所有测试，让它们操作客户端并显式同步以保证状态一致，
	// 或者改用不依赖本地缓存的 fake 客户端。

	// 客户端已带有初始 StatefulSet，因此必须手动把它们写入本地 setsIndexer 缓存。
	if sets, err := client.AppsV1().StatefulSets("").List(context.TODO(), metav1.ListOptions{}); err != nil {
		panic(err)
	} else {
		for _, set := range sets.Items {
			if err := om.setsIndexer.Update(&set); err != nil {
				panic(err)
			}
		}
	}

	return om, ssu, ssc
}

// burst 将 StatefulSet 的 Pod 管理策略设为并行，用于覆盖并行（burst）语义的测试。
func burst(set *apps.StatefulSet) *apps.StatefulSet {
	set.Spec.PodManagementPolicy = apps.ParallelPodManagement
	return set
}

// setMinReadySeconds 设置 StatefulSet 的 MinReadySeconds，用于验证就绪等待时间的行为。
func setMinReadySeconds(set *apps.StatefulSet, minReadySeconds int32) *apps.StatefulSet {
	set.Spec.MinReadySeconds = minReadySeconds
	return set
}

// runTestOverPVCRetentionPolicies 在全部 PVC 保留策略组合上运行给定子测试。
// 先以指定名称执行一次，再针对每种策略（含未设置策略）以策略描述为名运行。
func runTestOverPVCRetentionPolicies(t *testing.T, testName string, testFn func(*testing.T, *apps.StatefulSetPersistentVolumeClaimRetentionPolicy)) {
	t.Run(testName, func(t *testing.T) {
		testFn(t, &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		})
	})
	for _, policy := range []*apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		},
		// 覆盖未显式设置任何保留策略的情形。
		nil,
	} {
		subtestName := pvcDeletePolicyString(policy)
		if testName != "" {
			subtestName = fmt.Sprintf("%s/%s", testName, subtestName)
		}
		t.Run(subtestName, func(t *testing.T) {
			testFn(t, policy)
		})
	}
}

// pvcDeletePolicyString 把 PVC 保留策略组合映射为简短可读的子测试名称。
func pvcDeletePolicyString(policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) string {
	if policy == nil {
		return "nullPolicy"
	}
	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		return "Retain"
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		return "SetDeleteOnly"
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		return "ScaleDownOnly"
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		return "Delete"
	}
	return "invalid"
}

// TestStatefulSetControl 在多种 PVC 保留策略下，按单调与并行两种模式运行控制器的核心行为用例。
// 每个用例都会附带相应的不变量校验函数。
func TestStatefulSetControl(t *testing.T) {
	simpleSetFn := func() *apps.StatefulSet { return newStatefulSet(3) }
	largeSetFn := func() *apps.StatefulSet { return newStatefulSet(5) }

	testCases := []struct {
		fn  func(*testing.T, *apps.StatefulSet, invariantFunc)
		obj func() *apps.StatefulSet
	}{
		{CreatePods, simpleSetFn},
		{ScalesUp, simpleSetFn},
		{ScalesDown, simpleSetFn},
		{ReplacesPods, largeSetFn},
		{RecreatesFailedPod, simpleSetFn},
		{RecreatesSucceededPod, simpleSetFn},
		{RecreatesFailedPodWithDeleteFailure, simpleSetFn},
		{RecreatesSucceededPodWithDeleteFailure, simpleSetFn},
		{CreatePodFailure, simpleSetFn},
		{UpdatePodFailure, simpleSetFn},
		{UpdateSetStatusFailure, simpleSetFn},
		{NewRevisionDeletePodFailure, simpleSetFn},
		{RecreatesPVCForPendingPod, simpleSetFn},
	}

	for _, testCase := range testCases {
		fnName := runtime.FuncForPC(reflect.ValueOf(testCase.fn).Pointer()).Name()
		if i := strings.LastIndex(fnName, "."); i != -1 {
			fnName = fnName[i+1:]
		}
		testObj := testCase.obj
		testFn := testCase.fn
		runTestOverPVCRetentionPolicies(
			t,
			fmt.Sprintf("%s/Monotonic", fnName),
			func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
				set := testObj()
				set.Spec.PersistentVolumeClaimRetentionPolicy = policy
				testFn(t, set, assertMonotonicInvariants)
			},
		)
		runTestOverPVCRetentionPolicies(
			t,
			fmt.Sprintf("%s/Burst", fnName),
			func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
				set := burst(testObj())
				set.Spec.PersistentVolumeClaimRetentionPolicy = policy
				testFn(t, set, assertBurstInvariants)
			},
		)
	}
}

// CreatePods 验证控制器能从零创建出指定数量的 Pod。
// 同时校验状态字段正确，且每个 Pod 的序号标签与名称中的序号一致。
func CreatePods(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale statefulset to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set ReadyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set UpdatedReplicas correctly")
	}

	// 校验所有 Pod 都带有正确的序号标签。
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if len(pods) != 3 {
		t.Errorf("Expected 3 pods, got %d", len(pods))
	}
	for _, pod := range pods {
		podIndexFromLabel, exists := pod.Labels[apps.PodIndexLabel]
		if !exists {
			t.Errorf("Missing pod index label: %s", apps.PodIndexLabel)
			continue
		}
		podIndexFromName := strconv.Itoa(getOrdinal(pod))
		if podIndexFromLabel != podIndexFromName {
			t.Errorf("Pod index label value (%s) does not match pod index in pod name (%s)", podIndexFromLabel, podIndexFromName)
		}
	}
}

// ScalesUp 验证控制器在副本数增加时正确扩容并更新状态。
func ScalesUp(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	*set.Spec.Replicas = 4
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to scale StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 4 {
		t.Error("Failed to scale statefulset to 4 replicas")
	}
	if set.Status.ReadyReplicas != 4 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 4 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

// ScalesDown 验证控制器在副本数降为零时正确缩容并更新状态。
func ScalesDown(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	*set.Spec.Replicas = 0
	if err := scaleDownStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale StatefulSet : %s", err)
	}

	// 校验缩容后 StatefulSet 的状态已更新。
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 0 {
		t.Error("Failed to scale statefulset to 0 replicas")
	}
	if set.Status.ReadyReplicas != 0 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 0 {
		t.Error("Failed to set updatedReplicas correctly")
	}
}

// ReplacesPods 验证被删除的 Pod 会被控制器按序重建。
// 这里间隔删除多个 Pod，以覆盖重建过程中的顺序约束。
func ReplacesPods(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 5 {
		t.Error("Failed to scale statefulset to 5 replicas")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	for _, pod := range pods {
		podClaims := getPersistentVolumeClaims(set, pod)
		for _, claim := range claims {
			if _, found := podClaims[claim.Name]; found {
				if hasOwnerRef(claim, pod) {
					t.Errorf("Unexpected ownerRef on %s", claim.Name)
				}
			}
		}
	}
	sort.Sort(ascendingOrdinal(pods))
	om.podsIndexer.Delete(pods[0])
	om.podsIndexer.Delete(pods[2])
	om.podsIndexer.Delete(pods[4])
	for i := 0; i < 5; i += 2 {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			t.Errorf("Failed to update StatefulSet : %s", err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}
		if pods, err = om.setPodRunning(set, i); err != nil {
			t.Error(err)
		}
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			t.Errorf("Failed to update StatefulSet : %s", err)
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}
		if _, err = om.setPodReadyCondition(set, i, true, time.Now()); err != nil {
			t.Error(err)
		}
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Failed to update StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if e, a := int32(5), set.Status.Replicas; e != a {
		t.Errorf("Expected to scale to %d, got %d", e, a)
	}
}

// recreatesPod 验证处于终止阶段的 Pod 会被删除并重建。
// 通过 terminalPhase 指定终止状态，并可注入删除失败以验证失败路径。
func recreatesPod(t *testing.T, set *apps.StatefulSet, invariants invariantFunc, terminalPhase v1.PodPhase, testDeletePodFailure bool) {
	client := fake.NewSimpleClientset()
	om, _, ssc := setupController(client)
	expectedNumOfDeleteRequests := 0
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if om.deletePodTracker.requests != expectedNumOfDeleteRequests {
		t.Errorf("Found unexpected number of delete calls, got %v, expected 1", om.deletePodTracker.requests)
	}
	if pods, err = om.podsLister.Pods(set.Namespace).List(selector); err != nil {
		t.Error(err)
	}

	terminalPodOrdinal := -1
	for i, pod := range pods {
		// 至少设为 Pending，以表示 Pod 创建已被控制器确认。
		newPhase := v1.PodPending
		if i == 0 {
			// 将第一个 Pod 设为终止状态，作为待重建的目标。
			newPhase = terminalPhase
			terminalPodOrdinal = getOrdinal(pod)
		}
		pod.Status.Phase = newPhase
		if err = om.podsIndexer.Update(pod); err != nil {
			t.Error(err)
		}
	}
	if pods, err = om.podsLister.Pods(set.Namespace).List(selector); err != nil {
		t.Error(err)
	}
	if testDeletePodFailure {
		// 注入 Pod 删除失败，验证错误会向上传播且不破坏不变量。
		om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)
		expectedNumOfDeleteRequests++
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); !isOrHasInternalError(err) {
			t.Errorf("StatefulSetControl did not return InternalError, found %s", err)
		}
		if err := invariants(set, om); err != nil {
			t.Error(err)
		}
		if om.deletePodTracker.requests != expectedNumOfDeleteRequests {
			t.Errorf("Found unexpected number of delete calls, got %v, expected %v", om.deletePodTracker.requests, expectedNumOfDeleteRequests)
		}
		if pods, err = om.podsLister.Pods(set.Namespace).List(selector); err != nil {
			t.Error(err)
		}
	}

	// 期望控制器发起一次 Pod 删除。
	expectedNumOfDeleteRequests++
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if om.deletePodTracker.requests != expectedNumOfDeleteRequests {
		t.Errorf("Found unexpected number of delete calls, got %v, expected %v", om.deletePodTracker.requests, expectedNumOfDeleteRequests)
	}
	if pods, err = om.podsLister.Pods(set.Namespace).List(selector); err != nil {
		t.Error(err)
	}

	// 期望不再有额外的删除请求，而是触发新 Pod 的创建。
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if om.deletePodTracker.requests != expectedNumOfDeleteRequests {
		t.Errorf("Found unexpected number of delete calls, got %v, expected %v", om.deletePodTracker.requests, expectedNumOfDeleteRequests)
	}
	if pods, err = om.podsLister.Pods(set.Namespace).List(selector); err != nil {
		t.Error(err)
	}
	recreatedPod := findPodByOrdinal(pods, terminalPodOrdinal)
	// 新建的 Pod 应处于无阶段状态，与终止中的旧 Pod 区分开。
	if recreatedPod == nil || isCreated(recreatedPod) {
		t.Error("StatefulSet did not recreate failed Pod")
	}
	expectedNumberOfCreateRequests := 2
	if monotonic := !allowsBurst(set); !monotonic {
		expectedNumberOfCreateRequests = int(*set.Spec.Replicas + 1)
	}
	if om.createPodTracker.requests != expectedNumberOfCreateRequests {
		t.Errorf("Found unexpected number of create calls, got %v, expected %v", om.deletePodTracker.requests, expectedNumberOfCreateRequests)
	}

}

// RecreatesFailedPod 验证失败的 Pod 会被重建。
func RecreatesFailedPod(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodFailed, false)
}

// RecreatesSucceededPod 验证成功完成的 Pod 会被重建。
func RecreatesSucceededPod(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodSucceeded, false)
}

// RecreatesFailedPodWithDeleteFailure 验证删除失败后失败 Pod 最终仍能被重建。
func RecreatesFailedPodWithDeleteFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodFailed, true)
}

// RecreatesSucceededPodWithDeleteFailure 验证删除失败后成功 Pod 最终仍能被重建。
func RecreatesSucceededPodWithDeleteFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	recreatesPod(t, set, invariants, v1.PodSucceeded, true)
}

// CreatePodFailure 验证创建 Pod 失败时控制器返回错误，并在重试后恢复正常扩容。
func CreatePodFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)
	om.SetCreateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); !isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError, found %s", err)
	}
	// 重新获取 StatefulSet，使 set.Status 反映上次操作，供下一次扩容使用。
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to updatedReplicas correctly")
	}
}

// UpdatePodFailure 验证更新 Pod 身份失败时控制器返回 InternalError。
func UpdatePodFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)
	om.SetUpdateStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)

	// 先成功完成一轮循环，确保 Pod 都已存在，再注入更新错误。
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas correctly")
	}

	// 修改某个 Pod 的身份，使控制器在后续循环中尝试更新它。
	pods, err := om.podsLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Error listing pods: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("Expected 3 pods, got %d", len(pods))
	}
	sort.Sort(ascendingOrdinal(pods))
	pods[0].Name = "goo-0"
	om.podsIndexer.Update(pods[0])

	// 此时更新应因注入的错误而失败。
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); !isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError, found %s", err)
	}
}

// UpdateSetStatusFailure 验证更新 StatefulSet 状态失败时的错误传播与重试行为。
func UpdateSetStatusFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, ssu, ssc := setupController(client)
	ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); !isOrHasInternalError(err) {
		t.Errorf("StatefulSetControl did not return InternalError, found %s", err)
	}
	// 重新获取 StatefulSet，使 set.Status 反映上次操作，供下一次扩容使用。
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set readyReplicas to 3")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set updatedReplicas to 3")
	}
}

// NewRevisionDeletePodFailure 验证新版本滚动更新中删除 Pod 失败时的处理。
// 删除失败不应推进 CurrentReplicas；删除成功后才更新计数。
func NewRevisionDeletePodFailure(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)
	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale StatefulSet to 3 replicas")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}

	// 修改镜像触发新版本生成。
	updateSet := set.DeepCopy()
	updateSet.Spec.Template.Spec.Containers[0].Image = "nginx-new"
	if err := om.setsIndexer.Update(updateSet); err != nil {
		t.Error("Failed to update StatefulSet")
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}

	// 注入删除失败，验证当前版本计数不会推进。
	om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 0)
	_, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err == nil {
		t.Error("Expected err in update StatefulSet when deleting a pod")
	}

	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	if set.Status.CurrentReplicas != 3 {
		t.Fatalf("Failed pod deletion should not update CurrentReplicas: want 3, got %d", set.Status.CurrentReplicas)
	}
	if set.Status.CurrentRevision == set.Status.UpdateRevision {
		t.Error("Failed to create new revision")
	}

	// 取消错误注入，验证删除成功后版本计数正常推进。
	om.SetDeleteStatefulPodError(nil, 0)
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("Unexpected err in update StatefulSet: %v", err)
	}
	if status.CurrentReplicas != 2 {
		t.Fatalf("Pod deletion should update CurrentReplicas: want 2, got %d", status.CurrentReplicas)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
}

// emptyInvariants 是空的不变量校验函数，用于不需要额外校验的场景。
func emptyInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	return nil
}

// TestStatefulSetControlWithStartOrdinal 验证起始序号非零时控制器创建 Pod 的序号正确。
func TestStatefulSetControlWithStartOrdinal(t *testing.T) {
	simpleSetFn := func() *apps.StatefulSet {
		statefulSet := newStatefulSet(3)
		statefulSet.Spec.Ordinals = &apps.StatefulSetOrdinals{Start: int32(2)}
		return statefulSet
	}

	testCases := []struct {
		fn  func(*testing.T, *apps.StatefulSet, invariantFunc)
		obj func() *apps.StatefulSet
	}{
		{CreatesPodsWithStartOrdinal, simpleSetFn},
	}

	for _, testCase := range testCases {
		testObj := testCase.obj
		testFn := testCase.fn

		set := testObj()
		testFn(t, set, emptyInvariants)
	}
}

// CreatesPodsWithStartOrdinal 验证从指定起始序号开始创建 Pod，且序号连续递增。
func CreatesPodsWithStartOrdinal(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)

	if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	var err error
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != 3 {
		t.Error("Failed to scale statefulset to 3 replicas")
	}
	if set.Status.ReadyReplicas != 3 {
		t.Error("Failed to set ReadyReplicas correctly")
	}
	if set.Status.UpdatedReplicas != 3 {
		t.Error("Failed to set UpdatedReplicas correctly")
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	sort.Sort(ascendingOrdinal(pods))
	for i, pod := range pods {
		expectedOrdinal := 2 + i
		actualPodOrdinal := getOrdinal(pod)
		if actualPodOrdinal != expectedOrdinal {
			t.Errorf("Expected pod ordinal %d. Got %d", expectedOrdinal, actualPodOrdinal)
		}
	}
}

// RecreatesPVCForPendingPod 验证 Pending Pod 缺失的 PVC 会被控制器重新创建。
func RecreatesPVCForPendingPod(t *testing.T, set *apps.StatefulSet, invariants invariantFunc) {
	client := fake.NewSimpleClientset()
	om, _, ssc := setupController(client)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	for _, claim := range getPersistentVolumeClaims(set, pods[0]) {
		om.claimsIndexer.Delete(&claim)
	}
	pods[0].Status.Phase = v1.PodPending
	om.podsIndexer.Update(pods[0])
	if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		t.Errorf("Error updating StatefulSet %s", err)
	}
	// 通过不变量校验确认每个 Pod 的 PVC 都不缺失。
	if err := invariants(set, om); err != nil {
		t.Error(err)
	}
	_, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
}

// TestStatefulSetControlScaleDownDeleteError 验证缩容时删除 Pod 失败后的重试与最终成功。
func TestStatefulSetControlScaleDownDeleteError(t *testing.T) {
	runTestOverPVCRetentionPolicies(
		t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
			set := newStatefulSet(3)
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			invariants := assertMonotonicInvariants
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)

			if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
				t.Errorf("Failed to turn up StatefulSet : %s", err)
			}
			var err error
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			*set.Spec.Replicas = 0
			om.SetDeleteStatefulPodError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			if err := scaleDownStatefulSetControl(set, ssc, om, invariants); !isOrHasInternalError(err) {
				t.Errorf("StatefulSetControl failed to throw error on delete %s", err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			if err := scaleDownStatefulSetControl(set, ssc, om, invariants); err != nil {
				t.Errorf("Failed to turn down StatefulSet %s", err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			if set.Status.Replicas != 0 {
				t.Error("Failed to scale statefulset to 0 replicas")
			}
			if set.Status.ReadyReplicas != 0 {
				t.Error("Failed to set readyReplicas to 0")
			}
			if set.Status.UpdatedReplicas != 0 {
				t.Error("Failed to set updatedReplicas to 0")
			}
		})
}

// TestStatefulSetControl_getSetRevisions 验证 getStatefulSetRevisions 的版本创建、查找与回滚行为。
// 覆盖初次创建、更新创建新版本、避免重复创建以及回滚到历史版本等场景。
func TestStatefulSetControl_getSetRevisions(t *testing.T) {
	updateRevision := func(cr *apps.ControllerRevision, revision int64) *apps.ControllerRevision {
		clone := cr.DeepCopy()
		clone.Revision = revision
		return clone
	}

	runTestOverPVCRetentionPolicies(
		t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
			set := newStatefulSet(3)
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			set.Status.CollisionCount = new(int32)
			rev0 := newRevisionOrDie(set, 1)
			set1 := set.DeepCopy()
			set1.Spec.Template.Spec.Containers[0].Image = "foo"
			set1.Status.CurrentRevision = rev0.Name
			set1.Status.CollisionCount = new(int32)
			rev1 := newRevisionOrDie(set1, 2)
			set2 := set1.DeepCopy()
			set2.Spec.Template.Labels["new"] = "label"
			set2.Status.CurrentRevision = rev0.Name
			set2.Status.CollisionCount = new(int32)
			rev2 := newRevisionOrDie(set2, 3)
			tests := []struct {
				name            string
				existing        []*apps.ControllerRevision
				set             *apps.StatefulSet
				expectedCount   int
				expectedCurrent *apps.ControllerRevision
				expectedUpdate  *apps.ControllerRevision
				err             bool
			}{
				{
					name:            "creates initial revision",
					existing:        nil,
					set:             set,
					expectedCount:   1,
					expectedCurrent: rev0,
					expectedUpdate:  rev0,
					err:             false,
				},
				{
					name:            "creates revision on update",
					existing:        []*apps.ControllerRevision{rev0},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must not recreate a new revision of same set",
					existing:        []*apps.ControllerRevision{rev0, rev1},
					set:             set1,
					expectedCount:   2,
					expectedCurrent: rev0,
					expectedUpdate:  rev1,
					err:             false,
				},
				{
					name:            "must rollback to a previous revision",
					existing:        []*apps.ControllerRevision{rev0, rev1, rev2},
					set:             set1,
					expectedCount:   3,
					expectedCurrent: rev0,
					expectedUpdate:  updateRevision(rev1, 4),
					err:             false,
				},
			}
			for _, test := range tests {
				client := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
				spc := NewStatefulPodControlFromManager(newFakeObjectManager(informerFactory), &noopRecorder{})
				ssu := newFakeStatefulSetStatusUpdater(informerFactory.Apps().V1().StatefulSets())
				ssc := defaultStatefulSetControl{spc, ssu, history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions()), lru.New(maxRevisionEqualityCacheEntries)}

				stop := make(chan struct{})
				defer close(stop)
				informerFactory.Start(stop)
				cache.WaitForCacheSync(
					stop,
					informerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
					informerFactory.Core().V1().Pods().Informer().HasSynced,
					informerFactory.Apps().V1().ControllerRevisions().Informer().HasSynced,
				)
				test.set.Status.CollisionCount = new(int32)
				for i := range test.existing {
					ssc.controllerHistory.CreateControllerRevision(test.set, test.existing[i], test.set.Status.CollisionCount)
				}
				revisions, err := ssc.ListRevisions(test.set)
				if err != nil {
					t.Fatal(err)
				}
				current, update, _, err := ssc.getStatefulSetRevisions(test.set, revisions)
				if err != nil {
					t.Fatalf("error getting statefulset revisions:%v", err)
				}
				revisions, err = ssc.ListRevisions(test.set)
				if err != nil {
					t.Fatal(err)
				}
				if len(revisions) != test.expectedCount {
					t.Errorf("%s: want %d revisions got %d", test.name, test.expectedCount, len(revisions))
				}
				if test.err {
					t.Errorf("%s: expected error", test.name)
				}
				if !test.err && !history.EqualRevision(current, test.expectedCurrent) {
					t.Errorf("%s: for current want %v got %v", test.name, test.expectedCurrent, current)
				}
				if !test.err && !history.EqualRevision(update, test.expectedUpdate) {
					t.Errorf("%s: for update want %v got %v", test.name, test.expectedUpdate, update)
				}
				if !test.err && test.expectedCurrent != nil && current != nil && test.expectedCurrent.Revision != current.Revision {
					t.Errorf("%s: for current revision want %d got %d", test.name, test.expectedCurrent.Revision, current.Revision)
				}
				if !test.err && test.expectedUpdate != nil && update != nil && test.expectedUpdate.Revision != update.Revision {
					t.Errorf("%s: for update revision want %d got %d", test.name, test.expectedUpdate.Revision, update.Revision)
				}
			}
		})
}

// setupPodManagementPolicy 设置 StatefulSet 的 Pod 管理策略，便于按策略驱动同一测试流程。
func setupPodManagementPolicy(podManagementPolicy apps.PodManagementPolicyType, set *apps.StatefulSet) *apps.StatefulSet {
	set.Spec.PodManagementPolicy = podManagementPolicy
	return set
}

// TestStatefulSetControlRollingUpdateWithMaxUnavailable 验证滚动更新时 maxUnavailable 的并发控制。
// 分别在并行与有序策略下，检查可同时不可用 Pod 的数量与顺序约束。
func TestStatefulSetControlRollingUpdateWithMaxUnavailable(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailableStatefulSet, true)

	simpleParallelVerificationFn := func(
		set *apps.StatefulSet,
		spc *fakeObjectManager,
		ssc StatefulSetControlInterface,
		pods []*v1.Pod,
		totalPods int,
		selector labels.Selector,
		now time.Time,
	) []*v1.Pod {
		// 并行模式下两个 Pod 被同时删除，因此会同时创建两个新 Pod。
		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 4/5, got pods %v", len(pods))
		}

		// Pod 4 就绪后，可以开始更新 Pod 3。
		spc.setPodRunning(set, 4)
		originalPods, _ := spc.setPodReadyCondition(set, 4, true, now)
		sort.Sort(ascendingOrdinal(originalPods))
		if _, err := ssc.UpdateStatefulSet(context.TODO(), set, originalPods, now); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(pods))
		// 此时应只缺少 Pod 3，其余 Pod 0、1、2、4、5 都在。
		if !reflect.DeepEqual(pods, append(originalPods[:3], originalPods[4:]...)) {
			t.Fatalf("Expected pods %v, got pods %v", append(originalPods[:3], originalPods[4:]...), pods)
		}

		// 创建新的 Pod 3。
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, now); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		if len(pods) != totalPods {
			t.Fatalf("Expected create pods 2/3, got pods %v", pods)
		}

		return pods
	}
	simpleOrderedVerificationFn := func(
		set *apps.StatefulSet,
		spc *fakeObjectManager,
		ssc StatefulSetControlInterface,
		pods []*v1.Pod,
		totalPods int,
		selector labels.Selector,
		now time.Time,
	) []*v1.Pod {
		// OrderedReady 模式下一次只创建一个 Pod。
		if len(pods) != 5 {
			t.Fatalf("Expected create pods 5, got pods %v", len(pods))
		}
		spc.setPodRunning(set, 4)
		spc.setPodReadyCondition(set, 4, true, now) // nolint:errcheck

		// 创建新的 Pod 4，同样受一次只创建一个 Pod 的限制。
		if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, now); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		// 此时只有 4 个 Pod，Pod 5 尚未创建。
		if len(pods) != totalPods-1 {
			t.Fatalf("Expected create pods %d, got pods %v", totalPods-1, len(pods))
		}

		return pods
	}
	testCases := []struct {
		policyType apps.PodManagementPolicyType
		verifyFn   func(
			set *apps.StatefulSet,
			spc *fakeObjectManager,
			ssc StatefulSetControlInterface,
			pods []*v1.Pod,
			totalPods int,
			selector labels.Selector,
			now time.Time,
		) []*v1.Pod
	}{
		{apps.OrderedReadyPodManagement, simpleOrderedVerificationFn},
		{apps.ParallelPodManagement, simpleParallelVerificationFn},
	}
	for _, tc := range testCases {
		// 配置 StatefulSet 控制器与滚动更新参数。
		var totalPods int32 = 6
		var partition int32 = 3
		var maxUnavailable = intstr.FromInt32(2)
		set := setupPodManagementPolicy(tc.policyType, newStatefulSet(totalPods))
		set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
			Type: apps.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
				return &apps.RollingUpdateStatefulSetStrategy{
					Partition:      &partition,
					MaxUnavailable: &maxUnavailable,
				}
			}(),
		}

		fakeClock := testingclock.NewFakeClock(time.Date(2026, time.March, 12, 12, 0, 0, 0, time.UTC))
		client := fake.NewSimpleClientset()
		spc, _, ssc := setupController(client)
		if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants, nil); err != nil {
			t.Fatal(err)
		}
		set, err := spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatal(err)
		}

		// 修改镜像以触发滚动更新。
		set.Spec.Template.Spec.Containers[0].Image = "foo"

		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatal(err)
		}
		originalPods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(originalPods))

		// maxUnavailable 为 2，因此先删除 Pod 4 和 5。
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, originalPods, fakeClock.Now()); err != nil {
			t.Fatal(err)
		}
		pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		sort.Sort(ascendingOrdinal(pods))

		// 此时应只剩下 Pod 0、1、2、3。
		if !reflect.DeepEqual(pods, originalPods[:4]) {
			t.Fatalf("Expected pods %v, got pods %v", originalPods[:4], pods)
		}

		// 创建被删除的新 Pod。
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, fakeClock.Now()); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}

		tc.verifyFn(set, spc, ssc, pods, int(totalPods), selector, fakeClock.Now())

		// Pod 3、4、5 已就绪，不应再更新其他 Pod。
		spc.setPodRunning(set, 3)
		spc.setPodRunning(set, 5)
		spc.setPodReadyCondition(set, 5, true, fakeClock.Now()) // nolint:errcheck
		originalPods, _ = spc.setPodReadyCondition(set, 3, true, fakeClock.Now())
		sort.Sort(ascendingOrdinal(originalPods))
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, originalPods, fakeClock.Now()); err != nil {
			t.Fatal(err)
		}
		pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(ascendingOrdinal(pods))
		if !reflect.DeepEqual(pods, originalPods) {
			t.Fatalf("Expected pods %v, got pods %v", originalPods, pods)
		}
	}

}

// setupForInvariant 准备一个用于不变量校验的 StatefulSet 环境。
// 创建 6 个 Pod，并设置 partition 为 3、maxUnavailable 为 2。
func setupForInvariant(t *testing.T) (*apps.StatefulSet, *fakeObjectManager, StatefulSetControlInterface, intstr.IntOrString, int32) {
	var totalPods int32 = 6
	set := newStatefulSet(totalPods)
	// 更新序号大于等于 3 的 Pod（即 3、4、5）。
	var partition int32 = 3
	var maxUnavailable = intstr.FromInt32(2)
	set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
		Type: apps.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
			return &apps.RollingUpdateStatefulSetStrategy{
				Partition:      &partition,
				MaxUnavailable: &maxUnavailable,
			}
		}(),
	}

	client := fake.NewSimpleClientset()
	spc, _, ssc := setupController(client)
	if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants, nil); err != nil {
		t.Fatal(err)
	}
	set, err := spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatal(err)
	}

	return set, spc, ssc, maxUnavailable, totalPods
}

// TestStatefulSetControlRollingUpdateWithMaxUnavailableInOrderedModeVerifyInvariant 验证在已有不可用 Pod 时，
// 有序模式下的滚动更新是否遵守 maxUnavailable 预算。
func TestStatefulSetControlRollingUpdateWithMaxUnavailableInOrderedModeVerifyInvariant(t *testing.T) {
	// 逐个让 StatefulSet 中的 Pod 不可用，并验证设置了 maxUnavailable 时滚动更新不会继续推进。
	// 这里显式列出各种组合而非简单循环，是为了便于后续扩展更多参数场景。
	testCases := []struct {
		ordinalOfPodToTerminate []int
	}{

		{[]int{}},
		{[]int{5}},
		{[]int{3}},
		{[]int{4}},
		{[]int{5, 4}},
		{[]int{5, 3}},
		{[]int{4, 3}},
		{[]int{5, 4, 3}},
		// 注意该序号实际小于 partition（3）。
		{[]int{2}},
		// 注意该序号实际小于 partition（3）。
		{[]int{1}},
	}
	for _, tc := range testCases {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailableStatefulSet, true)
		set, spc, ssc, maxUnavailable, totalPods := setupForInvariant(t)
		t.Run(fmt.Sprintf("terminating pod at ordinal %d", tc.ordinalOfPodToTerminate), func(t *testing.T) {
			status := apps.StatefulSetStatus{Replicas: int32(totalPods)}
			updateRevision := &apps.ControllerRevision{}

			for i := 0; i < len(tc.ordinalOfPodToTerminate); i++ {
				// 先确保至少有一个 Pod 不可用，再尝试更新。
				_, err := spc.addTerminatingPod(set, tc.ordinalOfPodToTerminate[i])
				if err != nil {
					t.Fatal(err)
				}
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatal(err)
			}

			originalPods, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}

			sort.Sort(ascendingOrdinal(originalPods))

			// 开始滚动更新。
			set.Spec.Template.Spec.Containers[0].Image = "foo"

			// 尝试更新 StatefulSet；主流程仅在特性门控启用时调用该函数。
			if _, err = updateStatefulSetAfterInvariantEstablished(context.TODO(), ssc.(*defaultStatefulSetControl), set, originalPods, updateRevision, status, time.Now()); err != nil {
				t.Fatal(err)
			}
			pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}

			sort.Sort(ascendingOrdinal(pods))

			expecteddPodsToBeDeleted := maxUnavailable.IntValue() - len(tc.ordinalOfPodToTerminate)
			if expecteddPodsToBeDeleted < 0 {
				expecteddPodsToBeDeleted = 0
			}

			expectedPodsAfterUpdate := int(totalPods) - expecteddPodsToBeDeleted

			if len(pods) != expectedPodsAfterUpdate {
				t.Errorf("Expected pods %v, got pods %v", expectedPodsAfterUpdate, len(pods))
			}

		})
	}
}

// TestStatefulSetControlRollingUpdateWithMaxUnavailableAndUnavailableStalePod 是 issue 137409 的回归测试。
// 当启用 MaxUnavailableStatefulSet 时，并行策略下处于旧版本且已不可用（如崩溃循环）的 Pod 也必须被删除，以触发版本更新。
// 原缺陷会导致死锁：Pod 因版本错误而永远不可用，控制器又因其不可用而认为预算耗尽，从而永远不更新它。
// 对于 OrderedReady 策略，保持原有的严格行为以维持向后兼容。
func TestStatefulSetControlRollingUpdateWithMaxUnavailableAndUnavailableStalePod(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailableStatefulSet, true)

	testCases := []struct {
		name                      string
		podManagementPolicy       apps.PodManagementPolicyType
		replicas                  int32
		partition                 int32
		maxUnavailable            intstr.IntOrString
		minReadySeconds           int32
		unavailableOrdinals       []int
		makeUnavailable           func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error
		expectedRemainingOrdinals []int
	}{
		{
			name:                "Parallel/single replica: unavailable pod on old revision must be updated",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            1,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{0},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{},
		},
		{
			name:                "Parallel/3 replicas, highest-ordinal pod unavailable on old revision",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1},
		},
		{
			name:                "Parallel/5 replicas, 2 pods unavailable on old revision",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(2),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "OrderedReady/single replica: unavailable pod on old revision blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            1,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{0},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0},
		},
		{
			name:                "OrderedReady/3 replicas, highest-ordinal pod unavailable on old revision blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "OrderedReady/5 replicas, 2 pods unavailable on old revision blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(2),
			unavailableOrdinals: []int{4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3},
		},
		{
			name:                "Parallel/3 replicas, highest-ordinal pod terminating on old revision: budget consumed, next pod must NOT be deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodTerminated(set, ordinal)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "OrderedReady/3 replicas, highest-ordinal pod terminating on old revision blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodTerminated(set, ordinal)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "Parallel/5 replicas maxUnavailable=3, pod-4 terminating + pod-3 crashlooping: only 1 more pods should be deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				if ordinal == 3 {
					_, err := spc.setPodReadyCondition(set, ordinal, false, now)
					return err
				}
				_, err := spc.setPodTerminated(set, ordinal)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 4},
		},
		{
			name:                "Parallel/3 replicas, highest-ordinal pod ready but within minReadySeconds on old revision: pod gets deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			minReadySeconds:     30,
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodAvailable(set, ordinal, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1},
		},
		{
			name:                "OrderedReady/3 replicas, highest-ordinal pod ready but within minReadySeconds on old revision blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            3,
			maxUnavailable:      intstr.FromInt32(1),
			minReadySeconds:     30,
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodAvailable(set, ordinal, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "Parallel/5 replicas, pod-3 unavailable + pod-4 terminating on old revision, only 1 more pods should be deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				if ordinal == 3 {
					_, err := spc.setPodReadyCondition(set, ordinal, false, now)
					return err
				}
				_, err := spc.setPodTerminated(set, ordinal)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 4},
		},
		{
			name:                "Parallel/5 replicas, pod-3 terminating + pod-4 unavailable on old revision, only 1 more pods should be deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				if ordinal == 3 {
					_, err := spc.setPodTerminated(set, ordinal)
					return err
				}
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 3},
		},
		{
			name:                "OrderedReady/5 replicas, 1 pod unavailable(#3) + 1 pod terminating(#4) on old revision",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				if ordinal == 3 {
					_, err := spc.setPodReadyCondition(set, ordinal, false, now)
					return err
				}
				_, err := spc.setPodTerminated(set, ordinal)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 4},
		},
		{
			name:                "OrderedReady/5 replicas, 1 pod terminating(#3) + 1 pod unavailable(#4) on old revision",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				if ordinal == 3 {
					_, err := spc.setPodTerminated(set, ordinal)
					return err
				}
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3},
		},
		{
			name:                "Parallel/5 replicas partition=2: unavailable pod in update range (#4) gets deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			partition:           2,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3},
		},
		{
			name:                "Parallel/5 replicas partition=2: unavailable pod at partition boundary (#2) gets deleted, highest stale pod (#4) is unaffected",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			partition:           2,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{2},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 3, 4},
		},
		{
			name:                "Parallel/5 replicas partition=2: unavailable pod below partition (#1) consumes budget, no update",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			partition:           2,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{1},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3, 4},
		},
		{
			name:                "OrderedReady/5 replicas partition=2: unavailable pod in update range (#4) blocks update",
			podManagementPolicy: apps.OrderedReadyPodManagement,
			replicas:            5,
			partition:           2,
			maxUnavailable:      intstr.FromInt32(1),
			unavailableOrdinals: []int{4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3, 4},
		},
		{
			name:                "Parallel/5 replicas partition=3 maxUnavailable=2: both update-range pods unavailable, both get deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			partition:           3,
			maxUnavailable:      intstr.FromInt32(2),
			unavailableOrdinals: []int{3, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2},
		},
		{
			name:                "Parallel/5 replicas partition=3 maxUnavailable=2: pod below partition (#1) + pod in range (#4) unavailable, one pod deleted",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			partition:           3,
			maxUnavailable:      intstr.FromInt32(2),
			unavailableOrdinals: []int{1, 4},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{0, 1, 2, 3},
		},
		{
			name:                "Parallel/4 replicas maxUnavailable=2: lower-ordinal unavailable pods get deleted, higher-ordinal good pods must stay",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            4,
			maxUnavailable:      intstr.FromInt32(2),
			unavailableOrdinals: []int{0, 1},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{2, 3},
		},
		{
			name:                "Parallel/5 replicas maxUnavailable=3: 2 lower-ordinal unavailable pods deleted first, 1 good pod from the top",
			podManagementPolicy: apps.ParallelPodManagement,
			replicas:            5,
			maxUnavailable:      intstr.FromInt32(3),
			unavailableOrdinals: []int{0, 1},
			makeUnavailable: func(spc *fakeObjectManager, set *apps.StatefulSet, ordinal int, now time.Time) error {
				_, err := spc.setPodReadyCondition(set, ordinal, false, now)
				return err
			},
			expectedRemainingOrdinals: []int{2, 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(time.Date(2026, time.March, 12, 12, 0, 0, 0, time.UTC))
			set := newStatefulSet(tc.replicas)
			set.Spec.PodManagementPolicy = tc.podManagementPolicy
			set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{
					Partition:      &tc.partition,
					MaxUnavailable: &tc.maxUnavailable,
				},
			}
			client := fake.NewSimpleClientset()
			spc, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants, fakeClock); err != nil {
				t.Fatal(err)
			}
			set, err := spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatal(err)
			}

			// IsPodAvailable 会检查 lastTransitionTime，因此依赖 minReadySeconds 的用例需要让 Pod 上报更早的时间。
			if tc.minReadySeconds > 0 {
				pastTime := fakeClock.Now().Add(-time.Duration(tc.minReadySeconds+60) * time.Second)
				for ordinal := 0; ordinal < int(tc.replicas); ordinal++ {
					if _, err := spc.setPodAvailable(set, ordinal, pastTime); err != nil {
						t.Fatalf("setPodAvailable(%d) pre-minReadySeconds setup: %v", ordinal, err)
					}
				}
			}

			for _, ordinal := range tc.unavailableOrdinals {
				if err := tc.makeUnavailable(spc, set, ordinal, fakeClock.Now()); err != nil {
					t.Fatalf("makeUnavailable(%d): %v", ordinal, err)
				}
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatal(err)
			}
			pods, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}
			sort.Sort(ascendingOrdinal(pods))

			status := apps.StatefulSetStatus{Replicas: tc.replicas}
			updateRevision := &apps.ControllerRevision{}
			// 仅为不变量校验设置 minReadySeconds，避免影响更新前的状态计算。
			set.Spec.MinReadySeconds = tc.minReadySeconds
			if _, err = updateStatefulSetAfterInvariantEstablished(
				t.Context(),
				ssc.(*defaultStatefulSetControl),
				set,
				pods,
				updateRevision,
				status,
				fakeClock.Now(),
			); err != nil {
				t.Fatal(err)
			}

			podsAfter, err := spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}
			if len(podsAfter) != len(tc.expectedRemainingOrdinals) {
				t.Errorf("got %d pods remaining, want %d", len(podsAfter), len(tc.expectedRemainingOrdinals))
			}
			for _, p := range podsAfter {
				ordinal := getOrdinal(p)
				if p.DeletionTimestamp != nil && !slices.Contains(tc.unavailableOrdinals, ordinal) {
					t.Errorf("pod %s, has DeletionTimestamp set unexpectedly", p.Name)
				}
			}
			remainingOrdinals := make([]int, 0, len(podsAfter))
			for _, p := range podsAfter {
				remainingOrdinals = append(remainingOrdinals, getOrdinal(p))
			}
			sort.Ints(remainingOrdinals)
			if !slices.Equal(remainingOrdinals, tc.expectedRemainingOrdinals) {
				t.Errorf("got remaining pod ordinals %v, want %v", remainingOrdinals, tc.expectedRemainingOrdinals)
			}
		})
	}
}

// TestStatefulSetControlRollingUpdate 验证 RollingUpdate 策略下的镜像更新。
// 覆盖单调与并行模式，以及扩容、缩容与更新组合场景。
func TestStatefulSetControlRollingUpdate(t *testing.T) {
	tests := []struct {
		name       string
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
		update     func(set *apps.StatefulSet) *apps.StatefulSet
		validate   func(set *apps.StatefulSet, pods []*v1.Pod) error
	}{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := test.initial()
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set = test.update(set)
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validate(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
		})
	}
}

// TestStatefulSetControlOnDeleteUpdate 验证 OnDelete 策略下镜像不会自动更新。
// 只有当 Pod 被删除重建后才会使用新版本，并校验 PVC 的所有权引用正确。
func TestStatefulSetControlOnDeleteUpdate(t *testing.T) {
	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image

	tests := []struct {
		name            string
		invariants      func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial         func() *apps.StatefulSet
		update          func(set *apps.StatefulSet) *apps.StatefulSet
		validateUpdate  func(set *apps.StatefulSet, pods []*v1.Pod) error
		validateRestart func(set *apps.StatefulSet, pods []*v1.Pod) error
	}{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 3 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 3 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRestart: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		for _, test := range tests {
			set := test.initial()
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{Type: apps.OnDeleteStatefulSetStrategyType}
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set = test.update(set)
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}

			// Pod 可能在更新过程中被删除；清理仍带有 Pod ownerRef 的 PVC。
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			for _, claim := range claims {
				for _, ref := range claim.GetOwnerReferences() {
					if strings.HasPrefix(ref.Name, "foo-") {
						om.claimsIndexer.Delete(claim)
						break
					}
				}
			}

			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validateUpdate(set, pods); err != nil {
				for i := range pods {
					t.Log(pods[i].Name)
				}
				t.Fatalf("%s: %s", test.name, err)

			}
			claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			for _, claim := range claims {
				for _, ref := range claim.GetOwnerReferences() {
					if strings.HasPrefix(ref.Name, "foo-") {
						t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
					}
				}
			}

			replicas := *set.Spec.Replicas
			*set.Spec.Replicas = 0
			if err := scaleDownStatefulSetControl(set, ssc, om, test.invariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			*set.Spec.Replicas = replicas

			claims, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			for _, claim := range claims {
				for _, ref := range claim.GetOwnerReferences() {
					if strings.HasPrefix(ref.Name, "foo-") {
						t.Fatalf("Unexpected pod reference on %s: %v", claim.Name, claim.GetOwnerReferences())
					}
				}
			}

			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validateRestart(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if set.Status.CurrentRevision != set.Status.UpdateRevision {
				t.Fatalf("%s: after OnDelete update completes, CurrentRevision %s should equal UpdateRevision %s",
					test.name, set.Status.CurrentRevision, set.Status.UpdateRevision)
			}
		}
	})
}

// TestStatefulSetControlRollingUpdateWithPartition 验证带 partition 的滚动更新。
// 序号小于 partition 的 Pod 保持旧版本，大于等于 partition 的 Pod 更新到新版本。
func TestStatefulSetControlRollingUpdateWithPartition(t *testing.T) {
	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image

	tests := []struct {
		name       string
		partition  int32
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
		update     func(set *apps.StatefulSet) *apps.StatefulSet
		validate   func(set *apps.StatefulSet, pods []*v1.Pod) error
	}{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			partition:  2,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			partition:  2,
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			partition:  2,
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			partition:  2,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if i < 2 && pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
					if i >= 2 && pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		for _, test := range tests {
			set := test.initial()
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: func() *apps.RollingUpdateStatefulSetStrategy {
					return &apps.RollingUpdateStatefulSetStrategy{Partition: &test.partition}
				}(),
			}
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set = test.update(set)
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validate(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
		}
	})
}

// TestStatefulSetControlRecreateUpdatePreservesPVC 验证 Recreate 策略更新时 PVC 会被保留。
// 通过比较更新前后 PVC 的 UID，确认 PVC 未被删除重建。
func TestStatefulSetControlRecreateUpdatePreservesPVC(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetRecreateStrategy, true)
	runTestOverPVCRetentionPolicies(
		t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
			set := newStatefulSet(3)
			set.Spec.UpdateStrategy.Type = apps.RecreateStatefulSetStrategyType
			set.Spec.PersistentVolumeClaimRetentionPolicy = policy
			invariants := assertMonotonicInvariants
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)

			if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
				t.Errorf("Failed to turn up StatefulSet : %s", err)
			}
			var err error
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("Error getting label selector: %v", err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("Error listing pods: %v", err)
			}
			pvcsBefore := make(map[string]v1.PersistentVolumeClaim)
			for _, pod := range pods {
				for _, claim := range getPersistentVolumeClaims(set, pod) {
					pvc, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
					if err != nil {
						t.Fatalf("Error getting claim %s: %v", claim.Name, err)
					}
					pvcsBefore[pvc.Name] = *pvc
				}
			}

			set.Spec.Template.Spec.Containers[0].Image = "foo"
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("Failed to update StatefulSet: %s", err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}

			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("Error listing pods after update: %v", err)
			}
			for _, pod := range pods {
				for _, claim := range getPersistentVolumeClaims(set, pod) {
					pvc, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
					if err != nil {
						t.Fatalf("Error getting claim %s after update: %v", claim.Name, err)
					}
					before, ok := pvcsBefore[pvc.Name]
					if !ok {
						t.Fatalf("PVC %s not found in pre-update claims", pvc.Name)
					}
					if pvc.UID != before.UID {
						t.Fatalf("PVC %s UID changed: before=%v after=%v", pvc.Name, before.UID, pvc.UID)
					}
				}
			}
		})
}

// TestStatefulSetControlRecreateUpdate 验证 Recreate 策略下的更新行为。
// 覆盖正常更新、无变化时的空操作、卡住或失败 Pod 的处理，以及扩缩容场景。
func TestStatefulSetControlRecreateUpdate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetRecreateStrategy, true)
	type testcase struct {
		name       string
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
		preUpdate  func(set *apps.StatefulSet, om *fakeObjectManager, t *testing.T) error
		update     func(set *apps.StatefulSet) *apps.StatefulSet
		validate   func(set *apps.StatefulSet, pods []*v1.Pod) error
	}

	tests := []testcase{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "noOp when revisions match",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				if len(pods) != 3 {
					return fmt.Errorf("expected 3 pods, got %d", len(pods))
				}
				if set.Status.CurrentRevision != set.Status.UpdateRevision {
					return fmt.Errorf("expected CurrentRevision == UpdateRevision, got %s != %s",
						set.Status.CurrentRevision, set.Status.UpdateRevision)
				}
				for _, pod := range pods {
					if !isRunningAndReady(pod) {
						return fmt.Errorf("expected pod %s to be Running and Ready", pod.Name)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic stuck pod is deleted",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			preUpdate: func(set *apps.StatefulSet, om *fakeObjectManager, t *testing.T) error {
				selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
				if err != nil {
					return err
				}
				pods, err := om.podsLister.Pods(set.Namespace).List(selector)
				if err != nil {
					return err
				}
				if err := om.podsIndexer.Delete(findPodByOrdinal(pods, 1)); err != nil {
					return err
				}
				if err := om.podsIndexer.Delete(findPodByOrdinal(pods, 2)); err != nil {
					return err
				}
				if _, err := om.setPodPending(set, 0); err != nil {
					return err
				}
				return nil
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic failed pod is deleted",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			preUpdate: func(set *apps.StatefulSet, om *fakeObjectManager, t *testing.T) error {
				selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
				if err != nil {
					return err
				}
				pods, err := om.podsLister.Pods(set.Namespace).List(selector)
				if err != nil {
					return err
				}
				if err := om.podsIndexer.Delete(findPodByOrdinal(pods, 1)); err != nil {
					return err
				}
				if err := om.podsIndexer.Delete(findPodByOrdinal(pods, 2)); err != nil {
					return err
				}
				pod := findPodByOrdinal(pods, 0)
				pod.Status.Phase = v1.PodFailed
				return om.podsIndexer.Update(pod)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := test.initial()
			set.Spec.UpdateStrategy.Type = apps.RecreateStatefulSetStrategyType
			set.Spec.UpdateStrategy.RollingUpdate = nil

			client := fake.NewSimpleClientset(set)

			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}

			if test.preUpdate != nil {
				if err := test.preUpdate(set, om, t); err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
			}

			set = test.update(set)

			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}

			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validate(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}

		})
	}
}

// TestStatefulSetHonorRevisionHistoryLimit 验证控制器遵循 revisionHistoryLimit 配置。
// 反复更新镜像时，历史版本数量应始终受限于配置值。
func TestStatefulSetHonorRevisionHistoryLimit(t *testing.T) {
	runTestOverPVCRetentionPolicies(t, "", func(t *testing.T, policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy) {
		invariants := assertMonotonicInvariants
		set := newStatefulSet(3)
		set.Spec.PersistentVolumeClaimRetentionPolicy = policy
		client := fake.NewSimpleClientset(set)
		om, ssu, ssc := setupController(client)

		if err := scaleUpStatefulSetControl(set, ssc, om, invariants, nil); err != nil {
			t.Errorf("Failed to turn up StatefulSet : %s", err)
		}
		var err error
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("Error getting updated StatefulSet: %v", err)
		}

		for i := 0; i < int(*set.Spec.RevisionHistoryLimit)+5; i++ {
			set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
			ssu.SetUpdateStatefulSetStatusError(apierrors.NewInternalError(errors.New("API server failed")), 2)
			updateStatefulSetControl(set, ssc, om, assertUpdateInvariants)
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("Error getting updated StatefulSet: %v", err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("Error listing revisions: %v", err)
			}
			// 额外的两个版本是 currentRevision 与 updateRevision。
			// 它们被视为存活版本，truncateHistory 只会清理非存活版本。
			if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
				t.Fatalf("%s: %d greater than limit %d", "", len(revisions), *set.Spec.RevisionHistoryLimit)
			}
		}
	})
}

// TestStatefulSetControlLimitsHistory 验证不同 revisionHistoryLimit 配置下的历史版本数量限制。
func TestStatefulSetControlLimitsHistory(t *testing.T) {
	tests := []struct {
		name       string
		invariants func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial    func() *apps.StatefulSet
	}{
		{
			name:       "monotonic update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
		},
		{
			name:       "burst update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
		},
		{
			name:       "zero revisionHistoryLimit",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				sts := newStatefulSet(3)
				sts.Spec.RevisionHistoryLimit = ptr.To(int32(0))
				return sts
			},
		},
		{
			name:       "negative revisionHistoryLimit",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				sts := newStatefulSet(3)
				sts.Spec.RevisionHistoryLimit = ptr.To(int32(-2))
				return sts
			},
		},
		{
			name:       "positive revisionHistoryLimit",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				sts := newStatefulSet(3)
				sts.Spec.RevisionHistoryLimit = ptr.To(int32(5))
				return sts
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := test.initial()
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			for i := 0; i < 10; i++ {
				set.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo-%d", i)
				if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
				selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
				if err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
				pods, err := om.podsLister.Pods(set.Namespace).List(selector)
				if err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
				set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
				if err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
				_, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
				if err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}
				revisions, err := ssc.ListRevisions(set)
				if err != nil {
					t.Fatalf("%s: %s", test.name, err)
				}

				if *set.Spec.RevisionHistoryLimit < 0 {
					// revisionHistoryLimit 为负数时不裁剪历史版本，版本数量会持续增长。
					continue
				}

				if len(revisions) > int(*set.Spec.RevisionHistoryLimit)+2 {
					t.Fatalf("%s: %d greater than limit %d", test.name, len(revisions), *set.Spec.RevisionHistoryLimit)
				}
			}
		})
	}
}

// TestStatefulSetControlRollback 验证回滚到历史版本后 Pod 会恢复原有镜像。
// 覆盖单调与并行模式，以及扩容、缩容组合场景。
func TestStatefulSetControlRollback(t *testing.T) {
	originalImage := newStatefulSet(3).Spec.Template.Spec.Containers[0].Image
	tests := []struct {
		name             string
		invariants       func(set *apps.StatefulSet, om *fakeObjectManager) error
		initial          func() *apps.StatefulSet
		update           func(set *apps.StatefulSet) *apps.StatefulSet
		validateUpdate   func(set *apps.StatefulSet, pods []*v1.Pod) error
		validateRollback func(set *apps.StatefulSet, pods []*v1.Pod) error
	}{
		{
			name:       "monotonic image update",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale up",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(3)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "monotonic image update and scale down",
			invariants: assertMonotonicInvariants,
			initial: func() *apps.StatefulSet {
				return newStatefulSet(5)
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale up",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(3))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 5
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
		{
			name:       "burst image update and scale down",
			invariants: assertBurstInvariants,
			initial: func() *apps.StatefulSet {
				return burst(newStatefulSet(5))
			},
			update: func(set *apps.StatefulSet) *apps.StatefulSet {
				*set.Spec.Replicas = 3
				set.Spec.Template.Spec.Containers[0].Image = "foo"
				return set
			},
			validateUpdate: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != "foo" {
						return fmt.Errorf("want pod %s image foo found %s", pods[i].Name, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
			validateRollback: func(set *apps.StatefulSet, pods []*v1.Pod) error {
				sort.Sort(ascendingOrdinal(pods))
				for i := range pods {
					if pods[i].Spec.Containers[0].Image != originalImage {
						return fmt.Errorf("want pod %s image %s found %s", pods[i].Name, originalImage, pods[i].Spec.Containers[0].Image)
					}
				}
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := test.initial()
			client := fake.NewSimpleClientset(set)
			om, _, ssc := setupController(client)
			if err := scaleUpStatefulSetControl(set, ssc, om, test.invariants, nil); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err := om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set = test.update(set)
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validateUpdate(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			revisions, err := ssc.ListRevisions(set)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			history.SortControllerRevisions(revisions)
			set, err = ApplyRevision(set, revisions[0])
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := updateStatefulSetControl(set, ssc, om, assertUpdateInvariants); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
			if err := test.validateRollback(set, pods); err != nil {
				t.Fatalf("%s: %s", test.name, err)
			}
		})
	}
}

// TestStatefulSetAvailability 验证启用 minReadySeconds 时可用副本数的计算。
// 只有处于就绪状态超过规定时间的 Pod 才计入可用副本。
func TestStatefulSetAvailability(t *testing.T) {
	tests := []struct {
		name                   string
		inputSTS               *apps.StatefulSet
		expectedActiveReplicas int32
		readyDuration          time.Duration
	}{
		{
			name:                   "replicas running for required time, when minReadySeconds is enabled",
			inputSTS:               setMinReadySeconds(newStatefulSet(1), int32(3600)),
			readyDuration:          -120 * time.Minute,
			expectedActiveReplicas: int32(1),
		},
		{
			name:                   "replicas not running for required time, when minReadySeconds is enabled",
			inputSTS:               setMinReadySeconds(newStatefulSet(1), int32(3600)),
			readyDuration:          -30 * time.Minute,
			expectedActiveReplicas: int32(0),
		},
	}
	for _, test := range tests {
		set := test.inputSTS
		client := fake.NewSimpleClientset(set)
		spc, _, ssc := setupController(client)
		if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants, nil); err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		_, err = spc.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		set, err = spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		pods, err := spc.setPodAvailable(set, 0, time.Now().Add(test.readyDuration))
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
		if err != nil {
			t.Fatalf("%s: %s", test.name, err)
		}
		if status.AvailableReplicas != test.expectedActiveReplicas {
			t.Fatalf("expected %d active replicas got %d", test.expectedActiveReplicas, status.AvailableReplicas)
		}
	}
}

// TestStatefulSetStatusUpdate 验证同步错误与状态更新错误的传播优先级。
// 当两类错误同时发生时，同步错误优先返回。
func TestStatefulSetStatusUpdate(t *testing.T) {
	var (
		syncErr   = fmt.Errorf("sync error")
		statusErr = fmt.Errorf("status error")
	)

	testCases := []struct {
		desc string

		hasSyncErr   bool
		hasStatusErr bool

		expectedErr error
	}{
		{
			desc:         "no error",
			hasSyncErr:   false,
			hasStatusErr: false,
			expectedErr:  nil,
		},
		{
			desc:         "sync error",
			hasSyncErr:   true,
			hasStatusErr: false,
			expectedErr:  syncErr,
		},
		{
			desc:         "status error",
			hasSyncErr:   false,
			hasStatusErr: true,
			expectedErr:  statusErr,
		},
		{
			desc:         "sync and status error",
			hasSyncErr:   true,
			hasStatusErr: true,
			expectedErr:  syncErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			set := newStatefulSet(3)
			client := fake.NewSimpleClientset(set)
			om, ssu, ssc := setupController(client)

			if tc.hasSyncErr {
				om.SetCreateStatefulPodError(syncErr, 0)
			}
			if tc.hasStatusErr {
				ssu.SetUpdateStatefulSetStatusError(statusErr, 0)
			}

			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Error(err)
			}
			pods, err := om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Error(err)
			}
			_, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
			if ssu.updateStatusTracker.requests != 1 {
				t.Errorf("Did not update status")
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("Expected error: %v, got: %v", tc.expectedErr, err)
			}
		})
	}
}

// requestTracker 记录 fake 依赖的调用次数，并可按需注入错误与统计并发请求。
type requestTracker struct {
	sync.Mutex
	requests int
	err      error
	after    int

	// 以下字段用于并行请求统计，修改时需与 trackParallelRequests 保持一致。
	parallelLock                sync.Mutex
	shouldTrackParallelRequests bool
	parallelRequests            int
	maxParallelRequests         int
	parallelRequestDelay        time.Duration
}

// trackParallelRequests 统计同时进行的请求数量，并记录观测到的最大并发数。
// 通过 TryLock 区分首个并发请求与后续并发请求，并可注入延时制造冲突。
func (rt *requestTracker) trackParallelRequests() {
	if !rt.shouldTrackParallelRequests {
		// 未显式开启时不统计并行请求。
		return
	}
	if rt.parallelLock.TryLock() {
		// 成功加锁说明是唯一请求或首个并发请求。
		// 初始化新一轮并发请求计数。
		rt.parallelRequests = 1
	} else {
		// 锁被其他请求持有。
		// 等待锁释放后递增并发计数。
		rt.parallelLock.Lock()
		rt.parallelRequests++
	}
	defer rt.parallelLock.Unlock()
	// 记录观测到的最大并发冲突数。
	if rt.maxParallelRequests < rt.parallelRequests {
		rt.maxParallelRequests = rt.parallelRequests
	}
	// 注入延时以提高发生并发冲突的概率。
	if rt.parallelRequestDelay > 0 {
		time.Sleep(rt.parallelRequestDelay)
	}
}

// incWithOptionalError 递增请求计数，并在达到指定次数后返回一次注入的错误。
// 错误只生效一次，返回前会重置以确保后续调用正常。
func (rt *requestTracker) incWithOptionalError() error {
	rt.Lock()
	defer rt.Unlock()
	rt.requests++
	if rt.err != nil && rt.requests >= rt.after {
		// 重置注入错误，保证它只影响一次调用。
		defer func() {
			rt.err = nil
			rt.after = 0
		}()
		return rt.err
	}
	return nil
}

// newRequestTracker 以指定的初始计数、错误和触发次数构造 requestTracker。
func newRequestTracker(requests int, err error, after int) requestTracker {
	return requestTracker{
		requests: requests,
		err:      err,
		after:    after,
	}
}

// fakeObjectManager 实现 StatefulPodControlObjectManager。
// 它基于 informer 的本地索引器操作对象，并记录各类请求的调用情况。
type fakeObjectManager struct {
	podsLister       corelisters.PodLister
	claimsLister     corelisters.PersistentVolumeClaimLister
	setsLister       appslisters.StatefulSetLister
	podsIndexer      cache.Indexer
	claimsIndexer    cache.Indexer
	setsIndexer      cache.Indexer
	revisionsIndexer cache.Indexer
	createPodTracker requestTracker
	updatePodTracker requestTracker
	deletePodTracker requestTracker
}

// newFakeObjectManager 基于 informerFactory 构造 fakeObjectManager。
func newFakeObjectManager(informerFactory informers.SharedInformerFactory) *fakeObjectManager {
	podInformer := informerFactory.Core().V1().Pods()
	claimInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	setInformer := informerFactory.Apps().V1().StatefulSets()
	revisionInformer := informerFactory.Apps().V1().ControllerRevisions()

	return &fakeObjectManager{
		podInformer.Lister(),
		claimInformer.Lister(),
		setInformer.Lister(),
		podInformer.Informer().GetIndexer(),
		claimInformer.Informer().GetIndexer(),
		setInformer.Informer().GetIndexer(),
		revisionInformer.Informer().GetIndexer(),
		newRequestTracker(0, nil, 0),
		newRequestTracker(0, nil, 0),
		newRequestTracker(0, nil, 0),
	}
}

// CreatePod 将 Pod 写入本地索引器，并记录请求与注入错误。
// 同时设置 UID 以模拟真实 API 服务器创建的对象。
func (om *fakeObjectManager) CreatePod(ctx context.Context, pod *v1.Pod, ss *apps.StatefulSet) error {
	defer om.createPodTracker.trackParallelRequests()
	if err := om.createPodTracker.incWithOptionalError(); err != nil {
		return err
	}
	pod.SetUID(types.UID(pod.Name + "-uid"))
	return om.podsIndexer.Update(pod)
}

// GetPod 从本地索引器获取指定名称的 Pod。
func (om *fakeObjectManager) GetPod(namespace, podName string) (*v1.Pod, error) {
	return om.podsLister.Pods(namespace).Get(podName)
}

// UpdatePod 更新本地索引器中的 Pod，并记录请求与注入错误。
func (om *fakeObjectManager) UpdatePod(pod *v1.Pod, ss *apps.StatefulSet) error {
	defer om.updatePodTracker.trackParallelRequests()
	if err := om.updatePodTracker.incWithOptionalError(); err != nil {
		return err
	}
	return om.podsIndexer.Update(pod)
}

// DeletePod 从本地索引器中删除指定 Pod；Pod 不存在时不视为错误。
func (om *fakeObjectManager) DeletePod(pod *v1.Pod) error {
	defer om.deletePodTracker.trackParallelRequests()
	if err := om.deletePodTracker.incWithOptionalError(); err != nil {
		return err
	}
	if key, err := controller.KeyFunc(pod); err != nil {
		return err
	} else if obj, found, err := om.podsIndexer.GetByKey(key); err != nil {
		return err
	} else if found {
		return om.podsIndexer.Delete(obj)
	}
	return nil // 未找到对象，删除操作不返回错误。
}

// CreateClaim 将 PVC 写入本地索引器。
func (om *fakeObjectManager) CreateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error {
	om.claimsIndexer.Update(claim)
	return nil
}

// GetClaim 从本地索引器获取指定名称的 PVC。
func (om *fakeObjectManager) GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error) {
	return om.claimsLister.PersistentVolumeClaims(namespace).Get(claimName)
}

// UpdateClaim 校验 PVC 的 ownerRef 完整性后更新本地索引器。
func (om *fakeObjectManager) UpdateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error {
	// 校验 ownerRef 的各字段必须填写完整。
	refs := claim.GetOwnerReferences()
	for _, ref := range refs {
		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("invalid ownerRefs: %s %v", claim.Name, refs)
		}
	}
	om.claimsIndexer.Update(claim)
	return nil
}

// SetCreateStatefulPodError 设置创建 Pod 时注入的错误与触发次数。
func (om *fakeObjectManager) SetCreateStatefulPodError(err error, after int) {
	om.createPodTracker.err = err
	om.createPodTracker.after = after
}

// SetUpdateStatefulPodError 设置更新 Pod 时注入的错误与触发次数。
func (om *fakeObjectManager) SetUpdateStatefulPodError(err error, after int) {
	om.updatePodTracker.err = err
	om.updatePodTracker.after = after
}

// SetDeleteStatefulPodError 设置删除 Pod 时注入的错误与触发次数。
func (om *fakeObjectManager) SetDeleteStatefulPodError(err error, after int) {
	om.deletePodTracker.err = err
	om.deletePodTracker.after = after
}

// findPodByOrdinal 按序号查找 Pod，返回深拷贝以避免修改缓存中的对象。
func findPodByOrdinal(pods []*v1.Pod, ordinal int) *v1.Pod {
	for _, pod := range pods {
		if getOrdinal(pod) == ordinal {
			return pod.DeepCopy()
		}
	}

	return nil
}

// setPodPending 将指定序号的 Pod 状态设为 Pending，并返回更新后的 Pod 列表。
func (om *fakeObjectManager) setPodPending(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodPending: pod ordinal %d not found", ordinal)
	}
	pod.Status.Phase = v1.PodPending
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

// setPodRunning 将指定序号的 Pod 状态设为 Running，并返回更新后的 Pod 列表。
func (om *fakeObjectManager) setPodRunning(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodRunning: pod ordinal %d not found", ordinal)
	}
	pod.Status.Phase = v1.PodRunning
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

// updatePodCondition 更新 Pod 的 Ready 条件。
// 此处复制 podutil.UpdatePodCondition 的实现，是为了能够控制注入的 LastTransitionTime。
func (om *fakeObjectManager) updatePodCondition(status *v1.PodStatus, condition *v1.PodCondition) {
	// 此处复制 podutil.UpdatePodCondition 的实现，是为了能够控制注入的 LastTransitionTime。
	conditionIndex, oldCondition := podutil.GetPodCondition(status, v1.PodReady)
	if oldCondition == nil {
		status.Conditions = append(status.Conditions, *condition)
		return
	}
	if condition.Status == oldCondition.Status {
		condition.LastTransitionTime = oldCondition.LastTransitionTime
	}
	status.Conditions[conditionIndex] = *condition
}

// setPodReadyCondition 设置指定序号 Pod 的 Ready 条件及状态转换时间。
func (om *fakeObjectManager) setPodReadyCondition(set *apps.StatefulSet, ordinal int, ready bool, lastTransitionTime time.Time) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodReadyCondition: pod ordinal %d not found", ordinal)
	}
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionFalse, LastTransitionTime: metav1.Time{Time: lastTransitionTime}}
	if ready {
		condition.Status = v1.ConditionTrue
	}
	om.updatePodCondition(&pod.Status, &condition)
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

// setPodAvailable 将指定序号 Pod 标记为 Ready，并设置状态转换时间以模拟可用状态。
func (om *fakeObjectManager) setPodAvailable(set *apps.StatefulSet, ordinal int, lastTransitionTime time.Time) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodAvailable: pod ordinal %d not found", ordinal)
	}
	_, existingCondition := podutil.GetPodCondition(&pod.Status, v1.PodReady)
	if existingCondition != nil {
		existingCondition.Status = v1.ConditionTrue
		existingCondition.LastTransitionTime = metav1.Time{Time: lastTransitionTime}
	} else {
		existingCondition = &v1.PodCondition{
			Type:               v1.PodReady,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: lastTransitionTime},
		}
		pod.Status.Conditions = append(pod.Status.Conditions, *existingCondition)
	}
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod)
	return om.podsLister.Pods(set.Namespace).List(selector)
}

// addTerminatingPod 新增一个带删除时间戳的 Pod，用于模拟正在终止的 Pod。
func (om *fakeObjectManager) addTerminatingPod(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	pod := newStatefulSetPod(set, ordinal)
	pod.SetUID(types.UID(pod.Name + "-uid")) // 与 fakeObjectManager.CreatePod 保持一致
	pod.Status.Phase = v1.PodRunning
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	fakeResourceVersion(pod)
	om.updatePodCondition(&pod.Status, &condition)
	om.podsIndexer.Update(pod)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return om.podsLister.Pods(set.Namespace).List(selector)
}

// setPodTerminated 为指定序号的 Pod 设置删除时间戳，模拟已终止状态。
func (om *fakeObjectManager) setPodTerminated(set *apps.StatefulSet, ordinal int) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	pod := findPodByOrdinal(pods, ordinal)
	if pod == nil {
		return nil, fmt.Errorf("setPodTerminated: pod ordinal %d not found", ordinal)
	}
	deleted := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &deleted
	fakeResourceVersion(pod)
	om.podsIndexer.Update(pod) //nolint:errcheck
	return om.podsLister.Pods(set.Namespace).List(selector)
}

var _ StatefulPodControlObjectManager = &fakeObjectManager{}

// fakeStatefulSetStatusUpdater 实现 StatefulSetStatusUpdaterInterface。
// 它在本地索引器中更新状态，并记录调用次数与注入错误。
type fakeStatefulSetStatusUpdater struct {
	setsLister          appslisters.StatefulSetLister
	setsIndexer         cache.Indexer
	updateStatusTracker requestTracker
}

// newFakeStatefulSetStatusUpdater 基于 StatefulSet informer 构造 fake 状态更新器。
func newFakeStatefulSetStatusUpdater(setInformer appsinformers.StatefulSetInformer) *fakeStatefulSetStatusUpdater {
	return &fakeStatefulSetStatusUpdater{
		setInformer.Lister(),
		setInformer.Informer().GetIndexer(),
		newRequestTracker(0, nil, 0),
	}
}

// UpdateStatefulSetStatus 将新状态写回本地索引器，并记录请求与注入错误。
func (ssu *fakeStatefulSetStatusUpdater) UpdateStatefulSetStatus(ctx context.Context, set *apps.StatefulSet, status *apps.StatefulSetStatus) error {
	defer ssu.updateStatusTracker.trackParallelRequests()
	if err := ssu.updateStatusTracker.incWithOptionalError(); err != nil {
		return err
	}
	set.Status = *status
	ssu.setsIndexer.Update(set)
	return nil
}

// SetUpdateStatefulSetStatusError 设置更新状态时注入的错误与触发次数。
func (ssu *fakeStatefulSetStatusUpdater) SetUpdateStatefulSetStatusError(err error, after int) {
	ssu.updateStatusTracker.err = err
	ssu.updateStatusTracker.after = after
}

var _ StatefulSetStatusUpdaterInterface = &fakeStatefulSetStatusUpdater{}

// assertMonotonicInvariants 校验有序策略下的不变量。
// 包括 Pod 按序就绪、序号连续、存储与身份匹配，以及 PVC 保留策略正确。
func assertMonotonicInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for idx := 0; idx < len(pods); idx++ {
		if idx > 0 && isRunningAndReady(pods[idx]) && !isRunningAndReady(pods[idx-1]) {
			return fmt.Errorf("successor %s is Running and Ready while %s is not", pods[idx].Name, pods[idx-1].Name)
		}

		if ord := idx + getStartOrdinal(set); getOrdinal(pods[idx]) != ord {
			return fmt.Errorf("pods %s deployed in the wrong order %d", pods[idx].Name, ord)
		}

		if !storageMatches(set, pods[idx]) {
			return fmt.Errorf("pods %s does not match the storage specification of StatefulSet %s ", pods[idx].Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pods[idx]) {
			claim, _ := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err := checkClaimInvarients(set, pods[idx], claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pods[idx]) {
			return fmt.Errorf("pods %s does not match the identity specification of StatefulSet %s ", pods[idx].Name, set.Name)
		}
	}
	return nil
}

// assertBurstInvariants 校验并行策略下的不变量。
// 不要求 Pod 按序就绪，但仍校验存储、身份与 PVC 的约束。
func assertBurstInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for _, pod := range pods {
		if !storageMatches(set, pod) {
			return fmt.Errorf("pods %s does not match the storage specification of StatefulSet %s ", pod.Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pod) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pod, claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pod) {
			return fmt.Errorf("pods %s does not match the identity specification of StatefulSet %s ",
				pod.Name,
				set.Name)
		}
	}
	return nil
}

// assertUpdateInvariants 校验更新过程中的不变量。
// 除存储、身份和 PVC 外，还校验新旧版本 Pod 的分布与状态中的版本计数一致。
func assertUpdateInvariants(set *apps.StatefulSet, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	sort.Sort(ascendingOrdinal(pods))
	for _, pod := range pods {

		if !storageMatches(set, pod) {
			return fmt.Errorf("pod %s does not match the storage specification of StatefulSet %s ", pod.Name, set.Name)
		}

		for _, claim := range getPersistentVolumeClaims(set, pod) {
			claim, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).Get(claim.Name)
			if err != nil {
				return err
			}
			if err := checkClaimInvarients(set, pod, claim); err != nil {
				return err
			}
		}

		if !identityMatches(set, pod) {
			return fmt.Errorf("pod %s does not match the identity specification of StatefulSet %s ", pod.Name, set.Name)
		}
	}
	if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
		return nil
	}
	if set.Spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType ||
		set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType {
		for i := 0; i < int(set.Status.CurrentReplicas) && i < len(pods); i++ {
			if want, got := set.Status.CurrentRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want current revision %s got %s", pods[i].Name, want, got)
			}
		}
		for i, j := len(pods)-1, 0; j < int(set.Status.UpdatedReplicas); i, j = i-1, j+1 {
			if want, got := set.Status.UpdateRevision, getPodRevision(pods[i]); want != got {
				return fmt.Errorf("pod %s want update revision %s got %s", pods[i].Name, want, got)
			}
		}
	}
	return nil
}

// checkClaimInvarients 根据 PVC 保留策略校验 PVC 的 ownerRef 是否符合预期。
// 未保留的 PVC 即使缺失也不违反不变量。
func checkClaimInvarients(set *apps.StatefulSet, pod *v1.Pod, claim *v1.PersistentVolumeClaim) error {
	policy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if set.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		policy = *set.Spec.PersistentVolumeClaimRetentionPolicy
	}
	claimShouldBeRetained := policy.WhenScaled == apps.RetainPersistentVolumeClaimRetentionPolicyType
	if claim == nil {
		if claimShouldBeRetained {
			return fmt.Errorf("claim for Pod %s was not created", pod.Name)
		}
		return nil // 不保留的 PVC 没有需要满足的不变量。
	}

	if pod.Status.Phase != v1.PodRunning || !podutil.IsPodReady(pod) {
		// Pod 尚未启动时，不期望 PVC 上已设置 ownerRef。
		return nil
	}

	const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
	const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
	switch {
	case policy.WhenScaled == retain && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for StatefulSet retain", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for StatefulSet retain", claim.Name, pod.Name)
		}
	case policy.WhenScaled == retain && policy.WhenDeleted == delete:
		if !hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s does not have owner ref on %s for StatefulSet deletion", claim.Name, set.Name)
		}
		if hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on pod %s for StatefulSet deletion", claim.Name, pod.Name)
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == retain:
		if hasOwnerRef(claim, set) {
			return fmt.Errorf("claim %s has unexpected owner ref on %s for scaledown only", claim.Name, set.Name)
		}
		if !podInOrdinalRange(pod, set) && !hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s does not have owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
		if podInOrdinalRange(pod, set) && hasOwnerRef(claim, pod) {
			return fmt.Errorf("claim %s has unexpected owner ref on condemned pod %s for scaledown delete", claim.Name, pod.Name)
		}
	case policy.WhenScaled == delete && policy.WhenDeleted == delete:
		if !podInOrdinalRange(pod, set) {
			if !hasOwnerRef(claim, pod) || hasOwnerRef(claim, set) {
				return fmt.Errorf("condemned claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		} else {
			if hasOwnerRef(claim, pod) || !hasOwnerRef(claim, set) {
				return fmt.Errorf("live claim %s has bad owner refs: %v", claim.Name, claim.GetOwnerReferences())
			}
		}
	}
	return nil
}

// fakeResourceVersion 为对象设置或递增 resourceVersion，模拟 API 服务器的版本管理。
func fakeResourceVersion(object interface{}) {
	obj, isObj := object.(metav1.Object)
	if !isObj {
		return
	}
	if version := obj.GetResourceVersion(); version == "" {
		obj.SetResourceVersion("1")
	} else if intValue, err := strconv.ParseInt(version, 10, 32); err == nil {
		obj.SetResourceVersion(strconv.FormatInt(intValue+1, 10))
	}
}

// TestParallelScale 验证并行策略下大规模扩缩容能在有限轮次内完成。
// 同时通过追踪并发请求数确认控制器具备并行处理能力。
func TestParallelScale(t *testing.T) {
	for _, tc := range []struct {
		desc                        string
		replicas                    int32
		desiredReplicas             int32
		expectedMinParallelRequests int
	}{
		{
			desc:                        "scale up from 3 to 30",
			replicas:                    3,
			desiredReplicas:             30,
			expectedMinParallelRequests: 2,
		},
		{
			desc:                        "scale down from 10 to 1",
			replicas:                    10,
			desiredReplicas:             1,
			expectedMinParallelRequests: 2,
		},

		{
			desc:                        "scale down to 0",
			replicas:                    501,
			desiredReplicas:             0,
			expectedMinParallelRequests: 10,
		},
		{
			desc:                        "scale up from 0",
			replicas:                    0,
			desiredReplicas:             1000,
			expectedMinParallelRequests: 20,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			set := burst(newStatefulSet(0))
			set.Spec.VolumeClaimTemplates[0].ObjectMeta.Labels = map[string]string{"test": "test"}
			parallelScale(t, set, tc.replicas, tc.desiredReplicas, tc.expectedMinParallelRequests, assertBurstInvariants)
		})
	}

}

// parallelScale 在并行策略下执行扩容或缩容，并校验不变量与最小并发请求数。
func parallelScale(t *testing.T, set *apps.StatefulSet, replicas, desiredReplicas int32, expectedMinParallelRequests int, invariants invariantFunc) {
	var err error
	diff := desiredReplicas - replicas

	// 控制器单批最多处理 500 个 Pod；这里将差值除以 4，给最后一批留出足够空间。
	if maxParallelRequests := min(500, math.Abs(float64(diff))/4); expectedMinParallelRequests < 2 || float64(expectedMinParallelRequests) > maxParallelRequests {
		t.Fatalf("expectedMinParallelRequests should be between 2 and %v. Batch size of the controller is expontially increasing until 500. "+
			"Got expectedMinParallelRequests %v, ", maxParallelRequests, expectedMinParallelRequests)
	}
	client := fake.NewSimpleClientset(set)
	om, _, ssc := setupController(client)
	om.createPodTracker.shouldTrackParallelRequests = true
	om.createPodTracker.parallelRequestDelay = time.Millisecond

	*set.Spec.Replicas = replicas
	if err := parallelScaleUpStatefulSetControl(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}
	if set.Status.Replicas != replicas {
		t.Errorf("want %v, got %v replicas", replicas, set.Status.Replicas)
	}

	fn := parallelScaleUpStatefulSetControl
	if diff < 0 {
		fn = parallelScaleDownStatefulSetControl
	}
	*set.Spec.Replicas = desiredReplicas
	if err := fn(set, ssc, om, invariants); err != nil {
		t.Errorf("Failed to scale StatefulSet : %s", err)
	}

	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		t.Fatalf("Error getting updated StatefulSet: %v", err)
	}

	if set.Status.Replicas != desiredReplicas {
		t.Errorf("Failed to scale statefulset to %v replicas, got %v replicas", desiredReplicas, set.Status.Replicas)
	}

	if om.createPodTracker.maxParallelRequests < expectedMinParallelRequests {
		t.Errorf("want max parallelRequests requests >= %v, got %v", expectedMinParallelRequests, om.createPodTracker.maxParallelRequests)
	}
}

// parallelScaleUpStatefulSetControl 在并行策略下反复调用控制器直到扩容完成。
// 每轮先把新 Pod 置为 Pending，再调用控制器并校验不变量。
func parallelScaleUpStatefulSetControl(set *apps.StatefulSet,
	ssc StatefulSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	// 最多执行 2 轮；每轮可处理 500 个 Pod，共 1000 个，足以覆盖所有测试用例。
	// 若需要更多轮次，说明并行处理存在问题，测试应当失败。
	maxLoops := 2
	loops := maxLoops
	for set.Status.Replicas < *set.Spec.Replicas {
		if loops < 1 {
			return fmt.Errorf("after %v loops: want %v, got replicas %v", maxLoops, *set.Spec.Replicas, set.Status.Replicas)
		}
		loops--
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))

		ordinals := []int{}
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				ordinals = append(ordinals, getOrdinal(pod))
			}
		}
		// 确保所有 Pod 都处于有效阶段。
		for _, ord := range ordinals {
			if pods, err = om.setPodPending(set, ord); err != nil {
				return err
			}
		}

		// 执行一次控制器逻辑并校验不变量。
		_, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
		if err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
	}
	return invariants(set, om)
}

// parallelScaleDownStatefulSetControl 在并行策略下反复调用控制器直到缩容完成。
func parallelScaleDownStatefulSetControl(set *apps.StatefulSet, ssc StatefulSetControlInterface, om *fakeObjectManager, invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	// 最多执行 2 轮；每轮可处理 500 个 Pod，共 1000 个，足以覆盖所有测试用例。
	// 若需要更多轮次，说明并行处理存在问题，测试应当失败。
	maxLoops := 2
	loops := maxLoops
	for set.Status.Replicas > *set.Spec.Replicas {
		if loops < 1 {
			return fmt.Errorf("after %v loops: want %v replicas, got %v", maxLoops, *set.Spec.Replicas, set.Status.Replicas)
		}
		loops--
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			return err
		}
	}

	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		return err
	}
	if err := invariants(set, om); err != nil {
		return err
	}

	return nil
}

// scaleUpStatefulSetControl 反复驱动控制器，直到所有 Pod 就绪且状态更新完成。
// 每轮随机推进一个 Pod 的状态，并在每步之后校验不变量。
func scaleUpStatefulSetControl(set *apps.StatefulSet, ssc StatefulSetControlInterface, om *fakeObjectManager,
	invariants invariantFunc, fakeClock *testingclock.FakeClock) error {
	now := func() time.Time {
		if fakeClock != nil {
			return fakeClock.Now()
		}
		return time.Now()
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	for set.Status.ReadyReplicas < *set.Spec.Replicas {
		if fakeClock != nil {
			fakeClock.Step(10 * time.Second)
		}
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))

		// 确保所有 Pod 都处于有效阶段。
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, getOrdinal(pod)); err != nil {
					return err
				}
				break
			}
		}

		// 随机选择一个 Pod 并推进其状态，模拟 Pod 陆续就绪的过程。
		if len(pods) > 0 {
			idx := int(rand.Int63n(int64(len(pods))))
			pod := pods[idx]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, getOrdinal(pod)); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReadyCondition(set, getOrdinal(pod), true, now()); err != nil {
					return err
				}
			default:
				continue
			}
		}
		// 执行一次控制器逻辑并校验不变量。
		_, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, now())
		if err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
	}
	return invariants(set, om)
}

// scaleDownStatefulSetControl 驱动控制器完成缩容，并清理已删除 Pod 对应的 PVC。
// 每轮先让控制器删除旧 Pod，再模拟终止与移除过程。
func scaleDownStatefulSetControl(set *apps.StatefulSet, ssc StatefulSetControlInterface, om *fakeObjectManager, invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}

	for set.Status.Replicas > *set.Spec.Replicas {
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		if idx := len(pods) - 1; idx >= 0 {
			if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
				return err
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			if pods, err = om.addTerminatingPod(set, getOrdinal(pods[idx])); err != nil {
				return err
			}
			if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
				return err
			}
			set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				return err
			}
			pods, err = om.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				return err
			}
			sort.Sort(ascendingOrdinal(pods))

			if len(pods) > 0 {
				om.podsIndexer.Delete(pods[len(pods)-1])
			}
		}
		if _, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}

		if err := invariants(set, om); err != nil {
			return err
		}
	}
	// 如果某些 PVC 的 ownerRef 指向已被删除的 Pod，则将这些 PVC 删除。
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	currentPods := map[string]bool{}
	for _, pod := range pods {
		currentPods[pod.Name] = true
	}
	claims, err := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for _, claim := range claims {
		claimPodName := getClaimPodName(set, claim)
		if claimPodName == "" {
			continue // 跳过与 StatefulSet Pod 无关的 PVC。
		}
		if _, found := currentPods[claimPodName]; found {
			continue // 跳过仍有关联 Pod 的 PVC。
		}
		for _, refs := range claim.GetOwnerReferences() {
			if refs.Name == claimPodName {
				om.claimsIndexer.Delete(claim)
				break
			}
		}
	}

	return invariants(set, om)
}

// updateComplete 判断 StatefulSet 的更新是否已经全部完成。
// 依据更新策略分别检查 Pod 数量、就绪数与版本分布。
func updateComplete(set *apps.StatefulSet, pods []*v1.Pod) bool {
	sort.Sort(ascendingOrdinal(pods))
	if len(pods) != int(*set.Spec.Replicas) {
		return false
	}
	if set.Status.ReadyReplicas != *set.Spec.Replicas {
		return false
	}

	switch set.Spec.UpdateStrategy.Type {
	case apps.OnDeleteStatefulSetStrategyType:
		return true
	case apps.RollingUpdateStatefulSetStrategyType:
		if set.Spec.UpdateStrategy.RollingUpdate == nil || *set.Spec.UpdateStrategy.RollingUpdate.Partition <= 0 {
			if set.Status.CurrentReplicas < *set.Spec.Replicas {
				return false
			}
			for i := range pods {
				if getPodRevision(pods[i]) != set.Status.CurrentRevision {
					return false
				}
			}
		} else {
			partition := int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
			if len(pods) < partition {
				return false
			}
			for i := partition; i < len(pods); i++ {
				if getPodRevision(pods[i]) != set.Status.UpdateRevision {
					return false
				}
			}
		}
	case apps.RecreateStatefulSetStrategyType:
		if set.Status.CurrentReplicas < *set.Spec.Replicas {
			return false
		}
		for i := range pods {
			if getPodRevision(pods[i]) != set.Status.CurrentRevision {
				return false
			}
		}
	}
	return true
}

// verifyRecreateDeleteAll 校验 Recreate 策略在删除阶段一次性删除了所有旧版本 Pod。
func verifyRecreateDeleteAll(om *fakeObjectManager, set *apps.StatefulSet, expectedDeletes, deleteRequests int) error {
	deletesInReconcile := om.deletePodTracker.requests - deleteRequests
	if deletesInReconcile != expectedDeletes {
		return fmt.Errorf("recreate: expected all %d old-revision pods to be deleted at once, got %d",
			expectedDeletes, deletesInReconcile)
	}
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	if len(pods) != 0 {
		return fmt.Errorf("recreate: expected 0 pods after delete phase, got %d", len(pods))
	}
	return nil
}

// updateStatefulSetControl 反复驱动控制器，直到更新完成并校验不变量。
// 对 Recreate 策略还会额外校验旧版本 Pod 是否被一次性全部删除。
func updateStatefulSetControl(set *apps.StatefulSet,
	ssc StatefulSetControlInterface,
	om *fakeObjectManager,
	invariants invariantFunc) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}

	podCount := len(pods)
	deleteRequests := om.deletePodTracker.requests

	if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
		return err
	}

	set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
	if err != nil {
		return err
	}

	if set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType &&
		set.Status.CurrentRevision != set.Status.UpdateRevision {
		if err := verifyRecreateDeleteAll(om, set, podCount, deleteRequests); err != nil {
			return err
		}
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	for !updateComplete(set, pods) {
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		sort.Sort(ascendingOrdinal(pods))
		initialized := false
		for _, pod := range pods {
			if pod.Status.Phase == "" {
				if pods, err = om.setPodPending(set, getOrdinal(pod)); err != nil {
					return err
				}
				break
			}
		}
		if initialized {
			continue
		}

		if len(pods) > 0 {
			idx := int(rand.Int63n(int64(len(pods))))
			pod := pods[idx]
			switch pod.Status.Phase {
			case v1.PodPending:
				if pods, err = om.setPodRunning(set, getOrdinal(pod)); err != nil {
					return err
				}
			case v1.PodRunning:
				if pods, err = om.setPodReadyCondition(set, getOrdinal(pod), true, time.Now()); err != nil {
					return err
				}
			default:
				continue
			}
		}

		if _, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now()); err != nil {
			return err
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			return err
		}
		if err := invariants(set, om); err != nil {
			return err
		}
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
	}
	return invariants(set, om)
}

// newRevisionOrDie 为 StatefulSet 构造指定版本的 ControllerRevision，失败时直接 panic。
func newRevisionOrDie(set *apps.StatefulSet, revision int64) *apps.ControllerRevision {
	rev, err := newRevision(set, revision, set.Status.CollisionCount)
	if err != nil {
		panic(err)
	}
	return rev
}

// isOrHasInternalError 判断错误本身或聚合错误中是否包含 InternalError。
func isOrHasInternalError(err error) bool {
	if err == nil {
		return false
	}
	var agg utilerrors.Aggregate
	if errors.As(err, &agg) {
		if slices.ContainsFunc(agg.Errors(), apierrors.IsInternalError) {
			return true
		}
	}
	return apierrors.IsInternalError(err)
}

// TestStatefulSetRollingUpdateRespectsMinReadySeconds 验证滚动更新逻辑将 MinReadySeconds 传给 isUnavailable()。
// 只有就绪时间超过 MinReadySeconds 的 Pod 才计入可用副本。
func TestStatefulSetRollingUpdateRespectsMinReadySeconds(t *testing.T) {
	// 该测试验证在滚动更新逻辑中向 isUnavailable() 传递 MinReadySeconds 的改动确实生效。
	set := setMinReadySeconds(newStatefulSet(2), int32(30)) // MinReadySeconds 为 30 秒
	client := fake.NewClientset(set)
	om, _, ssc := setupController(client)

	// 手动创建已运行且就绪的 Pod。
	set, _ = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)

	// 手动创建已运行且就绪的 Pod。
	for i := 0; i < 2; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now().Add(-10 * time.Second)}},
		}
		fakeResourceVersion(pod)
		if err := om.podsIndexer.Add(pod); err != nil {
			t.Fatalf("failed to add pod to indexer: %s", err)
		}
	}

	// 修改镜像以触发滚动更新。
	set.Spec.Template.Spec.Containers[0].Image = "updated-image"

	// 获取当前 Pod 列表。
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to create selector: %s", err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// 此时应阻止滚动更新：Pod 就绪时间尚未达到 30 秒。
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	// 验证 MinReadySeconds 阻止了滚动更新继续推进。
	if status.AvailableReplicas != 0 {
		t.Errorf("expected 0 available replicas due to MinReadySeconds (pods ready for <30s), got %d", status.AvailableReplicas)
	}

	// 将 Pod 的就绪时间调整为超过 MinReadySeconds。
	for i := 0; i < 2; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			// 60 秒 > 30 秒
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now().Add(-60 * time.Second)}},
		}
		fakeResourceVersion(pod)
		if err := om.podsIndexer.Update(pod); err != nil {
			t.Fatalf("failed to update pod to indexer: %s", err)
		}
	}

	// 获取更新后的 Pod 列表。
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// 再次执行更新，此时应允许继续推进。
	status, err = ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	// 此时 Pod 应被视为可用。
	if status.AvailableReplicas != 2 {
		t.Errorf("expected 2 available replicas when MinReadySeconds is satisfied (pods ready for >30s), got %d", status.AvailableReplicas)
	}
}

// TestStatefulSetScaleDownRespectsMinReadySeconds 验证缩容操作同样受 MinReadySeconds 约束。
func TestStatefulSetScaleDownRespectsMinReadySeconds(t *testing.T) {
	// 验证缩容操作也遵循 MinReadySeconds。
	// 30 秒
	set := setMinReadySeconds(newStatefulSet(3), int32(30))
	client := fake.NewClientset(set)
	om, _, ssc := setupController(client)

	// 先扩容到 3 个副本。
	fakeClock := testingclock.NewFakeClock(time.Date(2026, time.May, 11, 10, 0, 0, 0, time.UTC))
	if err := scaleUpStatefulSetControl(set, ssc, om, assertMonotonicInvariants, fakeClock); err != nil {
		t.Fatalf("failed to scale up: %s", err)
	}

	// 创建已运行且就绪、但就绪时间不足的 Pod。
	for i := 0; i < 3; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now().Add(-10 * time.Second)}}, // 10 < 30
		}
		fakeResourceVersion(pod)
		if i == 0 {
			if err := om.podsIndexer.Add(pod); err != nil {
				t.Fatalf("failed to add pod to indexer: %s", err)
			}
		} else {
			if err := om.podsIndexer.Update(pod); err != nil {
				t.Fatalf("failed to update pod to indexer: %s", err)
			}
		}
	}

	// 现在缩容到 1 个副本。
	set.Spec.Replicas = &[]int32{1}[0]

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to create selector: %s", err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// 由于 Pod 可用时间不足 30 秒，缩容应被阻止。
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	if status.AvailableReplicas != 0 {
		t.Errorf("expected 0 available replicas due to MinReadySeconds during scale down, got %d", status.AvailableReplicas)
	}

	// Pod 仍应存在，说明缩容已被阻止。
	newPods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}
	if len(newPods) != 3 {
		t.Errorf("expected 3 pods to still exist (scale down blocked), got %d", len(newPods))
	}
}

// TestStatefulSetOnDeleteStrategyIgnoresMinReadySeconds 验证 OnDelete 策略不受 MinReadySeconds 影响。
func TestStatefulSetOnDeleteStrategyIgnoresMinReadySeconds(t *testing.T) {
	// 验证 OnDelete 策略不受 MinReadySeconds 影响。
	// 30 秒
	set := setMinReadySeconds(newStatefulSet(2), int32(30))
	set.Spec.UpdateStrategy.Type = apps.OnDeleteStatefulSetStrategyType
	client := fake.NewClientset(set)
	om, _, ssc := setupController(client)

	// 先扩容。
	fakeClock := testingclock.NewFakeClock(time.Date(2026, time.May, 11, 10, 0, 0, 0, time.UTC))
	if err := scaleUpStatefulSetControl(set, ssc, om, assertMonotonicInvariants, fakeClock); err != nil {
		t.Fatalf("failed to scale up: %s", err)
	}

	// 创建已就绪但就绪时间不足 MinReadySeconds 的 Pod。
	for i := 0; i < 2; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now().Add(-10 * time.Second)}}, // 10 < 30
		}
		fakeResourceVersion(pod)
		if i == 0 {
			if err := om.podsIndexer.Add(pod); err != nil {
				t.Fatalf("failed to add pod to indexer: %s", err)
			}
		} else {
			if err := om.podsIndexer.Update(pod); err != nil {
				t.Fatalf("failed to update pod to indexer: %s", err)
			}
		}
	}

	// 修改镜像触发更新。
	set.Spec.Template.Spec.Containers[0].Image = "updated-image"

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to create selector: %s", err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// 无论 MinReadySeconds 如何，OnDelete 策略都应完成更新流程。
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	// 状态应正常计算；由于 MinReadySeconds，AvailableReplicas 应为 0。
	if status.AvailableReplicas != 0 {
		t.Errorf("expected 0 available replicas due to MinReadySeconds, got %d", status.AvailableReplicas)
	}

	// 但更新应已完成，因为 OnDelete 不等待 Pod 就绪。
	if status.UpdateRevision == "" {
		t.Error("expected UpdateRevision to be set for OnDelete strategy")
	}
}

// TestStatefulSetZeroMinReadySeconds 验证 MinReadySeconds 为零时恢复原有行为。
func TestStatefulSetZeroMinReadySeconds(t *testing.T) {
	// 验证 MinReadySeconds 为 0 时行为与原始逻辑一致。
	// 0 秒表示立即可用
	set := setMinReadySeconds(newStatefulSet(2), int32(0))
	client := fake.NewClientset(set)
	om, _, ssc := setupController(client)

	// 创建处于 Running 且 Ready 的 Pod，即使刚刚就绪。
	for i := 0; i < 2; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			// 刚刚就绪
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now()}},
		}
		fakeResourceVersion(pod)
		if err := om.podsIndexer.Add(pod); err != nil {
			t.Fatalf("failed to add pod to indexer: %s", err)
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to create selector: %s", err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// MinReadySeconds 为 0 时，Pod 应立即被视为可用。
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	if status.AvailableReplicas != 2 {
		t.Errorf("expected 2 available replicas with zero MinReadySeconds, got %d", status.AvailableReplicas)
	}
}

// TestStatefulSetPartitionRollingUpdateWithMinReadySeconds 验证带 partition 的滚动更新遵循 MinReadySeconds。
func TestStatefulSetPartitionRollingUpdateWithMinReadySeconds(t *testing.T) {
	// 验证基于 partition 的滚动更新遵循 MinReadySeconds。
	// 30 秒
	set := setMinReadySeconds(newStatefulSet(4), int32(30))
	set = setupPodManagementPolicy(apps.OrderedReadyPodManagement, set)
	var partition int32 = 2
	set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
		Type: apps.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{
			Partition: &partition,
		},
	}
	client := fake.NewClientset(set)
	om, _, ssc := setupController(client)

	// 创建已就绪但就绪时间不足的 Pod。
	for i := 0; i < 4; i++ {
		pod := newStatefulSetPod(set, i)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Now().Add(-10 * time.Second)}}, // 10 < 30
		}
		fakeResourceVersion(pod)
		if err := om.podsIndexer.Add(pod); err != nil {
			t.Fatalf("failed to add pod to indexer: %s", err)
		}
	}

	// 修改镜像触发滚动更新；只应影响序号大于等于 partition 的 Pod，即 Pod 2 和 3。
	set.Spec.Template.Spec.Containers[0].Image = "updated-image"

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatalf("failed to create selector: %s", err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}

	// 由于 Pod 可用时间不足 30 秒，滚动更新应被阻止。
	status, err := ssc.UpdateStatefulSet(context.TODO(), set, pods, time.Now())
	if err != nil {
		t.Fatalf("failed to update StatefulSet: %s", err)
	}

	if status.AvailableReplicas != 0 {
		t.Errorf("expected 0 available replicas due to MinReadySeconds in partition update, got %d", status.AvailableReplicas)
	}

	// 所有 Pod 仍应存在，说明更新已被阻止。
	newPods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatalf("failed to list pods: %s", err)
	}
	if len(newPods) != 4 {
		t.Errorf("expected 4 pods to still exist (partition update blocked), got %d", len(newPods))
	}
}

// TestStatefulSetMetrics 验证 MaxUnavailable 与 UnavailableReplicas 指标值的计算。
// 覆盖不同管理策略、更新策略与 maxUnavailable 配置组合。
func TestStatefulSetMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailableStatefulSet, true)
	metrics.Register()

	tests := []struct {
		name                             string
		totalPods                        int32
		maxUnavailable                   *intstr.IntOrString
		updateStrategy                   apps.StatefulSetUpdateStrategyType
		podManagementPolicy              apps.PodManagementPolicyType
		unavailablePodCount              int
		expectedMaxUnavailableValue      int
		expectedUnavailableReplicasValue int
	}{
		{
			name:                             "ordered pods within limit",
			totalPods:                        5,
			maxUnavailable:                   &intstr.IntOrString{Type: intstr.Int, IntVal: 2},
			updateStrategy:                   apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy:              apps.OrderedReadyPodManagement,
			unavailablePodCount:              1,
			expectedMaxUnavailableValue:      2,
			expectedUnavailableReplicasValue: 1,
		},
		{
			name:                "parallel pods exceeding limit",
			totalPods:           10,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "20%"},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.ParallelPodManagement,
			// 10 个副本的 20% 为 2，此处违反限制但指标应显示当前值。
			unavailablePodCount:              3,
			expectedMaxUnavailableValue:      2,
			expectedUnavailableReplicasValue: 3,
		},
		{
			name:                "ordered pods exactly at limit",
			totalPods:           6,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 3},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.OrderedReadyPodManagement,
			// 正好处于限制值
			unavailablePodCount:              3,
			expectedMaxUnavailableValue:      3,
			expectedUnavailableReplicasValue: 3,
		},
		{
			name:                "parallel pods all available",
			totalPods:           4,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.ParallelPodManagement,
			// 所有 Pod 均可用
			unavailablePodCount:              0,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 0,
		},
		{
			name:                "ordered pods with percentage maxUnavailable",
			totalPods:           8,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "25%"},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.OrderedReadyPodManagement,
			// 8 个副本的 25% 为 2，在限制之内。
			unavailablePodCount:              1,
			expectedMaxUnavailableValue:      2,
			expectedUnavailableReplicasValue: 1,
		},
		{
			name:                "parallel pods with large percentage",
			totalPods:           5,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "80%"},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.ParallelPodManagement,
			// 5 个副本的 80% 为 4，正好处于限制值。
			unavailablePodCount:              4,
			expectedMaxUnavailableValue:      4,
			expectedUnavailableReplicasValue: 4,
		},
		{
			name:                "small statefulset with maxUnavailable 1",
			totalPods:           2,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.OrderedReadyPodManagement,
			// 正好处于限制值
			unavailablePodCount:              1,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 1,
		},
		{
			name:                "single pod statefulset",
			totalPods:           1,
			maxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			updateStrategy:      apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy: apps.ParallelPodManagement,
			// 单个 Pod 不可用
			unavailablePodCount:              1,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 1,
		},
		{
			name:      "OnDelete strategy defaults to maxUnavailable 1",
			totalPods: 3,
			// OnDelete 不使用 maxUnavailable。
			maxUnavailable:      nil,
			updateStrategy:      apps.OnDeleteStatefulSetStrategyType,
			podManagementPolicy: apps.OrderedReadyPodManagement,
			unavailablePodCount: 2,
			// 默认值
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 2,
		},
		{
			name:      "OnDelete strategy with parallel management",
			totalPods: 4,
			// OnDelete 不使用 maxUnavailable。
			maxUnavailable:                   nil,
			updateStrategy:                   apps.OnDeleteStatefulSetStrategyType,
			podManagementPolicy:              apps.ParallelPodManagement,
			unavailablePodCount:              1,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 1,
		},
		{
			name:      "RollingUpdate with nil maxUnavailable defaults to 1",
			totalPods: 5,
			// nil 应默认为 1
			maxUnavailable:                   nil,
			updateStrategy:                   apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy:              apps.OrderedReadyPodManagement,
			unavailablePodCount:              2,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 2,
		},
		{
			name:      "RollingUpdate with nil maxUnavailable and parallel pods",
			totalPods: 6,
			// nil 应默认为 1
			maxUnavailable:                   nil,
			updateStrategy:                   apps.RollingUpdateStatefulSetStrategyType,
			podManagementPolicy:              apps.ParallelPodManagement,
			unavailablePodCount:              0,
			expectedMaxUnavailableValue:      1,
			expectedUnavailableReplicasValue: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(time.Date(2026, time.March, 12, 12, 0, 0, 0, time.UTC))

			// 创建 StatefulSet。
			set := newStatefulSet(test.totalPods)
			if test.updateStrategy == apps.RollingUpdateStatefulSetStrategyType {
				var partition int32 = 0
				set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
					Type: apps.RollingUpdateStatefulSetStrategyType,
					RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{
						Partition:      &partition,
						MaxUnavailable: test.maxUnavailable,
					},
				}
			} else {
				set.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
					Type: test.updateStrategy,
				}
			}
			set = setupPodManagementPolicy(test.podManagementPolicy, set)
			set.Status.CollisionCount = new(int32)

			// 配置控制器。
			client := fake.NewClientset(set)
			spc, _, ssc := setupController(client)

			// 扩容 StatefulSet。
			if err := scaleUpStatefulSetControl(set, ssc, spc, assertBurstInvariants, nil); err != nil {
				t.Fatal(err)
			}
			set, err := spc.setsLister.StatefulSets(set.Namespace).Get(set.Name)
			if err != nil {
				t.Fatal(err)
			}

			// 修改镜像以触发滚动更新。
			set.Spec.Template.Spec.Containers[0].Image = "foo"

			// 获取标签选择器。
			selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
			if err != nil {
				t.Fatal(err)
			}

			// 让部分 Pod 不可用。
			var pods []*v1.Pod
			for i := 0; i < test.unavailablePodCount; i++ {
				if test.podManagementPolicy == apps.OrderedReadyPodManagement {
					_, _ = spc.setPodRunning(set, i)
					pods, _ = spc.setPodReadyCondition(set, i, false, fakeClock.Now())
				} else {
					pods, _ = spc.addTerminatingPod(set, i)
				}
			}

			// 让其余 Pod 保持就绪。
			for i := test.unavailablePodCount; i < int(test.totalPods); i++ {
				_, _ = spc.setPodRunning(set, i)
				pods, _ = spc.setPodReadyCondition(set, i, true, fakeClock.Now())
			}
			sort.Sort(ascendingOrdinal(pods))

			// 更新 StatefulSet。
			// 根据测试配置计算就绪副本数。
			readyReplicas := test.totalPods - int32(test.unavailablePodCount)
			status := apps.StatefulSetStatus{
				Replicas:      test.totalPods,
				ReadyReplicas: readyReplicas,
			}
			updateRevision := &apps.ControllerRevision{}
			_, err = updateStatefulSetAfterInvariantEstablished(context.TODO(), ssc.(*defaultStatefulSetControl), set, pods, updateRevision, status, fakeClock.Now())
			if err != nil {
				t.Fatal(err)
			}

			// 同时调用 updateStatefulSetStatus，确保指标得到更新。
			err = ssc.(*defaultStatefulSetControl).updateStatefulSetStatus(context.TODO(), set, &status)
			if err != nil {
				t.Fatal(err)
			}

			// 获取更新后的 Pod 列表。
			pods, err = spc.podsLister.Pods(set.Namespace).List(selector)
			if err != nil {
				t.Fatal(err)
			}
			sort.Sort(ascendingOrdinal(pods))

			// 校验指标值。
			maxUnavailableValue, err := testutil.GetGaugeMetricValue(
				metrics.MaxUnavailable.WithLabelValues(set.Namespace, set.Name, string(test.podManagementPolicy)))
			if err != nil {
				t.Fatal(err)
			}
			if int(maxUnavailableValue) != test.expectedMaxUnavailableValue {
				t.Errorf("Expected MaxUnavailable gauge value %d, got %d", test.expectedMaxUnavailableValue, int(maxUnavailableValue))
			}

			unavailableReplicasValue, err := testutil.GetGaugeMetricValue(
				metrics.UnavailableReplicas.WithLabelValues(set.Namespace, set.Name, string(test.podManagementPolicy)))
			if err != nil {
				t.Fatal(err)
			}
			if int(unavailableReplicasValue) != test.expectedUnavailableReplicasValue {
				t.Errorf("Expected UnavailableReplicas gauge value %d, got %d", test.expectedUnavailableReplicasValue, int(unavailableReplicasValue))
			}
		})
	}
}
