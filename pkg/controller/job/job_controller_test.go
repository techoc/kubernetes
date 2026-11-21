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

package job

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/job/metrics"
	"k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

var realClock = &clock.RealClock{}
var alwaysReady = func() bool { return true }

const fastSyncJobBatchPeriod = 10 * time.Millisecond
const fastJobApiBackoff = 10 * time.Millisecond
const fastRequeue = 10 * time.Millisecond

// testFinishedAt 表示比 Unix 纪元晚一秒的时间。
// 用于那些不希望触发退避（back-off）逻辑的测试用例。
var testFinishedAt = metav1.NewTime((time.Time{}).Add(time.Second))

// newJobWithName 创建一个指定名称、并行度、完成数、退避上限及完成模式的 Job。
func newJobWithName(name string, parallelism, completions, backoffLimit int32, completionMode batch.CompletionMode) *batch.Job {
	j := &batch.Job{
		TypeMeta: metav1.TypeMeta{Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			UID:       uuid.NewUUID(),
			Namespace: metav1.NamespaceDefault,
		},
		Spec: batch.JobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"foo": "bar"},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Image: "foo/bar"},
					},
				},
			},
		},
	}
	if completionMode != "" {
		j.Spec.CompletionMode = &completionMode
	}
	// 特殊情况：completions 或 parallelism 为 -1 时保持 nil（验证器不允许负值）。
	if completions >= 0 {
		j.Spec.Completions = &completions
	} else {
		j.Spec.Completions = nil
	}
	if parallelism >= 0 {
		j.Spec.Parallelism = &parallelism
	} else {
		j.Spec.Parallelism = nil
	}
	j.Spec.BackoffLimit = &backoffLimit

	return j
}

// newJob 使用默认名称 foobar 构造 Job。
func newJob(parallelism, completions, backoffLimit int32, completionMode batch.CompletionMode) *batch.Job {
	return newJobWithName("foobar", parallelism, completions, backoffLimit, completionMode)
}

// newControllerFromClient 基于客户端构造 Job 控制器，使用真实时钟。
func newControllerFromClient(ctx context.Context, t *testing.T, kubeClient clientset.Interface, resyncPeriod controller.ResyncPeriodFunc) (*Controller, informers.SharedInformerFactory) {
	t.Helper()
	return newControllerFromClientWithClock(ctx, t, kubeClient, resyncPeriod, realClock)
}

// newControllerFromClientWithClock 构造使用指定时钟的 Job 控制器，便于验证时间相关逻辑。
func newControllerFromClientWithClock(ctx context.Context, t *testing.T, kubeClient clientset.Interface, resyncPeriod controller.ResyncPeriodFunc, clock clock.WithTicker) (*Controller, informers.SharedInformerFactory) {
	t.Helper()
	sharedInformers := informers.NewSharedInformerFactory(kubeClient, resyncPeriod())
	jm, err := newControllerWithClock(ctx, kubeClient, clock,
		sharedInformers.Core().V1().Pods(),
		sharedInformers.Batch().V1().Jobs(),
		sharedInformers.Scheduling().V1beta1().Workloads(),
		sharedInformers.Scheduling().V1beta1().PodGroups(),
	)
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	jm.podControl = &controller.FakePodControl{}
	return jm, sharedInformers
}

// newPod 创建一个归属于给定 Job、标签与 Job selector 匹配的 Pod。
func newPod(name string, job *batch.Job) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID(name),
			Labels:          job.Spec.Selector.MatchLabels,
			Namespace:       job.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, controllerKind)},
		},
	}
}

// newPodList 为给定 Job 创建指定数量、处于指定 phase 的 Pod。
func newPodList(count int, status v1.PodPhase, job *batch.Job) []*v1.Pod {
	var pods []*v1.Pod
	for i := 0; i < count; i++ {
		newPod := newPod(fmt.Sprintf("pod-%v", rand.String(10)), job)
		newPod.Status = v1.PodStatus{Phase: status}
		newPod.Status.ContainerStatuses = []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{
						FinishedAt: testFinishedAt,
					},
				},
			},
		}
		newPod.Finalizers = append(newPod.Finalizers, batch.JobTrackingFinalizer)
		pods = append(pods, newPod)
	}
	return pods
}

// setPodsStatuses 按各阶段数量向索引器添加 Pod，并按需设置 Ready 状态和删除时间戳。
func setPodsStatuses(podIndexer cache.Indexer, job *batch.Job, pendingPods, activePods, succeededPods, failedPods, terminatingPods, readyPods int) {
	for _, pod := range newPodList(pendingPods, v1.PodPending, job) {
		podIndexer.Add(pod)
	}
	// 只把前 readyPods 个运行中的 Pod 标记为 Ready，便于分别校验 active 与 ready 计数。
	running := newPodList(activePods, v1.PodRunning, job)
	for i, p := range running {
		if i >= readyPods {
			break
		}
		p.Status.Conditions = append(p.Status.Conditions, v1.PodCondition{
			Type:   v1.PodReady,
			Status: v1.ConditionTrue,
		})
	}
	for _, pod := range running {
		podIndexer.Add(pod)
	}
	for _, pod := range newPodList(succeededPods, v1.PodSucceeded, job) {
		podIndexer.Add(pod)
	}
	for _, pod := range newPodList(failedPods, v1.PodFailed, job) {
		podIndexer.Add(pod)
	}
	terminating := newPodList(terminatingPods, v1.PodRunning, job)
	for _, p := range terminating {
		now := metav1.Now()
		p.DeletionTimestamp = &now
	}
	for _, pod := range terminating {
		podIndexer.Add(pod)
	}
}

// setPodsStatusesWithIndexes 按 (index, phase) 列表向索引器添加带完成索引的 Pod。
func setPodsStatusesWithIndexes(podIndexer cache.Indexer, job *batch.Job, status []indexPhase) {
	for _, s := range status {
		p := newPod(fmt.Sprintf("pod-%s", rand.String(10)), job)
		p.Status = v1.PodStatus{Phase: s.Phase}
		if s.Phase == v1.PodFailed || s.Phase == v1.PodSucceeded {
			// 终态 Pod 需要 FinishedAt，控制器依赖它判断完成时间与退避。
			p.Status.ContainerStatuses = []v1.ContainerStatus{
				{
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							FinishedAt: testFinishedAt,
						},
					},
				},
			}
		}
		if s.Index != noIndex {
			p.Annotations = map[string]string{
				batch.JobCompletionIndexAnnotation: s.Index,
			}
			p.Spec.Hostname = fmt.Sprintf("%s-%s", job.Name, s.Index)
		}
		p.Finalizers = append(p.Finalizers, batch.JobTrackingFinalizer)
		podIndexer.Add(p)
	}
}

// jobInitialStatus 描述 Job 的初始状态，用于模拟同步前已有的计数与 condition。
type jobInitialStatus struct {
	active     int
	succeed    int
	failed     int
	startTime  *time.Time
	conditions []batch.JobCondition
}

// TestControllerSyncJob 通过表驱动的方式验证 syncJob 在各种 Job 与 Pod 状态下的行为。
func TestControllerSyncJob(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	referenceTime := time.Now()

	testCases := map[string]struct {
		// Job 配置
		parallelism          int32
		completions          int32
		backoffLimit         int32
		deleting             bool
		podLimit             int
		completionMode       batch.CompletionMode
		wasSuspended         bool
		suspend              bool
		podReplacementPolicy *batch.PodReplacementPolicy
		podFailurePolicy     *batch.PodFailurePolicy
		initialStatus        *jobInitialStatus
		backoffRecord        *backoffRecord
		controllerTime       *time.Time

		// Pod 配置

		// 若设置了 podControllerError，finalizer 将无法被移除，
		// 因而不会发生状态更新，failedPods 与 succeededPods 的计数也无法递增。
		podControllerError        error
		pendingPods               int
		activePods                int
		readyPods                 int
		succeededPods             int
		failedPods                int
		terminatingPods           int
		podsWithIndexes           []indexPhase
		fakeExpectationAtCreation int32 // 负数：ExpectDeletions；正数：ExpectCreations

		// 失败 Pod 的 finalizer 被移除后计入失败数；终止中的计数包含本次同步触发的删除。
		// 初始状态已包含的失败计数无需额外 patch；成功时设置 SuccessCriteriaMet 与 Complete。
		// 失败时设置 FailureTarget 与 Failed；索引完成的 CompletedIndexes 按范围压缩。
		// 无有效索引的 Pod 视为多余，会被删除并计入终止中。
		// 期望值
		expectedCreations      int32
		expectedDeletions      int32
		expectedActive         int32
		expectedReady          *int32
		expectedSucceeded      int32
		expectedCompletedIdxs  string
		expectedFailed         int32
		expectedTerminating    *int32
		expectedConditions     []batch.JobCondition
		expectedCreatedIndexes sets.Set[int]
		expectedPodPatches     int

		// 特性开关
		jobManagedBy bool
	}{
		// 全新 Job：按 parallelism 创建 Pod，并更新状态计数。
		"job start": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			expectedCreations:   2,
			expectedActive:      2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// Work Queue 风格（completions 为 nil）的 Job 同样按 parallelism 创建 Pod。
		"WQ job start": {
			parallelism:         2,
			completions:         -1,
			backoffLimit:        6,
			expectedCreations:   2,
			expectedActive:      2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 待调度 Pod 计入 active，但不触发新建。
		"pending pods": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			pendingPods:         2,
			expectedActive:      2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// Pod 数与 parallelism 一致时不增不减，ready 单独统计。
		"correct # of pods": {
			parallelism:         3,
			completions:         5,
			backoffLimit:        6,
			activePods:          3,
			readyPods:           2,
			expectedActive:      3,
			expectedReady:       ptr.To[int32](2),
			expectedTerminating: ptr.To[int32](0),
		},
		// 动态 Job 的 Pod 数已满足 parallelism，不执行创建或删除。
		"WQ job: correct # of pods": {
			parallelism:         2,
			completions:         -1,
			backoffLimit:        6,
			activePods:          2,
			expectedActive:      2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 活跃 Pod 不足时补齐到 parallelism，同时清理成功 Pod 的 finalizer。
		"too few active pods": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          1,
			succeededPods:       1,
			expectedCreations:   1,
			expectedActive:      2,
			expectedSucceeded:   1,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// Failed 替换策略下，失败 Pod 被清理并重新创建。
		"WQ job: recreate pods when failed": {
			parallelism:          1,
			completions:          -1,
			backoffLimit:         6,
			activePods:           1,
			failedPods:           1,
			podReplacementPolicy: podReplacementPolicy(batch.Failed),
			terminatingPods:      1,
			expectedTerminating:  ptr.To[int32](1),
			expectedReady:        ptr.To[int32](0),
			// 移除 finalizer 并删除一个失败的 Pod
			expectedPodPatches: 1,
			expectedFailed:     1,
			expectedActive:     1,
		},
		// 开启替换策略但未显式设置时，沿用默认行为处理失败 Pod。
		"WQ job: turn on PodReplacementPolicy but not set PodReplacementPolicy": {
			parallelism:         1,
			completions:         1,
			backoffLimit:        6,
			activePods:          1,
			failedPods:          1,
			expectedTerminating: ptr.To[int32](1),
			expectedReady:       ptr.To[int32](0),
			terminatingPods:     1,
			expectedActive:      1,
			expectedPodPatches:  2,
			expectedFailed:      2,
		},
		// TerminatingOrFailed 策略下，终止中或失败的 Pod 都会触发替换。
		"WQ job: recreate pods when terminating or failed": {
			parallelism:          1,
			completions:          -1,
			backoffLimit:         6,
			activePods:           1,
			failedPods:           1,
			podReplacementPolicy: podReplacementPolicy(batch.TerminatingOrFailed),
			terminatingPods:      1,
			expectedTerminating:  ptr.To[int32](1),
			expectedReady:        ptr.To[int32](0),
			expectedActive:       1,
			expectedPodPatches:   2,
			expectedFailed:       2,
		},
		// 终止中的 Pod 过多时，删除多余的活跃 Pod，使总数回到 parallelism。
		"more terminating pods than parallelism": {
			parallelism:          1,
			completions:          1,
			backoffLimit:         6,
			activePods:           2,
			failedPods:           0,
			terminatingPods:      4,
			podReplacementPolicy: podReplacementPolicy(batch.Failed),
			expectedTerminating:  ptr.To[int32](5),
			expectedReady:        ptr.To[int32](0),
			expectedActive:       1,
			expectedDeletions:    1,
			expectedPodPatches:   1,
		},
		// 同时配置 PodFailurePolicy 时，上述删除逻辑仍然成立。
		"more terminating pods than parallelism; PodFailurePolicy used": {
			// 复现 https://github.com/kubernetes/kubernetes/issues/122235
			parallelism:          1,
			completions:          1,
			backoffLimit:         6,
			activePods:           2,
			failedPods:           0,
			terminatingPods:      4,
			podFailurePolicy:     &batch.PodFailurePolicy{},
			podReplacementPolicy: podReplacementPolicy(batch.Failed),
			expectedTerminating:  ptr.To[int32](5),
			expectedReady:        ptr.To[int32](0),
			expectedActive:       1,
			expectedDeletions:    1,
			expectedPodPatches:   1,
		},
		// 退避生效期间不创建 Pod，也不会误报 active 计数。
		"too few active pods and active back-off": {
			parallelism:  1,
			completions:  1,
			backoffLimit: 6,
			backoffRecord: &backoffRecord{
				failuresAfterLastSuccess: 1,
				lastFailureTime:          &referenceTime,
			},
			initialStatus: &jobInitialStatus{
				startTime: func() *time.Time {
					now := time.Now()
					return &now
				}(),
			},
			activePods:          0,
			succeededPods:       0,
			expectedCreations:   0,
			expectedActive:      0,
			expectedSucceeded:   0,
			expectedPodPatches:  0,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
			controllerTime:      &referenceTime,
		},
		// 回归测试：https://github.com/kubernetes/kubernetes/issues/139428。
		// 当需要替换 Pod 但因退避生效而延迟创建时，
		// manageJob 必须报告真实的 active 数量，而不是 0。
		// 若报告 0，而 Ready 仍反映正在运行的 Pod，状态更新将无法通过
		// apiserver 校验（"cannot set more ready pods than active"），
		// 从而阻塞 finalizer 的移除与状态刷写。
		// 退避期间仍有运行中 Pod：active 必须真实上报，否则 Ready > Active 会导致状态更新被拒绝。
		"too few active pods and active back-off with running pods": {
			parallelism:  2,
			completions:  2,
			backoffLimit: 6,
			backoffRecord: &backoffRecord{
				failuresAfterLastSuccess: 1,
				lastFailureTime:          &referenceTime,
			},
			initialStatus: &jobInitialStatus{
				startTime: func() *time.Time {
					now := time.Now()
					return &now
				}(),
			},
			activePods:          1,
			readyPods:           1,
			succeededPods:       0,
			expectedCreations:   0,
			expectedActive:      1,
			expectedSucceeded:   0,
			expectedPodPatches:  0,
			expectedReady:       ptr.To[int32](1),
			expectedTerminating: ptr.To[int32](0),
			controllerTime:      &referenceTime,
		},
		// 无退避时立即补建缺失的 Pod。
		"too few active pods and no back-offs": {
			parallelism:  1,
			completions:  1,
			backoffLimit: 6,
			backoffRecord: &backoffRecord{
				failuresAfterLastSuccess: 0,
				lastFailureTime:          &referenceTime,
			},
			activePods:          0,
			succeededPods:       0,
			expectedCreations:   1,
			expectedActive:      1,
			expectedSucceeded:   0,
			expectedPodPatches:  0,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
			controllerTime:      &referenceTime,
		},
		// 动态 Job 同样按 parallelism 补齐缺失的 Pod。
		"too few active pods with a dynamic job": {
			parallelism:         2,
			completions:         -1,
			backoffLimit:        6,
			activePods:          1,
			expectedCreations:   1,
			expectedActive:      2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// podControl 报错时状态计数不推进，但仍记录已尝试的创建与清理。
		"too few active pods, with controller error": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			podControllerError:  fmt.Errorf("fake error"),
			activePods:          1,
			succeededPods:       1,
			expectedCreations:   1,
			expectedActive:      1,
			expectedSucceeded:   0,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 活跃 Pod 超出 parallelism 时删除多余 Pod，并标记终止中的数量。
		"too many active pods": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          3,
			expectedDeletions:   1,
			expectedActive:      2,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](1),
		},
		// 删除失败时保留原有 active 计数，避免状态与实际不符。
		"too many active pods, with controller error": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			podControllerError:  fmt.Errorf("fake error"),
			activePods:          3,
			expectedDeletions:   0,
			expectedPodPatches:  1,
			expectedActive:      3,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 删除多余 Pod 后，ready 计数同步调整为剩余数量。
		"count ready pods when too many active pods": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          3,
			readyPods:           3,
			expectedDeletions:   1,
			expectedActive:      2,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](2),
			expectedTerminating: ptr.To[int32](1),
		},
		// 出现成功 Pod 会重置退避，同时补建活跃 Pod。
		"failed + succeed pods: reset backoff delay": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          1,
			succeededPods:       1,
			failedPods:          1,
			expectedCreations:   1,
			expectedActive:      2,
			expectedSucceeded:   1,
			expectedFailed:      1,
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 新增失败 Pod 后补建活跃 Pod，并清理失败 Pod 的 finalizer。
		"new failed pod": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          1,
			failedPods:          1,
			expectedCreations:   1,
			expectedActive:      2,
			expectedFailed:      1,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 初始状态已反映失败计数时，无需创建 Pod，只需处理 finalizer。
		"no new pod; possible finalizer update of failed pod": {
			parallelism:  1,
			completions:  1,
			backoffLimit: 6,
			initialStatus: &jobInitialStatus{
				active:  1,
				succeed: 0,
				failed:  1,
			},
			activePods:          1,
			failedPods:          0,
			expectedCreations:   0,
			expectedActive:      1,
			expectedFailed:      1,
			expectedPodPatches:  0,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// podControl 报错时失败计数不更新，但仍尝试补建。
		"only new failed pod with controller error": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			podControllerError:  fmt.Errorf("fake error"),
			activePods:          1,
			failedPods:          1,
			expectedCreations:   1,
			expectedActive:      1,
			expectedFailed:      0,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 成功 Pod 达到 completions 时，Job 被标记为完成并添加对应 condition。
		"job finish": {
			parallelism:       2,
			completions:       5,
			backoffLimit:      6,
			succeededPods:     5,
			expectedSucceeded: 5,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
			expectedPodPatches:  5,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 动态 Job 部分 Pod 成功时，状态正确反映成功与活跃数量。
		"WQ job finishing": {
			parallelism:         2,
			completions:         -1,
			backoffLimit:        6,
			activePods:          1,
			succeededPods:       1,
			expectedActive:      1,
			expectedSucceeded:   1,
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 动态 Job 的所有 Pod 均成功时，Job 完成。
		"WQ job all finished": {
			parallelism:       2,
			completions:       -1,
			backoffLimit:      6,
			succeededPods:     2,
			expectedSucceeded: 2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 动态 Job 中的单个失败不影响整体完成判定。
		"WQ job all finished despite one failure": {
			parallelism:       2,
			completions:       -1,
			backoffLimit:      6,
			succeededPods:     1,
			failedPods:        1,
			expectedSucceeded: 1,
			expectedFailed:    1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 大幅超出 parallelism 时，一次同步删除全部多余 Pod。
		"more active pods than parallelism": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          10,
			expectedDeletions:   8,
			expectedActive:      2,
			expectedPodPatches:  8,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](8),
		},
		// 活跃 Pod 多于剩余完成数时，按剩余完成数裁剪。
		"more active pods than remaining completions": {
			parallelism:         3,
			completions:         4,
			backoffLimit:        6,
			activePods:          3,
			succeededPods:       2,
			expectedDeletions:   1,
			expectedActive:      2,
			expectedSucceeded:   2,
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](1),
		},
		// 状态计数变化需要同步到 Job status，并清理对应 Pod 的 finalizer。
		"status change": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			activePods:          2,
			succeededPods:       2,
			expectedActive:      2,
			expectedSucceeded:   2,
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 删除中的 Job 不创建或删除 Pod，但仍统计现有 Pod 并清理 finalizer。
		"deleting job": {
			parallelism:         2,
			completions:         5,
			backoffLimit:        6,
			deleting:            true,
			pendingPods:         1,
			activePods:          1,
			succeededPods:       1,
			expectedActive:      2,
			expectedSucceeded:   1,
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// CreateLimit 限制单次同步的创建数量，用于验证批量上限逻辑。
		"limited pods": {
			parallelism:         100,
			completions:         200,
			backoffLimit:        6,
			podLimit:            10,
			expectedCreations:   10,
			expectedActive:      10,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 失败次数超过 backoffLimit 时，添加失败目标与失败 condition。
		"too many job failures": {
			parallelism:    2,
			completions:    5,
			deleting:       true,
			failedPods:     1,
			expectedFailed: 1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			expectedPodPatches:  1,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// expectations 未满足时仍按失败数设置 condition，但不重复处理 Pod。
		"job failures, unsatisfied expectations": {
			parallelism:               2,
			completions:               5,
			deleting:                  true,
			failedPods:                1,
			fakeExpectationAtCreation: 1,
			expectedFailed:            1,
			expectedPodPatches:        1,
			expectedReady:             ptr.To[int32](0),
			expectedTerminating:       ptr.To[int32](0),
		},
		// 索引 Job 按未完成的索引创建 Pod，期望索引为 0 和 1。
		"indexed job start": {
			parallelism:            2,
			completions:            5,
			backoffLimit:           6,
			completionMode:         batch.IndexedCompletion,
			expectedCreations:      2,
			expectedActive:         2,
			expectedCreatedIndexes: sets.New(0, 1),
			expectedReady:          ptr.To[int32](0),
			expectedTerminating:    ptr.To[int32](0),
		},
		// Failed 策略下仅替换失败索引，终止中的 Pod 不计入待替换。
		"indexed job with some pods deleted, podReplacementPolicy Failed": {
			parallelism:            2,
			completions:            5,
			backoffLimit:           6,
			completionMode:         batch.IndexedCompletion,
			expectedCreations:      1,
			expectedActive:         1,
			expectedCreatedIndexes: sets.New(0),
			podReplacementPolicy:   podReplacementPolicy(batch.Failed),
			terminatingPods:        1,
			expectedTerminating:    ptr.To[int32](1),
			expectedReady:          ptr.To[int32](0),
		},
		// TerminatingOrFailed 策略下，终止中的索引也会被重新创建。
		"indexed job with some pods deleted, podReplacementPolicy TerminatingOrFailed": {
			parallelism:            2,
			completions:            5,
			backoffLimit:           6,
			completionMode:         batch.IndexedCompletion,
			expectedCreations:      2,
			expectedActive:         2,
			expectedCreatedIndexes: sets.New(0, 1),
			podReplacementPolicy:   podReplacementPolicy(batch.TerminatingOrFailed),
			terminatingPods:        1,
			expectedTerminating:    ptr.To[int32](1),
			expectedReady:          ptr.To[int32](0),
			expectedPodPatches:     1,
		},
		// 索引 0-2 均成功时，CompletedIndexes 被压缩为范围 "0-2"。
		"indexed job completed": {
			parallelism:    2,
			completions:    3,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodFailed},
				{"1", v1.PodSucceeded},
				{"2", v1.PodSucceeded},
			},
			expectedSucceeded:     3,
			expectedFailed:        1,
			expectedCompletedIdxs: "0-2",
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
			expectedPodPatches:  4,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// backoffLimit 为 0 时，失败触发 JobFailureTarget，并删除运行中的 Pod。
		"indexed job failed": {
			parallelism:    2,
			completions:    3,
			backoffLimit:   0,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodFailed},
				{"2", v1.PodRunning},
			},
			expectedSucceeded:     1,
			expectedFailed:        2,
			expectedCompletedIdxs: "0",
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](1),
			expectedDeletions:   1,
		},
		// 索引 Job 失败时也同样统计终止中的 Pod 数量。
		"count terminating pods when indexed job fails": {
			parallelism:    2,
			completions:    3,
			backoffLimit:   0,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodFailed},
				{"2", v1.PodRunning},
			},
			expectedSucceeded:     1,
			expectedFailed:        2,
			expectedCompletedIdxs: "0",
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedDeletions:   1,
			expectedTerminating: ptr.To[int32](1),
		},
		// Job 失败时 ready 计数归零，活跃 Pod 被删除。
		"count ready pods when job fails": {
			parallelism:    2,
			completions:    3,
			backoffLimit:   0,
			activePods:     2,
			readyPods:      2,
			failedPods:     1,
			expectedFailed: 3,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedDeletions:   2,
			expectedTerminating: ptr.To[int32](2),
		},
		// 同一索引重复成功时只计一次，并补建剩余索引。
		"indexed job repeated completed index": {
			parallelism:    2,
			completions:    3,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodSucceeded},
				{"1", v1.PodSucceeded},
			},
			expectedCreations:      1,
			expectedActive:         1,
			expectedSucceeded:      2,
			expectedCompletedIdxs:  "0,1",
			expectedCreatedIndexes: sets.New(2),
			expectedPodPatches:     3,
			expectedReady:          ptr.To[int32](0),
			expectedTerminating:    ptr.To[int32](0),
		},
		// 部分索引已完成时，仅创建缺失索引对应的 Pod。
		"indexed job some running and completed pods": {
			parallelism:    8,
			completions:    20,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodRunning},
				{"2", v1.PodSucceeded},
				{"3", v1.PodPending},
				{"4", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{"7", v1.PodSucceeded},
				{"8", v1.PodSucceeded},
				{"9", v1.PodSucceeded},
			},
			expectedCreations:      6,
			expectedActive:         8,
			expectedSucceeded:      6,
			expectedCompletedIdxs:  "2,4,5,7-9",
			expectedCreatedIndexes: sets.New(1, 6, 10, 11, 12, 13),
			expectedPodPatches:     6,
			expectedReady:          ptr.To[int32](0),
			expectedTerminating:    ptr.To[int32](0),
		},
		// 失败索引需要重新创建，已完成索引保持不变。
		"indexed job some failed pods": {
			parallelism:    3,
			completions:    4,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodFailed},
				{"1", v1.PodPending},
				{"2", v1.PodFailed},
			},
			expectedCreations:      2,
			expectedActive:         3,
			expectedFailed:         2,
			expectedCreatedIndexes: sets.New(0, 2),
			expectedPodPatches:     2,
			expectedReady:          ptr.To[int32](0),
			expectedTerminating:    ptr.To[int32](0),
		},
		// 无有效索引的 Pod 会被删除，有效索引的 Pod 正常统计。
		"indexed job some pods without index": {
			parallelism:    2,
			completions:    5,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			activePods:     1,
			succeededPods:  1,
			failedPods:     1,
			podsWithIndexes: []indexPhase{
				{"invalid", v1.PodRunning},
				{"invalid", v1.PodSucceeded},
				{"invalid", v1.PodFailed},
				{"invalid", v1.PodPending},
				{"0", v1.PodSucceeded},
				{"1", v1.PodRunning},
				{"2", v1.PodRunning},
			},
			expectedDeletions:     3,
			expectedActive:        2,
			expectedSucceeded:     1,
			expectedFailed:        0,
			expectedCompletedIdxs: "0",
			expectedPodPatches:    8,
			expectedReady:         ptr.To[int32](0),
			expectedTerminating:   ptr.To[int32](3),
		},
		// 重复索引的 Pod 会被清理，每个索引只保留一个实例。
		"indexed job repeated indexes": {
			parallelism:    5,
			completions:    5,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			succeededPods:  1,
			failedPods:     1,
			podsWithIndexes: []indexPhase{
				{"invalid", v1.PodRunning},
				{"0", v1.PodSucceeded},
				{"1", v1.PodRunning},
				{"2", v1.PodRunning},
				{"2", v1.PodPending},
			},
			expectedCreations:     0,
			expectedDeletions:     2,
			expectedActive:        2,
			expectedSucceeded:     1,
			expectedCompletedIdxs: "0",
			expectedPodPatches:    5,
			expectedReady:         ptr.To[int32](0),
			expectedTerminating:   ptr.To[int32](2),
		},
		// 超出 completions 范围的索引视为无效并被删除。
		"indexed job with indexes outside of range": {
			parallelism:    2,
			completions:    5,
			backoffLimit:   6,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"5", v1.PodRunning},
				{"6", v1.PodSucceeded},
				{"7", v1.PodPending},
				{"8", v1.PodFailed},
			},
			expectedCreations:     0, // 一次同步中创建与删除只能发生一种
			expectedSucceeded:     1,
			expectedDeletions:     2,
			expectedCompletedIdxs: "0",
			expectedActive:        0,
			expectedFailed:        0,
			expectedPodPatches:    5,
			expectedReady:         ptr.To[int32](0),
			expectedTerminating:   ptr.To[int32](2),
		},
		"suspending a job with satisfied expectations": {
			// 当 expectations 已满足时，
			// 被挂起的 Job 应当删除活跃的 Pod。
			suspend:           true,
			parallelism:       2,
			activePods:        2, // parallelism == active，expectations 已满足
			completions:       4,
			backoffLimit:      6,
			expectedCreations: 0,
			expectedDeletions: 2,
			expectedActive:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionTrue,
					Reason:  "JobSuspended",
					Message: "Job suspended",
				},
			},
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](2),
		},
		"suspending a job with satisfied expectations; PodReplacementPolicy enabled": {
			// 当 expectations 已满足时，
			// 被挂起的 Job 应当删除活跃的 Pod。
			suspend:           true,
			parallelism:       2,
			activePods:        2, // parallelism == active，expectations 已满足
			completions:       4,
			backoffLimit:      6,
			expectedCreations: 0,
			expectedDeletions: 2,
			expectedActive:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionTrue,
					Reason:  "JobSuspended",
					Message: "Job suspended",
				},
			},
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](2),
		},
		// 挂起时即使存在 ready Pod，挂起后的计数也应归零。
		"count ready pods when suspending a job with satisfied expectations": {
			suspend:           true,
			parallelism:       2,
			activePods:        2, // parallelism == active，expectations 已满足
			readyPods:         2,
			completions:       4,
			backoffLimit:      6,
			expectedCreations: 0,
			expectedDeletions: 2,
			expectedActive:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionTrue,
					Reason:  "JobSuspended",
					Message: "Job suspended",
				},
			},
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](2),
		},
		"suspending a job with unsatisfied expectations": {
			// 与前一个用例不同，这里期望控制器在本次 syncJob 中
			// 不挂起 Job，因为控制器会先等待 expectations 被满足。
			// 下一次 syncJob 调用（此处未覆盖）才会与前一个用例一致。
			suspend:                   true,
			parallelism:               2,
			activePods:                3,  // active > parallelism，expectations 未满足
			fakeExpectationAtCreation: -1, // 控制器正在等待一次删除
			completions:               4,
			backoffLimit:              6,
			expectedCreations:         0,
			expectedDeletions:         0,
			expectedActive:            3,
			expectedReady:             ptr.To[int32](0),
			expectedTerminating:       ptr.To[int32](0),
		},
		// 挂起 expectations 未满足的 Job 时，ready 计数保持同步前的状态。
		"count ready pods when suspending a job with unsatisfied expectations": {
			suspend:                   true,
			parallelism:               2,
			activePods:                3, // active > parallelism，expectations 未满足
			readyPods:                 3,
			fakeExpectationAtCreation: -1, // 控制器正在等待一次删除
			completions:               4,
			backoffLimit:              6,
			expectedCreations:         0,
			expectedDeletions:         0,
			expectedActive:            3,
			expectedReady:             ptr.To[int32](3),
			expectedTerminating:       ptr.To[int32](0),
		},
		"suspending a job with unsatisfied expectations; PodReplacementPolicy enabled": {
			suspend:                   true,
			parallelism:               2,
			activePods:                3,  // active > parallelism，expectations 未满足
			fakeExpectationAtCreation: -1, // 控制器正在等待一次删除
			completions:               4,
			backoffLimit:              6,
			expectedCreations:         0,
			expectedDeletions:         0,
			expectedActive:            3,
			expectedReady:             ptr.To[int32](0),
			expectedTerminating:       ptr.To[int32](0),
		},
		// 恢复挂起时重新创建 Pod，并添加 Suspended=False condition。
		"resuming a suspended job": {
			wasSuspended:      true,
			suspend:           false,
			parallelism:       2,
			completions:       4,
			backoffLimit:      6,
			expectedCreations: 2,
			expectedDeletions: 0,
			expectedActive:    2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionFalse,
					Reason:  "JobResumed",
					Message: "Job resumed",
				},
			},
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		"suspending a deleted job": {
			// 通常期望删除活跃的 Pod（参见前面的用例），但此 Job 正在被删除，
			// 因此挂起前后不应有任何变化。
			// 同时不会添加 JobSuspended condition。
			suspend:             true,
			deleting:            true,
			parallelism:         2,
			activePods:          2, // parallelism == active，expectations 已满足
			completions:         4,
			backoffLimit:        6,
			expectedCreations:   0,
			expectedDeletions:   0,
			expectedActive:      2,
			expectedPodPatches:  2,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](0),
		},
		// 手动添加的 False condition 应被保留，不触发失败流程。
		"FailureTarget=False condition added manually is ignored": {
			parallelism: 1,
			completions: 1,
			activePods:  1,
			readyPods:   1,
			initialStatus: &jobInitialStatus{
				active: 1,
				startTime: func() *time.Time {
					now := time.Now()
					return &now
				}(),
				conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionFalse,
						Reason:  "ConditionAddedManually",
						Message: "Testing",
					},
				},
			},
			expectedActive:      1,
			expectedReady:       ptr.To[int32](1),
			expectedTerminating: ptr.To[int32](0),
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionFalse,
					Reason:  "ConditionAddedManually",
					Message: "Testing",
				},
			},
		},
		// 手动添加的 SuccessCriteriaMet=False 同样不触发成功判定。
		"SuccessCriteriaMet=False condition added manually is ignored": {
			parallelism: 1,
			completions: 1,
			activePods:  1,
			readyPods:   1,
			initialStatus: &jobInitialStatus{
				active: 1,
				startTime: func() *time.Time {
					now := time.Now()
					return &now
				}(),
				conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionFalse,
						Reason:  "ConditionAddedManually",
						Message: "Testing",
					},
				},
			},
			expectedActive:      1,
			expectedReady:       ptr.To[int32](1),
			expectedTerminating: ptr.To[int32](0),
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionFalse,
					Reason:  "ConditionAddedManually",
					Message: "Testing",
				},
			},
		},
		// 开启 JobManagedBy 时，退避超限的失败处理逻辑保持不变。
		"backoff limit exceeded; JobManagedBy enabled": {
			jobManagedBy:   true,
			parallelism:    2,
			completions:    3,
			backoffLimit:   0,
			completionMode: batch.IndexedCompletion,
			podsWithIndexes: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodFailed},
				{"2", v1.PodRunning},
			},
			expectedSucceeded:     1,
			expectedFailed:        2,
			expectedCompletedIdxs: "0",
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			expectedPodPatches:  3,
			expectedReady:       ptr.To[int32](0),
			expectedTerminating: ptr.To[int32](1),
			expectedDeletions:   1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			if !tc.jobManagedBy {
				// TODO: 将在 1.38 中移除。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.33"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.jobManagedBy,
			})
			// 控制器初始化
			clientSet := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})

			var fakeClock clock.WithTicker
			if tc.controllerTime != nil {
				fakeClock = clocktesting.NewFakeClock(*tc.controllerTime)
			} else {
				fakeClock = clocktesting.NewFakeClock(time.Now())
			}

			manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientSet, controller.NoResyncPeriodFunc, fakeClock)
			fakePodControl := controller.FakePodControl{Err: tc.podControllerError, CreateLimit: tc.podLimit}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady

			// 构造 Job 与 Pod
			job := newJob(tc.parallelism, tc.completions, tc.backoffLimit, tc.completionMode)
			job.Spec.Suspend = ptr.To(tc.suspend)
			job.Spec.PodReplacementPolicy = tc.podReplacementPolicy
			job.Spec.PodFailurePolicy = tc.podFailurePolicy
			if tc.initialStatus != nil {
				startTime := metav1.Now()
				job.Status.StartTime = &startTime
				job.Status.Active = int32(tc.initialStatus.active)
				job.Status.Succeeded = int32(tc.initialStatus.succeed)
				job.Status.Failed = int32(tc.initialStatus.failed)
				if tc.initialStatus.startTime != nil {
					startTime := metav1.NewTime(*tc.initialStatus.startTime)
					job.Status.StartTime = &startTime
				}
				job.Status.Conditions = append(job.Status.Conditions, tc.initialStatus.conditions...)
			}

			key, err := controller.KeyFunc(job)
			if err != nil {
				t.Errorf("Unexpected error getting job key: %v", err)
			}

			if tc.backoffRecord != nil {
				tc.backoffRecord.key = key
				manager.podBackoffStore.updateBackoffRecord(*tc.backoffRecord)
			}
			if tc.fakeExpectationAtCreation < 0 {
				manager.expectations.ExpectDeletions(logger, key, int(-tc.fakeExpectationAtCreation))
			} else if tc.fakeExpectationAtCreation > 0 {
				manager.expectations.ExpectCreations(logger, key, int(tc.fakeExpectationAtCreation))
			}
			if tc.wasSuspended {
				job.Status.Conditions = append(job.Status.Conditions, *newCondition(batch.JobSuspended, v1.ConditionTrue, "JobSuspended", "Job suspended", realClock.Now()))
			}
			if tc.deleting {
				now := metav1.Now()
				job.DeletionTimestamp = &now
			}
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			podIndexer := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer()
			setPodsStatuses(podIndexer, job, tc.pendingPods, tc.activePods, tc.succeededPods, tc.failedPods, tc.terminatingPods, tc.readyPods)
			setPodsStatusesWithIndexes(podIndexer, job, tc.podsWithIndexes)

			actual := job
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}

			// 执行同步
			err = manager.syncJob(ctx, testutil.GetKey(job, t))
			// podControl 报错时需要重新入队 syncJob 任务
			// 达到 CreateLimit 后应返回错误，以便任务重新入队。
			if tc.podControllerError != nil {
				if err == nil {
					t.Error("Syncing jobs expected to return error on podControl exception")
				}
			} else if tc.podLimit != 0 && fakePodControl.CreateCallCount > tc.podLimit {
				if err == nil {
					t.Error("Syncing jobs expected to return error when reached the podControl limit")
				}
			} else if err != nil {
				t.Errorf("Unexpected error when syncing jobs: %v", err)
			}
			// 校验创建/删除的 Pod
			if int32(len(fakePodControl.Templates)) != tc.expectedCreations {
				t.Errorf("Unexpected number of creates.  Expected %d, saw %d\n", tc.expectedCreations, len(fakePodControl.Templates))
			}
			if tc.completionMode == batch.IndexedCompletion {
				checkIndexedJobPods(t, &fakePodControl, tc.expectedCreatedIndexes, job.Name)
			} else {
				for _, p := range fakePodControl.Templates {
					// 假的 podControl 不会根据 owner reference 自动添加 generate name。
					if p.GenerateName != "" {
						t.Errorf("Got pod generate name %s, want %s", p.GenerateName, "")
					}
					if p.Spec.Hostname != "" {
						t.Errorf("Got pod hostname %q, want none", p.Spec.Hostname)
					}
				}
			}
			if int32(len(fakePodControl.DeletePodName)) != tc.expectedDeletions {
				t.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", tc.expectedDeletions, len(fakePodControl.DeletePodName))
			}
			// 每次创建都应带有对应的 ControllerRef。
			if len(fakePodControl.ControllerRefs) != int(tc.expectedCreations) {
				t.Errorf("Unexpected number of ControllerRefs.  Expected %d, saw %d\n", tc.expectedCreations, len(fakePodControl.ControllerRefs))
			}
			// 校验 ControllerRef 的内容。
			for _, controllerRef := range fakePodControl.ControllerRefs {
				if got, want := controllerRef.APIVersion, "batch/v1"; got != want {
					t.Errorf("controllerRef.APIVersion = %q, want %q", got, want)
				}
				if got, want := controllerRef.Kind, "Job"; got != want {
					t.Errorf("controllerRef.Kind = %q, want %q", got, want)
				}
				if got, want := controllerRef.Name, job.Name; got != want {
					t.Errorf("controllerRef.Name = %q, want %q", got, want)
				}
				if got, want := controllerRef.UID, job.UID; got != want {
					t.Errorf("controllerRef.UID = %q, want %q", got, want)
				}
				if controllerRef.Controller == nil || *controllerRef.Controller != true {
					t.Errorf("controllerRef.Controller is not set to true")
				}
			}
			// 校验状态
			if actual.Status.Active != tc.expectedActive {
				t.Errorf("Unexpected number of active pods.  Expected %d, saw %d\n", tc.expectedActive, actual.Status.Active)
			}
			if diff := cmp.Diff(tc.expectedReady, actual.Status.Ready); diff != "" {
				t.Errorf("Unexpected number of ready pods (-want,+got): %s", diff)
			}
			if actual.Status.Succeeded != tc.expectedSucceeded {
				t.Errorf("Unexpected number of succeeded pods.  Expected %d, saw %d\n", tc.expectedSucceeded, actual.Status.Succeeded)
			}
			if diff := cmp.Diff(tc.expectedCompletedIdxs, actual.Status.CompletedIndexes); diff != "" {
				t.Errorf("Unexpected completed indexes (-want,+got):\n%s", diff)
			}
			if actual.Status.Failed != tc.expectedFailed {
				t.Errorf("Unexpected number of failed pods.  Expected %d, saw %d\n", tc.expectedFailed, actual.Status.Failed)
			}
			if diff := cmp.Diff(tc.expectedTerminating, actual.Status.Terminating); diff != "" {
				t.Errorf("Unexpected number of terminating pods (-want,+got): %s", diff)
			}
			// 挂起时不应设置 StartTime，恢复后则必须存在。
			if actual.Status.StartTime != nil && tc.suspend {
				t.Error("Unexpected .status.startTime not nil when suspend is true")
			}
			if actual.Status.StartTime == nil && !tc.suspend {
				t.Error("Missing .status.startTime")
			}
			// 校验 condition
			if diff := cmp.Diff(tc.expectedConditions, actual.Status.Conditions, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected conditions (-want,+got):\n%s", diff)
			}
			// 校验 slow start 的批量上限
			expectedLimit := 0
			// 按 SlowStartInitialBatchSize 逐轮累加，推算出允许的最大创建调用次数。
			for pass := uint8(0); expectedLimit <= tc.podLimit; pass++ {
				expectedLimit += controller.SlowStartInitialBatchSize << pass
			}
			if tc.podLimit > 0 && fakePodControl.CreateCallCount > expectedLimit {
				t.Errorf("Unexpected number of create calls.  Expected <= %d, saw %d\n", fakePodControl.CreateLimit*2, fakePodControl.CreateCallCount)
			}
			if p := len(fakePodControl.Patches); p != tc.expectedPodPatches {
				t.Errorf("Got %d pod patches, want %d", p, tc.expectedPodPatches)
			}
		})
	}
}

// checkIndexedJobPods 校验索引型 Job 创建的 pod：完成索引须与期望集合一致，
// 且完成索引相关的环境变量、标签、主机名与生成名都应正确设置。
func checkIndexedJobPods(t *testing.T, control *controller.FakePodControl, wantIndexes sets.Set[int], jobName string) {
	t.Helper()
	gotIndexes := sets.New[int]()
	for _, p := range control.Templates {
		checkJobCompletionEnvVariable(t, &p.Spec)
		checkJobCompletionLabel(t, &p)
		ix := getCompletionIndex(p.Annotations)
		if ix == -1 {
			t.Errorf("Created pod %s didn't have completion index", p.Name)
		} else {
			gotIndexes.Insert(ix)
		}
		expectedName := fmt.Sprintf("%s-%d", jobName, ix)
		if expectedName != p.Spec.Hostname {
			t.Errorf("Got pod hostname %s, want %s", p.Spec.Hostname, expectedName)
		}
		expectedName += "-"
		if expectedName != p.GenerateName {
			t.Errorf("Got pod generate name %s, want %s", p.GenerateName, expectedName)
		}
	}
	if diff := cmp.Diff(sets.List(wantIndexes), sets.List(gotIndexes)); diff != "" {
		t.Errorf("Unexpected created completion indexes (-want,+got):\n%s", diff)
	}
}

// TestGetNewFinshedPods 验证 getNewFinishedPods 的计数逻辑：
// 覆盖已计数与未计数的终止 pod、预期已移除的 finalizer 以及删除中的 pod，断言成功与失败总数。
func TestGetNewFinshedPods(t *testing.T) {
	cases := map[string]struct {
		job                  batch.Job
		pods                 []*v1.Pod
		expectedRmFinalizers sets.Set[types.UID]
		wantSucceeded        int32
		wantFailed           int32
	}{
		// 已有计数与带 finalizer 的新终止 pod 混合，验证两部分计数正确合并。
		"some counted": {
			job: batch.Job{
				Status: batch.JobStatus{
					Succeeded:               2,
					Failed:                  1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).Pod,
				buildPod().uid("b").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodFailed).Pod,
				buildPod().uid("e").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("f").phase(v1.PodRunning).Pod,
			},
			wantSucceeded: 4,
			wantFailed:    2,
		},
		// 终止 pod 记录在 uncounted 中，验证它们与新终止 pod 一并计入总数。
		"some uncounted": {
			job: batch.Job{
				Status: batch.JobStatus{
					Succeeded: 1,
					Failed:    1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "c"},
						Failed:    []types.UID{"e", "f"},
					},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).Pod,
				buildPod().uid("b").phase(v1.PodSucceeded).Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("e").phase(v1.PodFailed).Pod,
				buildPod().uid("f").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("g").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			wantSucceeded: 4,
			wantFailed:    4,
		},
		// 部分 pod 的 finalizer 已被外部移除，验证它们不会再被当作待清理对象。
		"with expected removed finalizers": {
			job: batch.Job{
				Status: batch.JobStatus{
					Succeeded: 2,
					Failed:    2,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"d"},
					},
				},
			},
			expectedRmFinalizers: sets.New[types.UID]("b", "f"),
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).Pod,
				buildPod().uid("b").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodFailed).Pod,
				buildPod().uid("e").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("f").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("g").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			wantSucceeded: 4,
			wantFailed:    5,
		},
		// 删除中的 pod 即使未真正终止也按终态处理，验证该场景的成功与失败计数。
		"deleted pods": {
			job: batch.Job{
				Status: batch.JobStatus{
					Succeeded:               1,
					Failed:                  1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("c").phase(v1.PodRunning).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("d").phase(v1.PodPending).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("e").phase(v1.PodRunning).deletionTimestamp().Pod,
				buildPod().uid("f").phase(v1.PodPending).deletionTimestamp().Pod,
			},
			wantSucceeded: 2,
			wantFailed:    4,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			uncounted := newUncountedTerminatedPods(*tc.job.Status.UncountedTerminatedPods)
			jobCtx := &syncJobCtx{job: &tc.job, pods: tc.pods, uncounted: uncounted, expectedRmFinalizers: tc.expectedRmFinalizers}
			succeededPods, failedPods := getNewFinishedPods(jobCtx)
			// 最终计数由新终止 pod、历史计数与 uncounted 记录三部分相加得到。
			succeeded := int32(len(succeededPods)) + tc.job.Status.Succeeded + int32(len(uncounted.succeeded))
			failed := int32(len(failedPods)) + tc.job.Status.Failed + int32(len(uncounted.failed))
			if succeeded != tc.wantSucceeded {
				t.Errorf("getStatus reports %d succeeded pods, want %d", succeeded, tc.wantSucceeded)
			}
			if failed != tc.wantFailed {
				t.Errorf("getStatus reports %d succeeded pods, want %d", failed, tc.wantFailed)
			}
		})
	}
}

// TestTrackJobStatusAndRemoveFinalizers 验证状态跟踪与 finalizer 移除流程：
// 断言状态更新的顺序与内容、finalizer 移除数量、错误传播以及完成 pod 的指标上报。
func TestTrackJobStatusAndRemoveFinalizers(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	now := fakeClock.Now()
	minuteAgo := now.Add(-time.Minute)
	completedCond := newCondition(batch.JobComplete, v1.ConditionTrue, "", "", now)
	succeededCond := newCondition(batch.JobSuccessCriteriaMet, v1.ConditionTrue, "", "", minuteAgo)
	failedCond := newCondition(batch.JobFailed, v1.ConditionTrue, "", "", now)
	failureTargetCond := newCondition(batch.JobFailureTarget, v1.ConditionTrue, "", "", now)
	indexedCompletion := batch.IndexedCompletion
	mockErr := errors.New("mock error")
	cases := map[string]struct {
		job                     batch.Job
		pods                    []*v1.Pod
		finishedCond            *batch.JobCondition
		expectedRmFinalizers    sets.Set[types.UID]
		needsFlush              bool
		statusUpdateErr         error
		podControlErr           error
		wantErr                 error
		wantRmFinalizers        int
		wantStatusUpdates       []batch.JobStatus
		wantSucceededPodsMetric int
		wantFailedPodsMetric    int

		// 特性开关：是否启用 JobManagedBy。
		enableJobManagedBy bool
	}{
		// 无任何变更时不应产生状态更新或 finalizer 移除。
		"no updates": {},
		// 仅 active 数量变化时也要刷新状态，以便持久化新的计数。
		"new active": {
			job: batch.Job{
				Status: batch.JobStatus{
					Active: 1,
				},
			},
			needsFlush: true,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Active:                  1,
				},
			},
		},
		// 终止与删除中的 pod 先记入 uncounted，再 flush 为正式计数并移除 finalizer。
		"track finished pods": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("d").phase(v1.PodFailed).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().uid("e").phase(v1.PodPending).trackingFinalizer().deletionTimestamp().Pod,
				buildPod().phase(v1.PodPending).trackingFinalizer().Pod,
				buildPod().phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			wantRmFinalizers: 5,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "c"},
						Failed:    []types.UID{"b", "d", "e"},
					},
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               2,
					Failed:                  3,
				},
			},
			wantSucceededPodsMetric: 2,
			wantFailedPodsMetric:    3,
		},
		// 历史 uncounted 记录与本次新终止 pod 并存，验证两次状态更新中的计数递进。
		"past and new finished pods": {
			job: batch.Job{
				Status: batch.JobStatus{
					Active:    1,
					Succeeded: 2,
					Failed:    3,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "e"},
						Failed:    []types.UID{"b", "f"},
					},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("e").phase(v1.PodSucceeded).Pod,
				buildPod().phase(v1.PodFailed).Pod,
				buildPod().phase(v1.PodPending).Pod,
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			wantRmFinalizers: 4,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "c"},
						Failed:    []types.UID{"b", "d"},
					},
					Active:    1,
					Succeeded: 3,
					Failed:    4,
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Active:                  1,
					Succeeded:               5,
					Failed:                  6,
				},
			},
			wantSucceededPodsMetric: 3,
			wantFailedPodsMetric:    3,
		},
		// 已由外部清理 finalizer 的 pod 不应重复计数，验证剩余 pod 的计数与清理范围。
		"expecting removed finalizers": {
			job: batch.Job{
				Status: batch.JobStatus{
					Succeeded: 2,
					Failed:    3,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "g"},
						Failed:    []types.UID{"b", "h"},
					},
				},
			},
			expectedRmFinalizers: sets.New[types.UID]("c", "d", "g", "h"),
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("e").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("f").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("g").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("h").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			wantRmFinalizers: 4,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a", "e"},
						Failed:    []types.UID{"b", "f"},
					},
					Succeeded: 3,
					Failed:    4,
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               5,
					Failed:                  6,
				},
			},
			wantSucceededPodsMetric: 3,
			wantFailedPodsMetric:    3,
		},
		// 满足成功策略时，先记录 SuccessCriteriaMet，再补齐 Complete 条件与完成时间。
		"succeeding job by JobSuccessPolicy": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodPending).trackingFinalizer().Pod,
			},
			finishedCond:     succeededCond,
			wantRmFinalizers: 3,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
					Conditions: []batch.JobCondition{*succeededCond},
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  1,
					Conditions:              []batch.JobCondition{*succeededCond, *completedCond},
					CompletionTime:          &completedCond.LastTransitionTime,
				},
			},
			wantSucceededPodsMetric: 1,
			wantFailedPodsMetric:    1,
		},
		// 正常完成路径：验证计数落盘与 Complete 条件分别出现在两次状态更新中。
		"completing job": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			finishedCond:     completedCond,
			wantRmFinalizers: 2,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  1,
					Conditions:              []batch.JobCondition{*completedCond},
					CompletionTime:          &completedCond.LastTransitionTime,
				},
			},
			wantSucceededPodsMetric: 1,
			wantFailedPodsMetric:    1,
		},
		// 失败路径：验证终止条件写入状态，且运行中的 pod 也计入失败。
		"failing job": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("c").phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			finishedCond: failedCond,
			// 运行中的 pod 也计入失败数。
			wantRmFinalizers: 3,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b", "c"},
					},
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  2,
					Conditions:              []batch.JobCondition{*failedCond},
				},
			},
			wantSucceededPodsMetric: 1,
			wantFailedPodsMetric:    2,
		},
		// Job 删除中仍要移除全部 finalizer，但运行中的 pod 不计为失败。
		"deleted job": {
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{},
				},
				Status: batch.JobStatus{
					Active: 1,
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			// 运行中的 pod 会移除 finalizer，但不计入失败数。
			wantRmFinalizers: 3,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
					Active: 1,
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Active:                  1,
					Succeeded:               1,
					Failed:                  1,
				},
			},
			wantSucceededPodsMetric: 1,
			wantFailedPodsMetric:    1,
		},
		// 状态更新失败时立即返回错误，验证后续 finalizer 清理不会继续。
		"status update error": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			statusUpdateErr: mockErr,
			wantErr:         mockErr,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
				},
			},
		},
		// finalizer 移除失败时状态仍需落盘，验证错误向上传递且状态不被回滚。
		"pod patch errors": {
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			podControlErr:    mockErr,
			wantErr:          mockErr,
			wantRmFinalizers: 2,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
				},
			},
		},
		// 部分 pod 已无 finalizer：验证 patch 失败时仅清理剩余 finalizer，且计数照常推进。
		"pod patch errors with partial success": {
			job: batch.Job{
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"a"},
						Failed:    []types.UID{"b"},
					},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodSucceeded).Pod,
				buildPod().uid("c").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				buildPod().uid("d").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			podControlErr:    mockErr,
			wantErr:          mockErr,
			wantRmFinalizers: 2,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: []types.UID{"c"},
						Failed:    []types.UID{"d"},
					},
					Succeeded: 1,
					Failed:    1,
				},
			},
		},
		// 索引型 Job 的新成功 pod 更新 CompletedIndexes，重复的完成索引只计一次。
		"indexed job new successful pods": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](6),
				},
				Status: batch.JobStatus{
					Active: 1,
				},
			},
			pods: []*v1.Pod{
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("1").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("3").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("3").Pod,
				buildPod().phase(v1.PodRunning).trackingFinalizer().index("5").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantRmFinalizers: 4,
			wantStatusUpdates: []batch.JobStatus{
				{
					Active:                  1,
					Succeeded:               2,
					CompletedIndexes:        "1,3",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			wantSucceededPodsMetric: 2,
		},
		// 缩容后旧完成索引超出范围应被丢弃，验证 CompletedIndexes 可回退为空。
		"indexed job prev successful pods outside current completions index range with no new succeeded pods": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](2),
					Parallelism:    ptr.To[int32](2),
				},
				Status: batch.JobStatus{
					Active:           2,
					Succeeded:        1,
					CompletedIndexes: "3",
				},
			},
			pods: []*v1.Pod{
				buildPod().phase(v1.PodRunning).trackingFinalizer().index("0").Pod,
				buildPod().phase(v1.PodRunning).trackingFinalizer().index("1").Pod,
			},
			wantRmFinalizers: 0,
			wantStatusUpdates: []batch.JobStatus{
				{
					Active:                  2,
					Succeeded:               0,
					CompletedIndexes:        "",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
		},
		// 缩容后范围内出现新成功 pod，验证仅保留有效索引并更新成功计数。
		"indexed job prev successful pods outside current completions index range with new succeeded pods in range": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](2),
					Parallelism:    ptr.To[int32](2),
				},
				Status: batch.JobStatus{
					Active:           2,
					Succeeded:        1,
					CompletedIndexes: "3",
				},
			},
			pods: []*v1.Pod{
				buildPod().phase(v1.PodRunning).trackingFinalizer().index("0").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("1").Pod,
			},
			wantRmFinalizers: 1,
			wantStatusUpdates: []batch.JobStatus{
				{
					Active:                  2,
					Succeeded:               1,
					CompletedIndexes:        "1",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			wantSucceededPodsMetric: 1,
		},
		// 索引型 Job 的失败 pod 先记入 uncounted，再统一计入失败总数。
		"indexed job new failed pods": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](6),
				},
				Status: batch.JobStatus{
					Active: 1,
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodFailed).trackingFinalizer().index("1").Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().index("3").Pod,
				buildPod().uid("c").phase(v1.PodFailed).trackingFinalizer().index("3").Pod,
				buildPod().uid("d").phase(v1.PodRunning).trackingFinalizer().index("5").Pod,
				buildPod().phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			wantRmFinalizers: 4,
			wantStatusUpdates: []batch.JobStatus{
				{
					Active: 1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a", "b", "c"},
					},
				},
				{
					Active:                  1,
					Failed:                  3,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			wantFailedPodsMetric: 3,
		},
		// 历史完成索引与新增成功、失败 pod 并存，验证索引合并与计数更新。
		"indexed job past and new pods": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](7),
				},
				Status: batch.JobStatus{
					Failed:           2,
					Succeeded:        5,
					CompletedIndexes: "0-2,4,6,7",
				},
			},
			pods: []*v1.Pod{
				buildPod().phase(v1.PodSucceeded).index("0").Pod,
				buildPod().phase(v1.PodFailed).index("1").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("1").Pod,
				buildPod().phase(v1.PodSucceeded).trackingFinalizer().index("3").Pod,
				buildPod().uid("a").phase(v1.PodFailed).trackingFinalizer().index("2").Pod,
				buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().index("5").Pod,
			},
			wantRmFinalizers: 4,
			wantStatusUpdates: []batch.JobStatus{
				{
					Succeeded:        6,
					Failed:           2,
					CompletedIndexes: "0-4,6",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a", "b"},
					},
				},
				{
					Succeeded:               6,
					Failed:                  4,
					CompletedIndexes:        "0-4,6",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			wantSucceededPodsMetric: 1,
			wantFailedPodsMetric:    2,
		},
		// 构造大量终止 pod，验证单次更新能容纳全部结果而不丢失计数。
		"too many finished": {
			job: batch.Job{
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a", "b"},
					},
				},
			},
			pods: func() []*v1.Pod {
				pods := make([]*v1.Pod, 500)
				for i := range pods {
					pods[i] = buildPod().uid(strconv.Itoa(i)).phase(v1.PodSucceeded).trackingFinalizer().Pod
				}
				pods = append(pods, buildPod().uid("b").phase(v1.PodFailed).trackingFinalizer().Pod)
				return pods
			}(),
			wantRmFinalizers: 499,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Succeeded: func() []types.UID {
							uids := make([]types.UID, 499)
							for i := range uids {
								uids[i] = types.UID(strconv.Itoa(i))
							}
							return uids
						}(),
						Failed: []types.UID{"b"},
					},
					Failed: 1,
				},
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"b"},
					},
					Succeeded: 499,
					Failed:    1,
				},
			},
			wantSucceededPodsMetric: 499,
			wantFailedPodsMetric:    1,
		},
		// 大量完成索引需压缩为区间形式，验证 CompletedIndexes 的表示与计数上限处理。
		"too many indexed finished": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode: &indexedCompletion,
					Completions:    ptr.To[int32](501),
				},
			},
			pods: func() []*v1.Pod {
				pods := make([]*v1.Pod, 501)
				for i := range pods {
					pods[i] = buildPod().uid(strconv.Itoa(i)).index(strconv.Itoa(i)).phase(v1.PodSucceeded).trackingFinalizer().Pod
				}
				return pods
			}(),
			wantRmFinalizers: 500,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					CompletedIndexes:        "0-499",
					Succeeded:               500,
				},
			},
			wantSucceededPodsMetric: 500,
		},
		// 同一 pod 从失败翻转为成功时以当前状态为准，验证最终计数与失败条件一致。
		"pod flips from failed to succeeded": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](2),
					Parallelism: ptr.To[int32](2),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a", "b"},
					},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodFailed).trackingFinalizer().Pod,
				buildPod().uid("b").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			finishedCond:     failedCond,
			wantRmFinalizers: 2,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Failed:                  2,
					Conditions:              []batch.JobCondition{*failedCond},
				},
			},
			wantFailedPodsMetric: 2,
		},
		// pod 已进入终态，期望立即设置 JobFailed 条件。
		"pod is terminal; JobFailed condition is set": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a"},
					},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodFailed).trackingFinalizer().Pod,
			},
			finishedCond:     failedCond,
			wantRmFinalizers: 1,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Failed:                  1,
					Conditions:              []batch.JobCondition{*failedCond},
				},
			},
			wantFailedPodsMetric: 1,
		},
		// pod 仍在终止中时失败计数先行更新，但 JobFailed 条件被延后设置。
		"pod is terminating; counted as failed, but the JobFailed condition is delayed": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a"},
					},
					Conditions: []batch.JobCondition{*failureTargetCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: failedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Failed:                  1,
					Conditions:              []batch.JobCondition{*failureTargetCond},
				},
			},
			wantFailedPodsMetric: 1,
		},
		// 同上延后场景，验证启用 JobManagedBy 后行为保持一致。
		"pod is terminating; counted as failed, but the JobFailed condition is delayed; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a"},
					},
					Conditions: []batch.JobCondition{*failureTargetCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: failedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Failed:                  1,
					Conditions:              []batch.JobCondition{*failureTargetCond},
				},
			},
			wantFailedPodsMetric: 1,
		},
		// 禁用 JobManagedBy 时，终止中的 pod 不延迟失败条件。
		"pod is terminating; counted as failed, JobFailed condition is not delayed; JobManagedBy disabled": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a"},
					},
					Conditions: []batch.JobCondition{*failureTargetCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: failedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Failed:                  1,
					Conditions:              []batch.JobCondition{*failureTargetCond},
				},
			},
			wantFailedPodsMetric: 1,
		},
		// pod 终止中且已满足成功条件时，验证禁用 JobManagedBy 会延后 JobComplete 条件。
		"pod is terminating; JobSuccessCriteriaMet, but JobComplete condition is delayed; JobManagedBy disabled": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed:    []types.UID{"a"},
						Succeeded: []types.UID{"b"},
					},
					Conditions: []batch.JobCondition{*succeededCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: completedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  1,
					Conditions:              []batch.JobCondition{*succeededCond},
				},
			},
			wantFailedPodsMetric:    1,
			wantSucceededPodsMetric: 1,
		},
		// 同上成功场景，验证启用 JobManagedBy 后完成条件的处理。
		"pod is terminating; JobSuccessCriteriaMet, but JobComplete condition is delayed; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed:    []types.UID{"a"},
						Succeeded: []types.UID{"b"},
					},
					Conditions: []batch.JobCondition{*succeededCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: completedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  1,
					Conditions:              []batch.JobCondition{*succeededCond},
				},
			},
			wantFailedPodsMetric:    1,
			wantSucceededPodsMetric: 1,
		},
		// 禁用 JobManagedBy 时不延迟完成条件，验证状态中保留 SuccessCriteriaMet 与最终计数。
		"pod is terminating; JobSuccessCriteriaMet, JobComplete condition is not delayed; JobManagedBy disabled": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions: ptr.To[int32](1),
					Parallelism: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed:    []types.UID{"a"},
						Succeeded: []types.UID{"b"},
					},
					Conditions: []batch.JobCondition{*succeededCond},
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodRunning).deletionTimestamp().Pod,
			},
			finishedCond: completedCond,
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Succeeded:               1,
					Failed:                  1,
					Conditions:              []batch.JobCondition{*succeededCond},
				},
			},
			wantFailedPodsMetric:    1,
			wantSucceededPodsMetric: 1,
		},

		// 失败 pod 延迟移除 finalizer 且未达索引重试上限，验证不计数且仅更新索引标记。
		"indexed job with a failed pod with delayed finalizer removal; the pod is not counted": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode:       &indexedCompletion,
					Completions:          ptr.To[int32](6),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().index("1").Pod,
			},
			wantStatusUpdates: []batch.JobStatus{
				{
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					FailedIndexes:           ptr.To(""),
				},
			},
		},
		// 失败索引被新 pod 接管后，旧失败 pod 才可计入总数。
		"indexed job with a failed pod which is recreated by a running pod; the pod is counted": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode:       &indexedCompletion,
					Completions:          ptr.To[int32](6),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
				Status: batch.JobStatus{
					Active: 1,
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a1").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().index("1").Pod,
				buildPod().uid("a2").phase(v1.PodRunning).indexFailureCount("1").trackingFinalizer().index("1").Pod,
			},
			wantRmFinalizers: 1,
			wantStatusUpdates: []batch.JobStatus{
				{
					Active: 1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a1"},
					},
					FailedIndexes: ptr.To(""),
				},
				{
					Active:                  1,
					Failed:                  1,
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					FailedIndexes:           ptr.To(""),
				},
			},
			wantFailedPodsMetric: 1,
		},
		// 失败 pod 所属索引已达重试上限，验证计入失败数并标记 FailedIndexes。
		"indexed job with a failed pod for a failed index; the pod is counted": {
			job: batch.Job{
				Spec: batch.JobSpec{
					CompletionMode:       &indexedCompletion,
					Completions:          ptr.To[int32](6),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().index("1").Pod,
			},
			wantRmFinalizers: 1,
			wantStatusUpdates: []batch.JobStatus{
				{
					FailedIndexes: ptr.To("1"),
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{
						Failed: []types.UID{"a"},
					},
				},
				{
					Failed:                  1,
					FailedIndexes:           ptr.To("1"),
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				},
			},
			wantFailedPodsMetric: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if !tc.enableJobManagedBy {
				// TODO: 该特性门控模拟将在 1.38 中移除。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.32"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})

			clientSet := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, _ := newControllerFromClientWithClock(ctx, t, clientSet, controller.NoResyncPeriodFunc, fakeClock)
			fakePodControl := controller.FakePodControl{Err: tc.podControlErr}
			// 每个用例重置计数器，避免累积值影响指标断言。
			metrics.JobPodsFinished.Reset()
			manager.podControl = &fakePodControl
			var statusUpdates []batch.JobStatus
			// 捕获每次状态更新，便于断言状态变更的顺序与次数。
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				statusUpdates = append(statusUpdates, *job.Status.DeepCopy())
				return job, tc.statusUpdateErr
			}
			job := tc.job.DeepCopy()
			if job.Status.UncountedTerminatedPods == nil {
				job.Status.UncountedTerminatedPods = &batch.UncountedTerminatedPods{}
			}
			jobCtx := &syncJobCtx{
				job:                  job,
				pods:                 tc.pods,
				uncounted:            newUncountedTerminatedPods(*job.Status.UncountedTerminatedPods),
				expectedRmFinalizers: tc.expectedRmFinalizers,
				finishedCondition:    tc.finishedCond,
			}
			jobCtx.activePods = controller.FilterActivePods(logger, tc.pods)
			// 索引型 Job 需额外准备完成索引等上下文，供状态跟踪使用。
			if isIndexedJob(job) {
				jobCtx.succeededIndexes = parseIndexesFromString(logger, job.Status.CompletedIndexes, int(*job.Spec.Completions))
				if job.Spec.BackoffLimitPerIndex != nil {
					jobCtx.failedIndexes = calculateFailedIndexes(logger, job, tc.pods)
					jobCtx.podsWithDelayedDeletionPerIndex = getPodsWithDelayedDeletionPerIndex(logger, jobCtx)
				}
			}
			jobCtx.terminating = ptr.To(controller.CountTerminatingPods(tc.pods))

			// 调用被测函数，随后分别校验错误、状态序列、finalizer 与指标。
			err := manager.trackJobStatusAndRemoveFinalizers(ctx, jobCtx, tc.needsFlush)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Got error %v, want %v", err, tc.wantErr)
			}
			// 逐次比对状态更新并忽略时间戳，聚焦计数与条件的变化。
			if diff := cmp.Diff(tc.wantStatusUpdates, statusUpdates,
				cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("Unexpected status updates (-want,+got):\n%s", diff)
			}
			// patch 次数即被移除的 finalizer 数，用于验证哪些用例执行了清理。
			rmFinalizers := len(fakePodControl.Patches)
			if rmFinalizers != tc.wantRmFinalizers {
				t.Errorf("Removed %d finalizers, want %d", rmFinalizers, tc.wantRmFinalizers)
			}
			// 仅在成功路径校验指标，错误注入用例会在状态落盘前中断。
			if tc.wantErr == nil {
				completionMode := completionModeStr(job)
				// 分别读取成功与失败的完成 pod 计数，确认只统计预期数量。
				v, err := metricstestutil.GetCounterMetricValue(metrics.JobPodsFinished.WithLabelValues(completionMode, metrics.Succeeded))
				if err != nil {
					t.Fatalf("Obtaining succeeded job_pods_finished_total: %v", err)
				}
				if float64(tc.wantSucceededPodsMetric) != v {
					t.Errorf("Metric reports %.0f succeeded pods, want %d", v, tc.wantSucceededPodsMetric)
				}
				v, err = metricstestutil.GetCounterMetricValue(metrics.JobPodsFinished.WithLabelValues(completionMode, metrics.Failed))
				if err != nil {
					t.Fatalf("Obtaining failed job_pods_finished_total: %v", err)
				}
				if float64(tc.wantFailedPodsMetric) != v {
					t.Errorf("Metric reports %.0f failed pods, want %d", v, tc.wantFailedPodsMetric)
				}
			}
		})
	}
}

// TestSyncJobPastDeadline 验证单次 syncJob 调用对 active deadline 的跟踪：
// 超时后应删除活跃 pod、更新计数并设置失败相关条件。
func TestSyncJobPastDeadline(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	testCases := map[string]struct {
		// 任务配置
		parallelism           int32
		completions           int32
		activeDeadlineSeconds int64
		startTime             int64
		backoffLimit          int32
		suspend               bool

		// pod 配置
		activePods    int
		succeededPods int
		failedPods    int

		// 预期结果
		expectedDeletions  int32
		expectedActive     int32
		expectedSucceeded  int32
		expectedFailed     int32
		expectedConditions []batch.JobCondition

		// 特性开关：是否启用 JobManagedBy。
		enableJobManagedBy bool
	}{
		// 截止时间短于 pod 执行时间：期望删除活跃 pod 并进入失败目标状态。
		"activeDeadlineSeconds less than single pod execution": {
			parallelism:           1,
			completions:           1,
			activeDeadlineSeconds: 10,
			startTime:             15,
			backoffLimit:          6,
			activePods:            1,
			expectedDeletions:     1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
			},
		},
		// 截止时间足够完成首个 pod，验证已成功数保留且剩余活跃 pod 被删除。
		"activeDeadlineSeconds bigger than single pod execution": {
			parallelism:           1,
			completions:           2,
			activeDeadlineSeconds: 10,
			startTime:             15,
			backoffLimit:          6,
			activePods:            1,
			succeededPods:         1,
			expectedDeletions:     1,
			expectedSucceeded:     1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
			},
		},
		// 无任何 pod 启动时即超时，期望同时设置 FailureTarget 与 Failed 条件。
		"activeDeadlineSeconds times-out before any pod starts": {
			parallelism:           1,
			completions:           1,
			activeDeadlineSeconds: 10,
			startTime:             10,
			backoffLimit:          6,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
			},
		},
		// 重试次数先于截止时间耗尽，期望失败原因取 backoff limit 而非截止时间。
		"activeDeadlineSeconds with backofflimit reach": {
			parallelism:           1,
			completions:           1,
			activeDeadlineSeconds: 1,
			startTime:             10,
			failedPods:            1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		// 暂停的 Job 不推进截止时间，验证仅保留 Suspended 条件。
		"activeDeadlineSeconds is not triggered when Job is suspended": {
			suspend:               true,
			parallelism:           1,
			completions:           2,
			activeDeadlineSeconds: 10,
			startTime:             15,
			backoffLimit:          6,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionTrue,
					Reason:  "JobSuspended",
					Message: "Job suspended",
				},
			},
		},
		// 截止时间超时的常规路径，验证删除动作与部分成功计数的保留。
		"activeDeadlineSeconds exceeded": {
			parallelism:           1,
			completions:           2,
			activeDeadlineSeconds: 10,
			startTime:             15,
			backoffLimit:          6,
			activePods:            1,
			succeededPods:         1,
			expectedDeletions:     1,
			expectedSucceeded:     1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
			},
		},
		// 启用 JobManagedBy 后重复超时场景，验证行为保持一致。
		"activeDeadlineSeconds exceeded; JobManagedBy enabled": {
			enableJobManagedBy: true,

			parallelism:           1,
			completions:           2,
			activeDeadlineSeconds: 10,
			startTime:             15,
			backoffLimit:          6,
			activePods:            1,
			succeededPods:         1,
			expectedDeletions:     1,
			expectedSucceeded:     1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonDeadlineExceeded,
					Message: "Job was active longer than specified deadline",
				},
			},
		},
		// 双重失败条件下，验证最终原因以先触发的 backoff limit 为准。
		"activeDeadlineSeconds exceeded and backofflimit reached; JobManagedBy enabled": {
			enableJobManagedBy: true,

			parallelism:           1,
			completions:           1,
			activeDeadlineSeconds: 1,
			startTime:             10,
			failedPods:            1,
			expectedFailed:        1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.enableJobManagedBy {
				// TODO: this will be removed in 1.38.
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})
			// 控制器初始化
			clientSet := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientSet, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			// 用 FakePodControl 捕获删除请求，以断言超时后的清理动作。
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			var actual *batch.Job
			// 捕获最终状态，供后续校验计数与条件。
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}

			// 任务与 pod 配置
			job := newJob(tc.parallelism, tc.completions, tc.backoffLimit, batch.NonIndexedCompletion)
			job.Spec.ActiveDeadlineSeconds = &tc.activeDeadlineSeconds
			job.Spec.Suspend = ptr.To(tc.suspend)
			// 按 startTime 回推启动时刻，使 syncJob 判定截止时间已过。
			start := metav1.Unix(metav1.Now().Time.Unix()-tc.startTime, 0)
			job.Status.StartTime = &start
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			podIndexer := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer()
			// 在 informer 中预置指定数量的活跃、成功与失败 pod。
			setPodsStatuses(podIndexer, job, 0, tc.activePods, tc.succeededPods, tc.failedPods, 0, 0)

			// 执行同步
			err := manager.syncJob(ctx, testutil.GetKey(job, t))
			if err != nil {
				t.Errorf("Unexpected error when syncing jobs %v", err)
			}
			// 校验创建与删除的 pod 数量
			if int32(len(fakePodControl.Templates)) != 0 {
				t.Errorf("Unexpected number of creates.  Expected 0, saw %d\n", len(fakePodControl.Templates))
			}
			if int32(len(fakePodControl.DeletePodName)) != tc.expectedDeletions {
				t.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", tc.expectedDeletions, len(fakePodControl.DeletePodName))
			}
			// 校验状态字段
			if actual.Status.Active != tc.expectedActive {
				t.Errorf("Unexpected number of active pods.  Expected %d, saw %d\n", tc.expectedActive, actual.Status.Active)
			}
			if actual.Status.Succeeded != tc.expectedSucceeded {
				t.Errorf("Unexpected number of succeeded pods.  Expected %d, saw %d\n", tc.expectedSucceeded, actual.Status.Succeeded)
			}
			if actual.Status.Failed != tc.expectedFailed {
				t.Errorf("Unexpected number of failed pods.  Expected %d, saw %d\n", tc.expectedFailed, actual.Status.Failed)
			}
			// 超时判断依赖 startTime，缺失会导致后续逻辑无法复现。
			if actual.Status.StartTime == nil {
				t.Error("Missing .status.startTime")
			}
			// 校验 condition
			if diff := cmp.Diff(tc.expectedConditions, actual.Status.Conditions, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected conditions (-want,+got):\n%s", diff)
			}
		})
	}
}

// getCondition 判断 Job 上是否存在类型、状态和原因均匹配的 condition。
func getCondition(job *batch.Job, condition batch.JobConditionType, status v1.ConditionStatus, reason string) bool {
	for _, v := range job.Status.Conditions {
		if v.Type == condition && v.Status == status && v.Reason == reason {
			return true
		}
	}
	return false
}

// TestPastDeadlineJobFinished 验证 Job 会被持续跟踪，直到达到活跃截止时间（active deadline）；
// 到达该时间点后，Job 应被标记为 Failed。
func TestPastDeadlineJobFinished(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	clientset := fake.NewClientset()
	fakeClock := clocktesting.NewFakeClock(time.Now().Truncate(time.Second))
	// 使用假时钟，便于精确推进时间以验证截止时间到期逻辑。
	manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	manager.expectations = FakeJobExpectations{
		controller.NewControllerExpectations(), true, func() {
		},
	}
	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	// 启动控制器，使其通过 informer 异步协调创建的 Job。
	go manager.Run(ctx, 1)

	tests := []struct {
		name         string
		setStartTime bool
		jobName      string
	}{
		{
			name:         "New job created without start time being set",
			setStartTime: false,
			jobName:      "job1",
		},
		{
			name:         "New job created with start time being set",
			setStartTime: true,
			jobName:      "job2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 两个用例分别覆盖未设置和已设置 startTime 的场景，确保控制器都能补上 startTime。
			job := newJobWithName(tc.jobName, 1, 1, 6, batch.NonIndexedCompletion)
			job.Spec.ActiveDeadlineSeconds = ptr.To[int64](1)
			if tc.setStartTime {
				start := metav1.NewTime(fakeClock.Now())
				job.Status.StartTime = &start
			}

			// 通过 API 创建 Job，交由运行中的控制器进行协调。
			_, err := clientset.BatchV1().Jobs(job.GetNamespace()).Create(ctx, job, metav1.CreateOptions{})
			if err != nil {
				t.Errorf("Could not create Job: %v", err)
			}

			var j *batch.Job
			// 先等待控制器为 Job 写入 startTime，后续截止时间判断依赖该字段。
			err = wait.PollUntilContextTimeout(ctx, 200*time.Microsecond, 3*time.Second, true, func(ctx context.Context) (done bool, err error) {
				j, err = clientset.BatchV1().Jobs(metav1.NamespaceDefault).Get(ctx, job.GetName(), metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				return j.Status.StartTime != nil, nil
			})
			if err != nil {
				t.Errorf("Job failed to ensure that start time was set: %v", err)
			}
			// 再轮询直到出现 DeadlineExceeded 条件，并随时间推进FakeClock。
			err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 3*time.Second, false, func(ctx context.Context) (done bool, err error) {
				j, err = clientset.BatchV1().Jobs(metav1.NamespaceDefault).Get(ctx, job.GetName(), metav1.GetOptions{})
				if err != nil {
					return false, nil
				}
				if getCondition(j, batch.JobFailed, v1.ConditionTrue, batch.JobReasonDeadlineExceeded) {
					// 截止条件不应早于 activeDeadlineSeconds 到点时出现。
					if manager.clock.Since(j.Status.StartTime.Time) < time.Duration(*j.Spec.ActiveDeadlineSeconds)*time.Second {
						return true, errors.New("Job contains DeadlineExceeded condition earlier than expected")
					}
					return true, nil
				}
				manager.clock.Sleep(100 * time.Millisecond)
				return false, nil
			})
			if err != nil {
				t.Errorf("Job failed to enforce activeDeadlineSeconds configuration. Expected condition with Reason 'DeadlineExceeded' was not found in %v", j.Status)
			}
		})
	}
}

// TestSingleJobFailedCondition 验证活跃时间超过截止时间后，Failure 条件被置为 True，且不会重复添加。
func TestSingleJobFailedCondition(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	var actual *batch.Job
	manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
		actual = job
		return job, nil
	}

	job := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job.Spec.ActiveDeadlineSeconds = ptr.To[int64](10)
	start := metav1.Unix(metav1.Now().Time.Unix()-15, 0)
	job.Status.StartTime = &start
	// 预置一个 Failure=False 的条件，验证同步时原地更新而非新增重复条件。
	job.Status.Conditions = append(job.Status.Conditions, *newCondition(batch.JobFailed, v1.ConditionFalse, "DeadlineExceeded", "Job was active longer than specified deadline", realClock.Now()))
	sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
	err := manager.syncJob(ctx, testutil.GetKey(job, t))
	if err != nil {
		t.Errorf("Unexpected error when syncing jobs %v", err)
	}
	if len(fakePodControl.DeletePodName) != 0 {
		t.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", 0, len(fakePodControl.DeletePodName))
	}
	// 场景只涉及状态更新，不应触发 Pod 删除。
	if actual == nil {
		t.Error("Expected job modification\n")
	}
	// 断言 Failure 条件恰好只有一个，且状态已翻转为 True。
	failedConditions := getConditionsByType(actual.Status.Conditions, batch.JobFailed)
	if len(failedConditions) != 1 {
		t.Error("Unexpected number of failed conditions\n")
	}
	if failedConditions[0].Status != v1.ConditionTrue {
		t.Errorf("Unexpected status for the failed condition. Expected: %v, saw %v\n", v1.ConditionTrue, failedConditions[0].Status)
	}

}

// TestSyncJobComplete 验证已完成的 Job 再次同步时，状态条件保持不变。
func TestSyncJobComplete(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	job := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job.Status.Conditions = append(job.Status.Conditions, *newCondition(batch.JobComplete, v1.ConditionTrue, "", "", realClock.Now()))
	sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
	err := manager.syncJob(ctx, testutil.GetKey(job, t))
	if err != nil {
		t.Fatalf("Unexpected error when syncing jobs %v", err)
	}
	actual, err := manager.jobLister.Jobs(job.Namespace).Get(job.Name)
	if err != nil {
		t.Fatalf("Unexpected error when trying to get job from the store: %v", err)
	}
	// 验证已完成 Job 同步后条件保持一致。
	if got, expected := len(actual.Status.Conditions), 1; got != expected {
		t.Fatalf("Unexpected job status conditions amount; expected %d, got %d", expected, got)
	}
}

// TestSyncJobDeleted 验证对不存在的 Job（已从存储中删除）执行同步不会创建或删除任何 Pod。
func TestSyncJobDeleted(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, _ := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
		return job, nil
	}
	job := newJob(2, 2, 6, batch.NonIndexedCompletion)
	if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
		t.Fatalf("error %v while reconciling the job %v", err, testutil.GetKey(job, t))
	}
	// Job 已被删除，协调逻辑不应再创建 Pod。
	if len(fakePodControl.Templates) != 0 {
		t.Errorf("Unexpected number of creates.  Expected %d, saw %d\n", 0, len(fakePodControl.Templates))
	}
	if len(fakePodControl.DeletePodName) != 0 {
		t.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", 0, len(fakePodControl.DeletePodName))
	}
}

// TestSyncJobWhenManagedBy 验证 managedBy 字段与 JobManagedBy 特性开关的组合是否影响 Job 状态的更新。
func TestSyncJobWhenManagedBy(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	now := metav1.Now()
	baseJob := batch.Job{
		TypeMeta: metav1.TypeMeta{Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foobar",
			UID:       "foobaruid",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: batch.JobSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Image: "foo/bar"},
					},
				},
			},
			Parallelism:  ptr.To[int32](2),
			Completions:  ptr.To[int32](2),
			BackoffLimit: ptr.To[int32](6),
		},
		Status: batch.JobStatus{
			Active:    1,
			Ready:     ptr.To[int32](1),
			StartTime: &now,
		},
	}

	// 四个用例分别覆盖：特性开启但 managedBy 为自定义值（不接管）、特性开启且 managedBy 为本控制器或为空（接管并更新状态）、特性关闭（始终更新状态）。
	testCases := map[string]struct {
		enableJobManagedBy bool
		job                batch.Job
		wantStatus         batch.JobStatus
	}{
		"job with custom value of managedBy; feature enabled; the status is unchanged": {
			enableJobManagedBy: true,
			job: func() batch.Job {
				job := baseJob.DeepCopy()
				job.Spec.ManagedBy = ptr.To("custom-managed-by")
				return *job
			}(),
			wantStatus: baseJob.Status,
		},
		"job with well known value of the managedBy; feature enabled; the status is updated": {
			enableJobManagedBy: true,
			job: func() batch.Job {
				job := baseJob.DeepCopy()
				job.Spec.ManagedBy = ptr.To(batch.JobControllerName)
				return *job
			}(),
			wantStatus: batch.JobStatus{
				Active:                  2,
				Ready:                   ptr.To[int32](0),
				StartTime:               &now,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
		"job with custom value of managedBy; feature disabled; the status is updated": {
			job: func() batch.Job {
				job := baseJob.DeepCopy()
				job.Spec.ManagedBy = ptr.To("custom-managed-by")
				return *job
			}(),
			wantStatus: batch.JobStatus{
				Active:                  2,
				Ready:                   ptr.To[int32](0),
				StartTime:               &now,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
		"job without the managedBy; feature enabled; the status is updated": {
			enableJobManagedBy: true,
			job:                baseJob,
			wantStatus: batch.JobStatus{
				Active:                  2,
				Ready:                   ptr.To[int32](0),
				StartTime:               &now,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 将模拟版本固定为 1.34，使特性在该版本下的默认行为稳定可测。
			// TODO: this will be removed in 1.38.
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, tc.enableJobManagedBy)

			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			job := &tc.job

			actual := job
			manager.updateStatusHandler = func(_ context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}
			if err := sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job); err != nil {
				t.Fatalf("error %v while adding the %v job to the index", err, klog.KObj(job))
			}

			if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
				t.Fatalf("error %v while reconciling the job %v", err, testutil.GetKey(job, t))
			}

			// 断言同步后的状态与预期一致，从而验证 managedBy 是否阻止了控制器接管。
			if diff := cmp.Diff(tc.wantStatus, actual.Status); diff != "" {
				t.Errorf("Unexpected job status (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestSyncJobWithJobPodFailurePolicy 验证 Pod 失败策略（OnExitCodes/OnPodConditions）对失败 Pod 的处理：
// 包括 FailJob、Ignore、Count 三种动作，以及规则匹配顺序、FailureTarget 条件设置和状态计数的正确性。
func TestSyncJobWithJobPodFailurePolicy(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	now := metav1.Now()
	indexedCompletionMode := batch.IndexedCompletion
	validObjectMeta := metav1.ObjectMeta{
		Name:      "foobar",
		UID:       uuid.NewUUID(),
		Namespace: metav1.NamespaceDefault,
	}
	validSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"foo": "bar"},
	}
	validTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Image: "foo/bar"},
			},
		},
	}

	// 公共规则：退出码 1-3 忽略，5-7 使 Job 失败；多个用例复用。
	onExitCodeRules := []batch.PodFailurePolicyRule{
		{
			Action: batch.PodFailurePolicyActionIgnore,
			OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
				Operator: batch.PodFailurePolicyOnExitCodesOpIn,
				Values:   []int32{1, 2, 3},
			},
		},
		{
			Action: batch.PodFailurePolicyActionFailJob,
			OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
				Operator: batch.PodFailurePolicyOnExitCodesOpIn,
				Values:   []int32{5, 6, 7},
			},
		},
	}

	// 表驱动用例覆盖各类失败策略场景，断言最终的条件集合及 active/failed/succeeded/terminating 计数。
	testCases := map[string]struct {
		enableJobManagedBy    bool
		job                   batch.Job
		pods                  []v1.Pod
		wantConditions        []batch.JobCondition
		wantStatusFailed      int32
		wantStatusActive      int32
		wantStatusSucceeded   int32
		wantStatusTerminating *int32
	}{
		// 规则限定了 containerName；实际失败的容器名不匹配时应走默认处理（计入失败）而非 Ignore/FailJob。
		"default handling for pod failure if the container matching the exit codes does not match the containerName restriction": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									ContainerName: ptr.To("main-container"),
									Operator:      batch.PodFailurePolicyOnExitCodesOpIn,
									Values:        []int32{1, 2, 3},
								},
							},
							{
								Action: batch.PodFailurePolicyActionFailJob,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									ContainerName: ptr.To("main-container"),
									Operator:      batch.PodFailurePolicyOnExitCodesOpIn,
									Values:        []int32{5, 6, 7},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "monitoring-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode:   42,
										FinishedAt: testFinishedAt,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusSucceeded: 0,
			wantStatusFailed:    1,
		},
		// 退出码规则仅对失败 Pod 生效；运行中的 Pod 即使容器有终止状态也不能触发 FailJob。
		"running pod should not result in job fail based on OnExitCodes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    0,
			wantStatusSucceeded: 0,
		},
		// 退出码命中 FailJob 规则：期望添加 FailureTarget 与 Failed 两个条件并停止活跃 Pod。
		"fail job based on OnExitCodes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// Job 已有 FailureTarget 条件时再次同步，消息应复用，并补上 Failed 条件。
		"job marked already as failure target with failed pod": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
				Status: batch.JobStatus{
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobFailureTarget,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonPodFailurePolicy,
							Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 失败 Pod 已被删除的场景：条件消息保留原 Pod 名称，不会因 Pod 消失而改写。
		"job marked already as failure target with failed pod, message based on already deleted pod": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
				Status: batch.JobStatus{
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobFailureTarget,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonPodFailurePolicy,
							Message: "Container main-container for pod default/already-deleted-pod failed with exit code 5 matching FailJob rule at index 1",
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/already-deleted-pod failed with exit code 5 matching FailJob rule at index 1",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/already-deleted-pod failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 多个 Pod 场景下，一个失败 Pod 触发 FailJob，其余活跃 Pod 处于终止中（terminating）。
		"fail job with multiple pods": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](2),
					Completions:  ptr.To[int32](2),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
					},
				},
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-1 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:      0,
			wantStatusFailed:      2,
			wantStatusTerminating: ptr.To[int32](1),
			wantStatusSucceeded:   0,
		},
		// 开启 JobManagedBy 时，终态条件会延迟设置，但 FailureTarget 与计数行为保持一致。
		"fail job with multiple pods; JobManagedBy enabled delays setting terminal condition": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](2),
					Completions:  ptr.To[int32](2),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
					},
				},
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-1 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:      0,
			wantStatusFailed:      2,
			wantStatusTerminating: ptr.To[int32](1),
			wantStatusSucceeded:   0,
		},
		// Indexed 完成模式下，退出码规则同样适用，消息中使用带索引的 Pod 名称。
		"fail indexed job based on OnExitCodes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: &indexedCompletionMode,
					Parallelism:    ptr.To[int32](1),
					Completions:    ptr.To[int32](1),
					BackoffLimit:   ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// NotIn 操作符下，退出码不在排除列表中即命中 FailJob 规则。
		"fail job based on OnExitCodes with NotIn operator": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionFailJob,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpNotIn,
									Values:   []int32{5, 6, 7},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 42,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 42 matching FailJob rule at index 0",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container main-container for pod default/mypod-0 failed with exit code 42 matching FailJob rule at index 0",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// NotIn 操作符下退出码命中排除列表时不触发规则，走默认处理（计入失败次数）。
		"default handling job based on OnExitCodes with NotIn operator": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionFailJob,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpNotIn,
									Values:   []int32{5, 6, 7},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode:   5,
										FinishedAt: testFinishedAt,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// Init 容器的退出码也会匹配规则，消息中显示 init 容器名称。
		"fail job based on OnExitCodes for InitContainer": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						InitContainerStatuses: []v1.ContainerStatus{
							{
								Name: "init-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 5,
									},
								},
							},
						},
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "main-container",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 143,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container init-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Container init-container for pod default/mypod-0 failed with exit code 5 matching FailJob rule at index 1",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 多个规则同时匹配时只执行第一条：首条 Ignore 生效，失败不计入 backoff。
		"ignore pod failure; both rules are matching, the first is executed only": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name: "container1",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 2,
									},
								},
							},
							{
								Name: "container2",
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 6,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    0,
			wantStatusSucceeded: 0,
		},
		// 退出码命中 Ignore 规则：失败不计入 backoff，Job 保持活跃。
		"ignore pod failure based on OnExitCodes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 1,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    0,
			wantStatusSucceeded: 0,
		},
		// 未匹配任何规则的失败按默认处理，耗尽 backoff 后以 BackoffLimitExceeded 失败。
		"default job based on OnExitCodes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 10,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 开启 JobManagedBy 时先添加中间态 FailureTarget，再设置终态 Failed 条件。
		"default job based on OnExitCodes; JobManagedBy enabled triggers adding interim condition": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: onExitCodeRules,
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 10,
									},
								},
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 两条规则同时匹配时只执行第一条 Count，失败计入 backoff，Job 保持活跃。
		"count pod failure based on OnExitCodes; both rules are matching, the first is executed only": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionCount,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{1, 2},
								},
							},
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{2, 3},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode:   2,
										FinishedAt: testFinishedAt,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 基于 Pod condition 匹配时同样只执行首条 Count 规则，而非后续的 Ignore。
		"count pod failure based on OnPodConditions; both rules are matching, the first is executed only": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionCount,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.PodConditionType("ResourceLimitExceeded"),
										Status: v1.ConditionTrue,
									},
								},
							},
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.DisruptionTarget,
										Status: v1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						Conditions: []v1.PodCondition{
							{
								Type:   v1.PodConditionType("ResourceLimitExceeded"),
								Status: v1.ConditionTrue,
							},
							{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							},
						},
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										FinishedAt: testFinishedAt,
									},
								},
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// Pod 带有 DisruptionTarget 条件且命中 Ignore 规则：不计入失败，保持活跃。
		"ignore pod failure based on OnPodConditions": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.DisruptionTarget,
										Status: v1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						Conditions: []v1.PodCondition{
							{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    1,
			wantStatusFailed:    0,
			wantStatusSucceeded: 0,
		},
		// 带删除时间戳的失败 Pod 命中 Ignore 规则时，其被忽略且 Pod 重建被延迟（active 为 0）。
		"ignore pod failure based on OnPodConditions, ignored failures delays pod recreation": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.DisruptionTarget,
										Status: v1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: &now,
					},
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						Conditions: []v1.PodCondition{
							{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							},
						},
					},
				},
			},
			wantConditions:      nil,
			wantStatusActive:    0,
			wantStatusFailed:    0,
			wantStatusSucceeded: 0,
		},
		// Pod 条件命中 FailJob 规则：添加 FailureTarget 与 Failed 条件，消息中包含匹配的 condition。
		"fail job based on OnPodConditions": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:     validSelector,
					Template:     validTemplate,
					Parallelism:  ptr.To[int32](1),
					Completions:  ptr.To[int32](1),
					BackoffLimit: ptr.To[int32](6),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionFailJob,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.DisruptionTarget,
										Status: v1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
						Conditions: []v1.PodCondition{
							{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							},
						},
					},
				},
			},
			wantConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonPodFailurePolicy,
					Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
				},
			},
			wantStatusActive:    0,
			wantStatusFailed:    1,
			wantStatusSucceeded: 0,
		},
		// 启用失败策略时，带删除时间戳的 Pod 计入 terminating，而不会被视为 failed。
		"terminating Pod not considered failed when JobPodFailurePolicy is used": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Parallelism:  ptr.To[int32](1),
					Selector:     validSelector,
					Template:     validTemplate,
					BackoffLimit: ptr.To[int32](0),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionCount,
								OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{
									{
										Type:   v1.DisruptionTarget,
										Status: v1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: &now,
					},
				},
			},
			wantStatusTerminating: ptr.To[int32](1),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 未开启 JobManagedBy 时固定模拟版本为 1.33，以匹配该特性关闭时的默认行为。
			if !tc.enableJobManagedBy {
				// TODO: this will be removed in 1.38.
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.33"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})

			// 未显式设置时默认在 Pod 失败后替换它，与生产默认值保持一致。
			if tc.job.Spec.PodReplacementPolicy == nil {
				tc.job.Spec.PodReplacementPolicy = podReplacementPolicy(batch.Failed)
			}
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			job := &tc.job

			actual := job
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			for i, pod := range tc.pods {
				pb := podBuilder{Pod: &pod}.name(fmt.Sprintf("mypod-%d", i)).job(job)
				if job.Spec.CompletionMode != nil && *job.Spec.CompletionMode == batch.IndexedCompletion {
					pb.index(fmt.Sprintf("%v", i))
				}
				pb = pb.trackingFinalizer()
				sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pb.Pod)
			}

			if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
				t.Fatalf("error %v while reconciling the job %v", err, testutil.GetKey(job, t))
			}

			if diff := cmp.Diff(tc.wantConditions, actual.Status.Conditions, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected job conditions (-want,+got):\n%s", diff)
			}
			// 验证状态计数。
			if actual.Status.Active != tc.wantStatusActive {
				t.Errorf("unexpected number of active pods. Expected %d, saw %d\n", tc.wantStatusActive, actual.Status.Active)
			}
			if actual.Status.Succeeded != tc.wantStatusSucceeded {
				t.Errorf("unexpected number of succeeded pods. Expected %d, saw %d\n", tc.wantStatusSucceeded, actual.Status.Succeeded)
			}
			if actual.Status.Failed != tc.wantStatusFailed {
				t.Errorf("unexpected number of failed pods. Expected %d, saw %d\n", tc.wantStatusFailed, actual.Status.Failed)
			}
			if ptr.Deref(actual.Status.Terminating, 0) != ptr.Deref(tc.wantStatusTerminating, 0) {
				t.Errorf("unexpected number of terminating pods. Expected %d, saw %d\n", ptr.Deref(tc.wantStatusTerminating, 0), ptr.Deref(actual.Status.Terminating, 0))
			}
		})
	}
}

// TestSyncJobWithJobSuccessPolicy 验证 syncJob 在配置 successPolicy 时的状态计算：
// 仅当成功规则满足且未被 podFailurePolicy 或 backoffLimit 优先判负时置位 SuccessCriteriaMet。
func TestSyncJobWithJobSuccessPolicy(t *testing.T) {
	now := time.Now()
	validTypeMeta := metav1.TypeMeta{
		APIVersion: batch.SchemeGroupVersion.String(),
		Kind:       "Job",
	}
	validObjectMeta := metav1.ObjectMeta{
		Name:      "foobar",
		UID:       uuid.NewUUID(),
		Namespace: metav1.NamespaceDefault,
	}
	validSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"foo": "bar"},
	}
	validTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Image: "foobar"},
			},
		},
	}

	// 覆盖索引级失败计数、退避后重试、maxFailedIndexes 触发 FailJob 以及 JobManagedBy 组合。
	// 覆盖 successPolicy 与 podFailurePolicy、backoffLimitPerIndex 的优先级组合，以及 JobManagedBy 开关。
	testCases := map[string]struct {
		enableJobManagedBy bool
		job                batch.Job
		pods               []v1.Pod
		wantStatus         batch.JobStatus
	}{
		// 索引 0 失败、1 成功时满足 successPolicy，即使仍有索引在运行也应置位 SuccessCriteriaMet。
		"job with successPolicy; job has SuccessCriteriaMet condition if job meets to successPolicy and some indexes fail": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](3),
					Completions:    ptr.To[int32](3),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](2),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		// successPolicy 已满足且 podFailurePolicy 未触发 FailJob，应以成功为准。
		"job with podFailurePolicy and successPolicy; job has SuccessCriteriaMet condition if job meets to successPolicy and doesn't meet to podFailurePolicy": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{{
							Action: batch.PodFailurePolicyActionFailJob,
							OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							}},
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](1),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		// per-index 退避未超限时，successPolicy 仍可判定成功。
		"job with backoffLimitPerIndex and successPolicy; job has SuccessCriteriaMet condition if job meets to successPolicy and doesn't meet backoffLimitPerIndex": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](2),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](1),
				CompletedIndexes:        "1",
				FailedIndexes:           ptr.To(""),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		"job with successPolicy; job has both Complete and SuccessCriteriaMet condition when job meets to successPolicy and all pods have been already removed": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](2),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
				Status: batch.JobStatus{
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				FailedIndexes:           ptr.To(""),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		// 参考 issue 123775：确保条件不会因后续状态更新而丢失。
		"job with successPolicy; job has SuccessCriteriaMet condition when job meets to successPolicy and some pods still are running": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](3),
					Completions:          ptr.To[int32](3),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](3),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
				Status: batch.JobStatus{
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("1").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](2),
				FailedIndexes:           ptr.To(""),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		// REF: https://github.com/kubernetes/kubernetes/issues/123775
		"job with successPolicy; JobManagedBy feature enabled; job has SuccessCriteriaMet condition when job meets to successPolicy and some pods still are running": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](3),
					Completions:          ptr.To[int32](3),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](3),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
				Status: batch.JobStatus{
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("1").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodRunning).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](2),
				FailedIndexes:           ptr.To(""),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		"job with successPolicy and podFailurePolicy; job has a failed condition when job meets to both successPolicy and podFailurePolicy": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{{
							Action: batch.PodFailurePolicyActionFailJob,
							OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							}},
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					Conditions: []v1.PodCondition{{
						Type:   v1.DisruptionTarget,
						Status: v1.ConditionTrue,
					}},
				}).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodRunning).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  2,
				Succeeded:               1,
				Terminating:             ptr.To[int32](1),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonPodFailurePolicy,
						Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
					},
				},
			},
		},
		"job with successPolicy and backoffLimitPerIndex; job has a failed condition when job meets to both successPolicy and backoffLimitPerIndex": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](1),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				FailedIndexes:           ptr.To("0"),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
				},
			},
		},
		"job with successPolicy and backoffLimit; job has a failed condition when job meets to both successPolicy and backoffLimit": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](1),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  2,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonBackoffLimitExceeded,
						Message: "Job has reached the specified backoff limit",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonBackoffLimitExceeded,
						Message: "Job has reached the specified backoff limit",
					},
				},
			},
		},
		"job with successPolicy and podFailurePolicy; job with SuccessCriteriaMet has never been transitioned to FailureTarget and Failed even if job meets podFailurePolicy": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededCount: ptr.To[int32](1),
						}},
					},
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{{
							Action: batch.PodFailurePolicyActionFailJob,
							OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							}},
						}},
					},
				},
				Status: batch.JobStatus{
					Failed:                  0,
					Succeeded:               1,
					Terminating:             ptr.To[int32](0),
					CompletedIndexes:        "1",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					Conditions: []v1.PodCondition{{
						Type:   v1.DisruptionTarget,
						Status: v1.ConditionTrue,
					}},
				}).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		"job with successPolicy and backoffLimitPerIndex; job with SuccessCriteriaMet has never been transitioned to FailureTarget and Failed even if job meet backoffLimitPerIndex": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					BackoffLimitPerIndex: ptr.To[int32](1),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("1"),
						}},
					},
				},
				Status: batch.JobStatus{
					Failed:                  0,
					Succeeded:               1,
					Terminating:             ptr.To[int32](0),
					CompletedIndexes:        "1",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				FailedIndexes:           ptr.To("0"),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		"job with successPolicy and backoffLimit; job with SuccessCriteriaMet has never been transitioned to FailureTarget and Failed even if job meets backoffLimit": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](1),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
				},
				Status: batch.JobStatus{
					Failed:                  0,
					Succeeded:               1,
					Terminating:             ptr.To[int32](0),
					CompletedIndexes:        "1",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobSuccessCriteriaMet,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonSuccessPolicy,
							Message: "Matched rules at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonSuccessPolicy,
						Message: "Matched rules at index 0",
					},
				},
			},
		},
		"job with successPolicy and podFailureTarget; job with FailureTarget has never been transitioned to SuccessCriteriaMet even if job meets successPolicy": {
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
					SuccessPolicy: &batch.SuccessPolicy{
						Rules: []batch.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0,1"),
							SucceededCount:   ptr.To[int32](1),
						}},
					},
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{{
							Action: batch.PodFailurePolicyActionFailJob,
							OnPodConditions: []batch.PodFailurePolicyOnPodConditionsPattern{{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							}},
						}},
					},
				},
				Status: batch.JobStatus{
					Failed:                  1,
					Succeeded:               0,
					Terminating:             ptr.To[int32](0),
					CompletedIndexes:        "1",
					UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
					Conditions: []batch.JobCondition{
						{
							Type:    batch.JobFailureTarget,
							Status:  v1.ConditionTrue,
							Reason:  batch.JobReasonPodFailurePolicy,
							Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					Conditions: []v1.PodCondition{{
						Type:   v1.DisruptionTarget,
						Status: v1.ConditionTrue,
					}},
				}).Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonPodFailurePolicy,
						Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonPodFailurePolicy,
						Message: "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0",
					},
				},
			},
		},
		"job without successPolicy; job got SuccessCriteriaMet and Completion with CompletionsReached reason conditions": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   validTypeMeta,
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:       validSelector,
					Template:       validTemplate,
					CompletionMode: ptr.To(batch.IndexedCompletion),
					Completions:    ptr.To[int32](1),
					Parallelism:    ptr.To[int32](1),
					BackoffLimit:   ptr.To[int32](math.MaxInt32),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodSucceeded).trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  0,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "0",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
		},
	}
	// 每个用例独立构造控制器，避免状态与特性门控在用例间互相污染。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.enableJobManagedBy {
				// TODO: this will be remove in 1.38
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})

			clientSet := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			fakeClock := clocktesting.NewFakeClock(now)
			_, ctx := ktesting.NewTestContext(t)
			manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientSet, controller.NoResyncPeriodFunc, fakeClock)
			manager.podControl = &controller.FakePodControl{}
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			job := &tc.job

			actual := job
			manager.updateStatusHandler = func(_ context.Context, j *batch.Job) (*batch.Job, error) {
				actual = j
				return j, nil
			}
			if err := sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job); err != nil {
				t.Fatalf("Failed to add the Job %q to sharedInformer: %v", klog.KObj(job), err)
			}
			for i, pod := range tc.pods {
				pb := podBuilder{Pod: pod.DeepCopy()}.name(fmt.Sprintf("mypod-%d", i)).job(job)
				if isIndexedJob(job) {
					pb.index(strconv.Itoa(getCompletionIndex(pod.Annotations)))
				}
				if err := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pb.Pod); err != nil {
					t.Fatalf("Failed to add the Pod %q to sharedInformer: %v", klog.KObj(pb.Pod), err)
				}
			}

			if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
				t.Fatalf("failed to complete syncJob: %v", err)
			}

			// 比较时忽略时间戳字段，仅校验与 successPolicy 相关的状态语义。
			if diff := cmp.Diff(tc.wantStatus, actual.Status,
				cmpopts.IgnoreFields(batch.JobStatus{}, "StartTime", "CompletionTime", "Ready"),
				cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected Job status (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestSyncJobWithJobBackoffLimitPerIndex 验证索引 Job 启用 backoffLimitPerIndex 后，
// 各索引的失败计数、重试资格与 FailIndex/FailJob 行为是否正确。
func TestSyncJobWithJobBackoffLimitPerIndex(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	now := time.Now()
	validObjectMeta := metav1.ObjectMeta{
		Name:      "foobar",
		UID:       uuid.NewUUID(),
		Namespace: metav1.NamespaceDefault,
	}
	validSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"foo": "bar"},
	}
	validTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Image: "foo/bar"},
			},
		},
	}

	testCases := map[string]struct {
		enableJobManagedBy bool
		job                batch.Job
		pods               []v1.Pod
		wantStatus         batch.JobStatus
	}{
		"successful job after a single failure within index": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a1").index("0").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("a2").index("0").phase(v1.PodSucceeded).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               2,
				Terminating:             ptr.To[int32](0),
				CompletedIndexes:        "0,1",
				FailedIndexes:           ptr.To(""),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
					{
						Type:    batch.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
		},
		"single failed pod, not counted as the replacement pod creation is delayed": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  2,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				FailedIndexes:           ptr.To(""),
			},
		},
		"single failed pod replaced already": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("b").index("0").phase(v1.PodPending).indexFailureCount("1").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  2,
				Failed:                  1,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				FailedIndexes:           ptr.To(""),
			},
		},
		// 回归测试（issue 139428）：一个索引有运行中的 Pod，另一个索引的替换 Pod 因退避而延迟创建。
		// manageJob 必须报告真实的活跃数 1 而非 0；
		// 否则 active=0 但仍有 Pod 运行的状态会被 apiserver 拒绝。
		// 错误的状态会阻塞 finalizer 清理与状态写回。
		//
		//
		//
		"replacement pod creation delayed by per-index backoff while another index runs": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodRunning).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").status(v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "x",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									FinishedAt: metav1.NewTime(now),
								},
							},
						},
					},
				}).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  1,
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				FailedIndexes:           new(string),
			},
		},
		"single failed index due to exceeding the backoff limit per index, the job continues": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  1,
				Failed:                  1,
				FailedIndexes:           ptr.To("0"),
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
		"single failed index due to FailIndex action, the job continues": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionFailIndex,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{3},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 3,
								},
							},
						},
					},
				}).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  1,
				Failed:                  1,
				FailedIndexes:           ptr.To("0"),
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
		"job failed index due to FailJob action": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](6),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionFailJob,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{3},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "x",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 3,
								},
							},
						},
					},
				}).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  0,
				Failed:                  1,
				FailedIndexes:           ptr.To(""),
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonPodFailurePolicy,
						Message: "Container x for pod default/mypod-0 failed with exit code 3 matching FailJob rule at index 0",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonPodFailurePolicy,
						Message: "Container x for pod default/mypod-0 failed with exit code 3 matching FailJob rule at index 0",
					},
				},
			},
		},
		"job pod failure ignored due to matching Ignore action": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](6),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					PodFailurePolicy: &batch.PodFailurePolicy{
						Rules: []batch.PodFailurePolicyRule{
							{
								Action: batch.PodFailurePolicyActionIgnore,
								OnExitCodes: &batch.PodFailurePolicyOnExitCodesRequirement{
									Operator: batch.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{3},
								},
							},
						},
					},
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").status(v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "x",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 3,
								},
							},
						},
					},
				}).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Active:                  2,
				Failed:                  0,
				FailedIndexes:           ptr.To(""),
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
			},
		},
		"job failed due to exceeding backoffLimit before backoffLimitPerIndex": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](1),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodFailed).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  2,
				Succeeded:               0,
				FailedIndexes:           ptr.To(""),
				Terminating:             ptr.To[int32](0),
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonBackoffLimitExceeded,
						Message: "Job has reached the specified backoff limit",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonBackoffLimitExceeded,
						Message: "Job has reached the specified backoff limit",
					},
				},
			},
		},
		"job failed due to failed indexes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Succeeded:               1,
				Terminating:             ptr.To[int32](0),
				FailedIndexes:           ptr.To("0"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
				},
			},
		},
		"job failed due to exceeding max failed indexes": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](4),
					Completions:          ptr.To[int32](4),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					MaxFailedIndexes:     ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("d").index("3").phase(v1.PodRunning).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  3,
				Succeeded:               1,
				Terminating:             ptr.To[int32](1),
				FailedIndexes:           ptr.To("0,2"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonMaxFailedIndexesExceeded,
						Message: "Job has exceeded the specified maximal number of failed indexes",
					},
				},
			},
		},
		"job failed due to failed indexes; JobManagedBy disabled": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Terminating:             ptr.To[int32](0),
				Succeeded:               1,
				FailedIndexes:           ptr.To("0"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
				},
			},
		},
		"job failed due to failed indexes; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  1,
				Terminating:             ptr.To[int32](0),
				Succeeded:               1,
				FailedIndexes:           ptr.To("0"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
					{
						Type:    batch.JobFailed,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonFailedIndexes,
						Message: "Job has failed indexes",
					},
				},
			},
		},
		"job failed due to exceeding max failed indexes; JobManagedBy disabled": {
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](4),
					Completions:          ptr.To[int32](4),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					MaxFailedIndexes:     ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("d").index("3").phase(v1.PodRunning).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  3,
				Terminating:             ptr.To[int32](1),
				Succeeded:               1,
				FailedIndexes:           ptr.To("0,2"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonMaxFailedIndexesExceeded,
						Message: "Job has exceeded the specified maximal number of failed indexes",
					},
				},
			},
		},
		"job failed due to exceeding max failed indexes; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batch.Job{
				TypeMeta:   metav1.TypeMeta{Kind: "Job"},
				ObjectMeta: validObjectMeta,
				Spec: batch.JobSpec{
					Selector:             validSelector,
					Template:             validTemplate,
					Parallelism:          ptr.To[int32](4),
					Completions:          ptr.To[int32](4),
					BackoffLimit:         ptr.To[int32](math.MaxInt32),
					CompletionMode:       ptr.To(batch.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					MaxFailedIndexes:     ptr.To[int32](1),
				},
			},
			pods: []v1.Pod{
				*buildPod().uid("a").index("0").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("b").index("1").phase(v1.PodSucceeded).indexFailureCount("0").trackingFinalizer().Pod,
				*buildPod().uid("c").index("2").phase(v1.PodFailed).indexFailureCount("1").trackingFinalizer().Pod,
				*buildPod().uid("d").index("3").phase(v1.PodRunning).indexFailureCount("0").trackingFinalizer().Pod,
			},
			wantStatus: batch.JobStatus{
				Failed:                  3,
				Terminating:             ptr.To[int32](1),
				Succeeded:               1,
				FailedIndexes:           ptr.To("0,2"),
				CompletedIndexes:        "1",
				UncountedTerminatedPods: &batch.UncountedTerminatedPods{},
				Conditions: []batch.JobCondition{
					{
						Type:    batch.JobFailureTarget,
						Status:  v1.ConditionTrue,
						Reason:  batch.JobReasonMaxFailedIndexesExceeded,
						Message: "Job has exceeded the specified maximal number of failed indexes",
					},
				},
			},
		},
	}
	// 每个用例独立构造控制器以保证索引状态互不干扰。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.enableJobManagedBy {
				// TODO: this will be removed in 1.38.
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			fakeClock := clocktesting.NewFakeClock(now)
			manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			job := &tc.job

			actual := job
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			for i, pod := range tc.pods {
				pb := podBuilder{Pod: &pod}.name(fmt.Sprintf("mypod-%d", i)).job(job)
				if job.Spec.CompletionMode != nil && *job.Spec.CompletionMode == batch.IndexedCompletion {
					pb.index(fmt.Sprintf("%v", getCompletionIndex(pod.Annotations)))
				}
				pb = pb.trackingFinalizer()
				sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pb.Pod)
			}

			if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
				t.Fatalf("error %v while reconciling the job %v", err, testutil.GetKey(job, t))
			}

			// 校验状态的关键字段，忽略易变的时间戳与 Ready 计数。
			if diff := cmp.Diff(tc.wantStatus, actual.Status,
				cmpopts.IgnoreFields(batch.JobStatus{}, "StartTime", "CompletionTime", "Ready"),
				cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected job status. Diff: %s\n", diff)
			}
		})
	}
}

// TestSyncJobUpdateRequeue 验证状态更新失败时工作项会被重新入队，成功时不入队。
func TestSyncJobUpdateRequeue(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	cases := map[string]struct {
		updateErr    error
		wantRequeued bool
	}{
		"no error": {
			wantRequeued: false,
		},
		"generic error": {
			updateErr:    fmt.Errorf("update error"),
			wantRequeued: true,
		},
		"conflict error": {
			updateErr:    apierrors.NewConflict(schema.GroupResource{}, "", nil),
			wantRequeued: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(setDurationDuringTest(&DefaultJobApiBackOff, fastJobApiBackoff))
			fakeClient := clocktesting.NewFakeClock(time.Now())
			manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClient)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				return job, tc.updateErr
			}
			job := newJob(2, 2, 6, batch.NonIndexedCompletion)
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			manager.queue.Add(testutil.GetKey(job, t))
			manager.processNextWorkItem(ctx)
			// 失败场景断言工作项被重新入队；成功场景推进时钟并等待延迟队列清空。
			if tc.wantRequeued {
				verifyEmptyQueueAndAwaitForQueueLen(ctx, t, manager, 1)
			} else {
				// 推进时钟以确保没有待入队的工作项；
				// 短暂休眠让延迟队列有机会把预入队的工作项异步移入队列，
				// 避免把尚未入队的条目误判为没有重新入队。
				manager.clock.Sleep(fastJobApiBackoff)
				time.Sleep(time.Millisecond)
				verifyEmptyQueue(t, manager)
			}
		})
	}
}

// TestUpdateJobRequeue 验证 Job spec 更新会立即重新入队，而仅状态更新不会。
func TestUpdateJobRequeue(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	cases := map[string]struct {
		oldJob                  *batch.Job
		updateFn                func(job *batch.Job)
		wantRequeuedImmediately bool
	}{
		// Generation 递增代表 spec 变化，必须立即重新调谐。
		"spec update": {
			oldJob: newJob(1, 1, 1, batch.IndexedCompletion),
			updateFn: func(job *batch.Job) {
				job.Spec.Suspend = ptr.To(false)
				job.Generation++
			},
			wantRequeuedImmediately: true,
		},
		// 仅状态字段变化不应触发立即重新入队。
		"status update": {
			oldJob: newJob(1, 1, 1, batch.IndexedCompletion),
			updateFn: func(job *batch.Job) {
				job.Status.StartTime = &metav1.Time{Time: time.Now()}
			},
			wantRequeuedImmediately: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady

			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(tc.oldJob)
			newJob := tc.oldJob.DeepCopy()
			if tc.updateFn != nil {
				tc.updateFn(newJob)
			}
			manager.updateJob(logger, tc.oldJob, newJob)
			gotRequeuedImmediately := manager.queue.Len() > 0
			if tc.wantRequeuedImmediately != gotRequeuedImmediately {
				t.Fatalf("Want immediate requeue: %v, got immediate requeue: %v", tc.wantRequeuedImmediately, gotRequeuedImmediately)
			}
		})
	}
}

// TestDeleteJobClearsPodExpectations 验证删除 Job 会清除该 Job 的 Pod 创建预期，
// 避免后续同名 Job 被未满足的预期阻塞。
func TestDeleteJobClearsPodExpectations(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	job := newJob(1, 1, 1, batch.NonIndexedCompletion)
	if err := sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job); err != nil {
		t.Fatalf("Failed to add Job to informer: %v", err)
	}
	key, err := controller.KeyFunc(job)
	if err != nil {
		t.Fatalf("Unexpected error getting job key: %v", err)
	}

	// 模拟控制器已请求但尚未观察到的 Pod 创建，此时预期应为未满足。
	if err := manager.expectations.ExpectCreations(logger, key, 1); err != nil {
		t.Fatalf("ExpectCreations() error = %v", err)
	}
	if manager.expectations.SatisfiedExpectations(logger, key) {
		t.Fatal("expectations should be unsatisfied before deleting the Job")
	}

	manager.deleteJob(logger, job)

	if !manager.expectations.SatisfiedExpectations(logger, key) {
		t.Error("expectations should be cleared after deleting the Job")
	}
}

// TestGetPodCreationInfoForIndependentIndexes 验证独立索引场景下哪些索引可立即重建，
// 以及处于失败退避中的索引还需等待多长时间。
func TestGetPodCreationInfoForIndependentIndexes(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	now := time.Now()
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	cases := map[string]struct {
		indexesToAdd                    []int
		podsWithDelayedDeletionPerIndex map[int]*v1.Pod
		wantIndexesToAdd                []int
		wantRemainingTime               time.Duration
	}{
		"simple index creation": {
			indexesToAdd:     []int{1, 3},
			wantIndexesToAdd: []int{1, 3},
		},
		// 仅索引 1 的 Pod 处于延迟删除中，索引 3 可立即重建。
		"subset of indexes can be recreated now": {
			indexesToAdd: []int{1, 3},
			podsWithDelayedDeletionPerIndex: map[int]*v1.Pod{
				1: buildPod().indexFailureCount("0").index("1").customDeletionTimestamp(now).Pod,
			},
			wantIndexesToAdd: []int{3},
		},
		"subset of indexes can be recreated now as the pods failed long time ago": {
			indexesToAdd: []int{1, 3},
			podsWithDelayedDeletionPerIndex: map[int]*v1.Pod{
				1: buildPod().indexFailureCount("0").customDeletionTimestamp(now).Pod,
				3: buildPod().indexFailureCount("0").customDeletionTimestamp(now.Add(-DefaultJobPodFailureBackOff)).Pod,
			},
			wantIndexesToAdd: []int{3},
		},
		// 所有索引均处于退避期，等待时间为默认失败退避时长。
		"no indexes can be recreated now, need to wait default pod failure backoff": {
			indexesToAdd: []int{1, 2, 3},
			podsWithDelayedDeletionPerIndex: map[int]*v1.Pod{
				1: buildPod().indexFailureCount("1").customDeletionTimestamp(now).Pod,
				2: buildPod().indexFailureCount("0").customDeletionTimestamp(now).Pod,
				3: buildPod().indexFailureCount("2").customDeletionTimestamp(now).Pod,
			},
			wantRemainingTime: DefaultJobPodFailureBackOff,
		},
		"no indexes can be recreated now, need to wait but 1s already passed": {
			indexesToAdd: []int{1, 2, 3},
			podsWithDelayedDeletionPerIndex: map[int]*v1.Pod{
				1: buildPod().indexFailureCount("1").customDeletionTimestamp(now.Add(-time.Second)).Pod,
				2: buildPod().indexFailureCount("0").customDeletionTimestamp(now.Add(-time.Second)).Pod,
				3: buildPod().indexFailureCount("2").customDeletionTimestamp(now.Add(-time.Second)).Pod,
			},
			wantRemainingTime: DefaultJobPodFailureBackOff - time.Second,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fakeClock := clocktesting.NewFakeClock(now)
			manager, _ := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
			gotIndexesToAdd, gotRemainingTime := manager.getPodCreationInfoForIndependentIndexes(logger, tc.indexesToAdd, tc.podsWithDelayedDeletionPerIndex)
			// 先断言可重建索引集合，再断言剩余等待时间。
			if diff := cmp.Diff(tc.wantIndexesToAdd, gotIndexesToAdd); diff != "" {
				t.Fatalf("Unexpected indexes to add: %s", diff)
			}
			if diff := cmp.Diff(tc.wantRemainingTime, gotRemainingTime); diff != "" {
				t.Fatalf("Unexpected remaining time: %s", diff)
			}
		})
	}
}

// TestJobPodLookup 验证 getPodJobs 只返回标签与命名空间均匹配的 Job。
func TestJobPodLookup(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	testCases := []struct {
		job *batch.Job
		pod *v1.Pod

		expectedName string
	}{
		// 无标签的 Pod 不匹配任何 Job。
		{
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "basic"},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: metav1.NamespaceAll},
			},
			expectedName: "",
		},
		// 标签匹配但命名空间不同，不应匹配。
		{
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Spec: batch.JobSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo2",
					Namespace: "ns",
					Labels:    map[string]string{"foo": "bar"},
				},
			},
			expectedName: "",
		},
		// 命名空间与标签均匹配时应返回对应 Job。
		{
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "bar", Namespace: "ns"},
				Spec: batch.JobSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "foo",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"bar"},
							},
						},
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo3",
					Namespace: "ns",
					Labels:    map[string]string{"foo": "bar"},
				},
			},
			expectedName: "bar",
		},
	}
	for _, tc := range testCases {
		sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(tc.job)
		if jobs := manager.getPodJobs(tc.pod); len(jobs) > 0 {
			if got, want := len(jobs), 1; got != want {
				t.Errorf("len(jobs) = %v, want %v", got, want)
			}
			job := jobs[0]
			if tc.expectedName != job.Name {
				t.Errorf("Got job %+v expected %+v", job.Name, tc.expectedName)
			}
		} else if tc.expectedName != "" {
			t.Errorf("Expected a job %v pod %v, found none", tc.expectedName, tc.pod.Name)
		}
	}
}

// TestGetPodsForJob 验证按 Job 归属筛选 Pod 的规则，包括领养、释放及删除中的 Job。
func TestGetPodsForJob(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	job := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job.Name = "test_job"
	otherJob := newJob(1, 1, 6, batch.NonIndexedCompletion)
	otherJob.Name = "other_job"
	cases := map[string]struct {
		jobDeleted        bool
		jobDeletedInCache bool
		pods              []*v1.Pod
		wantPods          []string
		wantPodsFinalizer []string
	}{
		"only matching": {
			pods: []*v1.Pod{
				buildPod().name("pod1").job(job).trackingFinalizer().Pod,
				buildPod().name("pod2").job(otherJob).Pod,
				buildPod().name("pod3").ns(job.Namespace).Pod,
				buildPod().name("pod4").job(job).Pod,
			},
			wantPods:          []string{"pod1", "pod4"},
			wantPodsFinalizer: []string{"pod1"},
		},
		// 无属主的 Pod 若标签匹配应被领养，即使尚未添加 finalizer。
		"adopt": {
			pods: []*v1.Pod{
				buildPod().name("pod1").job(job).Pod,
				buildPod().name("pod2").job(job).clearOwner().Pod,
				buildPod().name("pod3").job(otherJob).Pod,
			},
			wantPods:          []string{"pod1", "pod2"},
			wantPodsFinalizer: []string{"pod2"},
		},
		// Job 正在删除时不应领养孤儿 Pod。
		"no adopt when deleting": {
			jobDeleted:        true,
			jobDeletedInCache: true,
			pods: []*v1.Pod{
				buildPod().name("pod1").job(job).Pod,
				buildPod().name("pod2").job(job).clearOwner().Pod,
			},
			wantPods: []string{"pod1"},
		},
		"no adopt when deleting race": {
			jobDeleted: true,
			pods: []*v1.Pod{
				buildPod().name("pod1").job(job).Pod,
				buildPod().name("pod2").job(job).clearOwner().Pod,
			},
			wantPods: []string{"pod1"},
		},
		// 标签不再匹配的 Pod 应被释放。
		"release": {
			pods: []*v1.Pod{
				buildPod().name("pod1").job(job).Pod,
				buildPod().name("pod2").job(job).clearLabels().Pod,
			},
			wantPods: []string{"pod1"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := job.DeepCopy()
			if tc.jobDeleted {
				job.DeletionTimestamp = &metav1.Time{}
			}
			clientSet := fake.NewClientset(job, otherJob)
			jm, informer := newControllerFromClient(ctx, t, clientSet, controller.NoResyncPeriodFunc)
			jm.podStoreSynced = alwaysReady
			jm.jobStoreSynced = alwaysReady
			cachedJob := job.DeepCopy()
			if tc.jobDeletedInCache {
				cachedJob.DeletionTimestamp = &metav1.Time{}
			}
			informer.Batch().V1().Jobs().Informer().GetIndexer().Add(cachedJob)
			informer.Batch().V1().Jobs().Informer().GetIndexer().Add(otherJob)
			for _, p := range tc.pods {
				informer.Core().V1().Pods().Informer().GetIndexer().Add(p)
			}

			pods, err := jm.getPodsForJob(ctx, job)
			if err != nil {
				t.Fatalf("getPodsForJob() error: %v", err)
			}
			got := make([]string, len(pods))
			var gotFinalizer []string
			for i, p := range pods {
				got[i] = p.Name
				// 单独校验带跟踪 finalizer 的 Pod 集合，确保领养语义正确。
				if hasJobTrackingFinalizer(p) {
					gotFinalizer = append(gotFinalizer, p.Name)
				}
			}
			sort.Strings(got)
			if diff := cmp.Diff(tc.wantPods, got); diff != "" {
				t.Errorf("getPodsForJob() returned (-want,+got):\n%s", diff)
			}
			sort.Strings(gotFinalizer)
			if diff := cmp.Diff(tc.wantPodsFinalizer, gotFinalizer); diff != "" {
				t.Errorf("Pods with finalizers (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestAddPod 验证已归属 Job 的 Pod 添加事件只入队其属主 Job。
func TestAddPod(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	pod2 := newPod("pod2", job2)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod2)

	// 添加属于 job1 的 Pod，只应入队 job1。
	jm.addPod(logger, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done := jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
	}
	expectedKey, _ := controller.KeyFunc(job1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	// 添加属于 job2 的 Pod，只应入队 job2。
	jm.addPod(logger, pod2)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done = jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
	}
	expectedKey, _ = controller.KeyFunc(job2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestAddPodOrphan 验证孤儿 Pod 添加事件会入队所有标签匹配的 Job。
func TestAddPodOrphan(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	job3 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job3.Name = "job3"
	job3.Spec.Selector.MatchLabels = map[string]string{"other": "labels"}
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job3)

	pod1 := newPod("pod1", job1)
	// 清除属主引用构造孤儿 Pod，期望所有标签匹配的控制器都被入队。
	pod1.OwnerReferences = nil
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	jm.addPod(logger, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 2)
}

// TestUpdatePod 验证已归属 Pod 的普通更新只入队其属主 Job。
func TestUpdatePod(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	pod2 := newPod("pod2", job2)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod2)

	prev := *pod1
	// 递增 resourceVersion 模拟普通更新，期望只入队所属 Job。
	bumpResourceVersion(pod1)
	jm.updatePod(logger, &prev, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done := jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
	}
	expectedKey, _ := controller.KeyFunc(job1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	prev = *pod2
	bumpResourceVersion(pod2)
	jm.updatePod(logger, &prev, pod2)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done = jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
	}
	expectedKey, _ = controller.KeyFunc(job2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestUpdatePodOrphanWithNewLabels 验证孤儿 Pod 标签变化后会入队新匹配的控制器。
func TestUpdatePodOrphanWithNewLabels(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	pod1.OwnerReferences = nil
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	// 孤儿 Pod 的标签发生变化，期望新匹配的控制器被入队。
	prev := *pod1
	prev.Labels = map[string]string{"foo2": "bar2"}
	bumpResourceVersion(pod1)
	jm.updatePod(logger, &prev, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 2)
}

// TestUpdatePodChangeControllerRef 验证 ControllerRef 变化时新旧属主都会被入队。
func TestUpdatePodChangeControllerRef(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	// ControllerRef 由 job1 变为 job2，期望新旧属主都被入队。
	prev := *pod1
	prev.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(job2, controllerKind)}
	bumpResourceVersion(pod1)
	jm.updatePod(logger, &prev, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 2)
}

// TestUpdatePodRelease 验证移除 ControllerRef 后所有匹配控制器都会被入队以便领养。
func TestUpdatePodRelease(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	// 移除 ControllerRef，期望所有匹配的控制器都被入队以尝试领养。
	prev := *pod1
	pod1.OwnerReferences = nil
	bumpResourceVersion(pod1)
	jm.updatePod(logger, &prev, pod1)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 2)
}

// TestDeletePod 验证删除已归属 Pod 时只入队其属主 Job。
func TestDeletePod(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	jm, informer := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)

	pod1 := newPod("pod1", job1)
	pod2 := newPod("pod2", job2)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod2)

	// 删除属于 job1 的 Pod，期望只入队 job1。
	jm.deletePod(logger, pod1, true)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done := jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
	}
	expectedKey, _ := controller.KeyFunc(job1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	jm.deletePod(logger, pod2, true)
	verifyEmptyQueueAndAwaitForQueueLen(ctx, t, jm, 1)
	key, done = jm.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
	}
	expectedKey, _ = controller.KeyFunc(job2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestDeletePodOrphan 验证删除孤儿 Pod 不会触发任何 Job 入队。
func TestDeletePodOrphan(t *testing.T) {
	// 关闭更新批处理，确保孤儿 Pod 删除完全不会触发入队。
	t.Cleanup(setDurationDuringTest(&SyncJobBatchPeriod, 0))
	logger, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	jm, informer := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady

	job1 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job1.Name = "job1"
	job2 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job2.Name = "job2"
	job3 := newJob(1, 1, 6, batch.NonIndexedCompletion)
	job3.Name = "job3"
	job3.Spec.Selector.MatchLabels = map[string]string{"other": "labels"}
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job1)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job2)
	informer.Batch().V1().Jobs().Informer().GetIndexer().Add(job3)

	pod1 := newPod("pod1", job1)
	pod1.OwnerReferences = nil
	informer.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	jm.deletePod(logger, pod1, true)
	if got, want := jm.queue.Len(), 0; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// FakeJobExpectations 通过可控的满足状态和回调模拟期望检查，便于验证调谐时序。
type FakeJobExpectations struct {
	*controller.ControllerExpectations
	satisfied    bool
	expSatisfied func()
}

// SatisfiedExpectations 返回预设的满足状态，并在检查时触发回调以模拟并发事件。
func (fe FakeJobExpectations) SatisfiedExpectations(logger klog.Logger, controllerKey string) bool {
	fe.expSatisfied()
	return fe.satisfied
}

// TestSyncJobExpectations 验证在统计活跃 Pod 与检查期望之间不能插入新的 Pod，
// 否则控制器会因看不到该 Pod 且误判期望已满足而多创建副本。
func TestSyncJobExpectations(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
		return job, nil
	}

	job := newJob(2, 2, 6, batch.NonIndexedCompletion)
	sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
	pods := newPodList(2, v1.PodPending, job)
	podIndexer := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	podIndexer.Add(pods[0])

	// 检查期望时动态加入 pods[1]：若先统计活跃 Pod 再检查期望，就会误判并多创建副本。
	manager.expectations = FakeJobExpectations{
		controller.NewControllerExpectations(), true, func() {
			// 若先统计活跃 Pod 再检查期望，控制器将看不到这个 Pod，
			// 却误以为期望已满足，从而错误地创建一个新副本。
			podIndexer.Add(pods[1])
		},
	}
	if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
		t.Fatalf("error %v while reconciling the job %v", err, testutil.GetKey(job, t))
	}
	// 最终断言没有多余的创建或删除操作。
	if len(fakePodControl.Templates) != 0 {
		t.Errorf("Unexpected number of creates.  Expected %d, saw %d\n", 0, len(fakePodControl.Templates))
	}
	if len(fakePodControl.DeletePodName) != 0 {
		t.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", 0, len(fakePodControl.DeletePodName))
	}
}

// TestWatchJobs 验证 Job 的 watch 事件能够经由工作队列到达 syncHandler。
func TestWatchJobs(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	clientset := fake.NewClientset()
	fakeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
	clientset.PrependWatchReactor("jobs", core.DefaultWatchReactor(fakeWatch, nil))
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	var testJob batch.Job
	received := make(chan struct{})

	// fakeWatcher 发出的事件应经由工作队列最终到达 syncHandler；
	// 这里通过替换 syncHandler 来观测事件是否送达。
	manager.syncHandler = func(ctx context.Context, key string) error {
		defer close(received)
		ns, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			t.Errorf("Error getting namespace/name from key %v: %v", key, err)
		}
		job, err := manager.jobLister.Jobs(ns).Get(name)
		if err != nil || job == nil {
			t.Errorf("Expected to find job under key %v: %v", key, err)
			return nil
		}
		if !apiequality.Semantic.DeepDerivative(*job, testJob) {
			t.Errorf("Expected %#v, but got %#v", testJob, *job)
		}
		return nil
	}
	// 仅启动 Job watcher 与工作队列，发送事件并确认其到达 sync 方法。
	//
	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())
	go manager.Run(ctx, 1)

	// 发送新建的 Job，验证事件能到达 syncHandler。
	testJob.Namespace = "bar"
	testJob.Name = "foo"
	fakeWatch.Add(&testJob)
	t.Log("Waiting for job to reach syncHandler")
	<-received
}

// TestWatchPods 验证 Pod 的 watch 事件能定位到管理它的 Job 并触发同步。
func TestWatchPods(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testJob := newJob(2, 2, 6, batch.NonIndexedCompletion)
	clientset := fake.NewClientset(testJob)
	fakeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
	clientset.PrependWatchReactor("pods", core.DefaultWatchReactor(fakeWatch, nil))
	manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	// 先将一个 Job 存入索引，供 Pod 事件定位属主。
	sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(testJob)
	received := make(chan struct{})
	// fakeWatcher 发出的 Pod 事件应找到管理它的 Job，
	// 并将其送入 syncHandler。
	manager.syncHandler = func(ctx context.Context, key string) error {
		ns, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			t.Errorf("Error getting namespace/name from key %v: %v", key, err)
		}
		job, err := manager.jobLister.Jobs(ns).Get(name)
		if err != nil {
			t.Errorf("Expected to find job under key %v: %v", key, err)
		}
		if !apiequality.Semantic.DeepDerivative(job, testJob) {
			t.Errorf("\nExpected %#v,\nbut got %#v", testJob, job)
			close(received)
			return nil
		}
		close(received)
		return nil
	}
	// 仅启动 Pod watcher 与工作队列，发送事件并确认同步的是正确的 Job。
	//
	go sharedInformerFactory.Core().V1().Pods().Informer().RunWithContext(ctx)
	go manager.Run(ctx, 1)

	pods := newPodList(1, v1.PodRunning, testJob)
	testPod := pods[0]
	testPod.Status.Phase = v1.PodFailed
	fakeWatch.Add(testPod)

	t.Log("Waiting for pod to reach syncHandler")
	<-received
}

// TestWatchOrphanPods 验证失去属主或属主已完成的 Pod 上的跟踪 finalizer 会被清理。
func TestWatchOrphanPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	clientset := fake.NewClientset()
	sharedInformers := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())
	manager, err := NewController(ctx, clientset, sharedInformers.Core().V1().Pods(), sharedInformers.Batch().V1().Jobs(), sharedInformers.Scheduling().V1beta1().Workloads(), sharedInformers.Scheduling().V1beta1().PodGroups())
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	podInformer := sharedInformers.Core().V1().Pods().Informer()
	go podInformer.RunWithContext(ctx)
	cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced)
	go manager.Run(ctx, 1)

	// 该用例故意不将 Job 加入存储，验证属主不存在时也会清理 finalizer。
	cases := map[string]struct {
		job     *batch.Job
		inCache bool
	}{
		"job_does_not_exist": {
			job: newJob(2, 2, 6, batch.NonIndexedCompletion),
		},
		"orphan": {},
		"job_finished": {
			job: func() *batch.Job {
				j := newJob(2, 2, 6, batch.NonIndexedCompletion)
				j.Status.Conditions = append(j.Status.Conditions, batch.JobCondition{
					Type:   batch.JobComplete,
					Status: v1.ConditionTrue,
				})
				return j
			}(),
			inCache: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.inCache {
				if err := sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Add(tc.job); err != nil {
					t.Fatalf("Failed to insert job in index: %v", err)
				}
				t.Cleanup(func() {
					sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Delete(tc.job)
				})
			}
			_, ctx := ktesting.NewTestContext(t)
			podBuilder := buildPod().name(name).deletionTimestamp().trackingFinalizer()
			if tc.job != nil {
				podBuilder = podBuilder.job(tc.job)
			}
			orphanPod := podBuilder.Pod
			orphanPod, err := clientset.CoreV1().Pods("default").Create(ctx, orphanPod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Creating orphan pod: %v", err)
			}

			if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
				p, err := clientset.CoreV1().Pods(orphanPod.Namespace).Get(ctx, orphanPod.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				return !hasJobTrackingFinalizer(p), nil
			}); err != nil {
				t.Errorf("Waiting for Pod to get the finalizer removed: %v", err)
			}
		})
	}
}

// TestSyncOrphanPod 验证不同属主与缓存状态下的孤儿 Pod 是否应移除跟踪 finalizer。
func TestSyncOrphanPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := fake.NewClientset()
	sharedInformers := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())
	manager, err := NewController(ctx, clientset, sharedInformers.Core().V1().Pods(), sharedInformers.Batch().V1().Jobs(), sharedInformers.Scheduling().V1beta1().Workloads(), sharedInformers.Scheduling().V1beta1().PodGroups())
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	podInformer := sharedInformers.Core().V1().Pods().Informer()
	go podInformer.RunWithContext(ctx)
	cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced)
	go manager.Run(ctx, 1)

	cases := map[string]struct {
		owner                *metav1.OwnerReference
		job                  *batch.Job
		inCache              bool
		wantFinalizerRemoved bool
		podSelector          *metav1.LabelSelector
	}{
		"controlled_by_existing_running_job": {
			owner: &metav1.OwnerReference{
				APIVersion:         "batch/v1",
				Kind:               "Job",
				Name:               "j",
				UID:                "111",
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			},
			job: func() *batch.Job {
				j := newJob(2, 2, 6, batch.NonIndexedCompletion)
				j.UID = "111"
				j.Name = "j"
				return j
			}(),
			inCache:              true,
			wantFinalizerRemoved: false,
		},
		"controlled_by_existing_finished_job": {
			owner: &metav1.OwnerReference{
				APIVersion:         "batch/v1",
				Kind:               "Job",
				Name:               "j",
				UID:                "111",
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			},
			job: func() *batch.Job {
				j := newJob(2, 2, 6, batch.NonIndexedCompletion)
				j.UID = "111"
				j.Name = "j"
				j.Status.Conditions = append(j.Status.Conditions, batch.JobCondition{
					Type:   batch.JobComplete,
					Status: v1.ConditionTrue,
				})
				return j
			}(),
			inCache:              true,
			wantFinalizerRemoved: true,
		},
		"controlled_by_non_existing_job": {
			owner: &metav1.OwnerReference{
				APIVersion:         "batch/v1",
				Kind:               "Job",
				Name:               "j",
				UID:                "111",
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			},
			wantFinalizerRemoved: true,
		},
		"controlled_by_cronjob": {
			owner: &metav1.OwnerReference{
				APIVersion:         "batch/v1",
				Kind:               "CronJob",
				Name:               "cj",
				UID:                "111",
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			},
			wantFinalizerRemoved: false,
		},
		"not_controlled": {
			wantFinalizerRemoved: true,
		},
		"controlled_by_custom_crd": {
			owner: &metav1.OwnerReference{
				APIVersion:         "example/v1",
				Kind:               "Job",
				Name:               "job",
				UID:                "111",
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			},
			wantFinalizerRemoved: false,
		},
		"owned_by_existing_running_job": {
			owner: &metav1.OwnerReference{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       "j",
				UID:        "111",
			},
			job: func() *batch.Job {
				j := newJob(2, 2, 6, batch.NonIndexedCompletion)
				j.UID = "111"
				j.Name = "j"
				return j
			}(),
			inCache:              true,
			wantFinalizerRemoved: true,
		},
		"owned_by_custom_crd": {
			owner: &metav1.OwnerReference{
				APIVersion: "example/v1",
				Kind:       "Job",
				Name:       "job",
				UID:        "111",
			},
			wantFinalizerRemoved: true,
		},
		"orphan_pods_by_label_selector": {
			podSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "test",
				},
			},
			wantFinalizerRemoved: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.inCache {
				if err := sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Add(tc.job); err != nil {
					t.Fatalf("Failed to insert job in index: %v", err)
				}
				t.Cleanup(func() {
					if err := sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Delete(tc.job); err != nil {
						t.Fatalf("Failed to delete job from index: %v", err)
					}
				})
			}
			podBuilder := buildPod().name(name).deletionTimestamp().trackingFinalizer()
			if tc.owner != nil {
				podBuilder = podBuilder.owner(*tc.owner)
			}
			orphanKey := orphanPodKey{
				kind:      OrphanPodKeyKindName,
				namespace: podBuilder.Pod.Namespace,
				value:     podBuilder.Pod.Name,
			}
			if tc.podSelector != nil {
				podBuilder = podBuilder.labels(tc.podSelector.MatchLabels)
				selector, err := metav1.LabelSelectorAsSelector(tc.podSelector)
				if err != nil {
					t.Fatalf("Error parsing pod label selector: %v", err)
				}
				orphanKey = orphanPodKey{
					kind:      OrphanPodKeyKindSelector,
					namespace: podBuilder.Pod.Namespace,
					value:     selector.String(),
				}
			}
			orphanPod, err := clientset.CoreV1().Pods("default").Create(ctx, podBuilder.Pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Creating orphan pod: %v", err)
			}
			// 按名称或标签选择器同步孤儿 Pod，覆盖两种 orphanKey 形态。
			err = manager.syncOrphanPod(ctx, orphanKey)
			if err != nil {
				t.Fatalf("Failed to sync orphan pod: %v", err)
			}
			if tc.wantFinalizerRemoved {
				if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
					p, err := clientset.CoreV1().Pods(orphanPod.Namespace).Get(ctx, orphanPod.Name, metav1.GetOptions{})
					if err != nil {
						return false, err
					}
					return !hasJobTrackingFinalizer(p), nil
				}); err != nil {
					t.Errorf("Waiting for the Job's finalizer to be removed: %v", err)
				}
			} else {
				// 短暂休眠给客户端缓存留出更新时间，
				// 避免在预期不移除 finalizer 时误判缓存状态。
				time.Sleep(time.Millisecond)
				orphanPod, err := clientset.CoreV1().Pods(orphanPod.Namespace).Get(ctx, orphanPod.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed to retrieve the latest pod: %v", err)
				}
				if !hasJobTrackingFinalizer(orphanPod) {
					t.Errorf("Unexpected removal of the Job's finalizer")
				}
			}
		})
	}
}

// bumpResourceVersion 递增对象的 resourceVersion，用于模拟一次真实的资源更新。
func bumpResourceVersion(obj metav1.Object) {
	ver, _ := strconv.ParseInt(obj.GetResourceVersion(), 10, 32)
	obj.SetResourceVersion(strconv.FormatInt(ver+1, 10))
}

// TestJobApiBackoffReset 验证同步失败会增加重试次数，而成功后会清空工作队列。
func TestJobApiBackoffReset(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&DefaultJobApiBackOff, fastJobApiBackoff))
	_, ctx := ktesting.NewTestContext(t)

	clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakeClock := clocktesting.NewFakeClock(time.Now())
	manager, sharedInformerFactory := newControllerFromClientWithClock(ctx, t, clientset, controller.NoResyncPeriodFunc, fakeClock)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
		return job, nil
	}

	job := newJob(1, 1, 2, batch.NonIndexedCompletion)
	key := testutil.GetKey(job, t)
	sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)

	// 注入控制器错误，期望该 key 被重新入队一次。
	fakePodControl.Err = errors.New("Controller error")
	manager.queue.Add(key)
	manager.processNextWorkItem(ctx)
	retries := manager.queue.NumRequeues(key)
	if retries != 1 {
		t.Fatalf("%s: expected exactly 1 retry, got %d", job.Name, retries)
	}
	// 等待延迟队列把工作项真正放入队列后再断言。
	awaitForQueueLen(ctx, t, manager, 1)

	// 同步成功后队列应被清空。
	fakePodControl.Err = nil
	manager.processNextWorkItem(ctx)
	verifyEmptyQueue(t, manager)
}

var _ workqueue.TypedRateLimitingInterface[string] = &fakeRateLimitingQueue{}

// fakeRateLimitingQueue 记录重试次数与延迟时长，用于断言退避行为。
type fakeRateLimitingQueue struct {
	workqueue.TypedInterface[string]
	requeues int
	item     string
	duration time.Duration
}

// AddRateLimited 是空实现，本测试不关注该调用。
func (f *fakeRateLimitingQueue) AddRateLimited(item string) {}

// Forget 重置重试计数，表示退避状态已清除。
func (f *fakeRateLimitingQueue) Forget(item string) {
	f.requeues = 0
}

// NumRequeues 返回当前记录的重试次数。
func (f *fakeRateLimitingQueue) NumRequeues(item string) int {
	return f.requeues
}

// AddAfter 记录入队项与延迟时长，供测试断言退避参数。
func (f *fakeRateLimitingQueue) AddAfter(item string, duration time.Duration) {
	f.item = item
	f.duration = duration
}

// TestJobBackoff 验证 Pod 更新触发的同步失败会使用批处理周期作为退避时长。
func TestJobBackoff(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)
	job := newJob(1, 1, 1, batch.NonIndexedCompletion)
	oldPod := newPod(fmt.Sprintf("pod-%v", rand.String(10)), job)
	oldPod.ResourceVersion = "1"
	newPod := oldPod.DeepCopy()
	newPod.ResourceVersion = "2"

	testCases := map[string]struct {
		requeues    int
		oldPodPhase v1.PodPhase
		phase       v1.PodPhase
		wantBackoff time.Duration
	}{
		"failure with pod updates batching": {
			requeues:    0,
			phase:       v1.PodFailed,
			wantBackoff: SyncJobBatchPeriod,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			queue := &fakeRateLimitingQueue{}
			manager.queue = queue
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)

			queue.requeues = tc.requeues
			newPod.Status.Phase = tc.phase
			oldPod.Status.Phase = v1.PodRunning
			if tc.oldPodPhase != "" {
				oldPod.Status.Phase = tc.oldPodPhase
			}
			manager.updatePod(logger, oldPod, newPod)
			if queue.duration != tc.wantBackoff {
				t.Errorf("unexpected backoff %v, expected %v", queue.duration, tc.wantBackoff)
			}
		})
	}
}

// TestJobBackoffForOnFailure 验证 RestartPolicy=OnFailure 时 backoffLimit 的计数语义。
// 通过容器重启次数模拟失败：达到上限后活跃 Pod 应归零并触发 FailureTarget 条件。
func TestJobBackoffForOnFailure(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	testCases := map[string]struct {
		// 通过 restartCounts 模拟各 Pod 的容器重启次数，验证其与 backoffLimit 的关系。
		// 达到上限后即使 Pod 仍在运行，也不应再计入活跃 Pod。
		// Job 配置
		parallelism  int32
		completions  int32
		backoffLimit int32
		suspend      bool

		// Pod 配置
		restartCounts []int32
		podPhase      v1.PodPhase

		// 预期状态与条件
		expectedActive     int32
		expectedSucceeded  int32
		expectedFailed     int32
		expectedConditions []batch.JobCondition

		// 特性开关
		enableJobManagedBy bool
	}{
		"backoffLimit 0 should have 1 pod active": {
			parallelism:        1,
			completions:        1,
			backoffLimit:       0,
			suspend:            false,
			restartCounts:      []int32{0},
			podPhase:           v1.PodRunning,
			expectedActive:     1,
			expectedSucceeded:  0,
			expectedFailed:     0,
			expectedConditions: nil,
		},
		"backoffLimit 1 with restartCount 0 should have 1 pod active": {
			parallelism:        1,
			completions:        1,
			backoffLimit:       1,
			suspend:            false,
			restartCounts:      []int32{0},
			podPhase:           v1.PodRunning,
			expectedActive:     1,
			expectedSucceeded:  0,
			expectedFailed:     0,
			expectedConditions: nil,
		},
		// 重启次数已达上限，运行中的 Pod 也应视为失败并触发 FailureTarget。
		"backoffLimit 1 with restartCount 1 and podRunning should have 0 pod active": {
			parallelism:       1,
			completions:       1,
			backoffLimit:      1,
			suspend:           false,
			restartCounts:     []int32{1},
			podPhase:          v1.PodRunning,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"backoffLimit 1 with restartCount 1 and podPending should have 0 pod active": {
			parallelism:       1,
			completions:       1,
			backoffLimit:      1,
			suspend:           false,
			restartCounts:     []int32{1},
			podPhase:          v1.PodPending,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"too many job failures with podRunning - single pod": {
			parallelism:       1,
			completions:       5,
			backoffLimit:      2,
			suspend:           false,
			restartCounts:     []int32{2},
			podPhase:          v1.PodRunning,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"too many job failures with podPending - single pod": {
			parallelism:       1,
			completions:       5,
			backoffLimit:      2,
			suspend:           false,
			restartCounts:     []int32{2},
			podPhase:          v1.PodPending,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    1,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"too many job failures with podPending - multiple pods": {
			parallelism:       2,
			completions:       5,
			backoffLimit:      2,
			suspend:           false,
			restartCounts:     []int32{1, 1},
			podPhase:          v1.PodPending,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"not enough failures": {
			parallelism:        2,
			completions:        5,
			backoffLimit:       3,
			suspend:            false,
			restartCounts:      []int32{1, 1},
			podPhase:           v1.PodRunning,
			expectedActive:     2,
			expectedSucceeded:  0,
			expectedFailed:     0,
			expectedConditions: nil,
		},
		// 挂起场景：活跃 Pod 应被删除，且只记录 JobSuspended 条件。
		"suspending a job": {
			parallelism:       2,
			completions:       4,
			backoffLimit:      6,
			suspend:           true,
			restartCounts:     []int32{1, 1},
			podPhase:          v1.PodRunning,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuspended,
					Status:  v1.ConditionTrue,
					Reason:  "JobSuspended",
					Message: "Job suspended",
				},
			},
		},
		// 全部 Pod 成功时，SuccessCriteriaMet 与 Complete 条件应同时置位。
		"finished job": {
			parallelism:       2,
			completions:       4,
			backoffLimit:      6,
			suspend:           true,
			restartCounts:     []int32{1, 1, 2, 0},
			podPhase:          v1.PodSucceeded,
			expectedActive:    0,
			expectedSucceeded: 4,
			expectedFailed:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
		},
		// 开启 JobManagedBy 后完成语义应保持不变。
		"finished job; JobManagedBy enabled": {
			enableJobManagedBy: true,

			parallelism:       2,
			completions:       4,
			backoffLimit:      6,
			suspend:           true,
			restartCounts:     []int32{1, 1, 2, 0},
			podPhase:          v1.PodSucceeded,
			expectedActive:    0,
			expectedSucceeded: 4,
			expectedFailed:    0,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobSuccessCriteriaMet,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
				{
					Type:    batch.JobComplete,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonCompletionsReached,
					Message: "Reached expected number of succeeded pods",
				},
			},
		},
		// 多 Pod 场景：失败数按 Pod 累加，超过上限后不再保留活跃 Pod。
		"too many job failures with podRunning - multiple pods": {
			parallelism:       2,
			completions:       5,
			backoffLimit:      2,
			suspend:           false,
			restartCounts:     []int32{1, 1},
			podPhase:          v1.PodRunning,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		"too many job failures with podRunning - multiple pods; JobManagedBy enabled": {
			enableJobManagedBy: true,

			parallelism:       2,
			completions:       5,
			backoffLimit:      2,
			suspend:           false,
			restartCounts:     []int32{1, 1},
			podPhase:          v1.PodRunning,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
	}

	// 每个用例独立构造控制器，避免 mock 状态在用例之间互相污染。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if !tc.enableJobManagedBy {
				// TODO: this will be removed in 1.38.
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: tc.enableJobManagedBy,
			})
			// 初始化控制器并拦截状态更新以捕获实际状态
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			var actual *batch.Job
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}

			// 构造 Job 与带指定重启次数的 Pod
			job := newJob(tc.parallelism, tc.completions, tc.backoffLimit, batch.NonIndexedCompletion)
			job.Spec.Template.Spec.RestartPolicy = v1.RestartPolicyOnFailure
			job.Spec.Suspend = ptr.To(tc.suspend)
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			podIndexer := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer()
			for i, pod := range newPodList(len(tc.restartCounts), tc.podPhase, job) {
				pod.Status.ContainerStatuses = []v1.ContainerStatus{{RestartCount: tc.restartCounts[i]}}
				podIndexer.Add(pod)
			}

			// 执行一次同步
			if err := manager.syncJob(ctx, testutil.GetKey(job, t)); err != nil {
				t.Errorf("unexpected error syncing job.  Got %#v", err)
			}
			// 校验状态计数：这是 backoff 判定的核心输出
			if actual.Status.Active != tc.expectedActive {
				t.Errorf("unexpected number of active pods.  Expected %d, saw %d\n", tc.expectedActive, actual.Status.Active)
			}
			if actual.Status.Succeeded != tc.expectedSucceeded {
				t.Errorf("unexpected number of succeeded pods.  Expected %d, saw %d\n", tc.expectedSucceeded, actual.Status.Succeeded)
			}
			if actual.Status.Failed != tc.expectedFailed {
				t.Errorf("unexpected number of failed pods.  Expected %d, saw %d\n", tc.expectedFailed, actual.Status.Failed)
			}
			// 校验条件：忽略时间戳字段，仅比较语义内容
			if diff := cmp.Diff(tc.expectedConditions, actual.Status.Conditions, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected conditions (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestJobBackoffOnRestartPolicyNever 验证 RestartPolicy=Never 下失败的 Pod 不重启，
// 失败计数直接累计；超过 backoffLimit 后同时置位 FailureTarget 与 Failed 条件。
func TestJobBackoffOnRestartPolicyNever(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	testCases := map[string]struct {
		// Job 配置
		parallelism  int32
		completions  int32
		backoffLimit int32

		// Pod 配置
		activePodsPhase v1.PodPhase
		activePods      int
		failedPods      int

		// 预期状态与条件
		expectedActive     int32
		expectedSucceeded  int32
		expectedFailed     int32
		expectedConditions []batch.JobCondition
	}{
		"not enough failures with backoffLimit 0 - single pod": {
			parallelism:        1,
			completions:        1,
			backoffLimit:       0,
			activePodsPhase:    v1.PodRunning,
			activePods:         1,
			failedPods:         0,
			expectedActive:     1,
			expectedSucceeded:  0,
			expectedFailed:     0,
			expectedConditions: nil,
		},
		"not enough failures with backoffLimit 1 - single pod": {
			parallelism:        1,
			completions:        1,
			backoffLimit:       1,
			activePodsPhase:    "",
			activePods:         0,
			failedPods:         1,
			expectedActive:     1,
			expectedSucceeded:  0,
			expectedFailed:     1,
			expectedConditions: nil,
		},
		// 失败数超过上限时，应同时出现 FailureTarget 与 Failed 条件。
		"too many failures with backoffLimit 1 - single pod": {
			parallelism:       1,
			completions:       1,
			backoffLimit:      1,
			activePodsPhase:   "",
			activePods:        0,
			failedPods:        2,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    2,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
		// 失败数未达上限时，控制器需补齐活跃 Pod 至 parallelism。
		"not enough failures with backoffLimit 6 - multiple pods": {
			parallelism:        2,
			completions:        2,
			backoffLimit:       6,
			activePodsPhase:    v1.PodRunning,
			activePods:         1,
			failedPods:         6,
			expectedActive:     2,
			expectedSucceeded:  0,
			expectedFailed:     6,
			expectedConditions: nil,
		},
		"too many failures with backoffLimit 6 - multiple pods": {
			parallelism:       2,
			completions:       2,
			backoffLimit:      6,
			activePodsPhase:   "",
			activePods:        0,
			failedPods:        7,
			expectedActive:    0,
			expectedSucceeded: 0,
			expectedFailed:    7,
			expectedConditions: []batch.JobCondition{
				{
					Type:    batch.JobFailureTarget,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
				{
					Type:    batch.JobFailed,
					Status:  v1.ConditionTrue,
					Reason:  batch.JobReasonBackoffLimitExceeded,
					Message: "Job has reached the specified backoff limit",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 初始化控制器并拦截状态更新
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			manager, sharedInformerFactory := newControllerFromClient(ctx, t, clientset, controller.NoResyncPeriodFunc)
			fakePodControl := controller.FakePodControl{}
			manager.podControl = &fakePodControl
			manager.podStoreSynced = alwaysReady
			manager.jobStoreSynced = alwaysReady
			var actual *batch.Job
			manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				actual = job
				return job, nil
			}

			// 构造 Job、失败 Pod 与活跃 Pod
			job := newJob(tc.parallelism, tc.completions, tc.backoffLimit, batch.NonIndexedCompletion)
			job.Spec.Template.Spec.RestartPolicy = v1.RestartPolicyNever
			sharedInformerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			podIndexer := sharedInformerFactory.Core().V1().Pods().Informer().GetIndexer()
			for _, pod := range newPodList(tc.failedPods, v1.PodFailed, job) {
				// 终止时间固定在 epoch 后 1 秒，避免退避计时干扰失败计数。
				pod.Status.ContainerStatuses = []v1.ContainerStatus{{State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
					FinishedAt: testFinishedAt,
				}}}}
				podIndexer.Add(pod)
			}
			for _, pod := range newPodList(tc.activePods, tc.activePodsPhase, job) {
				podIndexer.Add(pod)
			}

			// 执行一次同步
			err := manager.syncJob(ctx, testutil.GetKey(job, t))
			if err != nil {
				t.Fatalf("unexpected error syncing job: %#v\n", err)
			}
			// 校验状态计数
			if actual.Status.Active != tc.expectedActive {
				t.Errorf("unexpected number of active pods. Expected %d, saw %d\n", tc.expectedActive, actual.Status.Active)
			}
			if actual.Status.Succeeded != tc.expectedSucceeded {
				t.Errorf("unexpected number of succeeded pods. Expected %d, saw %d\n", tc.expectedSucceeded, actual.Status.Succeeded)
			}
			if actual.Status.Failed != tc.expectedFailed {
				t.Errorf("unexpected number of failed pods. Expected %d, saw %d\n", tc.expectedFailed, actual.Status.Failed)
			}
			// 校验条件
			if diff := cmp.Diff(tc.expectedConditions, actual.Status.Conditions, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("unexpected conditions (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestEnsureJobConditions 验证 ensureJobConditionStatus 仅在条件需要新增或变更时更新列表。
// isUpdated 决定上层是否需要写回状态，因此必须与条件的真实变化一致。
func TestEnsureJobConditions(t *testing.T) {
	// 覆盖新增、更新（reason/status 变化）以及无需变更三类分支。
	testCases := []struct {
		name         string
		haveList     []batch.JobCondition
		wantType     batch.JobConditionType
		wantStatus   v1.ConditionStatus
		wantReason   string
		expectList   []batch.JobCondition
		expectUpdate bool
	}{
		{
			name:         "append true condition",
			haveList:     []batch.JobCondition{},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionTrue,
			wantReason:   "foo",
			expectList:   []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			expectUpdate: true,
		},
		{
			name:         "append false condition",
			haveList:     []batch.JobCondition{},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionFalse,
			wantReason:   "foo",
			expectList:   []batch.JobCondition{},
			expectUpdate: false,
		},
		{
			name:         "update true condition reason",
			haveList:     []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionTrue,
			wantReason:   "bar",
			expectList:   []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "bar", "", realClock.Now())},
			expectUpdate: true,
		},
		{
			name:         "update true condition status",
			haveList:     []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionFalse,
			wantReason:   "foo",
			expectList:   []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionFalse, "foo", "", realClock.Now())},
			expectUpdate: true,
		},
		{
			name:         "update false condition status",
			haveList:     []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionFalse, "foo", "", realClock.Now())},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionTrue,
			wantReason:   "foo",
			expectList:   []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			expectUpdate: true,
		},
		{
			name:         "condition already exists",
			haveList:     []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			wantType:     batch.JobSuspended,
			wantStatus:   v1.ConditionTrue,
			wantReason:   "foo",
			expectList:   []batch.JobCondition{*newCondition(batch.JobSuspended, v1.ConditionTrue, "foo", "", realClock.Now())},
			expectUpdate: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotList, isUpdated := ensureJobConditionStatus(tc.haveList, tc.wantType, tc.wantStatus, tc.wantReason, "", realClock.Now())
			// isUpdated 为 false 表示状态无需写回，避免无意义的 API 更新。
			if isUpdated != tc.expectUpdate {
				t.Errorf("Got isUpdated=%v, want %v", isUpdated, tc.expectUpdate)
			}
			if len(gotList) != len(tc.expectList) {
				t.Errorf("got a list of length %d, want %d", len(gotList), len(tc.expectList))
			}
			if diff := cmp.Diff(tc.expectList, gotList, cmpopts.IgnoreFields(batch.JobCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("Unexpected JobCondition list: (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestFinalizersRemovedExpectations 验证最终态 Pod 的 finalizer 移除预期在同步失败、
// 缓存同步及 Pod 更新场景下的正确维护。
func TestFinalizersRemovedExpectations(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := fake.NewClientset()
	sharedInformers := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())
	manager, err := NewController(ctx, clientset, sharedInformers.Core().V1().Pods(), sharedInformers.Batch().V1().Jobs(), sharedInformers.Scheduling().V1beta1().Workloads(), sharedInformers.Scheduling().V1beta1().PodGroups())
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	// 先让 Pod 操作失败：同步失败时不应记录 finalizer 移除预期。
	manager.podControl = &controller.FakePodControl{Err: errors.New("fake pod controller error")}
	manager.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
		return job, nil
	}

	job := newJob(2, 2, 6, batch.NonIndexedCompletion)
	sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
	pods := append(newPodList(2, v1.PodSucceeded, job), newPodList(2, v1.PodFailed, job)...)
	podInformer := sharedInformers.Core().V1().Pods().Informer()
	podIndexer := podInformer.GetIndexer()
	uids := sets.New[types.UID]()
	for i := range pods {
		clientset.Tracker().Add(pods[i])
		podIndexer.Add(pods[i])
		uids.Insert(pods[i].UID)
	}
	jobKey := testutil.GetKey(job, t)

	if err := manager.syncJob(ctx, jobKey); err == nil {
		t.Fatal("missing error as the podControl is mocked to error")
	}
	gotExpectedUIDs := manager.finalizerExpectations.getExpectedUIDs(jobKey)
	if len(gotExpectedUIDs) != 0 {
		t.Errorf("Got unwanted expectations for removed finalizers after first syncJob with client failures:\n%s", sets.List(gotExpectedUIDs))
	}

	// 清除故障后重新同步：此时应为所有终态 Pod 记录 finalizer 移除预期。
	manager.podControl.(*controller.FakePodControl).Err = nil
	if err := manager.syncJob(ctx, jobKey); err != nil {
		t.Fatalf("unexpected error syncing job: %#v\n", err)
	}
	gotExpectedUIDs = manager.finalizerExpectations.getExpectedUIDs(jobKey)
	if diff := cmp.Diff(uids, gotExpectedUIDs); diff != "" {
		t.Errorf("Different expectations for removed finalizers after syncJob (-want,+got):\n%s", diff)
	}

	go sharedInformers.Core().V1().Pods().Informer().RunWithContext(ctx)
	cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced)

	// 缓存同步事件不应清除首次同步设置的预期。
	gotExpectedUIDs = manager.finalizerExpectations.getExpectedUIDs(jobKey)
	if diff := cmp.Diff(uids, gotExpectedUIDs); diff != "" {
		t.Errorf("Different expectations for removed finalizers after syncJob and cacheSync (-want,+got):\n%s", diff)
	}

	// 以不同方式更新四个 Pod：移除 finalizer、同时设置删除时间戳、仅设置删除时间戳、直接删除。

	podsResource := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	update := pods[0].DeepCopy()
	update.Finalizers = nil
	update.ResourceVersion = "1"
	err = clientset.Tracker().Update(podsResource, update, update.Namespace)
	if err != nil {
		t.Errorf("Removing finalizer: %v", err)
	}

	update = pods[1].DeepCopy()
	update.Finalizers = nil
	update.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	update.ResourceVersion = "1"
	err = clientset.Tracker().Update(podsResource, update, update.Namespace)
	if err != nil {
		t.Errorf("Removing finalizer and setting deletion timestamp: %v", err)
	}

	// 该 Pod 仅设置删除时间戳而保留 finalizer，因此其预期应继续存在。
	update = pods[2].DeepCopy()
	update.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	update.ResourceVersion = "1"
	err = clientset.Tracker().Update(podsResource, update, update.Namespace)
	if err != nil {
		t.Errorf("Setting deletion timestamp: %v", err)
	}

	err = clientset.Tracker().Delete(podsResource, pods[3].Namespace, pods[3].Name)
	if err != nil {
		t.Errorf("Deleting pod that had finalizer: %v", err)
	}

	// 只有保留 finalizer 的 Pod 仍需等待移除，其余三个预期应被事件处理逻辑清除。
	uids = sets.New(pods[2].UID)
	var diff string
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		gotExpectedUIDs = manager.finalizerExpectations.getExpectedUIDs(jobKey)
		diff = cmp.Diff(uids, gotExpectedUIDs)
		return diff == "", nil
	}); err != nil {
		t.Errorf("Timeout waiting for expectations (-want, +got):\n%s", diff)
	}
}

// TestFinalizerCleanup 验证已完成 Job 的在管 Pod 与失去关联的孤儿 Pod 上的跟踪 finalizer 均会被清理。
func TestFinalizerCleanup(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	clientset := fake.NewClientset()
	sharedInformers := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())
	manager, err := NewController(ctx, clientset, sharedInformers.Core().V1().Pods(), sharedInformers.Batch().V1().Jobs(), sharedInformers.Scheduling().V1beta1().Workloads(), sharedInformers.Scheduling().V1beta1().PodGroups())
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady

	// 启动 informer 与控制器，使 finalizer 清理通过真实 worker 路径执行。
	sharedInformers.Start(ctx.Done())
	sharedInformers.WaitForCacheSync(ctx.Done())
	// 使用 1 个 worker 启动控制器，确保在管 Pod 的 finalizer 由 syncJob 清理；
	// 而失去关联的孤儿 Pod 则由 orphanWorker 负责清理。
	go manager.Run(ctx, 1)
	// 单独启动 orphanWorker，覆盖孤儿 Pod 的清理路径。
	go wait.UntilWithContext(ctx, manager.orphanWorker, time.Second)

	// 创建一个简单 Job 并等待其被控制器感知。
	job := newJob(1, 1, 1, batch.NonIndexedCompletion)
	job, err = clientset.BatchV1().Jobs(job.GetNamespace()).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Creating job: %v", err)
	}

	// 等待 Job 出现在 jobLister 中，否则后续创建的 Pod 可能无法被正确关联跟踪。
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		job, _ := manager.jobLister.Jobs(job.GetNamespace()).Get(job.Name)
		return job != nil, nil
	}); err != nil {
		t.Fatalf("Waiting for Job object to appear in jobLister: %v", err)
	}

	selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
	if err != nil {
		t.Fatalf("Error parsing job selector: %v", err)
	}

	var pods []*v1.Pod
	// 等待控制器创建 Pod，以便后续验证其 finalizer。
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		pods, err = manager.podStore.Pods(job.Namespace).List(selector)
		if err != nil {
			return false, err
		}
		if len(pods) > 0 {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Waiting for Pod to be created by Job: %v", err)
	}

	// 将 Job 标记为完成，触发在管 Pod 的 finalizer 清理。
	job.Status.Conditions = append(job.Status.Conditions, batch.JobCondition{
		Type:   batch.JobComplete,
		Status: v1.ConditionTrue,
	})
	_, err = clientset.BatchV1().Jobs(job.GetNamespace()).UpdateStatus(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Updating job status: %v", err)
	}

	// 完成的 Job 对应的 Pod 不应再保留跟踪 finalizer。
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		p, err := clientset.CoreV1().Pods(pods[0].Namespace).Get(ctx, pods[0].Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return !hasJobTrackingFinalizer(p), nil
	}); err != nil {
		t.Errorf("Waiting for Pod to get the finalizer removed: %v", err)
	}

}

// checkJobCompletionLabel 校验 Pod 模板带有完成索引标签。
func checkJobCompletionLabel(t *testing.T, p *v1.PodTemplateSpec) {
	t.Helper()
	labels := p.GetLabels()
	if labels == nil || labels[batch.JobCompletionIndexAnnotation] == "" {
		t.Errorf("missing expected pod label %s", batch.JobCompletionIndexAnnotation)
	}
}

// checkJobCompletionEnvVariable 校验 InitContainer 与 Container 均注入了 JOB_COMPLETION_INDEX 环境变量。
func checkJobCompletionEnvVariable(t *testing.T, spec *v1.PodSpec) {
	t.Helper()
	fieldPath := fmt.Sprintf("metadata.labels['%s']", batch.JobCompletionIndexAnnotation)
	want := []v1.EnvVar{
		{
			Name: "JOB_COMPLETION_INDEX",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: fieldPath,
				},
			},
		},
	}
	for _, c := range spec.InitContainers {
		if diff := cmp.Diff(want, c.Env); diff != "" {
			t.Errorf("Unexpected Env in container %s (-want,+got):\n%s", c.Name, diff)
		}
	}
	for _, c := range spec.Containers {
		if diff := cmp.Diff(want, c.Env); diff != "" {
			t.Errorf("Unexpected Env in container %s (-want,+got):\n%s", c.Name, diff)
		}
	}
}

// podReplacementPolicy 返回指向给定替换策略的指针，便于构造 Job 配置。
func podReplacementPolicy(m batch.PodReplacementPolicy) *batch.PodReplacementPolicy {
	return &m
}

// verifyEmptyQueueAndAwaitForQueueLen 先确认队列为空，再等待队列达到指定长度。
func verifyEmptyQueueAndAwaitForQueueLen(ctx context.Context, t *testing.T, jm *Controller, wantQueueLen int) {
	t.Helper()
	verifyEmptyQueue(t, jm)
	awaitForQueueLen(ctx, t, jm, wantQueueLen)
}

// awaitForQueueLen 轮询等待队列长度达到期望值，容忍异步入队的时序延迟。
func awaitForQueueLen(ctx context.Context, t *testing.T, jm *Controller, wantQueueLen int) {
	t.Helper()
	verifyEmptyQueue(t, jm)
	if err := wait.PollUntilContextTimeout(ctx, fastRequeue, time.Second, true, func(ctx context.Context) (bool, error) {
		if requeued := jm.queue.Len() == wantQueueLen; requeued {
			return true, nil
		}
		jm.clock.Sleep(fastRequeue)
		return false, nil
	}); err != nil {
		t.Errorf("Failed to await for expected queue.Len(). want %v, got: %v", wantQueueLen, jm.queue.Len())
	}
}

// verifyEmptyQueue 断言工作队列当前为空，用于确认没有意外的入队。
func verifyEmptyQueue(t *testing.T, jm *Controller) {
	t.Helper()
	if jm.queue.Len() > 0 {
		t.Errorf("Unexpected queue.Len(). Want: %d, got: %d", 0, jm.queue.Len())
	}
}

// podBuilder 通过链式调用构造测试用 Pod，减少重复的字段设置代码。
type podBuilder struct {
	*v1.Pod
}

// buildPod 创建带随机 UID 的空 Pod 构建器。
func buildPod() podBuilder {
	return podBuilder{Pod: &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(rand.String(5)),
		},
	}}
}

// getConditionsByType 从条件列表中筛选出指定类型的条件。
func getConditionsByType(list []batch.JobCondition, cType batch.JobConditionType) []*batch.JobCondition {
	var result []*batch.JobCondition
	for i := range list {
		if list[i].Type == cType {
			result = append(result, &list[i])
		}
	}
	return result
}

// name 设置 Pod 名称。
func (pb podBuilder) name(n string) podBuilder {
	pb.Name = n
	return pb
}

// ns 设置 Pod 命名空间。
func (pb podBuilder) ns(n string) podBuilder {
	pb.Namespace = n
	return pb
}

// uid 设置 Pod UID。
func (pb podBuilder) uid(u string) podBuilder {
	pb.UID = types.UID(u)
	return pb
}

// labels 设置 Pod 标签。
func (pb podBuilder) labels(labels map[string]string) podBuilder {
	pb.Labels = labels
	return pb
}

// job 配置 Pod 匹配给定 Job 的选择器与命名空间，并设置控制器所有者引用。
func (pb podBuilder) job(j *batch.Job) podBuilder {
	pb.Labels = j.Spec.Selector.MatchLabels
	pb.Namespace = j.Namespace
	pb.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(j, controllerKind)}
	return pb
}

// owner 追加一个所有者引用。
func (pb podBuilder) owner(ownerRef metav1.OwnerReference) podBuilder {
	pb.OwnerReferences = append(pb.OwnerReferences, ownerRef)
	return pb
}

// clearOwner 清除所有者引用，用于构造孤儿 Pod。
func (pb podBuilder) clearOwner() podBuilder {
	pb.OwnerReferences = nil
	return pb
}

// clearLabels 清除标签，用于构造不匹配任何 Job 的 Pod。
func (pb podBuilder) clearLabels() podBuilder {
	pb.Labels = nil
	return pb
}

// index 设置完成索引注解。
func (pb podBuilder) index(ix string) podBuilder {
	return pb.annotation(batch.JobCompletionIndexAnnotation, ix)
}

// indexFailureCount 设置索引失败次数注解。
func (pb podBuilder) indexFailureCount(count string) podBuilder {
	return pb.annotation(batch.JobIndexFailureCountAnnotation, count)
}

// indexIgnoredFailureCount 设置被忽略的索引失败次数注解。
func (pb podBuilder) indexIgnoredFailureCount(count string) podBuilder {
	return pb.annotation(batch.JobIndexIgnoredFailureCountAnnotation, count)
}

// annotation 设置 Pod 注解，按需初始化注解 map。
func (pb podBuilder) annotation(key, value string) podBuilder {
	if pb.Annotations == nil {
		pb.Annotations = make(map[string]string)
	}
	pb.Annotations[key] = value
	return pb
}

// status 设置 Pod 状态。
func (pb podBuilder) status(s v1.PodStatus) podBuilder {
	pb.Status = s
	return pb
}

// phase 设置 Pod 生命周期阶段。
func (pb podBuilder) phase(p v1.PodPhase) podBuilder {
	pb.Status.Phase = p
	return pb
}

// trackingFinalizer 添加 Job 跟踪 finalizer，已存在时不重复添加。
func (pb podBuilder) trackingFinalizer() podBuilder {
	if slices.Contains(pb.Finalizers, batch.JobTrackingFinalizer) {
		return pb
	}
	pb.Finalizers = append(pb.Finalizers, batch.JobTrackingFinalizer)
	return pb
}

// deletionTimestamp 设置删除时间戳，表示 Pod 正处于删除中。
func (pb podBuilder) deletionTimestamp() podBuilder {
	pb.DeletionTimestamp = &metav1.Time{}
	return pb
}

// customDeletionTimestamp 设置指定的删除时间戳。
func (pb podBuilder) customDeletionTimestamp(t time.Time) podBuilder {
	pb.DeletionTimestamp = &metav1.Time{Time: t}
	return pb
}

// TestSyncJobPodSchedulingGroup 验证不同场景下 Job 创建的 Pod 是否正确携带
// schedulingGroup 以及 PodGroup 所有者引用。
func TestSyncJobPodSchedulingGroup(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	gangJob := newGangSchedulingJob("test-job", 4)
	gangJob.Spec.BackoffLimit = ptr.To[int32](6)
	templateName := fmt.Sprintf("%s-pgt-%d", gangJob.Name, 0)

	makeWorkload := func(job *batch.Job) *schedulingv1beta1.Workload {
		return &schedulingv1beta1.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:            computeWorkloadName(job),
				Namespace:       metav1.NamespaceDefault,
				UID:             types.UID("wl-uid"),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, controllerKind)},
			},
			Spec: schedulingv1beta1.WorkloadSpec{
				ControllerRef: &schedulingv1beta1.TypedLocalObjectReference{
					APIGroup: "batch",
					Kind:     "Job",
					Name:     job.Name,
				},
				PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
					{Name: templateName},
				},
			},
		}
	}

	makePodGroup := func(job *batch.Job, wl *schedulingv1beta1.Workload) *schedulingv1beta1.PodGroup {
		return &schedulingv1beta1.PodGroup{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "scheduling.k8s.io/v1beta1",
				Kind:       "PodGroup",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            computePodGroupName(wl.Name, templateName),
				Namespace:       metav1.NamespaceDefault,
				UID:             types.UID("pg-uid"),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, controllerKind)},
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				WorkloadRef: &schedulingv1beta1.WorkloadReference{
					WorkloadName: wl.Name,
					TemplateName: templateName,
				},
			},
		}
	}

	gangWorkload := makeWorkload(gangJob)
	gangPodGroup := makePodGroup(gangJob, gangWorkload)

	// 普通 Job（未配置 spec.scheduling）在通用工作负载表示下也会生成 Workload/PodGroup，
	// 因此其 Pod 同样应被赋予 schedulingGroup。
	basicJob := newJob(2, 5, 6, batch.NonIndexedCompletion)
	basicPodGroupName := computePodGroupName(computeWorkloadName(basicJob), podGroupTemplateName(basicJob))

	// 覆盖 gang Job、普通 Job、已有 schedulingGroup、挂起及特性关闭五类场景。
	testCases := map[string]struct {
		job              *batch.Job
		existingWorkload *schedulingv1beta1.Workload
		existingPodGroup *schedulingv1beta1.PodGroup

		workloadWithJob bool

		wantSchedulingGroupName string
		wantPodGroupOwnerRef    bool
		wantCreations           int32
	}{
		"gang-eligible job with PodGroup: pods get schedulingGroup and ownerRef": {
			job:              gangJob,
			workloadWithJob:  true,
			existingWorkload: gangWorkload,
			existingPodGroup: gangPodGroup,

			wantSchedulingGroupName: gangPodGroup.Name,
			wantPodGroupOwnerRef:    true,
			wantCreations:           4,
		},
		"basic job: Workload/PodGroup materialized and pods get schedulingGroup": {
			job:                     basicJob,
			workloadWithJob:         true,
			wantSchedulingGroupName: basicPodGroupName,
			wantPodGroupOwnerRef:    true,
			wantCreations:           2,
		},
		"job with pre-existing schedulingGroup: preserved without PodGroup ownerRef": {
			job: func() *batch.Job {
				j := newGangSchedulingJob("opt-out-job", 4)
				j.Spec.BackoffLimit = ptr.To[int32](6)
				j.Spec.Template.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
					PodGroupName: ptr.To("external-pg"),
				}
				return j
			}(),
			workloadWithJob:         true,
			wantSchedulingGroupName: "external-pg",
			wantCreations:           4,
		},
		"suspended gang-eligible job: discovers scheduling objects but creates no pods": {
			job: func() *batch.Job {
				j := newGangSchedulingJob("suspended-job", 4)
				j.Spec.BackoffLimit = ptr.To[int32](6)
				j.Spec.Suspend = ptr.To(true)
				return j
			}(),
			workloadWithJob: true,
			existingWorkload: func() *schedulingv1beta1.Workload {
				j := newGangSchedulingJob("suspended-job", 4)
				return makeWorkload(j)
			}(),
			existingPodGroup: func() *schedulingv1beta1.PodGroup {
				j := newGangSchedulingJob("suspended-job", 4)
				wl := makeWorkload(j)
				return makePodGroup(j, wl)
			}(),
			wantCreations: 0,
		},
		"feature gate disabled: gang-eligible job creates pods without scheduling objects": {
			job: func() *batch.Job {
				j := newGangSchedulingJob("gang-job", 4)
				j.Spec.BackoffLimit = ptr.To[int32](6)
				return j
			}(),
			wantCreations: 4,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objs []runtime.Object
			if tc.existingWorkload != nil {
				objs = append(objs, tc.existingWorkload)
			}
			if tc.existingPodGroup != nil {
				objs = append(objs, tc.existingPodGroup)
			}
			clientSet := fake.NewClientset(objs...)
			// TODO: this will be removed in 1.38.
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.36"))
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: tc.workloadWithJob,
				features.WorkloadWithJob: tc.workloadWithJob,
			})

			jm, sharedInformers := newControllerFromClient(ctx, t, clientSet, controller.NoResyncPeriodFunc)

			fakePodControl := controller.FakePodControl{}
			jm.podControl = &fakePodControl
			jm.updateStatusHandler = func(ctx context.Context, job *batch.Job) (*batch.Job, error) {
				return job, nil
			}

			sharedInformers.Start(ctx.Done())
			sharedInformers.WaitForCacheSync(ctx.Done())

			if err := sharedInformers.Batch().V1().Jobs().Informer().GetIndexer().Add(tc.job); err != nil {
				t.Fatalf("Failed to insert job in index: %v", err)
			}

			err := jm.syncJob(ctx, testutil.GetKey(tc.job, t))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 先校验创建的 Pod 数量，再逐个校验模板中的调度组与所有者引用。
			if int32(len(fakePodControl.Templates)) != tc.wantCreations {
				t.Fatalf("expected %d pod creations, got %d", tc.wantCreations, len(fakePodControl.Templates))
			}

			for i, tmpl := range fakePodControl.Templates {
				if tc.wantSchedulingGroupName != "" {
					if tmpl.Spec.SchedulingGroup == nil {
						t.Errorf("pod template [%d]: expected schedulingGroup, got nil", i)
						continue
					}
					if tmpl.Spec.SchedulingGroup.PodGroupName == nil || *tmpl.Spec.SchedulingGroup.PodGroupName != tc.wantSchedulingGroupName {
						got := "<nil>"
						if tmpl.Spec.SchedulingGroup.PodGroupName != nil {
							got = *tmpl.Spec.SchedulingGroup.PodGroupName
						}
						t.Errorf("pod template [%d]: schedulingGroup.podGroupName = %q, want %q", i, got, tc.wantSchedulingGroupName)
					}
				} else if tmpl.Spec.SchedulingGroup != nil {
					t.Errorf("pod template [%d]: expected nil schedulingGroup, got %v", i, tmpl.Spec.SchedulingGroup)
				}

				hasPGOwnerRef := false
				for _, ref := range tmpl.OwnerReferences {
					if ref.Kind == "PodGroup" {
						hasPGOwnerRef = true
						break
					}
				}
				if tc.wantPodGroupOwnerRef != hasPGOwnerRef {
					t.Errorf("pod template [%d]: PodGroup ownerRef present = %v, want %v", i, hasPGOwnerRef, tc.wantPodGroupOwnerRef)
				}
			}
		})
	}
}

// TestSyncOrphanPodsBySelectorRetryOnFailure 验证按选择器处理孤儿 Pod 时，
// 单个 Pod patch 失败不会中断其余 Pod，失败的 Pod 会被重新入队重试。
func TestSyncOrphanPodsBySelectorRetryOnFailure(t *testing.T) {
	const podsCount = 3

	_, ctx := ktesting.NewTestContext(t)
	clientset := fake.NewClientset()
	sharedInformers := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())
	manager, err := NewController(ctx, clientset, sharedInformers.Core().V1().Pods(), sharedInformers.Batch().V1().Jobs(), sharedInformers.Scheduling().V1beta1().Workloads(), sharedInformers.Scheduling().V1beta1().PodGroups())
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	manager.podStoreSynced = alwaysReady
	manager.jobStoreSynced = alwaysReady
	manager.podControl = &controller.FakePodControl{}

	podInformer := sharedInformers.Core().V1().Pods().Informer()
	go podInformer.RunWithContext(ctx)
	cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced)

	selector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"app": "test",
		},
	}
	selectorString, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		t.Fatalf("Error parsing pod label selector: %v", err)
	}

	var pods []*v1.Pod
	for i := range podsCount {
		pod := buildPod().name(fmt.Sprintf("pod%d", i+1)).ns("default").labels(selector.MatchLabels).deletionTimestamp().trackingFinalizer().Pod
		pods = append(pods, pod)
	}

	podIndexer := podInformer.GetIndexer()
	for _, pod := range pods {
		if err := podIndexer.Add(pod); err != nil {
			t.Fatalf("Failed to add pod to indexer: %v", err)
		}
	}

	for _, pod := range pods {
		_, err = clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Creating pod: %v", err)
		}
	}

	orphanKey := orphanPodKey{
		kind:      OrphanPodKeyKindSelector,
		namespace: "default",
		value:     selectorString.String(),
	}

	fakePodControl := manager.podControl.(*controller.FakePodControl)
	// 注入 patch 失败，验证失败的 Pod 会被加入孤儿队列以便后续重试。
	fakePodControl.Err = errors.New("server temporarily unavailable")

	err = manager.syncOrphanPod(ctx, orphanKey)
	if err != nil {
		t.Fatalf("syncOrphanPod should not return error even if some pods fail: %v", err)
	}

	// 即使部分 patch 失败，每个 Pod 仍应被尝试处理一次。
	if len(fakePodControl.Patches) != podsCount {
		t.Errorf("Expected %d patch attempts (one for each pod), got %d", podsCount, len(fakePodControl.Patches))
	}

	fakePodControl.Patches = nil
	fakePodControl.Err = nil

	// 等待限速器的退避延迟过期后再重试。
	// 默认限速器采用指数退避，初始延迟 5ms，
	// 但多次失败后延迟可能延长到秒级；此等待确保 drain 队列时条目已可被处理，
	// 而不会因 When() 返回未来时间而继续阻塞。
	//
	time.Sleep(2 * time.Second)

	for range podsCount {
		key, quit := manager.orphanQueue.Get()
		if quit {
			t.Fatalf("Queue unexpectedly empty")
		}
		if key.kind != OrphanPodKeyKindName {
			t.Errorf("Expected requeued key kind to be %v, got %v", OrphanPodKeyKindName, key.kind)
		}
		manager.orphanQueue.Done(key)

		err := manager.syncOrphanPod(ctx, key)
		if err != nil {
			t.Fatalf("syncOrphanPod failed on retry: %v", err)
		}
	}

	// 重试时每个 Pod 都应成功打上移除 finalizer 的 patch。
	if len(fakePodControl.Patches) != podsCount {
		t.Errorf("Expected %d patch attempts on retry, got %d", podsCount, len(fakePodControl.Patches))
	}

	// 逐一确认 patch 内容确实是删除 Job 跟踪 finalizer。
	for i, patch := range fakePodControl.Patches {
		patchStr := string(patch)
		if !strings.Contains(patchStr, "$deleteFromPrimitiveList/finalizers") || !strings.Contains(patchStr, batch.JobTrackingFinalizer) {
			t.Errorf("Patch %d: expected finalizer removal patch, got %s", i, patchStr)
		}
	}
}

// setDurationDuringTest 临时替换时长变量并返回恢复函数，通常由 t.Cleanup 调用。
func setDurationDuringTest(val *time.Duration, newVal time.Duration) func() {
	origVal := *val
	*val = newVal
	return func() {
		*val = origVal
	}
}
