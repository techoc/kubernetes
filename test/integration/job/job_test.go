/*
Copyright 2021 The Kubernetes Authors.

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
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	typedv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	restclient "k8s.io/client-go/rest"
	cache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	apischeduling "k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/controller"
	jobcontroller "k8s.io/kubernetes/pkg/controller/job"
	"k8s.io/kubernetes/pkg/controller/job/metrics"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/integration/util"
	"k8s.io/utils/ptr"
)

const waitInterval = time.Second
const fastPodFailureBackoff = 100 * time.Millisecond
const fastSyncJobBatchPeriod = 100 * time.Millisecond

// 用于补偿控制器延迟的等待时长，适用于预期 Job 控制器不会做出变更的测试。
// 这类测试会稍等片刻（略长于两次控制器同步的典型耗时），
// 然后确认确实没有发生变化。
const sleepDurationForControllerLatency = 100 * time.Millisecond

const restConfigQPS = 10_000
const restConfigBurst = 10_000

type metricLabelsWithValue struct {
	Labels []string
	Value  int
}

func validateCounterMetric(ctx context.Context, t *testing.T, counterVec *basemetrics.CounterVec, wantMetric metricLabelsWithValue) {
	t.Helper()
	var cmpErr error
	err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		cmpErr = nil
		value, err := testutil.GetCounterMetricValue(counterVec.WithLabelValues(wantMetric.Labels...))
		if err != nil {
			return true, fmt.Errorf("collecting the %q metric: %w", counterVec.Name, err)
		}
		if wantMetric.Value != int(value) {
			cmpErr = fmt.Errorf("Unexpected metric delta for %q metric with labels %q. want: %v, got: %v", counterVec.Name, wantMetric.Labels, wantMetric.Value, int(value))
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Errorf("Failed waiting for expected metric: %v", err)
	}
	if cmpErr != nil {
		t.Error(cmpErr)
	}
}

func validateTerminatedPodsTrackingFinalizerMetric(ctx context.Context, t *testing.T, want int) {
	validateCounterMetric(ctx, t, metrics.TerminatedPodsTrackingFinalizerTotal, metricLabelsWithValue{
		Value:  want,
		Labels: []string{metrics.Add},
	})
	validateCounterMetric(ctx, t, metrics.TerminatedPodsTrackingFinalizerTotal, metricLabelsWithValue{
		Value:  want,
		Labels: []string{metrics.Delete},
	})
}

// TestJobPodFailurePolicyWithFailedPodDeletedDuringControllerRestart 验证在以下场景中 Job 能被正确标记为 Failed：
// Job 控制器在移除 pod finalizer 之后、基于 pod failure policy 将 Job 标记为 Failed 之前崩溃。
// 移除失败 pod 的 finalizer 后，我们删除该失败 pod，以模拟 PodGC 的行为。
// 随后测试启动第二个控制器实例，检查它能否正确接手 Job 状态并将其标记为 Failed，
// 即使触发 pod failure policy 的 pod 已被删除。
// 注意：此场景需要使用 finalizer。没有 finalizer 时，无法保证失败的 pod 在被 PodGC 移除之前
// 会先经过 pod failure policy 规则的检查。
func TestJobPodFailurePolicyWithFailedPodDeletedDuringControllerRestart(t *testing.T) {
	count := int32(3)
	job := batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:                     "main-container",
							Image:                    "foo",
							ImagePullPolicy:          v1.PullIfNotPresent,
							TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
						},
					},
				},
			},
			Parallelism: &count,
			Completions: &count,
			PodFailurePolicy: &batchv1.PodFailurePolicy{
				Rules: []batchv1.PodFailurePolicyRule{
					{
						Action: batchv1.PodFailurePolicyActionFailJob,
						OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
							Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
							Values:   []int32{5},
						},
					},
				},
			},
		},
	}
	podStatusMatchingOnExitCodesTerminateRule := v1.PodStatus{
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
	}
	closeFn, restConfig, cs, ns := setup(t, "simple")
	t.Cleanup(closeFn)

	// 让 Job 控制器显著变慢，以触发竞态条件。
	restConfig.QPS = 1
	restConfig.Burst = 1
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})
	resetMetrics()
	restConfig.QPS = restConfigQPS
	restConfig.Burst = restConfigBurst

	// 创建一个 Job，其中包含一个匹配 exit code 规则的失败 pod 和几个成功的 pod
	jobObj, err := createJobWithDefaults(ctx, cs, ns.Name, &job)
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, cs, jobObj, "", podsByStatus{
		Active:      int(count),
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	jobPods, err := getJobPods(ctx, t, cs, jobObj, func(s v1.PodStatus) bool {
		return (s.Phase == v1.PodPending || s.Phase == v1.PodRunning)
	})
	if err != nil {
		t.Fatalf("Failed to list Job Pods: %v", err)
	}

	failedIndex := 1
	wg := sync.WaitGroup{}
	wg.Add(1)

	// 等待失败的 pod（索引为 failedIndex）的 finalizer 被移除。
	// Job 控制器会在向 Job 添加 FailureTarget condition（将其标记为待失败）之后
	// 立即移除该 finalizer。
	go func(ctx context.Context) {
		err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, time.Minute, true, func(ctx context.Context) (bool, error) {
			failedPodUpdated, err := cs.CoreV1().Pods(jobObj.Namespace).Get(ctx, jobPods[failedIndex].Name, metav1.GetOptions{})
			if err != nil {
				return true, err
			}
			if len(failedPodUpdated.Finalizers) == 0 {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			t.Logf("Failed awaiting for the finalizer removal for pod %v", klog.KObj(jobPods[failedIndex]))
		}
		wg.Done()
	}(ctx)

	// 我们将一个 pod 更新为匹配 pod failure policy 规则的失败状态。
	// 这会促使 Job 控制器移除该 pod 的 finalizer。
	failedPod := jobPods[failedIndex]
	updatedPod := failedPod.DeepCopy()
	updatedPod.Status = podStatusMatchingOnExitCodesTerminateRule
	_, err = updatePodStatuses(ctx, cs, []v1.Pod{*updatedPod})
	if err != nil {
		t.Fatalf("Failed to update pod statuses %q for pods of job %q", err, klog.KObj(jobObj))
	}
	wg.Wait()

	t.Logf("Finalizer is removed for the failed pod %q. Shutting down the controller.", klog.KObj(failedPod))
	// 在第一个 Job 控制器移除失败 pod 的 finalizer 后立即将其关闭。
	// 这很可能发生在第一个控制器来得及把 Job 标记为 Failed 之前。
	cancel()

	// 删除失败的 pod，确保第二个控制器实例不会用到它
	_, ctx = ktesting.NewTestContext(t)
	ctx, cancel = context.WithCancel(ctx)
	err = cs.CoreV1().Pods(failedPod.Namespace).Delete(ctx, failedPod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
	if err != nil {
		t.Fatalf("Error: '%v' while deleting pod: '%v'", err, klog.KObj(failedPod))
	}
	t.Logf("The failed pod %q is deleted", klog.KObj(failedPod))
	cancel()

	// 启动第二个控制器，将临时的 FailureTarget Job condition 升级为 Failed
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)
	// 验证 Job 被正确标记为 Failed
	validateJobFailed(ctx, t, cs, jobObj)
	validateNoOrphanPodsWithFinalizers(ctx, t, cs, jobObj)
}

// TestJobPodFailurePolicy 测试根据配置的 pod failure policy 规则处理 pod 失败的行为
func TestJobPodFailurePolicy(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	job := batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:                     "main-container",
							Image:                    "foo",
							ImagePullPolicy:          v1.PullIfNotPresent,
							TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
						},
					},
				},
			},
			PodFailurePolicy: &batchv1.PodFailurePolicy{
				Rules: []batchv1.PodFailurePolicyRule{
					{
						Action: batchv1.PodFailurePolicyActionIgnore,
						OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
							{
								Type: v1.DisruptionTarget,
							},
						},
					},
					{
						Action: batchv1.PodFailurePolicyActionCount,
						OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
							Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
							Values:   []int32{10},
						},
					},
					{
						Action: batchv1.PodFailurePolicyActionFailJob,
						OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
							Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
							Values:   []int32{5, 6, 7},
						},
					},
				},
			},
		},
	}
	podStatusMatchingOnExitCodesTerminateRule := v1.PodStatus{
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
	}
	podStatusMatchingOnExitCodesCountRule := v1.PodStatus{
		Phase: v1.PodFailed,
		ContainerStatuses: []v1.ContainerStatus{
			{
				Name: "main-container",
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{
						ExitCode: 10,
					},
				},
			},
		},
	}
	podStatusMatchingOnPodConditionsIgnoreRule := v1.PodStatus{
		Phase: v1.PodFailed,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.DisruptionTarget,
				Status: v1.ConditionTrue,
			},
		},
	}
	podStatusNotMatchingAnyRule := v1.PodStatus{
		Phase: v1.PodFailed,
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{},
				},
			},
		},
	}
	testCases := map[string]struct {
		restartController                        bool
		job                                      batchv1.Job
		podStatus                                v1.PodStatus
		wantActive                               int
		wantFailed                               int
		wantJobConditionType                     batchv1.JobConditionType
		wantJobFinishedMetric                    metricLabelsWithValue
		wantPodFailuresHandledByPolicyRuleMetric *metricLabelsWithValue
	}{
		"pod status matching the configured FailJob rule on exit codes; job terminated": {
			job:                  job,
			podStatus:            podStatusMatchingOnExitCodesTerminateRule,
			wantActive:           0,
			wantFailed:           1,
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "failed", "PodFailurePolicy"},
				Value:  1,
			},
			wantPodFailuresHandledByPolicyRuleMetric: &metricLabelsWithValue{
				Labels: []string{"FailJob"},
				Value:  1,
			},
		},
		"pod status matching the configured FailJob rule on exit codes; with controller restart; job terminated": {
			restartController:    true,
			job:                  job,
			podStatus:            podStatusMatchingOnExitCodesTerminateRule,
			wantActive:           0,
			wantFailed:           1,
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "failed", "PodFailurePolicy"},
				Value:  1,
			},
		},
		"pod status matching the configured Ignore rule on pod conditions; pod failure not counted": {
			job:                  job,
			podStatus:            podStatusMatchingOnPodConditionsIgnoreRule,
			wantActive:           1,
			wantFailed:           0,
			wantJobConditionType: batchv1.JobComplete,
			wantPodFailuresHandledByPolicyRuleMetric: &metricLabelsWithValue{
				Labels: []string{"Ignore"},
				Value:  1,
			},
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
				Value:  1,
			},
		},
		"pod status matching the configured Count rule on exit codes; pod failure counted": {
			job:                  job,
			podStatus:            podStatusMatchingOnExitCodesCountRule,
			wantActive:           1,
			wantFailed:           1,
			wantJobConditionType: batchv1.JobComplete,
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
				Value:  1,
			},
			wantPodFailuresHandledByPolicyRuleMetric: &metricLabelsWithValue{
				Labels: []string{"Count"},
				Value:  1,
			},
		},
		"pod status non-matching any configured rule; pod failure counted": {
			job:                  job,
			podStatus:            podStatusNotMatchingAnyRule,
			wantActive:           1,
			wantFailed:           1,
			wantJobConditionType: batchv1.JobComplete,
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
				Value:  1,
			},
			wantPodFailuresHandledByPolicyRuleMetric: &metricLabelsWithValue{
				Labels: []string{"Count"},
				Value:  0,
			},
		},
		"pod status matching the configured FailJob rule on a negative exit code - needed for Windows support": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:                     "main-container",
									Image:                    "foo",
									ImagePullPolicy:          v1.PullIfNotPresent,
									TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
								},
							},
						},
					},
					PodFailurePolicy: &batchv1.PodFailurePolicy{
						Rules: []batchv1.PodFailurePolicyRule{
							{
								Action: batchv1.PodFailurePolicyActionFailJob,
								OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
									Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{-1073741510, 137},
								},
							},
						},
					},
				},
			},
			podStatus: v1.PodStatus{
				Phase: v1.PodFailed,
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name: "main-container",
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{
								ExitCode: -1073741510,
							},
						},
					},
				},
			},
			wantActive:           0,
			wantFailed:           1,
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedMetric: metricLabelsWithValue{
				Labels: []string{"NonIndexed", "failed", "PodFailurePolicy"},
				Value:  1,
			},
			wantPodFailuresHandledByPolicyRuleMetric: &metricLabelsWithValue{
				Labels: []string{"FailJob"},
				Value:  1,
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "pod-failure-policy")
	t.Cleanup(closeFn)
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			resetMetrics()

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(func() {
				cancel()
			})

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &test.job)
			if err != nil {
				t.Fatalf("Error %q while creating the job %q", err, jobObj.Name)
			}
			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      1,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			op := func(p *v1.Pod) bool {
				p.Status = test.podStatus
				return true
			}

			if _, err := updateJobPodsStatus(ctx, clientSet, jobObj, op, 1); err != nil {
				t.Fatalf("Error %q while updating pod status for Job: %v", err, jobObj.Name)
			}

			if test.restartController {
				cancel()
				ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)
			}

			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      test.wantActive,
				Failed:      test.wantFailed,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			if test.wantJobConditionType == batchv1.JobComplete {
				if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
					t.Fatalf("Failed setting phase %q on Job Pod: %v", v1.PodSucceeded, err)
				}
			}
			validateJobCondition(ctx, t, clientSet, jobObj, test.wantJobConditionType)
			validateCounterMetric(ctx, t, metrics.JobFinishedNum, test.wantJobFinishedMetric)
			if test.wantPodFailuresHandledByPolicyRuleMetric != nil {
				validateCounterMetric(ctx, t, metrics.PodFailuresHandledByFailurePolicy, *test.wantPodFailuresHandledByPolicyRuleMetric)
			}
			validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
		})
	}
}

// TestSuccessPolicy 测试使用 successPolicy 时 Job 及其 pod 的处理行为。
func TestSuccessPolicy(t *testing.T) {
	type podTerminationWithExpectations struct {
		index                int
		status               v1.PodStatus
		wantActive           int
		wantFailed           int
		wantSucceeded        int
		wantActiveIndexes    sets.Set[int]
		wantCompletedIndexes string
		wantFailedIndexes    *string
		wantTerminating      *int32
	}

	podTemplateSpec := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:                     "main-container",
					Image:                    "foo",
					ImagePullPolicy:          v1.PullIfNotPresent,
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				},
			},
		},
	}
	testCases := map[string]struct {
		job                      batchv1.Job
		podTerminations          []podTerminationWithExpectations
		wantConditionTypes       []batchv1.JobConditionType
		wantJobFinishedNumMetric []metricLabelsWithValue
	}{
		"all indexes succeeded": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](1),
					Completions:    ptr.To[int32](1),
					CompletionMode: completionModePtr(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
					SuccessPolicy: &batchv1.SuccessPolicy{
						Rules: []batchv1.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("0"),
						}},
					},
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantActive:           0,
					wantFailed:           0,
					wantSucceeded:        1,
					wantCompletedIndexes: "0",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete},
			wantJobFinishedNumMetric: []metricLabelsWithValue{
				{
					Labels: []string{"Indexed", "succeeded", "SuccessPolicy"},
					Value:  1,
				},
			},
		},
		"job without successPolicy; incremented the jobs_finished_total metric with CompletionsReached reason": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](1),
					Completions:    ptr.To[int32](1),
					CompletionMode: completionModePtr(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantActive:           0,
					wantFailed:           0,
					wantSucceeded:        1,
					wantCompletedIndexes: "0",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete},
			wantJobFinishedNumMetric: []metricLabelsWithValue{
				{
					Labels: []string{"Indexed", "succeeded", "CompletionsReached"},
					Value:  1,
				},
			},
		},
		"job with successPolicy with succeededIndexes; job has SuccessCriteriaMet and Complete conditions even if some indexes remain pending": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					CompletionMode: completionModePtr(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
					SuccessPolicy: &batchv1.SuccessPolicy{
						Rules: []batchv1.SuccessPolicyRule{{
							SucceededIndexes: ptr.To("1"),
						}},
					},
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodPending,
					},
					wantActive:        2,
					wantActiveIndexes: sets.New(0, 1),
					wantFailed:        0,
					wantSucceeded:     0,
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantActive:           0,
					wantFailed:           0,
					wantSucceeded:        1,
					wantCompletedIndexes: "1",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete},
			wantJobFinishedNumMetric: []metricLabelsWithValue{
				{
					Labels: []string{"Indexed", "succeeded", "SuccessPolicy"},
					Value:  1,
				},
			},
		},
		"job with successPolicy with succeededCount; job has SuccessCriteriaMet and Complete conditions even if some indexes remain pending": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					CompletionMode: completionModePtr(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
					SuccessPolicy: &batchv1.SuccessPolicy{
						Rules: []batchv1.SuccessPolicyRule{{
							SucceededCount: ptr.To[int32](1),
						}},
					},
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodPending,
					},
					wantActive:        2,
					wantActiveIndexes: sets.New(0, 1),
					wantFailed:        0,
					wantSucceeded:     0,
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantActive:           0,
					wantFailed:           0,
					wantSucceeded:        1,
					wantCompletedIndexes: "1",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete},
			wantJobFinishedNumMetric: []metricLabelsWithValue{
				{
					Labels: []string{"Indexed", "succeeded", "SuccessPolicy"},
					Value:  1,
				},
			},
		},
		"job with successPolicy and backoffLimitPerIndex; job has a Failed condition if job meets backoffLimitPerIndex": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](0),
					Template:             podTemplateSpec,
					SuccessPolicy: &batchv1.SuccessPolicy{
						Rules: []batchv1.SuccessPolicyRule{{
							SucceededCount: ptr.To[int32](1),
						}},
					},
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        1,
					wantActiveIndexes: sets.New(1),
					wantFailed:        1,
					wantFailedIndexes: ptr.To("0"),
					wantSucceeded:     0,
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantActive:           0,
					wantFailed:           1,
					wantSucceeded:        1,
					wantFailedIndexes:    ptr.To("0"),
					wantCompletedIndexes: "1",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobFailed},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "success-policy")
	t.Cleanup(closeFn)
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			resetMetrics()

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &tc.job)
			if err != nil {
				t.Fatalf("Error %v while creating the Job %q", err, jobObj.Name)
			}
			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      int(*tc.job.Spec.Parallelism),
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})
			for _, podTermination := range tc.podTerminations {
				pod, err := getActivePodForIndex(ctx, clientSet, jobObj, podTermination.index)
				if err != nil {
					t.Fatalf("Listing Job Pods: %v", err)
				}
				pod.Status = podTermination.status
				if _, err = clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatalf("Error updating the Pod %q: %v", klog.KObj(pod), err)
				}
				validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
					Active:      podTermination.wantActive,
					Succeeded:   podTermination.wantSucceeded,
					Failed:      podTermination.wantFailed,
					Ready:       ptr.To[int32](0),
					Terminating: podTermination.wantTerminating,
				})
				validateIndexedJobPods(ctx, t, clientSet, jobObj, podTermination.wantActiveIndexes, podTermination.wantCompletedIndexes, podTermination.wantFailedIndexes)
			}
			for i := range tc.wantConditionTypes {
				validateJobCondition(ctx, t, clientSet, jobObj, tc.wantConditionTypes[i])
			}
			for i := range tc.wantJobFinishedNumMetric {
				validateCounterMetric(ctx, t, metrics.JobFinishedNum, tc.wantJobFinishedNumMetric[i])
			}
			validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
		})
	}
}

// TestBackoffLimitPerIndex_DelayedPodDeletion 测试在使用 BackoffLimitPerIndex 时，
// pod 的删除会延迟到替代 pod 创建之后，从而使替代 pod 的 index-failure-count
// annotation 递增。
func TestBackoffLimitPerIndex_DelayedPodDeletion(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))

	closeFn, restConfig, clientSet, ns := setup(t, "backoff-limit-per-index-failed")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism:          ptr.To[int32](1),
			Completions:          ptr.To[int32](1),
			BackoffLimitPerIndex: ptr.To[int32](1),
			CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0), "", ptr.To(""))

	// 索引 0 的第一个 pod 失败。
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 0); err != nil {
		t.Fatal("Failed trying to fail pod with index 0")
	}
	// 删除失败的 pod
	pod, err := getJobPodForIndex(ctx, clientSet, jobObj, 0, func(_ *v1.Pod) bool { return true })
	if err != nil {
		t.Fatalf("failed to get terminal pod for index: %v", 0)
	}
	if err := clientSet.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete pod: %v, error: %v", klog.KObj(pod), err)
	}

	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Failed:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0), "", ptr.To(""))

	// 验证替代 pod 已创建，且其 index-failure-count annotation 已递增。
	replacement, err := getActivePodForIndex(ctx, clientSet, jobObj, 0)
	if err != nil {
		t.Fatalf("Failed to get active replacement pod for index: %v, error: %v", 0, err)
	}
	gotIndexFailureCount, err := getIndexFailureCount(replacement)
	if err != nil {
		t.Fatalf("Failed read the index failure count annotation for pod: %v, error: %v", klog.KObj(replacement), err)
	}
	if diff := cmp.Diff(1, gotIndexFailureCount); diff != "" {
		t.Errorf("Unexpected index failure count for the replacement pod: %s", diff)
	}
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, 0); err != nil {
		t.Fatal("Failed trying to fail pod with index 0")
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      0,
		Succeeded:   1,
		Failed:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateJobComplete(ctx, t, clientSet, jobObj)
}

// TestBackoffLimitPerIndex_JobPodsCreatedWithExponentialBackoff 测试 pod 会按每个索引独立计算的
// 指数退避延迟重建。场景：
// - 索引 0 失败
// - 索引 0 失败
// - 索引 1 失败
// - 索引 0 成功
// - 索引 1 失败
// - 索引 1 成功
func TestBackoffLimitPerIndex_JobPodsCreatedWithExponentialBackoff(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, 2*time.Second))

	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Completions:          ptr.To[int32](2),
			Parallelism:          ptr.To[int32](2),
			BackoffLimitPerIndex: ptr.To[int32](2),
			CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
		},
	})
	if err != nil {
		t.Fatalf("Could not create job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 1), "", ptr.To(""))

	// 让索引 0 的第一个 pod 失败
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 0); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      2,
		Failed:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 1), "", ptr.To(""))

	// 让索引 0 的第二个 pod 失败
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 0); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      2,
		Failed:      2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 1), "", ptr.To(""))

	// 让索引 1 的第一个 pod 失败
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      2,
		Failed:      3,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 1), "", ptr.To(""))

	// 让索引 0 的第三个 pod 成功
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, 0); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodSucceeded, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Failed:      3,
		Succeeded:   1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(1), "0", ptr.To(""))

	// 让索引 1 的第二个 pod 失败
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Failed:      4,
		Succeeded:   1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(1), "0", ptr.To(""))

	// 让索引 1 的第三个 pod 成功
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodSucceeded, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      0,
		Failed:      4,
		Succeeded:   2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New[int](), "0,1", ptr.To(""))
	validateJobComplete(ctx, t, clientSet, jobObj)

	for index := 0; index < int(*jobObj.Spec.Completions); index++ {
		podsForIndex, err := getJobPodsForIndex(ctx, clientSet, jobObj, index, func(_ *v1.Pod) bool { return true })
		if err != nil {
			t.Fatalf("Failed to list job %q pods for index %v, error: %v", klog.KObj(jobObj), index, err)
		}
		validateExpotentialBackoffDelay(t, jobcontroller.DefaultJobPodFailureBackOff, podsForIndex)
	}
}

// TestDelayTerminalPhaseCondition 测试 Job 控制器的修复：直到所有 Pod 都进入终止状态后，
// 才设置终止阶段的 condition（Failed 和 Complete）。Job 的最终命运由临时的 Job condition
// FailureTarget 或 SuccessCriteriaMet 指示。
func TestDelayTerminalPhaseCondition(t *testing.T) {
	const blockDeletionFinalizerForTest string = "fake.example.com/blockDeletion"
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))

	podTemplateSpec := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers: []string{blockDeletionFinalizerForTest},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:                     "main-container",
					Image:                    "foo",
					ImagePullPolicy:          v1.PullIfNotPresent,
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				},
			},
		},
	}
	failOnePod := func(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job) {
		if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
			t.Fatalf("Failed setting phase %q on Job Pod: %v", v1.PodFailed, err)
		}
	}
	succeedOnePodAndScaleDown := func(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job) {
		// 将一个 pod 标记为成功
		if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, 0); err != nil {
			t.Fatalf("Failed setting phase %q on Job Pod: %v", v1.PodSucceeded, err)
		}
		jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)
		if _, err := updateJob(ctx, jobClient, jobObj.Name, func(j *batchv1.Job) {
			j.Spec.Parallelism = ptr.To[int32](1)
			j.Spec.Completions = ptr.To[int32](1)
		}); err != nil {
			t.Fatalf("Unexpected error when scaling down the job: %v", err)
		}
	}

	testCases := map[string]struct {
		enableJobManagedBy bool

		job                batchv1.Job
		action             func(context.Context, clientset.Interface, *batchv1.Job)
		wantInterimStatus  *batchv1.JobStatus
		wantTerminalStatus batchv1.JobStatus
	}{
		"job backoff limit exceeded; JobManagedBy disabled": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:  ptr.To[int32](2),
					Completions:  ptr.To[int32](2),
					Template:     podTemplateSpec,
					BackoffLimit: ptr.To[int32](0),
				},
			},
			action: failOnePod,
			wantInterimStatus: &batchv1.JobStatus{
				Failed:      2,
				Terminating: ptr.To[int32](1),
				Ready:       ptr.To[int32](0),
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailureTarget,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
				},
			},
			wantTerminalStatus: batchv1.JobStatus{
				Failed:      2,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailureTarget,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
					{
						Type:   batchv1.JobFailed,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
				},
			},
		},
		"job backoff limit exceeded; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:  ptr.To[int32](2),
					Completions:  ptr.To[int32](2),
					Template:     podTemplateSpec,
					BackoffLimit: ptr.To[int32](0),
				},
			},
			action: failOnePod,
			wantInterimStatus: &batchv1.JobStatus{
				Failed:      2,
				Terminating: ptr.To[int32](1),
				Ready:       ptr.To[int32](0),
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailureTarget,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
				},
			},
			wantTerminalStatus: batchv1.JobStatus{
				Failed:      2,
				Terminating: ptr.To[int32](0),
				Ready:       ptr.To[int32](0),
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailureTarget,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
					{
						Type:   batchv1.JobFailed,
						Status: v1.ConditionTrue,
						Reason: batchv1.JobReasonBackoffLimitExceeded,
					},
				},
			},
		},
		"job scale down to meet completions; JobManagedBy disabled": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					CompletionMode: ptr.To(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
				},
			},
			action: succeedOnePodAndScaleDown,
			wantInterimStatus: &batchv1.JobStatus{
				Succeeded:        1,
				Ready:            ptr.To[int32](0),
				Terminating:      ptr.To[int32](1),
				CompletedIndexes: "0",
				Conditions: []batchv1.JobCondition{
					{
						Type:    batchv1.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
			wantTerminalStatus: batchv1.JobStatus{
				Succeeded:        1,
				Ready:            ptr.To[int32](0),
				Terminating:      ptr.To[int32](0),
				CompletedIndexes: "0",
				Conditions: []batchv1.JobCondition{
					{
						Type:    batchv1.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
					{
						Type:    batchv1.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
		},
		"job scale down to meet completions; JobManagedBy enabled": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To[int32](2),
					Completions:    ptr.To[int32](2),
					CompletionMode: ptr.To(batchv1.IndexedCompletion),
					Template:       podTemplateSpec,
				},
			},
			action: succeedOnePodAndScaleDown,
			wantInterimStatus: &batchv1.JobStatus{
				Succeeded:        1,
				Terminating:      ptr.To[int32](1),
				Ready:            ptr.To[int32](0),
				CompletedIndexes: "0",
				Conditions: []batchv1.JobCondition{
					{
						Type:    batchv1.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
			wantTerminalStatus: batchv1.JobStatus{
				Succeeded:        1,
				Terminating:      ptr.To[int32](0),
				Ready:            ptr.To[int32](0),
				CompletedIndexes: "0",
				Conditions: []batchv1.JobCondition{
					{
						Type:    batchv1.JobSuccessCriteriaMet,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
					{
						Type:    batchv1.JobComplete,
						Status:  v1.ConditionTrue,
						Reason:  batchv1.JobReasonCompletionsReached,
						Message: "Reached expected number of succeeded pods",
					},
				},
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "delay-terminal-condition")
	t.Cleanup(closeFn)
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			resetMetrics()
			if !test.enableJobManagedBy {
				// TODO: 将在 1.38 中移除。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.JobManagedBy: test.enableJobManagedBy,
			})

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &test.job)
			if err != nil {
				t.Fatalf("Error %q while creating the job %q", err, jobObj.Name)
			}
			t.Cleanup(func() {
				if err := cleanUp(ctx, clientSet, jobObj, []string{blockDeletionFinalizerForTest, batchv1.JobTrackingFinalizer}); err != nil {
					t.Fatalf("Failed cleanup: %v", err)
				}
			})
			jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)

			waitForPodsToBeActive(ctx, t, jobClient, *jobObj.Spec.Parallelism, jobObj)

			test.action(ctx, clientSet, jobObj)
			if test.wantInterimStatus != nil {
				validateJobStatus(ctx, t, clientSet, jobObj, "Interim", *test.wantInterimStatus)

				// 将所有剩余 pod 设为终止阶段，以模拟 Kubelet
				// （或 PodGC 等其他组件）的行为。
				jobPods, err := getJobPods(ctx, t, clientSet, jobObj, func(s v1.PodStatus) bool {
					return (s.Phase == v1.PodPending || s.Phase == v1.PodRunning)
				})
				if err != nil {
					t.Fatalf("Failed to list Job Pods: %v", err)
				}
				if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, len(jobPods)); err != nil {
					t.Fatalf("Failed setting phase %q on Job Pod: %v", v1.PodSucceeded, err)
				}
			}
			validateJobStatus(ctx, t, clientSet, jobObj, "Terminal", test.wantTerminalStatus)
		})
	}
}

// TestBackoffLimitPerIndex 测试使用 backoff limit per index 时 Job 及其 pod 的处理行为。
func TestBackoffLimitPerIndex(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))

	type podTerminationWithExpectations struct {
		index                          int
		status                         v1.PodStatus
		wantActive                     int
		wantFailed                     int
		wantSucceeded                  int
		wantActiveIndexes              sets.Set[int]
		wantCompletedIndexes           string
		wantFailedIndexes              *string
		wantReplacementPodFailureCount *int
		wantTerminating                *int32
	}

	podTemplateSpec := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:                     "main-container",
					Image:                    "foo",
					ImagePullPolicy:          v1.PullIfNotPresent,
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				},
			},
		},
	}
	testCases := map[string]struct {
		job                               batchv1.Job
		podTerminations                   []podTerminationWithExpectations
		wantJobConditionType              batchv1.JobConditionType
		wantJobFinishedIndexesTotalMetric []metricLabelsWithValue
	}{
		"job succeeded": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					Template:             podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:                     2,
					wantFailed:                     1,
					wantActiveIndexes:              sets.New(0, 1),
					wantFailedIndexes:              ptr.To(""),
					wantReplacementPodFailureCount: ptr.To(1),
					wantTerminating:                ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobComplete,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"succeeded", "perIndex"},
					Value:  2,
				},
			},
		},
		"job index fails due to exceeding backoff limit per index": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](2),
					Template:             podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:                     2,
					wantFailed:                     1,
					wantActiveIndexes:              sets.New(0, 1),
					wantFailedIndexes:              ptr.To(""),
					wantReplacementPodFailureCount: ptr.To(1),
					wantTerminating:                ptr.To[int32](0),
				},
				{
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:                     2,
					wantFailed:                     2,
					wantActiveIndexes:              sets.New(0, 1),
					wantFailedIndexes:              ptr.To(""),
					wantReplacementPodFailureCount: ptr.To(2),
					wantTerminating:                ptr.To[int32](0),
				},
				{
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        1,
					wantFailed:        3,
					wantActiveIndexes: sets.New(1),
					wantFailedIndexes: ptr.To("0"),
					wantTerminating:   ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"failed", "perIndex"},
					Value:  1,
				},
				{
					Labels: []string{"succeeded", "perIndex"},
					Value:  1,
				},
			},
		},
		"job index fails due to exceeding the global backoff limit first": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](3),
					Completions:          ptr.To[int32](3),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					BackoffLimit:         ptr.To[int32](2),
					Template:             podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        3,
					wantFailed:        1,
					wantActiveIndexes: sets.New(0, 1, 2),
					wantFailedIndexes: ptr.To(""),
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        3,
					wantFailed:        2,
					wantActiveIndexes: sets.New(0, 1, 2),
					wantFailedIndexes: ptr.To(""),
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 2,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantFailed:        5,
					wantFailedIndexes: ptr.To(""),
					wantTerminating:   ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"succeeded", "perIndex"},
					Value:  0,
				},
				{
					Labels: []string{"failed", "perIndex"},
					Value:  0,
				},
			},
		},
		"job continues execution after a failed index, the job is marked Failed due to the failed index": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](0),
					Template:             podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        1,
					wantFailed:        1,
					wantActiveIndexes: sets.New(1),
					wantFailedIndexes: ptr.To("0"),
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
					wantFailed:           1,
					wantSucceeded:        1,
					wantFailedIndexes:    ptr.To("0"),
					wantCompletedIndexes: "1",
					wantTerminating:      ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"succeeded", "perIndex"},
					Value:  1,
				},
				{
					Labels: []string{"failed", "perIndex"},
					Value:  1,
				},
			},
		},
		"job execution terminated early due to exceeding max failed indexes": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](3),
					Completions:          ptr.To[int32](3),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](0),
					MaxFailedIndexes:     ptr.To[int32](1),
					Template:             podTemplateSpec,
				},
			},
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        2,
					wantFailed:        1,
					wantActiveIndexes: sets.New(1, 2),
					wantFailedIndexes: ptr.To("0"),
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
					wantActive:        0,
					wantFailed:        3,
					wantFailedIndexes: ptr.To("0,1"),
					wantTerminating:   ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"failed", "perIndex"},
					Value:  2,
				},
			},
		},
		"pod failure matching pod failure policy rule with FailIndex action": {
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:          ptr.To[int32](2),
					Completions:          ptr.To[int32](2),
					CompletionMode:       completionModePtr(batchv1.IndexedCompletion),
					BackoffLimitPerIndex: ptr.To[int32](1),
					Template:             podTemplateSpec,
					PodFailurePolicy: &batchv1.PodFailurePolicy{
						Rules: []batchv1.PodFailurePolicyRule{
							{
								Action: batchv1.PodFailurePolicyActionFailIndex,
								OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
									Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
									Values:   []int32{13},
								},
							},
							{
								Action: batchv1.PodFailurePolicyActionFailIndex,
								OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
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
			podTerminations: []podTerminationWithExpectations{
				{
					index: 0,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Terminated: &v1.ContainerStateTerminated{
										ExitCode: 13,
									},
								},
							},
						},
					},
					wantActive:        1,
					wantFailed:        1,
					wantActiveIndexes: sets.New(1),
					wantFailedIndexes: ptr.To("0"),
					wantTerminating:   ptr.To[int32](0),
				},
				{
					index: 1,
					status: v1.PodStatus{
						Phase: v1.PodFailed,
						Conditions: []v1.PodCondition{
							{
								Type:   v1.DisruptionTarget,
								Status: v1.ConditionTrue,
							},
						},
					},
					wantFailed:        2,
					wantFailedIndexes: ptr.To("0,1"),
					wantTerminating:   ptr.To[int32](0),
				},
			},
			wantJobConditionType: batchv1.JobFailed,
			wantJobFinishedIndexesTotalMetric: []metricLabelsWithValue{
				{
					Labels: []string{"failed", "perIndex"},
					Value:  2,
				},
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "backoff-limit-per-index")
	t.Cleanup(closeFn)
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			resetMetrics()

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &test.job)
			if err != nil {
				t.Fatalf("Error %q while creating the job %q", err, jobObj.Name)
			}
			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      int(*test.job.Spec.Parallelism),
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})
			for _, podTermination := range test.podTerminations {
				pod, err := getActivePodForIndex(ctx, clientSet, jobObj, podTermination.index)
				if err != nil {
					t.Fatalf("listing Job Pods: %v", err)
				}
				pod.Status = podTermination.status
				if _, err = clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatalf("Error updating the pod %q: %v", klog.KObj(pod), err)
				}
				validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
					Active:      podTermination.wantActive,
					Succeeded:   podTermination.wantSucceeded,
					Failed:      podTermination.wantFailed,
					Ready:       ptr.To[int32](0),
					Terminating: podTermination.wantTerminating,
				})
				validateIndexedJobPods(ctx, t, clientSet, jobObj, podTermination.wantActiveIndexes, podTermination.wantCompletedIndexes, podTermination.wantFailedIndexes)
				if podTermination.wantReplacementPodFailureCount != nil {
					replacement, err := getActivePodForIndex(ctx, clientSet, jobObj, podTermination.index)
					if err != nil {
						t.Fatalf("Failed to get active replacement pod for index: %v, error: %v", podTermination.index, err)
					}
					gotReplacementPodFailureCount, err := getIndexFailureCount(replacement)
					if err != nil {
						t.Fatalf("Failed read the index failure count annotation for pod: %v, error: %v", klog.KObj(replacement), err)
					}
					if *podTermination.wantReplacementPodFailureCount != gotReplacementPodFailureCount {
						t.Fatalf("Unexpected value of the index failure count annotation. Want: %v, got: %v", *podTermination.wantReplacementPodFailureCount, gotReplacementPodFailureCount)
					}
				}
			}

			remainingActive := test.podTerminations[len(test.podTerminations)-1].wantActive
			if remainingActive > 0 {
				if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, remainingActive); err != nil {
					t.Fatalf("Failed setting phase %q on Job Pod: %v", v1.PodSucceeded, err)
				}
			}
			validateJobCondition(ctx, t, clientSet, jobObj, test.wantJobConditionType)
			for _, wantMetricValue := range test.wantJobFinishedIndexesTotalMetric {
				validateCounterMetric(ctx, t, metrics.JobFinishedIndexesTotal, wantMetricValue)
			}
			validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
		})
	}
}

// TestManagedBy 验证 Job 控制器会根据 Job 的 managedBy 字段以及 JobManagedBy feature gate
// 的启用状态，正确地决定是协调（reconcile）还是跳过协调该 Job。
func TestManagedBy(t *testing.T) {
	const blockDeletionFinalizerForTest string = "fake.example.com/blockDeletion"
	customControllerName := "example.com/custom-job-controller"
	podTemplateSpec := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "main-container",
					Image: "foo",
				},
			},
		},
	}
	testCases := map[string]struct {
		enableJobManagedBy                     bool
		job                                    batchv1.Job
		wantReconciledByBuiltInController      bool
		wantJobByExternalControllerTotalMetric metricLabelsWithValue
	}{
		"the Job controller reconciles jobs without the managedBy": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: podTemplateSpec,
				},
			},
			wantReconciledByBuiltInController: true,
			wantJobByExternalControllerTotalMetric: metricLabelsWithValue{
				// 由于未指定 label 值，这里没有好的选择可以检查。
				// 就使用保留值吧，这样所有测试用例都能验证该指标。
				Labels: []string{batchv1.JobControllerName},
				Value:  0,
			},
		},
		"the Job controller reconciles jobs with the well known value of the managedBy field": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Template:  podTemplateSpec,
					ManagedBy: ptr.To(batchv1.JobControllerName),
				},
			},
			wantReconciledByBuiltInController: true,
			wantJobByExternalControllerTotalMetric: metricLabelsWithValue{
				Labels: []string{batchv1.JobControllerName},
				Value:  0,
			},
		},
		"the Job controller reconciles an unsuspended with the custom value of managedBy; feature disabled": {
			enableJobManagedBy: false,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Template:  podTemplateSpec,
					ManagedBy: ptr.To(customControllerName),
				},
			},
			wantReconciledByBuiltInController: true,
			wantJobByExternalControllerTotalMetric: metricLabelsWithValue{
				Labels: []string{customControllerName},
				Value:  0,
			},
		},
		"the Job controller does not reconcile an unsuspended with the custom value of managedBy": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Suspend:   ptr.To(false),
					Template:  podTemplateSpec,
					ManagedBy: ptr.To(customControllerName),
				},
			},
			wantReconciledByBuiltInController: false,
			wantJobByExternalControllerTotalMetric: metricLabelsWithValue{
				Labels: []string{customControllerName},
				Value:  1,
			},
		},
		"the Job controller does not reconcile a suspended with the custom value of managedBy": {
			enableJobManagedBy: true,
			job: batchv1.Job{
				Spec: batchv1.JobSpec{
					Suspend:   ptr.To(true),
					Template:  podTemplateSpec,
					ManagedBy: ptr.To(customControllerName),
				},
			},
			wantReconciledByBuiltInController: false,
			wantJobByExternalControllerTotalMetric: metricLabelsWithValue{
				Labels: []string{customControllerName},
				Value:  1,
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "managed-by")
	t.Cleanup(closeFn)
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			// TODO: 将在 1.38 中移除。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
			featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, test.enableJobManagedBy)

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)

			// 与其他测试一样，在启动控制器后重置指标以确保状态干净
			resetMetrics()

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &test.job)
			if err != nil {
				t.Fatalf("Error %v while creating the job %q", err, klog.KObj(jobObj))
			}

			t.Cleanup(func() {
				// 等待清理完成，防止该 Job 影响指标
				if err := cleanUp(ctx, clientSet, jobObj, []string{blockDeletionFinalizerForTest, batchv1.JobTrackingFinalizer}); err != nil {
					t.Fatalf("Failed cleanup: %v", err)
				}
			})

			if test.wantReconciledByBuiltInController {
				validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
					Active:      int(*jobObj.Spec.Parallelism),
					Ready:       ptr.To[int32](0),
					Terminating: ptr.To[int32](0),
				})
				validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, test.wantJobByExternalControllerTotalMetric)
			} else {
				validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, test.wantJobByExternalControllerTotalMetric)

				time.Sleep(sleepDurationForControllerLatency)
				jobObj, err = clientSet.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Error %v when getting the latest job %v", err, klog.KObj(jobObj))
				}
				if diff := cmp.Diff(batchv1.JobStatus{}, jobObj.Status); diff != "" {
					t.Fatalf("Unexpected status (-want/+got): %s", diff)
				}
			}
		})
	}
}

// TestManagedBy_Reenabling 验证当 JobManagedBy feature gate 被禁用再重新启用时，
// Job 控制器如何处理 managedBy 字段为自定义值的 Job。首先，feature gate 启用时跳过同步；
// 禁用时开始同步；再次启用 feature gate 时又停止同步。
func TestManagedBy_Reenabling(t *testing.T) {
	customControllerName := "example.com/custom-job-controller"
	// TODO: 将在 1.38 中移除。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.34"))
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, true)

	closeFn, restConfig, clientSet, ns := setup(t, "managed-by-reenabling")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})
	resetMetrics()

	baseJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-job-test",
			Namespace: ns.Name,
		},
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](1),
			Parallelism: ptr.To[int32](1),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "main-container",
							Image: "foo",
						},
					},
				},
			},
			ManagedBy: &customControllerName,
		},
	}
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &baseJob)
	if err != nil {
		t.Fatalf("Error %v when creating the job %q", err, klog.KObj(jobObj))
	}
	jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)

	validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, metricLabelsWithValue{
		Labels: []string{customControllerName},
		Value:  1,
	})

	time.Sleep(sleepDurationForControllerLatency)
	jobObj, err = jobClient.Get(ctx, jobObj.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error %v when getting the latest job %v", err, klog.KObj(jobObj))
	}
	if diff := cmp.Diff(batchv1.JobStatus{}, jobObj.Status); diff != "" {
		t.Fatalf("Unexpected status (-want/+got): %s", diff)
	}

	// 禁用 feature gate 并重启控制器
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, false)
	cancel()
	resetMetrics()
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)

	// 验证内置控制器会协调该 Job
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, metricLabelsWithValue{
		Labels: []string{customControllerName},
		Value:  0,
	})

	// 重新启用 feature gate 并重启控制器
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, true)
	cancel()
	resetMetrics()
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)

	// 将 pod 标记为已完成，但这不会导致 Job 状态被更新。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatalf("Error %v when setting phase %s on the pod of job %v", err, v1.PodSucceeded, klog.KObj(jobObj))
	}

	validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, metricLabelsWithValue{
		Labels: []string{customControllerName},
		Value:  1,
	})

	time.Sleep(sleepDurationForControllerLatency)
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
}

// TestImmediateJobRecreation 验证替代 Job 在重建后会迅速创建 Pod，
// 参见 https://github.com/kubernetes/kubernetes/issues/132042。
func TestImmediateJobRecreation(t *testing.T) {
	// 将退避延迟设置得非常高，确保测试不会因断言长时间等待而通过
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, 2*wait.ForeverTestTimeout))
	closeFn, restConfig, clientSet, ns := setup(t, "recreate-job-immediately")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	baseJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns.Name,
		},
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](1),
			Parallelism: ptr.To[int32](1),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "main-container",
							Image: "foo",
						},
					},
				},
			},
		},
	}
	jobSpec := func(idx int) batchv1.Job {
		spec := baseJob.DeepCopy()
		spec.Name = fmt.Sprintf("test-job-%d", idx)
		return *spec
	}

	var jobObjs []*batchv1.Job
	// 我们创建多个 Job 以提高复现概率。特别是，Job 数量要多于 Job 控制器的 worker 数量，
	// 这样在对应的替代 Job 创建之前，syncJob 极不可能执行（并清理内存状态）。
	for i := range 3 {
		jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, ptr.To(jobSpec(i)))
		if err != nil {
			t.Fatalf("Error %v when creating the job %q", err, klog.KObj(jobObj))
		}
		jobObjs = append(jobObjs, jobObj)
	}

	for _, jobObj := range jobObjs {
		validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
			Active:      1,
			Ready:       ptr.To[int32](0),
			Terminating: ptr.To[int32](0),
		})

		if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
			t.Fatalf("Error %v when setting phase %s on the pod of job %v", err, v1.PodFailed, klog.KObj(jobObj))
		}

		// 等待失败 Pod 被计入统计
		validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
			Failed:      1,
			Ready:       ptr.To[int32](0),
			Terminating: ptr.To[int32](0),
		})
	}

	for i := 0; i < len(jobObjs); i++ {
		jobObj := jobObjs[i]
		jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)
		if err := jobClient.Delete(ctx, jobObj.Name, metav1.DeleteOptions{
			// 使用 propagationPolicy=background，这样无需等待 Job 对象被删除。
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil {
			t.Fatalf("Error %v when deleting the job %v", err, klog.KObj(jobObj))
		}

		// 立即重建 Job
		jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, ptr.To(jobSpec(i)))
		if err != nil {
			t.Fatalf("Error %q while creating the job %q", err, klog.KObj(jobObj))
		}
		jobObjs[i] = jobObj
	}

	// 总超时时间（3*5s）小于 2*ForeverTestTimeout。
	for _, jobObj := range jobObjs {
		// 最多等待 5s 直到 Active=1。该断言验证退避延迟没有被应用到
		// 替代的 Job 实例上。
		validateJobsPodsStatusOnlyWithTimeout(ctx, t, clientSet, jobObj, "", podsByStatus{
			Active:      1,
			Ready:       ptr.To[int32](0),
			Terminating: ptr.To[int32](0),
		}, 5*time.Second)
	}
}

// TestJobRecreationClearsPodExpectations 验证当旧 Job 留下一个未完成的 Pod 创建 expectation 时，
// 替代 Job 仍能被协调。测试会阻塞旧的 Pod POST，替换 Job，等待 informer 观察到替代 Job 的 UID，
// 然后释放 POST 并验证协调是否发生。
func TestJobRecreationClearsPodExpectations(t *testing.T) {
	baseJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job"},
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](1),
			Parallelism: ptr.To[int32](1),
		},
	}
	suspendedJob := baseJob.DeepCopy()
	suspendedJob.Spec.Suspend = new(true)
	unsuspendedJob := baseJob.DeepCopy()
	unsuspendedJob.Spec.Suspend = new(false)
	tests := map[string]struct {
		baseJob            batchv1.Job
		replacementJob     batchv1.Job
		wantConditionTypes []batchv1.JobConditionType
		wantPodsStatus     podsByStatus
	}{
		"suspended replacement": {
			baseJob:            *baseJob.DeepCopy(),
			replacementJob:     *suspendedJob,
			wantConditionTypes: []batchv1.JobConditionType{batchv1.JobSuspended},
			wantPodsStatus: podsByStatus{
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			},
		},
		"unsuspended replacement": {
			baseJob:        *baseJob.DeepCopy(),
			replacementJob: *unsuspendedJob,
			wantPodsStatus: podsByStatus{
				Active:      1,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			testJobRecreationClearsPodExpectations(t, tc.baseJob, tc.replacementJob, tc.wantConditionTypes, tc.wantPodsStatus)
		})
	}
}

func testJobRecreationClearsPodExpectations(t *testing.T, baseJob, replacementJob batchv1.Job, wantConditionTypes []batchv1.JobConditionType, wantPodsStatus podsByStatus) {
	closeFn, restConfig, clientSet, ns := setup(t, "recreate-job-expectations")
	t.Cleanup(closeFn)

	// 阶段 1：在旧 Job 的 expectation 被记录后，阻塞其 Pod POST。
	podCreateStarted := make(chan struct{})
	allowPodCreate := make(chan struct{})
	var blockOnce sync.Once
	restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/pods") {
				blockOnce.Do(func() {
					close(podCreateStarted)
					<-allowPodCreate
				})
			}
			return rt.RoundTrip(req)
		})
	}
	t.Cleanup(func() {
		select {
		case <-allowPodCreate:
		default:
			close(allowPodCreate)
		}
	})

	informerSet := informers.NewSharedInformerFactoryWithOptions(clientset.NewForConfigOrDie(restclient.AddUserAgent(restConfig, "job-informers")), 0)
	jc, ctx, cancel := createJobControllerWithSharedInformers(t, restConfig, informerSet)
	t.Cleanup(cancel)
	informerSet.Start(ctx.Done())
	go jc.Run(ctx, 1)
	informerSet.WaitForCacheSync(ctx.Done())

	baseJob.Namespace = ns.Name
	oldJob, err := createJobWithDefaults(ctx, clientSet, ns.Name, &baseJob)
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	select {
	case <-podCreateStarted:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Timed out waiting for the old Job's Pod creation to start")
	}

	// 阶段 2：替换 Job，并等待 informer 观察到它的新 UID。
	jobClient := clientSet.BatchV1().Jobs(ns.Name)
	if err := jobClient.Delete(ctx, oldJob.Name, metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationBackground)}); err != nil {
		t.Fatalf("Failed to delete old Job: %v", err)
	}
	replacementJob.Namespace = ns.Name
	newJob, err := createJobWithDefaults(ctx, clientSet, ns.Name, &replacementJob)
	if err != nil {
		t.Fatalf("Failed to create replacement Job: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		cachedJob, err := informerSet.Batch().V1().Jobs().Lister().Jobs(ns.Name).Get(newJob.Name)
		return err == nil && cachedJob.UID == newJob.UID, nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for the informer to observe the replacement Job: %v", err)
	}
	close(allowPodCreate)

	// 阶段 3：验证旧的 POST 完成后，替代 Job 能被协调。
	for _, conditionType := range wantConditionTypes {
		validateJobCondition(ctx, t, clientSet, newJob, conditionType)
	}
	validateJobsPodsStatusOnlyWithTimeout(ctx, t, clientSet, newJob, "after recreation", wantPodsStatus, 5*time.Second)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestManagedBy_RecreatedJob 验证当 Job 是重建的、且前一个 Job 仍有待处理的同步排队时，
// Job 控制器会跳过对带 managedBy 字段的 Job 的协调。
// 此场景中，我们先创建一个不带 managedBy 字段的 Job，并将其 pod 标记为成功。
// 这会将 Job 对象的同步以 1s 延迟排队。随后，不等 Job 状态更新，我们就删除同名 Job
// 并以带 managedBy 字段的配置重建它。排队的更新开始在新 Job 上执行，但会被跳过。
func TestManagedBy_RecreatedJob(t *testing.T) {
	customControllerName := "example.com/custom-job-controller"
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, true)

	closeFn, restConfig, clientSet, ns := setup(t, "managed-by-recreate-job")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)
	resetMetrics()

	baseJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-job-test",
			Namespace: ns.Name,
		},
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](1),
			Parallelism: ptr.To[int32](1),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "main-container",
							Image: "foo",
						},
					},
				},
			},
		},
	}
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &baseJob)
	if err != nil {
		t.Fatalf("Error %v when creating the job %q", err, klog.KObj(jobObj))
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 将 pod 标记为完成会将 Job 的协调操作排队
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatalf("Error %v when setting phase %s on the pod of job %v", err, v1.PodSucceeded, klog.KObj(jobObj))
	}

	jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)
	if err = jobClient.Delete(ctx, jobObj.Name, metav1.DeleteOptions{
		// Use propagationPolicy=background so that we don't need to wait for the job object to be gone.
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil {
		t.Fatalf("Error %v when deleting the job %v", err, klog.KObj(jobObj))
	}

	jobWithManagedBy := baseJob.DeepCopy()
	jobWithManagedBy.Spec.ManagedBy = ptr.To(customControllerName)
	jobObj, err = createJobWithDefaults(ctx, clientSet, ns.Name, jobWithManagedBy)
	if err != nil {
		t.Fatalf("Error %q while creating the job %q", err, klog.KObj(jobObj))
	}

	validateCounterMetric(ctx, t, metrics.JobByExternalControllerTotal, metricLabelsWithValue{
		Labels: []string{customControllerName},
		Value:  1,
	})

	time.Sleep(sleepDurationForControllerLatency)
	jobObj, err = jobClient.Get(ctx, jobObj.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error %v when getting the latest job %v", err, klog.KObj(jobObj))
	}
	if diff := cmp.Diff(batchv1.JobStatus{}, jobObj.Status); diff != "" {
		t.Fatalf("Unexpected status (-want/+got): %s", diff)
	}
}

// TestManagedBy_UsingReservedJobFinalizers 记录以下情况下 Job 控制器的行为：
// 一个 managedBy 字段为自定义值的 Job 创建了带有 batch.kubernetes.io/job-tracking finalizer 的 pod。
// 内置控制器不应移除该 finalizer。注意，虽然不鼓励在由外部控制器管理的 Job 中使用该 finalizer，
// 但如果有人 fork 了控制器却没有重命名 finalizer，就可能出现这种情况。
func TestManagedBy_UsingReservedJobFinalizers(t *testing.T) {
	customControllerName := "example.com/custom-job-controller"
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, true)

	closeFn, restConfig, clientSet, ns := setup(t, "managed-by-reserved-finalizers")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)
	resetMetrics()

	jobSpec := batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "batch/v1",
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-job-test",
			Namespace: ns.Name,
		},
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](1),
			Parallelism: ptr.To[int32](1),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "main-container",
							Image: "foo",
						},
					},
				},
			},
			ManagedBy: ptr.To(customControllerName),
		},
	}
	// 创建一个 managedBy 为自定义值的 Job
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &jobSpec)
	if err != nil {
		t.Fatalf("Error %v when creating the job %q", err, klog.KObj(jobObj))
	}

	podControl := controller.RealPodControl{
		KubeClient: clientSet,
		Recorder:   &record.FakeRecorder{},
	}

	// 手动创建 pod，模拟 managedBy 字段所指的外部控制器的行为。
	// 创建 pod 时带上内置 finalizer。
	podTemplate := jobObj.Spec.Template.DeepCopy()
	podTemplate.Finalizers = append(podTemplate.Finalizers, batchv1.JobTrackingFinalizer)
	err = podControl.CreatePodsWithGenerateName(ctx, jobObj.Namespace, podTemplate, jobObj, metav1.NewControllerRef(jobObj, batchv1.SchemeGroupVersion.WithKind("Job")), "pod1")
	if err != nil {
		t.Fatalf("Error %v when creating a pod for job %q", err, klog.KObj(jobObj))
	}

	// 获取 Job 的 pod 列表，以取得对已创建 pod 的引用。
	jobPods, err := getJobPods(ctx, t, clientSet, jobObj, func(ps v1.PodStatus) bool { return true })
	if err != nil {
		t.Fatalf("Error %v getting the list of pods for job %q", err, klog.KObj(jobObj))
	}
	if len(jobPods) != 1 {
		t.Fatalf("Unexpected number (%d) of pods for job: %v", len(jobPods), klog.KObj(jobObj))
	}

	// 在将父 Job 标记为完成之前，先把 pod 标记为已完成（成功）。
	podObj := jobPods[0]
	podObj.Status.Phase = v1.PodSucceeded
	podObj, err = clientSet.CoreV1().Pods(ns.Name).UpdateStatus(ctx, podObj, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error %v when marking the %q pod as succeeded", err, klog.KObj(podObj))
	}

	// 触发 Job 终止，使内置控制器收到 UpdateJob 事件；若不是因为 managedBy 为自定义值，
	// 控制器在收到该事件时会移除 pod 的 finalizer。
	jobObj.Status.Conditions = append(jobObj.Status.Conditions, batchv1.JobCondition{
		Type:   batchv1.JobSuccessCriteriaMet,
		Status: v1.ConditionTrue,
	})
	jobObj.Status.StartTime = ptr.To(metav1.Now())
	if jobObj, err = clientSet.BatchV1().Jobs(jobObj.Namespace).UpdateStatus(ctx, jobObj, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Error %v when updating the job as finished %v", err, klog.KObj(jobObj))
	}

	podObj, err = clientSet.CoreV1().Pods(ns.Name).Get(ctx, podObj.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error %v when getting the latest version of the pod %v", err, klog.KObj(podObj))
	}

	// 更新 pod，使内置控制器收到 UpdatePod 事件；若不是因为 Job 上 managedBy 为自定义值，
	// 控制器在收到该事件时会移除 pod 的 finalizer。
	podObj.Status.Conditions = append(podObj.Status.Conditions, v1.PodCondition{
		Type:   v1.PodConditionType("CustomCondition"),
		Status: v1.ConditionTrue,
	})
	podObj, err = clientSet.CoreV1().Pods(ns.Name).UpdateStatus(ctx, podObj, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error %v when adding a condition to the pod status %v", err, klog.KObj(podObj))
	}

	time.Sleep(sleepDurationForControllerLatency)
	podObj, err = clientSet.CoreV1().Pods(ns.Name).Get(ctx, podObj.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error %v when getting the latest version of the pod %v", err, klog.KObj(podObj))
	}

	if diff := cmp.Diff([]string{batchv1.JobTrackingFinalizer}, podObj.Finalizers); diff != "" {
		t.Fatalf("Unexpected change in the set of finalizers for pod %q, because the owner job %q has custom managedBy, diff=%s", klog.KObj(podObj), klog.KObj(jobObj), diff)
	}
}

func getIndexFailureCount(p *v1.Pod) (int, error) {
	if p.Annotations == nil {
		return 0, errors.New("no annotations found")
	}
	v, ok := p.Annotations[batchv1.JobIndexFailureCountAnnotation]
	if !ok {
		return 0, fmt.Errorf("annotation %s not found", batchv1.JobIndexFailureCountAnnotation)
	}
	return strconv.Atoi(v)
}

func completionModePtr(cm batchv1.CompletionMode) *batchv1.CompletionMode {
	return &cm
}

// TestNonParallelJob 测试只执行一个 Pod 的 Job。
// 测试会在某些时刻重建 Job 控制器，以确保新控制器能够接手。
func TestNonParallelJob(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})
	resetMetrics()

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 重启控制器。
	cancel()
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)

	// 失败的 Pod 会被替换。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Failed:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "failed"},
		Value:  1,
	})

	// 重启控制器。
	cancel()
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)

	// Pod 成功后不再创建新的 Pod。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodSucceeded, err)
	}
	validateJobComplete(ctx, t, clientSet, jobObj)
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Failed:      1,
		Succeeded:   1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
	validateCounterMetric(ctx, t, metrics.JobFinishedNum, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
		Value:  1,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded"},
		Value:  1,
	})
}

func TestParallelJob(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	closeFn, restConfig, clientSet, ns := setup(t, "parallel")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)
	resetMetrics()

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](5),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	want := podsByStatus{
		Active:      5,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)

	// 如果启用了，则跟踪 ready 的 pod。
	if _, err := setJobPodsReady(ctx, clientSet, jobObj, 2); err != nil {
		t.Fatalf("Failed Marking Pods as ready: %v", err)
	}
	want.Ready = ptr.To[int32](2)
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)

	// 失败的 Pod 会被替换。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 2); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodFailed, err)
	}
	want = podsByStatus{
		Active:      5,
		Failed:      2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	// 一旦有一个 Pod 成功，即使其他 Pod 失败，也不再创建新的 Pod。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodSucceeded, err)
	}
	want = podsByStatus{
		Failed:      2,
		Succeeded:   1,
		Active:      4,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 2); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodFailed, err)
	}
	want = podsByStatus{
		Failed:      4,
		Succeeded:   1,
		Active:      2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	// 剩余 Pod 成功后不再创建新的 Pod。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 2); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodSucceeded, err)
	}
	validateJobComplete(ctx, t, clientSet, jobObj)
	want = podsByStatus{
		Failed:      4,
		Succeeded:   3,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
	validateTerminatedPodsTrackingFinalizerMetric(ctx, t, 7)
	validateCounterMetric(ctx, t, metrics.JobFinishedNum, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
		Value:  1,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded"},
		Value:  3,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "failed"},
		Value:  4,
	})
}

func TestParallelJobChangingParallelism(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "parallel")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](2),
			Parallelism:  ptr.To[int32](5),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      5,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 将 parallelism 减少一个大于 backoffLimit 的数值。
	patch := []byte(`{"spec":{"parallelism":2}}`)
	jobObj, err = clientSet.BatchV1().Jobs(ns.Name).Patch(ctx, jobObj.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("Updating Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      2,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 再次增加 parallelism。
	patch = []byte(`{"spec":{"parallelism":4}}`)
	jobObj, err = clientSet.BatchV1().Jobs(ns.Name).Patch(ctx, jobObj.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("Updating Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      4,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 让 Job 成功
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 4); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodSucceeded, err)
	}
	validateJobComplete(ctx, t, clientSet, jobObj)
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Succeeded:   4,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
}

func TestParallelJobWithCompletions(t *testing.T) {
	// 降低 Job 单次同步的限制，以便用较少的 pod 测试部分更新。
	t.Cleanup(setDuringTest(&jobcontroller.MaxUncountedPods, 10))
	t.Cleanup(setDuringTest(&jobcontroller.MaxPodCreateDeletePerSync, 10))
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	closeFn, restConfig, clientSet, ns := setup(t, "completions")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)
	resetMetrics()

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](54),
			Completions: ptr.To[int32](56),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	want := podsByStatus{
		Active:      54,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	// 如果启用了，则跟踪 ready 的 pod。
	if _, err := setJobPodsReady(ctx, clientSet, jobObj, 52); err != nil {
		t.Fatalf("Failed Marking Pods as ready: %v", err)
	}
	want.Ready = ptr.To[int32](52)
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)

	// 失败的 Pod 会被替换。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 2); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodFailed, err)
	}
	want = podsByStatus{
		Active:      54,
		Failed:      2,
		Ready:       ptr.To[int32](50),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	// 持续创建 Pod，直到成功的 Pod 数量达到 completions。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 53); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodSucceeded, err)
	}
	want = podsByStatus{
		Failed:      2,
		Succeeded:   53,
		Active:      3,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	// Job 完成后不再创建新的 Pod。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 3); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pods: %v", v1.PodSucceeded, err)
	}
	validateJobComplete(ctx, t, clientSet, jobObj)
	want = podsByStatus{
		Failed:      2,
		Succeeded:   56,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", want)
	validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
	validateCounterMetric(ctx, t, metrics.JobFinishedNum, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded", "CompletionsReached"},
		Value:  1,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "succeeded"},
		Value:  56,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"NonIndexed", "failed"},
		Value:  2,
	})
}

func TestIndexedJob(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	closeFn, restConfig, clientSet, ns := setup(t, "indexed")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)
	resetMetrics()

	mode := batchv1.IndexedCompletion
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism:    ptr.To[int32](3),
			Completions:    ptr.To[int32](4),
			CompletionMode: &mode,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      3,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 1, 2), "", nil)
	validateCounterMetric(ctx, t, metrics.JobFinishedIndexesTotal, metricLabelsWithValue{
		Labels: []string{"succeeded", "global"},
		Value:  0,
	})

	// 一个 Pod 成功。
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, 1); err != nil {
		t.Fatal("Failed trying to succeed pod with index 1")
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      3,
		Succeeded:   1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 2, 3), "1", nil)
	validateCounterMetric(ctx, t, metrics.JobFinishedIndexesTotal, metricLabelsWithValue{
		Labels: []string{"succeeded", "global"},
		Value:  1,
	})

	// 一个 Pod 失败，它应该被重建。
	if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, 2); err != nil {
		t.Fatal("Failed trying to succeed pod with index 2")
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      3,
		Failed:      1,
		Succeeded:   1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, sets.New(0, 2, 3), "1", nil)
	validateCounterMetric(ctx, t, metrics.JobFinishedIndexesTotal, metricLabelsWithValue{
		Labels: []string{"succeeded", "global"},
		Value:  1,
	})

	// 剩余的 Pod 成功。
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, 3); err != nil {
		t.Fatal("Failed trying to succeed remaining pods")
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      0,
		Failed:      1,
		Succeeded:   4,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	validateIndexedJobPods(ctx, t, clientSet, jobObj, nil, "0-3", nil)
	validateJobComplete(ctx, t, clientSet, jobObj)
	validateFinishedPodsNoFinalizer(ctx, t, clientSet, jobObj)
	validateTerminatedPodsTrackingFinalizerMetric(ctx, t, 5)
	validateCounterMetric(ctx, t, metrics.JobFinishedIndexesTotal, metricLabelsWithValue{
		Labels: []string{"succeeded", "global"},
		Value:  4,
	})
	validateCounterMetric(ctx, t, metrics.JobFinishedNum, metricLabelsWithValue{
		Labels: []string{"Indexed", "succeeded", "CompletionsReached"},
		Value:  1,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"Indexed", "succeeded"},
		Value:  4,
	})
	validateCounterMetric(ctx, t, metrics.JobPodsFinished, metricLabelsWithValue{
		Labels: []string{"Indexed", "failed"},
		Value:  1,
	})
}

func TestJobPodReplacementPolicy(t *testing.T) {
	const blockDeletionFinalizerForTest string = "fake.example.com/blockDeletion"
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	indexedCompletion := batchv1.IndexedCompletion
	nonIndexedCompletion := batchv1.NonIndexedCompletion
	var podReplacementPolicy = func(obj batchv1.PodReplacementPolicy) *batchv1.PodReplacementPolicy {
		return &obj
	}
	type jobStatus struct {
		active      int
		failed      int
		terminating *int32
	}
	type jobPodsCreationMetrics struct {
		new                         int
		recreateTerminatingOrFailed int
		recreateFailed              int
	}
	cases := map[string]struct {
		jobSpec                 *batchv1.JobSpec
		wantStatusAfterDeletion jobStatus
		wantStatusAfterFailure  jobStatus
		wantMetrics             jobPodsCreationMetrics
	}{
		"delete & fail pods, recreate terminating pods, and verify job status counters": {
			jobSpec: &batchv1.JobSpec{
				Parallelism:    ptr.To[int32](2),
				Completions:    ptr.To[int32](2),
				CompletionMode: &indexedCompletion,
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{blockDeletionFinalizerForTest},
					},
				},
			},
			wantStatusAfterDeletion: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](2),
			},
			wantStatusAfterFailure: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](0),
			},
			wantMetrics: jobPodsCreationMetrics{
				new:                         2,
				recreateTerminatingOrFailed: 2,
			},
		},
		"IndexedJob, TerminatingOrFailed policy, delete & fail pods, recreate terminating pods, and verify job status counters": {
			jobSpec: &batchv1.JobSpec{
				Parallelism:          ptr.To[int32](2),
				Completions:          ptr.To[int32](2),
				CompletionMode:       &indexedCompletion,
				PodReplacementPolicy: podReplacementPolicy(batchv1.TerminatingOrFailed),
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{blockDeletionFinalizerForTest},
					},
				},
			},
			wantStatusAfterDeletion: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](2),
			},
			wantStatusAfterFailure: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](0),
			},
			wantMetrics: jobPodsCreationMetrics{
				new:                         2,
				recreateTerminatingOrFailed: 2,
			},
		},
		"NonIndexedJob, TerminatingOrFailed policy, delete & fail pods, recreate terminating pods, and verify job status counters": {
			jobSpec: &batchv1.JobSpec{
				Parallelism:          ptr.To[int32](2),
				Completions:          ptr.To[int32](2),
				CompletionMode:       &nonIndexedCompletion,
				PodReplacementPolicy: podReplacementPolicy(batchv1.TerminatingOrFailed),
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{blockDeletionFinalizerForTest},
					},
				},
			},
			wantStatusAfterDeletion: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](2),
			},
			wantStatusAfterFailure: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](0),
			},
			wantMetrics: jobPodsCreationMetrics{
				new:                         2,
				recreateTerminatingOrFailed: 2,
			},
		},
		"Failed policy, delete & fail pods, recreate failed pods, and verify job status counters": {
			jobSpec: &batchv1.JobSpec{
				Parallelism:          ptr.To[int32](2),
				Completions:          ptr.To[int32](2),
				CompletionMode:       &indexedCompletion,
				PodReplacementPolicy: podReplacementPolicy(batchv1.Failed),
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{blockDeletionFinalizerForTest},
					},
				},
			},
			wantStatusAfterDeletion: jobStatus{
				active:      0,
				failed:      0,
				terminating: ptr.To[int32](2),
			},
			wantStatusAfterFailure: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](0),
			},
			wantMetrics: jobPodsCreationMetrics{
				new:            2,
				recreateFailed: 2,
			},
		},
		"NonIndexedJob, Failed policy, delete & fail pods, recreate failed pods, and verify job status counters": {
			jobSpec: &batchv1.JobSpec{
				Parallelism:          ptr.To[int32](2),
				Completions:          ptr.To[int32](2),
				CompletionMode:       &nonIndexedCompletion,
				PodReplacementPolicy: podReplacementPolicy(batchv1.Failed),
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{blockDeletionFinalizerForTest},
					},
				},
			},
			wantStatusAfterDeletion: jobStatus{
				active:      0,
				failed:      0,
				terminating: ptr.To[int32](2),
			},
			wantStatusAfterFailure: jobStatus{
				active:      2,
				failed:      2,
				terminating: ptr.To[int32](0),
			},
			wantMetrics: jobPodsCreationMetrics{
				new:            2,
				recreateFailed: 2,
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "pod-replacement-policy")
	t.Cleanup(closeFn)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)
			resetMetrics()

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
				Spec: *tc.jobSpec,
			})
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}
			jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)

			waitForPodsToBeActive(ctx, t, jobClient, 2, jobObj)
			t.Cleanup(func() {
				if err := cleanUp(ctx, clientSet, jobObj, []string{blockDeletionFinalizerForTest, batchv1.JobTrackingFinalizer}); err != nil {
					t.Fatalf("Failed cleanup: %v", err)
				}
			})

			deletePods(ctx, t, clientSet, ns.Name)

			validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "AfterDeletion", podsByStatus{
				Terminating: tc.wantStatusAfterDeletion.terminating,
				Failed:      tc.wantStatusAfterDeletion.failed,
				Active:      tc.wantStatusAfterDeletion.active,
				Ready:       ptr.To[int32](0),
			})

			failTerminatingPods(ctx, t, clientSet, ns.Name)
			validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "AfterFailure", podsByStatus{
				Terminating: tc.wantStatusAfterFailure.terminating,
				Failed:      tc.wantStatusAfterFailure.failed,
				Active:      tc.wantStatusAfterFailure.active,
				Ready:       ptr.To[int32](0),
			})

			validateCounterMetric(
				ctx,
				t,
				metrics.JobPodsCreationTotal,
				metricLabelsWithValue{Labels: []string{"new", "succeeded"}, Value: tc.wantMetrics.new},
			)
			validateCounterMetric(
				ctx,
				t,
				metrics.JobPodsCreationTotal,
				metricLabelsWithValue{Labels: []string{"recreate_terminating_or_failed", "succeeded"}, Value: tc.wantMetrics.recreateTerminatingOrFailed},
			)
			validateCounterMetric(
				ctx,
				t,
				metrics.JobPodsCreationTotal,
				metricLabelsWithValue{Labels: []string{"recreate_failed", "succeeded"}, Value: tc.wantMetrics.recreateFailed},
			)
		})
	}
}

func TestElasticIndexedJob(t *testing.T) {
	const initialCompletions int32 = 3
	type jobUpdate struct {
		completions          *int32
		succeedIndexes       []int
		failIndexes          []int
		wantSucceededIndexes string
		wantFailed           int
		wantRemainingIndexes sets.Set[int]
		wantActivePods       int
		wantTerminating      *int32
	}
	cases := map[string]struct {
		jobUpdates []jobUpdate
		wantErr    *apierrors.StatusError
	}{
		"scale up": {
			jobUpdates: []jobUpdate{
				{
					// 将 completions 从 3 扩容到 4，然后让索引 0-3 成功
					completions:          ptr.To[int32](4),
					succeedIndexes:       []int{0, 1, 2, 3},
					wantSucceededIndexes: "0-3",
					wantTerminating:      ptr.To[int32](0),
				},
			},
		},
		"scale down": {
			jobUpdates: []jobUpdate{
				// 在 completions 仍为原值 (3) 时，先让索引 1 成功、索引 2 失败。
				{
					succeedIndexes:       []int{1},
					failIndexes:          []int{2},
					wantSucceededIndexes: "1",
					wantFailed:           1,
					wantRemainingIndexes: sets.New(0, 2),
					wantActivePods:       2,
					wantTerminating:      ptr.To[int32](0),
				},
				// 将 completions 从 3 缩容到 1，验证之前超出范围的失败仍会被计数，
				// 但超出范围的成功不会被计数。
				{
					completions:          ptr.To[int32](1),
					succeedIndexes:       []int{0},
					wantSucceededIndexes: "0",
					wantFailed:           1,
					wantTerminating:      ptr.To[int32](0),
				},
			},
		},
		"index finishes successfully, scale down, scale up": {
			jobUpdates: []jobUpdate{
				// 在 completions 仍为原值 (3) 时，先让索引 2 成功。
				{
					succeedIndexes:       []int{2},
					wantSucceededIndexes: "2",
					wantRemainingIndexes: sets.New(0, 1),
					wantActivePods:       2,
					wantTerminating:      ptr.To[int32](0),
				},
				// 将 completions 从 3 缩容到 2，以排除之前成功的索引。
				{
					completions:          ptr.To[int32](2),
					wantRemainingIndexes: sets.New(0, 1),
					wantActivePods:       2,
					wantTerminating:      ptr.To[int32](0),
				},
				// 将 completions 扩容回去，以包含之前暂时超出范围的成功索引。
				{
					completions:          ptr.To[int32](3),
					succeedIndexes:       []int{0, 1, 2},
					wantSucceededIndexes: "0-2",
					wantTerminating:      ptr.To[int32](0),
				},
			},
		},
		"scale down to 0, verify that the job succeeds": {
			jobUpdates: []jobUpdate{
				{
					completions:     ptr.To[int32](0),
					wantTerminating: ptr.To[int32](0),
				},
			},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "indexed")
	t.Cleanup(closeFn)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)
			resetMetrics()

			// 以 Indexed completion 模式创建初始 Job。
			mode := batchv1.IndexedCompletion
			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism:    ptr.To(initialCompletions),
					Completions:    ptr.To(initialCompletions),
					CompletionMode: &mode,
				},
			})
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}
			jobClient := clientSet.BatchV1().Jobs(jobObj.Namespace)

			// 等待 pod 启动。
			err = wait.PollUntilContextTimeout(ctx, 5*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
				job, err := jobClient.Get(ctx, jobObj.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if job.Status.Active == initialCompletions {
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Fatalf("Error waiting for Job pods to become active: %v", err)
			}

			for _, update := range tc.jobUpdates {
				// 如有必要，更新 Job spec。
				if update.completions != nil {
					if jobObj, err = updateJob(ctx, jobClient, jobObj.Name, func(j *batchv1.Job) {
						j.Spec.Completions = update.completions
						j.Spec.Parallelism = update.completions
					}); err != nil {
						if diff := cmp.Diff(tc.wantErr, err); diff != "" {
							t.Fatalf("Unexpected or missing errors (-want/+got): %s", diff)
						}
						return
					}
				}

				// 让指定的索引成功。
				for _, idx := range update.succeedIndexes {
					if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodSucceeded, idx); err != nil {
						t.Fatalf("Failed trying to succeed pod with index %d: %v", idx, err)
					}
				}

				// 让指定的索引失败。
				for _, idx := range update.failIndexes {
					if err := setJobPhaseForIndex(ctx, clientSet, jobObj, v1.PodFailed, idx); err != nil {
						t.Fatalf("Failed trying to fail pod with index %d: %v", idx, err)
					}
				}

				validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
					Active:      update.wantActivePods,
					Succeeded:   len(update.succeedIndexes),
					Failed:      update.wantFailed,
					Ready:       ptr.To[int32](0),
					Terminating: update.wantTerminating,
				})
				validateIndexedJobPods(ctx, t, clientSet, jobObj, update.wantRemainingIndexes, update.wantSucceededIndexes, nil)
			}

			validateJobComplete(ctx, t, clientSet, jobObj)
		})
	}
}

// BenchmarkLargeIndexedJob 对 Indexed Job 的完成过程进行基准测试。
// 我们认为大型 Job 更常以 Indexed 方式使用，而且它们跟踪起来也更快，
// 因为所需的 API 调用更少。
func BenchmarkLargeIndexedJob(b *testing.B) {
	closeFn, restConfig, clientSet, ns := setup(b, "indexed")
	// 在基准测试期间，我们限制客户端的 QPS 和 Burst，以确保 Job 控制器
	// 在受限的 QPS 和 Burst 条件下仍有足够好的性能。
	restConfig.QPS = 100
	restConfig.Burst = 100
	b.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(b, restConfig)
	b.Cleanup(cancel)
	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   1.5,
		Steps:    30,
		Cap:      5 * time.Minute,
	}
	cases := map[string]struct {
		nPods                int32
		backoffLimitPerIndex *int32
	}{
		"regular indexed job without failures; size=10": {
			nPods: 10,
		},
		"job with backoffLimitPerIndex without failures; size=10": {
			nPods:                10,
			backoffLimitPerIndex: ptr.To[int32](1),
		},
		"regular indexed job without failures; size=100": {
			nPods: 100,
		},
		"job with backoffLimitPerIndex without failures; size=100": {
			nPods:                100,
			backoffLimitPerIndex: ptr.To[int32](1),
		},
	}
	mode := batchv1.IndexedCompletion
	for name, tc := range cases {
		b.Run(name, func(b *testing.B) {
			hasJobBackoffLimitPerIndex := tc.backoffLimitPerIndex != nil
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StartTimer()
				jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("npods-%d-%d-%v", tc.nPods, n, hasJobBackoffLimitPerIndex),
					},
					Spec: batchv1.JobSpec{
						Parallelism:          ptr.To(tc.nPods),
						Completions:          ptr.To(tc.nPods),
						CompletionMode:       &mode,
						BackoffLimitPerIndex: tc.backoffLimitPerIndex,
					},
				})
				if err != nil {
					b.Fatalf("Failed to create Job: %v", err)
				}
				b.Cleanup(func() {
					if err := cleanUp(ctx, clientSet, jobObj, []string{batchv1.JobTrackingFinalizer}); err != nil {
						b.Fatalf("Failed cleanup: %v", err)
					}
				})
				remaining := int(tc.nPods)
				if err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
					if succ, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, remaining); err != nil {
						remaining -= succ
						b.Logf("Transient failure succeeding pods: %v", err)
						return false, nil
					}
					return true, nil
				}); err != nil {
					b.Fatalf("Could not succeed the remaining %d pods: %v", remaining, err)
				}
				validateJobComplete(ctx, b, clientSet, jobObj)
				b.StopTimer()
			}
		})
	}
}

// BenchmarkLargeFailureHandling 对 Indexed Job 处理大量 pod 失败的情况进行基准测试。
// 我们设置了最小的退避延迟，使 Job 控制器在处理使用全局 backoffLimit 的 Indexed Job
// 与使用 per-index backoffLimit 的 Indexed Job 时，尽管失败处理模式不同，性能仍具有可比性。
func BenchmarkLargeFailureHandling(b *testing.B) {
	b.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	b.Cleanup(setDurationDuringTest(&jobcontroller.MaxJobPodFailureBackOff, fastPodFailureBackoff))
	closeFn, restConfig, clientSet, ns := setup(b, "indexed")
	// 在基准测试期间，我们限制客户端的 QPS 和 Burst，以确保 Job 控制器
	// 在受限的 QPS 和 Burst 条件下仍有足够好的性能。
	restConfig.QPS = 100
	restConfig.Burst = 100
	b.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(b, restConfig)
	b.Cleanup(cancel)
	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   1.5,
		Steps:    30,
		Cap:      5 * time.Minute,
	}
	cases := map[string]struct {
		nPods                int32
		backoffLimitPerIndex *int32
		customTimeout        *time.Duration
	}{
		"regular indexed job with failures; size=10": {
			nPods: 10,
		},
		"job with backoffLimitPerIndex with failures; size=10": {
			nPods:                10,
			backoffLimitPerIndex: ptr.To[int32](1),
		},
		"regular indexed job with failures; size=100": {
			nPods: 100,
		},
		"job with backoffLimitPerIndex with failures; size=100": {
			nPods:                100,
			backoffLimitPerIndex: ptr.To[int32](1),
		},
	}
	mode := batchv1.IndexedCompletion
	for name, tc := range cases {
		b.Run(name, func(b *testing.B) {
			enableJobBackoffLimitPerIndex := tc.backoffLimitPerIndex != nil
			timeout := ptr.Deref(tc.customTimeout, wait.ForeverTestTimeout)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("npods-%d-%d-%v", tc.nPods, n, enableJobBackoffLimitPerIndex),
					},
					Spec: batchv1.JobSpec{
						Parallelism:          ptr.To(tc.nPods),
						Completions:          ptr.To(tc.nPods),
						CompletionMode:       &mode,
						BackoffLimitPerIndex: tc.backoffLimitPerIndex,
						BackoffLimit:         ptr.To(tc.nPods),
					},
				})
				if err != nil {
					b.Fatalf("Failed to create Job: %v", err)
				}
				b.Cleanup(func() {
					if err := cleanUp(ctx, clientSet, jobObj, []string{batchv1.JobTrackingFinalizer}); err != nil {
						b.Fatalf("Failed cleanup: %v", err)
					}
				})
				validateJobsPodsStatusOnlyWithTimeout(ctx, b, clientSet, jobObj, "", podsByStatus{
					Active:      int(tc.nPods),
					Ready:       ptr.To[int32](0),
					Terminating: ptr.To[int32](0),
				}, timeout)

				b.StartTimer()
				remaining := int(tc.nPods)
				if err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
					if fail, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, remaining); err != nil {
						remaining -= fail
						b.Logf("Transient failure failing pods: %v", err)
						return false, nil
					}
					return true, nil
				}); err != nil {
					b.Fatalf("Could not succeed the remaining %d pods: %v", remaining, err)
				}
				validateJobsPodsStatusOnlyWithTimeout(ctx, b, clientSet, jobObj, "", podsByStatus{
					Active:      int(tc.nPods),
					Ready:       ptr.To[int32](0),
					Failed:      int(tc.nPods),
					Terminating: ptr.To[int32](0),
				}, timeout)
				b.StopTimer()
			}
		})
	}
}

// cleanUp 移除指定的 pod finalizer，然后删除所有 pod 和 Job。
func cleanUp(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, podFinalizersToRemove []string) error {
	if err := removePodsFinalizers(ctx, clientSet, jobObj.Namespace, podFinalizersToRemove); err != nil {
		return err
	}
	// 分页清理 pod，因为 DeleteCollection 可能会超时。
	// #90743
	for {
		pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
		if err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			break
		}
		err = clientSet.CoreV1().Pods(jobObj.Namespace).DeleteCollection(ctx,
			metav1.DeleteOptions{},
			metav1.ListOptions{
				Limit: 1000,
			})
		if err != nil {
			return err
		}
	}

	// 将传播策略设为 background，确保 Job 在删除时不会收到任何 finalizer
	return clientSet.BatchV1().Jobs(jobObj.Namespace).Delete(ctx, jobObj.Name, metav1.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	})
}

func TestOrphanPodsFinalizersClearedWithGC(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "orphan-pod-finalizers")
	t.Cleanup(closeFn)
	for _, policy := range []metav1.DeletionPropagation{metav1.DeletePropagationOrphan, metav1.DeletePropagationBackground, metav1.DeletePropagationForeground} {
		t.Run(string(policy), func(t *testing.T) {
			informerSet := informers.NewSharedInformerFactory(clientset.NewForConfigOrDie(restclient.AddUserAgent(restConfig, "controller-informers")), 0)
			// 让 Job 控制器显著变慢，以触发竞态条件。
			restConfig.QPS = 1
			restConfig.Burst = 1
			jc, ctx, cancel := createJobControllerWithSharedInformers(t, restConfig, informerSet)
			resetMetrics()
			t.Cleanup(cancel)
			restConfig.QPS = restConfigQPS
			restConfig.Burst = restConfigBurst
			runGC := util.CreateGCController(ctx, t, *restConfig, informerSet)
			informerSet.Start(ctx.Done())
			go jc.Run(ctx, 1)
			runGC()

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism: ptr.To[int32](2),
				},
			})
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}
			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      2,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			// 删除 Job。GC 应级联删除这些 pod。
			err = clientSet.BatchV1().Jobs(jobObj.Namespace).Delete(ctx, jobObj.Name, metav1.DeleteOptions{
				PropagationPolicy: &policy,
			})
			if err != nil {
				t.Fatalf("Failed to delete job: %v", err)
			}
			validateNoOrphanPodsWithFinalizers(ctx, t, clientSet, jobObj)
			// pod 从未结束，因此不会被计入该指标。
			validateTerminatedPodsTrackingFinalizerMetric(ctx, t, 0)
		})
	}
}

func TestFinalizersClearedWhenBackoffLimitExceeded(t *testing.T) {
	// 将未计数 pod 的最大数量设为小于 parallelism 的值，
	// 以确保它不会影响 pod 的终止。
	t.Cleanup(setDuringTest(&jobcontroller.MaxUncountedPods, 50))
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	// 在 Indexed 模式下，使用 finalizer 的 Job 跟踪所需调用更少，
	// 因此更有可能在所有 pod 都可见之前处理完所有 finalizer。
	mode := batchv1.IndexedCompletion
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			CompletionMode: &mode,
			Completions:    ptr.To[int32](100),
			Parallelism:    ptr.To[int32](100),
			BackoffLimit:   ptr.To[int32](0),
		},
	})
	if err != nil {
		t.Fatalf("Could not create job: %v", err)
	}

	// 尽快让一个 pod 失败。
	err = wait.PollUntilContextTimeout(ctx, time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
		if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Could not fail pod: %v", err)
	}

	validateJobFailed(ctx, t, clientSet, jobObj)
	validateCounterMetric(ctx, t, metrics.JobFinishedNum, metricLabelsWithValue{
		Labels: []string{"Indexed", "failed", "BackoffLimitExceeded"},
		Value:  1,
	})

	validateNoOrphanPodsWithFinalizers(ctx, t, clientSet, jobObj)
}

func TestJobPodsCreatedWithExponentialBackoff(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, 2*time.Second))
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{})
	if err != nil {
		t.Fatalf("Could not create job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 让第一个 pod 失败
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Failed:      1,
		Terminating: ptr.To[int32](0),
	})

	// 让第二个 pod 失败
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Failed setting phase %s on Job Pod: %v", v1.PodFailed, err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Failed:      2,
		Terminating: ptr.To[int32](0),
	})

	jobPods, err := getJobPods(ctx, t, clientSet, jobObj, func(ps v1.PodStatus) bool { return true })
	if err != nil {
		t.Fatalf("Failed to list Job Pods: %v", err)
	}
	if len(jobPods) != 3 {
		t.Fatalf("Expected to get %v pods, received %v", 4, len(jobPods))
	}
	validateExpotentialBackoffDelay(t, jobcontroller.DefaultJobPodFailureBackOff, jobPods)
}

func validateExpotentialBackoffDelay(t *testing.T, defaultPodFailureBackoff time.Duration, pods []*v1.Pod) {
	t.Helper()
	creationTime := []time.Time{}
	finishTime := []time.Time{}
	for _, pod := range pods {
		creationTime = append(creationTime, pod.CreationTimestamp.Time)
		if len(pod.Status.ContainerStatuses) > 0 {
			finishTime = append(finishTime, pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt.Time)
		}
	}

	sort.Slice(creationTime, func(i, j int) bool {
		return creationTime[i].Before(creationTime[j])
	})
	sort.Slice(finishTime, func(i, j int) bool {
		return finishTime[i].Before(finishTime[j])
	})

	diff := creationTime[1].Sub(finishTime[0])

	if diff < defaultPodFailureBackoff {
		t.Fatalf("Second pod should be created at least %v seconds after the first pod, time difference: %v", defaultPodFailureBackoff, diff)
	}

	if diff >= 2*defaultPodFailureBackoff {
		t.Fatalf("Second pod should be created before %v seconds after the first pod, time difference: %v", 2*defaultPodFailureBackoff, diff)
	}

	diff = creationTime[2].Sub(finishTime[1])

	if diff < 2*defaultPodFailureBackoff {
		t.Fatalf("Third pod should be created at least %v seconds after the second pod, time difference: %v", 2*defaultPodFailureBackoff, diff)
	}

	if diff >= 4*defaultPodFailureBackoff {
		t.Fatalf("Third pod should be created before %v seconds after the second pod, time difference: %v", 4*defaultPodFailureBackoff, diff)
	}
}

// TestJobFailedWithInterrupts 测试即使控制器在中途失败，
// 一个 pod 失败而其余 pod 成功的 Job 仍会被标记为 Failed。
func TestJobFailedWithInterrupts(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Completions:  ptr.To[int32](10),
			Parallelism:  ptr.To[int32](10),
			BackoffLimit: ptr.To[int32](0),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					NodeName: "foo", // 已调度的 pod 不会被立即删除。
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Could not create job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      10,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
	t.Log("Finishing pods")
	if _, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodFailed, 1); err != nil {
		t.Fatalf("Could not fail a pod: %v", err)
	}
	remaining := 9
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
		if succ, err := setJobPodsPhase(ctx, clientSet, jobObj, v1.PodSucceeded, remaining); err != nil {
			remaining -= succ
			t.Logf("Transient failure succeeding pods: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("Could not succeed the remaining %d pods: %v", remaining, err)
	}
	t.Log("Recreating job controller")
	cancel()
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)
	validateJobCondition(ctx, t, clientSet, jobObj, batchv1.JobFailed)
}

func validateNoOrphanPodsWithFinalizers(ctx context.Context, t *testing.T, clientSet clientset.Interface, jobObj *batchv1.Job) {
	t.Helper()
	orphanPods := 0
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
		pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(jobObj.Spec.Selector),
		})
		if err != nil {
			return false, err
		}
		orphanPods = 0
		for _, pod := range pods.Items {
			if hasJobTrackingFinalizer(&pod) {
				orphanPods++
			}
		}
		return orphanPods == 0, nil
	}); err != nil {
		t.Errorf("Failed waiting for pods to be freed from finalizer: %v", err)
		t.Logf("Last saw %d orphan pods", orphanPods)
	}
}

func TestOrphanPodsFinalizersClearedOnRestart(t *testing.T) {
	// 步骤 0：创建 Job。
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](1),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	// 步骤 2：在控制器停止期间删除该 Job。
	cancel()

	_, ctx = ktesting.NewTestContext(t)
	err = clientSet.BatchV1().Jobs(jobObj.Namespace).Delete(ctx, jobObj.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete job: %v", err)
	}

	// 步骤 3：重启控制器。
	ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)
	validateNoOrphanPodsWithFinalizers(ctx, t, clientSet, jobObj)
}

func TestSuspendJob(t *testing.T) {
	type step struct {
		flag       bool
		wantActive int
		wantStatus v1.ConditionStatus
		wantReason string
	}
	testCases := []struct {
		featureGate bool
		create      step
		update      step
	}{
		// 穷举测试除平凡的 true->true 和 false->false 之外的所有组合。
		{
			create: step{flag: false, wantActive: 2},
			update: step{flag: true, wantActive: 0, wantStatus: v1.ConditionTrue, wantReason: "Suspended"},
		},
		{
			create: step{flag: true, wantActive: 0, wantStatus: v1.ConditionTrue, wantReason: "Suspended"},
			update: step{flag: false, wantActive: 2, wantStatus: v1.ConditionFalse, wantReason: "Resumed"},
		},
	}

	closeFn, restConfig, clientSet, ns := setup(t, "suspend")
	t.Cleanup(closeFn)
	for _, tc := range testCases {
		name := fmt.Sprintf("feature=%v,create=%v,update=%v", tc.featureGate, tc.create.flag, tc.update.flag)
		t.Run(name, func(t *testing.T) {
			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)
			events, err := clientSet.EventsV1().Events(ns.Name).Watch(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer events.Stop()

			parallelism := int32(2)
			job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
				Spec: batchv1.JobSpec{
					Parallelism: ptr.To(parallelism),
					Completions: ptr.To[int32](4),
					Suspend:     ptr.To(tc.create.flag),
				},
			})
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}

			validate := func(s string, active int, status v1.ConditionStatus, reason string) {
				validateJobPodsStatus(ctx, t, clientSet, job, "", podsByStatus{
					Active:      active,
					Ready:       ptr.To[int32](0),
					Terminating: ptr.To[int32](0),
				})
				job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed to get Job after %s: %v", s, err)
				}
				if got, want := getJobConditionStatus(ctx, job, batchv1.JobSuspended), status; got != want {
					t.Errorf("Unexpected Job condition %q status after %s: got %q, want %q", batchv1.JobSuspended, s, got, want)
				}
				if err := waitForEvent(ctx, events, job.UID, reason); err != nil {
					t.Errorf("Waiting for event with reason %q after %s: %v", reason, s, err)
				}
			}
			validate("create", tc.create.wantActive, tc.create.wantStatus, tc.create.wantReason)

			job.Spec.Suspend = ptr.To(tc.update.flag)
			job, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to update Job: %v", err)
			}
			validate("update", tc.update.wantActive, tc.update.wantStatus, tc.update.wantReason)
		})
	}
}

// TestStartTimeUpdateOnResume 验证 Job 控制器在恢复挂起的 Job 时能更新 startTime
// （https://github.com/kubernetes/kubernetes/issues/134521）。
func TestStartTimeUpdateOnResume(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.JobManagedBy, true)

	closeFn, restConfig, clientSet, ns := setup(t, "suspend-starttime-validation")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](1),
			Completions: ptr.To[int32](2),
			Suspend:     ptr.To(false),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}
	if job.Status.StartTime == nil {
		t.Fatalf("Job startTime was not set")
	}

	job.Spec.Suspend = ptr.To(true)
	job, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to suspend Job: %v", err)
	}

	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      0,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}
	if getJobConditionStatus(ctx, job, batchv1.JobSuspended) != v1.ConditionTrue {
		t.Fatalf("JobSuspended condition was not set to True")
	}

	job.Spec.Suspend = ptr.To(false)
	job, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to resume Job: %v", err)
	}

	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}
	if getJobConditionStatus(ctx, job, batchv1.JobSuspended) != v1.ConditionFalse {
		t.Error("JobSuspended condition was not set to False")
	}
	if job.Status.StartTime == nil {
		t.Error("Job startTime was not set after resume")
	}
}

// TestSuspendJobWithZeroCompletions 验证 completions=0 的挂起 Job 会被标记为 Complete。
func TestSuspendJobWithZeroCompletions(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "suspended-with-zero-completions")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(func() {
		cancel()
	})
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Completions: ptr.To[int32](0),
			Suspend:     ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	for _, condition := range []batchv1.JobConditionType{batchv1.JobSuccessCriteriaMet, batchv1.JobComplete} {
		validateJobCondition(ctx, t, clientSet, jobObj, condition)
	}
}

func TestSuspendJobControllerRestart(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "suspend")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](2),
			Completions: ptr.To[int32](4),
			Suspend:     ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	validateJobPodsStatus(ctx, t, clientSet, job, "", podsByStatus{
		Active:      0,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})
}

func TestNodeSelectorUpdate(t *testing.T) {
	closeFn, restConfig, clientSet, ns := setup(t, "suspend")

	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{Spec: batchv1.JobSpec{
		Parallelism: ptr.To[int32](1),
		Suspend:     ptr.To(true),
	}})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}
	jobName := job.Name
	jobNamespace := job.Namespace
	jobClient := clientSet.BatchV1().Jobs(jobNamespace)

	// 由于 MutableSchedulingDirectives 设置为 true，需要等待 suspend condition 被设置，
	// 以表明 Job 确实已被挂起。
	waitForPodsToBeActive(ctx, t, jobClient, 0, job)
	validateJobCondition(ctx, t, clientSet, job, batchv1.JobSuspended)

	// (1) 在同一次更新中设置 node selector。

	nodeSelector := map[string]string{"foo": "bar"}
	if _, err := updateJob(ctx, jobClient, jobName, func(j *batchv1.Job) {
		j.Spec.Template.Spec.NodeSelector = nodeSelector
		j.Spec.Suspend = ptr.To(false)
	}); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// (2) 检查 pod 是否使用预期的 node selector 创建。

	var pod *v1.Pod
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := clientSet.CoreV1().Pods(jobNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("Failed to list Job Pods: %v", err)
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		pod = &pods.Items[0]
		return true, nil
	}); err != nil || pod == nil {
		t.Fatalf("pod not found: %v", err)
	}

	// 如果 feature gate 已启用，则 Job 现在应已恢复，且 pod 带有该 node selector。
	if diff := cmp.Diff(nodeSelector, pod.Spec.NodeSelector); diff != "" {
		t.Errorf("Unexpected nodeSelector (-want,+got):\n%s", diff)
	}

	// (3) 再次更新 node selector。由于 Job 已恢复，此操作应失败。
	_, err = updateJob(ctx, jobClient, jobName, func(j *batchv1.Job) {
		j.Spec.Template.Spec.NodeSelector = map[string]string{"foo": "baz"}
	})

	if err == nil || !strings.Contains(err.Error(), "spec.template: Invalid value") {
		t.Errorf("Expected \"spec.template: Invalid value\" error, got: %v", err)
	}

}

func TestUpdateJobPodResources(t *testing.T) {
	testCases := map[string]struct {
		enableFeatureGate bool
		suspend           bool
		updateResources   bool
		containers        []v1.Container
		expectUpdate      bool
		startThenSuspend  bool
		initialResources  *v1.ResourceRequirements
		jobName           string
	}{
		"suspended job, feature gate enabled, update resources": {
			enableFeatureGate: true,
			suspend:           true,
			updateResources:   true,
			containers: []v1.Container{{
				Name:  "test-container",
				Image: "busybox",
			}},
			expectUpdate: true,
			jobName:      "update-suspend",
		},
		"non-suspended job, feature gate enabled, update resources": {
			enableFeatureGate: true,
			suspend:           false,
			updateResources:   true,
			containers: []v1.Container{{
				Name:  "test-container",
				Image: "busybox",
			}},
			expectUpdate: false,
			jobName:      "update-fail-running",
		},
		"suspended job, feature gate disabled, update resources": {
			enableFeatureGate: false,
			suspend:           true,
			updateResources:   true,
			containers: []v1.Container{{
				Name:  "test-container",
				Image: "busybox",
			}},
			expectUpdate: false,
			jobName:      "no-update-with-feature-disabled",
		},
		"started then suspended job, feature gate enabled, update resources": {
			enableFeatureGate: true,
			suspend:           false,
			updateResources:   true,
			containers: []v1.Container{{
				Name:  "test-container",
				Image: "busybox",
			}},
			expectUpdate:     true,
			startThenSuspend: true,
			jobName:          "suspend-resume-mutate",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.MutablePodResourcesForSuspendedJobs, tc.enableFeatureGate)

			closeFn, restConfig, cs, ns := setup(t, "update-job-pod-resources")
			defer closeFn()

			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			defer cancel()

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.jobName,
					Namespace: ns.Name,
				},
				Spec: batchv1.JobSpec{
					Suspend:     ptr.To(tc.suspend),
					Parallelism: ptr.To[int32](1),
					Completions: ptr.To[int32](1),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers:    tc.containers,
							RestartPolicy: v1.RestartPolicyNever,
						},
					},
				},
			}

			job, err := createJobWithDefaults(ctx, cs, ns.Name, job)
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}

			jobClient := cs.BatchV1().Jobs(ns.Name)

			// 处理先启动后挂起的场景
			if tc.startThenSuspend {
				t.Logf("Waiting for pods to be active on starting job")
				waitForPodsToBeActive(ctx, t, jobClient, 1, job)
				// 挂起 Job
				_, err = updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
					j.Spec.Suspend = ptr.To(true)
				})
				if err != nil {
					t.Fatalf("Failed to suspend Job: %v", err)
				}
			}
			if tc.suspend || tc.startThenSuspend {
				// 等待 Job 被挂起
				waitForPodsToBeActive(ctx, t, jobClient, 0, job)
				validateJobCondition(ctx, t, cs, job, batchv1.JobSuspended)
				t.Logf("job is suspended")
			}

			if tc.updateResources {
				_, err := updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
					for i := range j.Spec.Template.Spec.Containers {
						j.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("200m"),
								v1.ResourceMemory: resource.MustParse("256Mi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("400m"),
								v1.ResourceMemory: resource.MustParse("512Mi"),
							},
						}
					}
				})
				if err != nil && tc.expectUpdate {
					t.Fatalf("Failed to update Job: %v", err)
				}
			}

			// 如果 Job 已被挂起，或属于先启动后挂起的场景，则恢复它
			if tc.suspend || tc.startThenSuspend {
				_, err = updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
					j.Spec.Suspend = ptr.To(false)
				})
				if err != nil {
					t.Fatalf("Failed to unsuspend Job: %v", err)
				}
			}
			waitForPodsToBeActive(ctx, t, jobClient, 1, job)
			pods, err := getJobPods(ctx, t, cs, job, func(s v1.PodStatus) bool { return true })
			if err != nil {
				t.Fatalf("Failed to get Job pods: %v", err)
			}
			if len(pods) != 1 {
				t.Fatalf("Expected 1 pod, got %d", len(pods))
			}

			pod := pods[0]
			expectedResources := v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("200m"),
					v1.ResourceMemory: resource.MustParse("256Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("400m"),
					v1.ResourceMemory: resource.MustParse("512Mi"),
				},
			}

			for i := range tc.containers {
				if tc.expectUpdate {
					if diff := cmp.Diff(expectedResources, pod.Spec.Containers[i].Resources); diff != "" {
						t.Errorf("Unexpected resources in pod spec for container %q (-want +got):\n%s", pod.Spec.Containers[i].Name, diff)
					}
				} else {
					if len(pod.Spec.Containers[i].Resources.Requests) > 0 ||
						len(pod.Spec.Containers[i].Resources.Limits) > 0 {
						t.Errorf("Expected no resource updates for container %q, but got: %+v", pod.Spec.Containers[i].Name, pod.Spec.Containers[i].Resources)
					}
				}
			}
		})
	}
}

// TestDelayedJobUpdateEvent 测试即使 Job 事件被延迟，Job 也只会创建一个 Pod。
// 此测试验证 finishedJobStore 工作正常，在 Job 成功或失败事件被延迟时，
// 能阻止 Job 控制器创建新的 pod。
func TestDelayedJobUpdateEvent(t *testing.T) {
	t.Cleanup(setDurationDuringTest(&jobcontroller.DefaultJobPodFailureBackOff, fastPodFailureBackoff))
	t.Cleanup(setDurationDuringTest(&jobcontroller.SyncJobBatchPeriod, fastSyncJobBatchPeriod))
	closeFn, restConfig, clientSet, ns := setup(t, "simple")
	t.Cleanup(closeFn)
	// 该 transform 用于为 Job 事件引入延迟。由于所有对象在加入 informer 缓存前
	// 都必须先经过 transform 函数，这便成了一种间接引入 watch 事件延迟的方式。
	transformOpt := informers.WithTransform(cache.TransformFunc(func(obj interface{}) (interface{}, error) {
		_, ok := obj.(*batchv1.Job)
		if ok {
			// 这将确保 pod 事件在 Job 事件之前被处理。
			time.Sleep(2 * fastSyncJobBatchPeriod)
		}
		return obj, nil
	}))

	type jobStatus struct {
		succeeded int
		failed    int
		status    batchv1.JobConditionType
	}

	cases := map[string]struct {
		podReplacementPolicyEnabled bool
		job                         *batchv1.Job
		podUpdate                   func(*v1.Pod) bool
		wantStatus                  jobStatus
	}{
		"job succeeded event delayed": {
			job: &batchv1.Job{},
			podUpdate: func(p *v1.Pod) bool {
				p.Status.Phase = v1.PodSucceeded
				p.Status.ContainerStatuses = []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{
								FinishedAt: metav1.Now(),
							},
						},
					},
				}
				return true
			},
			wantStatus: jobStatus{
				succeeded: 1,
				failed:    0,
				status:    batchv1.JobComplete,
			},
		},
		"job failed event delayed": {
			job: &batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:                     "main-container",
									Image:                    "foo",
									ImagePullPolicy:          v1.PullIfNotPresent,
									TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
								},
							},
						},
					},
					BackoffLimit: ptr.To[int32](0),
				},
			},
			podUpdate: func(p *v1.Pod) bool {
				p.Status = v1.PodStatus{
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
				}
				return true
			},
			wantStatus: jobStatus{
				succeeded: 0,
				failed:    1,
				status:    batchv1.JobFailed,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig, transformOpt)
			t.Cleanup(cancel)
			resetMetrics()

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, tc.job)
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}

			validateJobPodsStatus(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      1,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			if _, err := updateJobPodsStatus(ctx, clientSet, jobObj, tc.podUpdate, 1); err != nil {
				t.Fatalf("Error %q while updating pod status for Job: %v", err, jobObj.Name)
			}

			validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
				Failed:      tc.wantStatus.failed,
				Succeeded:   tc.wantStatus.succeeded,
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			validateJobCondition(ctx, t, clientSet, jobObj, tc.wantStatus.status)

			jobPods, err := getJobPods(ctx, t, clientSet, jobObj, func(ps v1.PodStatus) bool { return true })
			if err != nil {
				t.Fatalf("Error %v getting the list of pods for job %q", err, klog.KObj(jobObj))
			}
			if len(jobPods) != 1 {
				t.Errorf("Found %d Pods for the job %q, want 1", len(jobPods), klog.KObj(jobObj))
			}
		})
	}

}

// TestMutableSchedulingDirectivesForSuspendedJobs 验证当该功能对挂起的 Job 启用时，
// 可以修改 scheduling directives。
func TestMutableSchedulingDirectivesForSuspendedJobs(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.MutableSchedulingDirectivesForSuspendedJobs, true)

	closeFn, restConfig, clientSet, ns := setup(t, "mutable-scheduling-directives-for-suspended-jobs")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](1),
			Completions: ptr.To[int32](2),
			Suspend:     ptr.To(false),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	// 等待 Job 开始运行并设置 status.startTime
	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}
	if job.Status.StartTime == nil {
		t.Fatalf("Job startTime was not set")
	}

	// 验证对未挂起的 Job 的更新会失败
	jobCopy := job.DeepCopy()
	jobCopy.Spec.Template.Spec.NodeSelector = map[string]string{
		"key": "value",
	}

	_, err = clientSet.BatchV1().Jobs(job.Namespace).Update(ctx, jobCopy, metav1.UpdateOptions{})
	if err == nil {
		t.Fatalf("update of scheduling directives succeeded for unsuspended job %s", klog.KObj(jobCopy))
	}

	// 挂起 Job
	job.Spec.Suspend = ptr.To(true)
	job, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to suspend Job: %v", err)
	}

	// 等待 Job 没有 active Pod 且具有 JobSuspended condition
	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      0,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}

	if job.Status.StartTime != nil {
		t.Fatalf("Job startTime was not cleaned")
	}

	if getJobConditionStatus(ctx, job, batchv1.JobSuspended) != v1.ConditionTrue {
		t.Fatalf("JobSuspended condition was not set to True")
	}

	// 由于 Job 已挂起，现在该更新会被接受
	job.Spec.Template.Spec.NodeSelector = map[string]string{
		"key": "value",
	}

	job, err = clientSet.BatchV1().Jobs(job.Namespace).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update of scheduling directives failed for suspended job %s", klog.KObj(job))
	}

	// 再次恢复 Job，pod 会被创建
	job.Spec.Suspend = ptr.To(false)
	job, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to resume Job: %v", err)
	}

	// 等待 Job 再次运行，并对状态进行断言
	validateJobsPodsStatusOnly(ctx, t, clientSet, job, "", podsByStatus{
		Active:      1,
		Ready:       ptr.To[int32](0),
		Terminating: ptr.To[int32](0),
	})

	job, err = clientSet.BatchV1().Jobs(ns.Name).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job: %v", err)
	}

	if getJobConditionStatus(ctx, job, batchv1.JobSuspended) != v1.ConditionFalse {
		t.Error("JobSuspended condition was not set to False")
	}

	if job.Status.StartTime == nil {
		t.Error("Job startTime was not set after resume")
	}
}

// TestMutableSchedulingDirectivesForSuspendedJobsNotYetStarted 验证对于从未启动过的挂起 Job，
// 即使在 Job 控制器设置 JobSuspended condition 之前，也可以修改 scheduling directives。
func TestMutableSchedulingDirectivesForSuspendedJobsNotYetStarted(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.MutableSchedulingDirectivesForSuspendedJobs, true)

	closeFn, restConfig, clientSet, ns := setup(t, "mutable-scheduling-directives-not-started")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	// 创建一个挂起的 Job。
	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](1),
			Completions: ptr.To[int32](1),
			Suspend:     ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	// 不等待 JobSuspended condition 被设置，立即更新 nodeSelector。
	// 这模拟了在创建挂起 Job 后立即注入 scheduling directives 的外部控制器（例如 MultiKueue）。
	nodeSelector := map[string]string{"foo": "bar"}
	job.Spec.Template.Spec.NodeSelector = nodeSelector
	_, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update scheduling directives on suspended Job that was never started: %v", err)
	}
}

// TestMutablePodResourcesForSuspendedJobsNotYetStarted 验证对于从未启动过的挂起 Job，
// 即使在 Job 控制器设置 JobSuspended condition 之前，也可以修改 pod 资源。
func TestMutablePodResourcesForSuspendedJobsNotYetStarted(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.MutablePodResourcesForSuspendedJobs, true)

	closeFn, restConfig, clientSet, ns := setup(t, "mutable-pod-resources-not-started")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	// 创建一个挂起的 Job。
	job, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To[int32](1),
			Completions: ptr.To[int32](1),
			Suspend:     ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	// 不等待 JobSuspended condition 被设置，立即更新 pod 资源。
	// 这模拟了在创建挂起 Job 后立即注入资源需求的外部控制器（例如 MultiKueue）。
	wantResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("200m"),
			v1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	job.Spec.Template.Spec.Containers[0].Resources = wantResources
	_, err = clientSet.BatchV1().Jobs(ns.Name).Update(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update pod resources on suspended Job that was never started: %v", err)
	}
}

type podsByStatus struct {
	Active      int
	Ready       *int32
	Failed      int
	Succeeded   int
	Terminating *int32
}

func validateJobsPodsStatusOnly(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job, statusType string, desired podsByStatus) {
	t.Helper()
	validateJobsPodsStatusOnlyWithTimeout(ctx, t, clientSet, jobObj, statusType, desired, wait.ForeverTestTimeout)
}

func validateJobsPodsStatusOnlyWithTimeout(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job, statusType string, desired podsByStatus, timeout time.Duration) {
	t.Helper()
	var actualCounts podsByStatus
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, timeout, true, func(ctx context.Context) (bool, error) {
		updatedJob, err := clientSet.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
		if err != nil {
			t.Logf("Failed to get updated Job: %v", err)
			return false, nil
		}
		actualCounts = podsByStatus{
			Active:      int(updatedJob.Status.Active),
			Ready:       updatedJob.Status.Ready,
			Succeeded:   int(updatedJob.Status.Succeeded),
			Failed:      int(updatedJob.Status.Failed),
			Terminating: updatedJob.Status.Terminating,
		}
		return cmp.Equal(actualCounts, desired), nil
	}); err != nil {
		diff := cmp.Diff(desired, actualCounts)
		t.Errorf("Waiting for Job %s Status: %v\nPods (-want,+got):\n%s", statusType, err, diff)
	}
}

func validateJobStatus(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job, statusType string, wantStatus batchv1.JobStatus) {
	t.Helper()
	diff := ""
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		gotJob, err := clientSet.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get updated Job: %v, last status diff (-want,+got):\n%s", err, diff)
		}
		diff = cmp.Diff(wantStatus, gotJob.Status,
			cmpopts.EquateEmpty(),
			cmpopts.IgnoreFields(batchv1.JobStatus{}, "StartTime", "UncountedTerminatedPods", "CompletionTime"),
			cmpopts.IgnoreFields(batchv1.JobCondition{}, "LastProbeTime", "LastTransitionTime", "Message"),
		)
		return diff == "", nil
	}); err != nil {
		t.Fatalf("Waiting for Job %s Status: %v\n, Status diff (-want,+got):\n%s", statusType, err, diff)
	}
}

func validateJobPodsStatus(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job, statusType string, desired podsByStatus) {
	t.Helper()
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, statusType, desired)
	var active []*v1.Pod
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, time.Second*5, true, func(ctx context.Context) (bool, error) {
		pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("Failed to list Job Pods: %v", err)
		}
		active = nil
		for _, pod := range pods.Items {
			phase := pod.Status.Phase
			if metav1.IsControlledBy(&pod, jobObj) && (phase == v1.PodPending || phase == v1.PodRunning) {
				p := pod
				active = append(active, &p)
			}
		}
		return len(active) == desired.Active, nil
	}); err != nil {
		if len(active) != desired.Active {
			t.Errorf("Found %d active Pods, want %d", len(active), desired.Active)
		}
	}
	for _, p := range active {
		if !hasJobTrackingFinalizer(p) {
			t.Errorf("Active pod %s doesn't have tracking finalizer", p.Name)
		}
	}
}

func getJobPods(ctx context.Context, t *testing.T, clientSet clientset.Interface, jobObj *batchv1.Job, filter func(v1.PodStatus) bool) ([]*v1.Pod, error) {
	t.Helper()
	allPods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	jobPods := make([]*v1.Pod, 0)
	for _, pod := range allPods.Items {
		if metav1.IsControlledBy(&pod, jobObj) && filter(pod.Status) {
			p := pod
			jobPods = append(jobPods, &p)
		}
	}
	return jobPods, nil
}

func validateFinishedPodsNoFinalizer(ctx context.Context, t *testing.T, clientSet clientset.Interface, jobObj *batchv1.Job) {
	t.Helper()
	pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list Job Pods: %v", err)
	}
	for _, pod := range pods.Items {
		phase := pod.Status.Phase
		if metav1.IsControlledBy(&pod, jobObj) && (phase == v1.PodPending || phase == v1.PodRunning) && hasJobTrackingFinalizer(&pod) {
			t.Errorf("Finished pod %s still has a tracking finalizer", pod.Name)
		}
	}
}

// validateIndexedJobPods 验证 Indexed Job 中 active 和已完成 Pod 的 index 与 hostname。
// 请在 validateJobPodsStatus 之后调用。
func validateIndexedJobPods(ctx context.Context, t *testing.T, clientSet clientset.Interface, jobObj *batchv1.Job, wantActive sets.Set[int], gotCompleted string, wantFailed *string) {
	t.Helper()
	updatedJob, err := clientSet.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get updated Job: %v", err)
	}
	if updatedJob.Status.CompletedIndexes != gotCompleted {
		t.Errorf("Got completed indexes %q, want %q", updatedJob.Status.CompletedIndexes, gotCompleted)
	}
	if diff := cmp.Diff(wantFailed, updatedJob.Status.FailedIndexes); diff != "" {
		t.Errorf("Got unexpected failed indexes: %s", diff)
	}
	pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list Job Pods: %v", err)
	}
	gotActive := sets.New[int]()
	for _, pod := range pods.Items {
		if metav1.IsControlledBy(&pod, jobObj) {
			if pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning {
				annotationIx, err := getCompletionIndex(pod.Annotations)
				if err != nil {
					t.Errorf("Failed getting completion index in annotations for pod %s: %v", pod.Name, err)
				}
				labelIx, err := getCompletionIndex(pod.Labels)
				if err != nil {
					t.Errorf("Failed getting completion index in labels for pod %s: %v", pod.Name, err)
				}
				if annotationIx != labelIx {
					t.Errorf("Mismatch in value of annotation index: %v and label index: %v", labelIx,
						annotationIx)
				}
				gotActive.Insert(labelIx)
				expectedName := fmt.Sprintf("%s-%d", jobObj.Name, labelIx)
				if diff := cmp.Equal(expectedName, pod.Spec.Hostname); !diff {
					t.Errorf("Got pod hostname %s, want %s", pod.Spec.Hostname, expectedName)
				}
			}
		}
	}
	if wantActive == nil {
		wantActive = sets.New[int]()
	}
	if diff := cmp.Diff(sets.List(wantActive), sets.List(gotActive)); diff != "" {
		t.Errorf("Unexpected active indexes (-want,+got):\n%s", diff)
	}
}

func waitForEvent(ctx context.Context, events watch.Interface, uid types.UID, reason string) error {
	if reason == "" {
		return nil
	}
	return wait.PollUntilContextTimeout(ctx, waitInterval, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		for {
			var ev watch.Event
			select {
			case ev = <-events.ResultChan():
			default:
				return false, nil
			}
			e, ok := ev.Object.(*eventsv1.Event)
			if !ok {
				continue
			}
			ctrl := "job-controller"
			if (e.ReportingController == ctrl || e.DeprecatedSource.Component == ctrl) && e.Reason == reason && e.Regarding.UID == uid {
				return true, nil
			}
		}
	})
}

func getJobConditionStatus(ctx context.Context, job *batchv1.Job, cType batchv1.JobConditionType) v1.ConditionStatus {
	for _, cond := range job.Status.Conditions {
		if cond.Type == cType {
			return cond.Status
		}
	}
	return ""
}

func validateJobFailed(ctx context.Context, t *testing.T, clientSet clientset.Interface, jobObj *batchv1.Job) {
	t.Helper()
	validateJobCondition(ctx, t, clientSet, jobObj, batchv1.JobFailed)
}

func validateJobComplete(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job) {
	t.Helper()
	validateJobCondition(ctx, t, clientSet, jobObj, batchv1.JobComplete)
}

func validateJobCondition(ctx context.Context, t testing.TB, clientSet clientset.Interface, jobObj *batchv1.Job, cond batchv1.JobConditionType) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, waitInterval, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		j, err := clientSet.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to obtain updated Job: %v", err)
		}
		return getJobConditionStatus(ctx, j, cond) == v1.ConditionTrue, nil
	}); err != nil {
		t.Errorf("Waiting for Job to have condition %s: %v", cond, err)
	}
}

func setJobPodsPhase(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, phase v1.PodPhase, cnt int) (int, error) {
	op := func(p *v1.Pod) bool {
		p.Status.Phase = phase
		if phase == v1.PodFailed || phase == v1.PodSucceeded {
			p.Status.ContainerStatuses = []v1.ContainerStatus{
				{
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							FinishedAt: metav1.Now(),
						},
					},
				},
			}
		}
		return true
	}
	return updateJobPodsStatus(ctx, clientSet, jobObj, op, cnt)
}

func setJobPodsReady(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, cnt int) (int, error) {
	op := func(p *v1.Pod) bool {
		if podutil.IsPodReady(p) {
			return false
		}
		p.Status.Conditions = append(p.Status.Conditions, v1.PodCondition{
			Type:   v1.PodReady,
			Status: v1.ConditionTrue,
		})
		return true
	}
	return updateJobPodsStatus(ctx, clientSet, jobObj, op, cnt)
}

func updateJobPodsStatus(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, op func(*v1.Pod) bool, cnt int) (int, error) {
	pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("listing Job Pods: %w", err)
	}
	updates := make([]v1.Pod, 0, cnt)
	for _, pod := range pods.Items {
		if len(updates) == cnt {
			break
		}
		if p := pod.Status.Phase; metav1.IsControlledBy(&pod, jobObj) && p != v1.PodFailed && p != v1.PodSucceeded {
			if !op(&pod) {
				continue
			}
			updates = append(updates, pod)
		}
	}
	successful, err := updatePodStatuses(ctx, clientSet, updates)
	if successful != cnt {
		return successful, fmt.Errorf("couldn't set phase on %d Job pods", cnt-successful)
	}
	return successful, err
}

func updatePodStatuses(ctx context.Context, clientSet clientset.Interface, updates []v1.Pod) (int, error) {
	wg := sync.WaitGroup{}
	wg.Add(len(updates))
	errCh := make(chan error, len(updates))
	var updated atomic.Int32

	for _, pod := range updates {
		go func() {
			_, err := clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, &pod, metav1.UpdateOptions{})
			if err != nil {
				errCh <- err
			} else {
				updated.Add(1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return int(updated.Load()), fmt.Errorf("updating Pod status: %w", err)
	default:
	}
	return int(updated.Load()), nil
}

func setJobPhaseForIndex(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, phase v1.PodPhase, ix int) error {
	pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing Job Pods: %w", err)
	}
	for _, pod := range pods.Items {
		if p := pod.Status.Phase; !metav1.IsControlledBy(&pod, jobObj) || p == v1.PodFailed || p == v1.PodSucceeded {
			continue
		}
		if pix, err := getCompletionIndex(pod.Annotations); err == nil && pix == ix {
			pod.Status.Phase = phase
			if phase == v1.PodFailed || phase == v1.PodSucceeded {
				pod.Status.ContainerStatuses = []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{
								FinishedAt: metav1.Now(),
							},
						},
					},
				}
			}
			_, err := clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, &pod, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("updating pod %s status: %w", pod.Name, err)
			}
			return nil
		}
	}
	return errors.New("no pod matching index found")
}

func getActivePodForIndex(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, ix int) (*v1.Pod, error) {
	return getJobPodForIndex(ctx, clientSet, jobObj, ix, func(p *v1.Pod) bool {
		return !podutil.IsPodTerminal(p)
	})
}

func getJobPodForIndex(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, ix int, filter func(*v1.Pod) bool) (*v1.Pod, error) {
	pods, err := getJobPodsForIndex(ctx, clientSet, jobObj, ix, filter)
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("Pod not found for index: %v", ix)
	}
	return pods[0], nil
}

func getJobPodsForIndex(ctx context.Context, clientSet clientset.Interface, jobObj *batchv1.Job, ix int, filter func(*v1.Pod) bool) ([]*v1.Pod, error) {
	pods, err := clientSet.CoreV1().Pods(jobObj.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing Job Pods: %w", err)
	}
	var result []*v1.Pod
	for _, pod := range pods.Items {
		if !metav1.IsControlledBy(&pod, jobObj) {
			continue
		}
		if !filter(&pod) {
			continue
		}
		if pix, err := getCompletionIndex(pod.Annotations); err == nil && pix == ix {
			result = append(result, &pod)
		}
	}
	return result, nil
}

func getCompletionIndex(lookupMap map[string]string) (int, error) {
	v, ok := lookupMap[batchv1.JobCompletionIndexAnnotation]
	if !ok {
		return 0, fmt.Errorf("key %s not found in lookup Map", batchv1.JobCompletionIndexAnnotation)
	}
	return strconv.Atoi(v)
}

func createJobWithDefaults(ctx context.Context, clientSet clientset.Interface, ns string, jobObj *batchv1.Job) (*batchv1.Job, error) {
	if jobObj.Name == "" {
		jobObj.GenerateName = "test-job"
	}
	if len(jobObj.Spec.Template.Spec.Containers) == 0 {
		jobObj.Spec.Template.Spec.Containers = []v1.Container{
			{Name: "foo", Image: "bar"},
		}
	}
	if jobObj.Spec.Template.Spec.RestartPolicy == "" {
		jobObj.Spec.Template.Spec.RestartPolicy = v1.RestartPolicyNever
	}
	return clientSet.BatchV1().Jobs(ns).Create(ctx, jobObj, metav1.CreateOptions{})
}

func setup(t testing.TB, nsBaseName string) (framework.TearDownFunc, *restclient.Config, clientset.Interface, *v1.Namespace) {
	// 禁用 ServiceAccount 准入插件，因为我们没有运行 serviceaccount 控制器。
	flags := framework.DefaultTestServerFlags()
	if feature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		flags = append(flags, "--runtime-config=scheduling.k8s.io/v1beta1=true")
	}
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, flags, framework.SharedEtcd())

	config := restclient.CopyConfig(server.ClientConfig)
	config.QPS = restConfigQPS
	config.Burst = restConfigBurst
	config.Timeout = 0
	clientSet, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("Error creating clientset: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(clientSet, nsBaseName, t)
	closeFn := func() {
		framework.DeleteNamespaceOrDie(clientSet, ns, t)
		server.TearDownFn()
	}
	return closeFn, config, clientSet, ns
}

func startJobControllerAndWaitForCaches(tb testing.TB, restConfig *restclient.Config, options ...informers.SharedInformerOption) (context.Context, context.CancelFunc) {
	tb.Helper()
	informerSet := informers.NewSharedInformerFactoryWithOptions(clientset.NewForConfigOrDie(restclient.AddUserAgent(restConfig, "job-informers")), 0, options...)
	jc, ctx, cancel := createJobControllerWithSharedInformers(tb, restConfig, informerSet)
	informerSet.Start(ctx.Done())
	go jc.Run(ctx, 1)

	// 由于此方法会在单独的 goroutine 中启动控制器，
	// 而测试不会检查 /readyz，因此测试无法判断何时可以安全地调用服务器、
	// 确定请求不会被拒绝。所以我们等待缓存同步完成。
	informerSet.WaitForCacheSync(ctx.Done())
	return ctx, cancel
}

func resetMetrics() {
	metrics.TerminatedPodsTrackingFinalizerTotal.Reset()
	metrics.JobFinishedNum.Reset()
	metrics.JobPodsFinished.Reset()
	metrics.PodFailuresHandledByFailurePolicy.Reset()
	metrics.JobFinishedIndexesTotal.Reset()
	metrics.JobPodsCreationTotal.Reset()
	metrics.JobByExternalControllerTotal.Reset()
}

func createJobControllerWithSharedInformers(tb testing.TB, restConfig *restclient.Config, informerSet informers.SharedInformerFactory) (*jobcontroller.Controller, context.Context, context.CancelFunc) {
	tb.Helper()
	clientSet := clientset.NewForConfigOrDie(restclient.AddUserAgent(restConfig, "job-controller"))
	_, ctx := ktesting.NewTestContext(tb)
	ctx, cancel := context.WithCancel(ctx)

	var jc *jobcontroller.Controller
	var err error
	if feature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) {
		jc, err = jobcontroller.NewController(ctx, clientSet, informerSet.Core().V1().Pods(), informerSet.Batch().V1().Jobs(),
			informerSet.Scheduling().V1beta1().Workloads(),
			informerSet.Scheduling().V1beta1().PodGroups())
	} else {
		jc, err = jobcontroller.NewController(ctx, clientSet, informerSet.Core().V1().Pods(), informerSet.Batch().V1().Jobs(), nil, nil)
	}
	if err != nil {
		tb.Fatalf("Error creating Job controller: %v", err)
	}
	return jc, ctx, cancel
}

func hasJobTrackingFinalizer(obj metav1.Object) bool {
	for _, fin := range obj.GetFinalizers() {
		if fin == batchv1.JobTrackingFinalizer {
			return true
		}
	}
	return false
}

func setDuringTest(val *int, newVal int) func() {
	origVal := *val
	*val = newVal
	return func() {
		*val = origVal
	}
}

func setDurationDuringTest(val *time.Duration, newVal time.Duration) func() {
	origVal := *val
	*val = newVal
	return func() {
		*val = origVal
	}
}

func updateJob(ctx context.Context, jobClient typedv1.JobInterface, jobName string, updateFunc func(*batchv1.Job)) (*batchv1.Job, error) {
	var job *batchv1.Job
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		newJob, err := jobClient.Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updateFunc(newJob)
		job, err = jobClient.Update(ctx, newJob, metav1.UpdateOptions{})
		return err
	})
	return job, err
}

func waitForPodsToBeActive(ctx context.Context, t *testing.T, jobClient typedv1.JobInterface, podCount int32, jobObj *batchv1.Job) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, 5*time.Millisecond, wait.ForeverTestTimeout, true, func(context.Context) (done bool, err error) {
		job, err := jobClient.Get(ctx, jobObj.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return job.Status.Active == podCount, nil
	})
	if err != nil {
		t.Fatalf("Error waiting for Job pods to become active: %v", err)
	}
}

// waitForWorkload 等待由给定 Job 拥有的 Workload 出现（expectAbsent=false）
// 或不存在（expectAbsent=true）。
func waitForWorkload(ctx context.Context, t *testing.T, clientSet clientset.Interface, job *batchv1.Job, expectAbsent bool, timeout time.Duration) *schedulingv1beta1.Workload {
	t.Helper()
	var workload *schedulingv1beta1.Workload
	err := wait.PollUntilContextTimeout(ctx, waitInterval, timeout, true, func(ctx context.Context) (bool, error) {
		workloads, listErr := clientSet.SchedulingV1beta1().Workloads(job.Namespace).List(ctx, metav1.ListOptions{})
		if listErr != nil {
			return false, listErr
		}
		for i := range workloads.Items {
			for _, ref := range workloads.Items[i].OwnerReferences {
				if ref.UID == job.UID && ref.Kind == "Job" {
					if expectAbsent {
						return false, nil
					}
					workload = &workloads.Items[i]
					return true, nil
				}
			}
		}
		return expectAbsent, nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for Workload owned by Job %q (expectAbsent=%v): %v", job.Name, expectAbsent, err)
	}
	return workload
}

// waitForPodGroup 等待由给定 Job 拥有的 PodGroup 出现（expectAbsent=false）
// 或不存在（expectAbsent=true）。
func waitForPodGroup(ctx context.Context, t *testing.T, clientSet clientset.Interface, job *batchv1.Job, expectAbsent bool, timeout time.Duration) *schedulingv1beta1.PodGroup {
	t.Helper()
	var podGroup *schedulingv1beta1.PodGroup
	err := wait.PollUntilContextTimeout(ctx, waitInterval, timeout, true, func(ctx context.Context) (bool, error) {
		podGroups, listErr := clientSet.SchedulingV1beta1().PodGroups(job.Namespace).List(ctx, metav1.ListOptions{})
		if listErr != nil {
			return false, listErr
		}
		for i := range podGroups.Items {
			for _, ref := range podGroups.Items[i].OwnerReferences {
				if ref.UID == job.UID && ref.Kind == "Job" {
					if expectAbsent {
						return false, nil
					}
					podGroup = &podGroups.Items[i]
					return true, nil
				}
			}
		}
		return expectAbsent, nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for PodGroup owned by Job %q (expectAbsent=%v): %v", job.Name, expectAbsent, err)
	}
	return podGroup
}

func deletePods(ctx context.Context, t *testing.T, clientSet clientset.Interface, namespace string) {
	t.Helper()
	err := clientSet.CoreV1().Pods(namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{},
		metav1.ListOptions{
			Limit: 1000,
		})
	if err != nil {
		t.Fatalf("Failed to cleanup Pods: %v", err)
	}
}

func removePodsFinalizers(ctx context.Context, clientSet clientset.Interface, namespace string, finalizersNames []string) error {
	pods, err := clientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	finalizersSet := sets.New(finalizersNames...)
	return updatePod(ctx, clientSet, pods.Items, func(pod *v1.Pod) {
		podFinalizers := []string{}
		for _, podFinalizer := range pod.Finalizers {
			if _, found := finalizersSet[podFinalizer]; !found {
				podFinalizers = append(podFinalizers, podFinalizer)
			}
		}
		pod.Finalizers = podFinalizers
	})
}

func updatePod(ctx context.Context, clientSet clientset.Interface, pods []v1.Pod, updateFunc func(*v1.Pod)) error {
	for _, val := range pods {
		if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			newPod, err := clientSet.CoreV1().Pods(val.Namespace).Get(ctx, val.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			updateFunc(newPod)
			_, err = clientSet.CoreV1().Pods(val.Namespace).Update(ctx, newPod, metav1.UpdateOptions{})
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func failTerminatingPods(ctx context.Context, t *testing.T, clientSet clientset.Interface, namespace string) {
	t.Helper()
	pods, err := clientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	var terminatingPods []v1.Pod
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			pod.Status.Phase = v1.PodFailed
			terminatingPods = append(terminatingPods, pod)
		}
	}
	_, err = updatePodStatuses(ctx, clientSet, terminatingPods)
	if err != nil {
		t.Fatalf("Failed to update pod statuses: %v", err)
	}
}

// TestMutablePodResourcesWithPodReplacementPolicyFailed 测试当 PodReplacementPolicy=Failed 的 Job
// 被挂起且 pod 正在终止时，可以更新 Job 的资源，但新 pod 只会在终止中的 pod 被移除之后才创建。
// 新 pod 应使用更新后的资源。
func TestMutablePodResourcesWithPodReplacementPolicyFailed(t *testing.T) {
	const blockDeletionFinalizerForTest string = "fake.example.com/blockDeletion"

	// 启用 MutablePodResourcesForSuspendedJobs feature gate
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.MutablePodResourcesForSuspendedJobs, true)

	closeFn, restConfig, cs, ns := setup(t, "mutable-resources-pod-replacement")
	defer closeFn()

	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	defer cancel()

	// 创建一个 PodReplacementPolicy=Failed 的 Job，并添加 finalizer 以阻塞 pod 删除
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: ns.Name,
		},
		Spec: batchv1.JobSpec{
			Parallelism:          ptr.To[int32](2),
			Completions:          ptr.To[int32](2),
			PodReplacementPolicy: ptr.To(batchv1.Failed),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{blockDeletionFinalizerForTest},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:  "test-container",
						Image: "busybox",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("100m"),
								v1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("200m"),
								v1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					}},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
		},
	}

	job, err := createJobWithDefaults(ctx, cs, ns.Name, job)
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	jobClient := cs.BatchV1().Jobs(ns.Name)

	// 等待 pod 进入 active 状态
	t.Log("Waiting for 2 pods to be active")
	waitForPodsToBeActive(ctx, t, jobClient, 2, job)

	// 挂起 Job
	t.Log("Suspending the job")
	_, err = updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
		j.Spec.Suspend = ptr.To(true)
	})
	if err != nil {
		t.Fatalf("Failed to suspend Job: %v", err)
	}

	// 等待 Job 被挂起
	err = wait.PollUntilContextTimeout(ctx, time.Second, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		j, err := jobClient.Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return j.Spec.Suspend != nil && *j.Spec.Suspend, nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for job to be suspended: %v", err)
	}
	t.Log("Job is suspended")

	// 删除 pod，使其进入 Terminating 状态
	t.Log("Deleting pods to make them go to Terminating state")
	deletePods(ctx, t, cs, ns.Name)

	// 验证 pod 处于 Terminating 状态（因为 finalizer）
	validateJobsPodsStatusOnly(ctx, t, cs, job, "", podsByStatus{
		Terminating: ptr.To[int32](2),
		Active:      0,
		Ready:       ptr.To[int32](0),
	})
	t.Log("Pods are in Terminating state")

	// 用新的资源更新 Job 的 pod template
	t.Log("Updating job resources")
	_, err = updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
		for i := range j.Spec.Template.Spec.Containers {
			j.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("300m"),
					v1.ResourceMemory: resource.MustParse("384Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("600m"),
					v1.ResourceMemory: resource.MustParse("768Mi"),
				},
			}
		}
	})
	if err != nil {
		t.Fatalf("Failed to update Job resources: %v", err)
	}
	t.Log("Job resources updated")

	// 稍等片刻，确保控制器有时间处理该更新。
	// 在 PodReplacementPolicy=Failed 下，旧 pod 处于终止状态时不应创建新 pod
	time.Sleep(sleepDurationForControllerLatency)

	// 验证旧 pod 处于终止状态时没有创建新 pod
	validateJobsPodsStatusOnly(ctx, t, cs, job, "", podsByStatus{
		Terminating: ptr.To[int32](2),
		Active:      0,
		Ready:       ptr.To[int32](0),
	})
	t.Log("Verified: No new pods created while old pods are terminating")

	// 移除 finalizer，使 pod 能够完成终止
	t.Log("Removing finalizers from terminating pods")
	err = removePodsFinalizers(ctx, cs, ns.Name, []string{blockDeletionFinalizerForTest})
	if err != nil {
		t.Fatalf("Failed to remove finalizers: %v", err)
	}

	// 等待 pod 被删除，并验证 terminating 计数降为 0
	err = wait.PollUntilContextTimeout(ctx, time.Second, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		updatedJob, err := jobClient.Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return updatedJob.Status.Terminating == nil || *updatedJob.Status.Terminating == 0, nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for terminating pods to be removed: %v", err)
	}
	t.Log("Terminating pods have been removed")

	// 恢复 Job
	t.Log("Unsuspending the job")
	_, err = updateJob(ctx, jobClient, job.Name, func(j *batchv1.Job) {
		j.Spec.Suspend = ptr.To(false)
	})
	if err != nil {
		t.Fatalf("Failed to unsuspend Job: %v", err)
	}

	// 等待新 pod 被创建
	waitForPodsToBeActive(ctx, t, jobClient, 2, job)
	t.Log("New pods are active")

	// 验证新 pod 使用了更新后的资源
	pods, err := getJobPods(ctx, t, cs, job, func(s v1.PodStatus) bool { return true })
	if err != nil {
		t.Fatalf("Failed to get Job pods: %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("Expected 2 pods, got %d", len(pods))
	}

	expectedResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("300m"),
			v1.ResourceMemory: resource.MustParse("384Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("600m"),
			v1.ResourceMemory: resource.MustParse("768Mi"),
		},
	}

	for _, pod := range pods {
		for i := range pod.Spec.Containers {
			if diff := cmp.Diff(expectedResources, pod.Spec.Containers[i].Resources); diff != "" {
				t.Errorf("Unexpected resources in pod %s for container %q (-want +got):\n%s", pod.Name, pod.Spec.Containers[i].Name, diff)
			}
		}
	}
	t.Log("Verified: New pods have the updated resources")
}

func TestJobGangScheduling(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload:                 true,
		features.WorkloadWithJob:                 true,
		features.TopologyAwareWorkloadScheduling: true,
	})

	cases := map[string]struct {
		jobSpec        batchv1.JobSpec
		expectWorkload bool
		expectPodGroup bool
		wantActivePods int32
		suspend        bool
		// 在 Workload 创建后重启控制器，
		// 并验证 Workload 的 UID 被保留。
		simulateRestart   bool
		wantGangMinCount  *int32
		wantDisruptionAll bool
		wantTopologyKey   string
	}{
		"eligible job creates workload and podgroup": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
			},
			expectWorkload: true,
			expectPodGroup: true,
			wantActivePods: 4,
		},
		"non-indexed basic job still materializes objects (Universal Representation)": {
			jobSpec: batchv1.JobSpec{
				Parallelism: ptr.To[int32](3),
			},
			expectWorkload: true,
			expectPodGroup: true,
			wantActivePods: 3,
		},
		"single-pod basic job materializes objects": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](1),
				Completions:    ptr.To[int32](1),
				CompletionMode: new(batchv1.IndexedCompletion),
			},
			expectWorkload: true,
			expectPodGroup: true,
			wantActivePods: 1,
		},
		"completions != parallelism basic job materializes objects": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](2),
				Completions:    ptr.To[int32](5),
				CompletionMode: new(batchv1.IndexedCompletion),
			},
			expectWorkload: true,
			expectPodGroup: true,
			wantActivePods: 2,
		},
		"high-level controller owns scheduling objects via existing schedulingGroup": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
				Template: v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						SchedulingGroup: &v1.PodSchedulingGroup{
							PodGroupName: new("external-pod-group"),
						},
						Containers:    []v1.Container{{Name: "foo", Image: "bar"}},
						RestartPolicy: v1.RestartPolicyNever,
					},
				},
			},
			wantActivePods: 4,
		},
		"suspended job creates workload before pods": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
				Suspend:        new(true),
			},
			expectWorkload: true,
			expectPodGroup: true,
			wantActivePods: 4,
			suspend:        true,
		},
		"controller restart preserves workload": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
			},
			expectWorkload:  true,
			expectPodGroup:  true,
			wantActivePods:  4,
			simulateRestart: true,
		},
		"gang job with all-or-nothing disruption mode": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
				Scheduling: &batchv1.JobSchedulingConfiguration{
					SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](4)},
					},
					DisruptionMode: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
						All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
					},
				},
			},
			expectWorkload:    true,
			expectPodGroup:    true,
			wantActivePods:    4,
			wantGangMinCount:  ptr.To[int32](4),
			wantDisruptionAll: true,
		},
		"gang job with topology constraint": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](4),
				Completions:    ptr.To[int32](4),
				CompletionMode: new(batchv1.IndexedCompletion),
				Scheduling: &batchv1.JobSchedulingConfiguration{
					SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](2)},
					},
					SchedulingConstraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/rack"}},
					},
				},
			},
			expectWorkload:   true,
			expectPodGroup:   true,
			wantActivePods:   4,
			wantGangMinCount: ptr.To[int32](2),
			wantTopologyKey:  "topology.kubernetes.io/rack",
		},
		"gang job with disruption and topology": {
			jobSpec: batchv1.JobSpec{
				Parallelism:    ptr.To[int32](6),
				Completions:    ptr.To[int32](6),
				CompletionMode: new(batchv1.IndexedCompletion),
				Scheduling: &batchv1.JobSchedulingConfiguration{
					SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](6)},
					},
					DisruptionMode: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
						All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
					},
					SchedulingConstraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					},
				},
			},
			expectWorkload:    true,
			expectPodGroup:    true,
			wantActivePods:    6,
			wantGangMinCount:  ptr.To[int32](6),
			wantDisruptionAll: true,
			wantTopologyKey:   "topology.kubernetes.io/zone",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			closeFn, restConfig, clientSet, ns := setup(t, "gang-scheduling")
			t.Cleanup(closeFn)
			ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
			t.Cleanup(cancel)

			jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
				Spec: tc.jobSpec,
			})
			if err != nil {
				t.Fatalf("Failed to create Job: %v", err)
			}

			// 验证在 pod 之前已存在调度对象，然后恢复 Job。
			if tc.suspend {
				if tc.expectWorkload {
					waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
				}
				jobPods, err := getJobPods(ctx, t, clientSet, jobObj, func(v1.PodStatus) bool { return true })
				if err != nil {
					t.Fatalf("Failed to get job pods: %v", err)
				}
				if len(jobPods) != 0 {
					t.Errorf("Expected 0 pods for suspended job, got %d", len(jobPods))
				}
				_, err = updateJob(ctx, clientSet.BatchV1().Jobs(ns.Name), jobObj.Name, func(j *batchv1.Job) {
					j.Spec.Suspend = new(false)
				})
				if err != nil {
					t.Fatalf("Failed to unsuspend job: %v", err)
				}
			}

			// 模拟控制器重启，验证 Workload 的 UID 被保留。
			if tc.simulateRestart {
				originalWorkloadUID := waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout).UID

				cancel()
				ctx, cancel = startJobControllerAndWaitForCaches(t, restConfig)
				t.Cleanup(cancel)

				_, err = updateJob(ctx, clientSet.BatchV1().Jobs(ns.Name), jobObj.Name, func(j *batchv1.Job) {
					if j.Annotations == nil {
						j.Annotations = make(map[string]string)
					}
					j.Annotations["test-resync"] = "true"
				})
				if err != nil {
					t.Fatalf("Failed to update job: %v", err)
				}

				workload := waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
				if workload.UID != originalWorkloadUID {
					t.Errorf("Workload UID changed after restart: got %v, want %v", workload.UID, originalWorkloadUID)
				}
			}

			// 等待 Workload 和 PodGroup 出现（或确认其不存在）。
			absentTimeout := 10 * time.Second
			var workload *schedulingv1beta1.Workload
			var podGroup *schedulingv1beta1.PodGroup
			if tc.expectWorkload {
				workload = waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
			} else {
				waitForWorkload(ctx, t, clientSet, jobObj, true, absentTimeout)
			}
			if tc.expectPodGroup {
				podGroup = waitForPodGroup(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
			} else {
				waitForPodGroup(ctx, t, clientSet, jobObj, true, absentTimeout)
			}

			// 验证 pod 状态。
			validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
				Active:      int(tc.wantActivePods),
				Ready:       ptr.To[int32](0),
				Terminating: ptr.To[int32](0),
			})

			// 验证 PodGroup 的 ownerRef 以及 pod 的 schedulingGroup。
			if workload != nil && podGroup != nil {
				expectedOwnerRefs := map[types.UID]string{
					jobObj.UID:   "Job",
					workload.UID: "Workload",
				}
				for _, ref := range podGroup.OwnerReferences {
					delete(expectedOwnerRefs, ref.UID)
				}
				for uid, kind := range expectedOwnerRefs {
					t.Errorf("PodGroup missing %s ownerReference (UID %v)", kind, uid)
				}

				pods, err := getJobPods(ctx, t, clientSet, jobObj, func(v1.PodStatus) bool { return true })
				if err != nil {
					t.Fatalf("Failed to get job pods: %v", err)
				}
				for _, pod := range pods {
					if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
						t.Errorf("Pod %s missing schedulingGroup", pod.Name)
						continue
					}
					if *pod.Spec.SchedulingGroup.PodGroupName != podGroup.Name {
						t.Errorf("Pod %s schedulingGroup = %q, want %q", pod.Name, *pod.Spec.SchedulingGroup.PodGroupName, podGroup.Name)
					}
				}
			}

			if workload != nil && podGroup != nil {
				if len(workload.Spec.PodGroupTemplates) != 1 {
					t.Fatalf("expected exactly one PodGroupTemplate, got %d", len(workload.Spec.PodGroupTemplates))
				}
				pgt := workload.Spec.PodGroupTemplates[0]

				if tc.wantGangMinCount != nil {
					if pgt.SchedulingPolicy.Gang == nil || pgt.SchedulingPolicy.Gang.MinCount != *tc.wantGangMinCount {
						t.Errorf("Workload gang = %+v, want minCount %d", pgt.SchedulingPolicy.Gang, *tc.wantGangMinCount)
					}
					if podGroup.Spec.SchedulingPolicy.Gang == nil || podGroup.Spec.SchedulingPolicy.Gang.MinCount != *tc.wantGangMinCount {
						t.Errorf("PodGroup gang = %+v, want minCount %d", podGroup.Spec.SchedulingPolicy.Gang, *tc.wantGangMinCount)
					}
				}

				if tc.wantDisruptionAll {
					if pgt.DisruptionMode == nil || pgt.DisruptionMode.All == nil {
						t.Errorf("Workload disruptionMode = %+v, want All", pgt.DisruptionMode)
					}
					if podGroup.Spec.DisruptionMode == nil || podGroup.Spec.DisruptionMode.All == nil {
						t.Errorf("PodGroup disruptionMode = %+v, want All", podGroup.Spec.DisruptionMode)
					}
				}

				if tc.wantTopologyKey != "" {
					if pgt.SchedulingConstraints == nil || len(pgt.SchedulingConstraints.Topology) != 1 ||
						pgt.SchedulingConstraints.Topology[0].Key != tc.wantTopologyKey {
						t.Errorf("Workload topology = %+v, want key %q", pgt.SchedulingConstraints, tc.wantTopologyKey)
					}
					if podGroup.Spec.SchedulingConstraints == nil || len(podGroup.Spec.SchedulingConstraints.Topology) != 1 ||
						podGroup.Spec.SchedulingConstraints.Topology[0].Key != tc.wantTopologyKey {
						t.Errorf("PodGroup topology = %+v, want key %q", podGroup.Spec.SchedulingConstraints, tc.wantTopologyKey)
					}
				}
			}

		})
	}
}

func TestJobGangSchedulingElasticScaling(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
		features.WorkloadWithJob: true,
	})

	closeFn, restConfig, clientSet, ns := setup(t, "gang-elastic")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism:    ptr.To[int32](8),
			Completions:    ptr.To[int32](8),
			CompletionMode: new(batchv1.IndexedCompletion),
			Scheduling: &batchv1.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
					Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](4)},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	workload := waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
	podGroup := waitForPodGroup(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)

	// 编译生成的对象应带有请求的 gang minCount。
	if len(workload.Spec.PodGroupTemplates) != 1 || workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang == nil {
		t.Fatalf("expected a single gang PodGroupTemplate, got %+v", workload.Spec.PodGroupTemplates)
	}
	if got := workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount; got != 4 {
		t.Errorf("Workload gang minCount = %d, want 4", got)
	}
	if podGroup.Spec.SchedulingPolicy.Gang == nil || podGroup.Spec.SchedulingPolicy.Gang.MinCount != 4 {
		t.Errorf("PodGroup gang minCount = %+v, want 4", podGroup.Spec.SchedulingPolicy.Gang)
	}

	// 弹性扩缩容：提高 minCount，并验证它会传播到两个对象。
	if _, err := updateJob(ctx, clientSet.BatchV1().Jobs(ns.Name), jobObj.Name, func(j *batchv1.Job) {
		j.Spec.Scheduling.SchedulingPolicy.Gang.MinCount = ptr.To[int32](6)
	}); err != nil {
		t.Fatalf("Failed to update gang minCount: %v", err)
	}

	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		wl, err := clientSet.SchedulingV1beta1().Workloads(ns.Name).Get(ctx, workload.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		pg, err := clientSet.SchedulingV1beta1().PodGroups(ns.Name).Get(ctx, podGroup.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		wlGang := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
		pgGang := pg.Spec.SchedulingPolicy.Gang
		return wlGang != nil && wlGang.MinCount == 6 && pgGang != nil && pgGang.MinCount == 6, nil
	}); err != nil {
		t.Errorf("Workload/PodGroup gang minCount not updated to 6: %v", err)
	}
}

func TestJobGangSchedulingSuspendResume(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
		features.WorkloadWithJob: true,
	})

	closeFn, restConfig, clientSet, ns := setup(t, "gang-suspend")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	// 以挂起状态创建：Workload/PodGroup 必须在任何 pod 之前存在。
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		Spec: batchv1.JobSpec{
			Parallelism:    new(int32(4)),
			Completions:    new(int32(4)),
			CompletionMode: new(batchv1.IndexedCompletion),
			Suspend:        new(true),
			Scheduling: &batchv1.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
					Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](4)},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Job: %v", err)
	}

	workload := waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
	podGroup := waitForPodGroup(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
	workloadUID, podGroupUID := workload.UID, podGroup.UID
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active: 0, Ready: new(int32(0)), Terminating: new(int32(0)),
	})

	setSuspend := func(suspend bool) {
		if _, err := updateJob(ctx, clientSet.BatchV1().Jobs(ns.Name), jobObj.Name, func(j *batchv1.Job) {
			j.Spec.Suspend = new(suspend)
		}); err != nil {
			t.Fatalf("Failed to set suspend=%v: %v", suspend, err)
		}
	}
	verifyPodsInPodGroup := func(stage string) {
		pods, err := getJobPods(ctx, t, clientSet, jobObj, func(v1.PodStatus) bool { return true })
		if err != nil {
			t.Fatalf("[%s] Failed to get job pods: %v", stage, err)
		}
		for _, pod := range pods {
			if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil ||
				*pod.Spec.SchedulingGroup.PodGroupName != podGroup.Name {
				t.Errorf("[%s] Pod %s schedulingGroup = %+v, want podGroupName %q", stage, pod.Name, pod.Spec.SchedulingGroup, podGroup.Name)
			}
		}
	}

	// 恢复：pod 被创建并连接到现有的 PodGroup。
	setSuspend(false)
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active: 4, Ready: new(int32(0)), Terminating: new(int32(0)),
	})
	verifyPodsInPodGroup("after resume")

	// 挂起：pod 被移除，但调度对象保持不变。
	setSuspend(true)
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active: 0, Ready: new(int32(0)), Terminating: new(int32(0)),
	})
	if wl := waitForWorkload(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout); wl.UID != workloadUID {
		t.Errorf("Workload was recreated across suspend: UID %v, want %v", wl.UID, workloadUID)
	}
	if pg := waitForPodGroup(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout); pg.UID != podGroupUID {
		t.Errorf("PodGroup was recreated across suspend: UID %v, want %v", pg.UID, podGroupUID)
	}

	// 再次恢复：复用相同的对象，并将 pod 重新连接到它们。
	setSuspend(false)
	validateJobsPodsStatusOnly(ctx, t, clientSet, jobObj, "", podsByStatus{
		Active: 4, Ready: new(int32(0)), Terminating: new(int32(0)),
	})
	verifyPodsInPodGroup("after second resume")
}

// TestJobDelegatedPodGroup 验证以下场景：一个非 root Job 的父控制器拥有 Workload，
// 并通过 group-template annotation 将 PodGroup 管理委托出去；此时会从父级持久化的模板中
// 具化出 PodGroup，而 Job 控制器不会创建 Workload，也不会取得其所有权。
func TestJobDelegatedPodGroup(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
		features.WorkloadWithJob: true,
	})

	closeFn, restConfig, clientSet, ns := setup(t, "delegated-podgroup")
	t.Cleanup(closeFn)
	ctx, cancel := startJobControllerAndWaitForCaches(t, restConfig)
	t.Cleanup(cancel)

	const (
		parentKind   = "SuperJob"
		parentGroup  = "example.com"
		parentName   = "parent-superjob"
		templateName = "workers"
	)

	// 一个由父级拥有的 Workload，带有一个命名 gang 模板。其 spec.controllerRef
	// 指向父控制器，以便 Job 控制器能够发现它。委托的 PodGroup 必须从该持久化模板
	// 具化而来，而不是从 Job spec 编译得到。
	parentWorkload, err := clientSet.SchedulingV1beta1().Workloads(ns.Name).Create(ctx, &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-workload",
			Namespace: ns.Name,
		},
		Spec: schedulingv1beta1.WorkloadSpec{
			ControllerRef: &schedulingv1beta1.TypedLocalObjectReference{
				APIGroup: parentGroup,
				Kind:     parentKind,
				Name:     parentName,
			},
			PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{{
				Name: templateName,
				SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
					Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 3},
				},
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create parent Workload: %v", err)
	}

	// 一个由父控制器拥有的非 root Job，通过 group-template annotation
	// 委托 PodGroup 管理。
	jobObj, err := createJobWithDefaults(ctx, clientSet, ns.Name, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "delegated-job",
			Annotations: map[string]string{apischeduling.GroupTemplateNameAnnotation: templateName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: parentGroup + "/v1",
				Kind:       parentKind,
				Name:       parentName,
				UID:        types.UID("parent-superjob-uid"),
				Controller: new(true),
			}},
		},
		Spec: batchv1.JobSpec{
			Parallelism:    new(int32(3)),
			Completions:    new(int32(3)),
			CompletionMode: new(batchv1.IndexedCompletion),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create delegated Job: %v", err)
	}

	// The controller creates a PodGroup owned by the Job, but never a Workload
	// (the parent owns the only Workload).
	podGroup := waitForPodGroup(ctx, t, clientSet, jobObj, false, wait.ForeverTestTimeout)
	waitForWorkload(ctx, t, clientSet, jobObj, true, 10*time.Second)

	// The PodGroup is materialized from the parent's persisted template.
	if podGroup.Spec.WorkloadRef == nil ||
		podGroup.Spec.WorkloadRef.WorkloadName != parentWorkload.Name ||
		podGroup.Spec.WorkloadRef.TemplateName != templateName {
		t.Errorf("PodGroup workloadRef = %+v, want workload %q template %q",
			podGroup.Spec.WorkloadRef, parentWorkload.Name, templateName)
	}
	if podGroup.Spec.SchedulingPolicy.Gang == nil || podGroup.Spec.SchedulingPolicy.Gang.MinCount != 3 {
		t.Errorf("PodGroup gang = %+v, want minCount 3", podGroup.Spec.SchedulingPolicy.Gang)
	}

	// The PodGroup carries a controller ownerRef to the Job and does NOT link the
	// parent-owned Workload as an owner (delegated mode).
	var hasJobController bool
	for _, ref := range podGroup.OwnerReferences {
		if ref.Kind == "Workload" {
			t.Errorf("delegated PodGroup unexpectedly links Workload owner %q", ref.Name)
		}
		if ref.Kind == "Job" && ref.UID == jobObj.UID && ref.Controller != nil && *ref.Controller {
			hasJobController = true
		}
	}
	if !hasJobController {
		t.Errorf("PodGroup missing controller ownerReference to Job %q; got %+v", jobObj.Name, podGroup.OwnerReferences)
	}

	// The delegated Job never takes ownership of the parent Workload.
	gotParent, err := clientSet.SchedulingV1beta1().Workloads(ns.Name).Get(ctx, parentWorkload.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get parent Workload: %v", err)
	}
	for _, ref := range gotParent.OwnerReferences {
		if ref.UID == jobObj.UID {
			t.Errorf("parent Workload unexpectedly owned by delegated Job %q", jobObj.Name)
		}
	}
}
