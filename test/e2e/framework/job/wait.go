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

package job

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/utils/ptr"
)

// JobState 用于校验 Job 是否满足某个条件。
// 满足时返回空字符串。
// 否则返回的字符串解释为何不满足。
// 字符串应简短，调用方会自行附上 job 对象的 dump。
type JobState func(job *batchv1.Job) string

// WaitForJobPodsRunning 等待命名空间 ns 中名为 JobName 的 Job 的所有 Pod 进入 Running。
// 只能在 Pod 会长时间运行时使用，否则会有竞态。
// WaitForJobPodsRunningWithTimeout 同上，但多一个 timeout 参数。
func WaitForJobPodsRunning(ctx context.Context, c clientset.Interface, ns, jobName string, expectedCount int32) error {
	return waitForJobPodsInPhase(ctx, c, ns, jobName, expectedCount, v1.PodRunning, JobTimeout)
}

// WaitForJobPodsRunningWithTimeout 等待命名空间 ns 中名为 JobName 的 Job 的所有 Pod 进入 Running。只能在 Pod 会长时间运行时使用，否则会有竞态。
// WaitForJobPodsRunningWithTimeout 与 WaitForJobPodsRunning 相同，但多一个 timeout 参数。
func WaitForJobPodsRunningWithTimeout(ctx context.Context, c clientset.Interface, ns, jobName string, expectedCount int32, timeout time.Duration) error {
	return waitForJobPodsInPhase(ctx, c, ns, jobName, expectedCount, v1.PodRunning, timeout)
}

// WaitForJobPodsSucceeded 等待命名空间 ns 中名为 JobName 的 Job 的所有 Pod 进入 Succeeded。
func WaitForJobPodsSucceeded(ctx context.Context, c clientset.Interface, ns, jobName string, expectedCount int32) error {
	return waitForJobPodsInPhase(ctx, c, ns, jobName, expectedCount, v1.PodSucceeded, JobTimeout)
}

// waitForJobPodsInPhase 等待命名空间 ns 中名为 JobName 的 Job 的所有 Pod 进入给定 phase。
func waitForJobPodsInPhase(ctx context.Context, c clientset.Interface, ns, jobName string, expectedCount int32, phase v1.PodPhase, timeout time.Duration) error {
	get := func(ctx context.Context) (*v1.PodList, error) {
		return GetJobPods(ctx, c, ns, jobName)
	}
	match := func(pods *v1.PodList) (func() string, error) {
		count := int32(0)
		for _, p := range pods.Items {
			if p.Status.Phase == phase {
				count++
			}
		}
		if count == expectedCount {
			return nil, nil
		}
		return func() string {
			return fmt.Sprintf("job %q expected %d pods in %q phase, but got %d:\n%s",
				klog.KRef(ns, jobName), expectedCount, phase, count, format.Object(pods, 1))
		}, nil
	}
	return framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithPolling(framework.Poll).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(match))
}

// WaitForJobComplete 用 c 等待命名空间 ns 中名为 jobName 的 Job 完成预期的 completions。
// 该函数检查成功的 Job Pod 数是否达到预期 completions，
// 且 Job 带有预期 reason 的 "Complete" 条件。
func WaitForJobComplete(ctx context.Context, c clientset.Interface, ns, jobName string, reason string, completions int32) error {
	// 该函数由 HandleRetry 调用，遇到瞬时 API 错误会重试，
	// 遇到其他错误则停止轮询。
	get := func(ctx context.Context) (*batchv1.Job, error) {
		job, err := c.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if isJobFailed(job) {
			return nil, gomega.StopTrying("job failed while waiting for its completion").Attach("job", job)
		}
		return job, nil
	}
	match := func(job *batchv1.Job) (func() string, error) {
		if job.Status.Succeeded == completions {
			return nil, nil
		}
		return func() string {
			return fmt.Sprintf("expected job %q to have %v successful pods. got %v", klog.KObj(job), completions, job.Status.Succeeded)
		}, nil
	}
	err := framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithTimeout(JobTimeout).
		WithPolling(framework.Poll).
		Should(framework.MakeMatcher(match))
	if err != nil {
		return err
	}
	return WaitForJobCondition(ctx, c, ns, jobName, batchv1.JobComplete, &reason)
}

// WaitForJobReady 等待 Job 的 .status.ready 字段变为指定值
func WaitForJobReady(ctx context.Context, c clientset.Interface, ns, jobName string, ready *int32) error {
	return WaitForJobState(ctx, c, ns, jobName, JobTimeout, func(job *batchv1.Job) string {
		if ptr.Equal(ready, job.Status.Ready) {
			return ""
		}
		return "job does not match intended ready status"
	})
}

// WaitForJobSuspend 用 c 等待命名空间 ns 中名为 jobName 的 Job 进入 suspend 条件。
func WaitForJobSuspend(ctx context.Context, c clientset.Interface, ns, jobName string) error {
	return WaitForJobState(ctx, c, ns, jobName, JobTimeout, func(job *batchv1.Job) string {
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobSuspended && c.Status == v1.ConditionTrue {
				return ""
			}
		}
		return "job should be suspended"
	})
}

// WaitForJobFailed 用 c 等待命名空间 ns 中名为 jobName 的 Job 失败
func WaitForJobFailed(ctx context.Context, c clientset.Interface, ns, jobName string) error {
	// 该函数由 HandleRetry 调用，遇到瞬时 API 错误会重试，
	// 遇到其他错误则停止轮询。
	get := func(ctx context.Context) (*batchv1.Job, error) {
		job, err := c.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if isJobCompleted(job) {
			return nil, gomega.StopTrying("job completed while waiting for its failure").Attach("job", job)
		}
		return job, nil
	}
	match := func(job *batchv1.Job) (func() string, error) {
		if isJobFailed(job) {
			return nil, nil
		}
		return func() string {
			return fmt.Sprintf("expected job %q to fail", klog.KObj(job))
		}, nil
	}
	return framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithTimeout(JobTimeout).
		WithPolling(framework.Poll).
		Should(framework.MakeMatcher(match))
}

// WaitForJobCondition 等待指定 Job 出现带特定 reason 的预期条件。
// reason 传 nil 时不检查条件中的 "reason" 字符串。
func WaitForJobCondition(ctx context.Context, c clientset.Interface, ns, jobName string, cType batchv1.JobConditionType, reason *string) error {
	match := func(job *batchv1.Job) (func() string, error) {
		for _, c := range job.Status.Conditions {
			if c.Type == cType && c.Status == v1.ConditionTrue {
				if reason == nil || *reason == c.Reason {
					return nil, nil
				}
			}
		}
		return func() string {
			return fmt.Sprintf("expected job %q to reach the expected condition %q with reason %q", klog.KObj(job), cType, ptr.Deref(reason, "<nil>"))
		}, nil
	}
	return framework.Gomega().
		Eventually(ctx, framework.GetObject(c.BatchV1().Jobs(ns).Get, jobName, metav1.GetOptions{})).
		WithTimeout(JobTimeout).
		WithPolling(framework.Poll).
		Should(framework.MakeMatcher(match))
}

// WaitForJobFinish 用 c 等待命名空间 ns 中名为 jobName 的 Job 结束（Failed 或 Complete）。
func WaitForJobFinish(ctx context.Context, c clientset.Interface, ns, jobName string) error {
	return WaitForJobFinishWithTimeout(ctx, c, ns, jobName, JobTimeout)
}

// WaitForJobFinishWithTimeout 用 c 等待命名空间 ns 中名为 jobName 的 Job 结束（Failed 或 Complete）。
func WaitForJobFinishWithTimeout(ctx context.Context, c clientset.Interface, ns, jobName string, timeout time.Duration) error {
	return framework.Gomega().
		Eventually(ctx, framework.GetObject(c.BatchV1().Jobs(ns).Get, jobName, metav1.GetOptions{})).
		WithPolling(framework.Poll).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(job *batchv1.Job) (func() string, error) {
			if isJobFinished(job) {
				return nil, nil
			}
			return func() string {
				return fmt.Sprintf("expected job %q to be finished\n%s", klog.KObj(job), format.Object(job, 1))
			}, nil
		}))
}

func isJobFinished(j *batchv1.Job) bool {
	return isJobCompleted(j) || isJobFailed(j)
}

func isJobFailed(j *batchv1.Job) bool {
	return isConditionTrue(j, batchv1.JobFailed)
}

func isJobCompleted(j *batchv1.Job) bool {
	return isConditionTrue(j, batchv1.JobComplete)
}

func isConditionTrue(j *batchv1.Job, condition batchv1.JobConditionType) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == condition && c.Status == v1.ConditionTrue {
			return true
		}
	}

	return false
}

// WaitForJobGone 用 c 等待最多 timeout，直到命名空间 ns 中名为 jobName 的 Job 被移除。
func WaitForJobGone(ctx context.Context, c clientset.Interface, ns, jobName string, timeout time.Duration) error {
	return framework.Gomega().
		Eventually(ctx, func(ctx context.Context) error {
			_, err := framework.GetObject(c.BatchV1().Jobs(ns).Get, jobName, metav1.GetOptions{})(ctx)
			return err
		}).
		WithPolling(framework.Poll).
		WithTimeout(timeout).
		Should(gomega.MatchError(apierrors.IsNotFound, fmt.Sprintf("that expected job %q to be gone", klog.KRef(ns, jobName))))
}

// WaitForAllJobPodsGone 等待命名空间 ns 中名为 jobName 的 Job 的所有 Pod
// 被删除。
func WaitForAllJobPodsGone(ctx context.Context, c clientset.Interface, ns, jobName string) error {
	get := func(ctx context.Context) (*v1.PodList, error) {
		return GetJobPods(ctx, c, ns, jobName)
	}
	return framework.Gomega().
		Eventually(ctx, framework.HandleRetry(get)).
		WithPolling(framework.Poll).
		WithTimeout(JobTimeout).
		Should(gomega.HaveField("Items", gomega.BeEmpty()))
}

// WaitForJobState 等待 job 匹配给定的状态函数。
func WaitForJobState(ctx context.Context, c clientset.Interface, ns, jobName string, timeout time.Duration, state JobState) error {
	return framework.Gomega().
		Eventually(ctx, framework.GetObject(c.BatchV1().Jobs(ns).Get, jobName, metav1.GetOptions{})).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(job *batchv1.Job) (func() string, error) {
			matches := state(job)
			if matches == "" {
				return nil, nil
			}
			return func() string {
				return fmt.Sprintf("%v\n%s", matches, format.Object(job, 1))
			}, nil
		}))
}
