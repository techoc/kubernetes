/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller/job/metrics"
)

// TestUIDTrackingExpectations 验证 UID 期望跟踪器在并发写入、观察与删除下的行为：
// 期望集合可被并行追加与消费，清空后不应残留。
func TestUIDTrackingExpectations(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tracks := []struct {
		job         string
		firstRound  []types.UID
		secondRound []types.UID
	}{
		{
			job:         "foo",
			firstRound:  []types.UID{"a", "b", "c", "d"},
			secondRound: []types.UID{"e", "f"},
		},
		{
			job:         "bar",
			firstRound:  []types.UID{"x", "y", "z"},
			secondRound: []types.UID{"u", "v", "w"},
		},
		{
			job:         "baz",
			firstRound:  []types.UID{"w"},
			secondRound: []types.UID{"a"},
		},
	}
	expectations := newUIDTrackingExpectations()

	// 并行插入第一批 UID，验证跟踪器内部同步机制能承受并发调用。
	var wg sync.WaitGroup
	wg.Add(len(tracks))
	errs := make([]error, len(tracks))
	for i := range tracks {
		track := tracks[i]
		go func(errID int) {
			errs[errID] = expectations.expectFinalizersRemoved(logger, track.job, track.firstRound)
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Failed adding first round of UIDs for job %s: %v", tracks[i].job, err)
		}
	}

	// 断言每个 job 的期望集合与插入顺序一致（orderedIntervals 保留顺序）。
	for _, track := range tracks {
		uids := expectations.getSet(track.job)
		if uids == nil {
			t.Errorf("Set of UIDs is empty for job %s", track.job)
		} else if diff := cmp.Diff(track.firstRound, sets.List(uids.set)); diff != "" {
			t.Errorf("Unexpected keys for job %s (-want,+got):\n%s", track.job, diff)
		}
	}

	// 并行消费第一批 UID 并插入第二批，模拟真实控制器中事件与期望并发到达。

	for i, track := range tracks {
		wg.Add(len(track.firstRound) + 1)
		for _, uid := range track.firstRound {
			go func() {
				expectations.finalizerRemovalObserved(logger, track.job, uid)
				wg.Done()
			}()
		}
		go func(errID int) {
			errs[errID] = expectations.expectFinalizersRemoved(logger, track.job, track.secondRound)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// 检查第二批插入无错误，且旧 UID 已被消费、新 UID 完整写入。
	for i, err := range errs {
		if err != nil {
			t.Errorf("Failed adding second round of UIDs for job %s: %v", tracks[i].job, err)
		}
	}

	for _, track := range tracks {
		uids := expectations.getSet(track.job)
		if uids == nil {
			t.Errorf("Set of UIDs is empty for job %s", track.job)
		} else if diff := cmp.Diff(track.secondRound, sets.List(uids.set)); diff != "" {
			t.Errorf("Unexpected keys for job %s (-want,+got):\n%s", track.job, diff)
		}
	}
	// 清理期望后必须彻底删除对应条目，避免污染后续 reconcile。
	for _, track := range tracks {
		expectations.deleteExpectations(logger, track.job)
		uids := expectations.getSet(track.job)
		if uids != nil {
			t.Errorf("Wanted expectations for job %s to be cleared, but they were not", track.job)
		}
	}
}

// TestRecordFinishedPodWithTrackingFinalizer 验证 finishing Pod 带/不带 tracking finalizer
// 时指标的记录语义：进入终态记 add，finalizer 被移除记 delete，其余情况不记录。
func TestRecordFinishedPodWithTrackingFinalizer(t *testing.T) {
	metrics.Register()
	cases := map[string]struct {
		oldPod *v1.Pod // 更新前 Pod
		newPod *v1.Pod // 更新后 Pod
		want   string  // 期望的指标输出，空串表示不应记录
	}{
		"new non-finished Pod with finalizer": {
			newPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			},
			want: "",
		},
		"pod with finalizer fails": {
			oldPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
				},
			},
			want: `# HELP job_controller_terminated_pods_tracking_finalizer_total [BETA] The number of terminated pods (phase=Failed|Succeeded)\nthat have the finalizer batch.kubernetes.io/job-tracking\nThe event label can be "add" or "delete".
# TYPE job_controller_terminated_pods_tracking_finalizer_total counter
job_controller_terminated_pods_tracking_finalizer_total{event="add"} 1
`,
		},
		"pod with finalizer succeeds": {
			oldPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				},
			},
			want: `# HELP job_controller_terminated_pods_tracking_finalizer_total [BETA] The number of terminated pods (phase=Failed|Succeeded)\nthat have the finalizer batch.kubernetes.io/job-tracking\nThe event label can be "add" or "delete".
# TYPE job_controller_terminated_pods_tracking_finalizer_total counter
job_controller_terminated_pods_tracking_finalizer_total{event="add"} 1
`,
		},
		"succeeded pod loses finalizer": {
			oldPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{batch.JobTrackingFinalizer},
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				},
			},
			newPod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				},
			},
			want: `# HELP job_controller_terminated_pods_tracking_finalizer_total [BETA] The number of terminated pods (phase=Failed|Succeeded)\nthat have the finalizer batch.kubernetes.io/job-tracking\nThe event label can be "add" or "delete".
# TYPE job_controller_terminated_pods_tracking_finalizer_total counter
job_controller_terminated_pods_tracking_finalizer_total{event="delete"} 1
`,
		},
		"pod without finalizer removed": {
			oldPod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				},
			},
			want: "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// 每个子用例重置计数器，避免相互干扰；断言精确匹配指标文本。
			metrics.TerminatedPodsTrackingFinalizerTotal.Reset()
			recordFinishedPodWithTrackingFinalizer(tc.oldPod, tc.newPod)
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.want), "job_controller_terminated_pods_tracking_finalizer_total"); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestRecordJobPodFailurePolicyActions 验证失败策略动作计数指标按 action 标签分组，
// 且 nil 输入不产生任何指标。
func TestRecordJobPodFailurePolicyActions(t *testing.T) {
	metrics.Register()

	cases := map[string]struct {
		input map[string]int // 动作名到发生次数的映射
		want  string         // 期望的指标输出
	}{
		"no actions": {
			input: nil,
			want:  "",
		},
		"single action": {
			input: map[string]int{
				"FailJob": 2,
			},
			want: `# HELP job_controller_pod_failures_handled_by_failure_policy_total [BETA] The number of failed Pods handled by failure policy with\n			respect to the failure policy action applied based on the matched\n			rule. Possible values of the action label correspond to the\n			possible values for the failure policy rule action, which are:\n			"FailJob", "Ignore" and "Count".
# TYPE job_controller_pod_failures_handled_by_failure_policy_total counter
job_controller_pod_failures_handled_by_failure_policy_total{action="FailJob"} 2
`,
		},
		"multiple actions": {
			input: map[string]int{
				"FailJob": 2,
				"Ignore":  3,
				"Count":   1,
			},
			want: `# HELP job_controller_pod_failures_handled_by_failure_policy_total [BETA] The number of failed Pods handled by failure policy with\n			respect to the failure policy action applied based on the matched\n			rule. Possible values of the action label correspond to the\n			possible values for the failure policy rule action, which are:\n			"FailJob", "Ignore" and "Count".
# TYPE job_controller_pod_failures_handled_by_failure_policy_total counter
job_controller_pod_failures_handled_by_failure_policy_total{action="Count"} 1
job_controller_pod_failures_handled_by_failure_policy_total{action="FailJob"} 2
job_controller_pod_failures_handled_by_failure_policy_total{action="Ignore"} 3
`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// 重置计数器后逐个断言指标输出，保证多动作场景的标签与数值都正确。
			metrics.PodFailuresHandledByFailurePolicy.Reset()
			recordJobPodFailurePolicyActions(tc.input)
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.want), "job_controller_pod_failures_handled_by_failure_policy_total"); err != nil {
				t.Error(err)
			}
		})
	}
}
