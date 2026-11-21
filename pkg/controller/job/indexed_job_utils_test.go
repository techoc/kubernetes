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
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/ptr"
)

// noIndex 在测试用例中表示 Pod 无完成索引，用于构造无索引 Pod 的场景。
const noIndex = "-"

// TestCalculateSucceededIndexes 以表驱动方式验证成功索引区间的计算：
//   - prevSucceeded 是状态中已有的完成索引字符串，wantStatusIntervals 是解析后的旧列表；
//   - pods 是本次同步的 Pod 列表（索引+相位），wantIntervals 是期望的合并后新列表；
//   - 覆盖越界索引、负数索引、无索引、损坏字符串以及新旧区间交错的情形。
func TestCalculateSucceededIndexes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：prevSucceeded 为状态中已有索引；pods 为输入 Pod 列表；completions 为完成总数；
	// wantStatusIntervals 为期望的旧列表；wantIntervals 为期望的新列表。
	cases := map[string]struct {
		prevSucceeded       string
		pods                []indexPhase
		completions         int32
		wantStatusIntervals orderedIntervals
		wantIntervals       orderedIntervals
	}{
		// 单个成功 Pod 应生成单点区间。
		"one index": {
			pods:          []indexPhase{{"1", v1.PodSucceeded}},
			completions:   2,
			wantIntervals: []interval{{1, 1}},
		},
		// 重复的成功索引应去重，失败 Pod 不应计入。
		"two separate": {
			pods: []indexPhase{
				{"2", v1.PodFailed},
				{"5", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{"10", v1.PodFailed},
				{"10", v1.PodSucceeded},
			},
			completions:   11,
			wantIntervals: []interval{{5, 5}, {10, 10}},
		},
		// 连续的成功索引应合并为区间，非成功相位不计入。
		"two intervals": {
			pods: []indexPhase{
				{"0", v1.PodRunning},
				{"1", v1.PodPending},
				{"2", v1.PodSucceeded},
				{"3", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{"6", v1.PodSucceeded},
				{"7", v1.PodSucceeded},
			},
			completions:   8,
			wantIntervals: []interval{{2, 3}, {5, 7}},
		},
		// 同时包含单点、区间与非法索引（noIndex、"-2"），仅合法成功索引被纳入。
		"one index and one interval": {
			pods: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodFailed},
				{"2", v1.PodSucceeded},
				{"3", v1.PodSucceeded},
				{"4", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{noIndex, v1.PodSucceeded},
				{"-2", v1.PodSucceeded},
			},
			completions:   6,
			wantIntervals: []interval{{0, 0}, {2, 5}},
		},
		// completions 缩小后，越界索引（含无索引、负数）必须被过滤。
		"out of range": {
			pods: []indexPhase{
				{"0", v1.PodSucceeded},
				{"1", v1.PodSucceeded},
				{"2", v1.PodSucceeded},
				{"3", v1.PodFailed},
				{"4", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{noIndex, v1.PodSucceeded},
				{"-2", v1.PodSucceeded},
			},
			completions:   5,
			wantIntervals: []interval{{0, 2}, {4, 4}},
		},
		// 状态中的区间整体越界：解析时被截断到 completions 范围内。
		"prev interval out of range": {
			prevSucceeded:       "0-5,8-10",
			completions:         8,
			wantStatusIntervals: []interval{{0, 5}},
			wantIntervals:       []interval{{0, 5}},
		},
		// 状态中的区间部分越界：只保留合法部分。
		"prev interval partially out of range": {
			prevSucceeded:       "0-5,8-10",
			completions:         10,
			wantStatusIntervals: []interval{{0, 5}, {8, 9}},
			wantIntervals:       []interval{{0, 5}, {8, 9}},
		},
		// 新旧区间彼此分离时，不应错误合并。
		"prev and new separate": {
			prevSucceeded: "0,4,5,10-12",
			pods: []indexPhase{
				{"2", v1.PodSucceeded},
				{"7", v1.PodSucceeded},
				{"8", v1.PodSucceeded},
			},
			completions: 13,
			wantStatusIntervals: []interval{
				{0, 0},
				{4, 5},
				{10, 12},
			},
			wantIntervals: []interval{
				{0, 0},
				{2, 2},
				{4, 5},
				{7, 8},
				{10, 12},
			},
		},
		// 新区间位于旧区间之间时，应正确合并相邻部分。
		"prev between new": {
			prevSucceeded: "3,4,6",
			pods: []indexPhase{
				{"2", v1.PodSucceeded},
				{"7", v1.PodSucceeded},
				{"8", v1.PodSucceeded},
			},
			completions: 9,
			wantStatusIntervals: []interval{
				{3, 4},
				{6, 6},
			},
			wantIntervals: []interval{
				{2, 4},
				{6, 8},
			},
		},
		// 新索引落在旧区间内部时，不应扩大旧区间之外。
		"new between prev": {
			prevSucceeded: "2,7,8",
			pods: []indexPhase{
				{"3", v1.PodSucceeded},
				{"4", v1.PodSucceeded},
				{"6", v1.PodSucceeded},
			},
			completions: 9,
			wantStatusIntervals: []interval{
				{2, 2},
				{7, 8},
			},
			wantIntervals: []interval{
				{2, 4},
				{6, 8},
			},
		},
		// 新区间完全包含在旧区间内：结果保持旧区间，另加独立的新区间。
		"new within prev": {
			prevSucceeded: "2-7",
			pods: []indexPhase{
				{"0", v1.PodSucceeded},
				{"3", v1.PodSucceeded},
				{"5", v1.PodSucceeded},
				{"9", v1.PodSucceeded},
			},
			completions: 10,
			wantStatusIntervals: []interval{
				{2, 7},
			},
			wantIntervals: []interval{
				{0, 0},
				{2, 7},
				{9, 9},
			},
		},
		// 状态字符串损坏的区间应被忽略，不影响合法区间与新增索引。
		"corrupted interval": {
			prevSucceeded: "0,1-foo,bar",
			pods: []indexPhase{
				{"3", v1.PodSucceeded},
			},
			completions: 4,
			wantStatusIntervals: []interval{
				{0, 0},
			},
			wantIntervals: []interval{
				{0, 0},
				{3, 3},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := &batch.Job{
				Status: batch.JobStatus{
					CompletedIndexes: tc.prevSucceeded,
				},
				Spec: batch.JobSpec{
					Completions: ptr.To(tc.completions),
				},
			}
			pods := hollowPodsWithIndexPhase(tc.pods)
			// 计算前给每个 Pod 加上 tracking finalizer，模拟“尚未计入状态”的 Pod。
			for _, p := range pods {
				p.Finalizers = append(p.Finalizers, batch.JobTrackingFinalizer)
			}
			gotStatusIntervals, gotIntervals := calculateSucceededIndexes(logger, job, pods)
			if diff := cmp.Diff(tc.wantStatusIntervals, gotStatusIntervals); diff != "" {
				t.Errorf("Unexpected completed indexes from status (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantIntervals, gotIntervals); diff != "" {
				t.Errorf("Unexpected completed indexes (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestIsIndexFailed 验证索引失败判定：
//   - 失败次数达到 BackoffLimitPerIndex 时判定索引失败；
//   - 命中 FailIndex 策略时无论次数均判定失败。
func TestIsIndexFailed(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：job 提供 Job 配置（含 BackoffLimitPerIndex 与失败策略）；pod 为候选失败 Pod；wantResult 为期望判定。
	cases := map[string]struct {
		job        batch.Job
		pod        *v1.Pod
		wantResult bool
	}{
		// BackoffLimitPerIndex=0 时，失败一次即达到上限。
		"failed pod exceeding backoffLimitPerIndex, when backoffLimitPerIndex=0": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pod:        buildPod().indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
			wantResult: true,
		},
		// BackoffLimitPerIndex=1 时，失败计数为 1 才判定失败。
		"failed pod exceeding backoffLimitPerIndex, when backoffLimitPerIndex=1": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pod:        buildPod().indexFailureCount("1").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
			wantResult: true,
		},
		// 退出码命中 FailIndex 规则：即使次数未达上限也应判定失败。
		"matching FailIndex pod failure policy": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
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
			pod: buildPod().indexFailureCount("0").status(v1.PodStatus{
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
			}).index("0").trackingFinalizer().Pod,
			wantResult: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotResult := isIndexFailed(logger, &tc.job, tc.pod)
			if diff := cmp.Diff(tc.wantResult, gotResult); diff != "" {
				t.Errorf("Unexpected result (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestCalculateFailedIndexes 验证失败索引区间计算：
//   - 仅有带 tracking finalizer、索引合法且失败次数达上限的 Pod 才计入；
//   - 状态中已有的失败区间会被保留并正确扩展。
func TestCalculateFailedIndexes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：job 提供状态与配置；pods 为输入 Pod；两个 want 字段分别校验旧列表与新列表。
	cases := map[string]struct {
		job                   batch.Job
		pods                  []*v1.Pod
		wantPrevFailedIndexes orderedIntervals
		wantFailedIndexes     orderedIntervals
	}{
		// 失败计数等于上限的索引被计入，未达上限的不计入。
		"one new index failed": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().indexFailureCount("1").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
			},
			wantFailedIndexes: []interval{{1, 1}},
		},
		// 无 finalizer 的 Pod 已被计数过，应忽略。
		"pod without finalizer is ignored": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("0").Pod,
			},
			wantFailedIndexes: nil,
		},
		// 索引超出 completions 的 Pod 应忽略。
		"pod outside completions is ignored": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("3").Pod,
			},
			wantFailedIndexes: nil,
		},
		// 新失败索引扩展状态中已有的失败区间。
		"extend the failed indexes": {
			job: batch.Job{
				Status: batch.JobStatus{
					FailedIndexes: ptr.To("0"),
				},
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
			},
			wantFailedIndexes: []interval{{0, 1}},
		},
		// 状态中失败区间为空字符串时，应视为无历史记录。
		"prev failed indexes empty": {
			job: batch.Job{
				Status: batch.JobStatus{
					FailedIndexes: ptr.To(""),
				},
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
			},
			wantFailedIndexes: []interval{{1, 1}},
		},
		// 状态中越界的失败索引应被丢弃。
		"prev failed indexes outside the completions": {
			job: batch.Job{
				Status: batch.JobStatus{
					FailedIndexes: ptr.To("9"),
				},
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().indexFailureCount("0").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
			},
			wantFailedIndexes: []interval{{1, 1}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			failedIndexes := calculateFailedIndexes(logger, &tc.job, tc.pods)
			if diff := cmp.Diff(&tc.wantFailedIndexes, failedIndexes); diff != "" {
				t.Errorf("Unexpected failed indexes (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestGetPodsWithDelayedDeletionPerIndex 验证按索引延迟删除的 Pod 选择：
//   - 只有未被替换、索引有效且带 finalizer 的失败 Pod 才延迟删除；
//   - 同一索引多个候选时选失败计数最大、完成时间最晚的那个。
func TestGetPodsWithDelayedDeletionPerIndex(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	now := time.Now()
	// 表驱动用例字段：job/pods 为输入；expectedRmFinalizers 为期望立即移除 finalizer 的 Pod；
	// wantPodsWithDelayedDeletionPerIndex 为期望延迟删除的 Pod UID 列表。
	cases := map[string]struct {
		job                                 batch.Job
		pods                                []*v1.Pod
		expectedRmFinalizers                sets.Set[types.UID]
		wantPodsWithDelayedDeletionPerIndex []string
	}{
		// 已成功/已失败索引中的 Pod 无需延迟，索引未失败的 Pod 需保留。
		"failed pods are kept corresponding to non-failed indexes are kept": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](3),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().uid("b").indexFailureCount("1").phase(v1.PodFailed).index("1").trackingFinalizer().Pod,
				buildPod().uid("c").indexFailureCount("0").phase(v1.PodFailed).index("2").trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{"a", "c"},
		},
		// 无 finalizer 说明删除已开始，无需延迟。
		"failed pod without finalizer; the pod's deletion is not delayed as it already started": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").indexFailureCount("0").phase(v1.PodFailed).index("0").Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{},
		},
		// 已标记移除 finalizer 的 Pod，删除已开始，无需延迟。
		"failed pod with expected finalizer removal; the pod's deletion is not delayed as it already started": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
			},
			expectedRmFinalizers:                sets.New[types.UID]("a"),
			wantPodsWithDelayedDeletionPerIndex: []string{},
		},
		// 索引越界的失败 Pod 不参与延迟删除逻辑。
		"failed pod with index outside of completions; the pod's deletion is not delayed": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](0),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a").indexFailureCount("0").phase(v1.PodFailed).index("4").trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{},
		},
		// 已有活跃 Pod 替换该索引，无需延迟删除失败 Pod。
		"failed pod for active index; the pod's deletion is not delayed as it is already replaced": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a1").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().uid("a2").indexFailureCount("1").phase(v1.PodRunning).index("0").trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{},
		},
		// 该索引已有成功 Pod，无需延迟删除失败 Pod。
		"failed pod for succeeded index; the pod's deletion is not delayed as it is already replaced": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](1),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a1").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().uid("a2").indexFailureCount("1").phase(v1.PodSucceeded).index("0").trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{},
		},
		// 多个失败 Pod 时，仅保留失败计数最高的那个。
		"multiple failed pods for index with different failure count; only the pod with highest failure count is kept": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](4),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a1").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().uid("a3").indexFailureCount("2").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
				buildPod().uid("a2").indexFailureCount("1").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{"a3"},
		},
		// 失败计数相同时，保留完成（删除）时间最晚的那个。
		"multiple failed pods for index with different finish times; only the last failed pod is kept": {
			job: batch.Job{
				Spec: batch.JobSpec{
					Completions:          ptr.To[int32](2),
					BackoffLimitPerIndex: ptr.To[int32](4),
				},
			},
			pods: []*v1.Pod{
				buildPod().uid("a1").indexFailureCount("1").phase(v1.PodFailed).index("0").customDeletionTimestamp(now.Add(-time.Second)).trackingFinalizer().Pod,
				buildPod().uid("a3").indexFailureCount("1").phase(v1.PodFailed).index("0").customDeletionTimestamp(now).trackingFinalizer().Pod,
				buildPod().uid("a2").indexFailureCount("1").phase(v1.PodFailed).index("0").customDeletionTimestamp(now.Add(-2 * time.Second)).trackingFinalizer().Pod,
			},
			wantPodsWithDelayedDeletionPerIndex: []string{"a3"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			activePods := controller.FilterActivePods(logger, tc.pods)
			failedIndexes := calculateFailedIndexes(logger, &tc.job, tc.pods)
			_, succeededIndexes := calculateSucceededIndexes(logger, &tc.job, tc.pods)
			jobCtx := &syncJobCtx{
				job:                  &tc.job,
				pods:                 tc.pods,
				activePods:           activePods,
				succeededIndexes:     succeededIndexes,
				failedIndexes:        failedIndexes,
				expectedRmFinalizers: tc.expectedRmFinalizers,
			}
			gotPodsWithDelayedDeletionPerIndex := getPodsWithDelayedDeletionPerIndex(logger, jobCtx)
			gotPodsWithDelayedDeletionPerIndexSet := sets.New[string]()
			// 结果以 map 返回，转成集合后比较，避免依赖遍历顺序。
			for _, pod := range gotPodsWithDelayedDeletionPerIndex {
				gotPodsWithDelayedDeletionPerIndexSet.Insert(string(pod.UID))
			}
			if diff := cmp.Diff(tc.wantPodsWithDelayedDeletionPerIndex, sets.List(gotPodsWithDelayedDeletionPerIndexSet)); diff != "" {
				t.Errorf("Unexpected set of pods with delayed deletion (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestGetNewIndexFailureCountValue 验证替换 Pod 的失败计数继承：
//   - 无被替换 Pod 时计数从 0 开始；
//   - 命中 Ignore 策略时递增忽略计数而非失败计数。
func TestGetNewIndexFailureCountValue(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：job 提供失败策略；pod 为被替换 Pod；两个 want 字段分别为期望的失败/忽略计数。
	cases := map[string]struct {
		job                             batch.Job
		pod                             *v1.Pod
		wantNewIndexFailureCount        int32
		wantNewIndexIgnoredFailureCount int32
	}{
		// 首次创建 Pod 时没有任何前序失败计数。
		"first pod created": {
			job:                      batch.Job{},
			wantNewIndexFailureCount: 0,
		},
		// 继承被替换 Pod 的 0 计数后再加 1。
		"failed pod being replaced with 0 index failure count": {
			job:                      batch.Job{},
			pod:                      buildPod().uid("a").indexFailureCount("0").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
			wantNewIndexFailureCount: 1,
		},
		// 继承被替换 Pod 的计数 3 后递增为 4。
		"failed pod being replaced with >0 index failure count": {
			job:                      batch.Job{},
			pod:                      buildPod().uid("a").indexFailureCount("3").phase(v1.PodFailed).index("0").trackingFinalizer().Pod,
			wantNewIndexFailureCount: 4,
		},
		// 命中 Ignore 策略：失败计数保持 3，忽略计数加 1。
		"failed pod being replaced, matching the ignore rule": {
			job: batch.Job{
				Spec: batch.JobSpec{
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
			pod: buildPod().uid("a").indexFailureCount("3").status(v1.PodStatus{
				Phase: v1.PodFailed,
				Conditions: []v1.PodCondition{
					{
						Type:   v1.DisruptionTarget,
						Status: v1.ConditionTrue,
					},
				},
			}).index("3").trackingFinalizer().Pod,
			wantNewIndexFailureCount:        3,
			wantNewIndexIgnoredFailureCount: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotNewIndexFailureCount, gotNewIndexIgnoredFailureCount := getNewIndexFailureCounts(logger, &tc.job, tc.pod)
			if diff := cmp.Diff(tc.wantNewIndexFailureCount, gotNewIndexFailureCount); diff != "" {
				t.Errorf("Unexpected set of pods with delayed deletion (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantNewIndexIgnoredFailureCount, gotNewIndexIgnoredFailureCount); diff != "" {
				t.Errorf("Unexpected set of pods with delayed deletion (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestIntervalsHaveIndex 验证 has 的二分查找覆盖空列表、全部之前/之后、区间间隙以及命中两个区间的场景。
func TestIntervalsHaveIndex(t *testing.T) {
	// 表驱动用例字段：intervals 为有序区间列表；index 为待查索引；wantHas 为期望的覆盖结果。
	cases := map[string]struct {
		intervals orderedIntervals
		index     int
		wantHas   bool
	}{
		// 空列表必然不包含任何索引。
		"empty": {
			index: 4,
		},
		// 索引在所有区间之前。
		"before all": {
			index:     1,
			intervals: []interval{{2, 4}, {5, 7}},
		},
		// 索引在所有区间之后。
		"after all": {
			index:     9,
			intervals: []interval{{2, 4}, {6, 8}},
		},
		// 索引落在两个区间之间。
		"in between": {
			index:     5,
			intervals: []interval{{2, 4}, {6, 8}},
		},
		// 索引命中第一个区间的边界。
		"in first": {
			index:     2,
			intervals: []interval{{2, 4}, {6, 8}},
			wantHas:   true,
		},
		// 索引命中第二个区间的边界。
		"in second": {
			index:     8,
			intervals: []interval{{2, 4}, {6, 8}},
			wantHas:   true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			has := tc.intervals.has(tc.index)
			if has != tc.wantHas {
				t.Errorf("intervalsHaveIndex(_, _) = %t, want %t", has, tc.wantHas)
			}
		})
	}
}

// TestFirstPendingIndexes 验证待处理索引的选择：
//   - 跳过活跃 Pod、已成功、已失败索引占用的编号；
//   - 请求数量超过可用数量时只返回合法范围内的索引。
func TestFirstPendingIndexes(t *testing.T) {
	// 表驱动用例字段：cnt 为请求数量；completions 为总数；activePods/succeededIndexes/failedIndexes 为已占用索引；want 为期望结果。
	cases := map[string]struct {
		cnt              int
		completions      int
		activePods       []indexPhase
		succeededIndexes []interval
		failedIndexes    *orderedIntervals
		want             []int
	}{
		// 请求数超过完成总数时，只返回所有合法索引。
		"cnt greater than completions": {
			cnt:         5,
			completions: 3,
			want:        []int{0, 1, 2},
		},
		// 请求数小于可用数时按升序返回前几个。
		"cnt less than completions": {
			cnt:         2,
			completions: 5,
			want:        []int{0, 1},
		},
		// 前面索引被活跃 Pod 占用，应从空闲处开始分配。
		"first pods active": {
			activePods: []indexPhase{
				{"0", v1.PodRunning},
				{"1", v1.PodPending},
			},
			cnt:         3,
			completions: 10,
			want:        []int{2, 3, 4},
		},
		// 尾部索引被占用/成功时，只返回前面的空闲索引。
		"last pods active or succeeded": {
			activePods: []indexPhase{
				{"6", v1.PodPending},
			},
			succeededIndexes: []interval{{4, 5}},
			cnt:              6,
			completions:      6,
			want:             []int{0, 1, 2, 3},
		},
		// 混合活跃、成功、无索引与负数索引的复杂场景。
		"mixed": {
			activePods: []indexPhase{
				{"3", v1.PodPending},
				{"5", v1.PodRunning},
				{"8", v1.PodPending},
				{noIndex, v1.PodRunning},
				{"-3", v1.PodRunning},
			},
			succeededIndexes: []interval{{2, 4}, {9, 9}},
			cnt:              5,
			completions:      20,
			want:             []int{0, 1, 6, 7, 10},
		},
		// 同时排除活跃、成功与失败索引。
		"with failed indexes": {
			activePods: []indexPhase{
				{"3", v1.PodPending},
				{"9", v1.PodPending},
			},
			succeededIndexes: []interval{{1, 1}, {5, 5}, {9, 9}},
			failedIndexes:    &orderedIntervals{{2, 2}, {6, 7}},
			cnt:              5,
			completions:      20,
			want:             []int{0, 4, 8, 10, 11},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			jobCtx := &syncJobCtx{
				activePods:       hollowPodsWithIndexPhase(tc.activePods),
				succeededIndexes: tc.succeededIndexes,
				failedIndexes:    tc.failedIndexes,
				job:              newJob(1, 1, 1, batch.IndexedCompletion),
			}
			got := firstPendingIndexes(jobCtx, tc.cnt, tc.completions)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Wrong first pending indexes (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestAppendDuplicatedIndexPodsForRemoval 验证重复索引 Pod 的清理：
//   - 每个索引只保留一个 Pod，其余放入待删除列表；
//   - 无索引、非数字索引、越界索引的 Pod 全部放入待删除列表。
func TestAppendDuplicatedIndexPodsForRemoval(t *testing.T) {
	// 表驱动用例字段：pods 为输入；wantRm 为期望删除列表；wantLeft 为期望保留列表；completions 为合法索引上限。
	cases := map[string]struct {
		pods        []indexPhase
		wantRm      []indexPhase
		wantLeft    []indexPhase
		completions int32
	}{
		// 全部唯一时，无索引和越界（索引 6 == completions）的 Pod 仍被删除。
		"all unique": {
			pods: []indexPhase{
				{noIndex, v1.PodPending},
				{"2", v1.PodPending},
				{"5", v1.PodRunning},
				{"6", v1.PodRunning},
			},
			wantRm: []indexPhase{
				{noIndex, v1.PodPending},
				{"6", v1.PodRunning},
			},
			wantLeft: []indexPhase{
				{"2", v1.PodPending},
				{"5", v1.PodRunning},
			},
			completions: 6,
		},
		// 多个重复索引：每个索引保留一个，越界的 6 被删除。
		"all with index": {
			pods: []indexPhase{
				{"5", v1.PodPending},
				{"0", v1.PodRunning},
				{"3", v1.PodPending},
				{"0", v1.PodRunning},
				{"3", v1.PodRunning},
				{"0", v1.PodPending},
				{"6", v1.PodRunning},
				{"6", v1.PodPending},
			},
			wantRm: []indexPhase{
				{"0", v1.PodPending},
				{"0", v1.PodRunning},
				{"3", v1.PodPending},
				{"6", v1.PodRunning},
				{"6", v1.PodPending},
			},
			wantLeft: []indexPhase{
				{"0", v1.PodRunning},
				{"3", v1.PodRunning},
				{"5", v1.PodPending},
			},
			completions: 6,
		},
		// 混合非法索引与重复索引：非法索引全删，重复索引保留最新的一个。
		"mixed": {
			pods: []indexPhase{
				{noIndex, v1.PodPending},
				{"invalid", v1.PodRunning},
				{"-2", v1.PodRunning},
				{"0", v1.PodPending},
				{"1", v1.PodPending},
				{"1", v1.PodPending},
				{"1", v1.PodRunning},
			},
			wantRm: []indexPhase{
				{noIndex, v1.PodPending},
				{"invalid", v1.PodRunning},
				{"-2", v1.PodRunning},
				{"1", v1.PodPending},
				{"1", v1.PodPending},
			},
			wantLeft: []indexPhase{
				{"0", v1.PodPending},
				{"1", v1.PodRunning},
			},
			completions: 6,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pods := hollowPodsWithIndexPhase(tc.pods)
			rm, left := appendDuplicatedIndexPodsForRemoval(nil, nil, pods, int(tc.completions))
			rmInt := toIndexPhases(rm)
			leftInt := toIndexPhases(left)
			if diff := cmp.Diff(tc.wantRm, rmInt); diff != "" {
				t.Errorf("Unexpected pods for removal (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantLeft, leftInt); diff != "" {
				t.Errorf("Unexpected pods to keep (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPodGenerateNameWithIndex 验证带索引的 Pod name 前缀生成：
//   - 短名字直接追加 "-<index>-"；
//   - 超长名字截断时保留索引后缀。
func TestPodGenerateNameWithIndex(t *testing.T) {
	// 表驱动用例字段：jobname 为 Job 名；index 为完成索引；wantPodGenerateName 为期望的 name 前缀。
	cases := map[string]struct {
		jobname             string
		index               int
		wantPodGenerateName string
	}{
		// 短名字直接拼接索引后缀。
		"short job name": {
			jobname:             "indexed-job",
			index:               1,
			wantPodGenerateName: "indexed-job-1-",
		},
		// Job 名本身超长，必须截断并保留索引后缀。
		"job name exceeds MaxGeneneratedNameLength": {
			jobname:             "hhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooo",
			index:               1,
			wantPodGenerateName: "hhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhh-1-",
		},
		// 追加索引后才超长，同样保留索引后缀。
		"job name with index suffix exceeds MaxGeneratedNameLength": {
			jobname:             "hhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhoo",
			index:               1,
			wantPodGenerateName: "hhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhhooooohhhhh-1-",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			podGenerateName := podGenerateNameWithIndex(tc.jobname, tc.index)
			if diff := cmp.Equal(tc.wantPodGenerateName, podGenerateName); !diff {
				t.Errorf("Got pod generateName %s, want %s", podGenerateName, tc.wantPodGenerateName)
			}
		})
	}
}

// TestGetIndexFailureCount 验证失败计数注解的解析：
//   - 合法值（含 int32 最大值）正常返回；
//   - 缺失、越界、负数或非数字注解均回退为 0。
func TestGetIndexFailureCount(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 表驱动用例字段：pod 为带注解的 Pod；wantResult 为期望解析结果。
	cases := map[string]struct {
		pod        *v1.Pod
		wantResult int32
	}{
		// 注解缺失时返回 0。
		"no annotation": {
			pod:        &v1.Pod{},
			wantResult: 0,
		},
		// 合法正整数正常返回。
		"valid value": {
			pod:        buildPod().indexFailureCount("2").Pod,
			wantResult: 2,
		},
		// int32 最大值是合法上界。
		"valid maxint32 value": {
			pod:        buildPod().indexFailureCount(strconv.FormatInt(math.MaxInt32, 10)).Pod,
			wantResult: math.MaxInt32,
		},
		// 超过 int32 范围的值被当作无效，返回 0。
		"too large value": {
			pod:        buildPod().indexFailureCount(strconv.FormatInt(math.MaxInt32+1, 10)).Pod,
			wantResult: 0,
		},
		// 负数注解返回 0。
		"negative value": {
			pod:        buildPod().indexFailureCount("-1").Pod,
			wantResult: 0,
		},
		// 非数字注解返回 0。
		"invalid int value": {
			pod:        buildPod().indexFailureCount("xyz").Pod,
			wantResult: 0,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotResult := getIndexFailureCount(logger, tc.pod)
			if diff := cmp.Equal(tc.wantResult, gotResult); !diff {
				t.Errorf("Unexpected result. want: %d, got: %d", tc.wantResult, gotResult)
			}
		})
	}
}

// hollowPodsWithIndexPhase 根据索引与相位描述构造最小化的 Pod 对象，
// 仅设置测试所需的状态与完成索引注解（noIndex 时不设置注解）。
func hollowPodsWithIndexPhase(descs []indexPhase) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(descs))
	for _, desc := range descs {
		p := &v1.Pod{
			Status: v1.PodStatus{
				Phase: desc.Phase,
			},
		}
		if desc.Index != noIndex {
			p.Annotations = map[string]string{
				batch.JobCompletionIndexAnnotation: desc.Index,
			}
		}
		pods = append(pods, p)
	}
	return pods
}

// indexPhase 是测试用例中 Pod 的简化描述：Index 为完成索引字符串，Phase 为 Pod 相位。
type indexPhase struct {
	// Index 是完成索引字符串；noIndex 表示无索引。
	Index string
	// Phase 是 Pod 的生命周期相位。
	Phase v1.PodPhase
}

// toIndexPhases 将 Pod 列表转换为 indexPhase 列表，便于与期望值比较；
// 无完成索引注解时记为 noIndex。
func toIndexPhases(pods []*v1.Pod) []indexPhase {
	result := make([]indexPhase, len(pods))
	for i, p := range pods {
		// 无索引注解时统一记为 noIndex，便于比较。
		index := noIndex
		if p.Annotations != nil {
			index = p.Annotations[batch.JobCompletionIndexAnnotation]
		}
		result[i] = indexPhase{index, p.Status.Phase}
	}
	return result
}
