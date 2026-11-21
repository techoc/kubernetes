/*
Copyright 2018 The Kubernetes Authors.

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

package ttlafterfinished

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// newJob 构造测试用 Job：按传入的完成/失败时间填入对应状态条件，并设置 TTL。
// 使用零值时间表示“没有该状态”，便于覆盖未完成、已完成和已失败等场景。
func newJob(completionTime, failedTime metav1.Time, ttl *int32) *batchv1.Job {
	j := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foobar",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: batchv1.JobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"foo": "bar"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Image: "foo/bar"},
					},
				},
			},
		},
	}

	// 只有非零时间才写入完成条件：控制器依据该条件判断 Job 是否终止。
	if !completionTime.IsZero() {
		c := batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: completionTime}
		j.Status.Conditions = append(j.Status.Conditions, c)
	}

	// 失败条件与完成条件分别构造，覆盖 TTL 对两类终止状态的清理逻辑。
	if !failedTime.IsZero() {
		c := batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failedTime}
		j.Status.Conditions = append(j.Status.Conditions, c)
	}

	// nil 表示未配置 TTL，此时 Job 不应被清理。
	if ttl != nil {
		j.Spec.TTLSecondsAfterFinished = ttl
	}

	return j
}

// TestTimeLeft 验证 timeLeft 能正确计算距离清理的剩余时间与过期时刻，
// 并覆盖未终止、未配置 TTL 等应返回错误的场景。
func TestTimeLeft(t *testing.T) {
	now := metav1.Now()

	testCases := []struct {
		name             string
		completionTime   metav1.Time
		failedTime       metav1.Time
		ttl              *int32
		since            *time.Time
		expectErr        bool
		expectErrStr     string
		expectedTimeLeft *time.Duration
		expectedExpireAt time.Time
	}{
		{
			name:         "Error case: Job unfinished",
			ttl:          ptr.To[int32](100),
			since:        &now.Time,
			expectErr:    true,
			expectErrStr: "should not be cleaned up",
		},
		{
			name:           "Error case: Job completed now, no TTL",
			completionTime: now,
			since:          &now.Time,
			expectErr:      true,
			expectErrStr:   "should not be cleaned up",
		},
		{
			name:             "Job completed now, 0s TTL",
			completionTime:   now,
			ttl:              ptr.To[int32](0),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(0 * time.Second),
			expectedExpireAt: now.Time,
		},
		{
			name:             "Job completed now, 10s TTL",
			completionTime:   now,
			ttl:              ptr.To[int32](10),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(10 * time.Second),
			expectedExpireAt: now.Add(10 * time.Second),
		},
		{
			// 剩余时间应为 TTL 减去已流逝时间，同时验证过期时刻按绝对时间计算。
			name:             "Job completed 10s ago, 15s TTL",
			completionTime:   metav1.NewTime(now.Add(-10 * time.Second)),
			ttl:              ptr.To[int32](15),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(5 * time.Second),
			expectedExpireAt: now.Add(5 * time.Second),
		},
		{
			name:         "Error case: Job failed now, no TTL",
			failedTime:   now,
			since:        &now.Time,
			expectErr:    true,
			expectErrStr: "should not be cleaned up",
		},
		{
			name:             "Job failed now, 0s TTL",
			failedTime:       now,
			ttl:              ptr.To[int32](0),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(0 * time.Second),
			expectedExpireAt: now.Time,
		},
		{
			name:             "Job failed now, 10s TTL",
			failedTime:       now,
			ttl:              ptr.To[int32](10),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(10 * time.Second),
			expectedExpireAt: now.Add(10 * time.Second),
		},
		{
			// 失败 Job 的计时起点同样是失败时间，确保两类终止状态行为一致。
			name:             "Job failed 10s ago, 15s TTL",
			failedTime:       metav1.NewTime(now.Add(-10 * time.Second)),
			ttl:              ptr.To[int32](15),
			since:            &now.Time,
			expectedTimeLeft: ptr.To(5 * time.Second),
			expectedExpireAt: now.Add(5 * time.Second),
		},
	}

	for _, tc := range testCases {

		job := newJob(tc.completionTime, tc.failedTime, tc.ttl)
		_, ctx := ktesting.NewTestContext(t)
		logger := klog.FromContext(ctx)
		gotTimeLeft, gotExpireAt, gotErr := timeLeft(logger, job, tc.since)
		// 先核对错误的“有无”，再核对错误文案，避免对 nil 错误调用 Error() 而 panic。
		if tc.expectErr != (gotErr != nil) {
			t.Errorf("%s: expected error is %t, got %t, error: %v", tc.name, tc.expectErr, gotErr != nil, gotErr)
		}
		// 用例自身必须给出非空错误子串，否则下面的包含判断会形同虚设。
		if tc.expectErr && len(tc.expectErrStr) == 0 {
			t.Errorf("%s: invalid test setup; error message must not be empty for error cases", tc.name)
		}
		// 错误信息要能说明“为何不清理”，便于使用者诊断配置问题。
		if tc.expectErr && !strings.Contains(gotErr.Error(), tc.expectErrStr) {
			t.Errorf("%s: expected error message contains %q, got %v", tc.name, tc.expectErrStr, gotErr)
		}
		if !tc.expectErr {
			// 剩余时间决定入队的延迟，偏差会直接导致清理过早或过晚。
			if *gotTimeLeft != *tc.expectedTimeLeft {
				t.Errorf("%s: expected time left %v, got %v", tc.name, tc.expectedTimeLeft, gotTimeLeft)
			}
			// 过期时刻是控制器计算剩余时间的基准，必须按绝对时间精确校验。
			if *gotExpireAt != tc.expectedExpireAt {
				t.Errorf("%s: expected expire at %v, got %v", tc.name, tc.expectedExpireAt, *gotExpireAt)
			}
		}
	}
}
