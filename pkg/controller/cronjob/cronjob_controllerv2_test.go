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

package cronjob

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"

	_ "k8s.io/kubernetes/pkg/apis/batch/install"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller"
)

// 测试用固定参数：shortDead/mediumDead/longDead 分别覆盖不同时长的截止阈值，
// noDead 用负数作哨兵，表示不设置 StartingDeadlineSeconds。
// errorSchedule 与 errorTimeZone 故意非法，用于验证解析失败只产生警告而不崩溃。
var (
	shortDead  int64 = 10
	mediumDead int64 = 2 * 60 * 60
	longDead   int64 = 1000000
	noDead     int64 = -12345

	errorSchedule = "obvious error schedule"
	// 每小时整点触发一次的调度表达式。
	onTheHour = "0 * * * ?"
	everyHour = "@every 1h"

	// 非法时区名，用于触发时区解析失败。
	errorTimeZone = "bad timezone"
	// IANA 时区标识，用于验证时区调度场景。
	newYork = "America/New_York"
)

// cronJob 构造一个仅填充关键字段的 CronJob fixture：
// 创建时间设为整点前，便于覆盖“首次调度时间已到/未到”两类分支。
func cronJob() batchv1.CronJob {
	return batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "mycronjob",
			Namespace:         "snazzycats",
			UID:               types.UID("1a2b3c"),
			CreationTimestamp: metav1.Time{Time: justBeforeTheHour()},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          "* * * * ?",
			ConcurrencyPolicy: "Allow",
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"a": "b"},
					Annotations: map[string]string{"x": "y"},
				},
				Spec: jobSpec(),
			},
		},
	}
}

// jobSpec 返回最小可用的 JobSpec fixture：并行度与完成数均为 1，
// 镜像用占位值 foo/bar，测试只关心调度逻辑而非 Pod 内容。
func jobSpec() batchv1.JobSpec {
	one := int32(1)
	return batchv1.JobSpec{
		Parallelism: &one,
		Completions: &one,
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
	}
}

// justASecondBeforeTheHour 构造用于调度测试的时间点。
// justASecondBeforeTheHour 返回 09:59:59Z，用于验证“距整点仅差一秒”的边界。
func justASecondBeforeTheHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T09:59:59Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// justAfterThePriorHour 构造用于调度测试的时间点。
// justAfterThePriorHour 返回 09:01:00Z，即上一个整点刚过，
// 用作“上次调度之后”的固定参照时间。
func justAfterThePriorHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T09:01:00Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// justBeforeThePriorHour 构造用于调度测试的时间点。
// justBeforeThePriorHour 返回 08:59:00Z，即上一个整点之前，
// 用作需要更早创建时间时的固定参照。
func justBeforeThePriorHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T08:59:00Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// justAfterTheHour 构造用于调度测试的时间点。
// justAfterTheHour 返回 10:01:00Z 的指针，表示当前整点刚过，
// 返回指针便于直接赋给 LastScheduleTime 等指针字段。
func justAfterTheHour() *time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T10:01:00Z")
	if err != nil {
		panic("test setup error")
	}
	return &T1
}

// justAfterTheHourInZone 构造用于调度测试的时间点。
// justAfterTheHourInZone 按给定时区解析 10:01:00Z 对应的绝对时刻，
// 用于验证不同时区下“整点刚过”的判定一致。
func justAfterTheHourInZone(tz string) time.Time {
	location, err := time.LoadLocation(tz)
	if err != nil {
		panic("tz error: " + err.Error())
	}

	T1, err := time.ParseInLocation(time.RFC3339, "2016-05-19T10:01:00Z", location)
	if err != nil {
		panic("test setup error: " + err.Error())
	}
	return T1
}

// justBeforeTheHour 构造用于调度测试的时间点。
// justBeforeTheHour 返回 09:59:00Z，即整点前一分钟，
// 用作“尚未到调度时间”的默认 now。
func justBeforeTheHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T09:59:00Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// justBeforeTheNextHour 构造用于调度测试的时间点。
// justBeforeTheNextHour 返回 10:59:00Z，用于构造跨越整点的调度边界场景。
func justBeforeTheNextHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T10:59:00Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// weekAfterTheHour 构造用于调度测试的时间点。
// weekAfterTheHour 返回一周后的 10:00:00Z，
// 用于模拟长期未调度且已超过截止时间的场景。
func weekAfterTheHour() time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-26T10:00:00Z")
	if err != nil {
		panic("test setup error")
	}
	return T1
}

// TestControllerV2SyncCronJob 表驱动验证 syncCronJob 的核心行为：
// 非法 schedule/时区只告警、未到点按间隔重新入队、到点创建 Job（含三种并发策略）、
// 已完成任务的历史清理、命名空间终止时跳过创建，以及 LastSuccessfulTime 的写入。
func TestControllerV2SyncCronJob(t *testing.T) {
	// 先确认三个 deadline 常量落在预期量级，否则后续基于它们的断言失去意义。
	if shortDead/60/60 >= 1 {
		t.Errorf("shortDead should be less than one hour")
	}

	if mediumDead/60/60 < 1 || mediumDead/60/60 >= 24 {
		t.Errorf("mediumDead should be between one hour and one day")
	}

	if longDead/60/60/24 < 10 {
		t.Errorf("longDead should be at least ten days")
	}

	testCases := map[string]struct {
		// CronJob 的 spec 字段
		concurrencyPolicy          batchv1.ConcurrencyPolicy
		suspend                    bool
		schedule                   string
		timeZone                   *string
		deadline                   int64
		successfulJobsHistoryLimit *int32

		// CronJob 的 status 字段
		ranPreviously bool
		stillActive   bool

		// 环境与外部依赖（fake 客户端返回的错误、当前时间等）
		cronjobCreationTime time.Time
		jobCreationTime     time.Time
		lastScheduleTime    time.Time
		now                 time.Time
		jobCreateError      error
		jobGetErr           error

		// 期望结果（创建/删除、事件数、重排延迟等）
		expectCreate               bool
		expectDelete               bool
		expectCompleted            bool
		expectActive               int
		expectedWarnings           int
		expectErr                  bool
		expectRequeueAfter         bool
		expectedRequeueDuration    time.Duration
		expectUpdateStatus         bool
		jobStillNotFoundInLister   bool
		jobPresentInCJActiveStatus bool
	}{
		"never ran, not valid schedule, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   errorSchedule,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectedWarnings:           1,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not valid schedule, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   errorSchedule,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectedWarnings:           1,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not valid schedule, R": {
			concurrencyPolicy:          "Forbid",
			schedule:                   errorSchedule,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectedWarnings:           1,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not valid time zone": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			timeZone:                   &errorTimeZone,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectedWarnings:           1,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true},
		"never ran, not time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, not time in zone": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			timeZone:                   &newYork,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time in zone, but time zone disabled": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			timeZone:                   &newYork,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justAfterTheHourInZone(newYork),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time in zone": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			timeZone:                   &newYork,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justAfterTheHourInZone(newYork),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time in zone, but TZ is also set in schedule": {
			concurrencyPolicy:          "Allow",
			schedule:                   "TZ=UTC " + onTheHour,
			timeZone:                   &newYork,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justAfterTheHourInZone(newYork),
			expectCreate:               true,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, suspended": {
			concurrencyPolicy:          "Allow",
			suspend:                    true,
			schedule:                   onTheHour,
			deadline:                   noDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justAfterTheHour().Add(time.Minute * time.Duration(shortDead+1)),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute - time.Minute*time.Duration(shortDead+1) + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"never ran, is time, not past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   longDead,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},

		"prev ran but done, not time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, not time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, not time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, create job failed, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			jobCreateError:             errors.NewAlreadyExists(schema.GroupResource{Resource: "job", Group: "batch"}, ""),
			expectErr:                  false,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, job not present in CJ active status, create job failed, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			jobCreateError:             errors.NewAlreadyExists(schema.GroupResource{Resource: "job", Group: "batch"}, ""),
			expectErr:                  false,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: false,
		},
		"prev ran but done, is time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, suspended": {
			concurrencyPolicy:          "Allow",
			suspend:                    true,
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, is time, not past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},

		"still active, not time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"still active, not time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"still active, not time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        justBeforeTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               2,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectDelete:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, get job failed, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			jobGetErr:                  errors.NewBadRequest("request is invalid"),
			expectActive:               1,
			expectedWarnings:           1,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, suspended": {
			concurrencyPolicy:          "Allow",
			suspend:                    true,
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectActive:               1,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"still active, is time, not past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               2,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},

		// 这些场景中错过的调度次数过多，且没有 deadline 或 deadline 太长，
		// 因此控制器应当放弃调度。
		"prev ran but done, long overdue, not past deadline, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, not past deadline, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, not past deadline, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, no deadline, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, no deadline, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, no deadline, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},

		"prev ran but done, long overdue, past medium deadline, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   mediumDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, past short deadline, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},

		"prev ran but done, long overdue, past medium deadline, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   mediumDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, past short deadline, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},

		"prev ran but done, long overdue, past medium deadline, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   mediumDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"prev ran but done, long overdue, past short deadline, F": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},

		// 时钟回拨场景：控制器看到 Job 已创建，因此不采取任何动作。
		"this ran but done, time drifted back, F": {
			concurrencyPolicy:       "Forbid",
			schedule:                onTheHour,
			deadline:                noDead,
			ranPreviously:           true,
			jobCreationTime:         *justAfterTheHour(),
			now:                     justBeforeTheHour(),
			jobCreateError:          errors.NewAlreadyExists(schema.GroupResource{Resource: "jobs", Group: "batch"}, ""),
			expectRequeueAfter:      true,
			expectedRequeueDuration: 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:      true,
		},

		// 模拟 Job lister 滞后：Job 已创建但暂时查不到。
		"this started but went missing, not past deadline, A": {
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            topOfTheHour().Add(time.Millisecond * 100),
			now:                        justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
			jobStillNotFoundInLister:   true,
			jobPresentInCJActiveStatus: true,
		},
		"this started but went missing, not past deadline, f": {
			concurrencyPolicy:          "Forbid",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            topOfTheHour().Add(time.Millisecond * 100),
			now:                        justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
			jobStillNotFoundInLister:   true,
			jobPresentInCJActiveStatus: true,
		},
		"this started but went missing, not past deadline, R": {
			concurrencyPolicy:          "Replace",
			schedule:                   onTheHour,
			deadline:                   longDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            topOfTheHour().Add(time.Millisecond * 100),
			now:                        justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
			jobStillNotFoundInLister:   true,
			jobPresentInCJActiveStatus: true,
		},

		// 模拟 CronJob 列表滞后：Job 已启动但未出现在 Active 列表。
		"this started but is not present in cronjob active list, not past deadline, A": {
			concurrencyPolicy:       "Allow",
			schedule:                onTheHour,
			deadline:                longDead,
			ranPreviously:           true,
			stillActive:             true,
			jobCreationTime:         topOfTheHour().Add(time.Millisecond * 100),
			now:                     justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:            1,
			expectRequeueAfter:      true,
			expectedRequeueDuration: 1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
		},
		"this started but is not present in cronjob active list, not past deadline, f": {
			concurrencyPolicy:       "Forbid",
			schedule:                onTheHour,
			deadline:                longDead,
			ranPreviously:           true,
			stillActive:             true,
			jobCreationTime:         topOfTheHour().Add(time.Millisecond * 100),
			now:                     justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:            1,
			expectRequeueAfter:      true,
			expectedRequeueDuration: 1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
		},
		"this started but is not present in cronjob active list, not past deadline, R": {
			concurrencyPolicy:       "Replace",
			schedule:                onTheHour,
			deadline:                longDead,
			ranPreviously:           true,
			stillActive:             true,
			jobCreationTime:         topOfTheHour().Add(time.Millisecond * 100),
			now:                     justAfterTheHour().Add(time.Millisecond * 100),
			expectActive:            1,
			expectRequeueAfter:      true,
			expectedRequeueDuration: 1*time.Hour - 1*time.Minute - time.Millisecond*100 + nextScheduleDelta,
		},

		// @every 风格调度表达式的相关场景。
		"with @every schedule, never ran, not time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			cronjobCreationTime:        justBeforeTheHour(),
			jobCreationTime:            justBeforeTheHour(),
			now:                        *topOfTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, never ran, is time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			now:                        justBeforeTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
			expectCreate:               true,
			expectActive:               1,
			expectUpdateStatus:         true,
		},
		"with @every schedule, never ran, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   shortDead,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			now:                        justBeforeTheHour().Add(time.Second * time.Duration(shortDead+1)),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead+1) + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, never ran, is time, not past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   longDead,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			now:                        justBeforeTheHour().Add(time.Second * time.Duration(shortDead-1)),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead-1) + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, prev ran but done, not time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			ranPreviously:              true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        *topOfTheHour(),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"with @every schedule, prev ran but done, is time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			ranPreviously:              true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        topOfTheHour().Add(1 * time.Hour),
			expectCreate:               true,
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"with @every schedule, prev ran but done, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        justBeforeTheNextHour().Add(time.Second * time.Duration(shortDead+1)),
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead+1) + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		// 该用例目前会失败：getNextScheduleTime 中围绕 StartingDeadlineSecond 的逻辑
		// 扰乱了计算 schedule.Next(earliestTime) 所依据的时间点。传统 cron 表达式下一切正常，
		// 但使用 @every X 时，schedule.Next(earliestTime) 只是返回在 earliestTime 基础上偏移 X 的时间。
		// "with @every schedule, prev ran but done, is time, not past deadline": {
		// 	concurrencyPolicy:          "Allow",
		// 	schedule:                   everyHour,
		// 	deadline:                   shortDead,
		// 	ranPreviously:              true,
		// 	cronjobCreationTime:        justBeforeThePriorHour(),
		// 	jobCreationTime:            justBeforeThePriorHour(),
		// 	lastScheduleTime:           justBeforeTheHour(),
		// 	now:                        justBeforeTheNextHour().Add(time.Second * time.Duration(shortDead-1)),
		// 	expectCreate:               true,
		// 	expectActive:               1,
		// 	expectRequeueAfter:         true,
		// 	expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead-1) + nextScheduleDelta,
		// 	expectUpdateStatus:         true,
		// 	jobPresentInCJActiveStatus: true,
		// },
		"with @every schedule, still active, not time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeTheHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        *topOfTheHour(),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 1*time.Minute + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, still active, is time": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeThePriorHour(),
			lastScheduleTime:           justBeforeThePriorHour(),
			now:                        *justAfterTheHour(),
			expectCreate:               true,
			expectActive:               2,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - 2*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, still active, is time, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			stillActive:                true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeTheHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        justBeforeTheNextHour().Add(time.Second * time.Duration(shortDead+1)),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead+1) + nextScheduleDelta,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, still active, is time, not past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   longDead,
			ranPreviously:              true,
			stillActive:                true,
			cronjobCreationTime:        justBeforeThePriorHour(),
			jobCreationTime:            justBeforeTheHour(),
			lastScheduleTime:           justBeforeTheHour(),
			now:                        justBeforeTheNextHour().Add(time.Second * time.Duration(shortDead-1)),
			expectCreate:               true,
			expectActive:               2,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead-1) + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
		},
		"with @every schedule, prev ran but done, long overdue, no deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   noDead,
			ranPreviously:              true,
			cronjobCreationTime:        justAfterThePriorHour(),
			lastScheduleTime:           *justAfterTheHour(),
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour(),
			expectCreate:               true,
			expectActive:               1,
			expectedWarnings:           1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Minute + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"with @every schedule, prev ran but done, long overdue, past deadline": {
			concurrencyPolicy:          "Allow",
			schedule:                   everyHour,
			deadline:                   shortDead,
			ranPreviously:              true,
			cronjobCreationTime:        justAfterThePriorHour(),
			lastScheduleTime:           *justAfterTheHour(),
			jobCreationTime:            justAfterThePriorHour(),
			now:                        weekAfterTheHour().Add(1 * time.Minute).Add(time.Second * time.Duration(shortDead+1)),
			expectActive:               1,
			expectRequeueAfter:         true,
			expectedRequeueDuration:    1*time.Hour - time.Second*time.Duration(shortDead+1) + nextScheduleDelta,
			expectUpdateStatus:         true,
			jobPresentInCJActiveStatus: true,
			expectCompleted:            true,
		},
		"do nothing if the namespace is terminating": {
			jobCreateError: &errors.StatusError{ErrStatus: metav1.Status{Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{
				{
					Type:    v1.NamespaceTerminatingCause,
					Message: fmt.Sprintf("namespace %s is being terminated", metav1.NamespaceDefault),
					Field:   "metadata.namespace",
				}}}}},
			concurrencyPolicy:          "Allow",
			schedule:                   onTheHour,
			deadline:                   noDead,
			ranPreviously:              true,
			stillActive:                true,
			jobCreationTime:            justAfterThePriorHour(),
			now:                        *justAfterTheHour(),
			expectActive:               0,
			expectRequeueAfter:         false,
			expectUpdateStatus:         false,
			expectErr:                  true,
			jobPresentInCJActiveStatus: false,
		},
		"set lastsuccessfultime if successfulJobHistoryLimit is zero": {
			successfulJobsHistoryLimit: ptr.To[int32](0),
			ranPreviously:              true,
			schedule:                   onTheHour,
			expectUpdateStatus:         true,
			expectCompleted:            true,
			jobPresentInCJActiveStatus: true,
		},
		"set lastsuccessfultime if successfulJobHistoryLimit is ten": {
			successfulJobsHistoryLimit: ptr.To[int32](10),
			ranPreviously:              true,
			schedule:                   onTheHour,
			expectUpdateStatus:         true,
			expectCompleted:            true,
			jobPresentInCJActiveStatus: true,
		},
		"set lastsuccessfultime if successfulJobHistoryLimit is nil": {
			ranPreviously:              true,
			schedule:                   onTheHour,
			expectUpdateStatus:         true,
			expectCompleted:            true,
			jobPresentInCJActiveStatus: true,
		},
	}
	for name, tc := range testCases {

		t.Run(name, func(t *testing.T) {
			cj := cronJob()
			cj.Spec.ConcurrencyPolicy = tc.concurrencyPolicy
			cj.Spec.Suspend = &tc.suspend
			cj.Spec.Schedule = tc.schedule
			cj.Spec.TimeZone = tc.timeZone
			cj.Spec.SuccessfulJobsHistoryLimit = tc.successfulJobsHistoryLimit
			if tc.deadline != noDead {
				cj.Spec.StartingDeadlineSeconds = &tc.deadline
			}

			var (
				job *batchv1.Job
				err error
			)
			js := []*batchv1.Job{}
			// realCJ 模拟 API Server 侧的对象（供 fakeCJControl 使用）。
			// 它与待同步的 cj 分离，便于最后断言 sync 未就地修改入参。
			realCJ := cj.DeepCopy()
			if tc.ranPreviously {
				cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: justBeforeThePriorHour()}
				if !tc.cronjobCreationTime.IsZero() {
					cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: tc.cronjobCreationTime}
				}
				cj.Status.LastScheduleTime = &metav1.Time{Time: justAfterThePriorHour()}
				if !tc.lastScheduleTime.IsZero() {
					cj.Status.LastScheduleTime = &metav1.Time{Time: tc.lastScheduleTime}
				}
				job, err = getJobFromTemplate2(&cj, tc.jobCreationTime)
				if err != nil {
					t.Fatalf("%s: unexpected error creating a job from template: %v", name, err)
				}
				job.UID = "1234"
				job.Namespace = cj.Namespace

				ref, err := getRef(job)
				if err != nil {
					t.Fatalf("%s: unexpected error getting the job object reference: %v", name, err)
				}
				if tc.jobPresentInCJActiveStatus {
					cj.Status.Active = []v1.ObjectReference{*ref}
				}

				// stillActive 表示 Job 仍在运行：把它放进 realCJ.Active 模拟集群现状，
				// 这样 sync 才能观察到“活跃的 Job”。
				if tc.stillActive {
					realCJ.Status.Active = []v1.ObjectReference{*ref}
					if !tc.jobStillNotFoundInLister {
						js = append(js, job)
					}
				} else {
					// 非活跃分支：给 Job 打上 Complete 条件与完成时间，
					// 使 sync 能识别为已结束并在需要时更新 LastSuccessfulTime。
					job.Status.CompletionTime = &metav1.Time{Time: job.ObjectMeta.CreationTimestamp.Add(time.Second * 10)}
					job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
						Type:   batchv1.JobComplete,
						Status: v1.ConditionTrue,
					})
					if !tc.jobStillNotFoundInLister {
						js = append(js, job)
					}
				}
			} else {
				cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: justBeforeTheHour()}
				if !tc.cronjobCreationTime.IsZero() {
					cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: tc.cronjobCreationTime}
				}
				if tc.stillActive {
					t.Errorf("%s: test setup error: this case makes no sense", name)
				}
			}

			// fake 控制面把用例中的错误原样注入，用于覆盖创建/查询失败路径。
			jc := &fakeJobControl{Job: job, CreateErr: tc.jobCreateError, Err: tc.jobGetErr}
			cjc := &fakeCJControl{CronJob: realCJ}
			recorder := record.NewFakeRecorder(10)

			jm := ControllerV2{
				jobControl:     jc,
				cronJobControl: cjc,
				recorder:       recorder,
				now: func() time.Time {
					return tc.now
				},
			}
			// 传入深拷贝，最后再比较地址，确保 syncCronJob 不修改调用方的对象。
			cjCopy := cj.DeepCopy()
			requeueAfter, updateStatus, err := jm.syncCronJob(context.TODO(), cjCopy, js)
			if tc.expectErr && err == nil {
				t.Errorf("%s: expected error got none with requeueAfter time: %#v", name, requeueAfter)
			}
			if tc.expectRequeueAfter {
				if !reflect.DeepEqual(requeueAfter, &tc.expectedRequeueDuration) {
					t.Errorf("%s: expected requeueAfter: %+v, got requeueAfter time: %+v", name, tc.expectedRequeueDuration, requeueAfter)
				}
			}
			if updateStatus != tc.expectUpdateStatus {
				t.Errorf("%s: expected updateStatus: %t, actually: %t", name, tc.expectUpdateStatus, updateStatus)
			}
			expectedCreates := 0
			if tc.expectCreate {
				expectedCreates = 1
			}
			// 上次创建的 Job 已完成时，LastSuccessfulTime 应等于其完成时间。
			if tc.ranPreviously && !tc.stillActive {
				completionTime := tc.jobCreationTime.Add(10 * time.Second)
				if cjCopy.Status.LastSuccessfulTime == nil || !cjCopy.Status.LastSuccessfulTime.Time.Equal(completionTime) {
					t.Errorf("cj.status.lastSuccessfulTime: %s expected, got %#v", completionTime, cj.Status.LastSuccessfulTime)
				}
			}
			if len(jc.Jobs) != expectedCreates {
				t.Errorf("%s: expected %d job started, actually %v", name, expectedCreates, len(jc.Jobs))
			}
			for i := range jc.Jobs {
				job := &jc.Jobs[i]
				controllerRef := metav1.GetControllerOf(job)
				if controllerRef == nil {
					t.Errorf("%s: expected job to have ControllerRef: %#v", name, job)
				} else {
					if got, want := controllerRef.APIVersion, "batch/v1"; got != want {
						t.Errorf("%s: controllerRef.APIVersion = %q, want %q", name, got, want)
					}
					if got, want := controllerRef.Kind, "CronJob"; got != want {
						t.Errorf("%s: controllerRef.Kind = %q, want %q", name, got, want)
					}
					if got, want := controllerRef.Name, cj.Name; got != want {
						t.Errorf("%s: controllerRef.Name = %q, want %q", name, got, want)
					}
					if got, want := controllerRef.UID, cj.UID; got != want {
						t.Errorf("%s: controllerRef.UID = %q, want %q", name, got, want)
					}
					if controllerRef.Controller == nil || *controllerRef.Controller != true {
						t.Errorf("%s: controllerRef.Controller is not set to true", name)
					}
				}
			}

			expectedDeletes := 0
			if tc.expectDelete {
				expectedDeletes = 1
			}
			if len(jc.DeleteJobName) != expectedDeletes {
				t.Errorf("%s: expected %d job deleted, actually %v", name, expectedDeletes, len(jc.DeleteJobName))
			}

			// 遍历 Job 列表会触发一次状态更新；若本轮创建了 Job，还会再更新一次。
			expectUpdates := 1
			expectedEvents := 0
			if tc.expectCreate {
				expectedEvents++
				expectUpdates++
			}
			if tc.expectDelete {
				expectedEvents++
			}
			if tc.expectCompleted {
				expectedEvents++
			}
			if name == "still active, is time, F" {
				// Forbid 策略下已有活跃 Job 时会额外发出“未调度”事件，仅此用例涉及。
				expectedEvents++
			}
			expectedEvents += tc.expectedWarnings

			if len(recorder.Events) != expectedEvents {
				t.Errorf("%s: expected %d event, actually %v", name, expectedEvents, len(recorder.Events))
			}

			numWarnings := 0
			for i := 1; i <= len(recorder.Events); i++ {
				e := <-recorder.Events
				if strings.HasPrefix(e, v1.EventTypeWarning) {
					numWarnings++
				}
			}
			if numWarnings != tc.expectedWarnings {
				t.Errorf("%s: expected %d warnings, actually %v", name, tc.expectedWarnings, numWarnings)
			}

			if len(cjc.Updates) == expectUpdates && tc.expectActive != len(cjc.Updates[expectUpdates-1].Status.Active) {
				t.Errorf("%s: expected Active size %d, got %d", name, tc.expectActive, len(cjc.Updates[expectUpdates-1].Status.Active))
			}

			if &cj == cjCopy {
				t.Errorf("syncCronJob is not creating a copy of the original cronjob")
			}
		})
	}

}

// fakeQueue 是 TypedRateLimitingInterface 的最小替身，
// 只记录 AddAfter 收到的 key 与 delay，供 updateCronJob 测试断言重排延迟。
type fakeQueue struct {
	workqueue.TypedRateLimitingInterface[string]
	delay time.Duration
	key   interface{}
}

// AddAfter 不真正入队，仅记录参数，避免测试中等待真实定时器。
func (f *fakeQueue) AddAfter(key string, delay time.Duration) {
	f.delay = delay
	f.key = key
}

// TestControllerV2UpdateCronJob 表驱动验证 updateCronJob 在 spec 变化后的重排延迟：
// template 变化立即重排，schedule 变化按新表达式计算下一次触发的等待时间，
// @every 节奏变化与时区变化也有对应预期。该测试会真实等待约 61 秒。
func TestControllerV2UpdateCronJob(t *testing.T) {
	tests := []struct {
		name          string
		oldCronJob    *batchv1.CronJob
		newCronJob    *batchv1.CronJob
		expectedDelay time.Duration
	}{
		{
			name: "spec.template changed",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			expectedDelay: 0 * time.Second,
		},
		{
			name: "spec.schedule changed",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "30 * * * *",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "*/1 * * * *",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			expectedDelay: 1*time.Second + nextScheduleDelta,
		},
		{
			name: "spec.schedule with @every changed - cadence decrease",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "@every 1m",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "@every 3m",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			expectedDelay: 2*time.Minute + 1*time.Second + nextScheduleDelta,
		},
		{
			name: "spec.schedule with @every changed - cadence increase",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "@every 3m",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					Schedule: "@every 1m",
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: justBeforeTheHour()},
				},
			},
			expectedDelay: 1*time.Second + nextScheduleDelta,
		},
		{
			name: "spec.timeZone not changed",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					TimeZone: &newYork,
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					TimeZone: &newYork,
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			expectedDelay: 0 * time.Second,
		},
		{
			name: "spec.timeZone changed",
			oldCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					TimeZone: &newYork,
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "b"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			newCronJob: &batchv1.CronJob{
				Spec: batchv1.CronJobSpec{
					TimeZone: nil,
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"a": "foo"},
							Annotations: map[string]string{"x": "y"},
						},
						Spec: jobSpec(),
					},
				},
			},
			expectedDelay: 0 * time.Second,
		},

		// TODO: 补充更多调度更新相关的测试用例。
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			kubeClient := fake.NewSimpleClientset()
			sharedInformers := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			jm, err := NewControllerV2(ctx, sharedInformers.Batch().V1().Jobs(), sharedInformers.Batch().V1().CronJobs(), kubeClient)
			if err != nil {
				t.Errorf("unexpected error %v", err)
				return
			}
			jm.now = justASecondBeforeTheHour
			queue := &fakeQueue{TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueueWithConfig(
				workqueue.DefaultTypedControllerRateLimiter[string](),
				workqueue.TypedRateLimitingQueueConfig[string]{
					Name: "test-update-cronjob",
				},
			)}
			jm.queue = queue
			jm.jobControl = &fakeJobControl{}
			jm.cronJobControl = &fakeCJControl{}
			jm.recorder = record.NewFakeRecorder(10)

			jm.updateCronJob(logger, tt.oldCronJob, tt.newCronJob)
			if queue.delay.Seconds() != tt.expectedDelay.Seconds() {
				t.Errorf("Expected delay %#v got %#v", tt.expectedDelay.Seconds(), queue.delay.Seconds())
			}
		})
	}
}

// TestControllerV2CleanupFinishedJobs 验证已完成 Job 的历史清理：
// 即使创建 Job 因配额不足失败，仍要删除超出成功历史限制的旧 Job，避免死锁；
// 未达到限制时不得删除。
func TestControllerV2CleanupFinishedJobs(t *testing.T) {
	tests := []struct {
		name                string
		now                 time.Time
		cronJob             *batchv1.CronJob
		finishedJobs        []*batchv1.Job
		jobCreateError      error
		expectedDeletedJobs []string
	}{
		{
			name: "jobs are still deleted when a cronjob can't create jobs due to jobs quota being reached (avoiding a deadlock)",
			now:  *justAfterTheHour(),
			cronJob: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo-ns", Name: "fooer"},
				Spec: batchv1.CronJobSpec{
					Schedule:                   onTheHour,
					SuccessfulJobsHistoryLimit: ptr.To[int32](1),
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"key": "value"}},
					},
				},
				Status: batchv1.CronJobStatus{LastScheduleTime: &metav1.Time{Time: justAfterThePriorHour()}},
			},
			finishedJobs: []*batchv1.Job{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:       "foo-ns",
						Name:            "finished-job-started-hour-ago",
						OwnerReferences: []metav1.OwnerReference{{Name: "fooer", Controller: ptr.To(true)}},
					},
					Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: justBeforeThePriorHour()}},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:       "foo-ns",
						Name:            "finished-job-started-minute-ago",
						OwnerReferences: []metav1.OwnerReference{{Name: "fooer", Controller: ptr.To(true)}},
					},
					Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: justBeforeTheHour()}},
				},
			},
			jobCreateError:      errors.NewInternalError(fmt.Errorf("quota for # of jobs reached")),
			expectedDeletedJobs: []string{"finished-job-started-hour-ago"},
		},
		{
			name: "jobs are not deleted if history limit not reached",
			now:  justBeforeTheHour(),
			cronJob: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo-ns", Name: "fooer"},
				Spec: batchv1.CronJobSpec{
					Schedule:                   onTheHour,
					SuccessfulJobsHistoryLimit: ptr.To[int32](2),
					JobTemplate: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"key": "value"}},
					},
				},
				Status: batchv1.CronJobStatus{LastScheduleTime: &metav1.Time{Time: justAfterThePriorHour()}},
			},
			finishedJobs: []*batchv1.Job{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:       "foo-ns",
						Name:            "finished-job-started-hour-ago",
						OwnerReferences: []metav1.OwnerReference{{Name: "fooer", Controller: ptr.To(true)}},
					},
					Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: justBeforeThePriorHour()}},
				},
			},
			jobCreateError:      nil,
			expectedDeletedJobs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			// 统一给 finishedJobs 打上 Complete 条件，模拟“已成功结束”的 Job。
			for _, job := range tt.finishedJobs {
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: v1.ConditionTrue}}
			}

			client := fake.NewSimpleClientset()

			informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
			// 直接向 informer 索引器注入对象，跳过事件回调，使测试可确定性地驱动一次 sync。
			_ = informerFactory.Batch().V1().CronJobs().Informer().GetIndexer().Add(tt.cronJob)
			for _, job := range tt.finishedJobs {
				_ = informerFactory.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
			}

			jm, err := NewControllerV2(ctx, informerFactory.Batch().V1().Jobs(), informerFactory.Batch().V1().CronJobs(), client)
			if err != nil {
				t.Errorf("unexpected error %v", err)
				return
			}
			jobControl := &fakeJobControl{CreateErr: tt.jobCreateError}
			jm.jobControl = jobControl
			jm.now = func() time.Time {
				return tt.now
			}

			jm.enqueueController(tt.cronJob)
			jm.processNextWorkItem(ctx)

			if len(tt.expectedDeletedJobs) != len(jobControl.DeleteJobName) {
				t.Fatalf("expected '%v' jobs to be deleted, instead deleted '%s'", tt.expectedDeletedJobs, jobControl.DeleteJobName)
			}
			sort.Strings(jobControl.DeleteJobName)
			sort.Strings(tt.expectedDeletedJobs)
			for i, deletedJob := range jobControl.DeleteJobName {
				if deletedJob != tt.expectedDeletedJobs[i] {
					t.Fatalf("expected '%v' jobs to be deleted, instead deleted '%s'", tt.expectedDeletedJobs, jobControl.DeleteJobName)
				}
			}
		})
	}
}

// TestControllerV2JobAlreadyExistsButNotInActiveStatus 验证：已创建但初始未写入 CronJob.Status.Active 的 Job，
// 会在下一次 sync 被补回 Active 列表。否则控制器会因反复尝试创建同名 Job 而不断重试。
func TestControllerV2JobAlreadyExistsButNotInActiveStatus(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	cj := cronJob()
	cj.Spec.ConcurrencyPolicy = "Forbid"
	cj.Spec.Schedule = everyHour
	cj.Status.LastScheduleTime = &metav1.Time{Time: justBeforeThePriorHour()}
	cj.Status.Active = []v1.ObjectReference{}
	cjCopy := cj.DeepCopy()

	job, err := getJobFromTemplate2(&cj, justAfterThePriorHour())
	if err != nil {
		t.Fatalf("Unexpected error creating a job from template: %v", err)
	}
	job.UID = "1234"
	job.Namespace = cj.Namespace

	client := fake.NewSimpleClientset(cjCopy, job)
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	_ = informerFactory.Batch().V1().CronJobs().Informer().GetIndexer().Add(cjCopy)

	jm, err := NewControllerV2(ctx, informerFactory.Batch().V1().Jobs(), informerFactory.Batch().V1().CronJobs(), client)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	jobControl := &fakeJobControl{Job: job, CreateErr: errors.NewAlreadyExists(schema.GroupResource{Resource: "job", Group: "batch"}, "")}
	jm.jobControl = jobControl
	cronJobControl := &fakeCJControl{}
	jm.cronJobControl = cronJobControl
	jm.now = justBeforeTheHour

	jm.enqueueController(cjCopy)
	jm.processNextWorkItem(ctx)

	if len(cronJobControl.Updates) != 1 {
		t.Fatalf("Unexpected updates to cronjob, got: %d, expected 1", len(cronJobControl.Updates))
	}
	if len(cronJobControl.Updates[0].Status.Active) != 1 {
		t.Errorf("Unexpected active jobs count, got: %d, expected 1", len(cronJobControl.Updates[0].Status.Active))
	}

	expectedActiveRef, err := getRef(job)
	if err != nil {
		t.Fatalf("Error getting expected job ref: %v", err)
	}
	if !reflect.DeepEqual(cronJobControl.Updates[0].Status.Active[0], *expectedActiveRef) {
		t.Errorf("Unexpected job reference in cronjob active list, got: %v, expected: %v", cronJobControl.Updates[0].Status.Active[0], expectedActiveRef)
	}

	if len(jobControl.GetJobNamespace) != 1 {
		t.Fatalf("Unexpected get job count, got: %d, expected 1", len(jobControl.GetJobNamespace))
	}
	if jobControl.GetJobNamespace[0] != cj.Namespace {
		t.Fatalf("Unexpected job's namespace, got: %s, expected %s", jobControl.GetJobNamespace[0], cj.Namespace)
	}
}

// TestControllerV2JobAlreadyExistsButDifferentOwner 验证不属于该 CronJob 控制器的已存在 Job 会被安全忽略：
// 不写回状态、也不把它认作自己的活跃 Job。
func TestControllerV2JobAlreadyExistsButDifferentOwner(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	cj := cronJob()
	cj.Spec.ConcurrencyPolicy = "Forbid"
	cj.Spec.Schedule = everyHour
	cj.Status.LastScheduleTime = &metav1.Time{Time: justBeforeThePriorHour()}
	cj.Status.Active = []v1.ObjectReference{}
	cjCopy := cj.DeepCopy()

	job, err := getJobFromTemplate2(&cj, justAfterThePriorHour())
	if err != nil {
		t.Fatalf("Unexpected error creating a job from template: %v", err)
	}
	job.UID = "1234"
	job.Namespace = cj.Namespace

	// 清空属主引用，构造“不属于该 CronJob 的已存在 Job”，
	// 这是本用例要验证的安全忽略场景。
	job.OwnerReferences = []metav1.OwnerReference{}

	client := fake.NewSimpleClientset(cjCopy, job)
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	_ = informerFactory.Batch().V1().CronJobs().Informer().GetIndexer().Add(cjCopy)

	jm, err := NewControllerV2(ctx, informerFactory.Batch().V1().Jobs(), informerFactory.Batch().V1().CronJobs(), client)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	jobControl := &fakeJobControl{Job: job, CreateErr: errors.NewAlreadyExists(schema.GroupResource{Resource: "job", Group: "batch"}, "")}
	jm.jobControl = jobControl
	cronJobControl := &fakeCJControl{}
	jm.cronJobControl = cronJobControl
	jm.now = justBeforeTheHour

	jm.enqueueController(cjCopy)
	jm.processNextWorkItem(ctx)

	if len(cronJobControl.Updates) != 0 {
		t.Fatalf("Unexpected updates to cronjob, got: %d, expected 0", len(cronJobControl.Updates))
	}

	if len(jobControl.GetJobNamespace) != 1 {
		t.Fatalf("Unexpected get job count, got: %d, expected 1", len(jobControl.GetJobNamespace))
	}
	if jobControl.GetJobNamespace[0] != cj.Namespace {
		t.Fatalf("Unexpected job's namespace, got: %s, expected %s", jobControl.GetJobNamespace[0], cj.Namespace)
	}
}

// TestControllerV2ProcessNextItemClearFailureRate 验证成功处理一个 key 后会清空其限流失败计数，
// 避免先前错误导致的退避状态一直残留，使后续重试被无限延迟。
func TestControllerV2ProcessNextItemClearFailureRate(t *testing.T) {

	t.Run("cronjob controller resets rate limiter successfully", func(t *testing.T) {
		_, ctx := ktesting.NewTestContext(t)

		cj := cronJob()
		cj.Spec.Suspend = ptr.To(true)
		cj.Spec.Schedule = onTheHour
		cjCopy := cj.DeepCopy()

		client := fake.NewSimpleClientset(cjCopy)
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		_ = informerFactory.Batch().V1().CronJobs().Informer().GetIndexer().Add(cjCopy)

		jm, err := NewControllerV2(ctx, informerFactory.Batch().V1().Jobs(), informerFactory.Batch().V1().CronJobs(), client)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		jm.jobControl = &fakeJobControl{}
		jm.cronJobControl = &fakeCJControl{CronJob: cjCopy}
		jm.now = justBeforeTheHour

		key, _ := controller.KeyFunc(cjCopy)

		// 人为制造两次限流失败，模拟该 key 之前处理出错、NumRequeues 大于 0 的状态。
		for range 2 {
			jm.queue.AddRateLimited(key)
			k, quit := jm.queue.Get()
			if quit {
				t.Fatal("queue shut down unexpectedly")
			}
			jm.queue.Done(k)
			// 此处故意不调用 Forget，保留退避计数以便下一步断言。
		}
		if requeues := jm.queue.NumRequeues(key); requeues != 2 {
			t.Fatalf("expected >0 requeues after priming, got %d", requeues)
		}

		// 正常处理该 key，期望 processNextWorkItem 内部调用 Forget 清零计数。
		jm.queue.Add(key)
		jm.processNextWorkItem(ctx)

		if requeues := jm.queue.NumRequeues(key); requeues != 0 {
			t.Errorf("cronjob controller: expected NumRequeues 0 after successful sync, got %d "+
				"(rate limiter failure count was not reset — queue.Forget was not called)", requeues)
		}
	})
}
