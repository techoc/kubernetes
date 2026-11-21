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

package cronjob

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	cron "github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// TestGetJobFromTemplate2 验证从模板创建 Job 时正确复制标签、注解与调度时间注释。
func TestGetJobFromTemplate2(t *testing.T) {
	// 需复制模板的标签、注解等字段并添加 created-by 引用，Job 才能被控制器归属与回收。
	var (
		one             int64 = 1
		no              bool
		timeZoneUTC     = "UTC"
		timeZoneCorrect = "Europe/Rome"
		scheduledTime   = *topOfTheHour()
	)

	cj := batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mycronjob",
			Namespace: "snazzycats",
			UID:       types.UID("1a2b3c"),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          "* * * * ?",
			ConcurrencyPolicy: batchv1.AllowConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.Time{Time: scheduledTime},
					Labels:            map[string]string{"a": "b"},
				},
				Spec: batchv1.JobSpec{
					ActiveDeadlineSeconds: &one,
					ManualSelector:        &no,
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
			},
		},
	}

	testCases := []struct {
		name                        string
		timeZone                    *string
		inputAnnotations            map[string]string
		expectedScheduledTime       func() time.Time
		expectedNumberOfAnnotations int
	}{
		{
			name:             "UTC timezone and one annotation",
			timeZone:         &timeZoneUTC,
			inputAnnotations: map[string]string{"x": "y"},
			expectedScheduledTime: func() time.Time {
				return scheduledTime
			},
			expectedNumberOfAnnotations: 2,
		},
		{
			name:             "nil timezone and one annotation",
			timeZone:         nil,
			inputAnnotations: map[string]string{"x": "y"},
			expectedScheduledTime: func() time.Time {
				return scheduledTime
			},
			expectedNumberOfAnnotations: 2,
		},
		{
			name:             "correct timezone and multiple annotation",
			timeZone:         &timeZoneCorrect,
			inputAnnotations: map[string]string{"x": "y", "z": "x"},
			expectedScheduledTime: func() time.Time {
				location, _ := time.LoadLocation(timeZoneCorrect)
				return scheduledTime.In(location)
			},
			expectedNumberOfAnnotations: 3,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			cj.Spec.JobTemplate.Annotations = tt.inputAnnotations
			cj.Spec.TimeZone = tt.timeZone

			var job *batchv1.Job
			job, err := getJobFromTemplate2(&cj, scheduledTime)
			if err != nil {
				t.Errorf("Did not expect error: %s", err)
			}
			if !strings.HasPrefix(job.ObjectMeta.Name, "mycronjob-") {
				t.Errorf("Wrong Name")
			}
			if len(job.ObjectMeta.Labels) != 1 {
				t.Errorf("Wrong number of labels")
			}
			if len(job.ObjectMeta.Annotations) != tt.expectedNumberOfAnnotations {
				t.Errorf("Wrong number of annotations")
			}

			// 调度时间注释须与按目标时区换算后的期望值一致，否则定时语义会错乱。
			scheduledAnnotation := job.ObjectMeta.Annotations[batchv1.CronJobScheduledTimestampAnnotation]
			timeZoneLocation, err := time.LoadLocation(ptr.Deref(tt.timeZone, ""))
			if err != nil {
				t.Errorf("Wrong timezone location")
			}
			if len(job.ObjectMeta.Annotations) != 0 && scheduledAnnotation != tt.expectedScheduledTime().Format(time.RFC3339) {
				t.Errorf("Wrong cronJob scheduled timestamp annotation, expexted %s, got %s.", tt.expectedScheduledTime().In(timeZoneLocation).Format(time.RFC3339), scheduledAnnotation)
			}
		})
	}
}

// TestNextScheduleTime 覆盖首次调度、已有上次调度、截止时间及非法计划等场景下 nextScheduleTime 的返回值。
func TestNextScheduleTime(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 计划设为每小时整点，便于用固定时间点推导预期结果。
	schedule := "0 * * * ?"

	ParseSchedule := func(schedule string) cron.Schedule {
		sched, err := cron.ParseStandard(schedule)
		if err != nil {
			t.Errorf("Error parsing schedule: %#v", err)
			return nil
		}
		return sched
	}
	// FakeRecorder 承接事件，避免测试依赖真实事件管道。
	recorder := record.NewFakeRecorder(50)
	// T1、T2 是相邻的两个预定启动时间，作为后续场景的基准。
	T1 := *topOfTheHour()
	T2 := *deltaTimeAfterTopOfTheHour(1 * time.Hour)

	cj := batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mycronjob",
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID("1a2b3c"),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          schedule,
			ConcurrencyPolicy: batchv1.AllowConcurrent,
			JobTemplate:       batchv1.JobTemplateSpec{},
		},
	}
	{
		// 场景 1：没有已知启动时间；创建时间早于 T1。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		// 当前时间虽晚于创建时间但仍早于 T1，不应触发调度。
		now := T1.Add(-7 * time.Minute)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule != nil {
			t.Errorf("expected no start time, got:  %v", schedule)
		}
	}
	{
		// 场景 2：没有已知启动时间且当前时间已过 T1，应补一次调度。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		now := T1.Add(2 * time.Second)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule == nil {
			t.Errorf("expected 1 start time, got nil")
		} else if !schedule.Equal(T1) {
			t.Errorf("expected: %v, got: %v", T1, schedule)
		}
	}
	{
		// 场景 3：LastScheduleTime 已覆盖 T1，无需重复调度。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: T1}
		now := T1.Add(2 * time.Minute)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule != nil {
			t.Errorf("expected 0 start times, got: %v", schedule)
		}
	}
	{
		// 场景 4：已有 T1 的调度记录，当前时间越过 T2，应返回 T2。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: T1}
		now := T2.Add(5 * time.Minute)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule == nil {
			t.Errorf("expected 1 start times, got nil")
		} else if !schedule.Equal(T2) {
			t.Errorf("expected: %v, got: %v", T2, schedule)
		}
	}
	{
		// 场景 5：跨越多个调度点；函数只返回最近一次，其余由后续循环补上。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		now := T2.Add(5 * time.Minute)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule == nil {
			t.Errorf("expected 1 start times, got nil")
		} else if !schedule.Equal(T2) {
			t.Errorf("expected: %v, got: %v", T2, schedule)
		}
	}
	{
		// 场景 6：当前时间远超上次调度且未设截止时间，应判定存在遗漏。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		now := T2.Add(10 * 24 * time.Hour)
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule == nil {
			t.Errorf("expected more than 0 missed times")
		}
	}
	{
		// 场景 7：仍有遗漏，但短截止时间会把过旧的调度点排除在外。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		now := T2.Add(10 * 24 * time.Hour)
		deadline := int64(2 * 60 * 60)
		cj.Spec.StartingDeadlineSeconds = &deadline
		schedule, _ := nextScheduleTime(logger, &cj, now, ParseSchedule(cj.Spec.Schedule), recorder)
		if schedule == nil {
			t.Errorf("expected more than 0 missed times")
		}
	}
	{
		// 场景 8：非法计划产生的错误必须向上传播，不能被吞掉。
		cj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(10 * time.Second)}
		cj.Status.LastScheduleTime = nil
		now := *deltaTimeAfterTopOfTheHour(1 * time.Hour)
		schedule, err := nextScheduleTime(logger, &cj, now, ParseSchedule("59 23 31 2 *"), recorder)
		if schedule != nil {
			t.Errorf("expected no start time, got:  %v", schedule)
		}
		if err == nil {
			t.Errorf("expected error")
		}
	}
}

// TestByJobStartTime 验证 byJobStartTime 对 nil 与已设置 StartTime 的 Job 的排序规则。
func TestByJobStartTime(t *testing.T) {
	// nil 的 StartTime 视为最早，同时间再按名称打破平局。
	now := metav1.NewTime(time.Date(2018, time.January, 1, 2, 3, 4, 5, time.UTC))
	later := metav1.NewTime(time.Date(2019, time.January, 1, 2, 3, 4, 5, time.UTC))
	aNil := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{},
	}
	bNil := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b"},
		Status:     batchv1.JobStatus{},
	}
	aSet := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{StartTime: &now},
	}
	bSet := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b"},
		Status:     batchv1.JobStatus{StartTime: &now},
	}
	aSetLater := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{StartTime: &later},
	}

	testCases := []struct {
		name            string
		input, expected []*batchv1.Job
	}{
		{
			name:     "both have nil start times",
			input:    []*batchv1.Job{bNil, aNil},
			expected: []*batchv1.Job{aNil, bNil},
		},
		{
			name:     "only the first has a nil start time",
			input:    []*batchv1.Job{aNil, bSet},
			expected: []*batchv1.Job{bSet, aNil},
		},
		{
			name:     "only the second has a nil start time",
			input:    []*batchv1.Job{aSet, bNil},
			expected: []*batchv1.Job{aSet, bNil},
		},
		{
			name:     "both have non-nil, equal start time",
			input:    []*batchv1.Job{bSet, aSet},
			expected: []*batchv1.Job{aSet, bSet},
		},
		{
			name:     "both have non-nil, different start time",
			input:    []*batchv1.Job{aSetLater, bSet},
			expected: []*batchv1.Job{bSet, aSetLater},
		},
	}

	for _, testCase := range testCases {
		sort.Sort(byJobStartTime(testCase.input))
		if !reflect.DeepEqual(testCase.input, testCase.expected) {
			t.Errorf("case: '%s', jobs not sorted as expected", testCase.name)
		}
	}
}

// TestMostRecentScheduleTime 校验 mostRecentScheduleTime 计算的最早时间、最近调度时间及遗漏程度。
func TestMostRecentScheduleTime(t *testing.T) {
	metav1TopOfTheHour := metav1.NewTime(*topOfTheHour())
	metav1HalfPastTheHour := metav1.NewTime(*deltaTimeAfterTopOfTheHour(30 * time.Minute))
	metav1MinuteAfterTopOfTheHour := metav1.NewTime(*deltaTimeAfterTopOfTheHour(1 * time.Minute))
	oneMinute := int64(60)
	tenSeconds := int64(10)

	tests := []struct {
		name                  string
		cj                    *batchv1.CronJob
		includeSDS            bool
		now                   time.Time
		expectedEarliestTime  time.Time
		expectedRecentTime    *time.Time
		expectedTooManyMissed missedSchedulesType
		wantErr               bool
	}{
		{
			name: "now before next schedule",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 * * * *",
				},
			},
			now:                  topOfTheHour().Add(30 * time.Second),
			expectedRecentTime:   nil,
			expectedEarliestTime: *topOfTheHour(),
		},
		{
			name: "now just after next schedule",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 * * * *",
				},
			},
			now:                  topOfTheHour().Add(61 * time.Minute),
			expectedRecentTime:   deltaTimeAfterTopOfTheHour(60 * time.Minute),
			expectedEarliestTime: *topOfTheHour(),
		},
		{
			name: "missed 5 schedules",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(*deltaTimeAfterTopOfTheHour(10 * time.Second)),
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 * * * *",
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(301 * time.Minute),
			expectedRecentTime:    deltaTimeAfterTopOfTheHour(300 * time.Minute),
			expectedEarliestTime:  *deltaTimeAfterTopOfTheHour(10 * time.Second),
			expectedTooManyMissed: fewMissed,
		},
		{
			name: "complex schedule",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 6-16/4 * * 1-5",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(24*time.Hour + 31*time.Minute),
			expectedRecentTime:    deltaTimeAfterTopOfTheHour(24*time.Hour + 30*time.Minute),
			expectedEarliestTime:  *deltaTimeAfterTopOfTheHour(30 * time.Minute),
			expectedTooManyMissed: fewMissed,
		},
		{
			name: "another complex schedule",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 10,11,12 * * 1-5",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(30*time.Hour + 30*time.Minute),
			expectedRecentTime:    nil,
			expectedEarliestTime:  *deltaTimeAfterTopOfTheHour(30 * time.Minute),
			expectedTooManyMissed: fewMissed,
		},
		{
			name: "complex schedule with longer diff between executions",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 6-16/4 * * 1-5",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(96*time.Hour + 31*time.Minute),
			expectedRecentTime:    deltaTimeAfterTopOfTheHour(96*time.Hour + 30*time.Minute),
			expectedEarliestTime:  *deltaTimeAfterTopOfTheHour(30 * time.Minute),
			expectedTooManyMissed: fewMissed,
		},
		{
			name: "complex schedule with shorter diff between executions",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 6-16/4 * * 1-5",
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(24*time.Hour + 31*time.Minute),
			expectedRecentTime:    deltaTimeAfterTopOfTheHour(24*time.Hour + 30*time.Minute),
			expectedEarliestTime:  *topOfTheHour(),
			expectedTooManyMissed: fewMissed,
		},
		{
			name: "@every schedule",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(*deltaTimeAfterTopOfTheHour(-59 * time.Minute)),
				},
				Spec: batchv1.CronJobSpec{
					Schedule:                "@every 1h",
					StartingDeadlineSeconds: &tenSeconds,
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1MinuteAfterTopOfTheHour,
				},
			},
			now:                   *deltaTimeAfterTopOfTheHour(7 * 24 * time.Hour),
			expectedRecentTime:    deltaTimeAfterTopOfTheHour((6 * 24 * time.Hour) + 23*time.Hour + 1*time.Minute),
			expectedEarliestTime:  *deltaTimeAfterTopOfTheHour(1 * time.Minute),
			expectedTooManyMissed: manyMissed,
		},
		{
			name: "rogue cronjob",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(*deltaTimeAfterTopOfTheHour(10 * time.Second)),
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "59 23 31 2 *",
				},
			},
			now:                *deltaTimeAfterTopOfTheHour(1 * time.Hour),
			expectedRecentTime: nil,
			wantErr:            true,
		},
		{
			name: "earliestTime being CreationTimestamp and LastScheduleTime",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 * * * *",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1TopOfTheHour,
				},
			},
			now:                  *deltaTimeAfterTopOfTheHour(30 * time.Second),
			expectedEarliestTime: *topOfTheHour(),
			expectedRecentTime:   nil,
		},
		{
			name: "earliestTime being LastScheduleTime",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "*/5 * * * *",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:                  *deltaTimeAfterTopOfTheHour(31 * time.Minute),
			expectedEarliestTime: *deltaTimeAfterTopOfTheHour(30 * time.Minute),
			expectedRecentTime:   nil,
		},
		{
			name: "earliestTime being LastScheduleTime (within StartingDeadlineSeconds)",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule:                "*/5 * * * *",
					StartingDeadlineSeconds: &oneMinute,
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:                  *deltaTimeAfterTopOfTheHour(31 * time.Minute),
			expectedEarliestTime: *deltaTimeAfterTopOfTheHour(30 * time.Minute),
			expectedRecentTime:   nil,
		},
		{
			name: "earliestTime being LastScheduleTime (outside StartingDeadlineSeconds)",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule:                "*/5 * * * *",
					StartingDeadlineSeconds: &oneMinute,
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			includeSDS:           true,
			now:                  *deltaTimeAfterTopOfTheHour(32 * time.Minute),
			expectedEarliestTime: *deltaTimeAfterTopOfTheHour(31 * time.Minute),
			expectedRecentTime:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched, err := cron.ParseStandard(tt.cj.Spec.Schedule)
			if err != nil {
				t.Errorf("error setting up the test, %s", err)
			}
			gotEarliestTime, gotRecentTime, gotTooManyMissed, err := mostRecentScheduleTime(tt.cj, tt.now, sched, tt.includeSDS)
			// 先处理预期的错误分支，避免对 nil 结果继续做时间断言而误报。
			if tt.wantErr {
				if err == nil {
					t.Error("mostRecentScheduleTime() got no error when expected one")
				}
				return
			}
			if !tt.wantErr && err != nil {
				t.Error("mostRecentScheduleTime() got error when none expected")
			}
			if gotEarliestTime.IsZero() {
				t.Errorf("earliestTime should never be 0, want %v", tt.expectedEarliestTime)
			}
			if !gotEarliestTime.Equal(tt.expectedEarliestTime) {
				t.Errorf("expectedEarliestTime - got %v, want %v", gotEarliestTime, tt.expectedEarliestTime)
			}
			if !reflect.DeepEqual(gotRecentTime, tt.expectedRecentTime) {
				t.Errorf("expectedRecentTime - got %v, want %v", gotRecentTime, tt.expectedRecentTime)
			}
			if gotTooManyMissed != tt.expectedTooManyMissed {
				t.Errorf("expectedNumberOfMisses - got %v, want %v", gotTooManyMissed, tt.expectedTooManyMissed)
			}
		})
	}
}

// TestNextScheduleTimeDuration 检查 nextScheduleTimeDuration 在复杂计划与漏跑场景下的重排队间隔。
func TestNextScheduleTimeDuration(t *testing.T) {
	metav1TopOfTheHour := metav1.NewTime(*topOfTheHour())
	metav1HalfPastTheHour := metav1.NewTime(*deltaTimeAfterTopOfTheHour(30 * time.Minute))
	metav1TwoHoursLater := metav1.NewTime(*deltaTimeAfterTopOfTheHour(2 * time.Hour))

	tests := []struct {
		name             string
		cj               *batchv1.CronJob
		now              time.Time
		expectedDuration time.Duration
	}{
		{
			name: "complex schedule skipping weekend",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 6-16/4 * * 1-5",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:              *deltaTimeAfterTopOfTheHour(24*time.Hour + 31*time.Minute),
			expectedDuration: 3*time.Hour + 59*time.Minute + nextScheduleDelta,
		},
		{
			name: "another complex schedule skipping weekend",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "30 10,11,12 * * 1-5",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1HalfPastTheHour,
				},
			},
			now:              *deltaTimeAfterTopOfTheHour(30*time.Hour + 30*time.Minute),
			expectedDuration: 66*time.Hour + nextScheduleDelta,
		},
		{
			name: "once a week cronjob, missed two runs",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 12 * * 4",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &metav1TwoHoursLater,
				},
			},
			now:              *deltaTimeAfterTopOfTheHour(19*24*time.Hour + 1*time.Hour + 30*time.Minute),
			expectedDuration: 48*time.Hour + 30*time.Minute + nextScheduleDelta,
		},
		{
			name: "no previous run of a cronjob",
			cj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1TopOfTheHour,
				},
				Spec: batchv1.CronJobSpec{
					Schedule: "0 12 * * 5",
				},
			},
			now:              *deltaTimeAfterTopOfTheHour(6 * time.Hour),
			expectedDuration: 20*time.Hour + nextScheduleDelta,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched, err := cron.ParseStandard(tt.cj.Spec.Schedule)
			if err != nil {
				t.Errorf("error setting up the test, %s", err)
			}
			gotScheduleTimeDuration := nextScheduleTimeDuration(tt.cj, tt.now, sched)
			// 负间隔意味着调度计算出错，必须先拦截再比对具体数值。
			if *gotScheduleTimeDuration < 0 {
				t.Errorf("scheduleTimeDuration should never be less than 0, got %s", gotScheduleTimeDuration)
			}
			if !reflect.DeepEqual(gotScheduleTimeDuration, &tt.expectedDuration) {
				t.Errorf("scheduleTimeDuration - got %s, want %s", gotScheduleTimeDuration, tt.expectedDuration)
			}
		})
	}
}

// topOfTheHour 返回测试中作为基准的固定整点时间，保证各用例时间可复现。
func topOfTheHour() *time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T10:00:00Z")
	if err != nil {
		panic("test setup error")
	}
	return &T1
}

// deltaTimeAfterTopOfTheHour 返回基准整点加上偏移后的时间，便于构造相对时间点。
func deltaTimeAfterTopOfTheHour(duration time.Duration) *time.Time {
	T1, err := time.Parse(time.RFC3339, "2016-05-19T10:00:00Z")
	if err != nil {
		panic("test setup error")
	}
	t := T1.Add(duration)
	return &t
}
