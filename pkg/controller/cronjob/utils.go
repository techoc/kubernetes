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
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// 本文件包含处理 Job、CronJob 和时间的工具函数。

// missedSchedulesType 表示错过的调度次数级别。
type missedSchedulesType int

const (
	// noneMissed 表示没有错过任何调度。
	noneMissed missedSchedulesType = iota
	// fewMissed 表示错过了少量调度。
	fewMissed
	// manyMissed 表示错过了大量调度。
	manyMissed
)

// String 返回 missedSchedulesType 的可读名称。
func (e missedSchedulesType) String() string {
	switch e {
	case noneMissed:
		return "none"
	case fewMissed:
		return "few"
	case manyMissed:
		return "many"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

// inActiveList 检查 CronJob 的 .status.active 中是否存在具有指定 UID 的 Job。
func inActiveList(cj *batchv1.CronJob, uid types.UID) bool {
	for _, j := range cj.Status.Active {
		if j.UID == uid {
			return true
		}
	}
	return false
}

// inActiveListByName 检查 CronJob 的 status.active 中是否存在名称和命名空间
// 都相同的 Job。
func inActiveListByName(cj *batchv1.CronJob, job *batchv1.Job) bool {
	for _, j := range cj.Status.Active {
		if j.Name == job.Name && j.Namespace == job.Namespace {
			return true
		}
	}
	return false
}

// deleteFromActiveList 从 CronJob 的 status.active 中移除指定 UID 的 Job 引用。
func deleteFromActiveList(cj *batchv1.CronJob, uid types.UID) {
	if cj == nil {
		return
	}
	// TODO: @alpatel 可通过原地删除元素降低内存占用，例如：
	//  cj.Status.Active = append(cj.Status.Active[:indexToRemove], cj.Status.Active[indexToRemove:]...)
	newActive := []corev1.ObjectReference{}
	for _, j := range cj.Status.Active {
		if j.UID != uid {
			newActive = append(newActive, j)
		}
	}
	cj.Status.Active = newActive
}

// mostRecentScheduleTime 返回：
//   - 上一次调度时间，或 CronJob 的创建时间；
//   - 最近一次应该创建 Job 的时间；若该时间晚于当前时间则为 nil；
//   - 表示未错过、错过少量或错过大量调度的值；
//   - 错误：当调度表达式语法正确但逻辑上无意义时（例如只有 30 天的月份里
//     指定第 31 天）。
func mostRecentScheduleTime(cj *batchv1.CronJob, now time.Time, schedule cron.Schedule, includeStartingDeadlineSeconds bool) (time.Time, *time.Time, missedSchedulesType, error) {
	earliestTime := cj.ObjectMeta.CreationTimestamp.Time
	missedSchedules := noneMissed
	if cj.Status.LastScheduleTime != nil {
		earliestTime = cj.Status.LastScheduleTime.Time
	}
	if includeStartingDeadlineSeconds && cj.Spec.StartingDeadlineSeconds != nil {
		// 超过起始截止时间（startingDeadline）之后，控制器将不再调度任何 Job。
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cj.Spec.StartingDeadlineSeconds))

		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}

	t1 := schedule.Next(earliestTime)
	t2 := schedule.Next(t1)

	if now.Before(t1) {
		return earliestTime, nil, missedSchedules, nil
	}
	if now.Before(t2) {
		return earliestTime, &t1, missedSchedules, nil
	}

	// cron.ParseStandard("59 23 31 2 *") 可能返回语法有效但逻辑无效的调度：
	// 分 59、时 23、日 31、月 2，其中 2 月显然没有 31 号。
	// 此时 timeBetweenTwoSchedules 为 0，需将这种无效调度作为错误返回。
	timeBetweenTwoSchedules := int64(t2.Sub(t1).Round(time.Second).Seconds())
	if timeBetweenTwoSchedules < 1 {
		return earliestTime, nil, missedSchedules, fmt.Errorf("time difference between two schedules is less than 1 second")
	}
	// 计算错过调度次数的逻辑采用粗略近似：先算两次调度（t1、t2）的间隔，
	// 再看从上次调度到现在能容纳多少个这样的间隔。
	timeElapsed := int64(now.Sub(t1).Seconds())
	numberOfMissedSchedules := (timeElapsed / timeBetweenTwoSchedules) + 1

	var mostRecentTime time.Time
	// 为了让常规调度和 @every 形式的调度都能得到准确的最近时间，先按
	// “错过的调度次数 × 间隔” 推算一个可能的起始时间。这对保证 @every
	// 从正确时间开始至关重要，这也是 numberOfMissedSchedules-1 的由来；
	// 再额外 -1 是为了往前多退一个时间单位，让 cron 库在不规则的调度
	// （如 30 6-16/4 * * 1-5）下也能算出正确的调度。
	potentialEarliest := t1.Add(time.Duration((numberOfMissedSchedules-1-1)*timeBetweenTwoSchedules) * time.Second)
	for t := schedule.Next(potentialEarliest); !t.After(now); t = schedule.Next(t) {
		mostRecentTime = t
	}

	// 一个对象可能错过多次启动。例如控制器在周五下午 5:01 卡住，
	// 直到周二上午才有人发现并重启，那么每个小时调度一次的 CronJob
	// 错过的 80 多次 Job 都应该自动启动（前提是允许并发和延迟启动）。
	//
	// 但如果存在缺陷，或控制器所在服务器/apiserver 时钟不准
	// （用于设置 creationTimestamp），错过的启动时间可能多到离谱
	// （甚至相差数十年），足以耗尽控制器的 CPU 和内存。因此此时不
	// 再枚举所有错过的启动时间。
	//
	// 阈值 100 是大致选定的：大于 80 次，但又远算不上“海量”。
	switch {
	case numberOfMissedSchedules > 100:
		missedSchedules = manyMissed
	// 仍然报告少量错过的调度。
	case numberOfMissedSchedules > 0:
		missedSchedules = fewMissed
	}

	if mostRecentTime.IsZero() {
		return earliestTime, nil, missedSchedules, nil
	}
	return earliestTime, &mostRecentTime, missedSchedules, nil
}

// nextScheduleTimeDuration 根据调度表达式和上次调度时间返回下次重新入队的
// 等待时长。它在下次重入队时间上增加 100ms 余量，以抵消 NTP 时间偏差；
// 即使实际偏差超过该调整量（现实中通常约 100 秒），Job 仍会执行而不会
// 错过调度。
func nextScheduleTimeDuration(cj *batchv1.CronJob, now time.Time, schedule cron.Schedule) *time.Duration {
	earliestTime, mostRecentTime, missedSchedules, err := mostRecentScheduleTime(cj, now, schedule, false)
	if err != nil {
		// 出错时仍需重新入队，因此以当前时间为基准瞄准下一个调度点。
		mostRecentTime = &now
	} else if mostRecentTime == nil {
		if missedSchedules == noneMissed {
			// 从 earliestTime 起没有错过任何调度。
			mostRecentTime = &earliestTime
		} else {
			// 若从 earliestTime 起有错过调度，始终以当前时间为准。
			mostRecentTime = &now
		}
	}

	t := schedule.Next(*mostRecentTime).Add(nextScheduleDelta).Sub(now)
	return &t
}

// nextScheduleTime 返回上次调度之后、当前时间之前最近一次应该调度的
// time.Time；若没有未满足的调度时间则返回 nil，并返回可能的错误。
// 若错过的启动次数过多（>100），还会记录一条警告事件。
func nextScheduleTime(logger klog.Logger, cj *batchv1.CronJob, now time.Time, schedule cron.Schedule, recorder record.EventRecorder) (*time.Time, error) {
	_, mostRecentTime, missedSchedules, err := mostRecentScheduleTime(cj, now, schedule, true)

	if mostRecentTime == nil || mostRecentTime.After(now) {
		return nil, err
	}

	if missedSchedules == manyMissed {
		recorder.Eventf(cj, corev1.EventTypeWarning, "TooManyMissedTimes", "too many missed start times. Set or decrease .spec.startingDeadlineSeconds or check clock skew")
		logger.Info("too many missed times", "cronjob", klog.KObj(cj))
	}

	return mostRecentTime, err
}

// copyLabels 复制 JobTemplate 的标签，避免直接引用模板中的 map。
func copyLabels(template *batchv1.JobTemplateSpec) labels.Set {
	l := make(labels.Set)
	for k, v := range template.Labels {
		l[k] = v
	}
	return l
}

// copyAnnotations 复制 JobTemplate 的注解，避免直接引用模板中的 map。
func copyAnnotations(template *batchv1.JobTemplateSpec) labels.Set {
	a := make(labels.Set)
	for k, v := range template.Annotations {
		a[k] = v
	}
	return a
}

// getJobFromTemplate2 根据 CronJob 创建 Job。它将 Unix 时间转换为距 epoch
// 的分钟数并拼接到 Job 名称中，因为 cronjob_controller v2 的最小调度粒度
// 为 1 分钟。
func getJobFromTemplate2(cj *batchv1.CronJob, scheduledTime time.Time) (*batchv1.Job, error) {
	labels := copyLabels(&cj.Spec.JobTemplate)
	// 同一名义启动时间对应的 Job 名称必须确定，避免重复创建同一个 Job。
	name := getJobName(cj, scheduledTime)
	annotations := copyAnnotations(&cj.Spec.JobTemplate)
	timeZoneLocation, err := time.LoadLocation(ptr.Deref(cj.Spec.TimeZone, ""))
	if err != nil {
		return nil, err
	}
	// 将 Job 的创建时间以 RFC3339 格式附加到 CronJob 的注解中。
	annotations[batchv1.CronJobScheduledTimestampAnnotation] = scheduledTime.In(timeZoneLocation).Format(time.RFC3339)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:            labels,
			Annotations:       annotations,
			Name:              name,
			CreationTimestamp: metav1.Time{Time: scheduledTime},
			OwnerReferences:   []metav1.OwnerReference{*metav1.NewControllerRef(cj, controllerKind)},
		},
	}
	cj.Spec.JobTemplate.Spec.DeepCopyInto(&job.Spec)
	return job, nil
}

// getTimeHashInMinutes 返回以分钟表示的 Unix epoch 时间。
func getTimeHashInMinutes(scheduledTime time.Time) int64 {
	return scheduledTime.Unix() / 60
}

// byJobStartTime 按启动时间对 Job 列表排序，启动时间相同则用名称作为次序。
type byJobStartTime []*batchv1.Job

// Len 返回 Job 数量。
func (o byJobStartTime) Len() int { return len(o) }

// Swap 交换第 i 个和第 j 个 Job。
func (o byJobStartTime) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 判断第 i 个 Job 是否应排在第 j 个之前。
func (o byJobStartTime) Less(i, j int) bool {
	if o[i].Status.StartTime == nil && o[j].Status.StartTime != nil {
		return false
	}
	if o[i].Status.StartTime != nil && o[j].Status.StartTime == nil {
		return true
	}
	if o[i].Status.StartTime.Equal(o[j].Status.StartTime) {
		return o[i].Name < o[j].Name
	}
	return o[i].Status.StartTime.Before(o[j].Status.StartTime)
}
