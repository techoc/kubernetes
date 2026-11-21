/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// backoffRecord 记录某个 Job 在最近一次成功之后连续失败的次数与最后失败时间，
// 用于计算指数退避的剩余等待时间。
type backoffRecord struct {
	key                      string     // Job 的键
	failuresAfterLastSuccess int32      // 最近一次成功后的连续失败次数
	lastFailureTime          *time.Time // 最后一次失败的时间
}

// backoffStore 用 cache.Store 缓存各 Job 的退避记录。
type backoffStore struct {
	store cache.Store
}

// updateBackoffRecord 写入或更新退避记录。
// 已存在时原地更新字段，避免替换指针导致其他持有者看到旧值。
func (s *backoffStore) updateBackoffRecord(record backoffRecord) error {
	b, ok, err := s.store.GetByKey(record.key)
	if err != nil {
		return err
	}

	if !ok {
		err = s.store.Add(&record)
		if err != nil {
			return err
		}
	} else {
		backoffRecord := b.(*backoffRecord)
		backoffRecord.failuresAfterLastSuccess = record.failuresAfterLastSuccess
		backoffRecord.lastFailureTime = record.lastFailureTime
	}

	return nil
}

// removeBackoffRecord 删除 Job 的退避记录；Job 完成或被删除后不再需要退避状态。
func (s *backoffStore) removeBackoffRecord(jobId string) error {
	b, ok, err := s.store.GetByKey(jobId)
	if err != nil {
		return err
	}

	if ok {
		err = s.store.Delete(b)
		if err != nil {
			return err
		}
	}

	return nil

}

// newBackoffStore 创建以 backoffRecord.key 为键的退避记录存储。
func newBackoffStore() *backoffStore {
	return &backoffStore{
		store: cache.NewStore(backoffRecordKeyFunc),
	}
}

var backoffRecordKeyFunc = func(obj interface{}) (string, error) {
	// 只接受 backoffRecord 指针，其他类型视为编程错误。
	if u, ok := obj.(*backoffRecord); ok {
		return u.key, nil
	}
	return "", fmt.Errorf("could not find key for obj %#v", obj)
}

// newBackoffRecord 根据新观测到的成功/失败 Pod 计算退避记录。
// 成功 Pod 会重置连续失败计数；只有晚于最后一次成功时间的失败才计入退避，
// 防止把同步之前就存在的失败误算进来。
func (s *backoffStore) newBackoffRecord(key string, newSucceededPods []*v1.Pod, newFailedPods []*v1.Pod) backoffRecord {
	var backoff *backoffRecord

	// 以已有记录为基础累积，确保跨多次同步的失败次数不丢失。
	if b, exists, _ := s.store.GetByKey(key); exists {
		old := b.(*backoffRecord)
		backoff = &backoffRecord{
			key:                      old.key,
			failuresAfterLastSuccess: old.failuresAfterLastSuccess,
			lastFailureTime:          old.lastFailureTime,
		}
	} else {
		backoff = &backoffRecord{
			key:                      key,
			failuresAfterLastSuccess: 0,
			lastFailureTime:          nil,
		}
	}

	// 先按完成时间排序，才能用最后一个元素表示最近的事件。
	sortByFinishedTime(newSucceededPods)
	sortByFinishedTime(newFailedPods)

	// 本次没有新成功：继续累加失败次数。
	if len(newSucceededPods) == 0 {
		// 既无成功也无失败，保持原记录不变。
		if len(newFailedPods) == 0 {
			return *backoff
		}

		backoff.failuresAfterLastSuccess = backoff.failuresAfterLastSuccess + int32(len(newFailedPods))
		lastFailureTime := getFinishedTime(newFailedPods[len(newFailedPods)-1])
		backoff.lastFailureTime = &lastFailureTime
		return *backoff

	} else {
		// 本次出现成功，失败序列被打断，重置退避状态。
		if len(newFailedPods) == 0 {
			backoff.failuresAfterLastSuccess = 0
			backoff.lastFailureTime = nil
			return *backoff
		}

		backoff.failuresAfterLastSuccess = 0
		backoff.lastFailureTime = nil

		// 只统计严格晚于最后成功时间的失败：按时间从后向前遍历，遇到更早的失败即停止。
		lastSuccessTime := getFinishedTime(newSucceededPods[len(newSucceededPods)-1])
		for i := len(newFailedPods) - 1; i >= 0; i-- {
			failedTime := getFinishedTime(newFailedPods[i])
			if !failedTime.After(lastSuccessTime) {
				break
			}
			if backoff.lastFailureTime == nil {
				backoff.lastFailureTime = &failedTime
			}
			backoff.failuresAfterLastSuccess += 1
		}

		return *backoff

	}

}

// sortByFinishedTime 按 Pod 的完成时间升序排序。
func sortByFinishedTime(pods []*v1.Pod) {
	sort.Slice(pods, func(i, j int) bool {
		p1 := pods[i]
		p2 := pods[j]
		p1FinishTime := getFinishedTime(p1)
		p2FinishTime := getFinishedTime(p2)

		return p1FinishTime.Before(p2FinishTime)
	})
}

// getFinishedTime 按以下优先级推断 Pod 的完成时间：
// 1. 所有容器均已结束时，取最晚的结束时间；
// 2. 存在 Ready=False 条件时，取该条件的最后转换时间；
// 3. 已被删除时，用 deletionTimestamp 减去宽限期估算删除时刻；
// 4. 兜底使用 Pod 的创建时间。
//
// Kubelet 管理的 Pod 进入终态时会被标记 Ready=False，因此命中 (1) 或 (2)；
// 孤立 Pod 会被 PodGC 删除，因此命中 (3)。
func getFinishedTime(p *v1.Pod) time.Time {
	if finishTime := getFinishTimeFromContainers(p); finishTime != nil {
		return *finishTime
	}
	if finishTime := getFinishTimeFromPodReadyFalseCondition(p); finishTime != nil {
		return *finishTime
	}
	if finishTime := getFinishTimeFromDeletionTimestamp(p); finishTime != nil {
		return *finishTime
	}
	// 正常运行 Kubelet 与 PodGC 的集群中不应走到这里，创建时间只是最后兜底。
	return p.CreationTimestamp.Time
}

// getFinishTimeFromContainers 从容器状态中提取完成时间。
func getFinishTimeFromContainers(p *v1.Pod) *time.Time {
	finishTime := latestFinishTime(nil, p.Status.ContainerStatuses, nil)
	// 还需检查可重启的 init 容器（sidecar）状态：sidecar 的结束时间总是晚于普通容器，
	// 只取普通容器会低估 Pod 的真实完成时间。
	names := sets.New[string]()
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			names.Insert(c.Name)
		}
	}
	finishTime = latestFinishTime(finishTime, p.Status.InitContainerStatuses, func(status v1.ContainerStatus) bool {
		return names.Has(status.Name)
	})

	return finishTime
}

// latestFinishTime 在容器状态列表中取最晚结束时间。
// 任一被检查的容器尚未终止或结束时间为零时返回 nil，表示信息不完整。
func latestFinishTime(prevFinishTime *time.Time, cs []v1.ContainerStatus, check func(status v1.ContainerStatus) bool) *time.Time {
	var finishTime = prevFinishTime
	for _, containerState := range cs {
		if check != nil && !check(containerState) {
			continue
		}
		if containerState.State.Terminated == nil ||
			containerState.State.Terminated.FinishedAt.Time.IsZero() {
			return nil
		}
		if finishTime == nil || finishTime.Before(containerState.State.Terminated.FinishedAt.Time) {
			finishTime = &containerState.State.Terminated.FinishedAt.Time
		}
	}
	return finishTime
}

// getFinishTimeFromPodReadyFalseCondition 取 Ready=False 条件的最后转换时间。
func getFinishTimeFromPodReadyFalseCondition(p *v1.Pod) *time.Time {
	if _, c := apipod.GetPodCondition(&p.Status, v1.PodReady); c != nil && c.Status == v1.ConditionFalse && !c.LastTransitionTime.Time.IsZero() {
		return &c.LastTransitionTime.Time
	}
	return nil
}

// getFinishTimeFromDeletionTimestamp 用删除时间戳减去宽限期估算 Pod 实际终止的时刻。
func getFinishTimeFromDeletionTimestamp(p *v1.Pod) *time.Time {
	if p.DeletionTimestamp != nil {
		finishTime := p.DeletionTimestamp.Time.Add(-time.Duration(ptr.Deref(p.DeletionGracePeriodSeconds, 0)) * time.Second)
		return &finishTime
	}
	return nil
}

// getRemainingTime 计算当前退避记录还需等待的时间。
func (backoff backoffRecord) getRemainingTime(clock clock.WithTicker, defaultBackoff time.Duration, maxBackoff time.Duration) time.Duration {
	return getRemainingTimeForFailuresCount(clock, defaultBackoff, maxBackoff, backoff.failuresAfterLastSuccess, backoff.lastFailureTime)
}

// getRemainingTimePerIndex 计算某个索引创建替代 Pod 前还需等待的时间。
// 连续失败次数取自该索引最后一个失败 Pod 上的 job-index-failure-count 注解，
// 该 Pod 的完成时间同时用作最近一次失败时间。
func getRemainingTimePerIndex(logger klog.Logger, clock clock.WithTicker, defaultBackoff time.Duration, maxBackoff time.Duration, lastFailedPod *v1.Pod) time.Duration {
	if lastFailedPod == nil {
		// 该索引尚无失败记录，无需退避。
		return time.Duration(0)
	}
	failureCount := getIndexAbsoluteFailureCount(logger, lastFailedPod) + 1
	lastFailureTime := getFinishedTime(lastFailedPod)
	return getRemainingTimeForFailuresCount(clock, defaultBackoff, maxBackoff, failureCount, &lastFailureTime)
}

// getRemainingTimeForFailuresCount 按指数退避公式计算剩余等待时间：
// 退避时长随失败次数翻倍并封顶于 maxBackoff，再扣除自上次失败以来已过去的时间。
func getRemainingTimeForFailuresCount(clock clock.WithTicker, defaultBackoff time.Duration, maxBackoff time.Duration, failuresCount int32, lastFailureTime *time.Time) time.Duration {
	if failuresCount == 0 {
		return 0
	}

	backoffDuration := defaultBackoff
	for i := 1; i < int(failuresCount); i++ {
		backoffDuration = backoffDuration * 2
		// 达到上限后不再翻倍，防止等待时间无限增长。
		if backoffDuration >= maxBackoff {
			backoffDuration = maxBackoff
			break
		}
	}

	timeElapsedSinceLastFailure := clock.Since(*lastFailureTime)

	if backoffDuration < timeElapsedSinceLastFailure {
		return 0
	}

	return backoffDuration - timeElapsedSinceLastFailure
}
