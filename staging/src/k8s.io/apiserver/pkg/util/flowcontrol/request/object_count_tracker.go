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

package request

import (
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// 删除类型（主要是 CRD）并不频繁，因此可以拉长缓存清理间隔。
	//
	pruneInterval = 1 * time.Hour

	// 存储层每 1 分钟轮询一次对象计数，因此允许最多 2-3 次瞬时失败后再取到最新值。
	//
	// 超过该阈值未更新即认为数据陈旧，防止用过期计数做限流决策。
	staleTolerationThreshold = 3 * time.Minute
)

var (
	// 未跟踪某资源的对象计数时返回 ObjectCountNotFoundErr。
	//
	ObjectCountNotFoundErr = errors.New("object count not found for the given resource")

	// 因瞬时失败导致对象计数陈旧时返回 ObjectCountStaleErr。
	//
	ObjectCountStaleErr = errors.New("object count has gone stale for the given resource")
)

// StorageObjectCountTracker 用于跟踪每种资源的对象总数。
type StorageObjectCountTracker interface {
	// 调用 Set 更新给定资源的当前对象总数。
	//
	Set(string, storage.Stats)

	// Get 返回给定资源的对象总数。
	// 可能返回以下错误：
	// - 若因瞬时失败导致计数陈旧，返回 ObjectCountStaleErr。
	//
	Get(string) (storage.Stats, error)

	// RunUntil 启动所有必要的维护任务。
	RunUntil(stopCh <-chan struct{})
}

// NewStorageObjectCountTracker 返回 StorageObjectCountTracker 实例，
func NewStorageObjectCountTracker() StorageObjectCountTracker {
	return &objectCountTracker{
		clock:  &clock.RealClock{},
		counts: map[string]*timestampedStats{},
	}
}

// timestampedStats 保存资源计数及最后更新时间，便于陈旧后清理。
type timestampedStats struct {
	storage.Stats
	lastUpdatedAt time.Time
}

// objectCountTracker 用读写互斥锁实现 StorageObjectCountTracker。
//
// 读多写少，使用 RWMutex；Set 是高频写路径，因此只更新已有条目。
type objectCountTracker struct {
	clock clock.PassiveClock

	lock   sync.RWMutex
	counts map[string]*timestampedStats
}

// Set 更新资源计数；已存在时原地更新以避免重新分配。
func (t *objectCountTracker) Set(groupResource string, stats storage.Stats) {
	now := t.clock.Now()

	// 写锁
	t.lock.Lock()
	defer t.lock.Unlock()

	if item, ok := t.counts[groupResource]; ok {
		item.Stats = stats
		item.lastUpdatedAt = now
		return
	}

	t.counts[groupResource] = &timestampedStats{
		Stats:         stats,
		lastUpdatedAt: now,
	}
}

// Get 返回资源计数；陈旧数据仍返回数值，但附带 ObjectCountStaleErr 供调用方决策。
func (t *objectCountTracker) Get(groupResource string) (storage.Stats, error) {
	staleThreshold := t.clock.Now().Add(-staleTolerationThreshold)

	t.lock.RLock()
	defer t.lock.RUnlock()

	if item, ok := t.counts[groupResource]; ok {
		if item.lastUpdatedAt.Before(staleThreshold) {
			return item.Stats, ObjectCountStaleErr
		}
		return item.Stats, nil
	}
	return storage.Stats{}, ObjectCountNotFoundErr
}

// RunUntil 运行所有必要的维护任务。
func (t *objectCountTracker) RunUntil(stopCh <-chan struct{}) {
	// 每次都返回 false 表示“未完成”，从而按固定间隔持续执行 prune。
	wait.PollUntil(
		pruneInterval,
		func() (bool, error) {
			// 每个 pruneInterval 都执行清理
			return false, t.prune(pruneInterval)
		}, stopCh)
	klog.InfoS("StorageObjectCountTracker pruner is exiting")
}

// prune 删除超过阈值未更新的条目；资源被删除后计数应随之消失。
func (t *objectCountTracker) prune(threshold time.Duration) error {
	oldestLastUpdatedAtAllowed := t.clock.Now().Add(-threshold)

	// 写锁
	t.lock.Lock()
	defer t.lock.Unlock()

	for groupResource, count := range t.counts {
		if count.lastUpdatedAt.After(oldestLastUpdatedAtAllowed) {
			continue
		}
		delete(t.counts, groupResource)
	}

	return nil
}
