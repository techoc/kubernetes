/*
Copyright 2015 The Kubernetes Authors.

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

package queue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
)

// WorkQueue 允许按时间戳排队条目：时间戳到期后条目才算可处理。
type WorkQueue interface {
	// GetWork 取出并返回所有已就绪的条目。
	GetWork() []types.UID
	// Enqueue 插入新条目，已存在时覆盖。
	Enqueue(item types.UID, delay time.Duration)
}

// basicWorkQueue 是基于 map 与互斥锁的 WorkQueue 实现。
type basicWorkQueue struct {
	clock clock.Clock
	lock  sync.Mutex
	queue map[types.UID]time.Time
}

var _ WorkQueue = &basicWorkQueue{}

// NewBasicWorkQueue 使用给定时钟创建基础的 WorkQueue。
func NewBasicWorkQueue(clock clock.Clock) WorkQueue {
	queue := make(map[types.UID]time.Time)
	return &basicWorkQueue{queue: queue, clock: clock}
}

// GetWork 返回所有到期条目，并从队列中移除。
func (q *basicWorkQueue) GetWork() []types.UID {
	q.lock.Lock()
	defer q.lock.Unlock()
	now := q.clock.Now()
	var items []types.UID
	for k, v := range q.queue {
		if v.Before(now) {
			items = append(items, k)
			delete(q.queue, k)
		}
	}
	return items
}

// Enqueue 设置条目的到期时间；已存在时覆盖旧时间。
func (q *basicWorkQueue) Enqueue(item types.UID, delay time.Duration) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queue[item] = q.clock.Now().Add(delay)
}
