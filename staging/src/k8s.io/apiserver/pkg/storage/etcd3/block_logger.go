/*
Copyright The Kubernetes Authors.

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

package etcd3

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// blockLogger is a rate-limited logger that tracks how long a watchChan is blocked.
//
// 用于诊断 etcd watch 分发链路上的阻塞：单个 watcher 消费慢会反压整个 watchCache。
// 采用固定时间窗口聚合而非逐事件打日志，避免慢消费者本身再放大日志量。
type blockLogger struct {
	interval      time.Duration
	threshold     time.Duration
	msg           string
	objectType    string
	groupResource schema.GroupResource

	clock clock.Clock

	// mu 保护窗口内的累计状态；recordWait 会被多个 watch goroutine 并发调用。
	mu          sync.Mutex
	lastLogTime time.Time
	eventCount  int
	timeWaiting time.Duration
}

// newBlockLogger 创建记录器并初始化窗口起点，保证第一个窗口从构造时刻算起。
func newBlockLogger(interval time.Duration, threshold time.Duration, msg string, objectType string, groupResource schema.GroupResource, clock clock.Clock) *blockLogger {
	l := &blockLogger{
		interval:      interval,
		threshold:     threshold,
		msg:           msg,
		objectType:    objectType,
		groupResource: groupResource,
		clock:         clock,
	}
	l.lastLogTime = l.clock.Now()
	return l
}

// recordWait 累计一次阻塞时长，并在窗口到期时决定是否输出日志。
//
// 窗口内总等待时长低于阈值就静默丢弃，只有持续阻塞才值得告警；这与"偶发一次慢"
// 的正常抖动区分开，减少噪音。
func (l *blockLogger) recordWait(waitTime time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.eventCount++
	l.timeWaiting += waitTime

	now := l.clock.Now()
	window := now.Sub(l.lastLogTime)
	if window < l.interval {
		return
	}

	if l.timeWaiting > l.threshold {
		klog.V(3).InfoS(l.msg,
			"objectType", l.objectType,
			"groupResource", l.groupResource,
			"eventCount", l.eventCount,
			"timeWaiting", l.timeWaiting,
			"window", window,
		)
	}
	// 无论是否打印都重置窗口，避免长时间阻塞时每来一个事件就重复输出。
	l.eventCount = 0
	l.timeWaiting = 0
	l.lastLogTime = now
}
