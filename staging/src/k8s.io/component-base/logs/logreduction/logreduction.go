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

// Package logreduction 按父级 ID（如容器 ID）对重复日志做限流：
// 相同消息在每个间隔内最多打印一次，用于抑制崩溃循环等场景的日志风暴。
package logreduction

import (
	"sync"
	"time"
)

// nowfunc 允许测试替换时钟，避免依赖真实时间导致用例不稳定。
var nowfunc = time.Now

// LogReduction provides a filter for consecutive identical log messages;
// a message will be printed no more than once per interval.
// If a string of messages is interrupted by a different message,
// the interval timer will be reset.
// LogReduction 为连续相同日志提供过滤：同一 parentID 下相同消息在 interval 内
// 最多打印一次；一旦出现不同消息，计时器重置，新的相同消息会立即打印。
type LogReduction struct {
	lastError           map[string]string
	errorPrinted        map[string]time.Time
	errorMapLock        sync.Mutex
	identicalErrorDelay time.Duration
}

// NewLogReduction 初始化 LogReduction；identicalErrorDelay 是相同消息的最小打印间隔。
func NewLogReduction(identicalErrorDelay time.Duration) *LogReduction {
	l := new(LogReduction)
	l.lastError = make(map[string]string)
	l.errorPrinted = make(map[string]time.Time)
	l.identicalErrorDelay = identicalErrorDelay
	return l
}

// cleanupErrorTimeouts 删除已超过间隔的条目，防止 map 随 parentID 数量无限增长。
// 调用方必须持有 errorMapLock。
func (l *LogReduction) cleanupErrorTimeouts() {
	for name, timeout := range l.errorPrinted {
		if nowfunc().Sub(timeout) >= l.identicalErrorDelay {
			delete(l.errorPrinted, name)
			delete(l.lastError, name)
		}
	}
}

// ShouldMessageBePrinted 根据该消息上次打印的时间决定是否放行。
// parentID 隔离不同来源（例如不同容器），使限流互不影响。
func (l *LogReduction) ShouldMessageBePrinted(message string, parentID string) bool {
	l.errorMapLock.Lock()
	defer l.errorMapLock.Unlock()
	l.cleanupErrorTimeouts()
	lastMsg, ok := l.lastError[parentID]
	lastPrinted, ok1 := l.errorPrinted[parentID]
	// 以下任一情况都放行：该 parent 尚无记录、消息不同、或已超过间隔。
	if !ok || !ok1 || message != lastMsg || nowfunc().Sub(lastPrinted) >= l.identicalErrorDelay {
		l.errorPrinted[parentID] = nowfunc()
		l.lastError[parentID] = message
		return true
	}
	return false
}

// ClearID 清除与某个 parent（例如容器 ID）相关的限流记录，
// 用于容器重建后让旧状态立即失效。
func (l *LogReduction) ClearID(parentID string) {
	l.errorMapLock.Lock()
	defer l.errorMapLock.Unlock()
	delete(l.lastError, parentID)
	delete(l.errorPrinted, parentID)
}
