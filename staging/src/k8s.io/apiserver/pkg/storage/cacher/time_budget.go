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

package cacher

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

const (
	// 每秒补充 50ms 的预算，即最多使用 5% 的时间做这类工作。
	refreshPerSecond = 50 * time.Millisecond
	// 单次最多可累积 100ms，避免长时间空闲后突然占用过多。
	maxBudget = 100 * time.Millisecond
)

// timeBudget 维护一份可消耗、会周期性补充的时间额度。用法如下：
//
//	budget := newTimeBudget(...)
//	...
//	timeout := budget.takeAvailable()
//	// Now you can spend at most timeout on doing stuff
//	...
//	// If you didn't use all timeout, return what you didn't use
//	budget.returnUnused(<unused part of timeout>)
//
// 不建议多线程并发使用：先取走全部额度的调用方会让后来者拿到 0，
// 即使前者稍后可能归还部分额度。
//
// 该预算用于限制 cacher 中非关键工作（如事件分发）的耗时，保护 watch
// 延迟敏感路径。虽然实现有锁，但语义上不支持并发争用。
type timeBudget interface {
	takeAvailable() time.Duration
	returnUnused(unused time.Duration)
}

// timeBudgetImpl 以“按流逝时间线性累积”的方式实现预算：不是定时器，
// 而是在每次访问时按 now-last 计算应补充的额度，因此没有后台 goroutine。
type timeBudgetImpl struct {
	sync.Mutex
	clock     clock.Clock
	budget    time.Duration
	maxBudget time.Duration
	refresh   time.Duration
	// last store last access time
	last time.Time
}

// newTimeBudget 构造初始额度为零的新预算。
func newTimeBudget() timeBudget {
	result := &timeBudgetImpl{
		clock:     clock.RealClock{},
		budget:    time.Duration(0),
		refresh:   refreshPerSecond,
		maxBudget: maxBudget,
	}
	result.last = result.clock.Now()
	return result
}

// takeAvailable 返回当前全部可用预算并重置为零。
// 调用方必须把未用完的部分通过 returnUnused 归还，否则这部分额度丢失。
func (t *timeBudgetImpl) takeAvailable() time.Duration {
	t.Lock()
	defer t.Unlock()
	// 自上次访问以来累积的额度
	now := t.clock.Now()
	acc := now.Sub(t.last).Seconds() * t.refresh.Seconds()
	if acc < 0 {
		acc = 0
	}
	// 更新当前额度并记录时间
	if t.budget = t.budget + time.Duration(acc*1e9); t.budget > t.maxBudget {
		t.budget = t.maxBudget
	}
	t.last = now
	result := t.budget
	t.budget = time.Duration(0)
	return result
}

// returnUnused 把未消耗掉的预算加回去。
// 负值意味着实际超支，此时不做任何补偿（超支部分无法追回）。
func (t *timeBudgetImpl) returnUnused(unused time.Duration) {
	t.Lock()
	defer t.Unlock()
	if unused < 0 {
		// 实际用量超出允许值时不做任何补偿。
		return
	}
	// 直接把未用额度加回预算；takeAvailable() 会另行计入经过的时间。
	if t.budget = t.budget + unused; t.budget > t.maxBudget {
		t.budget = t.maxBudget
	}
}
