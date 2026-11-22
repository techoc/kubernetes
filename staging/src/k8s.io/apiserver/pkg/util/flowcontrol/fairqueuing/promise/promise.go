/*
Copyright 2019 The Kubernetes Authors.

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

package promise

import (
	"context"
	"sync"
)

// promise implements the WriteOnce interface.
// 并发语义：Set 只生效一次；Get 在未设置前阻塞，直到被 Set 或 doneCtx 结束。
type promise struct {
	doneCtx context.Context
	doneVal interface{}
	setCh   chan struct{}
	onceler sync.Once
	value   interface{}
}

var _ WriteOnce = &promise{}

// NewWriteOnce makes a new thread-safe WriteOnce.
//
// If `initial` is non-nil then that value is Set at creation time.
//
// If a `Get` is waiting soon after the channel associated with the
// `doneCtx` becomes selectable (which never happens for the nil
// channel) then `Set(doneVal)` effectively happens at that time.
//
// doneCtx 用于给等待设一个上界：超时后以 doneVal 作为结果，避免 Get 永久阻塞。
func NewWriteOnce(initial interface{}, doneCtx context.Context, doneVal interface{}) WriteOnce {
	p := &promise{
		doneCtx: doneCtx,
		doneVal: doneVal,
		setCh:   make(chan struct{}),
	}
	if initial != nil {
		p.Set(initial)
	}
	return p
}

// Get 等待值被设置；若 doneCtx 先结束，则以 doneVal 完成设置。
func (p *promise) Get() interface{} {
	select {
	case <-p.setCh:
	case <-p.doneCtx.Done():
		// 超时路径也走 Set，保证后续并发 Get 不会重复等待。
		p.Set(p.doneVal)
	}
	return p.value
}

// Set 写入值并唤醒等待者；仅第一次调用生效，返回是否由本次调用写入。
func (p *promise) Set(value interface{}) bool {
	var ans bool
	p.onceler.Do(func() {
		p.value = value
		close(p.setCh)
		ans = true
	})
	return ans
}
