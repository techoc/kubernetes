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

package cacher

import (
	"sync"
	"time"

	"k8s.io/apiserver/pkg/storage"

	"k8s.io/utils/clock"
)

const (
	// compactorPollPeriod determines period of reading compaction revision from storage.
	// 15 秒的轮询间隔在“及时释放历史事件”和“避免频繁访问 etcd”之间取值；
	// CompactionRevision 是 etcd 元数据读取，成本不高但不应每个请求都做。
	compactorPollPeriod = 15 * time.Second
)

// newCompactor 构造 compactor；clock 可注入以便测试控制时间。
func newCompactor(store storage.Interface, wc *watchCache, clock clock.Clock) *compactor {
	pr := &compactor{
		clock: clock,
		store: store,
		wc:    wc,
	}
	return pr
}

// compactor 周期性把底层存储的 compact revision 同步到 watchCache，
// 使 watch 缓存丢弃已压缩的历史事件，避免向客户端发送会被后续
// “Too large resource version” / 数据不一致影响的旧事件。
type compactor struct {
	clock clock.Clock
	store storage.Interface
	wc    *watchCache

	// lock 保护 compactRevision，使其可被其他 goroutine 安全读取。
	lock            sync.Mutex
	compactRevision int64
}

// Run 直到 stopCh 关闭前，每 compactorPollPeriod 执行一次压缩检查。
func (c *compactor) Run(stopCh <-chan struct{}) {
	timer := c.clock.NewTimer(compactorPollPeriod)
	defer timer.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-timer.C():
			c.compactIfNeeded()
			timer.Reset(compactorPollPeriod)
		}
	}
}

// compactIfNeeded 在存储报告的 revision 前进时推进 watchCache 的压缩点。
// rev==0 表示存储未配置压缩；与已记录值相同则无需重复动作。
func (c *compactor) compactIfNeeded() {
	rev := c.store.CompactRevision()
	if rev == 0 {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	// 单调性保护：压缩点只能前进，不能回退（例如 etcd 回滚或读取到旧值）。
	if rev <= c.compactRevision {
		return
	}
	c.wc.storage.Compact(uint64(rev))
	c.compactRevision = rev
}

// Revision 返回最近一次同步的压缩 revision。
func (c *compactor) Revision() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.compactRevision
}
