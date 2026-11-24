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

package cache

import (
	"container/heap"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// NewExpiring 创建并返回一个使用系统时钟的过期缓存。
func NewExpiring() *Expiring {
	return NewExpiringWithClock(clock.RealClock{})
}

// NewExpiringWithClock 与 NewExpiring 相同，但允许注入自定义时钟，便于测试时控制时间推进。
func NewExpiringWithClock(clock clock.Clock) *Expiring {
	return &Expiring{
		clock: clock,
		cache: make(map[interface{}]entry),
	}
}

// Expiring 是一个带逐条 TTL 的缓存映射，每个条目按照自己的过期时间失效。
type Expiring struct {
	// AllowExpiredGet 为 true 时，Get 不检查条目是否过期。
	// 仅当同一个 key 始终对应完全相同的 value 时才可启用；此时过期条目会继续被视为有效，
	// 直到下一次 Set 触发垃圾回收。不能在 Get 并发执行期间修改该字段。
	AllowExpiredGet bool

	clock clock.Clock

	// mu 保护下面的缓存状态及过期堆。
	mu sync.RWMutex
	// cache 是缓存实际使用的键值映射。
	cache map[interface{}]entry
	// generation 是缓存条目的轻量版本号。每个清理任务同时记录 key 和 generation；
	// 执行时只有 generation 与当前条目一致才允许删除。这样同一个 key 多次 Set 后，
	// 旧版本的清理任务不会误删后来写入的新值。
	// generation 的具体数值没有业务含义，只用于判断条目是否已被替换。
	generation uint64

	heap expiringHeap
}

type entry struct {
	val        interface{}
	expiry     time.Time
	generation uint64
}

// Get 根据 key 查询缓存；不存在或已过期时返回 (nil, false)。
func (c *Expiring) Get(key interface{}) (val interface{}, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.cache[key]
	if !ok {
		return nil, false
	}
	// 读取时也会检查过期时间，而不只依赖 GC；因此即使清理尚未运行，也不会返回已过期条目。
	if !c.AllowExpiredGet && !c.clock.Now().Before(e.expiry) {
		return nil, false
	}
	return e.val, true
}

// Set 写入或覆盖指定 key 的 value，并从本次调用时刻开始计算 ttl。
// 对同一个 key 再次调用 Set 可以延长或缩短其有效期。过期条目的物理清理由 Set 触发，
// 但 Get 会独立判断过期状态，因此尚未被 GC 删除的条目也不会被读取到。
func (c *Expiring) Set(key interface{}, val interface{}, ttl time.Duration) {
	now := c.clock.Now()
	expiry := now.Add(ttl)

	c.mu.Lock()
	defer c.mu.Unlock()

	// 递增 generation，使针对该 key 旧值安排的清理任务失效。
	c.generation++

	c.cache[key] = entry{
		val:        val,
		expiry:     expiry,
		generation: c.generation,
	}

	// 在把新过期记录压入堆之前，先同步清理已经过期的记录。
	c.gc(now)

	heap.Push(&c.heap, &expiringHeapEntry{
		key:        key,
		expiry:     expiry,
		generation: c.generation,
	})
}

// Delete 从缓存中删除指定 key，不要求条目已经过期。
func (c *Expiring) Delete(key interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.del(key, 0)
}

// del 删除指定 key 对应的条目。generation 指定预期删除的条目版本；如果当前版本已变化
// （例如旧清理任务执行前，该 key 已被重新 Set），则本次删除不执行。generation 为 0 时
// 忽略版本检查并直接删除。
//
// 调用 del 前必须持有写锁。
func (c *Expiring) del(key interface{}, generation uint64) {
	e, ok := c.cache[key]
	if !ok {
		return
	}
	// 清理任务版本过期时，不得删除同一 key 后来写入的新值。
	if generation != 0 && generation != e.generation {
		return
	}
	delete(c.cache, key)
}

// Len 返回缓存中当前保存的条目数，包括尚未被 GC 物理删除的过期条目。
func (c *Expiring) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

func (c *Expiring) gc(now time.Time) {
	for {
		// 堆为空，或堆顶条目尚未过期时，后续条目也不可能过期，可以结束本轮 GC。
		// heap[0] 是堆顶元素（下一条最早过期的记录）。需要注意，heap.Pop() 会先将
		// 堆顶与末尾元素交换，再调用 expiringHeap.Pop() 取出末尾元素。
		if len(c.heap) == 0 || now.Before(c.heap[0].expiry) {
			return
		}
		cleanup := heap.Pop(&c.heap).(*expiringHeapEntry)
		// 如果条目已被覆盖，del 会通过 generation 检查忽略旧记录；新条目有独立的过期堆记录。
		c.del(cleanup.key, cleanup.generation)
	}
}

type expiringHeapEntry struct {
	key        interface{}
	expiry     time.Time
	generation uint64
}

// expiringHeap 是按过期时间升序排列的最小堆。Expiring 将其作为优先队列，
// 每次只需检查堆顶即可找到下一条需要清理的记录。
type expiringHeap []*expiringHeapEntry

var _ heap.Interface = &expiringHeap{}

func (cq expiringHeap) Len() int {
	return len(cq)
}

func (cq expiringHeap) Less(i, j int) bool {
	return cq[i].expiry.Before(cq[j].expiry)
}

func (cq expiringHeap) Swap(i, j int) {
	cq[i], cq[j] = cq[j], cq[i]
}

func (cq *expiringHeap) Push(c interface{}) {
	*cq = append(*cq, c.(*expiringHeapEntry))
}

func (cq *expiringHeap) Pop() interface{} {
	c := (*cq)[cq.Len()-1]
	*cq = (*cq)[:cq.Len()-1]
	return c
}
