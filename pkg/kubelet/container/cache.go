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

package container

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// Cache 保存各 Pod 的 PodStatus，代表容器运行时中所有可见的 Pod/容器。
// 所有缓存条目至少与全局时间戳（由 UpdateTime() 设置）一样新，
// 个别条目可能比全局时间戳略新。若运行时不知道某个 Pod 的任何状态，
// Cache 会返回一个只填充了 ID 的空 PodStatus。
//
// 负责填充缓存的组件应调用 Delete() 显式释放缓存条目。
type Cache interface {
	ROCache
	// Set 仅在给定时间戳比缓存数据更新时，才为 Pod 设置 PodStatus；
	// 返回缓存是否被更新。
	Set(types.UID, *PodStatus, error, time.Time) (updated bool)
	// SetObservedTime 修改 Pod 在缓存中的观测时间戳，表示运行时最近确认过
	// 该 Pod 状态是准确的，即使状态本身没有变化。
	SetObservedTime(types.UID, time.Time)
	// Delete 删除指定 Pod 的缓存条目。
	Delete(types.UID)
	// UpdateTime 更新缓存的全局时间戳。
	UpdateTime(time.Time)
}

// ROCache 是 Pod 状态缓存的只读接口，提供两种获取 PodStatus 的方式：
// 非阻塞的 Get() 与阻塞的 GetNewerThan()。
type ROCache interface {
	// Get 非阻塞地返回 Pod 状态。
	Get(types.UID) (*PodStatus, error)
	// GetNewerThan 是阻塞调用，只在状态比给定时间更新时才返回。
	GetNewerThan(types.UID, time.Time) (*PodStatus, error)
}

// data 是缓存中单个 Pod 的状态记录。
type data struct {
	// status 是 Pod 的状态。
	status *PodStatus
	// err 是检查 Pod 时得到的错误。
	err error
	// modified 是数据最后一次修改的时间。
	modified time.Time
	// observedTime 是运行时最后一次单独观测/确认该数据的时间。
	// 实际观测时间应取 observedTime 与全局缓存时间戳中较新的一个。
	// 当 Pod 被重新列举但状态没有变化时会更新该字段。
	observedTime time.Time
}

// subRecord 是 GetNewerThan 的一个订阅记录。
type subRecord struct {
	time time.Time
	ch   chan *data
}

// cache 实现 Cache 接口。
type cache struct {
	// lock 保护所有内部数据结构。
	lock sync.RWMutex
	// pods 保存各 Pod 的状态。
	pods map[types.UID]*data
	// timestamp 是表示缓存数据新鲜程度的全局时间戳：所有缓存内容至少比它新。
	// 初始化后该时间戳为 nil，只有在缓存准备好对外提供状态时才会被设置。
	timestamp *time.Time
	// subscribers 保存各 Pod 的订阅记录。
	subscribers map[types.UID][]*subRecord
}

// NewCache 创建 Pod 缓存。
func NewCache() Cache {
	return &cache{pods: map[types.UID]*data{}, subscribers: map[types.UID][]*subRecord{}}
}

// Get 返回 Pod 的 PodStatus；调用方不应修改返回的对象。
func (c *cache) Get(id types.UID) (*PodStatus, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	d := c.get(id)
	return d.status, d.err
}

// GetNewerThan 阻塞等待，直到缓存中存在比 minTime 更新的 Pod 状态。
func (c *cache) GetNewerThan(id types.UID, minTime time.Time) (*PodStatus, error) {
	ch := c.subscribe(id, minTime)
	// 订阅后从通道读取结果；结果可能由 notify 发送，也可能在订阅时立即发送。
	d := <-ch
	return d.status, d.err
}

// Set 仅在数据比缓存更新时才为 Pod 设置 PodStatus。
func (c *cache) Set(id types.UID, status *PodStatus, err error, timestamp time.Time) (updated bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if utilfeature.DefaultFeatureGate.Enabled(features.EventedPLEG) {
		// 仅当缓存中尚无该值，或缓存时间戳早于本次更新时间时，才写入缓存。
		if cachedVal, ok := c.pods[id]; ok && cachedVal.modified.After(timestamp) {
			return false
		}
	}

	c.pods[id] = &data{status: status, err: err, modified: timestamp, observedTime: timestamp}
	// 更新缓存后唤醒满足时间条件的订阅者。
	c.notify(id, timestamp)
	return true
}

// SetObservedTime 修改 Pod 在缓存中的观测时间戳，并在需要时通知订阅者。
func (c *cache) SetObservedTime(id types.UID, timestamp time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if d, ok := c.pods[id]; ok {
		d.observedTime = timestamp
	}
	// 条件满足时唤醒对应的订阅者。
	c.notify(id, timestamp)
}

// Delete 删除 Pod 的缓存条目。
func (c *cache) Delete(id types.UID) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.pods, id)
}

// UpdateTime 修改缓存的全局时间戳，并在需要时通知订阅者。
func (c *cache) UpdateTime(timestamp time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.timestamp = &timestamp
	// Notify all the subscribers if the condition is met.
	for id := range c.subscribers {
		c.notify(id, *c.timestamp)
	}
}

// makeDefaultData 返回只带 ID 的默认（空）Pod 状态。
func makeDefaultData(id types.UID) *data {
	return &data{status: &PodStatus{ID: id}, err: nil}
}

// get 返回指定 Pod 的缓存数据；未命中时返回默认空状态。
func (c *cache) get(id types.UID) *data {
	d, ok := c.pods[id]
	if !ok {
		// 缓存应保存容器运行时已知的 *所有* Pod/容器信息。未命中说明上次查询运行时
		// 时该 Pod 没有任何状态，实际含义是不存在与该 Pod 关联的可见 Pod/容器。
		// 此时返回一个默认的（几乎为空的）PodStatus 来表示这一情况。
		return makeDefaultData(id)
	}
	return d
}

// getIfNewerThan 返回比给定时间更新的数据，否则返回 nil。
// 调用方应已持有锁。
func (c *cache) getIfNewerThan(id types.UID, minTime time.Time) *data {
	d, ok := c.pods[id]
	globalTimestampIsNewer := (c.timestamp != nil && c.timestamp.After(minTime))
	if !ok && globalTimestampIsNewer {
		// 状态未缓存，但全局时间戳比 minTime 新，返回默认状态。
		return makeDefaultData(id)
	}
	if ok && (globalTimestampIsNewer || d.modified.After(minTime) || d.observedTime.After(minTime)) {
		// 状态已缓存时，满足以下任一条件就返回该状态：
		//   * 状态在 minTime 之后被修改过
		//   * 状态在 minTime 之后被观测过
		//   * 缓存的全局时间戳比 minTime 新。
		return d
	}
	// Pod 状态尚未就绪。
	return nil
}

// notify 在满足条件时向指定 Pod 的订阅者发送通知。调用方应已持有锁。
func (c *cache) notify(id types.UID, timestamp time.Time) {
	list, ok := c.subscribers[id]
	if !ok {
		// 没有订阅者需要通知。
		return
	}
	newList := []*subRecord{}
	for i, r := range list {
		if timestamp.Before(r.time) {
			// 不满足时间要求，保留该订阅记录。
			newList = append(newList, list[i])
			continue
		}
		// 一次性订阅：发送结果后关闭通道，避免调用方永久阻塞。
		r.ch <- c.get(id)
		close(r.ch)
	}
	if len(newList) == 0 {
		delete(c.subscribers, id)
	} else {
		c.subscribers[id] = newList
	}
}

// subscribe 注册订阅并返回用于接收结果的通道。
func (c *cache) subscribe(id types.UID, timestamp time.Time) chan *data {
	ch := make(chan *data, 1)
	c.lock.Lock()
	defer c.lock.Unlock()
	d := c.getIfNewerThan(id, timestamp)
	if d != nil {
		// 缓存条目已就绪时立即发送数据并返回。
		ch <- d
		return ch
	}
	// 否则登记订阅记录，等待后续 notify 唤醒。
	c.subscribers[id] = append(c.subscribers[id], &subRecord{time: timestamp, ch: ch})
	return ch
}
