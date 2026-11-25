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

// Package generic 提供 webhook 准入插件的通用调度框架：从静态配置与
// ValidatingAdmissionPolicy 等 API 来源收集 webhook，并统一调用它们。
package generic

import (
	"sync"

	"k8s.io/apiserver/pkg/admission/plugin/webhook"
)

// compositeWebhookSource 把多个 webhook 来源合并为单一来源。
// 静态（基于 manifest）的 webhook 排在基于 API 的 webhook 之前。
//
// 缓存 last* 字段是为了避免每次准入请求都重新拼接切片：Webhooks() 在热路径上
// 被频繁调用，而底层来源通常长时间不变。比较的是切片底层数组指针，而不是
// 逐元素比较，以保持 O(1) 判定。
type compositeWebhookSource struct {
	staticSource Source
	apiSource    Source

	mu              sync.RWMutex
	lastStaticSlice []webhook.WebhookAccessor
	lastAPISlice    []webhook.WebhookAccessor
	lastCombined    []webhook.WebhookAccessor
}

var _ Source = &compositeWebhookSource{}

// NewCompositeWebhookSource 创建一个合并静态来源与 API 来源的 webhook 来源。
// 静态 webhook 先被求值，其后是基于 API 的 webhook。
// 若 staticSource 为 nil，则只返回 apiSource 的 webhook。
func NewCompositeWebhookSource(staticSource, apiSource Source) Source {
	if staticSource == nil {
		return apiSource
	}
	return &compositeWebhookSource{
		staticSource: staticSource,
		apiSource:    apiSource,
	}
}

// Webhooks 返回两个来源的全部 webhook accessor。
// 静态 webhook 在前，随后是基于 API 的 webhook。
// 当底层切片未变化时，合并后的切片会被缓存并复用。
func (c *compositeWebhookSource) Webhooks() []webhook.WebhookAccessor {
	var staticSlice, apiSlice []webhook.WebhookAccessor
	if c.staticSource != nil {
		staticSlice = c.staticSource.Webhooks()
	}
	if c.apiSource != nil {
		apiSlice = c.apiSource.Webhooks()
	}

	// 先在读锁下快速判断缓存是否仍然有效，命中时不进入写锁。
	c.mu.RLock()
	if slicesAreEqual(c.lastStaticSlice, staticSlice) && slicesAreEqual(c.lastAPISlice, apiSlice) {
		combined := c.lastCombined
		c.mu.RUnlock()
		return combined
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	// 在写锁下重新检查。
	// 可能在释放读锁到获取写锁之间，另一个 goroutine 已经完成重建。
	if slicesAreEqual(c.lastStaticSlice, staticSlice) && slicesAreEqual(c.lastAPISlice, apiSlice) {
		return c.lastCombined
	}

	combined := make([]webhook.WebhookAccessor, 0, len(staticSlice)+len(apiSlice))
	combined = append(combined, staticSlice...)
	combined = append(combined, apiSlice...)

	c.lastStaticSlice = staticSlice
	c.lastAPISlice = apiSlice
	c.lastCombined = combined
	return combined
}

// slicesAreEqual 报告两个切片是否共享同一底层数组且长度相同。
//
// 只比较首元素地址：只要底层来源返回同一个切片（即使内容被原地修改），就认为
// 未变化。这依赖来源返回稳定切片的约定，换来的是零分配的比较。
func slicesAreEqual[T any](a, b []T) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

// HasSynced 仅当两个来源都已同步时返回 true。
//
// nil 来源视为已同步：未配置的来源不阻塞准入链启动。
func (c *compositeWebhookSource) HasSynced() bool {
	staticSynced := c.staticSource == nil || c.staticSource.HasSynced()
	apiSynced := c.apiSource == nil || c.apiSource.HasSynced()
	return staticSynced && apiSynced
}
