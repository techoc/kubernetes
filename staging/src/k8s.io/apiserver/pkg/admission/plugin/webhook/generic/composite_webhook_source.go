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

// compositeWebhookSource combines multiple webhook sources into a single source.
// Static (manifest-based) webhooks are returned before API-based webhooks.
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

// NewCompositeWebhookSource creates a webhook source that combines static and API-based sources.
// Static webhooks are evaluated first, followed by API-based webhooks.
// If staticSource is nil, only apiSource webhooks are returned.
func NewCompositeWebhookSource(staticSource, apiSource Source) Source {
	if staticSource == nil {
		return apiSource
	}
	return &compositeWebhookSource{
		staticSource: staticSource,
		apiSource:    apiSource,
	}
}

// Webhooks returns all webhook accessors from both sources.
// Static webhooks come first, followed by API-based webhooks.
// The combined slice is cached and reused when the underlying slices haven't changed.
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
	// Re-check under write lock.
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

// slicesAreEqual reports whether two slices share the same backing array and length.
//
// 只比较首元素地址：只要底层来源返回同一个切片（即使内容被原地修改），就认为
// 未变化。这依赖来源返回稳定切片的约定，换来的是零分配的比较。
func slicesAreEqual[T any](a, b []T) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

// HasSynced returns true only when both sources have synced.
//
// nil 来源视为已同步：未配置的来源不阻塞准入链启动。
func (c *compositeWebhookSource) HasSynced() bool {
	staticSynced := c.staticSource == nil || c.staticSource.HasSynced()
	apiSynced := c.apiSource == nil || c.apiSource.HasSynced()
	return staticSynced && apiSynced
}
