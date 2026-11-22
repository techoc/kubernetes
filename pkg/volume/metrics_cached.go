/*
Copyright 2014 The Kubernetes Authors.

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

package volume

import (
	"sync"
	"sync/atomic"
)

var _ MetricsProvider = &cachedMetrics{}

// cachedMetrics 包装另一个 MetricsProvider 并缓存其指标结果。
type cachedMetrics struct {
	wrapped       MetricsProvider
	resultError   error
	resultMetrics *Metrics
	once          cacheOnce
}

// NewCachedMetrics 返回一个包装 provider 并缓存其结果的 MetricsProvider。
func NewCachedMetrics(provider MetricsProvider) MetricsProvider {
	return &cachedMetrics{wrapped: provider}
}

// GetMetrics 调用被包装提供者的 GetMetrics 并缓存结果；出错时不缓存，
// 以便下次调用重试。参见 MetricsProvider.GetMetrics。
func (md *cachedMetrics) GetMetrics() (*Metrics, error) {
	md.once.cache(func() error {
		md.resultMetrics, md.resultError = md.wrapped.GetMetrics()
		return md.resultError
	})
	return md.resultMetrics, md.resultError
}

// cacheOnce 类似 sync.Once，但出错时不标记为完成，允许后续重试。
type cacheOnce struct {
	m    sync.Mutex
	done uint32
}

// cache 最多执行一次成功调用 f；失败时保持未完成后续可重试。
func (o *cacheOnce) cache(f func() error) {
	if atomic.LoadUint32(&o.done) == 1 {
		return
	}
	// 慢路径：加锁后再次确认。
	o.m.Lock()
	defer o.m.Unlock()
	if o.done == 0 {
		err := f()
		if err == nil {
			atomic.StoreUint32(&o.done, 1)
		}
	}
}
