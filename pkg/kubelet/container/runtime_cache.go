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

//go:generate mockery
package container

import (
	"context"
	"sync"
	"time"
)

// RuntimeCache 提供获取缓存 Pod 列表的接口。
type RuntimeCache interface {
	// GetPods 返回缓存的 Pod 列表，必要时刷新。
	GetPods(context.Context) ([]*Pod, error)
	// ForceUpdateIfOlder 在缓存早于指定时间时强制刷新。
	ForceUpdateIfOlder(context.Context, time.Time) error
}

// podsGetter 抽象从容器运行时获取 Pod 列表的能力。
type podsGetter interface {
	// GetPods 返回 Pod 列表，bool 参数表示是否强制刷新。
	GetPods(context.Context, bool) ([]*Pod, error)
}

// NewRuntimeCache 创建容器运行时缓存。
func NewRuntimeCache(getter podsGetter, cachePeriod time.Duration) (RuntimeCache, error) {
	return &runtimeCache{
		getter:      getter,
		cachePeriod: cachePeriod,
	}, nil
}

// runtimeCache 缓存 Pod 列表。更新 Pod 前会先记录时间戳（cacheTime），
// 因此时间戳不会新于 Pod 数据（可能略旧），且始终单调递增。
// 调用方不应修改 GetPods 返回的 Pod 对象。
type runtimeCache struct {
	sync.Mutex
	// getter 是用于刷新缓存的底层容器运行时。
	getter podsGetter
	// cachePeriod 是缓存刷新间隔。
	cachePeriod time.Duration
	// cacheTime 是缓存最近一次更新的时间。
	cacheTime time.Time
	// pods 是缓存的 Pod 列表。
	pods []*Pod
}

// GetPods 返回缓存的 Pod；缓存过期时先拉取最新 Pod 再返回。
func (r *runtimeCache) GetPods(ctx context.Context) ([]*Pod, error) {
	r.Lock()
	defer r.Unlock()
	// 缓存过期时才重新拉取。
	if time.Since(r.cacheTime) > r.cachePeriod {
		if err := r.updateCache(ctx); err != nil {
			return nil, err
		}
	}
	return r.pods, nil
}

// ForceUpdateIfOlder 在缓存早于 minExpectedCacheTime 时刷新缓存。
func (r *runtimeCache) ForceUpdateIfOlder(ctx context.Context, minExpectedCacheTime time.Time) error {
	r.Lock()
	defer r.Unlock()
	// 缓存早于期望时间时强制刷新。
	if r.cacheTime.Before(minExpectedCacheTime) {
		return r.updateCache(ctx)
	}
	return nil
}

// updateCache 拉取最新 Pod 并更新缓存与时间戳。
func (r *runtimeCache) updateCache(ctx context.Context) error {
	pods, timestamp, err := r.getPodsWithTimestamp(ctx)
	if err != nil {
		return err
	}
	r.pods, r.cacheTime = pods, timestamp
	return nil
}

// getPodsWithTimestamp 记录时间戳并从 getter 获取 Pod。
func (r *runtimeCache) getPodsWithTimestamp(ctx context.Context) ([]*Pod, time.Time, error) {
	// 必须先记录时间戳再获取 Pod，避免缓存时间晚于数据导致陈旧判断错误。
	timestamp := time.Now()
	pods, err := r.getter.GetPods(ctx, false)
	return pods, timestamp, err
}
