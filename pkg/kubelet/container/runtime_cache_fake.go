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

import "context"

// TestRuntimeCache 内嵌 runtimeCache 并补充若干测试方法。
// 它必须声明在 container 包中才能访问 runtimeCache；
// 也不能放在 "..._test.go" 文件中，以便 runtime_cache_test.go 跨包引用
// （本包被 vendor 时，测试文件中的跨包声明无法通过点导入使用）。
type TestRuntimeCache struct {
	runtimeCache
}

// UpdateCacheWithLock 在持锁状态下刷新缓存。
func (r *TestRuntimeCache) UpdateCacheWithLock(ctx context.Context) error {
	r.Lock()
	defer r.Unlock()
	return r.updateCache(ctx)
}

// GetCachedPods 返回当前缓存的 Pod 列表。
func (r *TestRuntimeCache) GetCachedPods() []*Pod {
	r.Lock()
	defer r.Unlock()
	return r.pods
}

// NewTestRuntimeCache 创建 TestRuntimeCache 实例。
func NewTestRuntimeCache(getter podsGetter) *TestRuntimeCache {
	return &TestRuntimeCache{
		runtimeCache: runtimeCache{
			getter: getter,
		},
	}
}
