/*
Copyright 2025 The Kubernetes Authors.

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

package util

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/resource"
)

// cachingVerifier 包装给定的 Verifier，永久缓存其返回值。
type cachingVerifier struct {
	cache map[schema.GroupVersionKind]error
	mu    sync.RWMutex
	next  resource.Verifier
}

// newCachingVerifier 使用给定的底层 verifier 创建一个新的缓存。
func newCachingVerifier(next resource.Verifier) *cachingVerifier {
	return &cachingVerifier{
		cache: make(map[schema.GroupVersionKind]error),
		next:  next,
	}
}

// HasSupport 实现 resource.Verifier。它永久缓存来自底层 verifier 的返回值。
func (cv *cachingVerifier) HasSupport(gvk schema.GroupVersionKind) error {
	// 尝试获取缓存的值。
	cv.mu.RLock()
	err, ok := cv.cache[gvk]
	cv.mu.RUnlock()
	if ok {
		return err
	}

	// 缓存未命中，获取实际结果。
	err = cv.next.HasSupport(gvk)

	// 更新缓存。
	cv.mu.Lock()
	cv.cache[gvk] = err
	cv.mu.Unlock()
	return err
}
