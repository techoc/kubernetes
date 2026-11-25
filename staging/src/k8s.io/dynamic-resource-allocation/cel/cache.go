/*
Copyright 2024 The Kubernetes Authors.

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

// Package cel 提供 CEL 表达式的编译、缓存与运行时错误处理辅助能力，
// 供动态资源分配（DRA）相关组件使用。
package cel

import (
	"sync"

	"k8s.io/utils/keymutex"
	"k8s.io/utils/lru"
)

// Cache 是已编译 CEL 表达式的线程安全 LRU 缓存。字段含义：
// compileMutex 按缓存键加锁，保证同一表达式不会被并发重复编译；
// cacheMutex 保护底层 LRU 容器的并发读写；
// cache 保存编译结果，超出容量时淘汰最旧条目；
// compiler 根据启用的特性集合选取合适的编译器。
type Cache struct {
	compileMutex keymutex.KeyMutex
	cacheMutex   sync.RWMutex
	cache        *lru.Cache
	compiler     *compiler
}

// NewCache 创建一个缓存：maxCacheEntries 限定最多缓存的条目数，
// 超过后最旧的条目会被淘汰；features 用于选择与之匹配的编译器。
func NewCache(maxCacheEntries int, features Features) *Cache {
	// 用哈希键锁和带容量上限的 LRU 容器初始化缓存，
	// 编译器由传入的特性集合决定。
	return &Cache{
		compileMutex: keymutex.NewHashed(0),
		cache:        lru.New(maxCacheEntries),
		compiler:     GetCompiler(features),
	}
}

// GetOrCompile 先查缓存，命中则直接返回；未命中则编译并只缓存成功的结果。
// 该方法关闭成本估算，用于普通的选择器表达式。
func (c *Cache) GetOrCompile(expression string) CompilationResult {
	// 使用 "selector" 作为缓存命名空间，并关闭成本估算。
	return c.getOrCompile(expression, "selector", Options{DisableCostEstimation: true})
}

// GetOrCompileDerivedAttribute 先查独立的 "derived" 缓存，命中则直接返回已有结果；
// 未命中时启用 DerivedAttribute 选项进行编译，只缓存成功结果，并返回本次编译结果。
func (c *Cache) GetOrCompileDerivedAttribute(expression string) CompilationResult {
	// 派生属性允许返回非布尔结果，因此需要与选择器不同的编译选项。
	return c.getOrCompile(expression, "derived", Options{
		DisableCostEstimation: true,
		DerivedAttribute:      true,
	})
}

// getOrCompile 是编译与缓存的通用实现：按命名空间和表达式构造缓存键并加键锁，
// 命中缓存直接返回，未命中则编译，且只缓存编译成功的结果。
func (c *Cache) getOrCompile(expression string, cacheScope string, opts Options) CompilationResult {
	cacheKey := cacheScope + ":" + expression
	// 命名空间前缀使同一表达式在选择器与派生属性场景下使用不同的缓存键。
	// 编译 CEL 表达式开销较大，按缓存键加锁比并行重复编译更划算。
	c.compileMutex.LockKey(cacheKey)
	// 用 defer 保证任何返回路径都会释放对应键上的锁。
	//nolint:errcheck // 仅对未知 key 返回错误，此处不属于该情况。
	defer c.compileMutex.UnlockKey(cacheKey)

	// 先读缓存；命中则直接返回已有的编译结果。
	cached := c.get(cacheKey)
	if cached != nil {
		return *cached
	}

	// 未命中时执行编译；失败的结果不入缓存，避免反复缓存同样的错误。
	expr := c.compiler.CompileCELExpression(expression, opts)
	if expr.Error == nil {
		c.add(cacheKey, &expr)
	}
	return expr
}

// add 在写锁保护下把编译结果写入 LRU 缓存。
func (c *Cache) add(expression string, expr *CompilationResult) {
	// LRU 写入可能淘汰旧条目，必须独占访问底层容器。
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.cache.Add(expression, expr)
}

// get 在读锁保护下查询缓存；未命中时返回 nil。
func (c *Cache) get(expression string) *CompilationResult {
	// 多个读操作之间可以并发，使用读锁即可。
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	// lru.Cache.Get 的第二个返回值表示是否命中。
	expr, found := c.cache.Get(expression)
	if !found {
		return nil
	}
	// 缓存中保存的是编译结果指针，做类型断言还原。
	return expr.(*CompilationResult)
}
