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

package generic

import (
	"context"
	"sync"
)

// HookSource 是静态来源（如 manifest 文件监听）能提供的最小接口：只要求给出
// 当前 hook 列表和同步状态，不强制实现 Run。
type HookSource[H Hook] interface {
	Hooks() []H
	HasSynced() bool
}

// 组合来源封装两类策略：来自集群内配置的静态策略与来自 API 对象的动态策略。
// 静态策略先返回，意味着平台下发的基线策略先于用户通过 API 定义的策略生效。
type compositePolicySource[H Hook] struct {
	staticSource HookSource[H]
	apiSource    Source[H]

	// 缓存上一次合并结果及其两个输入切片。比较依据是底层数组指针而非逐元素
	// 比较：上层只会在内容变化时替换切片，复用同一底层数组即表示未变。
	mu           sync.RWMutex
	lastStatic   []H
	lastAPI      []H
	lastCombined []H
}

// 编译期断言：组合来源必须满足 Source 接口。
var _ Source[Hook] = &compositePolicySource[Hook]{}

// NewCompositePolicySource 创建一个组合静态来源与 API 来源的策略源：
// 静态策略先于 API 策略返回。
// 未配置静态来源时直接返回 apiSource，省去无意义的包装与合并开销。
func NewCompositePolicySource[H Hook](staticSource HookSource[H], apiSource Source[H]) Source[H] {
	if staticSource == nil {
		return apiSource
	}
	return &compositePolicySource[H]{
		staticSource: staticSource,
		apiSource:    apiSource,
	}
}

// Hooks 返回两个来源的全部策略 hook，静态在前；底层切片未变时复用缓存的合并结果。
// 先读后写锁的双重检查：快路径在 RLock 下完成，命中缓存时不升级为写锁，
// 避免每次准入求值都串行化。未命中时在写锁内再次检查，防止并发重建重复。
func (c *compositePolicySource[H]) Hooks() []H {
	var staticHooks, apiHooks []H

	// 静态策略在前（平台基线策略优先）
	if c.staticSource != nil {
		staticHooks = c.staticSource.Hooks()
	}

	// 然后是 API 来源的策略
	if c.apiSource != nil {
		apiHooks = c.apiSource.Hooks()
	}

	c.mu.RLock()
	if slicesAreEqual(staticHooks, c.lastStatic) && slicesAreEqual(apiHooks, c.lastAPI) {
		combined := c.lastCombined
		c.mu.RUnlock()
		return combined
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	// 并发写入者可能已在我们释放读锁后完成重建，此处必须复查。
	if slicesAreEqual(staticHooks, c.lastStatic) && slicesAreEqual(apiHooks, c.lastAPI) {
		return c.lastCombined
	}

	// 预分配容量并先追加静态策略，保证顺序稳定：调用方依赖“静态优先”的语义。
	combined := make([]H, 0, len(staticHooks)+len(apiHooks))
	combined = append(combined, staticHooks...)
	combined = append(combined, apiHooks...)

	c.lastStatic = staticHooks
	c.lastAPI = apiHooks
	c.lastCombined = combined
	return combined
}

// slicesAreEqual 判断两个切片是否共享同一底层数组且长度相同。
// 仅比较底层数组首地址与长度。空切片视为相等，因为长度为零时没有可比较
// 的地址，且语义上同样表示“无元素”。
func slicesAreEqual[T any](a, b []T) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

// Run 启动 API 来源。
// 静态来源的 Run 由插件自行启动，这里若重复启动会导致两个监听器竞争，
// 因此只驱动 API 来源。
func (c *compositePolicySource[H]) Run(ctx context.Context) error {
	if c.apiSource != nil {
		return c.apiSource.Run(ctx)
	}
	return nil
}

// 任一来源未就绪都不能对外提供准入决策，否则会漏掉该来源下应生效的策略。
func (c *compositePolicySource[H]) HasSynced() bool {
	staticSynced := c.staticSource == nil || c.staticSource.HasSynced()
	apiSynced := c.apiSource == nil || c.apiSource.HasSynced()
	return staticSynced && apiSynced
}
