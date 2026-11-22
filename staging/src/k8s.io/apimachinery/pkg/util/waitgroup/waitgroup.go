/*
Copyright 2017 The Kubernetes Authors.

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

package waitgroup

import (
	"fmt"
	"sync"
)

// SafeWaitGroup 是带保护的 sync.WaitGroup：一旦进入等待，就拒绝新增请求，
// 避免 Wait 与 Add 的竞争。首次使用后不得复制。
type SafeWaitGroup struct {
	wg sync.WaitGroup
	mu sync.RWMutex
	// wait 表示 Wait 是否已被调用；为 true 时任何正增量的 Add 都会返回错误。
	wait bool
}

// Add 增加 delta（可以为负），与 sync.WaitGroup 类似。
// 如果在 Wait 之后发生正增量的 Add，会返回错误，从而阻止不安全的 Add。
func (wg *SafeWaitGroup) Add(delta int) error {
	// 用读锁而非互斥锁：Add 与 Add 之间可以并发，只需与 Wait 的写互斥。
	wg.mu.RLock()
	defer wg.mu.RUnlock()
	if wg.wait && delta > 0 {
		return fmt.Errorf("add with positive delta after Wait is forbidden")
	}
	wg.wg.Add(delta)
	return nil
}

// Done 递减 WaitGroup 计数。
func (wg *SafeWaitGroup) Done() {
	wg.wg.Done()
}

// Wait 阻塞直到 WaitGroup 计数归零。
// 它先把状态标记为“等待中”，再实际等待：这样在进入等待后到达的 Add 会被拒绝。
func (wg *SafeWaitGroup) Wait() {
	wg.mu.Lock()
	wg.wait = true
	wg.mu.Unlock()
	wg.wg.Wait()
}
