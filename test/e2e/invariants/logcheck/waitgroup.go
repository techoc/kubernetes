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

package logcheck

import (
	"sync"
)

// 与 sync.WaitGroup 不同，waitGroup 支持 go 和 wait 之间的竞态。
// 一旦调用 wait，就不会再启动新的 goroutine。
type waitGroup struct {
	mutex sync.Mutex
	cond  *sync.Cond

	shuttingDown bool
	running      int
}

func (wg *waitGroup) wait() {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	wg.shuttingDown = true
	for wg.running > 0 {
		wg.cond.Wait()
	}
}

func (wg *waitGroup) add(n int) bool {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	if wg.shuttingDown {
		return false
	}

	wg.running += n
	return true
}

func (wg *waitGroup) done() {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	wg.running--
	wg.cond.Broadcast()
}

// 如果 waitGroup 尚未进入关闭流程，goIfNotShuttingDown 会在 goroutine 中执行回调。
// 无论哪种情况，cleanup 回调都保证只调用一次。
func (wg *waitGroup) goIfNotShuttingDown(cleanup, cb func()) {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	if cleanup == nil {
		cleanup = func() {}
	}

	if wg.shuttingDown {
		// 直接清理。
		cleanup()
		return
	}

	wg.running++
	go func() {
		defer wg.done()
		defer cleanup()
		cb()
	}()
}

func newWaitGroup() *waitGroup {
	var wg waitGroup
	wg.cond = sync.NewCond(&wg.mutex)
	return &wg
}
