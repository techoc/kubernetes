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

package devicetainteviction

import (
	"time"

	"k8s.io/client-go/util/workqueue"
)

// NOPQueue 是 [TypedRateLimitingInterface] 的空实现，所有方法都不做任何事。
// 用于不需要实际排队/限流的场景：Get 总是立即返回 shutdown=true，
// 使基于 workqueue 的控制循环可以“空转”运行而不处理任何条目。
type NOPQueue[T comparable] struct {
}

var _ workqueue.TypedRateLimitingInterface[int] = &NOPQueue[int]{}

// Add 实现 [TypedInterface]：忽略加入的条目，不排队。
func (m *NOPQueue[T]) Add(item T) {
}

// Len 实现 [TypedInterface]：队列始终为空，返回 0。
func (m *NOPQueue[T]) Len() int {
	return 0
}

// Get 实现 [TypedInterface]：立即以 shutdown=true 返回，
// 让调用方认为队列已关闭并退出取-item 循环，且不产生任何待处理条目。
func (m *NOPQueue[T]) Get() (item T, shutdown bool) {
	shutdown = true
	return
}

// Done 实现 [TypedInterface]：空实现，无需标记条目处理完成。
func (m *NOPQueue[T]) Done(item T) {
}

// ShutDown 实现 [TypedInterface]：空实现，队列本身不持有需要关闭的资源。
func (m *NOPQueue[T]) ShutDown() {
}

// ShutDownWithDrain 实现 [TypedInterface]：空实现，没有条目需要 drain。
func (m *NOPQueue[T]) ShutDownWithDrain() {
}

// ShuttingDown 实现 [TypedInterface]：始终返回 true，与 Get 的 shutdown 语义一致。
func (m *NOPQueue[T]) ShuttingDown() bool {
	return true
}

// AddAfter 实现 [TypedDelayingInterface.AddAfter]：忽略延迟入队请求。
func (m *NOPQueue[T]) AddAfter(item T, duration time.Duration) {
}

// AddRateLimited 实现 [TypedRateLimitingInterface.AddRateLimited]：忽略限速重入队请求。
func (m *NOPQueue[T]) AddRateLimited(item T) {
}

// Forget 实现 [TypedRateLimitingInterface.Forget]：空实现，无需清理限速计数。
func (m *NOPQueue[T]) Forget(item T) {
}

// NumRequeues 实现 [TypedRateLimitingInterface.NumRequeues]：从不重入队，故返回 0。
func (m *NOPQueue[T]) NumRequeues(item T) int {
	return 0
}
