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

package cache

import (
	"bytes"
	"sync"
	"testing"

	fcache "k8s.io/client-go/tools/cache/testing"
)

// newFakeControllerSource 创建假的 controller source，并注册测试清理回调，
// 避免测试结束后后台 goroutine 泄漏。
func newFakeControllerSource(tb testing.TB) *fcache.FakeControllerSource {
	source := fcache.NewFakeControllerSource()
	tb.Cleanup(source.Shutdown)
	return source
}

// threadSafeBuffer is a thread-safe wrapper around bytes.Buffer.
//
// 测试中 reflector 等组件会从多个 goroutine 并发写日志缓冲，普通 bytes.Buffer
// 并不安全，因此用互斥锁保护。
type threadSafeBuffer struct {
	buffer bytes.Buffer
	mu     sync.Mutex
}

// Write 实现 io.Writer，加锁后写入底层缓冲。
func (b *threadSafeBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

// String 加锁读取当前缓冲内容。
func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
