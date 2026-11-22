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

package admission

import (
	"testing"
	"time"
)

// TestWaitForReady 覆盖 Handler.WaitForReady 的三种就绪路径：
// 未设置 readyFunc（默认视为就绪）、readyFunc 立即返回 true、以及
// readyFunc 持续返回 false 时必须等到固定超时才放弃。最后一种
// 确保调用方不会被无限阻塞，这是启动阶段依赖插件顺序的关键约束。
func TestWaitForReady(t *testing.T) {
	handler := newFakeHandler()

	// 1. test no readyFunc
	// 没有配置就绪检查时默认就绪，避免测试或简单插件被迫实现
	// 一个永远为真的回调。
	if !handler.WaitForReady() {
		t.Errorf("Expect ready for no readyFunc provided.")
	}

	// 2. readyFunc return ready immediately
	// 回调立即就绪时应立刻返回，而不是空等一个轮询周期。
	readyFunc := func() bool {
		return true
	}
	handler.SetReadyFunc(readyFunc)
	if !handler.WaitForReady() {
		t.Errorf("Expect ready for readyFunc returns ready immediately.")
	}

	// 3. readyFunc always return not ready. WaitForReady timeout
	// 故意永不就绪，验证 WaitForReady 有上界（timeToWaitForReady），
	// 否则 apiserver 启动会永久挂起。
	readyFunc = func() bool {
		return false
	}
	startTime := time.Now()
	handler.SetReadyFunc(readyFunc)
	if handler.WaitForReady() {
		t.Errorf("Expect not ready for readyFunc returns not ready immediately.")
	}
	// 断言确实等待了完整超时时间，而不是提前返回 false。
	if time.Since(startTime) < timeToWaitForReady {
		t.Errorf("Expect WaitForReady timeout.")
	}
}

// newFakeHandler 构造一个只关心 Create/Update 操作的最小 Handler，
// 使测试无需注册任何插件即可验证就绪逻辑。
func newFakeHandler() *Handler {
	return NewHandler(Create, Update)
}
