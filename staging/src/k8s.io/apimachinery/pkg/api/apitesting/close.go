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

// Package apitesting 提供 API 测试相关的通用辅助函数。由于部分辅助代码
// 会随非测试代码一起被其他包引用，这里刻意不依赖 testify。
package apitesting

import (
	"io"
	"testing"
)

// Close 关闭 c，若返回错误则让测试失败。
// 常见用法是 `defer apitesting.Close(t, closer)`，避免在每个测试里
// 重复写错误检查。
func Close(t TestingT, c io.Closer) {
	t.Helper()
	assertNoError(t, c.Close())
}

// CloseNoOp 什么都不做。用于在语义上禁用某个 defer 时替换 Close，
// 例如条件分支中提前关闭资源、后续 defer 不应再次关闭的场景。
func CloseNoOp(TestingT, io.Closer) {}

// TestingT 模拟 assert.TestingT 与 assert.tHelper 的最小接口，
// 从而不必把 testify 引入为非测试依赖（staging 库对此有严格限制）。
type TestingT interface {
	Errorf(format string, args ...interface{})
	Helper()
}

// 编译期断言：testing.T / testing.B 必须满足 TestingT，
// 这样调用方可以直接把 *testing.T 传进来。
var _ TestingT = &testing.T{}
var _ TestingT = &testing.B{}

// assertNoError 模拟 assert.NoError，避免引入 testify 依赖。
// 用 %+v 输出错误以保留包装链的上下文。
//
// 在测试文件中应优先使用 github.com/stretchr/testify/assert。
func assertNoError(t TestingT, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Received unexpected error:\n%+v", err)
	}
}
