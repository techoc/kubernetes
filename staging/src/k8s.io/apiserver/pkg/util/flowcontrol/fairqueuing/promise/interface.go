/*
Copyright 2019 The Kubernetes Authors.

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

// Package promise 定义“写一次、可多次读取”的 promise 抽象。
// APF 用它在线程间传递延迟承诺的结果（例如 dispatch 的 R 值），
// 读方在值就绪前阻塞，避免忙等或复杂的条件变量管理。
package promise

// WriteOnce 表示初始未设置、只能设置一次且可读的变量，即通常所说的 "promise"。
// 写入最多一次，读取可无限次且并发安全。
type WriteOnce interface {
	// 读取当前值；若尚未设置则阻塞直到有值。
	// 调用方需自行处理取消/超时，例如在 select 中与其他 channel 配合使用。
	Get() interface{}

	// 写入值、唤醒所有等待的 goroutine 并返回 true；已有值则不修改并返回 false。
	// 后写者不能覆盖先写者，保证并发场景下结果只被确定一次。
	Set(interface{}) bool
}
