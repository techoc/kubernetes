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

// WriteOnce represents a variable that is initially not set and can
// be set once and is readable.  This is the common meaning for
// "promise".
// WriteOnce 的语义：写入最多一次，读取可无限次且并发安全。
type WriteOnce interface {
	// Get reads the current value of this variable.  If this
	// variable is not set yet then this call blocks until this
	// variable gets a value.
	// Get 在值未设置前会阻塞，调用方需自行处理取消/超时，
	// 例如在 select 中与其他 channel 配合使用。
	Get() interface{}

	// Set normally writes a value into this variable, unblocks every
	// goroutine waiting for this variable to have a value, and
	// returns true.  In the unhappy case that this variable is
	// already set, this method returns false without modifying the
	// variable's value.
	// Set 返回 false 时表示已有值且保持不变——后写者不能覆盖先写者，
	// 这保证并发场景下结果只被确定一次。
	Set(interface{}) bool
}
