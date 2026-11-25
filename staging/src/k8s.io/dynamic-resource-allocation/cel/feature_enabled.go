/*
Copyright 2022 The Kubernetes Authors.

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

package cel

// 本文件提供用于设置 compile.go 中 VersionedOptions.FeatureEnabled 的函数与全局变量。

import "sync/atomic"

var (
	// consumableCapacity 以原子值保存 DRA 可消耗容量特性的模拟开关；
	// 未写入时 Load 返回 nil，语义等同于特性关闭。
	consumableCapacity atomic.Value
)

// SetDRAConsumableCapacity 打开 DRA 可消耗容量特性的模拟开关。
func SetDRAConsumableCapacity() {
	// 写入 true，通过原子存储保证并发读写的可见性。
	consumableCapacity.Store(true)
}

// UnsetDRAConsumableCapacity 关闭 DRA 可消耗容量特性的模拟开关。
func UnsetDRAConsumableCapacity() {
	// 显式写入 false，使后续读取得到确定的关闭状态。
	consumableCapacity.Store(false)
}

// DRAConsumableCapacity 返回当前模拟的特性开关状态；尚未设置时默认关闭。
func DRAConsumableCapacity() bool {
	// 首次读取时原子值还未被写入，视为特性关闭。
	value := consumableCapacity.Load()
	if value == nil {
		return false
	}
	// 存入的是布尔值，做类型断言取出。
	return value.(bool)
}
