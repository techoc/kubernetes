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

import (
	"testing"
)

// TestCelEmulateFeatureFate 验证特性模拟开关的完整状态流转：
// 初始为关闭，Set 后为开启，Unset 后恢复关闭。
func TestCelEmulateFeatureFate(t *testing.T) {
	// 首先断言默认值为关闭，防止残留状态影响测试。
	if DRAConsumableCapacity() {
		t.Fatal("emulated value must start from false")
	}
	// 打开开关后应立即读到 true。
	SetDRAConsumableCapacity()
	if !DRAConsumableCapacity() {
		t.Fatal("emulated value must be set")
	}
	// 关闭开关后应恢复为 false。
	UnsetDRAConsumableCapacity()
	if DRAConsumableCapacity() {
		t.Fatal("emulated value must be unset")
	}
}
