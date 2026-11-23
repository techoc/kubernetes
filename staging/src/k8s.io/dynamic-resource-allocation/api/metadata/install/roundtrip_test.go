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

package install

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
)

// TestRoundTrip 测试元数据 API 组所有注册类型的往返（round-trip）转换。
// 输入：Install 作为注册函数，传入 nil 表示使用默认的 fuzzer/值生成方式。
// 断言由 roundtrip.RoundTripTestForAPIGroup 辅助函数完成：它会把每个版本化类型与内部类型
// 反复转换（fuzz → 转换到内部 → 转回版本 → 比较），要求转回后的对象与原始对象一致，
// 以此保证各版本与内部表示之间的转换不丢失信息。
func TestRoundTrip(t *testing.T) {
	roundtrip.RoundTripTestForAPIGroup(t, Install, nil)
}
