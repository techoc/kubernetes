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

package scale

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
)

// 注意：这些代码不能放在 scheme 包中，因为从 scheme 导入
// scheme/autoscalingv1 会与 conversions 形成依赖环。

// TestRoundTrip 验证 scale scheme 中所有类型经过 fuzz 后仍能无损 round-trip，
// 确保转换函数不会丢失或改变字段。
func TestRoundTrip(t *testing.T) {
	scheme := NewScaleConverter().Scheme()
	// 目前不需要自定义 fuzzer 函数，默认实现就够用。
	// 目前无需自定义 fuzzer，默认实现足以覆盖所有字段。
	roundtrip.RoundTripTestForScheme(t, scheme, nil)
}
