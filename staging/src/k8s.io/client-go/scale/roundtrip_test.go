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

// NB: this can't be in the scheme package, because importing'
// scheme/autoscalingv1 from scheme causes a dependency loop from
// conversions

// TestRoundTrip 验证 scale scheme 中所有类型经过 fuzz 后仍能无损 round-trip，
// 确保转换函数不会丢失或改变字段。
func TestRoundTrip(t *testing.T) {
	scheme := NewScaleConverter().Scheme()
	// we don't actually need any custom fuzzer funcs ATM -- the defaults
	// will do just fine
	// 目前无需自定义 fuzzer，默认实现足以覆盖所有字段。
	roundtrip.RoundTripTestForScheme(t, scheme, nil)
}
