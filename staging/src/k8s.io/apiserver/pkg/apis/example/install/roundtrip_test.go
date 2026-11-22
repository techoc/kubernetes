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

package install

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	examplefuzzer "k8s.io/apiserver/pkg/apis/example/fuzzer"
)

// TestRoundTrip 验证 example API 组在所有已注册版本之间往返编解码不丢信息。
// 先跑 JSON 往返，再跑 protobuf 往返；后者能暴露 JSON 测试遗漏的 tag/字段映射问题。
func TestRoundTrip(t *testing.T) {
	roundtrip.RoundTripTestForAPIGroup(t, Install, examplefuzzer.Funcs)
	roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, examplefuzzer.Funcs)
}
