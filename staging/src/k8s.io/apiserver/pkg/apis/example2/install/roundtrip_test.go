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

// TestRoundTrip 验证 example2 组在 Install 注册的所有版本都能无损往返：
// JSON 编码与 protobuf 编码各走一遍，确保转换函数与 tag 一致。
// 两个检查必须同时存在——protobuf 路径会暴露 JSON 测试覆盖不到的字段编号问题。
func TestRoundTrip(t *testing.T) {
	roundtrip.RoundTripTestForAPIGroup(t, Install, examplefuzzer.Funcs)
	roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, examplefuzzer.Funcs)
}
