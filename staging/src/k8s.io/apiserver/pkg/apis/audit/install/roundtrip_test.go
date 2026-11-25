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
	"k8s.io/apiserver/pkg/apis/audit/fuzzer"
)

// TestRoundTrip 验证 audit API 组在 JSON 与 protobuf 两种序列化下都能无损往返。
//
// 使用 fuzzer 生成随机对象覆盖零值、nil 指针与边界组合；这类测试能
// 发现手工编写的转换/深拷贝函数遗漏字段的问题，因此每个 API 组都应保留。
func TestRoundTrip(t *testing.T) {
	// JSON 往返验证标签与类型转换；protobuf 往返额外验证生成的 marshal 代码。
	roundtrip.RoundTripTestForAPIGroup(t, Install, fuzzer.Funcs)
	roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, fuzzer.Funcs)
}
