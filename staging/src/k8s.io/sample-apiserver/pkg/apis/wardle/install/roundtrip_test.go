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
	wardlefuzzer "k8s.io/sample-apiserver/pkg/apis/wardle/fuzzer"
)

// 用 fuzzer 生成的随机对象做编解码往返，验证各版本注册与转换闭环。
func TestRoundTripTypes(t *testing.T) {
	// 先跑 JSON 路径；protobuf 往返等生成代码补齐后再打开。
	roundtrip.RoundTripTestForAPIGroup(t, Install, wardlefuzzer.Funcs)
	// TODO: enable protobuf generation for the sample-apiserver
	// roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, wardlefuzzer.Funcs)
}
