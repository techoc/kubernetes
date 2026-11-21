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

package install

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal/fuzzer"
)

// TestRoundTrip 对 apiserverinternal API 组执行序列化往返测试。
// 分别覆盖 JSON 与 Protobuf 两种编码：两者使用不同的序列化路径，
// 都要保证 fuzz 出的对象经编码再解码后与原始对象完全一致，
// 以此验证各版本之间的转换与默认化不会丢失或改变数据。
func TestRoundTrip(t *testing.T) {
	// 使用 Install 注册过的 Scheme 和该组的 fuzzer，确保测试针对真实的注册状态。
	roundtrip.RoundTripTestForAPIGroup(t, Install, fuzzer.Funcs)
	// Protobuf 使用独立的编解码实现，需要单独跑一遍以覆盖其特有的转换路径。
	roundtrip.RoundTripProtobufTestForAPIGroup(t, Install, fuzzer.Funcs)
}
