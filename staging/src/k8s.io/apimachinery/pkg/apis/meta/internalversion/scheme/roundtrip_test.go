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

package scheme

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	"k8s.io/apimachinery/pkg/apis/meta/fuzzer"
)

// TestRoundTrip 用 fuzzer 生成的对象验证 Scheme 中注册类型的编解码往返：
// 序列化再反序列化后必须与原始对象语义一致。
func TestRoundTrip(t *testing.T) {
	roundtrip.RoundTripTestForScheme(t, Scheme, fuzzer.Funcs)
}
