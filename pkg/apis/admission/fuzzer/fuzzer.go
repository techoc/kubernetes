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

package fuzzer

import (
	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
)

// Funcs 返回 admission API 组专用的 fuzzer 函数列表。
// 这些函数会被混入序列化/往返测试中，为特定类型生成可控的测试数据。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// RawExtension 内部可放任意 JSON，默认 fuzzer 会生成难以预测的随机字节，
		// 导致编码/解码往返测试不稳定。这里固定填充一个合法的 Unstructured 对象。
		func(s *runtime.RawExtension, c randfill.Continue) {
			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "unknown.group/unknown",
				"kind":       "Something",
				"somekey":    "somevalue",
			}}
			s.Object = u
		},
	}
}
