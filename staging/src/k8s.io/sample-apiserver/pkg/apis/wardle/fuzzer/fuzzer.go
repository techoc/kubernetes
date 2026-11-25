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
	"k8s.io/sample-apiserver/pkg/apis/wardle"
	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
)

// Funcs 返回 Wardle API 的随机数据填充函数。
// 随机值必须落在校验规则之内，否则 round-trip 测试会因对象本身非法而失败。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// 先让 randfill 填原始值，再修正成互斥引用与匹配的 referenceType。
		func(s *wardle.FlunderSpec, c randfill.Continue) {
			c.FillNoCustom(s) // 填充本对象，避免再次调用当前自定义函数。

			// 两个引用字段不能同时有值，保留 FlunderReference、清掉另一个。
			if len(s.FlunderReference) != 0 && len(s.FischerReference) != 0 {
				s.FischerReference = ""
			}
			// 根据保留下来的引用字段反推类型；两者都为空时类型也置空。
			if len(s.FlunderReference) != 0 {
				s.ReferenceType = wardle.FlunderReferenceType
			} else if len(s.FischerReference) != 0 {
				s.ReferenceType = wardle.FischerReferenceType
			} else {
				s.ReferenceType = ""
			}
		},
	}
}
