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

// Package fuzzer 为 audit API 类型提供 fuzz 自定义函数，使 roundtrip 测试能生成
// 贴近真实事件形态的数据（含 runtime.Unknown 的请求/响应体）。
package fuzzer

import (
	"strings"

	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/audit"
)

// Funcs returns the fuzzer functions for the audit api group.
//
// 这些自定义函数让 fuzz 测试覆盖 roundtrip 的边界情况：RequestObject/ResponseObject
// 为 *runtime.Unknown，且 Raw 中的 JSON 故意带有空的 apiVersion/kind。
func Funcs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(e *audit.Event, c randfill.Continue) {
			c.FillNoCustom(e)
			// 随机决定是否填充请求体，确保 nil 与非 nil 两条路径都被覆盖。
			switch c.Bool() {
			case true:
				e.RequestObject = nil
			case false:
				e.RequestObject = &runtime.Unknown{
					TypeMeta:    runtime.TypeMeta{APIVersion: "", Kind: ""},
					Raw:         []byte(`{"apiVersion":"","kind":"Pod","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			}
			switch c.Bool() {
			case true:
				e.ResponseObject = nil
			case false:
				e.ResponseObject = &runtime.Unknown{
					TypeMeta:    runtime.TypeMeta{APIVersion: "", Kind: ""},
					Raw:         []byte(`{"apiVersion":"","kind":"Pod","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			}
		},
		func(o *audit.ObjectReference, c randfill.Continue) {
			c.FillNoCustom(o)
			switch c.Intn(3) {
			case 0:
				// core api group
				o.APIGroup = ""
				o.APIVersion = "v1"
			case 1:
				// other group
				o.APIGroup = "rbac.authorization.k8s.io"
				o.APIVersion = "v1beta1"
			default:
				// use random value, but without / as it is used as separator
				// "/" 在 ObjectReference 的字符串表示中作分隔符，随机值含它会破坏往返解析。
				o.APIGroup = strings.Replace(o.APIGroup, "/", "-", -1)
				o.APIVersion = strings.Replace(o.APIVersion, "/", "-", -1)
			}
		},
	}
}
