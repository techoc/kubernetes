/*
Copyright 2024 The Kubernetes Authors.

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

// Package direct 提供任意 Go 值与 CBOR 数据之间直接编解码的函数，
// 行为与 CBOR 序列化器保持一致。实现了 cbor.Marshaler 和 cbor.Unmarshaler
// 的类型应使用这里的函数，以保证字节表示一致。
//
// Package direct 提供与 CBOR serializer 行为一致的直接编解码入口。自定义
// Marshaler/Unmarshaler 必须使用这里的函数，否则可能使用与 serializer 不同的
// 编码选项，导致字节表示不一致。
package direct

import (
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"
)

// Marshal 把值序列化为 CBOR。若存在多种编码方式，选择与 runtime.Serializer
// 的 CBOR 实现一致的那种。
func Marshal(src any) ([]byte, error) {
	return modes.Encode.Marshal(src)
}

// Unmarshal 把 CBOR 反序列化到可寻址的值。若存在多种解码方式，选择与
// runtime.Serializer 的 CBOR 实现一致的那种。
func Unmarshal(src []byte, dst any) error {
	return modes.Decode.Unmarshal(src, dst)
}

// Diagnose 接受格式正确的 CBOR 字节，以人类可读的诊断表示法
// （RFC 8949 第 8 节）返回同一数据项的字符串。
// 该表示法不用于机器解析。
//
// 诊断文本仅用于日志和错误信息，不要依赖其格式做解析。
func Diagnose(src []byte) (string, error) {
	return modes.Diagnostic.Diagnose(src)
}
