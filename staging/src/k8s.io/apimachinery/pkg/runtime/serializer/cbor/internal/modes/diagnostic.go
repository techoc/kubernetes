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

// Package modes 集中定义 CBOR 编解码模式（Decode/Encode/Diagnostic），
// 保证库内各处的限制参数（嵌套层级、元素数量等）保持一致。
package modes

import (
	"github.com/fxamacker/cbor/v2"
)

// Diagnostic 是用于生成 CBOR 诊断表示（RFC 8949 §8）的模式。
// 其限制参数与 Decode 保持一致，避免诊断输出路径接受解码路径会拒绝的输入；
// ByteStringText 为 true 时按文本展示字节串，便于阅读错误信息。
var Diagnostic cbor.DiagMode = func() cbor.DiagMode {
	opts := Decode.DecOptions()
	diagnostic, err := cbor.DiagOptions{
		ByteStringText: true,

		MaxNestedLevels:  opts.MaxNestedLevels,
		MaxArrayElements: opts.MaxArrayElements,
		MaxMapPairs:      opts.MaxMapPairs,
	}.DiagMode()
	if err != nil {
		// 选项在编译期已确定，出错说明代码有 bug，启动时直接崩溃。
		panic(err)
	}
	return diagnostic
}()
