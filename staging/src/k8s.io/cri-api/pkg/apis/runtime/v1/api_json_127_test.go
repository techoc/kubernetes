//go:build go1.27

/*
Copyright The Kubernetes Authors.

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

package v1

// stdlibSerializedReplacementChar 定义在 Go 1.27 及之后版本下，encoding/json 对 UTF-8 替代字符（U+FFFD）
// 序列化后的结果：直接输出替代字符本身，不再做 \u 转义。
// 该文件通过 //go:build go1.27 约束，仅在 Go 1.27 及之后生效，与 api_json_126_test.go 互斥。
const stdlibSerializedReplacementChar = "\ufffd"
