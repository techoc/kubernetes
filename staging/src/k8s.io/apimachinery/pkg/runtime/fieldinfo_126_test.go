//go:build !go1.27

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

package runtime_test

// stdlibSupportsEmbedTag 表示当前 Go 版本的标准库是否支持 struct tag 中的 "embed" 指令。
// 在 Go 1.27 之前为 false，供测试按版本选择预期结果。
const stdlibSupportsEmbedTag = false
