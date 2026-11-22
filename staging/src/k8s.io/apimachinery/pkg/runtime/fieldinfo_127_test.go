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

package runtime_test

// stdlibSupportsEmbedTag 指示标准库是否原生支持 `embed` 结构体标签。
// 该文件仅在 go1.27 及以上版本参与编译；低版本由配套文件提供 false。
const stdlibSupportsEmbedTag = true
