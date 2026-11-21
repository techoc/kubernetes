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

// Package testing 包含两类测试：
//   - 所有依赖 Kubernetes API 类型的通用 API 测试；
//   - 所有跨 Kubernetes API 的测试。
//
// 这里的辅助函数多为供各 API group 的测试文件调用的公共入口，
// 以此避免每个 group 重复编写转换、兼容性、序列化等测试逻辑。
package testing
