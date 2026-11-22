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

// Package testing 是 IPVS 工具测试辅助代码所在的包。
// 这是一个空文件，目的是在非 Linux 平台运行单元测试时，
// 避免其他文件均受构建约束排除而导致 “build constraints exclude all Go files” 错误。
package testing
