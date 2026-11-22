/*
Copyright 2015 The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package
// +groupName=example.test.apiserver.code-generator.k8s.io
// +groupGoName=SecondExample

// Package example2 是 code-generator 的示例内部 API 包（第二个 group）。
// 指令 +groupName 指定完整 group 名，+groupGoName 指定生成代码中使用的
// Go 标识符前缀 SecondExample，避免与 example 包生成的符号冲突。
package example2
