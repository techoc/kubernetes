/*
Copyright 2019 The Kubernetes Authors.

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

// Package v1 是带有连字符群组名的示例 API 版本包。
// 目录/群组名含连字符，用于验证生成器对非标准群组名的处理；
// 生成指令说明：+groupGoName 指定合法的 Go 前缀，避免连字符进入标识符。
// +k8s:deepcopy-gen=package
// +k8s:defaulter-gen=TypeMeta
// +groupName=example-group.hyphens.code-generator.k8s.io
// +groupGoName=ExampleGroup
package v1
