/*
Copyright 2016 The Kubernetes Authors.

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

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=package
// +k8s:defaulter-gen=TypeMeta
// +groupName=example.dots.apiserver.code-generator.k8s.io
// +k8s:conversion-gen=k8s.io/code-generator/examples/apiserver/apis/example3.io
// +groupGoName=ThirdExample
// +k8s:openapi-model-package=io.k8s.code-generator.examples.apiserver.apis.example3.io.v1

// Package v1 是 example3.io 群组的版本化 API 包。
// 该示例同时启用 deepcopy/defaulter/openapi/conversion，目录名与群组名不一致，
// 由 +groupName 与 +groupGoName 分别声明外部群组名和 Go 标识符前缀。
package v1
