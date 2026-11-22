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

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=package
// +k8s:defaulter-gen=TypeMeta
// +k8s:conversion-gen=k8s.io/code-generator/examples/apiserver/apis/core
// +groupName=
// +k8s:openapi-model-package=io.k8s.code-generator.examples.apiserver.apis.core.v1

// Package v1 是 core 示例的 versioned 外部 API 包。
// 它同时开启 openapi、deepcopy、defaulter 与 conversion 生成，并指向
// 内部包 k8s.io/code-generator/examples/apiserver/apis/core 作为转换对端。
package v1
