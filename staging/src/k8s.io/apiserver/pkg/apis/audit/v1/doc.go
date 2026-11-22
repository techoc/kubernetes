/*
Copyright 2018 The Kubernetes Authors.

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
// +k8s:protobuf-gen=package
// +k8s:conversion-gen=k8s.io/apiserver/pkg/apis/audit
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:openapi-model-package=io.k8s.apiserver.pkg.apis.audit.v1

// +groupName=audit.k8s.io

// Package v1 定义审计事件的 v1 外部版本类型（audit.k8s.io/v1）。
// 该版本是写入审计日志和 webhook 的稳定线格式；内部表示见
// k8s.io/apiserver/pkg/apis/audit，两者之间的转换由 conversion-gen 生成。
package v1
