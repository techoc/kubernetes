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

// +k8s:deepcopy-gen=package
// +groupName=audit.k8s.io

// Package audit 定义审计事件的内部 API 类型。
//
// 这些类型与 audit.k8s.io/v1 的版本化类型一一对应，但不受 API 兼容性保证约束，
// 仅供 apiserver 内部使用；对外序列化一律经由 v1 转换完成。
package audit
