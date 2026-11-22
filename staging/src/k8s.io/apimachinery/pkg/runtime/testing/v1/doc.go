/*
Copyright 2025 The Kubernetes Authors.

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

// Package v1 提供 runtime 包测试与基准使用的 v1 版本辅助类型。
//
// 该包只服务于测试：其中的类型实现 runtime.Object 等接口，用于构造
// 编码/解码场景，不对外提供 API 兼容性保证。
package v1
