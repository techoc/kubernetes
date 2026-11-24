/*
Copyright 2021 The Kubernetes Authors.

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

// Package criapi 是 CRI（Container Runtime Interface，容器运行时接口）Go 语言模块的顶层包。
// 本包本身不包含具体的接口定义，仅作为模块入口；实际的 API 定义分布在以下子包中：
//   - pkg/apis：定义 kubelet 调用的 RuntimeService 与 ImageManagerService 等客户端接口；
//   - pkg/apis/runtime/v1：CRI API 的 protobuf 生成类型、常量以及兼容性辅助代码；
//   - pkg/errors：提供处理 CRI/gRPC 错误的辅助函数。
package criapi
