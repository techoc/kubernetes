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

// Package structured contains code for working with structured parameters.
// Package structured 提供处理结构化参数（structured parameters）的代码。
// 本包对外暴露 Allocator：它依据节点状态、ResourceClaim、DeviceClass 与 ResourceSlice 计算分配方案；
// 内部在 stable、incubating、experimental 三套实现中按特性集合挑选一套，返回的分配器可复用且线程安全。
package structured
