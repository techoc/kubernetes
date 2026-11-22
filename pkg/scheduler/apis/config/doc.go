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
// +groupName=kubescheduler.config.k8s.io

// Package config 定义调度器的内部 API 类型。
//
// 这里的类型是调度器在运行期实际使用的表示形式，与 v1 等外部版本分开：
//   - 外部版本负责序列化与兼容性，内部版本负责承载业务逻辑
//   - 两者之间通过 conversion 和 defaulting 函数互相转换
//
// 该包同时提供 DeepCopy 生成指令与 API 组名声明，供代码生成器使用。
package config
