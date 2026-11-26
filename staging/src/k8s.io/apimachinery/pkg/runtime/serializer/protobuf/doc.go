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

// Package protobuf 提供 protobuf 格式的 Kubernetes 序列化器。
//
// Package protobuf 提供 Kubernetes 的 protobuf 序列化器：使用
// `k8s.io/apimachinery/pkg/runtime.Unknown` 包装任意对象，并在头部写入
// 4 字节的 runtime 编码前缀以便识别。
package protobuf
