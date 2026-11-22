/*
Copyright 2022 The Kubernetes Authors.

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

// Package v2 contains definition of kms-plugin's serialized types.
//
// 这些类型是 KMS v2 插件与 apiserver 之间加密数据的线格式（wire format）。
// 字段一旦发布即受持久化数据兼容性约束：已写入 etcd 的密文必须始终能被解码，
// 因此只能新增可选字段，不能重命名或改变既有语义。
package v2
