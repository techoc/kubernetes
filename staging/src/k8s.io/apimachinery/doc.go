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

// Package apimachinery 是 Kubernetes 服务端与客户端共用的公共 API 机器层：
// 提供对象序列化与编解码、资源量化、标签与字段选择器、元数据类型、
// 轮询等待、JSON/YAML 处理等基础能力，被 kube-apiserver 及所有 API 客户端依赖。
package apimachinery
