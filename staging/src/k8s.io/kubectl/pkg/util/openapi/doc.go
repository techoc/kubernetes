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

// Package openapi 提供从 Kubernetes 服务端获取 openapi 文档
// 并为类型定义建立索引的库。
// 文档包含对象模型定义，以及 patchStrategy、patchMergeKey
// 等用于生成 patch 的扩展元数据。
package openapi
