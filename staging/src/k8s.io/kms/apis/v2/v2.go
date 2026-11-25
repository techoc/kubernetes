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

// Package v2 定义 KMS v2 插件的 gRPC 服务（由 api.proto 生成）。
// 相比 v1，v2 增加了 Status 健康状态轮询、key ID 与 annotations 字段，
// 支持 apiserver 判断数据是否因密钥轮换而过期（staleness）。
package v2
