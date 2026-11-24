/*
Copyright 2014 The Kubernetes Authors.

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

// +k8s:openapi-model-package=io.k8s.apimachinery.pkg.runtime

// Package runtime 提供 Kubernetes API 对象运行时支持，包括类型注册、版本转换、默认值处理、
// 序列化与反序列化。API 对象通常遵循以下约定：
//
// 0. 顶层 API 对象包含公共元数据结构体 TypeMeta，用于携带 apiVersion 和 kind。
//
// 1. 业务逻辑内部使用一套内部 API 类型，内部类型不直接作为稳定的外部协议暴露。
//
// 2. 在独立包中定义对外提供的版本化 API 类型。
//
// 3. 已发布的外部版本保持向后兼容：可以新增字段，但不能修改或删除已有字段。
//
// 4. API 发生不兼容演进时，新增一个版本化包，而不是破坏已有版本。
//
// 5. 每个外部版本包提供与内部版本之间的双向转换函数，运行时通过 Scheme 管理这些转换。
//
// 6. 按照弃用策略持续支持旧版本；由于第 5 条中的转换函数，可以方便地提供程序或库，将旧版本对象升级到新版本。
//
// 7. 所有序列化与反序列化都集中在统一的运行时入口处理。
//
// runtime 包提供对象转换辅助能力以简化第 5 条，并通过 Encode、Decode、DecodeInto 完成第 7 条。
// 你也可以注册使用指定版本的额外编解码器。
// 建议在各 API 包的 init 函数中向 runtime 注册本包提供的类型。
//
// 此外，types.go 还提供了一些可供所有 API 对象及版本共用的通用类型。
//
// 简言之：Scheme 是类型注册表，Codec 负责在内部/外部版本间转换并编解码；
// 所有 API 包在 init 中向 Scheme 注册自己的类型与转换函数。
package runtime
