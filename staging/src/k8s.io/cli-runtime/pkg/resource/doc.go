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

// Package resource 帮助客户端处理符合 Kubernetes API 约定的 RESTful 对象。
// Helper 提供简单的 CRUD 操作；Visitor 便于批量获取和操作多个资源；
// Builder 则把标准命令行参数转换为 Visitor，统一遍历服务端或本地文件系统中的资源。
//
// package resource 服务于命令行客户端的资源访问层：
//   - Builder 把 kubectl 风格的参数（-f、-k、resource/name、selector 等）归一化成 Result；
//   - Result 背后是 Visitor 链，既能遍历本地文件/URL/stdin，也能遍历服务端列表；
//   - Info 描述单个待处理的资源及其实施 REST 调用所需的上下文。
package resource
