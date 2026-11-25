/*
Copyright 2016 The Kubernetes Authors.

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

// Package handlers 提供实现 apiserver API 的 HTTP handler。
//
// 每个 handler 对应一类 REST 动作（create/update/delete/get/list/watch），
// 负责解码、准入、持久化、watch 流以及错误到 HTTP 状态码的映射。
package handlers
