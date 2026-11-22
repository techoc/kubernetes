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

// Package audit contains implementations for pkg/audit/AuditBackend interface
//
// Package audit 提供 pkg/audit 中 AuditBackend 接口的各种后端实现
// （如日志文件、webhook）。拆分到 plugin 目录是为了让 apiserver 核心
// 只依赖审计接口，由装配层按配置选择具体后端。
package audit
