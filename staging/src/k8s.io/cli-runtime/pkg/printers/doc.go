/*
Copyright 2019 The Kubernetes Authors.

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

// Package printers is helper for formatting and printing runtime objects into
// primitives io.writer.
//
// 该包把 runtime.Object（任意 Kubernetes 资源、List 或 Table）渲染成
// JSON/YAML/template/jsonpath/table 等面向终端的输出。所有 printer 都实现
// ResourcePrinter 接口，便于命令按 -o 参数动态选择。
package printers
