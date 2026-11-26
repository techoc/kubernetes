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

// Package appsint 提供转换逻辑所需的内部版本脚手架，自身不定义类型，
// 只是为了让 runtime.Scheme.ConvertToVersion 等方法按预期工作。
//
// appsint 包提供 extensions/apps 内部版本所需的“脚手架”，仅供 conversion 逻辑使用。
// 包内没有自有类型，只是为了让 runtime.Scheme.ConvertToVersion 及其相关方法
// 能按预期完成版本转换（例如把外部版本转换为内部版本）。
package appsint
