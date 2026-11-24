/*
Copyright 2015 The Kubernetes Authors.

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

// Package fields 实现简单的字段系统，支持解析字段选择器，并将选择器与字段集合进行匹配。
//
// fields 包实现了一套简单的字段系统：解析字段选择器（如 metadata.name=foo）
// 并将其与字段集合进行匹配。它不涉及类型信息，字段以点分路径字符串表示，
// 常用于对已解码对象做轻量过滤。
package fields
