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

// Package conversion 提供 Go 对象的版本化转换能力。
//
// 同一个逻辑对象可以定义多个 Go 结构版本。开发者为字段布局或语义发生变化的部分
// 编写转换函数；未变化字段可由生成的转换逻辑自动复制。借助该机制，内存中的对象
// 结构可以持续演进，同时保持磁盘存储格式和对外 API 版本之间的兼容。
package conversion
