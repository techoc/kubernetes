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

package sets

// Empty 是集合的占位值类型。
//
// Empty 之所以导出，是因为部分内部 API 对象需要在外部字符串数组与内部集合之间转换，
// 而当前的转换代码要求使用导出类型。作为 map 的值类型，空结构体不占额外内存，
// 这正是用 map[T]Empty 表示集合的关键。
type Empty struct{}
