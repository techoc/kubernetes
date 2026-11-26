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

// Package queryparams 把带版本的 runtime 对象转换为 URL 查询参数。
//
// 主要用于把 ListOptions 等参数对象编码为 HTTP 查询串；只支持简单值、
// 切片和 map，复杂的嵌套对象不适合放进 query。
package queryparams
