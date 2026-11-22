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

// package jsonpath is a template engine using jsonpath syntax,
// which can be seen at http://goessner.net/articles/JsonPath/.
// In addition, it has {range} {end} function to iterate list and slice.
//
// jsonpath 是基于 jsonpath 语法的模板引擎，用于从任意 Go 结构/JSON 中按路径取值并格式化输出；
// 在标准语法之外额外提供 {range}...{end} 用于遍历列表与切片。
package jsonpath
