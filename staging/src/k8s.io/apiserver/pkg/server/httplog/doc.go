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

// Package httplog contains a helper object and functions to maintain a log
// along with an http response.
// httplog 提供可包装 http.ResponseWriter 的日志对象：记录状态码、
// 响应字节数与耗时，并支持按需捕获响应体（仅用于异常响应的诊断输出）。
package httplog
