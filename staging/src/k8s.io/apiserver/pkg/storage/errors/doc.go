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

// Package storage provides conversion of storage errors to API errors.
//
// Package storage（此处实际为 errors 包）把底层存储/etcd 错误转换为
// 用户可理解的 apierrors，并保证错误码与语义在重试、冲突、键不存在等
// 场景下保持一致。
package errors
