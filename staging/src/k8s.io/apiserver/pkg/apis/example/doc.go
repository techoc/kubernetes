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

// +k8s:deepcopy-gen=package
// +groupName=example.apiserver.k8s.io

// package example 包含一个用于演示如何创建 api 组的示例 API。此外，它还
// 用于测试中。
//
// 该包同时作为“如何定义 API 组”的参考实现：类型、注册与转换都遵循
// apiserver 的标准约定，但不对外提供服务。
package example
