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

// Package namespace 定义 webhook 插件使用的工具，用于根据对象所属的
// namespace 决定是否对该对象应用某个 webhook。
//
// 命名空间谓词是准入 webhook 的第一层过滤：只有当对象的命名空间满足
// namespaceSelector 时，才可能把请求发送给该 webhook。selector 的匹配需要
// 读取 Namespace 对象，因此这里区分了 "不匹配" 与 "无法判断" 两种结果——
// 后者（例如 namespace 尚不存在或 API 调用失败）必须交由调用方按
// failurePolicy 决定，而不能静默地当作不匹配处理。
package namespace
