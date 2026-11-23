/*
Copyright 2021 The Kubernetes Authors.

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

// Package kubecontrollermanager 用独立模块的名字承载 kube-controller-manager 的公共代码。
//
// 这里刻意不放实现，因为该目录的用途是给仓库提供一个稳定的导入路径：
// 下游组件可以直接引用该模块，而不必反向依赖 k8s.io/kubernetes 主模块。
// 具体配置 API 放在 config/v1alpha1 子包中，由生成器根据类型定义产出深拷贝和 OpenAPI 代码。
package kubecontrollermanager
