/*
Copyright 2018 The Kubernetes Authors.

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

// Package genericclioptions 提供可挂载到命令上的标志及其绑定、补全逻辑和辅助函数。
// 本包任何代码都不得依赖 kube/kube，以保持 cli-runtime 的独立性。
//
// 该包提供一组可复用的命令行 flag：它们可以注册到任意 cobra 命令上，由命令负责
// 绑定变量、完成补全，并提供便捷的辅助函数。设计约束是本包不得依赖
// k8s.io/kubernetes，以保证 cli-runtime 能被 kubectl 之外的第三方工具独立引用。
package genericclioptions
