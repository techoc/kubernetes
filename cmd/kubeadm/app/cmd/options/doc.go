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

// Package options 集中定义 kubeadm cobra 命令的参数，
// 无论命令是硬编码的还是为阶段自动生成的。
//
// 新的 kubeadm 参数应始终先在此包中定义为常量再使用，
// 以统一不同命令的命名并控制参数蔓延。
//
// 除定义参数外，此包还提供参数管理工具。
//
// 有关阶段如何管理参数的更多细节，请参阅
// "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow" 包。
package options
