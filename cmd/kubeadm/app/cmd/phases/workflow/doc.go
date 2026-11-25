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

// Package workflow 实现用于构建可组合 kubeadm 工作流的工作流管理器。
//
// 可组合的 kubeadm 工作流由有序阶段序列构建；
// 每个阶段可以拥有自己的有序子阶段序列。例如：
//
//	preflight     	运行控制平面预检
//	certs         	生成建立控制平面所需的全部 PKI 资产
//	    /ca          	生成自签名 Kubernetes CA，为 Kubernetes 组件提供身份
//	    /apiserver   	生成 API 服务器服务证书和密钥
//	    ...
//	kubeconfig    	生成建立控制平面所需的全部 kubeconfig 文件
//	    /admin       	供管理员和 kubeadm 自身使用的 kubeconfig 文件
//	    /kubelet     	供 kubelet 使用的 kubeconfig 文件
//	    ...
//	...
//
// 阶段设计为可在不同 kubeadm 工作流中复用，
// 例如 certs 阶段可同时用于 kubeadm init 和 kubeadm join --control-plane。
//
// 每个工作流都可通过 Runner 定义和管理；Runner 按给定顺序运行全部阶段，
// 子阶段在其父阶段之后立即执行。
//
// 阶段运行器可绑定到 cobra 命令：此操作设置命令描述以列出阶段，
// 并自动创建用于单独调用阶段的子命令。
//
// 自动生成的子命令按以下规则获得参数：
//
//   - 全局参数始终由自动生成的命令继承（由 cobra 管理）。
//   - 父命令的本地参数可能由自动生成的命令继承，但需显式选择，
//     以便每个阶段只选取相关参数。
//   - 可以通过显式选择定义由自动生成的命令继承的额外参数，
//     但这些参数不会应用于父命令。
//
// 为控制参数定义，请参阅
// "k8s.io/kubernetes/cmd/kubeadm/app/cmd/options" 包。
package workflow
