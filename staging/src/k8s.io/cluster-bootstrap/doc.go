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

// Package clusterbootstrap 汇集了集群引导（cluster bootstrap）过程中共享的
// 常量与工具：bootstrap token 的生成、解析与校验，token Secret 的读取，
// 以及对集群信息 ConfigMap 的 JWS 签名验证。
//
// bootstrap token 是 kubeadm 等工作流在节点加入集群前使用的短期凭据，其信任
// 模型是：token 以 Secret 形式存放在集群内，由控制面组件（BootstrapSigner、
// TokenCleaner）签发和回收；新节点凭 token 认证并把集群 CA 与 API 端点作为
// 可验证的集群信息拉取下来。token 分为公开的 ID 与保密的 Secret 两段，正是
// 为了让 ID 可以出现在 Secret 名称和日志中，而 Secret 只在需要时以密文形式
// 传递。
package clusterbootstrap
