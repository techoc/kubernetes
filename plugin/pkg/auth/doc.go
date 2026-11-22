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

// Package auth 包含认证和授权插件的实现。
// 主要子包：
//
//   - authenticator/token/bootstrap：Bootstrap token 认证器，用于节点 TLS 引导流程。
//     新节点使用 bootstrap token 向 API Server 认证，然后获取长期证书。
//
//   - authorizer/rbac：RBAC（基于角色的访问控制）授权器实现。
//     根据 Role/ClusterRole 和 RoleBinding/ClusterRoleBinding 决定请求是否被允许。
//
//   - authorizer/node：Node 授权器，使用有向图限制 kubelet 只能访问与其关联的资源。
//     防止一个节点访问属于其他节点的 Pod、Secret 等资源。
//
//   - authorizer/rbac/bootstrappolicy：集群启动时自动创建的默认 RBAC 策略。
//     包括 cluster-admin、view、edit、system:node 等内置角色和绑定。
package auth
