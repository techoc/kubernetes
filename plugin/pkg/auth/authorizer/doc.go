/*
Copyright 2016 The Kubernetes Authors.

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

// Package authorizer 包含授权器插件的实现。
//
//   - rbac：RBAC 授权器，基于 Role/ClusterRole 和 RoleBinding/ClusterRoleBinding 进行访问控制。
//
//   - node：Node 授权器，使用有向图限制 kubelet 只能访问与其关联的资源（Pod、Secret、PVC 等）。
//     与 NodeRestriction 准入插件配合，形成双层防护。
//
//   - rbac/bootstrappolicy：集群启动时自动创建的默认 RBAC 策略，包括内置角色和绑定。
package authorizer
