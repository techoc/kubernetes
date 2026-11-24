/*
Copyright 2015 The Kubernetes Authors.

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

package types

// NodeName 用于保存 API 中 api.Node 的 Name 标识。定义为独立类型可以明确表达语义，
// 避免把节点名与主机名、云厂商实例 ID 或云厂商实例名称等概念混淆。
//
// 几种容易混淆的名称需要区分：
//
//   - Node.Name：Node API 对象的 Name 字段，业务代码中应使用 NodeName 表示。
//     由于 Name 属于 ObjectMeta，API 类型本身无法直接将其声明为 NodeName。
//
//   - Hostname：本机主机名（通常来自 uname -n）。部分组件支持 --hostname-override，
//     可覆盖默认主机名；在没有其他更合适标识时，kubelet 创建 Node 会使用 Hostname 作为 Node.Name。
//
// - 云厂商实例名称：例如 GCE 使用 InstanceName，AWS 使用 InstanceId。
//
//	在 GCE 中，Instance.Name 通常也会成为主机名，因此可以被映射为 Node.Name；
//	但这是 GCE 特有的行为，具体映射规则由云提供商实现决定。
//
//	在 AWS 中，InstanceID 不适合直接作为 Node.Name，因此 Kubernetes 使用 PrivateDnsName。
//	PrivateDnsName 不一定等于主机名，例如使用自定义 DHCP 域时两者可能不同。
//
// 译注：NodeName 是独立的命名类型，用类型系统区分节点名与主机名、云厂商实例名等
// 相似概念，避免在函数签名之间误传。
type NodeName string
