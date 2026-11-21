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

// Package ipam 提供多种为节点分配 IP 段的分配器。
// 目前支持以下几类（由 CIDRAllocatorType 标识）：
//   - RangeAllocator：为节点分配 PodCIDR，并与 RouteController 配合配置网络以打通连通性；
//   - CloudAllocator：从底层云平台分配的地址段同步 PodCIDR；
//   - （仅 Alpha）IPAMFromCluster：功能类似 RangeAllocator，同时把集群管理的地址段同步回云平台；
//   - （仅 Alpha）IPAMFromCloud：与 CloudAllocator 相同，把云平台信息同步进集群。
//
// 这些实现共享节点 CIDR 的分配语义，但同步方向（集群到云或云到集群）不同，
// 需根据部署环境选择。
package ipam
