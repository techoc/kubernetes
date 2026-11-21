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

// Package nodeipam 包含将云服务商中的实例与节点注册表同步的代码。
// 控制器为每个节点分配 CIDR，并在云实例信息变化时同步更新节点配置，
// 使节点能够获得可路由的 Pod 网段。
package nodeipam
