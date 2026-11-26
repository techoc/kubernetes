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

// Package service 实现把云负载均衡器与 Service registry 同步的逻辑。
// 控制器监听 Service 与 Node 变化，调用云厂商 LoadBalancer 接口完成创建、更新、
// 删除以及后端节点集同步，并通过 finalizer 保证资源清理不会早于 Service 删除。
package service
