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

// Package fake 提供 CRI 运行时服务与镜像服务的假 gRPC 实现，主要用于单元测试：
// 它在内存中模拟 PodSandbox、容器与镜像的状态，并把 gRPC 请求转发给 cri-api 中的 testing 假服务。
package fake
