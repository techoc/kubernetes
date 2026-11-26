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

// Package discovery 提供发现服务端所支持 API 组、版本和资源的方式。
//
// 它封装 /api、/apis 等端点，并缓存资源到 GroupVersionResource 的映射，
// 使 RESTMapper、动态客户端等无需硬编码即可适配不同集群的 API 表面。
package discovery
