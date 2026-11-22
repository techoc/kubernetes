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

// Package discovery provides ways to discover server-supported
// API groups, versions and resources.
//
// discovery 包提供发现机制，用于查询服务端支持的 API 组、版本和资源。
// 它封装了 /api、/apis 等 HTTP 端点，并提供资源到 GroupVersionResource 的映射缓存，
// 使客户端（如 RESTMapper、动态客户端）无需硬编码即可适配不同集群的 API 表面。
package discovery
