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

// Package endpoint 提供 EndpointController 的实现，用于管理和同步 Service 的端点。
// 该控制器监听 Service 与 Pod 的变化，维护与 Service 选择器匹配的 Endpoint（或 EndpointSlice）对象。
package endpoint
