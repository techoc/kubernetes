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

// Package request contains everything around extracting info from
// a http request object.
// TODO: this package is temporary. Handlers must move into pkg/apiserver/handlers to avoid dependency cycle
//
// Package request 负责从 HTTP 请求中提取请求信息（如 RequestInfo、用户、超时），
// 并把它们放进 context 供下游过滤器与 handler 使用。
// 本包刻意保持低依赖：它被几乎所有 endpoints 子包引用，任何重量级依赖
// 都可能引入循环依赖。
package request
