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

// Package filters contains all the http handler chain filters which
// _are_ api related, i.e. which are prerequisite for the API services
// to work (in contrast to the filters in the server package which are
// not part of the API contract).
//
// Package filters 汇总与 API 契约直接相关的 HTTP 过滤器（认证、鉴权、
// 审计、请求信息、警告等）。与 server 包中的过滤器不同，这里的顺序和
// 行为属于对外可观察的 API 语义，调整时必须考虑兼容性。
package filters
