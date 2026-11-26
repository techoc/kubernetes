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

// Package resourcequota 在请求所属命名空间下，按已应用的配额校验所有进入的请求。
// 按请求所属命名空间下的配额约束所有进入的请求。
// 除配额校验外，该插件还会按 Configuration 的 LimitedResources 规则，
// 拒绝那些消耗受限资源却没有覆盖配额的请求。
package resourcequota
