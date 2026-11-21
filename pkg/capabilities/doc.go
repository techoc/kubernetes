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

// Package capabilities 管理系统级别的能力（capabilities）配置，
// 例如是否允许特权容器、哪些 Pod 来源可以使用宿主命名空间，
// 以及单连接的带宽限制。配置是进程全局的，只能初始化一次。
package capabilities
