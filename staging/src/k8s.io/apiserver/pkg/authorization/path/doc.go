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

// Package path 包含的授权器允许特定路径与路径前缀。
//
// 该授权器按请求路径做白名单匹配，通常用于 /healthz、/metrics 等
// 不经过资源授权链的端点；它不感知用户身份，因此配置必须谨慎。
package path
