/*
Copyright 2024 The Kubernetes Authors.

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

// +groupName=apidiscovery.k8s.io

// Package v2 定义聚合发现（Aggregated Discovery）的 v2 API 类型。
// 相比 v1，v2 以精简的组/版本/资源层级描述发现信息，
// 便于大规模集群下减少发现请求的响应体积。
package v2
