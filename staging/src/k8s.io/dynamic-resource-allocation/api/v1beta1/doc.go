/*
Copyright 2025 The Kubernetes Authors.

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

// k8s:conversion-gen-external-types specifies the latest API version in k8s.io/api/resource.
// k8s:conversion-gen specifies the older version.
//
// +k8s:conversion-gen=k8s.io/api/resource/v1beta1
// +k8s:conversion-gen-external-types=k8s.io/api/resource/v1

// Package v1beta1 provides conversion code between the v1beta1 version of the resource API
// and the latest API version.
//
// v1beta1 包提供资源 API 的 v1beta1 版本与最新版本（v1）之间的转换代码，
// 主要弥补两版本在 DeviceRequest、Device、ResourceSliceSpec 等结构上的差异：
// 例如把 v1beta1 的顶层请求字段折叠进 v1 的 Exactly，以及把 BasicDevice 拆平。
package v1beta1
