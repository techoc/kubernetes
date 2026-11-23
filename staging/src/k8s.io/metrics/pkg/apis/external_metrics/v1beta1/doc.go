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

// +k8s:deepcopy-gen=package
// +k8s:protobuf-gen=package
// +k8s:conversion-gen=k8s.io/metrics/pkg/apis/external_metrics
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.metrics.pkg.apis.external_metrics.v1beta1

// Package v1beta1 is the v1beta1 version of the external metrics API.
//
// 中文说明：本文件只承载该版本的生成指令，真正的指标结构与注册逻辑在其他文件里；
// 外部指标由外部适配器提供，与集群内资源指标走不同的 API group。
package v1beta1
