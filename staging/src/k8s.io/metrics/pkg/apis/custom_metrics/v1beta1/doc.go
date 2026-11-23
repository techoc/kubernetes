/*
Copyright 2017 The Kubernetes Authors.

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
// +k8s:conversion-gen=k8s.io/metrics/pkg/apis/custom_metrics
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.metrics.pkg.apis.custom_metrics.v1beta1

// Package v1beta1 is the v1beta1 version of the custom_metrics API.
//
// 中文说明：本文件集中放生成指令；自定义指标的类型与查询选项定义在 types.go，
// 需要配合安装步骤把内部版本与外部版本一并注册到 scheme。
package v1beta1
