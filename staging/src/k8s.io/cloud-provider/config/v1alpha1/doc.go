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

// 注意：涉及类型转换的通用 ComponentConfig 包（例如 external 包）必须作为输入
// 传给 conversion-gen，它才能找到并调用对应的原生转换函数。

// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/component-base/config/v1alpha1
// +k8s:conversion-gen=k8s.io/cloud-provider/config
// +k8s:conversion-gen=k8s.io/cloud-provider/config/v1alpha1
// +k8s:conversion-gen=k8s.io/cloud-provider/controllers/service/config/v1alpha1
// +k8s:conversion-gen=k8s.io/controller-manager/config/v1alpha1
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:openapi-model-package=io.k8s.cloud-provider.config.v1alpha1

// +groupName=cloudcontrollermanager.config.k8s.io

// Package v1alpha1 定义 cloud-controller-manager 组件配置的 v1alpha1 外部 API 类型，
// 用于 --config 文件与 /configz 的序列化；内部表示位于 k8s.io/cloud-provider/config。
// 外部版本只做结构承载，实际运行逻辑始终基于转换后的内部类型。
package v1alpha1
