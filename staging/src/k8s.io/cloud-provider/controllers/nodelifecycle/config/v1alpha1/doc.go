/*
Copyright The Kubernetes Authors.

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
// +k8s:conversion-gen=k8s.io/cloud-provider/controllers/nodelifecycle/config
// +k8s:conversion-gen=k8s.io/cloud-provider/controllers/nodelifecycle/config/v1alpha1
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.cloud-provider.controllers.nodelifecycle.config.v1alpha1

// Package v1alpha1 定义 NodeLifecycleController 的外部（版本化）组件配置。
// 该版本仅用于配置文件读写，内部逻辑统一使用 config 包中的规范类型。
package v1alpha1
