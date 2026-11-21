/*
Copyright 2020 The Kubernetes Authors.

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

// Package v1alpha1 定义 EndpointSlice 镜像控制器配置的外部版本。
// 下面的生成器指令要求为整个包生成深拷贝方法，
// 并针对内部配置包与外部 v1alpha1 类型生成转换代码。
// +k8s:conversion-gen=k8s.io/kubernetes/pkg/controller/endpointslicemirroring/config
// +k8s:conversion-gen-external-types=k8s.io/kube-controller-manager/config/v1alpha1

package v1alpha1
