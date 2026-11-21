/*
Copyright 2019 The Kubernetes Authors.

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
// +k8s:conversion-gen=k8s.io/kubernetes/pkg/controller/podautoscaler/config
// +k8s:conversion-gen-external-types=k8s.io/kube-controller-manager/config/v1alpha1

// Package v1alpha1 包含 Pod 自动扩缩容控制器配置的 v1alpha1 外部版本。
// 其转换目标为内部版本 pkg/controller/podautoscaler/config，外部类型来自
// kube-controller-manager 的 config/v1alpha1；转换与深拷贝代码均由生成器产生。
package v1alpha1
