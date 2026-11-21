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

// Package v1alpha1 定义 DeviceTaintEviction 控制器配置的 v1alpha1 外部版本。
//
// 这里之所以只放生成指令而不声明类型，是因为外部配置类型统一定义在
// kube-controller-manager/config/v1alpha1；本包仅为 conversion-gen 提供
// 内部类型与外部类型之间的转换落点。
//
// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/kubernetes/pkg/controller/devicetainteviction/config
// +k8s:conversion-gen-external-types=k8s.io/kube-controller-manager/config/v1alpha1

package v1alpha1
