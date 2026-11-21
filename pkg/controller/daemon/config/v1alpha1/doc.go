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

// Package v1alpha1 提供 DaemonSet 控制器配置的外部版本类型。
// 下面的生成指令为这些类型生成深拷贝函数，并生成到内部配置包
// k8s.io/kubernetes/pkg/controller/daemon/config 的双向转换函数；
// external-types 指令用于排除同时注册在 kube-controller-manager 外部配置中的类型，
// 避免转换生成器重复处理。
// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/kubernetes/pkg/controller/daemon/config
// +k8s:conversion-gen-external-types=k8s.io/kube-controller-manager/config/v1alpha1

package v1alpha1
