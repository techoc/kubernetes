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
// +k8s:conversion-gen=k8s.io/apiserver/pkg/apis/apiserver
// +k8s:defaulter-gen=TypeMeta
// +groupName=apiserver.k8s.io
// +groupName=apiserver.config.k8s.io

// Package v1alpha1 是 API 的 v1alpha1 版本
//
// 该包同时承载两个 API 组的类型（apiserver.k8s.io 与 apiserver.config.k8s.io），
// 主要存放 kube-apiserver 的配置文件结构（如认证、鉴权、准入、加密配置）。
// 版本为 v1alpha1，字段语义可能变化，使用方需容忍后续的破坏性调整。
package v1alpha1
