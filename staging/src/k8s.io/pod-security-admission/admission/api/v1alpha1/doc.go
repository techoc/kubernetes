/*
Copyright 2021 The Kubernetes Authors.

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
// +k8s:conversion-gen=k8s.io/pod-security-admission/admission/api
// +k8s:defaulter-gen=TypeMeta
// +k8s:validation-gen=false
// +groupName=pod-security.admission.config.k8s.io

// Package v1alpha1 contains PodSecurity admission configuration file types
//
// 本包是配置文件格式的旧版本，仅保留用于读取已有文件；新配置应写到 v1。
package v1alpha1
