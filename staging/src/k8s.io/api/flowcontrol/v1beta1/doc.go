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
// +k8s:protobuf-gen=package
// +k8s:openapi-gen=true
// +k8s:prerelease-lifecycle-gen=true
// +k8s:openapi-model-package=io.k8s.api.flowcontrol.v1beta1

// +groupName=flowcontrol.apiserver.k8s.io

// Package v1beta1 存放 "flowcontrol.apiserver.k8s.io" API 组的 v1alpha1 版本类型。
// 注：包名是 v1beta1，但此处注释沿用上游对 v1alpha1 的表述。
package v1beta1
