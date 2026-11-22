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
// +k8s:protobuf-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.api.scheduling.v1alpha3

// +groupName=scheduling.k8s.io

// +k8s:validation-gen=*
// +k8s:validation-gen-input=k8s.io/api/scheduling/v1alpha3
// +k8s:validation-gen-scheme-registry=nil

// Package v1alpha3 是 scheduling API 的 v1alpha3 版本。
//
// 本包中会生成一份不注册的声明式校验副本，使树外消费者无需导入 k8s.io/kubernetes/pkg
// 也无需在导入时修改共享 scheme，即可运行与 kube-apiserver 相同的校验。已注册的副本位于
// k8s.io/kubernetes/pkg/apis/scheduling/v1alpha3。
// `*` 选择器会为所有类型生成校验器，包括无法从 TypeMeta 根类型到达的 WorkloadPodGroup*
// 构建块。

package v1alpha3
