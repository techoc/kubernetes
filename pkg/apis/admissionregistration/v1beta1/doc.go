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

// +k8s:conversion-gen=k8s.io/kubernetes/pkg/apis/admissionregistration
// +k8s:conversion-gen-external-types=k8s.io/api/admissionregistration/v1beta1
// +k8s:defaulter-gen=TypeMeta
// +k8s:defaulter-gen-input=k8s.io/api/admissionregistration/v1beta1
// +groupName=admissionregistration.k8s.io
// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-input=k8s.io/api/admissionregistration/v1beta1

// Package v1beta1 是该 API 的 v1beta1 版本。
// AdmissionConfiguration 与 AdmissionPluginConfiguration 是遗留的静态准入插件配置。
// ValidatingWebhookConfiguration 与 MutatingWebhookConfiguration 用于
// 新的动态准入控制器配置。
package v1beta1
