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
// +k8s:conversion-gen=k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota
// +k8s:defaulter-gen=TypeMeta
// +groupName=resourcequota.admission.k8s.io

// Package v1alpha1 是 API 的 v1alpha1 版本。
//
// 这是 resourcequota 准入配置的外部版本；conversion-gen 会生成到内部
// 包（apis/resourcequota）的转换代码，因此字段语义必须与内部类型保持一致。
package v1alpha1
