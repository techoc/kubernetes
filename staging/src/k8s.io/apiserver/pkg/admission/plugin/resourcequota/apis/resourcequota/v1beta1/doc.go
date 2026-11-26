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

// Package v1beta1 是本 API 的 v1beta1 版本。
//
// 这是 resourcequota 准入插件配置的 v1beta1 外部版本。该组的对象只在
// apiserver 内部使用，不对外暴露 REST 端点；+k8s 标记负责生成 deepcopy、
// 与内部版本之间的转换以及 TypeMeta 的默认值。
package v1beta1
