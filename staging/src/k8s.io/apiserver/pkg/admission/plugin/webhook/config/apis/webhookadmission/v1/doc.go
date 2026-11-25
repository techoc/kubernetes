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
// +k8s:conversion-gen=k8s.io/apiserver/pkg/admission/plugin/webhook/config/apis/webhookadmission
// +k8s:defaulter-gen=TypeMeta
// +groupName=apiserver.config.k8s.io

// Package v1 是该 API 的 v1 版本。
// 该版本是 Webhook 准入控制器配置的稳定版本，通过 conversion-gen
// 转换为内部版本后使用；版本升级时旧字段必须保持向后兼容。
package v1
