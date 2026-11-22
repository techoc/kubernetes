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

// Package v1beta1 is the v1beta1 version of the API.
//
// 这是 apiserver 配置 API 的 v1beta1 外部版本。外部版本用于序列化（配置文件、
// 网络协议），必须保持向后兼容：字段只能新增、不能删除或改变语义；内部版本
// k8s.io/apiserver/pkg/apis/apiserver 承载默认值填充与转换后的规范形态。
package v1beta1
