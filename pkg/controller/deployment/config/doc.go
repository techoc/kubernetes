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

// Package config 定义 Deployment 控制器的内部配置类型。
// 本包只声明配置结构，不做默认值或转换处理：外部版本到内部版本的转换与默认值
// 由 v1alpha1 包中生成的转换函数完成，控制器只消费内部类型。
// +k8s:deepcopy-gen=package

package config
