/*
Copyright 2018 The Kubernetes Authors.

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
// +groupName=cloudcontrollermanager.config.k8s.io

// Package config 定义 cloud-controller-manager 的内部组件配置类型。
// 该包是组件配置的规范版本（hub），外部 v1alpha1 通过自动生成的转换函数
// 与之互转，业务代码只依赖这里的内部类型。
package config
