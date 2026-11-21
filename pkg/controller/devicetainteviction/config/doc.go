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

// Package config 定义 DeviceTaintEviction 控制器的内部配置类型。
//
// 该包是配置的内部表示，外部版本位于 kube-controller-manager/config/v1alpha1，
// 二者之间通过自动生成的转换函数互转。
//
// +k8s:deepcopy-gen=package

package config
