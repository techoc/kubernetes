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

// Package config 定义 Kubernetes 组件共享的内部配置类型：客户端连接、
// 调试（profiling）以及 leader election 参数。
//
// 这些类型被 apiserver、controller-manager、scheduler 等组件的
// 配置文件复用，字段语义和默认值属于跨组件契约，变更时需保持兼容。
package config
