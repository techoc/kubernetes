/*
Copyright 2016 The Kubernetes Authors.

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
// +k8s:conversion-gen=k8s.io/apimachinery/pkg/apis/meta/v1

// Package internalversion 定义 apimachinery 中仅供 apiserver 内部使用的
// meta 类型（主要是 ListOptions 的内部形态）。它不对外暴露为 API 版本，
// 通过 conversion-gen 与 v1 互相转换，避免内部代码直接依赖外部版本。
package internalversion
