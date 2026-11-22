/*
Copyright 2014 The Kubernetes Authors.

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

// Package endpoints contains the generic code that provides a RESTful Kubernetes-style API service.
//
// 中文说明：本包负责把一个 registry 暴露为带版本协商、watch、分页、字段选择器等
// 语义的 Kubernetes 风格 REST API，是 kube-apiserver 与聚合 apiserver 共用的骨架。
package endpoints
