/*
Copyright 2024 The Kubernetes Authors.

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

// Package features 包含专门用于管理 zpages 相关特性的独立特性集。
// 这些特性门控控制 Kubernetes 组件中各类 zpages 的可用性与行为。
// 组件新增 zpages 时应使用此特性集，以正确管理其可用性。
// 该特性集独立于组件主特性集，使 zpages 的启停可以单独演进，也便于在模拟旧版本时关闭新端点。
package features
