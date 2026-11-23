/*
Copyright 2021 The Kubernetes Authors.

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

// Package metrics contains metrics interfaces and implementations for PodSecurity admission
//
// 本包把策略评估结果转成可观测指标。接口与实现分离是为了让准入链只依赖抽象，
// 测试时可替换为桩实现，无需真的启动指标后端。
package metrics
