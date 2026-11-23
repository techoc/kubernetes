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

// 命名空间标签是 PodSecurity 准入的入口，本包把标签键、取值与解析逻辑收在一处，
// 让准入侧和策略侧共享同一套解释，避免各处重复解析标签时产生分歧。

// Package api contains constants and helpers for PodSecurity admission label keys and values
package api
