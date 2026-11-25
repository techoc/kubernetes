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

// +k8s:deepcopy-gen=package

// 本包只承载准入配置文件在内存中的内部表示，apiserver 读入配置后即可解码到此处的类型。
// 与对外版本解耦，避免配置格式的演进牵动 API 兼容性。

// Package api 包含 PodSecurity 准入配置文件的内部类型。
package api
