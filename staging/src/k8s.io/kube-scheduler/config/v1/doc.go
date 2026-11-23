/*
Copyright 2022 The Kubernetes Authors.

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

// 这个文件专门用来承载包级生成指令和 API 组名。集中放在一处，生成器只需读一次，
// 类型文件也不必夹带这些元信息。指令 +groupName=kubescheduler.config.k8s.io 给出本 API 组的标识。
// 指令与 package 之间保留空行，避免它们被当成包文档注释。

// +k8s:deepcopy-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.kube-scheduler.config.v1

// +groupName=kubescheduler.config.k8s.io

package v1
