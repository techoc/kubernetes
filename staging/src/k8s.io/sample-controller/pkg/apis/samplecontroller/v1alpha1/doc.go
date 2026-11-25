/*
Copyright 2017 The Kubernetes Authors.

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
// +k8s:openapi-gen=true
// +groupName=samplecontroller.k8s.io
// +k8s:openapi-model-package=io.k8s.sample-controller.pkg.apis.samplecontroller.v1alpha1

// Package v1alpha1 是 samplecontroller API 的 v1alpha1 版本。
// 本包定义 Foo 自定义资源的类型与版本信息。
// API 版本单独成包，是为了在未来引入 v1beta1/v1 时能够并存并完成渐进式迁移。
// 指令 +k8s:deepcopy-gen=package 与 +groupName=samplecontroller.k8s.io 分别供代码生成器确定深拷贝实现和 API 组名。
package v1alpha1
