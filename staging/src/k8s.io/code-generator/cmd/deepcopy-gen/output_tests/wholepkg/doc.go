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

// wholepkg 用包级 +k8s:deepcopy-gen=package 打开生成，覆盖内置类型、map、slice、
// 接口、别名以及手动实现 DeepCopy 的类型；具体类型分布在同目录的 a.go/b.go 中。
package wholepkg
