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

// Package scheme 持有用于 Scale 各版本序列化、反序列化及相互转换的 runtime.Scheme。
//
// 内部版本与 extensions/v1beta1、apps/v1beta2 等外部版本都注册于此，
// 转换器据此在不同版本间映射；上方指令表示为本包所有类型生成 DeepCopy 方法。
package scheme
