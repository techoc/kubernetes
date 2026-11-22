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

package v1alpha1

import kruntime "k8s.io/apimachinery/pkg/runtime"

// addDefaultingFuncs 向 scheme 注册默认值函数；
// 由 generated 代码通过 SchemeBuilder 调用，保持与 k8s 默认值机制一致。
func addDefaultingFuncs(scheme *kruntime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Configuration 目前没有可设置的默认值。
// 保留该函数是为了让版本化 API 具备完整的 defaulting 注册点，
// 后续新增字段时可以直接在此补充默认值而不必改动生成代码结构。
func SetDefaults_Configuration(obj *Configuration) {}
