/*
Copyright 2018 The Kubernetes Authors.

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

package v1beta1

import kruntime "k8s.io/apimachinery/pkg/runtime"

// addDefaultingFuncs 把默认值函数注册进 scheme，供解码时自动填充。
func addDefaultingFuncs(scheme *kruntime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Configuration 当前无默认值可填。保留空实现是为了让生成代码
// 有明确注册点，同时为将来新增字段预留兼容入口（新增字段默认值应加在此处）。
func SetDefaults_Configuration(obj *Configuration) {}
