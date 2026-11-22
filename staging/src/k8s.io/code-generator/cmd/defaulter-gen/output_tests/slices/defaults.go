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

package slices

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 是手写入口，模拟 kube-apiserver 中注册默认化函数的惯例；
// 测试需要它存在，以便与生成的 RegisterDefaults 一起被调用。
//
//nolint:unused
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Ttest 手写 base defaulter：仅处理布尔指针字段。
// 生成器识别到它后不会再为 Ttest 生成 SetDefaults_Ttest，
// 但会生成调用它的 SetObjectDefaults_Ttest。
func SetDefaults_Ttest(obj *Ttest) {
	if obj.BoolField == nil {
		obj.BoolField = new(bool)
		*obj.BoolField = true
	}
}
