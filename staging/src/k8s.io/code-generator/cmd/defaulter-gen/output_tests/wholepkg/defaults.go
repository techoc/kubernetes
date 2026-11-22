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

package wholepkg

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 把生成/手写的默认值函数注册进 scheme。
// 之所以存在：旧版注册约定要求每个 API 包提供该入口。
//
//nolint:unused
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_StructPrimitives 为 BoolField 提供默认值 true，
// 用于验证 defaulter-gen 能正确处理指针字段（nil 时新建并赋值）。
func SetDefaults_StructPrimitives(obj *StructPrimitives) {
	if obj.BoolField == nil {
		obj.BoolField = new(bool)
		*obj.BoolField = true
	}
}
