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

package pointer

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 是 defaulter-gen 生成的标准注册入口，测试中不直接调用。
//
//nolint:unused
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Tpointer 为布尔指针字段设置默认值 true。
// 仅在字段为 nil 时分配新值，避免覆盖用户显式设置的 false。
func SetDefaults_Tpointer(obj *Tpointer) {
	if obj.BoolField == nil {
		obj.BoolField = new(bool)
		*obj.BoolField = true
	}
}
