/*
Copyright 2020 The Kubernetes Authors.

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

package marker

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 模拟手写 defaulter 的注册入口。生成代码只引用
// SetObjectDefaults_* 包装函数，真正注册进 scheme 需要这里显式挂载，
// 因此测试也用它验证生成代码与手写注册的衔接。
//
//nolint:unused
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_DefaultedWithFunction 是被 defaulter-gen 识别的手写默认值函数，
// 命名必须严格为 SetDefaults_<Type> 才会被生成的 SetObjectDefaults_<Type> 调用。
func SetDefaults_DefaultedWithFunction(obj *DefaultedWithFunction) {
	if obj.S1 == "" {
		obj.S1 = "default_function"
	}
}
