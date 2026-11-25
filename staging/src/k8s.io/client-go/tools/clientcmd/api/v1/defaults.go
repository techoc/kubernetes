/*
Copyright 2021 The Kubernetes Authors.

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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 把本包手工维护的默认值函数注册到 scheme；由 zz_generated.defaults.go
// 生成的代码通过它接入 scheme 的默认化流程。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ExecConfig 为旧版 exec 插件配置补齐 interactiveMode。
//
// v1beta1/v1alpha1 产生时还没有 interactiveMode 字段，为了向后兼容，缺省设为
// IfAvailableExecInteractiveMode，使旧插件在有 stdin 时仍能拿到输入。v1 要求显式声明，
// 因此其他版本保持空值，由后续校验拒绝，避免“意外获得 stdin”。
func SetDefaults_ExecConfig(exec *ExecConfig) {
	if len(exec.InteractiveMode) == 0 {
		switch exec.APIVersion {
		case "client.authentication.k8s.io/v1beta1", "client.authentication.k8s.io/v1alpha1":
			// 为向后兼容，默认使用 IfAvailableExecInteractiveMode
			exec.InteractiveMode = IfAvailableExecInteractiveMode
		default:
			// 要求其他版本显式声明是否需要 stdin
		}
	}
}
