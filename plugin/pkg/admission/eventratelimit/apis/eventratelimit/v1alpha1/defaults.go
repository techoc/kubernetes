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

// Package v1alpha1 提供事件速率限制配置 v1alpha1 版本的默认值设置与类型注册。
package v1alpha1

import kruntime "k8s.io/apimachinery/pkg/runtime"

// addDefaultingFuncs 向 scheme 注册生成的默认值设置函数。
func addDefaultingFuncs(scheme *kruntime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Configuration 为 Configuration 设置默认值；目前无需填充任何字段。
func SetDefaults_Configuration(obj *Configuration) {}
