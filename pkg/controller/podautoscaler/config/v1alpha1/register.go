/*
Copyright 2019 The Kubernetes Authors.

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

import (
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// SchemeBuilder 保存本 API 包需要执行的 scheme 初始化函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 在 SchemeBuilder 的基础上注册外部类型，
	// 本包中默认值与转换的初始化函数也通过它注册。
	// 包外代码只需使用公开的 SchemeBuilder/AddToScheme，内部细节对调用方不可见。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把本 API 组和版本注册到 scheme 的全局入口。
	AddToScheme = localSchemeBuilder.AddToScheme
)
