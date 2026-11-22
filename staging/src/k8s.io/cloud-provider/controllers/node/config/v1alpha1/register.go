/*
Copyright 2022 The Kubernetes Authors.

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

// 本文件把该版本的默认化与转换初始化函数注册进 scheme，供组件配置加载时使用。
var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder extends the SchemeBuilder instance with the external types. In this package,
	// defaulting and conversion init funcs are registered as well.
	// 使用指针是为了让生成的 defaults.go 把 AddToScheme 注册到同一个 builder 上。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is a global function that registers this API group & version to a scheme
	// 调用方通过 AddToScheme 一次性完成类型、默认值和转换函数的注册。
	AddToScheme = localSchemeBuilder.AddToScheme
)
