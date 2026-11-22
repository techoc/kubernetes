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

// Package v1alpha1 定义 component-base 共享配置（客户端连接、调试、leader election）的
// 外部版本 v1alpha1；内部表示位于 k8s.io/component-base/config，
// 二者之间的转换由 conversion-gen 生成的代码完成。
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	// 空初始化的 builder：本包的注册函数（defaulting/conversion）在 zz_generated 文件中
	// 通过 init 追加，从而保持手写文件与生成文件解耦。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder extends the SchemeBuilder instance with the external types. In this package,
	// defaulting and conversion init funcs are registered as well.
	// 用指针而非值，确保生成代码的 init 追加的函数能反映到 SchemeBuilder 本身。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is a global function that registers this API group & version to a scheme
	// 对外暴露的入口，供调用方把 v1alpha1 类型加入自己的 runtime.Scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)
