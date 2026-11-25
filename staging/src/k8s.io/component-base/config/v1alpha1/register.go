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
	// SchemeBuilder 是运行本 API 包 scheme 初始化函数所用的构建器。
	// 初始为空；本包注册函数在 zz_generated 文件中通过 init 追加，保持手写与生成文件解耦。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 用外部类型扩展 SchemeBuilder，并注册默认化与转换初始化函数。
	// 用指针而非值，确保生成代码追加的函数能反映到 SchemeBuilder 本身。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是全局函数，用于把本 API 组及版本注册到 scheme。
	// 对外暴露的入口，供调用方把 v1alpha1 类型加入自己的 runtime.Scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)
