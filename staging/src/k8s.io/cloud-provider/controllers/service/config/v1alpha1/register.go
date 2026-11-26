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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// 该文件注册 ServiceController 配置 v1alpha1 的类型、默认化与转换初始化函数。
var (
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 在 SchemeBuilder 上追加外部类型；本包还注册默认值与转换函数。
	// 生成的 defaults.go 会向该 builder 追加初始化函数，因此必须使用指针。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把本 API 组与版本注册到 scheme 的全局入口。
	// 调用方用它把该版本接入组件配置 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)
