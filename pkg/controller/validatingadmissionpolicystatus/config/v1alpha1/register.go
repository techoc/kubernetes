/*
Copyright 2023 The Kubernetes Authors.

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
	// SchemeBuilder 保存该 API 包所有需要执行的 scheme 初始化函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 指向同一个 builder，生成代码会向其追加外部类型以及默认值、
	// 转换相关的初始化函数；通过别名调用可确保这些追加项对 AddToScheme 生效。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把该 API 组与版本注册到 scheme 的全局入口。
	AddToScheme = localSchemeBuilder.AddToScheme
)
