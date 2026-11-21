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
	// localSchemeBuilder 在 SchemeBuilder 上扩展外部类型的注册；本包还会在其中注册
	// 默认值与转换相关的 init 函数。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是将本 API 组与版本注册到 scheme 的全局函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)
