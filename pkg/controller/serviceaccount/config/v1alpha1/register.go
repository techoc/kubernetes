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
	// SchemeBuilder 是本 API 包的 scheme 构建器，持有需要运行的 scheme 初始化函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 为 SchemeBuilder 实例补充外部类型；本包还在此注册默认值与转换的初始化函数。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是全局函数，用于把本 API 组与版本注册到 scheme 中。
	AddToScheme = localSchemeBuilder.AddToScheme
)
