/*
Copyright The Kubernetes Authors.

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
	// SchemeBuilder 是带有本 API 包 scheme 初始化函数的 scheme 构造器，
	// 默认为空，由本包生成的 zz_generated.*.go 中的 init 向其中注册
	// AddToScheme 与转换/默认值函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 在 SchemeBuilder 的基础上加入外部类型，本包中的默认值
	// 与转换初始化函数也注册在这里：它是指向 SchemeBuilder 的指针，
	// 生成的代码通过它注册外部类型的默认值函数。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把本 API 组与版本注册进 scheme 的全局函数：
	// 把本版本的类型注册进 runtime.Scheme，供 config 解码与转换使用。
	AddToScheme = localSchemeBuilder.AddToScheme
)
