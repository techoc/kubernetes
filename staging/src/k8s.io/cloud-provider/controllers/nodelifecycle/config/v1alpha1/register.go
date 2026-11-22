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
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	// 中文说明：默认为空，由本包生成的 zz_generated.*.go 中的 init 向其中注册
	// AddToScheme 与转换/默认值函数。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder extends the SchemeBuilder instance with the external types. In this package,
	// defaulting and conversion init funcs are registered as well.
	// 中文说明：指向 SchemeBuilder 的指针，生成的代码通过它注册外部类型的默认值函数。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is a global function that registers this API group & version to a scheme
	// 中文说明：把本版本的类型注册进 runtime.Scheme，供 config 解码与转换使用。
	AddToScheme = localSchemeBuilder.AddToScheme
)
