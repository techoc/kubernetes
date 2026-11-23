/*
Copyright 2025 The Kubernetes Authors.

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

// Package v1beta2 负责把 v1beta2 版本的资源类型注册到 scheme，
// 它直接复用 k8s.io/api/resource/v1beta2 中定义的 SchemeBuilder，
// 并通过 AddToScheme 把全部 v1beta2 类型暴露给调用方注册。
package v1beta2

import (
	"k8s.io/api/resource/v1beta2"
)

// localSchemeBuilder 指向 v1beta2 API 包中的 SchemeBuilder，用于保存类型注册函数。
// AddToScheme 是暴露给外部调用的入口，用于把所有 v1beta2 类型添加到指定的 scheme 中。
var (
	localSchemeBuilder = &v1beta2.SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)
