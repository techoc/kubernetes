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

package workloadbuilder

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// localSchemeBuilder 留作本包类型注册到 Scheme 的入口。
// 目前没有需要注册的类型（IR 类型是纯内存结构，不参与 API 序列化），
// 但保留它能为将来新增类型提供一致的位置，避免届时改动调用方。
var localSchemeBuilder = &runtime.SchemeBuilder{}
