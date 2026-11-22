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

package pointer

import (
	"k8s.io/code-generator/cmd/defaulter-gen/output_tests/empty"
)

// Tpointer 含一个可为 nil 的 BoolField，用于验证指针字段的默认值生成。
type Tpointer struct {
	empty.TypeMeta
	BoolField *bool
}

// Only test
// Ttest 同时包含值类型与指针类型的 Tpointer，用于验证两种嵌套路径下的
// 默认值传播。
type Ttest struct {
	empty.TypeMeta
	NTP Tpointer
	Tp  *Tpointer
}
