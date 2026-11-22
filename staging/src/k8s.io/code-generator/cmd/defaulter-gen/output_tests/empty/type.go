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

package empty

// Ttest 是本输出测试的最小类型，覆盖四种内建字段，用于验证 defaulter-gen
// 在没有任何默认值标记时也能正确生成（空的）默认值函数。
// Only test
type Ttest struct {
	BoolField   bool
	IntField    int
	StringField string
	FloatField  float64
}

// TypeMeta 用于验证包级 "+k8s:defaulter-gen=covers" 场景下的类型处理；
// 命名刻意与 metav1.TypeMeta 不同，避免引入额外依赖。
type TypeMeta struct {
	Fortest bool
}
