/*
Copyright 2017 The Kubernetes Authors.

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

package wholepkg

// Selector 验证生成器遇到接口类型时的行为：
// 接口不能直接深拷贝，但若被结构体字段引用，生成代码应调用其自身声明的
// DeepCopySelector 方法，而不是尝试复制接口底层值。
type Selector interface {
	DeepCopySelector() Selector
}
