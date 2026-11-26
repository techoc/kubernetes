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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// 手写的 deepcopy 桩，替代 deepcopy-gen 输出，使 register-gen 的测试不必依赖
// 多个生成器的执行顺序。SimpleType 无引用字段，因此可以直接值拷贝。

// // DeepCopyInto 是手写的深拷贝桩：复制接收者并写入 out，out 必须非 nil。
func (in *SimpleType) DeepCopyInto(out *SimpleType) {
	*out = *in
}

// // DeepCopy 是手写的深拷贝桩：复制接收者并返回新对象。
func (in *SimpleType) DeepCopy() *SimpleType {
	// nil 接收者返回 nil，调用方无需额外判空。
	if in == nil {
		return nil
	}
	out := new(SimpleType)
	in.DeepCopyInto(out)
	return out
}

// // DeepCopyObject 是手写的深拷贝桩：返回满足 runtime.Object 的副本。
func (in *SimpleType) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
