/*
Copyright 2020 The Kubernetes Authors.

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

// Package v1 是从 Gateway API 复制的最小复现包（见 #131533），
// 仅包含手动实现的深拷贝方法，用于验证生成器与手写代码共存。
package v1

// Copied from Gateway API as a minimal reproducer for #131533

// ParentReference 是空结构体，用于验证生成器不会重复生成已存在的手写
// DeepCopyInto/DeepCopy 方法。
type ParentReference struct {
}

// DeepCopyInto 是手写实现：空结构体直接整体赋值即可。
func (in *ParentReference) DeepCopyInto(out *ParentReference) {
	*out = *in
}
