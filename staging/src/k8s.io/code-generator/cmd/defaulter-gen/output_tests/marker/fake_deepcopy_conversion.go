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

package marker

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// 本文件手写 DeepCopy/DeepCopyObject/GetObjectKind，充当 deepcopy-gen 与
// register-gen 的输出，使 defaulter-gen 的 output test 不必依赖其他生成器即可编译。
// 这些类型只有标量字段，因此 DeepCopyInto 可以直接整体赋值。

// DeepCopy 返回一份独立副本；nil 接收者保持 nil，与生成代码的约定一致。
func (in *Defaulted) DeepCopy() *Defaulted {
	if in == nil {
		return nil
	}
	out := new(Defaulted)
	in.DeepCopyInto(out)
	return out
}

func (in *Defaulted) DeepCopyInto(out *Defaulted) {
	*out = *in
}

// DeepCopyObject 让类型满足 runtime.Object，供 scheme 注册与默认值分派使用。
func (in *Defaulted) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *DefaultedOmitempty) DeepCopy() *DefaultedOmitempty {
	if in == nil {
		return nil
	}
	out := new(DefaultedOmitempty)
	in.DeepCopyInto(out)
	return out
}

func (in *DefaultedOmitempty) DeepCopyInto(out *DefaultedOmitempty) {
	*out = *in
}

func (in *DefaultedOmitempty) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *DefaultedWithFunction) DeepCopy() *DefaultedWithFunction {
	if in == nil {
		return nil
	}
	out := new(DefaultedWithFunction)
	in.DeepCopyInto(out)
	return out
}

func (in *DefaultedWithFunction) DeepCopyInto(out *DefaultedWithFunction) {
	*out = *in
}

func (in *DefaultedWithFunction) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// GetObjectKind 返回空的 ObjectKind，满足 runtime.Object 接口。
func (in *Defaulted) GetObjectKind() schema.ObjectKind              { return schema.EmptyObjectKind }
func (in *DefaultedOmitempty) GetObjectKind() schema.ObjectKind     { return schema.EmptyObjectKind }
func (in *DefaultedWithFunction) GetObjectKind() schema.ObjectKind  { return schema.EmptyObjectKind }
func (in *DefaultedWithReference) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (in *DefaultedWithReference) DeepCopy() *DefaultedWithReference {
	if in == nil {
		return nil
	}
	out := new(DefaultedWithReference)
	in.DeepCopyInto(out)
	return out
}

func (in *DefaultedWithReference) DeepCopyInto(out *DefaultedWithReference) {
	*out = *in
}

func (in *DefaultedWithReference) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
