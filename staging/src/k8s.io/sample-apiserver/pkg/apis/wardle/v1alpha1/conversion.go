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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/sample-apiserver/pkg/apis/wardle"
)

// Convert_v1alpha1_FlunderSpec_To_wardle_FlunderSpec 把 v1alpha1 的 FlunderSpec 转换为内部版本。
// v1alpha1 用一个 Reference 字段加类型标签表示引用，内部对象拆成两个字段，转换时按标签分流。
func Convert_v1alpha1_FlunderSpec_To_wardle_FlunderSpec(in *FlunderSpec, out *wardle.FlunderSpec, s conversion.Scope) error {
	if in.ReferenceType != nil {
		// 默认化钩子会在解码时补齐 ReferenceType；nil 时不填引用类型，这里不再猜。
		switch *in.ReferenceType {
		case FlunderReferenceType:
			out.ReferenceType = wardle.FlunderReferenceType
			out.FlunderReference = in.Reference
		case FischerReferenceType:
			out.ReferenceType = wardle.FischerReferenceType
			out.FischerReference = in.Reference
		}
	}

	return nil
}

// Convert_wardle_FlunderSpec_To_v1alpha1_FlunderSpec 把内部版本的 FlunderSpec 转换为 v1alpha1。
// 反向转换要把两个引用字段合回一个，并通过指针类型的标签告诉客户端引用的是哪种对象。
func Convert_wardle_FlunderSpec_To_v1alpha1_FlunderSpec(in *wardle.FlunderSpec, out *FlunderSpec, s conversion.Scope) error {
	switch in.ReferenceType {
	case wardle.FlunderReferenceType:
		t := FlunderReferenceType
		out.ReferenceType = &t
		out.Reference = in.FlunderReference
	case wardle.FischerReferenceType:
		t := FischerReferenceType
		out.ReferenceType = &t
		out.Reference = in.FischerReference
	}

	return nil
}
