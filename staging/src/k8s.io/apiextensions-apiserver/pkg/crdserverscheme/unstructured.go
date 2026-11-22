/*
Copyright 2016 The Kubernetes Authors.

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

// Package crdserverscheme 提供 CR 的 runtime.ObjectTyper 实现。
// 由于 CR 是动态注册的，API Server 无法在编译时知道它们的类型。
// UnstructuredObjectTyper 通过读取对象的 apiVersion 和 kind 字段来识别 CR 类型。
package crdserverscheme

import (
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// UnstructuredObjectTyper 为 runtime.Unstructured 对象提供类型识别功能。
// CR 在运行时以 Unstructured 形式存在，此 typer 从对象的 apiVersion/kind 字段提取 GVK。
type UnstructuredObjectTyper struct {
}

// NewUnstructuredObjectTyper 创建 UnstructuredObjectTyper 实例。
// TODO: 仅适用于 apiextensions server，不识别任何预定义类型。应移到使用点。
func NewUnstructuredObjectTyper() *UnstructuredObjectTyper {
	dot := &UnstructuredObjectTyper{}
	return dot
}

// ObjectKinds 从 Unstructured 对象中提取 GVK（GroupVersionKind）。
// 非 Unstructured 对象返回未注册错误。unversionedType 始终为 false，
// 因为 Unstructured 对象总是包含完整的 GVK 信息。
func (d *UnstructuredObjectTyper) ObjectKinds(obj runtime.Object) (gvks []schema.GroupVersionKind, unversionedType bool, err error) {
	if _, ok := obj.(runtime.Unstructured); ok {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if len(gvk.Kind) == 0 {
			return nil, false, runtime.NewMissingKindErr("object has no kind field ")
		}
		if len(gvk.Version) == 0 {
			return nil, false, runtime.NewMissingVersionErr("object has no apiVersion field")
		}
		return []schema.GroupVersionKind{gvk}, false, nil
	}

	return nil, false, runtime.NewNotRegisteredErrForType("crdserverscheme.UnstructuredObjectTyper", reflect.TypeOf(obj))
}

// Recognizes 始终返回 false。
// UnstructuredObjectTyper 不维护类型注册表，无法预先判断 GVK 是否有效。
func (d *UnstructuredObjectTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	return false
}

var _ runtime.ObjectTyper = &UnstructuredObjectTyper{}
