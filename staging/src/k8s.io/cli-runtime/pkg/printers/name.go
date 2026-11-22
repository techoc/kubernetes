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

package printers

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// NamePrinter is an implementation of ResourcePrinter which outputs "resource/name" pair of an object.
type NamePrinter struct {
	// ShortOutput indicates whether an operation should be
	// printed along side the "resource/name" pair for an object.
	ShortOutput bool
	// Operation describes the name of the action that
	// took place on an object, to be included in the
	// finalized "successful" message.
	Operation string
}

// PrintObj is an implementation of ResourcePrinter.PrintObj which decodes the object
// and print "resource/name" pair. If the object is a List, print all items in it.
//
// 输出形如 deployment.apps/nginx 或 pod/foo；List 会逐个展开后打印。
func (p *NamePrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	// watch 事件把真实对象包在 .Object.Object 里，先拆一层再取 kind/name。
	switch castObj := obj.(type) {
	case *metav1.WatchEvent:
		obj = castObj.Object.Object
	}

	// we use reflect.Indirect here in order to obtain the actual value from a pointer.
	// using reflect.Indirect indiscriminately is valid here, as all runtime.Objects are supposed to be pointers.
	// we need an actual value in order to retrieve the package path for an object.
	//
	// 拦住内部 API 包（如 k8s.io/kubernetes/pkg/apis/...）的对象：它们不应被 CLI 直接打印。
	if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(InternalObjectPrinterErr)
	}

	if meta.IsListType(obj) {
		// we allow unstructured lists for now because they always contain the GVK information.  We should chase down
		// callers and stop them from passing unflattened lists
		// TODO chase the caller that is setting this and remove it.
		//
		// 只接受 unstructured list：其他 list 类型可能缺少逐项 GVK，无法可靠输出 kind。
		if _, ok := obj.(*unstructured.UnstructuredList); !ok {
			return fmt.Errorf("list types are not supported by name printing: %T", obj)
		}

		items, err := meta.ExtractList(obj)
		if err != nil {
			return err
		}
		// 递归打印每一项，保持与单对象相同的输出规则。
		for _, obj := range items {
			if err := p.PrintObj(obj, w); err != nil {
				return err
			}
		}
		return nil
	}

	// 没有 apiVersion/kind 时无法确定资源类型，直接报错而不是输出 "pod/<unknown>"。
	if obj.GetObjectKind().GroupVersionKind().Empty() {
		return fmt.Errorf("missing apiVersion or kind; try GetObjectKind().SetGroupVersionKind() if you know the type")
	}

	// 名称缺失时用占位符，保证输出结构完整且便于发现异常数据。
	name := "<unknown>"
	if acc, err := meta.Accessor(obj); err == nil {
		if n := acc.GetName(); len(n) > 0 {
			name = n
		}
	}

	return printObj(w, name, p.Operation, p.ShortOutput, GetObjectGroupKind(obj))
}

// GetObjectGroupKind 从对象的 TypeMeta 或 unstructured 内容推断 GroupKind；
// 两者都没有时返回 Kind "<unknown>"，由调用方决定如何呈现。
func GetObjectGroupKind(obj runtime.Object) schema.GroupKind {
	if obj == nil {
		return schema.GroupKind{Kind: "<unknown>"}
	}
	groupVersionKind := obj.GetObjectKind().GroupVersionKind()
	if len(groupVersionKind.Kind) > 0 {
		return groupVersionKind.GroupKind()
	}

	if uns, ok := obj.(*unstructured.Unstructured); ok {
		if len(uns.GroupVersionKind().Kind) > 0 {
			return uns.GroupVersionKind().GroupKind()
		}
	}

	return schema.GroupKind{Kind: "<unknown>"}
}

// printObj 组装最终一行输出：核心组为 "kind/name"，非核心组为 "kind.group/name"。
func printObj(w io.Writer, name string, operation string, shortOutput bool, groupKind schema.GroupKind) error {
	if len(groupKind.Kind) == 0 {
		return fmt.Errorf("missing kind for resource with name %v", name)
	}

	// operation 需要前置空格才能与 name 拼接；ShortOutput 时反而省略。
	if len(operation) > 0 {
		operation = " " + operation
	}

	if shortOutput {
		operation = ""
	}

	// 核心组不带 group 后缀，保持 "pod/foo" 这类简洁输出。
	if len(groupKind.Group) == 0 {
		fmt.Fprintf(w, "%s/%s%s\n", strings.ToLower(groupKind.Kind), name, operation)
		return nil
	}

	fmt.Fprintf(w, "%s.%s/%s%s\n", strings.ToLower(groupKind.Kind), groupKind.Group, name, operation)
	return nil
}
