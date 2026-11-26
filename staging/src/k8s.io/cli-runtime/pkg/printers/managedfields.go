/*
Copyright 2021 The Kubernetes Authors.

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
	"io"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// OmitManagedFieldsPrinter 包装已有 printer，输出前去掉对象的 managedFields。
//
// 用于输出用户关心的内容：managedFields 通常冗长且属于服务端记账信息。
type OmitManagedFieldsPrinter struct {
	Delegate ResourcePrinter
}

// 编译期断言接口实现，避免 Delegate 相关重构悄悄破坏契约。
var _ ResourcePrinter = (*OmitManagedFieldsPrinter)(nil)

// omitManagedFields 就地清空 managedFields；对象不是 metav1.Object 时原样返回。
func omitManagedFields(o runtime.Object) runtime.Object {
	a, err := meta.Accessor(o)
	if err != nil {
		// 对象不是 `metav1.Object`，直接忽略。
		return o
	}
	a.SetManagedFields(nil)
	return o
}

// PrintObj 先复制对象，去掉副本的 managedFields 后再输出，避免改动原对象。
//
// 必须先深拷贝：直接清空会污染调用方持有的对象（以及缓存中的对象）。
func (p *OmitManagedFieldsPrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	if obj == nil {
		return p.Delegate.PrintObj(obj, w)
	}
	if meta.IsListType(obj) {
		obj = obj.DeepCopyObject()
		_ = meta.EachListItem(obj, func(item runtime.Object) error {
			omitManagedFields(item)
			return nil
		})
	} else if _, err := meta.Accessor(obj); err == nil {
		obj = omitManagedFields(obj.DeepCopyObject())
	}
	return p.Delegate.PrintObj(obj, w)
}
