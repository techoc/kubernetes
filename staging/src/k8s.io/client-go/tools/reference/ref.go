/*
Copyright 2014 The Kubernetes Authors.

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

// Package reference 提供把任意 runtime.Object 转换为 core/v1.ObjectReference 的工具。
//
// ObjectReference 是事件、OwnerReference 等场景里对对象实体的轻量引用，
// 不携带对象正文，只保留定位所需的最小元数据。
package reference

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// GetReference 可能返回的错误。
	ErrNilObject = errors.New("can't reference a nil object")
)

// GetReference 返回指向给定对象的 ObjectReference；若对象不符合
// 允许这样做的约定则返回错误。
// TODO: 应当接受 meta.Interface，见 https://issue.k8s.io/7127
func GetReference(scheme *runtime.Scheme, obj runtime.Object) (*v1.ObjectReference, error) {
	if obj == nil {
		return nil, ErrNilObject
	}
	if ref, ok := obj.(*v1.ObjectReference); ok {
		// 不要对引用再做一层引用。
		//
		// 引用本身已经是引用，再包一层只会丢失语义，直接原样返回。
		return ref, nil
	}

	// 只实现了 List 接口的对象也有足够的元数据来构建引用。
	// 列表对象没有单个对象的元数据，退回 Common（ListMeta）取 resourceVersion。
	var listMeta metav1.Common
	objectMeta, err := meta.Accessor(obj)
	if err != nil {
		listMeta, err = meta.CommonAccessor(obj)
		if err != nil {
			return nil, err
		}
	} else {
		listMeta = objectMeta
	}

	gvk := obj.GetObjectKind().GroupVersionKind()

	// 如果 object meta 不包含 kind 和/或 version 的信息，
	// 就退回使用 scheme。
	//
	// TODO: 这对未注册到 scheme 的 CRD 不起作用。
	//
	// 反序列化后的对象通常已带 GVK；而手工构造的对象往往没有，
	// 此时只能借助 scheme 反查。
	if gvk.Empty() {
		if scheme == nil {
			return nil, errors.New("scheme is required to look up gvk")
		}
		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil {
			return nil, err
		}
		if len(gvks) == 0 || gvks[0].Empty() {
			return nil, fmt.Errorf("unexpected gvks registered for object %T: %v", obj, gvks)
		}
		// TODO: 同一对象可能注册在多个 group version 下
		// （尽管实践中似乎没有这样用）。这种情况下，
		// 所设置的 version 可能不正确。
		gvk = gvks[0]
	}

	kind := gvk.Kind
	version := gvk.GroupVersion().String()

	// 只有列表元数据。
	// 列表对象没有 name/namespace/uid，只能返回只带 resourceVersion 的引用。
	if objectMeta == nil {
		return &v1.ObjectReference{
			Kind:            kind,
			APIVersion:      version,
			ResourceVersion: listMeta.GetResourceVersion(),
		}, nil
	}

	return &v1.ObjectReference{
		Kind:            kind,
		APIVersion:      version,
		Name:            objectMeta.GetName(),
		Namespace:       objectMeta.GetNamespace(),
		UID:             objectMeta.GetUID(),
		ResourceVersion: objectMeta.GetResourceVersion(),
	}, nil
}

// GetPartialReference 与 GetReference 完全相同，只是允许设置 FieldPath。
//
// FieldPath 用于指向对象内部的具体字段（如 spec.containers{foo}），
// 事件记录等场景借此精确标注出问题的子字段。
func GetPartialReference(scheme *runtime.Scheme, obj runtime.Object, fieldPath string) (*v1.ObjectReference, error) {
	ref, err := GetReference(scheme, obj)
	if err != nil {
		return nil, err
	}
	ref.FieldPath = fieldPath
	return ref, nil
}
