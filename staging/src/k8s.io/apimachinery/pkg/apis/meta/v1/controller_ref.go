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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

// IsControlledBy 判断对象的 controllerRef 是否指向给定的 owner。
// 仅比较 UID，因为同一对象可能被重建（name 相同但 UID 不同），UID 才是身份的唯一标识。
func IsControlledBy(obj Object, owner Object) bool {
	ref := GetControllerOfNoCopy(obj)
	if ref == nil {
		return false
	}
	return ref.UID == owner.GetUID()
}

// GetControllerOf 在 controllee 存在 controller 时返回其 controllerRef 的副本。
// 之所以返回副本，是为了防止调用方修改返回值时意外污染对象自身的 ownerReferences。
func GetControllerOf(controllee Object) *OwnerReference {
	ref := GetControllerOfNoCopy(controllee)
	if ref == nil {
		return nil
	}
	// 浅拷贝结构体后，仍需为指针字段分配新地址，否则副本与原对象共享同一指针目标。
	cp := *ref
	cp.Controller = ptr.To(*ref.Controller)
	if ref.BlockOwnerDeletion != nil {
		cp.BlockOwnerDeletion = ptr.To(*ref.BlockOwnerDeletion)
	}
	return &cp
}

// GetControllerOfNoCopy 在 controllee 存在 controller 时返回其 controllerRef 的指针（不复制）。
// 返回的指针指向对象内部的 ownerReferences 切片元素，调用方只应读取；
// 若需要修改，请使用 GetControllerOf 获取副本。
func GetControllerOfNoCopy(controllee Object) *OwnerReference {
	// 用下标遍历以取得切片元素的地址；按值 range 会返回副本，指针将指向错误的地址。
	refs := controllee.GetOwnerReferences()
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

// NewControllerRef 创建一个指向给定 owner 的 OwnerReference。
// 默认将 Controller 与 BlockOwnerDeletion 都置为 true：
// 前者声明该引用是控制者（每个对象最多一个），后者防止 owner 在被依赖者仍存在时被删除。
func NewControllerRef(owner Object, gvk schema.GroupVersionKind) *OwnerReference {
	return &OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
}
