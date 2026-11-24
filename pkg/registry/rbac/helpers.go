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

package rbac

import (
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

// IsOnlyMutatingGCFields 判断更新是否只涉及 Garbage Collector 管理的 OwnerReferences/Finalizers 等元数据，避免清理操作被误判为权限提升。
// 比较前复制新对象并把 GC 管理字段重置为旧值；剩余内容与旧对象相等才表示纯 GC 元数据变更。
func IsOnlyMutatingGCFields(obj, old runtime.Object, equalities conversion.Equalities) bool {
	if old == nil || reflect.ValueOf(old).IsNil() {
		return false
	}

	// 复制新对象后再覆盖 GC 管理字段，避免为了比较而修改调用方传入的对象。
	copied := obj.DeepCopyObject()
	copiedMeta, err := meta.Accessor(copied)
	if err != nil {
		return false
	}
	oldMeta, err := meta.Accessor(old)
	if err != nil {
		return false
	}
	copiedMeta.SetOwnerReferences(oldMeta.GetOwnerReferences())
	copiedMeta.SetFinalizers(oldMeta.GetFinalizers())
	copiedMeta.SetSelfLink(oldMeta.GetSelfLink())
	copiedMeta.SetManagedFields(oldMeta.GetManagedFields())

	return equalities.DeepEqual(copied, old)
}
