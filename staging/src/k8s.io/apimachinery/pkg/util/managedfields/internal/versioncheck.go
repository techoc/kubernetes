/*
Copyright 2023 The Kubernetes Authors.

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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// versionCheckManager 包装 FieldManager，确保 apply 的对象版本与预期 GVK 一致。
type versionCheckManager struct {
	fieldManager Manager
	gvk          schema.GroupVersionKind
}

var _ Manager = &versionCheckManager{}

// NewVersionCheckManager 创建用于校验 apply 对象版本的管理器。
func NewVersionCheckManager(fieldManager Manager, gvk schema.GroupVersionKind) Manager {
	return &versionCheckManager{fieldManager: fieldManager, gvk: gvk}
}

// Update 实现 Manager：更新路径的版本一致性已在其他环节校验，这里直接透传。
func (f *versionCheckManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	//update 无需处理，其他位置已检查版本。
	return f.fieldManager.Update(liveObj, newObj, managed, manager)
}

// Apply 实现 Manager：apply 的对象 GVK 必须与预期一致，否则返回 BadRequest，
// 防止把错误版本的对象应用到资源上。
func (f *versionCheckManager) Apply(liveObj, appliedObj runtime.Object, managed Managed, fieldManager string, force bool) (runtime.Object, Managed, error) {
	if gvk := appliedObj.GetObjectKind().GroupVersionKind(); gvk != f.gvk {
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid object type: %v", gvk))
	}
	return f.fieldManager.Apply(liveObj, appliedObj, managed, fieldManager, force)
}
