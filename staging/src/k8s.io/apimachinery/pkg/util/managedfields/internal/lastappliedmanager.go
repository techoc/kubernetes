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

package internal

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
)

// lastAppliedManager 包装一个真正的字段管理器，并在 Apply 时额外处理
// client-side apply 遗留的 last-applied 注解，实现 CSA 到 SSA 的无冲突升级。
type lastAppliedManager struct {
	fieldManager    Manager
	typeConverter   TypeConverter
	objectConverter runtime.ObjectConvertor
	groupVersion    schema.GroupVersion
}

var _ Manager = &lastAppliedManager{}

// NewLastAppliedManager 将客户端应用注解转换为服务端应用托管字段。
func NewLastAppliedManager(fieldManager Manager, typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, groupVersion schema.GroupVersion) Manager {
	// groupVersion 用于把 live 对象转换到注解中记录的版本，二者必须一致才能
	// 做字段级比较。
	return &lastAppliedManager{
		fieldManager:    fieldManager,
		typeConverter:   typeConverter,
		objectConverter: objectConverter,
		groupVersion:    groupVersion,
	}
}

// Update 实现 Manager 接口。
func (f *lastAppliedManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	return f.fieldManager.Update(liveObj, newObj, managed, manager)
}

// Apply 会考虑 last-applied 注解，
// 将客户端应用管理的对象无冲突升级到服务端应用。
//
// 策略：先正常 apply；只有在出现冲突、且冲突字段都包含在 last-applied
// 注解中时才以 force 重试，从而把“此前由 CSA 管理的字段”视为本管理器所有。
func (f *lastAppliedManager) Apply(liveObj, newObj runtime.Object, managed Managed, manager string, force bool) (runtime.Object, Managed, error) {
	newLiveObj, newManaged, newErr := f.fieldManager.Apply(liveObj, newObj, managed, manager, force)
	//仅在使用 kubectl server-side-apply 时升级客户端应用注解。
	//用户可指定其他字段管理器以退出此行为。
	if manager != "kubectl" {
		return newLiveObj, newManaged, newErr
	}

	//检查是否存在冲突
	if newErr == nil {
		return newLiveObj, newManaged, newErr
	}
	// 只有字段冲突才尝试自动升级；其他错误原样返回。
	conflicts, ok := newErr.(merge.Conflicts)
	if !ok {
		return newLiveObj, newManaged, newErr
	}
	conflictSet := conflictsToSet(conflicts)

	//检查客户端应用是否允许冲突，
	//允许则强制执行 apply。
	allowedConflictSet, err := f.allowedConflictsFromLastApplied(liveObj)
	if err != nil {
		return newLiveObj, newManaged, newErr
	}
	// 存在任何不被 last-applied 覆盖的冲突时，不能 force，只返回剩余冲突。
	if !conflictSet.Difference(allowedConflictSet).Empty() {
		newConflicts := conflictsDifference(conflicts, allowedConflictSet)
		return newLiveObj, newManaged, newConflicts
	}

	return f.fieldManager.Apply(liveObj, newObj, managed, manager, true)
}

// allowedConflictsFromLastApplied 计算“可以无冲突接管”的字段集合：取
// last-applied 字段集，再减去在 live 对象中被修改、新增或删除的部分，
// 因为那些字段的实际归属已经无法从注解推断。
func (f *lastAppliedManager) allowedConflictsFromLastApplied(liveObj runtime.Object) (*fieldpath.Set, error) {
	var accessor, err = meta.Accessor(liveObj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}

	//没有客户端应用注解时无需处理。
	var annotations = accessor.GetAnnotations()
	if annotations == nil {
		return nil, fmt.Errorf("no last applied annotation")
	}
	var lastApplied, ok = annotations[LastAppliedConfigAnnotation]
	if !ok || lastApplied == "" {
		return nil, fmt.Errorf("no last applied annotation")
	}

	liveObjVersioned, err := f.objectConverter.ConvertToVersion(liveObj, f.groupVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to convert live obj to versioned: %v", err)
	}

	liveObjTyped, err := f.typeConverter.ObjectToTyped(liveObjVersioned)
	if err != nil {
		return nil, fmt.Errorf("failed to convert live obj to typed: %v", err)
	}

	var lastAppliedObj = &unstructured.Unstructured{Object: map[string]interface{}{}}
	err = json.Unmarshal([]byte(lastApplied), lastAppliedObj)
	if err != nil {
		return nil, fmt.Errorf("failed to decode last applied obj: %v in '%s'", err, lastApplied)
	}

	// 注解与 live 对象的 apiVersion 必须一致，否则字段路径无法对齐。
	if lastAppliedObj.GetAPIVersion() != f.groupVersion.String() {
		return nil, fmt.Errorf("expected version of last applied to match live object '%s', but got '%s': %v", f.groupVersion.String(), lastAppliedObj.GetAPIVersion(), err)
	}

	lastAppliedObjTyped, err := f.typeConverter.ObjectToTyped(lastAppliedObj)
	if err != nil {
		return nil, fmt.Errorf("failed to convert last applied to typed: %v", err)
	}

	lastAppliedObjFieldSet, err := lastAppliedObjTyped.ToFieldSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create fieldset for last applied object: %v", err)
	}

	// Compare 返回三类差异，用于剔除不可信的字段路径。
	comparison, err := lastAppliedObjTyped.Compare(liveObjTyped)
	if err != nil {
		return nil, fmt.Errorf("failed to compare last applied object and live object: %v", err)
	}

	//移除 last-applied 中与线上对象不同、额外添加或缺失的字段；
	//因为这些字段与线上对象不匹配，
	//所以我们不拥有它们。
	lastAppliedObjFieldSet = lastAppliedObjFieldSet.
		Difference(comparison.Modified).
		Difference(comparison.Added).
		Difference(comparison.Removed)

	return lastAppliedObjFieldSet, nil
}

// TODO: 替换为 merge.Conflicts.ToSet()。
//
// 把冲突列表转换为字段路径集合，便于做集合差运算。
func conflictsToSet(conflicts merge.Conflicts) *fieldpath.Set {
	conflictSet := fieldpath.NewSet()
	for _, conflict := range []merge.Conflict(conflicts) {
		conflictSet.Insert(conflict.Path)
	}
	return conflictSet
}

// conflictsDifference 过滤掉位于 s 中的冲突路径，保留真正需要报告给用户的
// 冲突，避免把已由 last-applied 覆盖的字段重复报错。
func conflictsDifference(conflicts merge.Conflicts, s *fieldpath.Set) merge.Conflicts {
	newConflicts := []merge.Conflict{}
	for _, conflict := range []merge.Conflict(conflicts) {
		if !s.Has(conflict.Path) {
			newConflicts = append(newConflicts, conflict)
		}
	}
	return newConflicts
}
