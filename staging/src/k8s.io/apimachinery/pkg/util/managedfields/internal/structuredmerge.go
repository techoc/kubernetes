/*
Copyright 2019 The Kubernetes Authors.

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

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
	"sigs.k8s.io/structured-merge-diff/v6/typed"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// structuredMergeManager 把对象转换为 SMD（structured-merge-diff）的 TypedValue，
// 调用 merge.Updater 计算新的字段所有权，再转换回对象。
type structuredMergeManager struct {
	typeConverter   TypeConverter
	objectConverter runtime.ObjectConvertor
	objectDefaulter runtime.ObjectDefaulter
	groupVersion    schema.GroupVersion
	hubVersion      schema.GroupVersion
	updater         merge.Updater
}

var _ Manager = &structuredMergeManager{}

// NewStructuredMergeManager 创建 Manager，合并 apply 请求并为其他请求类型更新托管字段。
func NewStructuredMergeManager(typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, objectDefaulter runtime.ObjectDefaulter, gv schema.GroupVersion, hub schema.GroupVersion, resetFields map[fieldpath.APIVersion]fieldpath.Filter) (Manager, error) {
	if typeConverter == nil {
		return nil, fmt.Errorf("typeconverter must not be nil")
	}
	return &structuredMergeManager{
		typeConverter:   typeConverter,
		objectConverter: objectConverter,
		objectDefaulter: objectDefaulter,
		groupVersion:    gv,
		hubVersion:      hub,
		updater: merge.Updater{
			// 转换器负责在 hub 版本与目标版本之间转换字段路径。
			Converter:    newVersionConverter(typeConverter, objectConverter, hub), //这是 k8s 提供给 SMD 的转换器。
			IgnoreFilter: resetFields,
		},
	}, nil
}

// NewCRDStructuredMergeManager 创建专用于 CRD 的 Manager，
// 允许处理模型中未定义的字段，也允许完全没有模型。
func NewCRDStructuredMergeManager(typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, objectDefaulter runtime.ObjectDefaulter, gv schema.GroupVersion, hub schema.GroupVersion, resetFields map[fieldpath.APIVersion]fieldpath.Filter) (_ Manager, err error) {
	return &structuredMergeManager{
		typeConverter:   typeConverter,
		objectConverter: objectConverter,
		objectDefaulter: objectDefaulter,
		groupVersion:    gv,
		hubVersion:      hub,
		updater: merge.Updater{
			// CRD 使用宽松版本转换器：没有 OpenAPI 模型时也要能工作。
			Converter:    newCRDVersionConverter(typeConverter, objectConverter, hub),
			IgnoreFilter: resetFields,
		},
	}, nil
}

// objectGVKNN 返回 "namespace/name; GVK" 形式的对象标识，用于错误信息。
// 取不到元数据时用 <unknown> 占位，保证错误信息仍可读。
func objectGVKNN(obj runtime.Object) string {
	name := "<unknown>"
	namespace := "<unknown>"
	if accessor, err := meta.Accessor(obj); err == nil {
		name = accessor.GetName()
		namespace = accessor.GetNamespace()
	}

	return fmt.Sprintf("%v/%v; %v", namespace, name, obj.GetObjectKind().GroupVersionKind())
}

// Update 实现 Manager 接口。
//
// 更新请求不合并补丁，只重新计算 newObj 相对于 liveObj 的字段所有权：
// 把两个对象都转成目标版本的 TypedValue，交给 SMD 的 Update。
func (f *structuredMergeManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	newObjVersioned, err := f.toVersioned(newObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert new object (%v) to proper version (%v): %v", objectGVKNN(newObj), f.groupVersion, err)
	}
	liveObjVersioned, err := f.toVersioned(liveObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert live object (%v) to proper version: %v", objectGVKNN(liveObj), err)
	}
	// Update 路径允许重复字段：kubectl 等客户端可能提交含重复项的对象，
	// 这里选择容忍而不是直接拒绝。
	newObjTyped, err := f.typeConverter.ObjectToTyped(newObjVersioned, typed.AllowDuplicates)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert new object (%v) to smd typed: %v", objectGVKNN(newObjVersioned), err)
	}
	liveObjTyped, err := f.typeConverter.ObjectToTyped(liveObjVersioned, typed.AllowDuplicates)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert live object (%v) to smd typed: %v", objectGVKNN(liveObjVersioned), err)
	}
	apiVersion := fieldpath.APIVersion(f.groupVersion.String())

	//TODO(apelisse): 实现 union 后使用第一个返回值。
	_, managedFields, err := f.updater.Update(liveObjTyped, newObjTyped, apiVersion, managed.Fields(), manager)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update ManagedFields (%v): %v", objectGVKNN(newObjVersioned), err)
	}
	// 时间戳沿用旧的：Update 不改变字段所有者的 apply 时间。
	managed = NewManaged(managedFields, managed.Times())

	return newObj, managed, nil
}

// Apply 实现 Manager 接口。
//
// Apply 与服务端 apply 语义一致：以 patchObj 为唯一所有者计算冲突，
// 再把结果转回对象。force=true 时覆盖冲突。
func (f *structuredMergeManager) Apply(liveObj, patchObj runtime.Object, managed Managed, manager string, force bool) (runtime.Object, Managed, error) {
	//检查补丁对象与线上对象版本相同。
	// 版本必须一致，否则字段路径无法直接比较，容易产生错误的冲突判断。
	if patchVersion := patchObj.GetObjectKind().GroupVersionKind().GroupVersion(); patchVersion != f.groupVersion {
		return nil, nil,
			errors.NewBadRequest(
				fmt.Sprintf("Incorrect version specified in apply patch. "+
					"Specified patch version: %s, expected: %s",
					patchVersion, f.groupVersion))
	}

	patchObjMeta, err := meta.Accessor(patchObj)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get accessor: %v", err)
	}
	// 客户端不得自带 managedFields，否则可以伪造字段所有权。
	if patchObjMeta.GetManagedFields() != nil {
		return nil, nil, errors.NewBadRequest("metadata.managedFields must be nil")
	}

	liveObjVersioned, err := f.toVersioned(liveObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert live object (%v) to proper version: %v", objectGVKNN(liveObj), err)
	}

	//不允许应用对象中出现重复项。
	// 与 Update 不同，apply 的 patch 必须是规范的，重复字段直接报错。
	patchObjTyped, err := f.typeConverter.ObjectToTyped(patchObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create typed patch object (%v): %v", objectGVKNN(patchObj), err)
	}

	liveObjTyped, err := f.typeConverter.ObjectToTyped(liveObjVersioned, typed.AllowDuplicates)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create typed live object (%v): %v", objectGVKNN(liveObjVersioned), err)
	}

	apiVersion := fieldpath.APIVersion(f.groupVersion.String())
	newObjTyped, managedFields, err := f.updater.Apply(liveObjTyped, patchObjTyped, apiVersion, managed.Fields(), manager, force)
	if err != nil {
		return nil, nil, err
	}
	managed = NewManaged(managedFields, managed.Times())

	if newObjTyped == nil {
		// nil 表示 apply 后对象没有变化；managed 仍可能更新。
		return nil, managed, nil
	}

	newObj, err := f.typeConverter.TypedToObject(newObjTyped)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert new typed object (%v) to object: %v", objectGVKNN(patchObj), err)
	}

	// 依次转换到目标版本、执行默认化、再转回 hub 版本：
	// 默认化必须在目标版本进行，因为默认值是按版本定义的。
	newObjVersioned, err := f.toVersioned(newObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert new object (%v) to proper version: %v", objectGVKNN(patchObj), err)
	}
	f.objectDefaulter.Default(newObjVersioned)

	newObjUnversioned, err := f.toUnversioned(newObjVersioned)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert to unversioned (%v): %v", objectGVKNN(patchObj), err)
	}
	return newObjUnversioned, managed, nil
}

// toVersioned 把对象转换到该 Manager 服务的 groupVersion。
func (f *structuredMergeManager) toVersioned(obj runtime.Object) (runtime.Object, error) {
	return f.objectConverter.ConvertToVersion(obj, f.groupVersion)
}

// toUnversioned 把对象转换回 hub（内部）版本。
func (f *structuredMergeManager) toUnversioned(obj runtime.Object) (runtime.Object, error) {
	return f.objectConverter.ConvertToVersion(obj, f.hubVersion)
}
