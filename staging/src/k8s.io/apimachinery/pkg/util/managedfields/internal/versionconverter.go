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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
)

// versionConverter 是 sigs.k8s.io/structured-merge-diff/merge.Converter 的实现。
//
// merge 引擎只认 typed value，而版本转换只能通过运行时对象做，
// 因此这里“typed -> 对象 -> hub 版本 -> 目标版本 -> typed”往返一次。
// hubGetter 区分内置类型（同 group 的 hub 版本）与 CRD（固定 hub）。
type versionConverter struct {
	typeConverter   TypeConverter
	objectConvertor runtime.ObjectConvertor
	hubGetter       func(from schema.GroupVersion) schema.GroupVersion
}

var _ merge.Converter = &versionConverter{}

// NewVersionConverter 根据 TypeConverter 和 ObjectConvertor 构建 VersionConverter。
func newVersionConverter(t TypeConverter, o runtime.ObjectConvertor, h schema.GroupVersion) merge.Converter {
	return &versionConverter{
		typeConverter:   t,
		objectConvertor: o,
		// 内置类型：保留原 group，只把版本换成 hub 版本。
		hubGetter: func(from schema.GroupVersion) schema.GroupVersion {
			return schema.GroupVersion{
				Group:   from.Group,
				Version: h.Version,
			}
		},
	}
}

// NewCRDVersionConverter 根据 TypeConverter 和 ObjectConvertor 构建用于 CRD 的 VersionConverter。
func newCRDVersionConverter(t TypeConverter, o runtime.ObjectConvertor, h schema.GroupVersion) merge.Converter {
	return &versionConverter{
		typeConverter:   t,
		objectConvertor: o,
		// CRD 的 hub 版本固定为给定值（可能跨 group）。
		hubGetter: func(from schema.GroupVersion) schema.GroupVersion {
			return h
		},
	}
}

// Convert 实现 sigs.k8s.io/structured-merge-diff/merge.Converter。
func (v *versionConverter) Convert(object *typed.TypedValue, version fieldpath.APIVersion) (*typed.TypedValue, error) {
	groupVersion, err := schema.ParseGroupVersion(string(version))
	if err != nil {
		return object, err
	}

	//如果目标版本与当前版本相同，直接返回。
	if typedVersion, ok := GroupVersionOfTypedValue(object); ok && typedVersion == groupVersion {
		return object, nil
	}

	//将 SMD typed value 转换为 Kubernetes 对象。
	objectToConvert, err := v.typeConverter.TypedToObject(object)
	if err != nil {
		return object, err
	}
	fromVersion := objectToConvert.GetObjectKind().GroupVersionKind().GroupVersion()

	//转换为内部版本。
	// 先转到 hub 版本再转到目标版本，避免任意两版本之间都注册
	// 转换函数（平方级组合）。
	internalObject, err := v.objectConvertor.ConvertToVersion(objectToConvert, v.hubGetter(fromVersion))
	if err != nil {
		return object, err
	}

	//将对象转换为目标版本。
	convertedObject, err := v.objectConvertor.ConvertToVersion(internalObject, groupVersion)
	if err != nil {
		return object, err
	}

	//将对象转换回 SMD typed value 并返回。
	return v.typeConverter.ObjectToTyped(convertedObject)
}

// IsMissingVersionError 判断错误是否表示“该版本/类型不可用”，从而让 merge
// 引擎决定是否可以跳过转换而不是直接失败。
func (v *versionConverter) IsMissingVersionError(err error) bool {
	return runtime.IsNotRegisteredError(err) || isNoCorrespondingTypeError(err)
}

// noCorrespondingTypeErr 表示 TypeConverter 的 schema 索引中没有该 GVK。
type noCorrespondingTypeErr struct {
	gvk schema.GroupVersionKind
}

// NewNoCorrespondingTypeError 构造错误，表示 schema 索引中没有给定 GVK 对应的类型。
func NewNoCorrespondingTypeError(gvk schema.GroupVersionKind) error {
	return &noCorrespondingTypeErr{gvk: gvk}
}

func (k *noCorrespondingTypeErr) Error() string {
	return fmt.Sprintf("no corresponding type for %v", k.gvk)
}

// isNoCorrespondingTypeError 用类型断言而非 errors.Is 判断；该错误不支持
// 包装链匹配，与调用点的使用方式一致。
func isNoCorrespondingTypeError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*noCorrespondingTypeErr)
	return ok
}
