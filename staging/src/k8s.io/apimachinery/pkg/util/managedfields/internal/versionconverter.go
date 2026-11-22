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

// versionConverter is an implementation of
// sigs.k8s.io/structured-merge-diff/merge.Converter
//
// 中文说明：merge 引擎只认 typed value，而版本转换只能通过运行时对象做，
// 因此这里“typed -> 对象 -> hub 版本 -> 目标版本 -> typed”往返一次。
// hubGetter 区分内置类型（同 group 的 hub 版本）与 CRD（固定 hub）。
type versionConverter struct {
	typeConverter   TypeConverter
	objectConvertor runtime.ObjectConvertor
	hubGetter       func(from schema.GroupVersion) schema.GroupVersion
}

var _ merge.Converter = &versionConverter{}

// NewVersionConverter builds a VersionConverter from a TypeConverter and an ObjectConvertor.
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

// NewCRDVersionConverter builds a VersionConverter for CRDs from a TypeConverter and an ObjectConvertor.
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

// Convert implements sigs.k8s.io/structured-merge-diff/merge.Converter
func (v *versionConverter) Convert(object *typed.TypedValue, version fieldpath.APIVersion) (*typed.TypedValue, error) {
	groupVersion, err := schema.ParseGroupVersion(string(version))
	if err != nil {
		return object, err
	}

	// If attempting to convert to the same version as we already have, just return it.
	if typedVersion, ok := GroupVersionOfTypedValue(object); ok && typedVersion == groupVersion {
		return object, nil
	}

	// Convert the smd typed value to a kubernetes object.
	objectToConvert, err := v.typeConverter.TypedToObject(object)
	if err != nil {
		return object, err
	}
	fromVersion := objectToConvert.GetObjectKind().GroupVersionKind().GroupVersion()

	// Convert to internal
	// 中文说明：先转到 hub 版本再转到目标版本，避免任意两版本之间都注册
	// 转换函数（平方级组合）。
	internalObject, err := v.objectConvertor.ConvertToVersion(objectToConvert, v.hubGetter(fromVersion))
	if err != nil {
		return object, err
	}

	// Convert the object into the target version
	convertedObject, err := v.objectConvertor.ConvertToVersion(internalObject, groupVersion)
	if err != nil {
		return object, err
	}

	// Convert the object back to a smd typed value and return it.
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
