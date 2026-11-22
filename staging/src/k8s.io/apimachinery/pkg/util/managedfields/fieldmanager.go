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

package managedfields

import (
	"fmt"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
)

// FieldManager 负责更新 managedFields 并合并 apply 配置。
// 这里用类型别名直接暴露 internal.FieldManager，使调用方无需依赖 internal 包，
// 同时两者保持完全相同的类型与方法集。
type FieldManager = internal.FieldManager

// NewDefaultFieldManager 创建一个默认 FieldManager：对 apply 请求执行合并，
// 对其他类型的请求维护 managedFields。
// hub 是转换时使用的中心版本；resetFields 指定哪些字段在版本转换后应被重置。
func NewDefaultFieldManager(typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, objectDefaulter runtime.ObjectDefaulter, objectCreater runtime.ObjectCreater, kind schema.GroupVersionKind, hub schema.GroupVersion, subresource string, resetFields map[fieldpath.APIVersion]fieldpath.Filter) (*FieldManager, error) {
	// 基于结构化合并（structured-merge-diff）创建内层 manager，
	// 再套上默认的版本检查、last-applied 维护等包装。
	f, err := internal.NewStructuredMergeManager(typeConverter, objectConverter, objectDefaulter, kind.GroupVersion(), hub, resetFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create field manager: %v", err)
	}
	return internal.NewDefaultFieldManager(f, typeConverter, objectConverter, objectCreater, kind, subresource), nil
}

// NewDefaultCRDFieldManager 创建专用于 CRD 的 FieldManager。
// 与内置类型不同，CRD 允许存在模型中未定义的字段，甚至完全没有模型，
// 因此使用 CRD 专用的结构化合并 manager。
func NewDefaultCRDFieldManager(typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, objectDefaulter runtime.ObjectDefaulter, objectCreater runtime.ObjectCreater, kind schema.GroupVersionKind, hub schema.GroupVersion, subresource string, resetFields map[fieldpath.APIVersion]fieldpath.Filter) (_ *FieldManager, err error) {
	f, err := internal.NewCRDStructuredMergeManager(typeConverter, objectConverter, objectDefaulter, kind.GroupVersion(), hub, resetFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create field manager: %v", err)
	}
	return internal.NewDefaultFieldManager(f, typeConverter, objectConverter, objectCreater, kind, subresource), nil
}

// ValidateManagedFields 校验线格式的 managedFields 是否能被成功解码。
// 它只做解码验证，不修改输入。
func ValidateManagedFields(encodedManagedFields []metav1.ManagedFieldsEntry) error {
	_, err := internal.DecodeManagedFields(encodedManagedFields)
	return err
}
