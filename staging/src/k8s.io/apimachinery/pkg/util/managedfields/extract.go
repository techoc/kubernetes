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

package managedfields

import (
	"fmt"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/typed"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// ExtractInto extracts the applied configuration state from object for fieldManager
// into applyConfiguration. If no managed fields are found for the given fieldManager,
// no error is returned, but applyConfiguration is left unpopulated. It is possible
// that no managed fields were found for the fieldManager because other field managers
// have taken ownership of all the fields previously owned by the fieldManager. It is
// also possible the fieldManager never owned fields.
//
// The provided object MUST bo a root resource object since subresource objects
// do not contain their own managed fields. For example, an autoscaling.Scale
// object read from a "scale" subresource does not have any managed fields and so
// cannot be used as the object.
//
// If the fields of a subresource are a subset of the fields of the root object,
// and their field paths and types are exactly the same, then ExtractInto can be
// called with the root resource as the object and the subresource as the
// applyConfiguration. This works for "status", obviously, because status is
// represented by the exact same object as the root resource. This does NOT
// work, for example, with the "scale" subresources of Deployment, ReplicaSet and
// StatefulSet. While the spec.replicas, status.replicas fields are in the same
// exact field path locations as they are in autoscaling.Scale, the selector
// fields are in different locations, and are a different type.
//
// 实现路径：取该 manager 的 FieldsV1 叶子集合 -> 从对象中抽取对应子树 ->
// 转回 unstructured -> 填充到调用方的 apply configuration。找不到条目时
// 返回 nil 而非错误，这是"可能从未拥有字段"的合法情况。
func ExtractInto(object runtime.Object, objectType typed.ParseableType, fieldManager string, applyConfiguration interface{}, subresource string) error {
	typedObj, err := toTyped(object, objectType)
	if err != nil {
		return fmt.Errorf("error converting obj to typed: %w", err)
	}

	accessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("error accessing metadata: %w", err)
	}
	fieldsEntry, ok := findManagedFields(accessor, fieldManager, subresource)
	if !ok {
		return nil
	}
	fieldset := &fieldpath.Set{}
	// FromJSON 接受 Reader，避免把 FieldsV1 的原始字节复制成字符串。
	err = fieldset.FromJSON(fieldsEntry.FieldsV1.GetRawReader())
	if err != nil {
		return fmt.Errorf("error marshalling FieldsV1 to JSON: %w", err)
	}

	// 只抽取叶子字段即可重建完整子树，非叶子节点由 Unstructured() 自动补齐。
	u := typedObj.ExtractItems(fieldset.Leaves()).AsValue().Unstructured()
	m, ok := u.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unable to convert managed fields for %s to unstructured, expected map, got %T", fieldManager, u)
	}

	// set the type meta manually if it doesn't exist to avoid missing kind errors
	// when decoding from unstructured JSON
	//
	// managed fields 不记录 kind/apiVersion，但 FromUnstructured 需要它们才能
	// 选择正确的解码路径，因此从源对象补上。
	if _, ok := m["kind"]; !ok && object.GetObjectKind().GroupVersionKind().Kind != "" {
		m["kind"] = object.GetObjectKind().GroupVersionKind().Kind
		m["apiVersion"] = object.GetObjectKind().GroupVersionKind().GroupVersion().String()
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, applyConfiguration); err != nil {
		return fmt.Errorf("error extracting into obj from unstructured: %w", err)
	}
	return nil
}

// findManagedFields 返回匹配 manager、操作类型为 Apply 且子资源一致的条目。
// 只认 Apply 操作：ExtractInto 的语义是还原 apply 配置，Update 条目无法表达。
func findManagedFields(accessor metav1.Object, fieldManager string, subresource string) (metav1.ManagedFieldsEntry, bool) {
	objManagedFields := accessor.GetManagedFields()
	for _, mf := range objManagedFields {
		if mf.Manager == fieldManager && mf.Operation == metav1.ManagedFieldsOperationApply && mf.Subresource == subresource {
			return mf, true
		}
	}
	return metav1.ManagedFieldsEntry{}, false
}

// toTyped 根据对象是否为 Unstructured 选择解析入口：
// unstructured 走 FromUnstructured，其余走反射式的 FromStructured。
func toTyped(obj runtime.Object, objectType typed.ParseableType) (*typed.TypedValue, error) {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return objectType.FromUnstructured(o.Object)
	default:
		return objectType.FromStructured(o)
	}
}
