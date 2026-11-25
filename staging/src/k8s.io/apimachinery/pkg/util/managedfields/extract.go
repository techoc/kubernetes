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

// ExtractInto 从 object 中提取 fieldManager 的应用配置状态并写入 applyConfiguration。
// 如果找不到该 fieldManager 的托管字段，
// 函数不返回错误，但 applyConfiguration 保持未填充。这可能是因为其他字段管理器
// 接管了该 fieldManager 原先拥有的所有字段，
// 也可能是因为该 fieldManager 从未拥有字段。
//
// object 必须是根资源对象，因为子资源对象不包含自身的托管字段。
// 例如，从 "scale" 子资源读取的 autoscaling.Scale
// 没有托管字段，因此不能作为 object。
// 如果子资源的字段是根资源字段的子集，
// 且字段路径和类型完全相同，则可以将根资源作为 object、子资源作为 applyConfiguration
// 调用 ExtractInto。"status" 显然满足条件，因为它与根资源使用同一对象表示；
// 但 Deployment、ReplicaSet 和 StatefulSet 的 "scale" 子资源不满足条件。
// 虽然 spec.replicas 和 status.replicas 与 autoscaling.Scale 中的字段路径相同，
// 但 selector 字段的位置和类型不同。
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

	//如果类型元数据不存在则手动设置，避免从非结构化 JSON 解码时因缺少 kind 而报错。
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
