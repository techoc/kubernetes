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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

var (
	// scale 子资源固定使用 autoscaling/v1 的组版本。
	scaleGroupVersion = schema.GroupVersion{Group: "autoscaling", Version: "v1"}
	// replicas 在 scale 子资源中的字段路径。
	replicasPathInScale = fieldpath.MakePathOrDie("spec", "replicas")
)

// ResourcePathMappings 把 group/version 映射到该版本下 replicas 的字段路径。
// 假设所有路径都指向叶子字段（managed fields 只能精确到叶子）。
type ResourcePathMappings map[string]fieldpath.Path

// ScaleHandler 管理主资源与 scale 子资源之间 managed fields 的转换。
type ScaleHandler struct {
	parentEntries []metav1.ManagedFieldsEntry
	groupVersion  schema.GroupVersion
	mappings      ResourcePathMappings
}

// NewScaleHandler 根据主资源的 managed fields、目标组版本与路径映射创建 ScaleHandler。
func NewScaleHandler(parentEntries []metav1.ManagedFieldsEntry, groupVersion schema.GroupVersion, mappings ResourcePathMappings) *ScaleHandler {
	return &ScaleHandler{
		parentEntries: parentEntries,
		groupVersion:  groupVersion,
		mappings:      mappings,
	}
}

// ToSubresource 过滤主资源的 managed fields 并转换，使其可用于 scale 子资源。
// 对包含 replicas 路径的条目做两处修改：
//  1. APIVersion 改为 scale 子资源的 APIVersion
//  2. 主资源的 replicas 路径改写为 scale 子资源中的 replicas 路径
func (h *ScaleHandler) ToSubresource() ([]metav1.ManagedFieldsEntry, error) {
	managed, err := internal.DecodeManagedFields(h.parentEntries)
	if err != nil {
		return nil, err
	}

	f := fieldpath.ManagedFields{}
	t := map[string]*metav1.Time{}
	for manager, versionedSet := range managed.Fields() {
		path, ok := h.mappings[string(versionedSet.APIVersion())]
		//APIVersion 未知时跳过该条目。
		// APIVersion 未知的条目无法定位 replicas，直接跳过。
		if !ok || path == nil {
			continue
		}

		if versionedSet.Set().Has(path) {
			// 该条目拥有 replicas 字段，转换为 scale 子资源下的等价条目。
			newVersionedSet := fieldpath.NewVersionedSet(
				fieldpath.NewSet(replicasPathInScale),
				fieldpath.APIVersion(scaleGroupVersion.String()),
				versionedSet.Applied(),
			)

			f[manager] = newVersionedSet
			t[manager] = managed.Times()[manager]
		}
	}

	return managedFieldsEntries(internal.NewManaged(f, t))
}

// ToParent 把 scaleEntries 与主资源的条目合并并做相应转换。
func (h *ScaleHandler) ToParent(scaleEntries []metav1.ManagedFieldsEntry) ([]metav1.ManagedFieldsEntry, error) {
	decodedParentEntries, err := internal.DecodeManagedFields(h.parentEntries)
	if err != nil {
		return nil, err
	}
	parentFields := decodedParentEntries.Fields()

	decodedScaleEntries, err := internal.DecodeManagedFields(scaleEntries)
	if err != nil {
		return nil, err
	}
	scaleFields := decodedScaleEntries.Fields()

	f := fieldpath.ManagedFields{}
	t := map[string]*metav1.Time{}

	for manager, versionedSet := range parentFields {
		//获取主资源的 "replicas" 路径。
		path, ok := h.mappings[string(versionedSet.APIVersion())]
		//APIVersion 未知时丢弃该条目。
		// APIVersion 未知的条目无法映射路径，直接丢弃。
		if !ok {
			continue
		}

		//如果父条目没有 replicas 路径或该路径为 nil，则保持原样；
		//没有 scale 子资源的自定义资源，其路径为 nil。
		// 主资源条目不含 replicas 路径、或该路径为 nil（无 scale 子资源的 CRD）时，
		// 原样保留该条目。
		if path == nil || !versionedSet.Set().Has(path) {
			f[manager] = versionedSet
			t[manager] = decodedParentEntries.Times()[manager]
			continue
		}

		if _, ok := scaleFields[manager]; !ok {
			//从主资源条目中“取走”replicas 路径。
			// scale 侧没有同名 manager 时，把 replicas 字段从主资源条目中“偷走”，
			// 避免同一字段被重复声明归属。
			newSet := versionedSet.Set().Difference(fieldpath.NewSet(path))

			// 偷走后集合为空则整条丢弃，没有剩余字段可保留。
			if !newSet.Empty() {
				newVersionedSet := fieldpath.NewVersionedSet(
					newSet,
					versionedSet.APIVersion(),
					versionedSet.Applied(),
				)
				f[manager] = newVersionedSet
				t[manager] = decodedParentEntries.Times()[manager]
			}
		} else {
			//字段未被取走，保持条目原样。
			// scale 侧已有同名条目，主资源条目保持原样，并消费掉 scale 条目避免重复处理。
			f[manager] = versionedSet
			t[manager] = decodedParentEntries.Times()[manager]
			delete(scaleFields, manager)
		}
	}

	// 剩余的 scale 条目（未与主资源条目重名的）映射回主资源的 replicas 路径。
	for manager, versionedSet := range scaleFields {
		if !versionedSet.Set().Has(replicasPathInScale) {
			continue
		}
		newVersionedSet := fieldpath.NewVersionedSet(
			fieldpath.NewSet(h.mappings[h.groupVersion.String()]),
			fieldpath.APIVersion(h.groupVersion.String()),
			versionedSet.Applied(),
		)
		f[manager] = newVersionedSet
		t[manager] = decodedParentEntries.Times()[manager]
	}

	return managedFieldsEntries(internal.NewManaged(f, t))
}

// managedFieldsEntries 把内部 Managed 结构重新编码进一个空的 unstructured 对象，
// 再通过访问器取出标准形式的 ManagedFieldsEntry 列表。
func managedFieldsEntries(entries internal.ManagedInterface) ([]metav1.ManagedFieldsEntry, error) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := internal.EncodeObjectManagedFields(obj, entries); err != nil {
		return nil, err
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}
	return accessor.GetManagedFields(), nil
}
