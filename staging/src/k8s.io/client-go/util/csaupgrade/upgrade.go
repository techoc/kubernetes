/*
Copyright 2022 The Kubernetes Authors.

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

// Package csaupgrade 帮助把对象上由客户端 apply（client-side apply, CSA）
// 记录的 managedFields 迁移到服务端 apply（server-side apply, SSA）的 field manager
// 名下，使后续 Apply 能接管原有字段而不产生冲突。
package csaupgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// 查找拥有给定字段集合中全部字段、且操作类型匹配的 managedFields 所有者。
// 任一字段集解码失败时忽略该条目，视为不匹配。
//
// “owns all of fields”通过差集是否为空判断：fields - entry 为空即包含。
func FindFieldsOwners(
	managedFields []metav1.ManagedFieldsEntry,
	operation metav1.ManagedFieldsOperationType,
	fields *fieldpath.Set,
) []metav1.ManagedFieldsEntry {
	var result []metav1.ManagedFieldsEntry
	for _, entry := range managedFields {
		if entry.Operation != operation {
			continue
		}

		fieldSet, err := decodeManagedFieldsEntrySet(entry)
		if err != nil {
			// 解码失败通常意味着条目来自不兼容版本或已损坏；跳过而非中断整个查询。
			continue
		}

		if fields.Difference(&fieldSet).Empty() {
			result = append(result, entry)
		}
	}
	return result
}

// 把 CSA 管理的字段迁移到 SSA manager 名下：原先由 `csaManager` 通过 Update 拥有的字段，
// 此后由 `ssaManager` 通过 Apply 管理。适用于曾用 CSA 管理、准备改用 SSA 的对象。
//
// 注意：
// 1) 操作不可逆，原有字段所有权信息会丢失；
// 2) 可在首次 SSA 之前或之后执行；
// 3) CSA 往往拥有更多字段（含默认值字段），迁移可能暂时去掉这些默认值，之后会被重新默认化；
// 4) 发送 patch 前要确认服务端 managedFields 未变化，避免覆盖。
//
// obj 是曾被 CSA 管理的目标对象；csaManagerNames 是要合并的 FieldManager 名称；
// ssaManagerName 是后续 Apply 使用的 FieldManager 名称。
//
// 就地修改 obj 的 managedFields；如需并发安全的写回，请使用 UpgradeManagedFieldsPatch。
func UpgradeManagedFields(
	obj runtime.Object,
	csaManagerNames sets.Set[string],
	ssaManagerName string,
	opts ...Option,
) error {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	filteredManagers := accessor.GetManagedFields()

	// 逐个 CSA manager 处理；每一步都基于上一步结果，便于多个旧 manager 合并。
	for csaManagerName := range csaManagerNames {
		filteredManagers, err = upgradedManagedFields(
			filteredManagers, csaManagerName, ssaManagerName, o)

		if err != nil {
			return err
		}
	}

	// 把改动写回对象。
	accessor.SetManagedFields(filteredManagers)
	return nil
}

// 计算迁移 managedFields 所需的最小 JSON Patch，参数含义见 UpgradeManagedFields。
// 出错时返回非 nil 错误；无需改动时返回 nil 字节。
func UpgradeManagedFieldsPatch(
	obj runtime.Object,
	csaManagerNames sets.Set[string],
	ssaManagerName string,
	opts ...Option,
) ([]byte, error) {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	managedFields := accessor.GetManagedFields()
	filteredManagers := accessor.GetManagedFields()
	for csaManagerName := range csaManagerNames {
		filteredManagers, err = upgradedManagedFields(
			filteredManagers, csaManagerName, ssaManagerName, o)
		if err != nil {
			return nil, err
		}
	}

	if reflect.DeepEqual(managedFields, filteredManagers) {
		// managedFields 与转换后一致时无需生成 patch。
		return nil, nil
	}

	// 创建一份基于 old 与 new 对象差异的 patch。
	// 只放入全部 managedFields，因为那是唯一会变的部分。
	//
	// 同时带上对 RV 的判断，以避免竞态：
	// 用资源版本做条件写，若服务端在读取后已被他人修改，patch 会失败而不是覆盖。
	jsonPatch := []map[string]interface{}{
		{
			"op":    "replace",
			"path":  "/metadata/managedFields",
			"value": filteredManagers,
		},
		{
			// 用 "replace" 而非 "test"，让 etcd 以 409 冲突拒绝，而不是 apiserver 报非法请求。
			"op":    "replace",
			"path":  "/metadata/resourceVersion",
			"value": accessor.GetResourceVersion(),
		},
	}

	return json.Marshal(jsonPatch)
}

// 返回从 CSA 迁移到 SSA 后的 managedFields 副本，出错时返回错误。
func upgradedManagedFields(
	managedFields []metav1.ManagedFieldsEntry,
	csaManagerName string,
	ssaManagerName string,
	opts options,
) ([]metav1.ManagedFieldsEntry, error) {
	if managedFields == nil {
		return nil, nil
	}

	// 克隆 managedFields，因为后续会修改其中的值。
	managedFieldsCopy := make([]metav1.ManagedFieldsEntry, len(managedFields))
	if copy(managedFieldsCopy, managedFields) != len(managedFields) {
		return nil, errors.New("failed to copy managed fields")
	}
	managedFields = managedFieldsCopy

	// 查找 SSA manager。
	replaceIndex, managerExists := findFirstIndex(managedFields,
		func(entry metav1.ManagedFieldsEntry) bool {
			return entry.Manager == ssaManagerName &&
				entry.Operation == metav1.ManagedFieldsOperationApply &&
				entry.Subresource == opts.subresource
		})

	if !managerExists {
		// SSA manager 不存在时，找最近的 CSA manager 转换为 SSA。managedFields 按最近者在前排序，因此取第一个匹配项。
		replaceIndex, managerExists = findFirstIndex(managedFields,
			func(entry metav1.ManagedFieldsEntry) bool {
				return entry.Manager == csaManagerName &&
					entry.Operation == metav1.ManagedFieldsOperationUpdate &&
					entry.Subresource == opts.subresource
			})

		if !managerExists {
			// 没有需要转换的 CSA manager，直接返回。
			return managedFields, nil
		}

		// 把 CSA manager 转换成 SSA manager：
		// 直接改写最新 CSA 条目的类型与名字，避免新增一条导致所有权分裂。
		managedFields[replaceIndex].Operation = metav1.ManagedFieldsOperationApply
		managedFields[replaceIndex].Manager = ssaManagerName
	}
	err := unionManagerIntoIndex(managedFields, replaceIndex, csaManagerName, opts)
	if err != nil {
		return nil, err
	}

	// 生成一份不含同名 CSA manager 的 managedFields：
	// 其余 Update 条目是旧版本残留，迁移后统一删除，防止同一字段被多个 manager 声明。
	filteredManagers := filter(managedFields, func(entry metav1.ManagedFieldsEntry) bool {
		return !(entry.Manager == csaManagerName &&
			entry.Operation == metav1.ManagedFieldsOperationUpdate &&
			entry.Subresource == opts.subresource)
	})

	return filteredManagers, nil
}

// 查找与目标 manager APIVersion 相同、名为 `csaManagerName` 的 Update manager，把两者的字段合并到
// targetIndex 指定的 manager 中，不修改其他 manager。
func unionManagerIntoIndex(
	entries []metav1.ManagedFieldsEntry,
	targetIndex int,
	csaManagerName string,
	opts options,
) error {
	ssaManager := entries[targetIndex]

	// 找出 APIVersion 相同的 Update manager，把 SSA 字段与它合并；
	// 丢弃其余同名的 Update manager。
	// 只在同一 APIVersion 内合并：不同版本的字段路径语义可能不同。
	csaManagerIndex, csaManagerExists := findFirstIndex(entries,
		func(entry metav1.ManagedFieldsEntry) bool {
			return entry.Manager == csaManagerName &&
				entry.Operation == metav1.ManagedFieldsOperationUpdate &&
				entry.Subresource == opts.subresource &&
				entry.APIVersion == ssaManager.APIVersion
		})

	targetFieldSet, err := decodeManagedFieldsEntrySet(ssaManager)
	if err != nil {
		return fmt.Errorf("failed to convert fields to set: %w", err)
	}

	combinedFieldSet := &targetFieldSet

	// 把 CSA manager 的字段并入现有 SSA manager；找不到合适对象时什么都不做。
	if csaManagerExists {
		csaManager := entries[csaManagerIndex]

		csaFieldSet, err := decodeManagedFieldsEntrySet(csaManager)
		if err != nil {
			return fmt.Errorf("failed to convert fields to set: %w", err)
		}

		combinedFieldSet = combinedFieldSet.Union(&csaFieldSet)
	}

	// 把字段集编码回序列化格式。
	err = encodeManagedFieldsEntrySet(&entries[targetIndex], *combinedFieldSet)
	if err != nil {
		return fmt.Errorf("failed to encode field set: %w", err)
	}

	return nil
}

func findFirstIndex[T any](
	collection []T,
	predicate func(T) bool,
) (int, bool) {
	// 返回第一个匹配项；managedFields 已按“最近者在前”排序，因此即最新条目。
	for idx, entry := range collection {
		if predicate(entry) {
			return idx, true
		}
	}

	return -1, false
}

func filter[T any](
	collection []T,
	predicate func(T) bool,
) []T {
	// 结果为空时返回 nil 而非空切片，以便与“字段未设置”在 reflect.DeepEqual 中一致。
	result := make([]T, 0, len(collection))

	for _, value := range collection {
		if predicate(value) {
			result = append(result, value)
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// 从 fieldmanager.internal 复制而来，以避免循环依赖。
// FieldsToSet 从字段 trie 构造路径集合。
func decodeManagedFieldsEntrySet(f metav1.ManagedFieldsEntry) (s fieldpath.Set, err error) {
	err = s.FromJSON(f.FieldsV1.GetRawReader())
	return s, err
}

// SetToFields 从路径集合构造字段 trie。
func encodeManagedFieldsEntrySet(f *metav1.ManagedFieldsEntry, s fieldpath.Set) (err error) {
	raw, err := s.ToJSON()
	f.FieldsV1.SetRawBytes(raw)
	return err
}
