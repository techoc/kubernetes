/*
Copyright 2014 The Kubernetes Authors.

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

// Package strategicpatch 实现 Kubernetes 的 strategic merge patch：
// 在 RFC 7386 JSON merge patch 基础上，利用 Go struct tag 中的
// patchStrategy/patchMergeKey 元数据决定列表元素是合并还是整体替换。
package strategicpatch

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/mergepatch"
)

//这是 JSON Merge Patch（https://tools.ietf.org/html/rfc7386）的替代实现，
//支持通过元数据标注 JSON 列表元素应当合并还是替换。
//
//
//更多信息参见 docs/devel/api-conventions.md 的 PATCH 章节。
//本包部分内容稍作调整后借鉴自
//evanphx/json-patch 和 openshift/origin。

// 这些是 patch 中具有特殊含义的键：$patch 控制整段语义，
// $deleteFromPrimitiveList/<field> 与 $setElementOrder/<field> 是列表的并行指令。
const (
	directiveMarker  = "$patch"
	deleteDirective  = "delete"
	replaceDirective = "replace"
	mergeDirective   = "merge"

	retainKeysStrategy = "retainKeys"

	deleteFromPrimitiveListDirectivePrefix = "$deleteFromPrimitiveList"
	retainKeysDirective                    = "$" + retainKeysStrategy
	setElementOrderDirectivePrefix         = "$setElementOrder"
)

// JSONMap 是编码为 map[string]interface{} 的 JSON 对象，
// 子节点可以是 map[string]interface{}、[]interface{} 或基本类型。
// 直接操作 JSONMap 无需 JSON 编解码，因此速度更快。
type JSONMap map[string]interface{}

// DiffOptions 控制两方/三方 diff 的生成内容。
// 零值表示：不生成 setElementOrder、包含所有变更与删除、不生成 retainKeys。
type DiffOptions struct {
	//SetElementOrder 决定是否生成 $setElementOrder 并行列表。
	SetElementOrder bool
	//IgnoreChangesAndAdditions 决定是否在补丁中保留修改和新增内容。
	IgnoreChangesAndAdditions bool
	//IgnoreDeletions 决定是否在补丁中保留删除内容。
	IgnoreDeletions bool
	//为 patchStrategy 引入 retainKeys 值。
	//所有需要保留的字段必须出现在 `retainKeys` 列表中；
	//列表中的字段与线上对象合并，缺失字段在应用补丁时清除。
	BuildRetainKeysDirective bool
}

// MergeOptions 控制 patch 合并阶段的语义。
type MergeOptions struct {
	//MergeParallelList 决定是否合并并行列表。
	//客户端调用 CreateThreeWayMergePatch() 中的 mergeMap() 时不合并并行列表；
	//服务端调用 StrategicMergeMapPatch() 中的 mergeMap() 时合并并行列表。
	MergeParallelList bool
	//IgnoreUnmatchedNulls 决定是否处理原对象中不匹配的 null。
	IgnoreUnmatchedNulls bool
}

//以下代码改编自 github.com/openshift/origin/pkg/util/jsonmerge。
//与原实现不同，这里不定义保存原对象、补丁和前置条件的 Delta，
//而是由 reconcile 方法直接接受前置条件集合。

// CreateTwoWayMergePatch 根据原始和修改后的 JSON 文档创建可传给 StrategicMergePatch 的补丁；
// 任一文档无效时返回错误。
//
// 两方 patch 不涉及并发修改，因此总是生成 setElementOrder 以保留用户给定的列表顺序。
func CreateTwoWayMergePatch(original, modified []byte, dataStruct interface{}, fns ...mergepatch.PreconditionFunc) ([]byte, error) {
	schema, err := NewPatchMetaFromStruct(dataStruct)
	if err != nil {
		return nil, err
	}

	return CreateTwoWayMergePatchUsingLookupPatchMeta(original, modified, schema, fns...)
}

// CreateTwoWayMergePatchUsingLookupPatchMeta 是 CreateTwoWayMergePatch 的
// schema 注入版本；空文档按空 map 处理，便于对新建对象生成 patch。
func CreateTwoWayMergePatchUsingLookupPatchMeta(
	original, modified []byte, schema LookupPatchMeta, fns ...mergepatch.PreconditionFunc) ([]byte, error) {
	originalMap := map[string]interface{}{}
	if len(original) > 0 {
		if err := json.Unmarshal(original, &originalMap); err != nil {
			return nil, mergepatch.ErrBadJSONDoc
		}
	}

	modifiedMap := map[string]interface{}{}
	if len(modified) > 0 {
		if err := json.Unmarshal(modified, &modifiedMap); err != nil {
			return nil, mergepatch.ErrBadJSONDoc
		}
	}

	patchMap, err := CreateTwoWayMergeMapPatchUsingLookupPatchMeta(originalMap, modifiedMap, schema, fns...)
	if err != nil {
		return nil, err
	}

	return json.Marshal(patchMap)
}

// CreateTwoWayMergeMapPatch 根据原始和修改后的 JSONMap 创建补丁，
// 序列化后的映射可传给 StrategicMergeMapPatch。
func CreateTwoWayMergeMapPatch(original, modified JSONMap, dataStruct interface{}, fns ...mergepatch.PreconditionFunc) (JSONMap, error) {
	schema, err := NewPatchMetaFromStruct(dataStruct)
	if err != nil {
		return nil, err
	}

	return CreateTwoWayMergeMapPatchUsingLookupPatchMeta(original, modified, schema, fns...)
}

// CreateTwoWayMergeMapPatchUsingLookupPatchMeta 生成 patch 后依次执行前置条件，
// 任一失败即返回 ErrPreconditionFailed（错误中附带生成的 patch 便于诊断）。
func CreateTwoWayMergeMapPatchUsingLookupPatchMeta(original, modified JSONMap, schema LookupPatchMeta, fns ...mergepatch.PreconditionFunc) (JSONMap, error) {
	diffOptions := DiffOptions{
		SetElementOrder: true,
	}
	patchMap, err := diffMaps(original, modified, schema, diffOptions)
	if err != nil {
		return nil, err
	}

	//对补丁应用前置条件，任一条件不满足则返回错误。
	for _, fn := range fns {
		if !fn(patchMap) {
			return nil, mergepatch.NewErrPreconditionFailed(patchMap)
		}
	}

	return patchMap, nil
}

// 返回递归战略合并补丁，使其应用于 original 后得到 modified。
// 包括：
// - modified 有而 original 无的字段加入补丁。
// - modified 与 original 值不同的字段在补丁中更新。
// - original 有而 modified 无的字段通过以下方式删除：
// - 映射字段设为 nil。
// - 映射列表且使用 merge 策略时，为元素添加 deleteDirective。
// - 基本类型列表且使用 merge 策略时，使用并行删除列表。
// - 映射或基本类型列表使用 replace 策略（默认）时，补丁值取 modified 的值。
// - 为使用 retainKeys 策略的字段构建 $retainKeys 指令。
func diffMaps(original, modified map[string]interface{}, schema LookupPatchMeta, diffOptions DiffOptions) (map[string]interface{}, error) {
	patch := map[string]interface{}{}

	//用于构建补丁中的 $retainKeys 指令。
	retainKeysList := make([]interface{}, 0, len(modified))

	//将 modified 映射中的每个值与 original 比较。
	for key, modifiedValue := range modified {
		//获取指针的底层类型。
		if diffOptions.BuildRetainKeysDirective && modifiedValue != nil {
			retainKeysList = append(retainKeysList, key)
		}

		originalValue, ok := original[key]
		if !ok {
			//key 是新增的，加入补丁。
			if !diffOptions.IgnoreChangesAndAdditions {
				patch[key] = modifiedValue
			}
			continue
		}

		//补丁可能带有 patch 指令。
		//TODO: 确认是否需要此分支；apply 应不需要它，original 映射何时会带有 patch 指令？
		foundDirectiveMarker, err := handleDirectiveMarker(key, originalValue, modifiedValue, patch)
		if err != nil {
			return nil, err
		}
		if foundDirectiveMarker {
			continue
		}

		if reflect.TypeOf(originalValue) != reflect.TypeOf(modifiedValue) {
			//类型已变化，加入补丁。
			if !diffOptions.IgnoreChangesAndAdditions {
				patch[key] = modifiedValue
			}
			continue
		}

		//类型相同，比较值。
		switch originalValueTyped := originalValue.(type) {
		case map[string]interface{}:
			modifiedValueTyped := modifiedValue.(map[string]interface{})
			err = handleMapDiff(key, originalValueTyped, modifiedValueTyped, patch, schema, diffOptions)
		case []interface{}:
			modifiedValueTyped := modifiedValue.([]interface{})
			err = handleSliceDiff(key, originalValueTyped, modifiedValueTyped, patch, schema, diffOptions)
		default:
			replacePatchFieldIfNotEqual(key, originalValue, modifiedValue, patch, diffOptions)
		}
		if err != nil {
			return nil, err
		}
	}

	updatePatchIfMissing(original, modified, patch, diffOptions)
	//仅在 retainKeysList 非空且满足以下任一条件时插入：
	//- 补丁非空。
	//- original 中还有需要清除的额外字段。
	if len(retainKeysList) > 0 &&
		(len(patch) > 0 || hasAdditionalNewField(original, modified)) {
		patch[retainKeysDirective] = sortScalars(retainKeysList)
	}
	return patch, nil
}

// handleDirectiveMarker 处理两个对象之间指令标记的差异。
//
// $patch 是元数据而非普通字段：只有值发生变化时才写入 patch，
// 且返回 true 表示该键已处理完，调用方应跳过常规 diff。
func handleDirectiveMarker(key string, originalValue, modifiedValue interface{}, patch map[string]interface{}) (bool, error) {
	if key == directiveMarker {
		originalString, ok := originalValue.(string)
		if !ok {
			return false, fmt.Errorf("invalid value for special key: %s", directiveMarker)
		}
		modifiedString, ok := modifiedValue.(string)
		if !ok {
			return false, fmt.Errorf("invalid value for special key: %s", directiveMarker)
		}
		if modifiedString != originalString {
			patch[directiveMarker] = modifiedValue
		}
		return true, nil
	}
	return false, nil
}

// handleMapDiff 比较 originalValueTyped 与 modifiedValue 两个映射，并将差异写入 patch 的 key。
func handleMapDiff(key string, originalValue, modifiedValue, patch map[string]interface{},
	schema LookupPatchMeta, diffOptions DiffOptions) error {
	subschema, patchMeta, err := schema.LookupPatchMetadataForStruct(key)

	if err != nil {
		//无法查找字段元数据；值相同时无需补丁，
		//因此不影响结果。
		if reflect.DeepEqual(originalValue, modifiedValue) {
			return nil
		}
		//否则返回错误。
		return err
	}
	retainKeys, patchStrategy, err := extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
	if err != nil {
		return err
	}
	diffOptions.BuildRetainKeysDirective = retainKeys
	switch patchStrategy {
	//元数据中的 patch strategy 要求替换整个对象，而不是计算差异。
	case replaceDirective:
		if !diffOptions.IgnoreChangesAndAdditions {
			patch[key] = modifiedValue
		}
	default:
		patchValue, err := diffMaps(originalValue, modifiedValue, subschema, diffOptions)
		if err != nil {
			return err
		}
		//映射不相同，使用提供的补丁值。
		if len(patchValue) > 0 {
			patch[key] = patchValue
		}
	}
	return nil
}

// handleSliceDiff 比较 originalValueTyped 与 modifiedValue 两个切片，并将差异写入 patch 的 key。
func handleSliceDiff(key string, originalValue, modifiedValue []interface{}, patch map[string]interface{},
	schema LookupPatchMeta, diffOptions DiffOptions) error {
	subschema, patchMeta, err := schema.LookupPatchMetadataForSlice(key)
	if err != nil {
		//无法查找字段元数据；值相同时无需补丁，
		//因此不影响结果。
		if reflect.DeepEqual(originalValue, modifiedValue) {
			return nil
		}
		//否则返回错误。
		return err
	}
	retainKeys, patchStrategy, err := extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
	if err != nil {
		return err
	}
	switch patchStrategy {
	//使用 mergePatchKey 合并两个切片。
	case mergeDirective:
		diffOptions.BuildRetainKeysDirective = retainKeys
		addList, deletionList, setOrderList, err := diffLists(originalValue, modifiedValue, subschema, patchMeta.GetPatchMergeKey(), diffOptions)
		if err != nil {
			return err
		}
		if len(addList) > 0 {
			patch[key] = addList
		}
		//生成用于删除的并行列表。
		if len(deletionList) > 0 {
			parallelDeletionListKey := fmt.Sprintf("%s/%s", deleteFromPrimitiveListDirectivePrefix, key)
			patch[parallelDeletionListKey] = deletionList
		}
		if len(setOrderList) > 0 {
			parallelSetOrderListKey := fmt.Sprintf("%s/%s", setElementOrderDirectivePrefix, key)
			patch[parallelSetOrderListKey] = setOrderList
		}
	default:
		replacePatchFieldIfNotEqual(key, originalValue, modifiedValue, patch, diffOptions)
	}
	return nil
}

// original 与 modified 深度不相等且未忽略修改时，replacePatchFieldIfNotEqual 更新补丁。
// original 是旧值，可以是线上对象或 last-applied 配置。
// modified 始终是用户的新配置。
func replacePatchFieldIfNotEqual(key string, original, modified interface{},
	patch map[string]interface{}, diffOptions DiffOptions) {
	if diffOptions.IgnoreChangesAndAdditions {
		//忽略修改时不处理。
		return
	}
	if reflect.DeepEqual(original, modified) {
		//内容相同则不处理。
		return
	}
	//创建补丁，用新值替换旧值。
	patch[key] = modified
}

// 未忽略删除时，updatePatchIfMissing 遍历 original，
// 清除 modified 中不存在的 key。
// original 是旧值，可以是线上对象或 last-applied 配置。
// modified 始终是用户的新配置。
func updatePatchIfMissing(original, modified, patch map[string]interface{}, diffOptions DiffOptions) {
	if diffOptions.IgnoreDeletions {
		//忽略删除时不处理。
		return
	}
	//为删除的值添加 nil。
	for key := range original {
		if _, found := modified[key]; !found {
			patch[key] = nil
		}
	}
}

// validateMergeKeyInLists 检查列表中的每个映射是否包含 merge key。
func validateMergeKeyInLists(mergeKey string, lists ...[]interface{}) error {
	for _, list := range lists {
		for _, item := range list {
			m, ok := item.(map[string]interface{})
			if !ok {
				return mergepatch.ErrBadArgType(m, item)
			}
			if _, ok = m[mergeKey]; !ok {
				return mergepatch.ErrNoMergeKey(m, mergeKey)
			}
		}
	}
	return nil
}

// normalizeElementOrder 分别按 patchOrder 和 serverOrder 排序 patch 与 serverOnly 列表，再合并两者。
// 合并时保持两个列表各自的相对顺序。
// patch 是补丁条目列表，serverOnly 是线上对象中仅服务端存在的条目列表。
// patchOrder 和 serverOrder 分别指定两个列表期望的顺序，
// kind 是两个列表中各条目的类型。
//
// 最终顺序的优先级：$setElementOrder > patch 中的顺序 > live 对象中的顺序。
func normalizeElementOrder(patch, serverOnly, patchOrder, serverOrder []interface{}, mergeKey string, kind reflect.Kind) ([]interface{}, error) {
	patch, err := normalizeSliceOrder(patch, patchOrder, mergeKey, kind)
	if err != nil {
		return nil, err
	}
	serverOnly, err = normalizeSliceOrder(serverOnly, serverOrder, mergeKey, kind)
	if err != nil {
		return nil, err
	}
	all := mergeSortedSlice(serverOnly, patch, serverOrder, mergeKey, kind)

	return all, nil
}

// mergeSortedSlice 尽力按 serverOrder 合并两个已排序列表。
// 将 left 中每个条目插入 right，通常两者会交错。
// 保持 left 和 right 各自的相对顺序。
// 其顺序优先于线上列表。
// left 中条目的插入位置按以下规则确定：
// 从 right 中上次插入位置向后扫描，
// 插入到第一个大于待插入条目的元素之前。
// 示例：以服务端专有条目为 left、补丁条目为 right，将前者插入后者，
// 并以线上对象顺序作为比较依据。
//
// less 只在两侧都能在 serverOrder 中找到位置时才有效；找不到时退化为"取 right"，
// 这样未知元素（例如新加入的 patch 项）会排在已知位置之后。
func mergeSortedSlice(left, right, serverOrder []interface{}, mergeKey string, kind reflect.Kind) []interface{} {
	//如果 l、r 均存在且 l 在 r 之前，则返回 true。
	less := func(l, r interface{}) (bool, bool) {
		li := index(serverOrder, l, mergeKey, kind)
		ri := index(serverOrder, r, mergeKey, kind)
		if li >= 0 && ri >= 0 {
			return li < ri, true
		} else {
			return false, false
		}
	}

	//left 和 right 不应重叠。
	size := len(left) + len(right)
	i, j := 0, 0
	s := make([]interface{}, size, size)

	for k := 0; k < size; k++ {
		if i >= len(left) && j < len(right) {
			//right 中仍有条目。
			s[k] = right[j]
			j++
		} else if j >= len(right) && i < len(left) {
			//left 中仍有条目。
			s[k] = left[i]
			i++
		} else {
			//如果 i、j 均未越界，则比较两个条目。
			less, foundBoth := less(left[i], right[j])
			if foundBoth && less {
				s[k] = left[i]
				i++
			} else {
				s[k] = right[j]
				j++
			}
		}
	}
	return s
}

// index 返回条目在给定列表中的索引，不存在时返回 -1。
// l 不能是切片组成的切片，调用前必须检查。
func index(l []interface{}, valToLookUp interface{}, mergeKey string, kind reflect.Kind) int {
	var getValFn func(interface{}) interface{}
	//根据条目 kind 选择合适的 getValFn；
	//映射返回 merge key 的值，
	//其他类型返回条目本身。
	switch kind {
	case reflect.Map:
		getValFn = func(item interface{}) interface{} {
			typedItem, ok := item.(map[string]interface{})
			if !ok {
				return nil
			}
			val := typedItem[mergeKey]
			return val
		}
	default:
		getValFn = func(item interface{}) interface{} {
			return item
		}
	}

	for i, v := range l {
		if getValFn(valToLookUp) == getValFn(v) {
			return i
		}
	}
	return -1
}

// extractToDeleteItems 将列表拆为两个：保留条目与待删除条目。
//
// $patch: delete 元素不会参与后续合并，先剥离出来单独处理。
func extractToDeleteItems(l []interface{}) ([]interface{}, []interface{}, error) {
	var nonDelete, toDelete []interface{}
	for _, v := range l {
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, nil, mergepatch.ErrBadArgType(m, v)
		}

		directive, foundDirective := m[directiveMarker]
		if foundDirective && directive == deleteDirective {
			toDelete = append(toDelete, v)
		} else {
			nonDelete = append(nonDelete, v)
		}
	}
	return nonDelete, toDelete, nil
}

// normalizeSliceOrder 按 order 排序 toSort 列表。
//
// 使用 SliceStable 保证不在 order 中的元素维持原有相对顺序；
// 删除指令被移到末尾，避免影响排序比较。
func normalizeSliceOrder(toSort, order []interface{}, mergeKey string, kind reflect.Kind) ([]interface{}, error) {
	var toDelete []interface{}
	if kind == reflect.Map {
		//确保 toSort 和 order 中的每个条目都有 merge key。
		err := validateMergeKeyInLists(mergeKey, toSort, order)
		if err != nil {
			return nil, err
		}
		toSort, toDelete, err = extractToDeleteItems(toSort)
		if err != nil {
			return nil, err
		}
	}

	sort.SliceStable(toSort, func(i, j int) bool {
		if ii := index(order, toSort[i], mergeKey, kind); ii >= 0 {
			if ij := index(order, toSort[j], mergeKey, kind); ij >= 0 {
				return ii < ij
			}
		}
		return true
	})
	toSort = append(toSort, toDelete...)
	return toSort, nil
}

// 返回递归战略合并补丁，以及必要时生成的并行删除列表和元素顺序列表。
// 只有使用 merge 策略的基本类型列表会生成并行删除列表。
// 对于使用 merge 语义的列表，这两个列表应用于 original 后应得到 modified。
func diffLists(original, modified []interface{}, schema LookupPatchMeta, mergeKey string, diffOptions DiffOptions) ([]interface{}, []interface{}, []interface{}, error) {
	if len(original) == 0 {
		//两个切片都为空时不处理。
		if len(modified) == 0 || diffOptions.IgnoreChangesAndAdditions {
			return nil, nil, nil, nil
		}

		// 旧切片为空时加入新切片的所有元素。
		return modified, nil, nil, nil
	}

	// 列表元素类型必须一致，且不允许列表的列表。
	elementType, err := sliceElementType(original, modified)
	if err != nil {
		return nil, nil, nil, err
	}

	var patchList, deleteList, setOrderList []interface{}
	kind := elementType.Kind()
	switch kind {
	case reflect.Map:
		patchList, deleteList, err = diffListsOfMaps(original, modified, schema, mergeKey, diffOptions)
		if err != nil {
			return nil, nil, nil, err
		}
		patchList, err = normalizeSliceOrder(patchList, modified, mergeKey, kind)
		if err != nil {
			return nil, nil, nil, err
		}
		orderSame, err := isOrderSame(original, modified, mergeKey)
		if err != nil {
			return nil, nil, nil, err
		}
		//将删除列表追加到补丁列表末尾。
		patchList = append(patchList, deleteList...)
		deleteList = nil
		//内容或顺序变化时生成 setElementOrder 列表。
		if diffOptions.SetElementOrder &&
			((!diffOptions.IgnoreChangesAndAdditions && (len(patchList) > 0 || !orderSame)) ||
				(!diffOptions.IgnoreDeletions && len(patchList) > 0)) {
			//生成仅含 merge key 的映射列表。
			setOrderList = make([]interface{}, len(modified))
			for i, v := range modified {
				typedV := v.(map[string]interface{})
				setOrderList[i] = map[string]interface{}{
					mergeKey: typedV[mergeKey],
				}
			}
		}
	case reflect.Slice:
		//API 不允许列表嵌套列表。
		return nil, nil, nil, mergepatch.ErrNoListOfLists
	default:
		patchList, deleteList, err = diffListsOfScalars(original, modified, diffOptions)
		if err != nil {
			return nil, nil, nil, err
		}
		patchList, err = normalizeSliceOrder(patchList, modified, mergeKey, kind)
		//内容或顺序变化时生成 setElementOrder 列表。
		if diffOptions.SetElementOrder && ((!diffOptions.IgnoreDeletions && len(deleteList) > 0) ||
			(!diffOptions.IgnoreChangesAndAdditions && !reflect.DeepEqual(original, modified))) {
			setOrderList = modified
		}
	}
	return patchList, deleteList, setOrderList, err
}

// isOrderSame 检查列表顺序是否变化。
func isOrderSame(original, modified []interface{}, mergeKey string) (bool, error) {
	if len(original) != len(modified) {
		return false, nil
	}
	for i, modifiedItem := range modified {
		equal, err := mergeKeyValueEqual(original[i], modifiedItem, mergeKey)
		if err != nil || !equal {
			return equal, err
		}
	}
	return true, nil
}

// diffListsOfScalars 返回新增列表和删除列表。
// diffOptions.IgnoreChangesAndAdditions 为 true 时不计算新增列表。
// diffOptions.IgnoreDeletions 为 true 时不计算删除列表。
// original 可能被修改，modified 保证不被修改。
//
// 标量无法用 merge key 匹配，只能先排序再做双指针归并；排序依赖
// fmt.Sprintf("%v") 的字符串序，因此不同数值类型可能按字典序而非数值序排列。
func diffListsOfScalars(original, modified []interface{}, diffOptions DiffOptions) ([]interface{}, []interface{}, error) {
	modifiedCopy := make([]interface{}, len(modified))
	copy(modifiedCopy, modified)
	//排序基本类型以便计算差异。
	originalScalars := sortScalars(original)
	modifiedScalars := sortScalars(modifiedCopy)

	originalIndex, modifiedIndex := 0, 0
	addList := []interface{}{}
	deletionList := []interface{}{}

	for {
		originalInBounds := originalIndex < len(originalScalars)
		modifiedInBounds := modifiedIndex < len(modifiedScalars)
		if !originalInBounds && !modifiedInBounds {
			break
		}
		//需要比较基本类型的字符串表示，
		//因为 scalar 是 interface，不支持 < 或 >；
		//sortScalars 据此比较。
		var originalString, modifiedString string
		var originalValue, modifiedValue interface{}
		if originalInBounds {
			originalValue = originalScalars[originalIndex]
			originalString = fmt.Sprintf("%v", originalValue)
		}
		if modifiedInBounds {
			modifiedValue = modifiedScalars[modifiedIndex]
			modifiedString = fmt.Sprintf("%v", modifiedValue)
		}

		originalV, modifiedV := compareListValuesAtIndex(originalInBounds, modifiedInBounds, originalString, modifiedString)
		switch {
		case originalV == nil && modifiedV == nil:
			originalIndex++
			modifiedIndex++
		case originalV != nil && modifiedV == nil:
			if !diffOptions.IgnoreDeletions {
				deletionList = append(deletionList, originalValue)
			}
			originalIndex++
		case originalV == nil && modifiedV != nil:
			if !diffOptions.IgnoreChangesAndAdditions {
				addList = append(addList, modifiedValue)
			}
			modifiedIndex++
		default:
			return nil, nil, fmt.Errorf("Unexpected returned value from compareListValuesAtIndex: %v and %v", originalV, modifiedV)
		}
	}

	return addList, deduplicateScalars(deletionList), nil
}

// 第一个返回值非 nil 表示 list1 含有 list2 没有的元素；
// 第二个返回值非 nil 表示 list2 含有 list1 没有的元素。
func compareListValuesAtIndex(list1Inbounds, list2Inbounds bool, list1Value, list2Value string) (interface{}, interface{}) {
	bothInBounds := list1Inbounds && list2Inbounds
	switch {
	//基本类型相同。
	case bothInBounds && list1Value == list2Value:
		return nil, nil
	//仅 list2 未越界。
	case !list1Inbounds:
		fallthrough
	//list2 有额外元素。
	case bothInBounds && list1Value > list2Value:
		return nil, list2Value
	//仅 original 未越界。
	case !list2Inbounds:
		fallthrough
	//original 有额外元素。
	case bothInBounds && list1Value < list2Value:
		return list1Value, nil
	default:
		return nil, nil
	}
}

// diffListsOfMaps 接收两个列表，返回包含新增和修改的递归补丁列表及删除列表。
//
// 两侧先按 merge key 排序，再做有序归并比较；merge key 值统一转成字符串比较，
// 因此数字键 1 与字符串键 "1" 会被视为同一个键。
func diffListsOfMaps(original, modified []interface{}, schema LookupPatchMeta, mergeKey string, diffOptions DiffOptions) ([]interface{}, []interface{}, error) {
	patch := make([]interface{}, 0, len(modified))
	deletionList := make([]interface{}, 0, len(original))

	originalSorted, err := sortMergeListsByNameArray(original, schema, mergeKey, false)
	if err != nil {
		return nil, nil, err
	}
	modifiedSorted, err := sortMergeListsByNameArray(modified, schema, mergeKey, false)
	if err != nil {
		return nil, nil, err
	}

	originalIndex, modifiedIndex := 0, 0
	for {
		originalInBounds := originalIndex < len(originalSorted)
		modifiedInBounds := modifiedIndex < len(modifiedSorted)
		bothInBounds := originalInBounds && modifiedInBounds
		if !originalInBounds && !modifiedInBounds {
			break
		}

		var originalElementMergeKeyValueString, modifiedElementMergeKeyValueString string
		var originalElementMergeKeyValue, modifiedElementMergeKeyValue interface{}
		var originalElement, modifiedElement map[string]interface{}
		if originalInBounds {
			originalElement, originalElementMergeKeyValue, err = getMapAndMergeKeyValueByIndex(originalIndex, mergeKey, originalSorted)
			if err != nil {
				return nil, nil, err
			}
			originalElementMergeKeyValueString = fmt.Sprintf("%v", originalElementMergeKeyValue)
		}
		if modifiedInBounds {
			modifiedElement, modifiedElementMergeKeyValue, err = getMapAndMergeKeyValueByIndex(modifiedIndex, mergeKey, modifiedSorted)
			if err != nil {
				return nil, nil, err
			}
			modifiedElementMergeKeyValueString = fmt.Sprintf("%v", modifiedElementMergeKeyValue)
		}

		switch {
		case bothInBounds && ItemMatchesOriginalAndModifiedSlice(originalElementMergeKeyValueString, modifiedElementMergeKeyValueString):
			//merge key 相同，递归合并。
			patchValue, err := diffMaps(originalElement, modifiedElement, schema, diffOptions)
			if err != nil {
				return nil, nil, err
			}
			if len(patchValue) > 0 {
				patchValue[mergeKey] = modifiedElementMergeKeyValue
				patch = append(patch, patchValue)
			}
			originalIndex++
			modifiedIndex++
		//仅 modified 未越界。
		case !originalInBounds:
			fallthrough
		//modified 有额外映射。
		case bothInBounds && ItemAddedToModifiedSlice(originalElementMergeKeyValueString, modifiedElementMergeKeyValueString):
			if !diffOptions.IgnoreChangesAndAdditions {
				patch = append(patch, modifiedElement)
			}
			modifiedIndex++
		//仅 original 未越界。
		case !modifiedInBounds:
			fallthrough
		//original 有额外映射。
		case bothInBounds && ItemRemovedFromModifiedSlice(originalElementMergeKeyValueString, modifiedElementMergeKeyValueString):
			if !diffOptions.IgnoreDeletions {
				//条目已删除，添加 delete 指令。
				deletionList = append(deletionList, CreateDeleteDirective(mergeKey, originalElementMergeKeyValue))
			}
			originalIndex++
		}
	}

	return patch, deletionList, nil
}

// getMapAndMergeKeyValueByIndex 根据索引返回列表中的映射及其 merge key 值。
func getMapAndMergeKeyValueByIndex(index int, mergeKey string, listOfMaps []interface{}) (map[string]interface{}, interface{}, error) {
	m, ok := listOfMaps[index].(map[string]interface{})
	if !ok {
		return nil, nil, mergepatch.ErrBadArgType(m, listOfMaps[index])
	}

	val, ok := m[mergeKey]
	if !ok {
		return nil, nil, mergepatch.ErrNoMergeKey(m, mergeKey)
	}
	return m, val, nil
}

// StrategicMergePatch 应用战略合并补丁。补丁和原始文档都必须是 JSON 编码内容；
// 可先根据原始和修改后的文档调用 CreateStrategicMergePatch 创建补丁。
func StrategicMergePatch(original, patch []byte, dataStruct interface{}) ([]byte, error) {
	schema, err := NewPatchMetaFromStruct(dataStruct)
	if err != nil {
		return nil, err
	}

	return StrategicMergePatchUsingLookupPatchMeta(original, patch, schema)
}

// StrategicMergePatchUsingLookupPatchMeta 是 schema 注入版本：
// 解码两侧 JSON 后合并，再把结果重新编码。
func StrategicMergePatchUsingLookupPatchMeta(original, patch []byte, schema LookupPatchMeta) ([]byte, error) {
	originalMap, err := handleUnmarshal(original)
	if err != nil {
		return nil, err
	}
	patchMap, err := handleUnmarshal(patch)
	if err != nil {
		return nil, err
	}

	result, err := StrategicMergeMapPatchUsingLookupPatchMeta(originalMap, patchMap, schema)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// handleUnmarshal 把 nil 输入当作空 JSON 对象，避免调用方额外判空。
func handleUnmarshal(j []byte) (map[string]interface{}, error) {
	if j == nil {
		j = []byte("{}")
	}

	m := map[string]interface{}{}
	err := json.Unmarshal(j, &m)
	if err != nil {
		return nil, mergepatch.ErrBadJSONDoc
	}
	return m, nil
}

// StrategicMergeMapPatch 应用战略合并补丁，original 和 patch 都必须是 JSONMap。
// 可调用 CreateTwoWayMergeMapPatch 创建补丁。
// 警告：此函数会修改 original 和 patch JSONMap，不应复用它们。
func StrategicMergeMapPatch(original, patch JSONMap, dataStruct interface{}) (JSONMap, error) {
	schema, err := NewPatchMetaFromStruct(dataStruct)
	if err != nil {
		return nil, err
	}

	//支持战略合并补丁的字段需要 Go 结构体 tag `patchMergeKey` 和 `patchStrategy`。
	//原生资源可以根据已知字段直接确定这些 tag。

	//自定义资源被解码为 Unstructured，且缺少如何处理各字段的元数据，
	//因此找不到 Go 结构体 tag，也无法直接执行战略合并；应快速失败并返回错误。
	//
	// 这是刻意的前置检查：若不在此拒绝，会静默退化成普通 merge patch，
	// 与调用方对 list merge key 的预期不符。
	if _, ok := dataStruct.(*unstructured.Unstructured); ok {
		return nil, mergepatch.ErrUnsupportedStrategicMergePatchFormat
	}

	return StrategicMergeMapPatchUsingLookupPatchMeta(original, patch, schema)
}

// StrategicMergeMapPatchUsingLookupPatchMeta 使用给定 schema 元数据将 patch 应用到 original。
// 与 StrategicMergeMapPatch 不同，调用方需直接提供字段的补丁策略和合并键信息。
func StrategicMergeMapPatchUsingLookupPatchMeta(original, patch JSONMap, schema LookupPatchMeta) (JSONMap, error) {
	// 服务端合并需要处理并行列表（删除/排序指令），且保留未匹配的 null
	// 以便把显式删除传递给下层存储。
	mergeOptions := MergeOptions{
		MergeParallelList:    true,
		IgnoreUnmatchedNulls: true,
	}
	return mergeMap(original, patch, schema, mergeOptions)
}

// MergeStrategicMergeMapPatchUsingLookupPatchMeta 合并战略合并补丁，并保留 `null` 字段和并行列表。
// 后一个补丁会覆盖前一个补丁对同一字段的修改；
// 如需检测冲突，应先调用 MergingMapsHaveConflicts。
// 将合并补丁应用于 JSONMap 的结果，等同于依次应用每个战略合并补丁。
//
// 与 StrategicMergeMapPatch 不同，这里不合并并行列表、也不忽略未匹配的 null，
// 因为输入本身就是 patch，需要保留全部指令以便后续整体应用。
func MergeStrategicMergeMapPatchUsingLookupPatchMeta(schema LookupPatchMeta, patches ...JSONMap) (JSONMap, error) {
	mergeOptions := MergeOptions{
		MergeParallelList:    false,
		IgnoreUnmatchedNulls: false,
	}
	merged := JSONMap{}
	var err error
	for _, patch := range patches {
		merged, err = mergeMap(merged, patch, schema, mergeOptions)
		if err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// handleDirectiveInMergeMap 处理合并两个映射时的 patch 指令。
//
// $patch: replace 会先删除指令键再返回 patch 本身；$patch: delete 返回空 map，
// 由上层决定是否保留（取决于 IgnoreUnmatchedNulls）。
func handleDirectiveInMergeMap(directive interface{}, patch map[string]interface{}) (map[string]interface{}, error) {
	if directive == replaceDirective {
		//补丁包含 "$patch: replace" 时不合并，直接使用补丁；
		//未来可增加仅影响 $patch 所在映射的单层 replace。
		delete(patch, directiveMarker)
		return patch, nil
	}

	if directive == deleteDirective {
		//补丁包含 "$patch: delete" 时不合并，直接返回空映射。
		return map[string]interface{}{}, nil
	}

	return nil, mergepatch.ErrBadPatchType(directive, patch)
}

// containsDirectiveMarker 判断列表元素是否带有 $patch 指令。
func containsDirectiveMarker(item interface{}) bool {
	m, ok := item.(map[string]interface{})
	if ok {
		if _, foundDirectiveMarker := m[directiveMarker]; foundDirectiveMarker {
			return true
		}
	}
	return false
}

// mergeKeyValueEqual 比较两个列表元素的 merge key 值。
// mergeKey 为空表示标量列表，直接比较值本身。
func mergeKeyValueEqual(left, right interface{}, mergeKey string) (bool, error) {
	if len(mergeKey) == 0 {
		return left == right, nil
	}
	typedLeft, ok := left.(map[string]interface{})
	if !ok {
		return false, mergepatch.ErrBadArgType(typedLeft, left)
	}
	typedRight, ok := right.(map[string]interface{})
	if !ok {
		return false, mergepatch.ErrBadArgType(typedRight, right)
	}
	mergeKeyLeft, ok := typedLeft[mergeKey]
	if !ok {
		return false, mergepatch.ErrNoMergeKey(typedLeft, mergeKey)
	}
	mergeKeyRight, ok := typedRight[mergeKey]
	if !ok {
		return false, mergepatch.ErrNoMergeKey(typedRight, mergeKey)
	}
	return mergeKeyLeft == mergeKeyRight, nil
}

// extractKey 去除前缀并返回原始 key。
//
// 只按第一个 '/' 切分：字段名本身可能含 '/'（如带斜杠的注解键）。
func extractKey(s, prefix string) (string, error) {
	substrings := strings.SplitN(s, "/", 2)
	if len(substrings) <= 1 || substrings[0] != prefix {
		switch prefix {
		case deleteFromPrimitiveListDirectivePrefix:
			return "", mergepatch.ErrBadPatchFormatForPrimitiveList
		case setElementOrderDirectivePrefix:
			return "", mergepatch.ErrBadPatchFormatForSetElementOrderList
		default:
			return "", fmt.Errorf("fail to find unknown prefix %q in %s\n", prefix, s)
		}
	}
	return substrings[1], nil
}

// validatePatchUsingSetOrderList 验证：
// setOrderList 中任意两个条目的相对顺序与 patch 列表一致。
// patch 列表条目必须是 $setElementOrder 列表的子集或与其相同（忽略删除项）。
//
// 用双指针做子序列检查：patch 中的每一项都必须按序出现在 setElementOrder 中，
// 否则说明用户给出的顺序指令与实际 patch 内容矛盾。
func validatePatchWithSetOrderList(patchList, setOrderList interface{}, mergeKey string) error {
	typedSetOrderList, ok := setOrderList.([]interface{})
	if !ok {
		return mergepatch.ErrBadPatchFormatForSetElementOrderList
	}
	typedPatchList, ok := patchList.([]interface{})
	if !ok {
		return mergepatch.ErrBadPatchFormatForSetElementOrderList
	}
	if len(typedSetOrderList) == 0 || len(typedPatchList) == 0 {
		return nil
	}

	var nonDeleteList []interface{}
	var err error
	if len(mergeKey) > 0 {
		nonDeleteList, _, err = extractToDeleteItems(typedPatchList)
		if err != nil {
			return err
		}
	} else {
		nonDeleteList = typedPatchList
	}

	patchIndex, setOrderIndex := 0, 0
	for patchIndex < len(nonDeleteList) && setOrderIndex < len(typedSetOrderList) {
		if containsDirectiveMarker(nonDeleteList[patchIndex]) {
			patchIndex++
			continue
		}
		mergeKeyEqual, err := mergeKeyValueEqual(nonDeleteList[patchIndex], typedSetOrderList[setOrderIndex], mergeKey)
		if err != nil {
			return err
		}
		if mergeKeyEqual {
			patchIndex++
		}
		setOrderIndex++
	}
	//patchIndex 未越界而 setOrderIndex 越界，表示 patch 列表与 setElementOrder 列表存在不匹配条目。
	//第二项是合理性检查；第一项成立时它应始终成立。
	if patchIndex < len(nonDeleteList) && setOrderIndex >= len(typedSetOrderList) {
		return fmt.Errorf("The order in patch list:\n%v\n doesn't match %s list:\n%v\n", typedPatchList, setElementOrderDirectivePrefix, setOrderList)
	}
	return nil
}

// preprocessDeletionListForMerging 预处理删除列表，返回 shouldContinue、isDeletionList 和 noPrefixKey。
//
// 非并行列表合并模式下，删除列表被当作普通字段直接写入 original；
// 合并模式下则剥离前缀，让后续 mergeSlice 按真正的字段名处理。
func preprocessDeletionListForMerging(key string, original map[string]interface{},
	patchVal interface{}, mergeDeletionList bool) (bool, bool, string, error) {
	//找到并行删除列表且需要合并该列表时，
	//将 key 恢复为原始 key，并设置 isDeleteList 标志。
	foundParallelListPrefix := strings.HasPrefix(key, deleteFromPrimitiveListDirectivePrefix)
	if foundParallelListPrefix {
		if !mergeDeletionList {
			original[key] = patchVal
			return true, false, "", nil
		}
		originalKey, err := extractKey(key, deleteFromPrimitiveListDirectivePrefix)
		return false, true, originalKey, err
	}
	return false, false, "", nil
}

// applyRetainKeysDirective 查找 retainKeys 指令并应用于 original：
// - 指令不存在时不处理。
// - 找到指令时清除 original 中不在指令列表内的 key。
// - 验证 patch 中所有 key 都在 retainKeys 指令内。
// 注意：original 也可能是另一个补丁请求，例如将新增和修改补丁应用到删除补丁；此时它可能带有指令。
//
// retainKeys 的语义是"白名单"：original 中不在列表里的字段会被清除，
// 而 patch 中出现未列入白名单的字段则视为格式错误。
func applyRetainKeysDirective(original, patch map[string]interface{}, options MergeOptions) error {
	retainKeysInPatch, foundInPatch := patch[retainKeysDirective]
	if !foundInPatch {
		return nil
	}
	//清理指令。
	delete(patch, retainKeysDirective)

	if !options.MergeParallelList {
		//如果 original 也是补丁，两个补丁都带有 retainKeys 指令时必须确保它们相同。
		//original 补丁没有该指令时，从 modified 补丁复制。
		retainKeysInOriginal, foundInOriginal := original[retainKeysDirective]
		if foundInOriginal {
			if !reflect.DeepEqual(retainKeysInOriginal, retainKeysInPatch) {
				//此错误不应发生。
				return fmt.Errorf("%v and %v are not deep equal: this may happen when calculating the 3-way diff patch", retainKeysInOriginal, retainKeysInPatch)
			}
		} else {
			original[retainKeysDirective] = retainKeysInPatch
		}
		return nil
	}

	retainKeysList, ok := retainKeysInPatch.([]interface{})
	if !ok {
		return mergepatch.ErrBadPatchFormatForRetainKeys
	}

	//验证 patch 中所有字段都存在于 retainKeysList。
	//该映射仅作为集合使用，值不会被读取。
	m := map[interface{}]struct{}{}
	for _, v := range retainKeysList {
		m[v] = struct{}{}
	}
	for k, v := range patch {
		if v == nil || strings.HasPrefix(k, deleteFromPrimitiveListDirectivePrefix) ||
			strings.HasPrefix(k, setElementOrderDirectivePrefix) {
			continue
		}
		//如果 patch 中含有不在 retainKeys 列表内的条目，
		//则补丁无效。
		if _, found := m[k]; !found {
			return mergepatch.ErrBadPatchFormatForRetainKeys
		}
	}

	//清除不存在的字段。
	for k := range original {
		if _, found := m[k]; !found {
			delete(original, k)
		}
	}
	return nil
}

// mergePatchIntoOriginal 处理 $setElementOrder 列表。
// 不合并指令时，确保 $setElementOrder 列表只存在于 original。
// 合并指令时，查找 $setElementOrder 列表及对应的 patch 列表，
// 校验后合并。
// 随后按 setElementOrder、patch 列表和线上列表的相对顺序排序。
// 优先级为 $setElementOrder > patch 列表顺序 > 线上列表顺序。
// 条目合并后会删除，避免后续重复处理。
// 参考：https://git.k8s.io/design-proposals-archive/cli/preserve-order-in-strategic-merge-patch.md
//
// 该函数会直接修改 patch：处理完的指令与对应列表键都会被删除，
// 因此调用方不能复用传入的 patch map。
func mergePatchIntoOriginal(original, patch map[string]interface{}, schema LookupPatchMeta, mergeOptions MergeOptions) error {
	for key, patchV := range patch {
		//没有顺序指令时不处理。
		if !strings.HasPrefix(key, setElementOrderDirectivePrefix) {
			continue
		}

		setElementOrderInPatch := patchV
		//将第二个补丁（patch）的指令复制到第一个补丁（original），
		//检查两者相等，并删除第二个补丁中的指令。
		if !mergeOptions.MergeParallelList {
			setElementOrderListInOriginal, ok := original[key]
			if ok {
				//检查 original 与 patch 中的 setElementOrder 列表是否匹配。
				if !reflect.DeepEqual(setElementOrderListInOriginal, setElementOrderInPatch) {
					return mergepatch.ErrBadPatchFormatForSetElementOrderList
				}
			} else {
				//将 setElementOrder 列表从 patch 移至 original。
				original[key] = setElementOrderInPatch
			}
		}
		delete(patch, key)

		var (
			ok                                          bool
			originalFieldValue, patchFieldValue, merged []interface{}
			patchStrategy                               string
			patchMeta                                   PatchMeta
			subschema                                   LookupPatchMeta
		)
		typedSetElementOrderList, ok := setElementOrderInPatch.([]interface{})
		if !ok {
			return mergepatch.ErrBadArgType(typedSetElementOrderList, setElementOrderInPatch)
		}
		//去除 setElementOrderDirectivePrefix，得到 original 中列表字段的 key。
		originalKey, err := extractKey(key, setElementOrderDirectivePrefix)
		if err != nil {
			return err
		}
		//尝试在 original 和 modified 中查找 originalKey 对应的列表并合并。
		originalList, foundOriginal := original[originalKey]
		patchList, foundPatch := patch[originalKey]
		if foundOriginal {
			originalFieldValue, ok = originalList.([]interface{})
			if !ok {
				return mergepatch.ErrBadArgType(originalFieldValue, originalList)
			}
		}
		if foundPatch {
			patchFieldValue, ok = patchList.([]interface{})
			if !ok {
				return mergepatch.ErrBadArgType(patchFieldValue, patchList)
			}
		}
		subschema, patchMeta, err = schema.LookupPatchMetadataForSlice(originalKey)
		if err != nil {
			return err
		}
		_, patchStrategy, err = extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
		if err != nil {
			return err
		}
		//检查元素顺序列表与其作用的字段是否一致。
		err = validatePatchWithSetOrderList(patchFieldValue, typedSetElementOrderList, patchMeta.GetPatchMergeKey())
		if err != nil {
			return err
		}

		switch {
		case foundOriginal && !foundPatch:
			//列表内容未变化。
			merged = originalFieldValue
		case !foundOriginal && foundPatch:
			//列表是新增的。
			v, keep := removeDirectives(patchFieldValue)
			if !keep {
				//不应发生，因为 patchFieldValue 是切片。
				continue
			}

			merged = v.([]interface{})
		case foundOriginal && foundPatch:
			merged, err = mergeSliceHandler(originalList, patchList, subschema,
				patchStrategy, patchMeta.GetPatchMergeKey(), false, mergeOptions)
			if err != nil {
				return err
			}
		case !foundOriginal && !foundPatch:
			continue
		}

		//将所有条目分为 patch 条目和服务端专有条目，再强制排序。
		var patchItems, serverOnlyItems []interface{}
		if len(patchMeta.GetPatchMergeKey()) == 0 {
			//基本类型无需 merge key 即可分组。
			patchItems, serverOnlyItems = partitionPrimitivesByPresentInList(merged, typedSetElementOrderList)

		} else {
			//映射需要 merge key 才能分组。
			patchItems, serverOnlyItems, err = partitionMapsByPresentInList(merged, typedSetElementOrderList, patchMeta.GetPatchMergeKey())
			if err != nil {
				return err
			}
		}

		elementType, err := sliceElementType(originalFieldValue, patchFieldValue)
		if err != nil {
			return err
		}
		kind := elementType.Kind()
		//规范化合并后的列表。
		//typedSetElementOrderList 已包含 typedPatchList 的全部相对顺序，
		//因此无需再使用 typedPatchList。
		both, err := normalizeElementOrder(patchItems, serverOnlyItems, typedSetElementOrderList, originalFieldValue, patchMeta.GetPatchMergeKey(), kind)
		if err != nil {
			return err
		}
		original[originalKey] = both
		//从 patch 中删除 patch 列表，避免后续重复处理。
		delete(patch, originalKey)
	}
	return nil
}

// partitionPrimitivesByPresentInList 将基本类型条目分为存在于 partitionBy 中和不存在的两个切片。
func partitionPrimitivesByPresentInList(original, partitionBy []interface{}) ([]interface{}, []interface{}) {
	patch := make([]interface{}, 0, len(original))
	serverOnly := make([]interface{}, 0, len(original))
	inPatch := map[interface{}]bool{}
	for _, v := range partitionBy {
		inPatch[v] = true
	}
	for _, v := range original {
		if !inPatch[v] {
			serverOnly = append(serverOnly, v)
		} else {
			patch = append(patch, v)
		}
	}
	return patch, serverOnly
}

// partitionMapsByPresentInList 将映射条目分为存在于 partitionBy 中和不存在的两个切片。
func partitionMapsByPresentInList(original, partitionBy []interface{}, mergeKey string) ([]interface{}, []interface{}, error) {
	patch := make([]interface{}, 0, len(original))
	serverOnly := make([]interface{}, 0, len(original))
	for _, v := range original {
		typedV, ok := v.(map[string]interface{})
		if !ok {
			return nil, nil, mergepatch.ErrBadArgType(typedV, v)
		}
		mergeKeyValue, foundMergeKey := typedV[mergeKey]
		if !foundMergeKey {
			return nil, nil, mergepatch.ErrNoMergeKey(typedV, mergeKey)
		}
		_, _, found, err := findMapInSliceBasedOnKeyValue(partitionBy, mergeKey, mergeKeyValue)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			serverOnly = append(serverOnly, v)
		} else {
			patch = append(patch, v)
		}
	}
	return patch, serverOnly, nil
}

// 移除对象中的指令，返回替代值以及是否保留该字段或索引。
// 可能修改输入。
//
// 带 $patch 指令的对象整体不保留（返回 keep=false），
// 列表中的此类元素会被丢弃；其余递归清理。
func removeDirectives(obj interface{}) (interface{}, bool) {
	if obj == nil {
		return obj, true
	} else if typedV, ok := obj.(map[string]interface{}); ok {
		if _, hasDirective := typedV[directiveMarker]; hasDirective {
			return nil, false
		}

		for k, v := range typedV {
			var keep bool
			typedV[k], keep = removeDirectives(v)
			if !keep {
				delete(typedV, k)
			}
		}
		return typedV, true
	} else if typedV, ok := obj.([]interface{}); ok {
		var res []interface{}
		if typedV != nil {
			//patch 非 nil 时确保结果非 nil。
			res = []interface{}{}
		}
		for _, v := range typedV {
			if newV, keep := removeDirectives(v); keep {
				res = append(res, newV)
			}
		}
		return res, true
	} else {
		return obj, true
	}
}

// 将补丁映射中的字段合并到 original；Go 中映射深拷贝非常复杂，
// 因此 original 和 patch 都可能被修改。
// mergeOptions.MergeParallelList 控制使用并行列表删除条目还是保留列表。
// 如果 patch 含有 original 中不存在的 null 字段（如 field_1: null），
// 要将它传递到最终结果，需设置
// mergeOptions.IgnoreUnmatchedNulls == false。
//
// 这是整个包的核心合并循环：先处理 map 级指令（$patch、$retainKeys、
// $setElementOrder），再逐键递归；null 表示删除，类型变化表示整体替换。
func mergeMap(original, patch map[string]interface{}, schema LookupPatchMeta, mergeOptions MergeOptions) (map[string]interface{}, error) {
	if v, ok := patch[directiveMarker]; ok {
		return handleDirectiveInMergeMap(v, patch)
	}

	//original 允许为 nil，以简化其他位置的逻辑。
	//original 为 nil 时替换为空映射，再应用补丁。
	if original == nil {
		original = map[string]interface{}{}
	}

	err := applyRetainKeysDirective(original, patch, mergeOptions)
	if err != nil {
		return nil, err
	}

	//处理 $setElementOrder 列表及使用相同 key 的其他列表。
	//不合并指令时，确保 $setElementOrder 列表只存在于 original。
	//合并指令时，共同处理 $setElementOrder 及其 patch 列表。
	//合并后的元素会从 patch 中删除，避免重复处理。
	err = mergePatchIntoOriginal(original, patch, schema, mergeOptions)
	if err != nil {
		return nil, err
	}

	//开始将补丁合并到 original。
	for k, patchV := range patch {
		skipProcessing, isDeleteList, noPrefixKey, err := preprocessDeletionListForMerging(k, original, patchV, mergeOptions.MergeParallelList)
		if err != nil {
			return nil, err
		}
		if skipProcessing {
			continue
		}
		if len(noPrefixKey) > 0 {
			k = noPrefixKey
		}

		//如果该 key 的值为 null，则从 original 删除已有 key；
		//否则检查是否保留或跳过它。
		//保留 null 有助于向 API 服务器发送显式删除，
		//某些情况下这会导致与 create 行为不一致。
		//参考：https://github.com/kubernetes/kubernetes/issues/123304
		//为避免破坏兼容性，
		//客户端也做了相应修改，确保 create 和 patch 行为幂等。
		if patchV == nil {
			delete(original, k)
			if mergeOptions.IgnoreUnmatchedNulls {
				continue
			}
		}

		_, ok := original[k]
		if !ok {
			if !isDeleteList {
				//原文档中不存在该 key 时，直接采用补丁值。
				if mergeOptions.IgnoreUnmatchedNulls {
					discardNullValuesFromPatch(patchV)
				}
				original[k], ok = removeDirectives(patchV)
				if !ok {
					delete(original, k)
				}
			}
			continue
		}

		originalType := reflect.TypeOf(original[k])
		patchType := reflect.TypeOf(patchV)
		if originalType != patchType {
			if !isDeleteList {
				if mergeOptions.IgnoreUnmatchedNulls {
					discardNullValuesFromPatch(patchV)
				}
				original[k], ok = removeDirectives(patchV)
				if !ok {
					delete(original, k)
				}
			}
			continue
		}
		//两者都是映射或列表时递归处理。
		switch originalType.Kind() {
		case reflect.Map:
			subschema, patchMeta, err2 := schema.LookupPatchMetadataForStruct(k)
			if err2 != nil {
				return nil, err2
			}
			_, patchStrategy, err2 := extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
			if err2 != nil {
				return nil, err2
			}
			original[k], err = mergeMapHandler(original[k], patchV, subschema, patchStrategy, mergeOptions)
		case reflect.Slice:
			subschema, patchMeta, err2 := schema.LookupPatchMetadataForSlice(k)
			if err2 != nil {
				return nil, err2
			}
			_, patchStrategy, err2 := extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
			if err2 != nil {
				return nil, err2
			}
			original[k], err = mergeSliceHandler(original[k], patchV, subschema, patchStrategy, patchMeta.GetPatchMergeKey(), isDeleteList, mergeOptions)
		default:
			original[k], ok = removeDirectives(patchV)
			if !ok {
				//patchV 本身是指令时不要保留它。
				delete(original, k)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return original, nil
}

// discardNullValuesFromPatch 从 patch 中丢弃所有 null 属性值。
// 它会遍历所有切片和映射类型。
//
// 仅在 IgnoreUnmatchedNulls 为 true 时使用：对象原本没有该字段时，
// 显式 null 没有"删除"对象，保留它只会产生无意义的空字段。
func discardNullValuesFromPatch(patchV interface{}) {
	switch patchV := patchV.(type) {
	case map[string]interface{}:
		for k, v := range patchV {
			if v == nil {
				delete(patchV, k)
			} else {
				discardNullValuesFromPatch(v)
			}
		}
	case []interface{}:
		for _, v := range patchV {
			discardNullValuesFromPatch(v)
		}
	}
}

// mergeMapHandler 根据 fieldPatchStrategy 和 mergeOptions，将 key 对应的 patchV 合并到 original。
//
// replace 策略下直接用 patch 覆盖；其余情况递归 mergeMap。
func mergeMapHandler(original, patch interface{}, schema LookupPatchMeta,
	fieldPatchStrategy string, mergeOptions MergeOptions) (map[string]interface{}, error) {
	typedOriginal, typedPatch, err := mapTypeAssertion(original, patch)
	if err != nil {
		return nil, err
	}

	if fieldPatchStrategy != replaceDirective {
		return mergeMap(typedOriginal, typedPatch, schema, mergeOptions)
	} else {
		return typedPatch, nil
	}
}

// mergeSliceHandler 根据 fieldPatchStrategy、fieldPatchMergeKey、isDeleteList 和 mergeOptions 合并切片。
//
// 删除列表无论字段自身策略如何都按 merge 语义处理，否则无法定位要删除的元素。
func mergeSliceHandler(original, patch interface{}, schema LookupPatchMeta,
	fieldPatchStrategy, fieldPatchMergeKey string, isDeleteList bool, mergeOptions MergeOptions) ([]interface{}, error) {
	typedOriginal, typedPatch, err := sliceTypeAssertion(original, patch)
	if err != nil {
		return nil, err
	}

	//无论字段的 patch strategy 是什么，删除列表的处理方式都相同。
	if fieldPatchStrategy == mergeDirective || isDeleteList {
		return mergeSlice(typedOriginal, typedPatch, schema, fieldPatchMergeKey, mergeOptions, isDeleteList)
	} else {
		return typedPatch, nil
	}
}

// 合并两个切片；Go 中切片深拷贝非常复杂，因此 original 和 patch 都可能被修改。
//
// 标量列表按"并集去重"合并；带 merge key 的 map 列表按 key 匹配后递归合并，
// 最后由 normalizeElementOrder 统一排序。
func mergeSlice(original, patch []interface{}, schema LookupPatchMeta, mergeKey string, mergeOptions MergeOptions, isDeleteList bool) ([]interface{}, error) {
	if len(original) == 0 && len(patch) == 0 {
		return original, nil
	}

	//所有值必须类型相同，且不能是列表。
	t, err := sliceElementType(original, patch)
	if err != nil {
		return nil, err
	}

	var merged []interface{}
	kind := t.Kind()
	//元素不是映射时，合并基本类型切片。
	if kind != reflect.Map {
		if mergeOptions.MergeParallelList && isDeleteList {
			return deleteFromSlice(original, patch), nil
		}
		//未来可增加不去重的 "concat" 模式。
		both := append(original, patch...)
		merged = deduplicateScalars(both)

	} else {
		if mergeKey == "" {
			return nil, fmt.Errorf("cannot merge lists without merge key for %s", schema.Name())
		}

		original, patch, err = mergeSliceWithSpecialElements(original, patch, mergeKey)
		if err != nil {
			return nil, err
		}

		merged, err = mergeSliceWithoutSpecialElements(original, patch, mergeKey, schema, mergeOptions)
		if err != nil {
			return nil, err
		}
	}

	//强制排序。
	var patchItems, serverOnlyItems []interface{}
	if len(mergeKey) == 0 {
		patchItems, serverOnlyItems = partitionPrimitivesByPresentInList(merged, patch)
	} else {
		patchItems, serverOnlyItems, err = partitionMapsByPresentInList(merged, patch, mergeKey)
		if err != nil {
			return nil, err
		}
	}
	return normalizeElementOrder(patchItems, serverOnlyItems, patch, original, mergeKey, kind)
}

// mergeSliceWithSpecialElements 在合并切片前处理带有 directiveMarker 的特殊元素，
// 返回更新后的 original 和已移除特殊元素的 patch。
// original 和 patch 必须是映射切片，调用前必须检查。
//
// 遇到 $patch: replace 时，整个列表以 patch 为准，original 中的内容全部丢弃；
// 遇到 $patch: delete 时按 merge key 从 original 中删除对应元素。
func mergeSliceWithSpecialElements(original, patch []interface{}, mergeKey string) ([]interface{}, []interface{}, error) {
	patchWithoutSpecialElements := []interface{}{}
	replace := false
	for _, v := range patch {
		typedV := v.(map[string]interface{})
		patchType, ok := typedV[directiveMarker]
		if !ok {
			patchWithoutSpecialElements = append(patchWithoutSpecialElements, v)
		} else {
			switch patchType {
			case deleteDirective:
				mergeValue, ok := typedV[mergeKey]
				if ok {
					var err error
					original, err = deleteMatchingEntries(original, mergeKey, mergeValue)
					if err != nil {
						return nil, nil, err
					}
				} else {
					return nil, nil, mergepatch.ErrNoMergeKey(typedV, mergeKey)
				}
			case replaceDirective:
				replace = true
				//继续遍历数组，清除其他 $patch 元素。
			case mergeDirective:
				return nil, nil, fmt.Errorf("merging lists cannot yet be specified in the patch")
			default:
				return nil, nil, mergepatch.ErrBadPatchType(patchType, typedV)
			}
		}
	}
	if replace {
		return patchWithoutSpecialElements, nil, nil
	}
	return original, patchWithoutSpecialElements, nil
}

// 根据 merge key 从合并列表中删除所有匹配条目。
//
// 循环删除而不是一次遍历：original 中可能存在 merge key 重复的非法数据，
// 需要把所有匹配项都清掉。
func deleteMatchingEntries(original []interface{}, mergeKey string, mergeValue interface{}) ([]interface{}, error) {
	for {
		_, originalKey, found, err := findMapInSliceBasedOnKeyValue(original, mergeKey, mergeValue)
		if err != nil {
			return nil, err
		}

		if !found {
			break
		}
		//删除 originalKey 对应的元素。
		original = append(original[:originalKey], original[originalKey+1:]...)
	}
	return original, nil
}

// mergeSliceWithoutSpecialElements 合并不含特殊元素的切片。
// original 和 patch 必须是映射切片，调用前必须检查。
//
// 这是 map 列表合并的主路径：按 merge key 找到 original 中的对应元素后递归 mergeMap，
// 找不到则追加到末尾。
func mergeSliceWithoutSpecialElements(original, patch []interface{}, mergeKey string, schema LookupPatchMeta, mergeOptions MergeOptions) ([]interface{}, error) {
	for _, v := range patch {
		typedV := v.(map[string]interface{})
		mergeValue, ok := typedV[mergeKey]
		if !ok {
			return nil, mergepatch.ErrNoMergeKey(typedV, mergeKey)
		}

		//在 original 中找到相同 merge key 的条目时合并映射，
		//否则追加到 original。
		originalMap, originalKey, found, err := findMapInSliceBasedOnKeyValue(original, mergeKey, mergeValue)
		if err != nil {
			return nil, err
		}

		if found {
			var mergedMaps interface{}
			var err error
			//合并到 original。
			mergedMaps, err = mergeMap(originalMap, typedV, schema, mergeOptions)
			if err != nil {
				return nil, err
			}

			original[originalKey] = mergedMaps
		} else {
			original = append(original, v)
		}
	}
	return original, nil
}

// deleteFromSlice 使用并行列表删除基本类型列表中的条目。
//
// 用 map 做 O(1) 查找；toDeleteMap 的值无实际意义，只充当集合。
func deleteFromSlice(current, toDelete []interface{}) []interface{} {
	toDeleteMap := map[interface{}]interface{}{}
	processed := make([]interface{}, 0, len(current))
	for _, v := range toDelete {
		toDeleteMap[v] = true
	}
	for _, v := range current {
		if _, found := toDeleteMap[v]; !found {
			processed = append(processed, v)
		}
	}
	return processed
}

// 此方法不再因切片中含有非映射元素而 panic。
//
// 返回元素、下标以及是否找到；merge key 值用 == 比较，
// 因此数值类型（int/float64）之间的差异不会被忽略。
func findMapInSliceBasedOnKeyValue(m []interface{}, key string, value interface{}) (map[string]interface{}, int, bool, error) {
	for k, v := range m {
		typedV, ok := v.(map[string]interface{})
		if !ok {
			return nil, 0, false, fmt.Errorf("value for key %v is not a map", k)
		}

		valueToMatch, ok := typedV[key]
		if ok && valueToMatch == value {
			return typedV, k, true, nil
		}
	}

	return nil, 0, false, nil
}

// 此函数按 key 排序 JSON 映射中所有需要合并的列表。
// 测试需要此操作，因为 JSON 中列表顺序有意义，
// 而战略合并补丁中 merge 列表的顺序不重要。
// 排序后可忽略顺序比较补丁后的映射。
func sortMergeListsByName(mapJSON []byte, schema LookupPatchMeta) ([]byte, error) {
	var m map[string]interface{}
	err := json.Unmarshal(mapJSON, &m)
	if err != nil {
		return nil, mergepatch.ErrBadJSONDoc
	}

	newM, err := sortMergeListsByNameMap(m, schema)
	if err != nil {
		return nil, err
	}

	return json.Marshal(newM)
}

// sortMergeListsByNameMap 递归按 mergeKey 排序映射中的 merge 列表。
//
// 并行指令列表（retainKeys、deleteFromPrimitiveList）也一并排序，
// 保证生成的 JSON 可做稳定的字节级比较。
func sortMergeListsByNameMap(s map[string]interface{}, schema LookupPatchMeta) (map[string]interface{}, error) {
	newS := map[string]interface{}{}
	for k, v := range s {
		if k == retainKeysDirective {
			typedV, ok := v.([]interface{})
			if !ok {
				return nil, mergepatch.ErrBadPatchFormatForRetainKeys
			}
			v = sortScalars(typedV)
		} else if strings.HasPrefix(k, deleteFromPrimitiveListDirectivePrefix) {
			typedV, ok := v.([]interface{})
			if !ok {
				return nil, mergepatch.ErrBadPatchFormatForPrimitiveList
			}
			v = sortScalars(typedV)
		} else if strings.HasPrefix(k, setElementOrderDirectivePrefix) {
			_, ok := v.([]interface{})
			if !ok {
				return nil, mergepatch.ErrBadPatchFormatForSetElementOrderList
			}
		} else if k != directiveMarker {
			//映射和切片递归处理。
			switch typedV := v.(type) {
			case map[string]interface{}:
				subschema, _, err := schema.LookupPatchMetadataForStruct(k)
				if err != nil {
					return nil, err
				}
				v, err = sortMergeListsByNameMap(typedV, subschema)
				if err != nil {
					return nil, err
				}
			case []interface{}:
				subschema, patchMeta, err := schema.LookupPatchMetadataForSlice(k)
				if err != nil {
					return nil, err
				}
				_, patchStrategy, err := extractRetainKeysPatchStrategy(patchMeta.GetPatchStrategies())
				if err != nil {
					return nil, err
				}
				if patchStrategy == mergeDirective {
					var err error
					v, err = sortMergeListsByNameArray(typedV, subschema, patchMeta.GetPatchMergeKey(), true)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		newS[k] = v
	}

	return newS, nil
}

// sortMergeListsByNameMap 递归按 mergeKey 排序数组中的 merge 列表。
//
// recurse=false 用于 diffListsOfMaps：只按 merge key 排序当前层，
// 不深入元素内部，避免改变元素内容。
func sortMergeListsByNameArray(s []interface{}, schema LookupPatchMeta, mergeKey string, recurse bool) ([]interface{}, error) {
	if len(s) == 0 {
		return s, nil
	}

	//尚不支持列表嵌套列表。
	t, err := sliceElementType(s)
	if err != nil {
		return nil, err
	}

	//如果元素不是映射：
	if t.Kind() != reflect.Map {
		//元素可能未按顺序合并，因此需要排序。
		return deduplicateAndSortScalars(s), nil
	}

	//元素是映射；如果某个 key 的值是映射或列表，
	//可能需要递归处理。
	newS := []interface{}{}
	for _, elem := range s {
		if recurse {
			typedElem := elem.(map[string]interface{})
			newElem, err := sortMergeListsByNameMap(typedElem, schema)
			if err != nil {
				return nil, err
			}

			newS = append(newS, newElem)
		} else {
			newS = append(newS, elem)
		}
	}

	//排序映射。
	newS = sortMapsBasedOnField(newS, mergeKey)
	return newS, nil
}

// sortMapsBasedOnField 按指定字段的字符串值排序 map 列表；
// 借助 mapSliceFromSlice/sliceFromMapSlice 适配 sort.Interface。
func sortMapsBasedOnField(m []interface{}, fieldName string) []interface{} {
	mapM := mapSliceFromSlice(m)
	ss := SortableSliceOfMaps{mapM, fieldName}
	sort.Sort(ss)
	newS := sliceFromMapSlice(ss.s)
	return newS
}

// mapSliceFromSlice 做类型收窄；元素不是 map 时按类型断言直接 panic，
// 调用前必须已通过 sliceElementType 校验。
func mapSliceFromSlice(m []interface{}) []map[string]interface{} {
	newM := []map[string]interface{}{}
	for _, v := range m {
		vt := v.(map[string]interface{})
		newM = append(newM, vt)
	}

	return newM
}

// sliceFromMapSlice 把排序后的 map 列表转回 []interface{}。
func sliceFromMapSlice(s []map[string]interface{}) []interface{} {
	newS := []interface{}{}
	for _, v := range s {
		newS = append(newS, v)
	}

	return newS
}

// SortableSliceOfMaps 按指定键的字符串形式对 map 列表排序。
// 用 fmt.Sprintf("%v") 是为了兼容任意类型的键值。
type SortableSliceOfMaps struct {
	s []map[string]interface{}
	k string //用于排序的 key。
}

func (ss SortableSliceOfMaps) Len() int {
	return len(ss.s)
}

func (ss SortableSliceOfMaps) Less(i, j int) bool {
	iStr := fmt.Sprintf("%v", ss.s[i][ss.k])
	jStr := fmt.Sprintf("%v", ss.s[j][ss.k])
	return sort.StringsAreSorted([]string{iStr, jStr})
}

func (ss SortableSliceOfMaps) Swap(i, j int) {
	ss.s[i], ss.s[j] = ss.s[j], ss.s[i]
}

// deduplicateAndSortScalars 先就地去重再排序，用于把无序的标量列表规范化。
func deduplicateAndSortScalars(s []interface{}) []interface{} {
	s = deduplicateScalars(s)
	return sortScalars(s)
}

// sortScalars 按字符串表示排序，调用方不得依赖数值语义顺序。
func sortScalars(s []interface{}) []interface{} {
	ss := SortableSliceOfScalars{s}
	sort.Sort(ss)
	return ss.s
}

// deduplicateScalars 就地去重：发现重复时用末尾元素覆盖当前位置并缩短切片。
// 该算法不保序，调用方随后通常会排序。
func deduplicateScalars(s []interface{}) []interface{} {
	//巧妙的去重算法。
	length := len(s) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if s[i] == s[j] {
				s[j] = s[length]
				s = s[0:length]
				length--
				j--
			}
		}
	}

	return s
}

// SortableSliceOfScalars 按 fmt.Sprintf("%v") 的字典序排序标量。
type SortableSliceOfScalars struct {
	s []interface{}
}

func (ss SortableSliceOfScalars) Len() int {
	return len(ss.s)
}

func (ss SortableSliceOfScalars) Less(i, j int) bool {
	iStr := fmt.Sprintf("%v", ss.s[i])
	jStr := fmt.Sprintf("%v", ss.s[j])
	return sort.StringsAreSorted([]string{iStr, jStr})
}

func (ss SortableSliceOfScalars) Swap(i, j int) {
	ss.s[i], ss.s[j] = ss.s[j], ss.s[i]
}

// 返回 N 个切片中元素的类型；类型不同、是切片或未定义时返回错误。
//
// 空切片不贡献类型；全部为空时报错，因为调用方无法据此决定合并策略。
func sliceElementType(slices ...[]interface{}) (reflect.Type, error) {
	var prevType reflect.Type
	for _, s := range slices {
		//遍历所有切片元素，确保类型一致。
		for _, v := range s {
			currentType := reflect.TypeOf(v)
			if prevType == nil {
				prevType = currentType
				//尚不支持列表嵌套列表。
				if prevType.Kind() == reflect.Slice {
					return nil, mergepatch.ErrNoListOfLists
				}
			} else {
				if prevType != currentType {
					return nil, fmt.Errorf("list element types are not identical: %v", fmt.Sprint(slices))
				}
				prevType = currentType
			}
		}
	}

	if prevType == nil {
		return nil, fmt.Errorf("no elements in any of the given slices")
	}

	return prevType, nil
}

// 如果 left 和 right 两个 JSON 对象在任一 key 上重叠且值不同，MergingMapsHaveConflicts 返回 true。
// 所有 key 必须是字符串；同一 Type 的补丁键一致，因此可适用于多种补丁类型。
// 此方法支持战略合并补丁语义。
//
// 仅当同一字段被双方以不同方式修改时才判为冲突；merge 策略的标量列表
// 天然是并集语义，因此不产生冲突。
func MergingMapsHaveConflicts(left, right map[string]interface{}, schema LookupPatchMeta) (bool, error) {
	return mergingMapFieldsHaveConflicts(left, right, schema, "", "")
}

// mergingMapFieldsHaveConflicts 是冲突检测的递归核心。
// 先比较 $patch 指令：一方替换/删除整段而另一方逐字段修改时必为冲突。
func mergingMapFieldsHaveConflicts(
	left, right interface{},
	schema LookupPatchMeta,
	fieldPatchStrategy, fieldPatchMergeKey string,
) (bool, error) {
	switch leftType := left.(type) {
	case map[string]interface{}:
		rightType, ok := right.(map[string]interface{})
		if !ok {
			return true, nil
		}
		leftMarker, okLeft := leftType[directiveMarker]
		rightMarker, okRight := rightType[directiveMarker]
		//如果任一侧带有指令标记，
		//应先处理指令，再检查各个 key，
		//因为指令作用于整个映射。
		if okLeft || okRight {
			//如果只有一侧带有指令标记，
			//则存在冲突：一侧删除或替换整个映射，
			//另一侧只修改单个 key。
			if okLeft != okRight {
				return true, nil
			}
			//如果两侧都有标记但指令不同，
			//则对映射的操作不同，存在冲突。
			if leftMarker != rightMarker {
				return true, nil
			}
		}
		if fieldPatchStrategy == replaceDirective {
			return false, nil
		}
		//检查各个 key。
		return mapsHaveConflicts(leftType, rightType, schema)

	case []interface{}:
		rightType, ok := right.([]interface{})
		if !ok {
			return true, nil
		}
		return slicesHaveConflicts(leftType, rightType, schema, fieldPatchStrategy, fieldPatchMergeKey)
	case string, float64, bool, int64, nil:
		return !reflect.DeepEqual(left, right), nil
	default:
		return true, fmt.Errorf("unknown type: %v", reflect.TypeOf(left))
	}
}

// mapsHaveConflicts 只检查双方都存在的键；$patch 与 $retainKeys 是指令而非字段，跳过。
func mapsHaveConflicts(typedLeft, typedRight map[string]interface{}, schema LookupPatchMeta) (bool, error) {
	for key, leftValue := range typedLeft {
		if key != directiveMarker && key != retainKeysDirective {
			if rightValue, ok := typedRight[key]; ok {
				var subschema LookupPatchMeta
				var patchMeta PatchMeta
				var patchStrategy string
				var err error
				switch leftValue.(type) {
				case []interface{}:
					subschema, patchMeta, err = schema.LookupPatchMetadataForSlice(key)
					if err != nil {
						return true, err
					}
					_, patchStrategy, err = extractRetainKeysPatchStrategy(patchMeta.patchStrategies)
					if err != nil {
						return true, err
					}
				case map[string]interface{}:
					subschema, patchMeta, err = schema.LookupPatchMetadataForStruct(key)
					if err != nil {
						return true, err
					}
					_, patchStrategy, err = extractRetainKeysPatchStrategy(patchMeta.patchStrategies)
					if err != nil {
						return true, err
					}
				}

				if hasConflicts, err := mergingMapFieldsHaveConflicts(leftValue, rightValue,
					subschema, patchStrategy, patchMeta.GetPatchMergeKey()); hasConflicts {
					return true, err
				}
			}
		}
	}

	return false, nil
}

// slicesHaveConflicts 按列表策略判断冲突：
// merge 策略的标量列表是并集语义，不冲突；map 列表先按 merge key 建索引再比较。
func slicesHaveConflicts(
	typedLeft, typedRight []interface{},
	schema LookupPatchMeta,
	fieldPatchStrategy, fieldPatchMergeKey string,
) (bool, error) {
	elementType, err := sliceElementType(typedLeft, typedRight)
	if err != nil {
		return true, err
	}

	if fieldPatchStrategy == mergeDirective {
		//基本类型列表合并按定义不存在冲突，
		//只有元素为映射时才需要继续检查。
		if elementType.Kind() != reflect.Map {
			return false, nil
		}

		//为每个切片构建映射，再比较两个映射。
		leftMap, err := sliceOfMapsToMapOfMaps(typedLeft, fieldPatchMergeKey)
		if err != nil {
			return true, err
		}

		rightMap, err := sliceOfMapsToMapOfMaps(typedRight, fieldPatchMergeKey)
		if err != nil {
			return true, err
		}

		return mapsOfMapsHaveConflicts(leftMap, rightMap, schema)
	}

	//没有类型信息，或这些是不合并的列表。
	if len(typedLeft) != len(typedRight) {
		return true, nil
	}

	//排序基本类型切片以避免顺序问题。
	//无法排序不合并的映射列表。
	if elementType.Kind() != reflect.Map {
		typedLeft = deduplicateAndSortScalars(typedLeft)
		typedRight = deduplicateAndSortScalars(typedRight)
	}

	//按顺序逐元素比较切片。
	//如果切片未排序，此检查会失败。
	for i := range typedLeft {
		if hasConflicts, err := mergingMapFieldsHaveConflicts(typedLeft[i], typedRight[i], schema, "", ""); hasConflicts {
			return true, err
		}
	}

	return false, nil
}

// sliceOfMapsToMapOfMaps 按 merge key 把列表转成 map，键统一用字符串形式，
// 因此重复 merge key 会互相覆盖（正常情况下唯一性由 API 校验保证）。
func sliceOfMapsToMapOfMaps(slice []interface{}, mergeKey string) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(slice))
	for _, value := range slice {
		typedValue, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid element type in merging list:%v", slice)
		}

		mergeValue, ok := typedValue[mergeKey]
		if !ok {
			return nil, fmt.Errorf("cannot find merge key `%s` in merging list element:%v", mergeKey, typedValue)
		}

		result[fmt.Sprintf("%s", mergeValue)] = typedValue
	}

	return result, nil
}

// mapsOfMapsHaveConflicts 逐个键比较两个按 merge key 索引的列表。
func mapsOfMapsHaveConflicts(typedLeft, typedRight map[string]interface{}, schema LookupPatchMeta) (bool, error) {
	for key, leftValue := range typedLeft {
		if rightValue, ok := typedRight[key]; ok {
			if hasConflicts, err := mergingMapFieldsHaveConflicts(leftValue, rightValue, schema, "", ""); hasConflicts {
				return true, err
			}
		}
	}

	return false, nil
}

// CreateThreeWayMergePatch 将修改后的配置与原始配置协调，
// 保留期间对原始配置做出且未被当前配置覆盖的修改和删除。
// 三个文档都必须以 JSON 编码内容传入；任一文档无效、
// 前置条件不满足，或 overwrite 为 false 且 modified 与 current 存在冲突时返回错误。
// 冲突是指某 key 从 original 到 modified 与从 original 到 current 的变化不同，
// 例如删除或修改值的方式不同。original 中不存在但 modified 显式定义的字段也会传递。
//
// 三方 patch 的构造：delta = current->modified（不含删除），
// deletions = original->modified（只含删除），再把 delta 叠加到 deletions 上，
// 这样既保留他人对 current 的修改，又执行本次的删除。
func CreateThreeWayMergePatch(original, modified, current []byte, schema LookupPatchMeta, overwrite bool, fns ...mergepatch.PreconditionFunc) ([]byte, error) {
	originalMap := map[string]interface{}{}
	if len(original) > 0 {
		if err := json.Unmarshal(original, &originalMap); err != nil {
			return nil, mergepatch.ErrBadJSONDoc
		}
	}

	modifiedMap := map[string]interface{}{}
	if len(modified) > 0 {
		if err := json.Unmarshal(modified, &modifiedMap); err != nil {
			return nil, mergepatch.ErrBadJSONDoc
		}
	}

	currentMap := map[string]interface{}{}
	if len(current) > 0 {
		if err := json.Unmarshal(current, &currentMap); err != nil {
			return nil, mergepatch.ErrBadJSONDoc
		}
	}

	//补丁包含 current 到 modified 的非删除差异，以及 original 到 modified 的删除。
	//计算时先求 original 到 modified 的删除，再求 current 到 modified 的非删除差异，
	//然后将 delta 作为补丁应用到 deletions；结果应严格只含新增。
	deltaMapDiffOptions := DiffOptions{
		IgnoreDeletions: true,
		SetElementOrder: true,
	}
	deltaMap, err := diffMaps(currentMap, modifiedMap, schema, deltaMapDiffOptions)
	if err != nil {
		return nil, err
	}
	deletionsMapDiffOptions := DiffOptions{
		SetElementOrder:           true,
		IgnoreChangesAndAdditions: true,
	}
	deletionsMap, err := diffMaps(originalMap, modifiedMap, schema, deletionsMapDiffOptions)
	if err != nil {
		return nil, err
	}

	// MergeOptions 零值：不合并并行列表，因为这里处理的是 patch 而非实际对象。
	mergeOptions := MergeOptions{}
	patchMap, err := mergeMap(deletionsMap, deltaMap, schema, mergeOptions)
	if err != nil {
		return nil, err
	}

	//对补丁应用前置条件，任一条件不满足则返回错误。
	for _, fn := range fns {
		if !fn(patchMap) {
			return nil, mergepatch.NewErrPreconditionFailed(patchMap)
		}
	}

	//如果 overwrite 为 False 且补丁含有被不同修改的 key，
	//则返回冲突错误。
	if !overwrite {
		changeMapDiffOptions := DiffOptions{}
		changedMap, err := diffMaps(originalMap, currentMap, schema, changeMapDiffOptions)
		if err != nil {
			return nil, err
		}

		hasConflicts, err := MergingMapsHaveConflicts(patchMap, changedMap, schema)
		if err != nil {
			return nil, err
		}

		if hasConflicts {
			return nil, mergepatch.NewErrConflict(mergepatch.ToYAMLOrError(patchMap), mergepatch.ToYAMLOrError(changedMap))
		}
	}

	return json.Marshal(patchMap)
}

// ItemAddedToModifiedSlice 判断排序后的 merge key：original 更大意味着
// modified 中新增了该项（modified 指针尚未前进）。
func ItemAddedToModifiedSlice(original, modified string) bool { return original > modified }

// ItemRemovedFromModifiedSlice 判断排序后的 merge key：original 更小意味着
// modified 中删除了该项。
func ItemRemovedFromModifiedSlice(original, modified string) bool { return original < modified }

// ItemMatchesOriginalAndModifiedSlice 判断两侧 merge key 相同。
func ItemMatchesOriginalAndModifiedSlice(original, modified string) bool { return original == modified }

// CreateDeleteDirective 构造列表元素删除指令：{"<mergeKey>": value, "$patch": "delete"}。
func CreateDeleteDirective(mergeKey string, mergeKeyValue interface{}) map[string]interface{} {
	return map[string]interface{}{mergeKey: mergeKeyValue, directiveMarker: deleteDirective}
}

// mapTypeAssertion 同时断言两侧都是 map，失败时返回带实际类型的错误。
func mapTypeAssertion(original, patch interface{}) (map[string]interface{}, map[string]interface{}, error) {
	typedOriginal, ok := original.(map[string]interface{})
	if !ok {
		return nil, nil, mergepatch.ErrBadArgType(typedOriginal, original)
	}
	typedPatch, ok := patch.(map[string]interface{})
	if !ok {
		return nil, nil, mergepatch.ErrBadArgType(typedPatch, patch)
	}
	return typedOriginal, typedPatch, nil
}

// sliceTypeAssertion 同时断言两侧都是 []interface{}。
func sliceTypeAssertion(original, patch interface{}) ([]interface{}, []interface{}, error) {
	typedOriginal, ok := original.([]interface{})
	if !ok {
		return nil, nil, mergepatch.ErrBadArgType(typedOriginal, original)
	}
	typedPatch, ok := patch.([]interface{})
	if !ok {
		return nil, nil, mergepatch.ErrBadArgType(typedPatch, patch)
	}
	return typedOriginal, typedPatch, nil
}

// extractRetainKeysPatchStrategy 处理可能含多个逗号分隔策略的字符串，
// 返回是否包含 retainKeys 以及其他策略。
//
// 仅支持 0/1/2 个策略，且最多一个 retainKeys；其他组合直接报错，
// 避免对未知策略做错误的静默处理。
func extractRetainKeysPatchStrategy(strategies []string) (bool, string, error) {
	switch len(strategies) {
	case 0:
		return false, "", nil
	case 1:
		singleStrategy := strategies[0]
		switch singleStrategy {
		case retainKeysStrategy:
			return true, "", nil
		default:
			return false, singleStrategy, nil
		}
	case 2:
		switch {
		case strategies[0] == retainKeysStrategy:
			return true, strategies[1], nil
		case strategies[1] == retainKeysStrategy:
			return true, strategies[0], nil
		default:
			return false, "", fmt.Errorf("unexpected patch strategy: %v", strategies)
		}
	default:
		return false, "", fmt.Errorf("unexpected patch strategy: %v", strategies)
	}
}

// 如果 original 含有 modified 没有的非 nil 值字段，hasAdditionalNewField 返回 true。
//
// null 值不算"字段存在"，因为 diff 生成时 null 就代表删除，无需 retainKeys 指令。
func hasAdditionalNewField(original, modified map[string]interface{}) bool {
	for k, v := range original {
		if v == nil {
			continue
		}
		if _, found := modified[k]; !found {
			return true
		}
	}
	return false
}
