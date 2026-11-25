/*
Copyright 2024 The Kubernetes Authors.

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

package validate

import (
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MatchItemFunc 接受条目并判断是否匹配条件，应定位列表内的唯一键。
type MatchItemFunc[T any] func(T) bool

// ValSliceItem 查找新列表中首个匹配项，并在旧列表中查找对应项；equiv 确认值未变时跳过校验，否则调用 itemValidator。
// 仅处理首个匹配项，假设 match 定位唯一主键且每个列表至多匹配一个元素；违反此假设时，列表重排会改变行为。
// match/equiv 不会收到 nil，校验路径指向该项在新列表中的索引；已移除条目不校验。
func ValSliceItem[TList ~[]TItem, TItem any](
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	newList, oldList TList,
	match MatchItemFunc[*TItem],
	equiv MatchFunc[*TItem],
	itemValidator func(ctx context.Context, op operation.Operation, fldPath *field.Path, newObj, oldObj *TItem) field.ErrorList,
) field.ErrorList {
	var matchedNew, matchedOld *TItem
	var newIndex int

	// 仅处理首个匹配项；后续重复项按设计忽略。
	for i := range newList {
		if match(&newList[i]) {
			matchedNew = &newList[i]
			newIndex = i
			break
		}
	}
	// 新列表中不存在该条目，无需校验。
	if matchedNew == nil {
		return nil
	}

	// 在旧列表中查找同一条目，以支持 ratcheting。
	for i := range oldList {
		if match(&oldList[i]) {
			matchedOld = &oldList[i]
			break
		}
	}

	// Ratcheting：更新时未变条目跳过校验，避免既有数据被更严格的新规则拒绝。
	//
	if op.Type == operation.Update && matchedOld != nil && equiv(matchedNew, matchedOld) {
		return nil
	}

	return itemValidator(ctx, op, fldPath.Index(newIndex), matchedNew, matchedOld)
}

// PtrSliceItem 是 ValSliceItem 的指针版本，查找、ratcheting 与校验语义相同；仅处理首个匹配项。
// 假设 match 定位唯一主键，违反时列表重排会改变行为；match/equiv 不会收到 nil，已移除条目不校验。
func PtrSliceItem[TList ~[]*TItem, TItem any](
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	newList, oldList TList,
	match MatchItemFunc[*TItem],
	equiv MatchFunc[*TItem],
	itemValidator func(ctx context.Context, op operation.Operation, fldPath *field.Path, newObj, oldObj *TItem) field.ErrorList,
) field.ErrorList {
	var matchedNew, matchedOld *TItem
	var newIndex int

	// 此处容忍 nil 元素；PtrSliceNoNils 会另行报告。
	for i := range newList {
		if newList[i] == nil {
			// nil 项应由 PtrSliceNoNils 校验，这里跳过。
			continue
		}
		if match(newList[i]) {
			matchedNew = newList[i]
			newIndex = i
			break
		}
	}
	if matchedNew == nil {
		return nil
	}

	// 旧列表中的 nil 项无法匹配，直接跳过。
	for i := range oldList {
		if oldList[i] == nil {
			continue
		}
		if match(oldList[i]) {
			matchedOld = oldList[i]
			break
		}
	}

	// 与 ValSliceItem 相同的棘轮规则：未改变的元素跳过校验。
	if op.Type == operation.Update && matchedOld != nil && equiv(matchedNew, matchedOld) {
		return nil
	}

	return itemValidator(ctx, op, fldPath.Index(newIndex), matchedNew, matchedOld)
}
