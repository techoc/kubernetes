/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// UpdateConstraint represents a constraint on update operations
// 中文说明：UPDATE 操作允许的变更类型，供各类 Update* 函数共享。
type UpdateConstraint int

const (
	// NoSet prevents unset->set transitions
	// 禁止从“未设置”变为“已设置”。
	NoSet UpdateConstraint = iota
	// NoUnset prevents set->unset transitions
	// 禁止从“已设置”变为“未设置”。
	NoUnset
	// NoModify prevents value changes but allows set/unset transitions
	// 禁止修改取值；set/unset 的转换交给 NoSet/NoUnset 单独约束。
	NoModify
	// NoAddItem prevents adding items to a slice or map
	// 禁止向 slice/map 新增元素。
	NoAddItem
	// NoRemoveItem prevents removing items from a slice or map
	// 禁止从 slice/map 删除元素。
	NoRemoveItem
)

// UpdateValueByCompare verifies update constraints for comparable value types.
// 中文说明：适用于可用 == 比较的标量值；零值被视为“未设置”。
func UpdateValueByCompare[T comparable](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// nil oldValue means no prior value to compare against (eg: a new item at +k8s:eachVal scope) -> no transition to check.
	if op.Type != operation.Update || oldValue == nil {
		return nil
	}

	var errs field.ErrorList
	var zero T

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			// 旧值为零、新值非零，说明本次更新把字段“设置”了。
			if *oldValue == zero && *value != zero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			// 旧值非零、新值为零，说明字段被“清空”。
			if *oldValue != zero && *value == zero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoModify:
			// Rely on validation ratcheting to detect that the value has changed.
			// This check only verifies that the field was set in both the old and
			// new objects, confirming it was a modification, not a set/unset.
			// 中文补充：两端都非零时才判定为“修改”，避免把 set/unset 误报为修改。
			if *oldValue != zero && *value != zero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

// UpdatePointer verifies update constraints for pointer types.
// 中文说明：指针的 nil 直接表示“未设置”，比零值判断更精确。
func UpdatePointer[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	if op.Type != operation.Update {
		return nil
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			if oldValue == nil && value != nil {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			if oldValue != nil && value == nil {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoModify:
			// Rely on validation ratcheting to detect that the value has changed.
			// This check only verifies that the field was non-nil in both the old
			// and new objects, confirming it was a modification, not a set/unset.
			if oldValue != nil && value != nil {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

// UpdateValueByReflect verifies update constraints for non-comparable value types using reflection.
// 中文说明：slice/map/struct 等不可用 == 比较的类型，用 semantic DeepEqual 判断是否为零值。
func UpdateValueByReflect[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// nil oldValue means no prior value to compare against (eg: a new item at +k8s:eachVal scope) -> no transition to check.
	if op.Type != operation.Update || oldValue == nil {
		return nil
	}

	var errs field.ErrorList
	var zero T
	// 预先计算新旧值是否为零值，避免在约束循环中重复做开销较大的反射比较。
	valueIsZero := equality.Semantic.DeepEqual(*value, zero)
	oldValueIsZero := equality.Semantic.DeepEqual(*oldValue, zero)

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			if oldValueIsZero && !valueIsZero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			if !oldValueIsZero && valueIsZero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoModify:
			// Rely on validation ratcheting to detect that the value has changed.
			// This check only verifies that the field was set in both the old and
			// new objects, confirming it was a modification, not a set/unset.
			if !oldValueIsZero && !valueIsZero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

// UpdateStruct verifies update constraints for non-pointer struct types.
// Non-pointer structs are always considered "set" and never "unset".
// 中文说明：非指针结构体无法表达“未设置”，因此只可能触发 NoModify。
func UpdateStruct[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// nil oldValue means no prior value to compare against (eg: a new item at +k8s:eachVal scope) -> no transition to check.
	if op.Type != operation.Update || oldValue == nil {
		return nil
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet, NoUnset:
			// These constraints don't apply to non-pointer structs
			// as they can't be unset. This should be caught at generation time.
			continue
		case NoModify:
			// Non-pointer structs are always considered "set". Therefore, any
			// change detected by validation ratcheting is a modification.
			// The deep equality check is redundant and has been removed.
			errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
		}
	}

	return errs
}

// ValSliceUpdate verifies update constraints for slices of values.
// NoAddItem and NoRemoveItem use the match function to find corresponding
// elements between value and oldValue. NoSet and NoUnset treat len == 0 as
// "unset".
//
// The match function will never be called with nil arguments.
// 中文说明：元素按值存储；用 match 在两个 slice 间做元素配对，判定是新增还是删除。
func ValSliceUpdate[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue []T, match MatchFunc[*T], constraints ...UpdateConstraint) field.ErrorList {
	if op.Type != operation.Update {
		return nil
	}

	// 没有 match 函数就无法判断元素对应关系，只能拒绝这类约束而不是静默放过。
	if match == nil && (slices.Contains(constraints, NoAddItem) || slices.Contains(constraints, NoRemoveItem)) {
		return field.ErrorList{field.InternalError(fldPath, fmt.Errorf("ValSliceUpdate: NoAddItem/NoRemoveItem require a non-nil match function"))}
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			// 空 slice 到非空 slice 视为“设置”；注意无法区分未指定与空。
			if len(oldValue) == 0 && len(value) > 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			if len(oldValue) > 0 && len(value) == 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoAddItem:
			// 新 slice 中每个元素都必须能在旧 slice 中找到对应项，否则即为新增。
			for i := range value {
				newItem := &value[i]
				if lookup(oldValue, newItem, match) == nil {
					errs = append(errs, field.Forbidden(fldPath.Index(i), "item may not be added").WithOrigin("update"))
				}
			}
		case NoRemoveItem:
			// 反向检查：旧 slice 的每个元素都必须仍存在于新 slice。
			for i := range oldValue {
				oldItem := &oldValue[i]
				if lookup(value, oldItem, match) == nil {
					errs = append(errs, field.Forbidden(fldPath, "item may not be removed").WithOrigin("update"))
				}
			}
		}
	}

	return errs
}

// PtrSliceUpdate verifies update constraints for slices of pointers.
// NoAddItem and NoRemoveItem use the match function to find corresponding
// elements between value and oldValue. NoSet and NoUnset treat len == 0 as
// "unset".
//
// The match function will never be called with nil arguments.
// 中文说明：与 ValSliceUpdate 相同，但元素为指针；nil 元素由 PtrSliceNoNils 负责校验，
// 这里跳过，以免 match 函数收到 nil。
func PtrSliceUpdate[T any](ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue []*T, match MatchFunc[*T], constraints ...UpdateConstraint) field.ErrorList {
	if op.Type != operation.Update {
		return nil
	}

	if match == nil && (slices.Contains(constraints, NoAddItem) || slices.Contains(constraints, NoRemoveItem)) {
		return field.ErrorList{field.InternalError(fldPath, fmt.Errorf("PtrSliceUpdate: NoAddItem/NoRemoveItem require a non-nil match function"))}
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			if len(oldValue) == 0 && len(value) > 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			if len(oldValue) > 0 && len(value) == 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoAddItem:
			for i := range value {
				newItem := value[i]
				if newItem == nil {
					// Ignore nil items; they are supposed to have been checked by PtrSliceNoNils.
					continue
				}
				if lookupPointer(oldValue, newItem, match) == nil {
					errs = append(errs, field.Forbidden(fldPath.Index(i), "item may not be added").WithOrigin("update"))
				}
			}
		case NoRemoveItem:
			for i := range oldValue {
				oldItem := oldValue[i]
				if oldItem == nil {
					continue
				}
				if lookupPointer(value, oldItem, match) == nil {
					errs = append(errs, field.Forbidden(fldPath, "item may not be removed").WithOrigin("update"))
				}
			}
		}
	}

	return errs
}

// UpdateMap verifies update constraints for map types.
// NoAddItem and NoRemoveItem compare keys between value and oldValue. NoSet
// and NoUnset treat len == 0 as "unset".
// 中文说明：只比较键集合，值的变化不在本函数职责内。
func UpdateMap[K comparable, V any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue map[K]V, constraints ...UpdateConstraint) field.ErrorList {
	if op.Type != operation.Update {
		return nil
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet:
			if len(oldValue) == 0 && len(value) > 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be set once created").WithOrigin("update"))
			}
		case NoUnset:
			if len(oldValue) > 0 && len(value) == 0 {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be cleared once set").WithOrigin("update"))
			}
		case NoAddItem:
			// 新 map 中出现旧 map 没有的键，即为新增。
			for k := range value {
				if _, ok := oldValue[k]; !ok {
					errs = append(errs, field.Forbidden(fldPath.Key(fmt.Sprintf("%v", k)), "item may not be added").WithOrigin("update"))
				}
			}
		case NoRemoveItem:
			// 旧 map 的键在新 map 中消失，即为删除。
			for k := range oldValue {
				if _, ok := value[k]; !ok {
					errs = append(errs, field.Forbidden(fldPath.Key(fmt.Sprintf("%v", k)), "item may not be removed").WithOrigin("update"))
				}
			}
		}
	}

	return errs
}
