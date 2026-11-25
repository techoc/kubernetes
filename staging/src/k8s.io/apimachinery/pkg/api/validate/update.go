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

// 中文说明：UPDATE 操作允许的变更类型，供各类 Update* 函数共享。
type UpdateConstraint int

const (
	// 禁止从“未设置”变为“已设置”。
	NoSet UpdateConstraint = iota
	// 禁止从“已设置”变为“未设置”。
	NoUnset
	// 禁止修改取值；set/unset 的转换交给 NoSet/NoUnset 单独约束。
	NoModify
	// 禁止向 slice/map 新增元素。
	NoAddItem
	// 禁止从 slice/map 删除元素。
	NoRemoveItem
)

// 中文说明：适用于可用 == 比较的标量值；零值被视为“未设置”。
func UpdateValueByCompare[T comparable](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// oldValue 为 nil 表示没有旧值可比较（如 +k8s:eachVal 作用域的新条目），无需检查状态转换。
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
			// 中文补充：两端都非零时才判定为“修改”，避免把 set/unset 误报为修改。
			if *oldValue != zero && *value != zero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

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
			// 依赖 validation ratcheting 检测值是否变化。
			// 这里只确认字段在旧新对象中均非 nil，即属于修改而非设置/取消设置。
			//
			if oldValue != nil && value != nil {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

// 中文说明：slice/map/struct 等不可用 == 比较的类型，用 semantic DeepEqual 判断是否为零值。
func UpdateValueByReflect[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// oldValue 为 nil 表示没有旧值可比较（如 +k8s:eachVal 作用域的新条目），无需检查状态转换。
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
			// 依赖 validation ratcheting 检测值是否变化。
			// 这里只确认字段在旧新对象中均已设置，即属于修改而非设置/取消设置。
			//
			if !oldValueIsZero && !valueIsZero {
				errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
			}
		}
	}

	return errs
}

// 中文说明：非指针结构体无法表达“未设置”，因此只可能触发 NoModify。
func UpdateStruct[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T, constraints ...UpdateConstraint) field.ErrorList {
	// oldValue 为 nil 表示没有旧值可比较（如 +k8s:eachVal 作用域的新条目），无需检查状态转换。
	if op.Type != operation.Update || oldValue == nil {
		return nil
	}

	var errs field.ErrorList

	for _, constraint := range constraints {
		switch constraint {
		case NoSet, NoUnset:
			// 这些约束不适用于非指针结构体，因为它们无法取消设置；此限制应在生成阶段检查。
			//
			continue
		case NoModify:
			// 非指针结构体始终视为已设置，因此 ratcheting 检测到的任何变化都是修改。
			// 深相等检查冗余，已移除。
			//
			errs = append(errs, field.Invalid(fldPath, nil, "field cannot be modified once set").WithOrigin("update"))
		}
	}

	return errs
}

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
					// nil 项应由 PtrSliceNoNils 校验，这里跳过。
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
