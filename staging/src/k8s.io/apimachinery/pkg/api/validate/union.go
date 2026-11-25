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
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ExtractorFn 从父对象提取值，具体可取字段值或字段是否已设置。
// 注意：obj 可能为 nil，提取器必须处理 nil 对象。
// 并非如此。
//
// 提取器。
type ExtractorFn[T, V any] func(obj T) V

// 两个回调分别对应“一个成员都没设置”和“设置了多个成员”两种非法状态；
// 传 nil 表示该情形不报错，调用方可借此只校验其中一个方向。
type UnionValidationOptions struct {
	// ErrorForEmpty 在未设置任何字段时返回错误；nil 表示无错误。
	ErrorForEmpty func(fldPath *field.Path, allFields []string) *field.Error

	// ErrorForMultiple 在设置多个字段时返回错误；nil 表示无错误。
	ErrorForMultiple func(fldPath *field.Path, specifiedFields []string, allFields []string) *field.Error
}

//	var UnionMembershipForABC := validate.NewUnionMembership(
//	 	validate.NewUnionMember("a"),
//	 	validate.NewUnionMember("b"),
//	 	validate.NewUnionMember("c"),
//	 )
//	func ValidateABC(ctx context.Context, op operation.Operation, fldPath *field.Path, in *ABC) (errs field.ErrorList) {
//		errs = append(errs, Union(ctx, op, fldPath, in, oldIn, UnionMembershipForABC,
//			func(in *ABC) bool { return in.A != nil },
//			func(in *ABC) bool { return in.B != "" },
//			func(in *ABC) bool { return in.C != 0 },
//		)...)
//		return errs
//	}
//
// op/oldObj 只用于更新时的 ratcheting：成员设置情况未变化就跳过重新校验，
// 避免历史对象因新的校验规则而无法更新。isSetFns 的顺序必须与 union.members
// 一一对应，否则返回 InternalError 而不是 panic；上下文参数目前未使用。
func Union[T any](_ context.Context, op operation.Operation, fldPath *field.Path, obj, oldObj T, union *UnionMembership, isSetFns ...ExtractorFn[T, bool]) field.ErrorList {
	// 默认规则：必须有且仅有一个成员被设置。错误值只暴露字段名集合，
	// 不携带实际数据，避免把用户对象内容写进错误信息。
	options := UnionValidationOptions{
		ErrorForEmpty: func(fldPath *field.Path, allFields []string) *field.Error {
			return field.Invalid(fldPath, "",
				fmt.Sprintf("must specify one of: %s", strings.Join(allFields, ", ")))
		},
		ErrorForMultiple: func(fldPath *field.Path, specifiedFields []string, allFields []string) *field.Error {
			return field.Invalid(fldPath, fmt.Sprintf("{%s}", strings.Join(specifiedFields, ", ")),
				fmt.Sprintf("must specify exactly one of: %s", strings.Join(allFields, ", ")))
		},
	}

	return unionValidate(op, fldPath, obj, oldObj, union, options, isSetFns...).WithOrigin("union")
}

//	var UnionMembershipForABC = validate.NewDiscriminatedUnionMembership("type",
//	 	validate.NewDiscriminatedUnionMember("a", "A"),
//	 	validate.NewDiscriminatedUnionMember("b", "B"),
//	 	validate.NewDiscriminatedUnionMember("c", "C"),
//	)
//	func ValidateABC(ctx context.Context, op operation.Operation, fldPath *field.Path, in *ABC) (errs field.ErrorList) {
//		errs = append(errs, DiscriminatedUnion(ctx, op, fldPath, in, oldIn, UnionMembershipForABC,
//			func(in *ABC) string { return string(in.Type) },
//			func(in *ABC) bool { return in.A != nil },
//			func(in *ABC) bool { return in.B != "" },
//			func(in *ABC) bool { return in.C != 0 },
//		)...)
//		return errs
//	}
//
// 与 Union 不同，这里校验的是“被设置的成员是否与判别器取值匹配”：
// 判别器指向某成员时该成员必须存在，其余成员不得存在。判别器取值本身
// 是否合法不在此处判断，需由调用方另行校验。
func DiscriminatedUnion[T any, D ~string](_ context.Context, op operation.Operation, fldPath *field.Path, obj, oldObj T, union *UnionMembership, discriminatorExtractor ExtractorFn[T, D], isSetFns ...ExtractorFn[T, bool]) (errs field.ErrorList) {
	// 数量不匹配属于调用方编程错误，用 InternalError 暴露，而不是静默截断。
	if len(union.members) != len(isSetFns) {
		return field.ErrorList{
			field.InternalError(fldPath,
				fmt.Errorf("number of extractors (%d) does not match number of union members (%d)",
					len(isSetFns), len(union.members))),
		}
	}
	// T 可能是不可比较的 struct，故用反射判断 oldObj 是否为零值；
	// 零值意味着没有可 ratchet 的旧对象（如 Create 场景）。
	hasOldValue := !reflect.ValueOf(oldObj).IsZero() // 因为 T 是 any，而非 comparable。
	var changed bool
	discriminatorValue := discriminatorExtractor(obj)
	if op.Type == operation.Update {
		oldDiscriminatorValue := discriminatorExtractor(oldObj)
		changed = discriminatorValue != oldDiscriminatorValue
	}

	for i, fieldIsSet := range isSetFns {
		member := union.members[i]
		isDiscriminatedMember := string(discriminatorValue) == member.discriminatorValue
		newIsSet := fieldIsSet(obj)
		// 只有在判别器尚未变化时才需要比较成员，尽量少做提取。
		if op.Type == operation.Update && !changed {
			oldIsSet := fieldIsSet(oldObj)
			changed = changed || newIsSet != oldIsSet
		}
		// 两个方向的违规：非目标成员被设置，或目标成员缺失。
		if newIsSet && !isDiscriminatedMember {
			errs = append(errs, field.Invalid(fldPath.Child(member.fieldName), "",
				fmt.Sprintf("may only be specified when `%s` is %q", union.discriminatorName, member.discriminatorValue)))
		} else if !newIsSet && isDiscriminatedMember {
			errs = append(errs, field.Invalid(fldPath.Child(member.fieldName), "",
				fmt.Sprintf("must be specified when `%s` is %q", union.discriminatorName, discriminatorValue)))
		}
	}
	// 联合判别器与成员关系未变时，无需重新校验。
	// 重新校验。
	if op.Type == operation.Update && hasOldValue && !changed {
		return nil
	}
	return errs.WithOrigin("union")
}

// 字段不导出：成员只能通过构造函数创建，以保证 fieldName 与
// discriminatorValue 的组合在所有校验器中保持一致。
type UnionMember struct {
	fieldName          string
	discriminatorValue string // 仅判别器联合使用；普通联合为空串
}

// NewUnionMember 为指定字段名创建 UnionMember。
func NewUnionMember(fieldName string) UnionMember {
	return UnionMember{fieldName: fieldName}
}

// NewDiscriminatedUnionMember 为指定字段名与判别值创建 UnionMember。
func NewDiscriminatedUnionMember(fieldName, discriminatorValue string) UnionMember {
	return UnionMember{fieldName: fieldName, discriminatorValue: discriminatorValue}
}

// 顺序很重要：Union/DiscriminatedUnion 使用位置下标把 isSetFns 与
// members 关联起来，因此两边的顺序必须严格一致。
type UnionMembership struct {
	discriminatorName string
	members           []UnionMember
}

// 唯一性由调用方保证；校验器不会去重，重复成员会导致重复校验与重复报错。
func NewUnionMembership(member ...UnionMember) *UnionMembership {
	return NewDiscriminatedUnionMembership("", member...)
}

// NewDiscriminatedUnionMembership 为指定判别字段与成员列表创建 UnionMembership；成员提供方式同 NewUnionMembership。
func NewDiscriminatedUnionMembership(discriminatorFieldName string, members ...UnionMember) *UnionMembership {
	return &UnionMembership{
		discriminatorName: discriminatorFieldName,
		members:           members,
	}
}

// 名称用反引号包裹，便于在错误信息中区分字段名与普通文字。
func (u UnionMembership) allFields() []string {
	memberNames := make([]string, 0, len(u.members))
	for _, f := range u.members {
		memberNames = append(memberNames, fmt.Sprintf("`%s`", f.fieldName))
	}
	return memberNames
}

// unionValidate 是 Union 及需要自定义空/多成员错误信息的调用方的共享实现。
func unionValidate[T any](op operation.Operation, fldPath *field.Path,
	obj, oldObj T, union *UnionMembership, options UnionValidationOptions, isSetFns ...ExtractorFn[T, bool],
) field.ErrorList {
	// 提取器与成员数量不一致说明调用方传参有误，无法继续可靠校验。
	if len(union.members) != len(isSetFns) {
		return field.ErrorList{
			field.InternalError(fldPath,
				fmt.Errorf("number of extractors (%d) does not match number of union members (%d)",
					len(isSetFns), len(union.members))),
		}
	}

	hasOldValue := !reflect.ValueOf(oldObj).IsZero() // 因为 T 是 any，而非 comparable。
	var specifiedFields []string
	var changed bool
	for i, fieldIsSet := range isSetFns {
		newIsSet := fieldIsSet(obj)
		// 更新时只在第一次发现差异前比较旧值，一旦 changed 即短路。
		if op.Type == operation.Update && !changed {
			oldIsSet := fieldIsSet(oldObj)
			changed = changed || newIsSet != oldIsSet
		}
		if newIsSet {
			specifiedFields = append(specifiedFields, union.members[i].fieldName)
		}
	}

	// 联合成员关系未变时，无需重新校验。
	if op.Type == operation.Update && hasOldValue && !changed {
		return nil
	}

	var errs field.ErrorList

	// 两个条件互斥，但为可读性保留独立分支；回调为 nil 表示该类错误被禁用。
	if len(specifiedFields) > 1 && options.ErrorForMultiple != nil {
		errs = append(errs, options.ErrorForMultiple(fldPath, specifiedFields, union.allFields()))
	}

	if len(specifiedFields) == 0 && options.ErrorForEmpty != nil {
		errs = append(errs, options.ErrorForEmpty(fldPath, union.allFields()))
	}

	return errs
}
