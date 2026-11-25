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

package field

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Error 是 'error' 接口的实现，表示字段级校验错误。
type Error struct {
	Type     ErrorType
	Field    string
	BadValue interface{}
	Detail   string

	// Origin 唯一标识该错误产生的来源。测试中用它把期望错误与实际错误进行
	// 比较，而无需依赖精确的 detail 字符串匹配。这样无论错误消息如何格式化
	// 或本地化，测试都能验证是正确的那条校验逻辑触发了错误。
	//
	// 取值应为以下之一：
	// - 简单的 camelCase 标识符（如 "maximum"、"maxItems"）
	// - 针对特定格式校验错误的 "format=<dash-style-identifier>" 结构化格式
	//   （如 "format=k8s-short-name"）
	//
	// 若 Origin 对应已有的声明式校验标签或 JSON Schema 关键字，应沿用同名以保持一致。
	//
	// Origin 应在仍能识别错误唯一来源的最深层校验函数中设置。
	Origin string

	// CoveredByDeclarative 为 true 时表示该错误已被声明式校验覆盖。
	// 该字段用于标识那些同样会被声明式校验捕获的命令式校验错误。
	CoveredByDeclarative bool

	// FromImperative 表示这些错误来源于手写校验。
	FromImperative bool

	// ShortCircuit 表示该错误会阻止对当前字段子字段的进一步校验。
	ShortCircuit bool

	// ShortCircuitedInDeclarative 表示该错误本应被声明式校验覆盖，但由于当前输入
	// 触发短路行为，声明式校验未返回它。
	ShortCircuitedInDeclarative bool

	// ValidationStabilityLevel 表示产生该错误的声明式校验的稳定性级别。
	// 仅应在声明式校验中使用。
	ValidationStabilityLevel ValidationStabilityLevel
}

// ValidationStabilityLevel 表示校验的稳定性级别。
type ValidationStabilityLevel int

const (
	stabilityLevelUnknown ValidationStabilityLevel = iota
	stabilityLevelAlpha
	stabilityLevelBeta
)

// String 返回稳定性级别的可读名称。
func (v ValidationStabilityLevel) String() string {
	switch v {
	case stabilityLevelAlpha:
		return "alpha"
	case stabilityLevelBeta:
		return "beta"
	default:
		return "unknown"
	}
}

var _ error = &Error{}

// IsAlpha 判断该错误是否为 alpha 级别的校验错误。
func (e *Error) IsAlpha() bool {
	return e.ValidationStabilityLevel == stabilityLevelAlpha
}

// IsBeta 判断该错误是否为 beta 级别的校验错误。
func (e *Error) IsBeta() bool {
	return e.ValidationStabilityLevel == stabilityLevelBeta
}

// Error 实现 error 接口。
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.ErrorBody())
}

// OmitValueType 是 BadValue 的哨兵类型，用于在错误消息中省略具体值。
type OmitValueType struct{}

var omitValue = OmitValueType{}

// ErrorBody 返回不含字段名的错误消息。
// 这有助于构建更友好的上层错误报告。
func (e *Error) ErrorBody() string {
	var s string
	switch e.Type {
	case ErrorTypeRequired, ErrorTypeForbidden, ErrorTypeTooLong, ErrorTypeTooShort, ErrorTypeInternal:
		s = e.Type.String()
	case ErrorTypeInvalid, ErrorTypeTypeInvalid, ErrorTypeNotSupported,
		ErrorTypeNotFound, ErrorTypeDuplicate, ErrorTypeTooMany, ErrorTypeTooFew:
		if e.BadValue == omitValue {
			s = e.Type.String()
			break
		}
		switch t := e.BadValue.(type) {
		case int64, int32, float64, float32, bool:
			// 简单类型使用简单格式化。
			s = fmt.Sprintf("%s: %v", e.Type, t)
		case string:
			s = fmt.Sprintf("%s: %q", e.Type, t)
		default:
			// 复杂类型需要更复杂的渲染方式。
			valstr := ""
			jb, err := json.Marshal(e.BadValue)
			if err == nil {
				// 最佳情况：JSON 可序列化。
				valstr = string(jb)
			} else if stringer, ok := e.BadValue.(fmt.Stringer); ok {
				// 定义了 String() 的类型比原始结构体更易读。
				valstr = stringer.String()
			} else {
				// 最差情况：回退到原始结构体。
				//TODO: 内部类型对 json.Marshalling 有 panic 防护，以防止意外将内部类型外部序列化。
				//目前使用 %#v，未来最好提供更具表达力的输出。
				valstr = fmt.Sprintf("%#v", e.BadValue)
			}
			s = fmt.Sprintf("%s: %s", e.Type, valstr)
		}
	default:
		// 未处理的错误码说明代码有缺陷，用内部错误形式报告。
		internal := InternalError(nil, fmt.Errorf("unhandled error code: %s: please report this", e.Type))
		s = internal.ErrorBody()
	}
	if len(e.Detail) != 0 {
		s += fmt.Sprintf(": %s", e.Detail)
	}

	return s
}

// WithOrigin 为 FieldError 添加来源信息。
func (e *Error) WithOrigin(o string) *Error {
	e.Origin = o
	return e
}

// MarkCoveredByDeclarative 把错误标记为已被声明式校验覆盖。
func (e *Error) MarkCoveredByDeclarative() *Error {
	e.CoveredByDeclarative = true
	return e
}

// ErrorType 是机器可读的值，进一步说明字段为何非法。
// 这些值应与 api/types.go 中的 CauseType 一一对应。
type ErrorType string

// TODO: 这些值与 api/types.go 重复，但存在循环依赖；需要消除重复。
const (
	// ErrorTypeNotFound 用于报告找不到请求的值（例如查找 ID）。见 NotFound()。
	ErrorTypeNotFound ErrorType = "FieldValueNotFound"
	// ErrorTypeRequired 用于报告未提供必需值（例如空字符串、null 或空数组）。
	// 见 Required()。
	ErrorTypeRequired ErrorType = "FieldValueRequired"
	// ErrorTypeDuplicate 用于报告必须唯一的值发生冲突（例如唯一 ID）。
	// 见 Duplicate()。
	ErrorTypeDuplicate ErrorType = "FieldValueDuplicate"
	// ErrorTypeInvalid 用于报告格式非法的值（例如正则不匹配、过长、越界）。
	// 见 Invalid()。
	ErrorTypeInvalid ErrorType = "FieldValueInvalid"
	// ErrorTypeNotSupported 用于报告枚举字段的未知取值（例如合法值列表）。
	// 见 NotSupported()。
	ErrorTypeNotSupported ErrorType = "FieldValueNotSupported"
	// ErrorTypeForbidden 用于报告格式合法、在某些条件下可接受，但当前条件
	// （如安全策略）不允许的值。见 Forbidden()。
	ErrorTypeForbidden ErrorType = "FieldValueForbidden"
	// ErrorTypeTooLong 用于报告给定值过长。
	// 它类似 ErrorTypeInvalid，但错误中不会包含过长的值。见 TooLong()。
	ErrorTypeTooLong ErrorType = "FieldValueTooLong"
	// ErrorTypeTooMany 用于报告“过多”。用于表示给定列表项过多。
	// 它类似 FieldValueTooLong，但表示数量而非长度。
	ErrorTypeTooMany ErrorType = "FieldValueTooMany"
	// ErrorTypeTooFew 用于报告“过少”。用于表示给定列表项过少。
	// 它类似 FieldValueTooLong，但表示数量而非长度。
	ErrorTypeTooFew ErrorType = "FieldValueTooFew"
	// ErrorTypeInternal 用于报告与用户输入无关的其他错误。见 InternalError()。
	ErrorTypeInternal ErrorType = "InternalError"
	// ErrorTypeTypeInvalid 表示值与该字段的 schema 类型不匹配。
	ErrorTypeTypeInvalid ErrorType = "FieldValueTypeInvalid"
	// ErrorTypeTooShort 用于报告给定值过短。它类似 ErrorTypeInvalid。见 TooShort()。
	ErrorTypeTooShort ErrorType = "FieldValueTooShort"
)

// String 把 ErrorType 转换为其对应的规范错误消息。
func (t ErrorType) String() string {
	switch t {
	case ErrorTypeNotFound:
		return "Not found"
	case ErrorTypeRequired:
		return "Required value"
	case ErrorTypeDuplicate:
		return "Duplicate value"
	case ErrorTypeInvalid:
		return "Invalid value"
	case ErrorTypeNotSupported:
		return "Unsupported value"
	case ErrorTypeForbidden:
		return "Forbidden"
	case ErrorTypeTooLong:
		return "Too long"
	case ErrorTypeTooMany:
		return "Too many"
	case ErrorTypeTooFew:
		return "Too few"
	case ErrorTypeInternal:
		return "Internal error"
	case ErrorTypeTypeInvalid:
		return "Invalid value"
	case ErrorTypeTooShort:
		return "Too short"
	default:
		return fmt.Sprintf("<unknown error %q>", string(t))
	}
}

// TypeInvalid 返回表示“类型非法”的 *Error。
func TypeInvalid(field *Path, value interface{}, detail string) *Error {
	return &Error{
		Type:     ErrorTypeTypeInvalid,
		Field:    field.String(),
		BadValue: value,
		Detail:   detail,
	}
}

// NotFound 返回表示“值未找到”的 *Error。
// 用于报告找不到请求的值（例如查找 ID）。
func NotFound(field *Path, value interface{}) *Error {
	return &Error{
		Type:     ErrorTypeNotFound,
		Field:    field.String(),
		BadValue: value,
	}
}

// Required 返回表示“缺少必需值”的 *Error。
// 用于报告未提供必需值（例如空字符串、null 或空数组）。
func Required(field *Path, detail string) *Error {
	return &Error{
		Type:     ErrorTypeRequired,
		Field:    field.String(),
		Detail:   detail,
		BadValue: "",
	}
}

// Duplicate 返回表示“值重复”的 *Error。
// 用于报告必须唯一的值发生冲突（例如名称或 ID）。
func Duplicate(field *Path, value interface{}) *Error {
	return &Error{
		Type:     ErrorTypeDuplicate,
		Field:    field.String(),
		BadValue: value,
	}
}

// Invalid 返回表示“值非法”的 *Error。
// 用于报告格式非法的值（例如正则不匹配、过长、越界）。
func Invalid(field *Path, value interface{}, detail string) *Error {
	return &Error{
		Type:     ErrorTypeInvalid,
		Field:    field.String(),
		BadValue: value,
		Detail:   detail,
	}

}

// NotSupported 返回表示“值不受支持”的 *Error。
// 用于报告枚举字段的未知取值（例如合法值列表）。
func NotSupported[T ~string](field *Path, value interface{}, validValues []T) *Error {
	detail := ""
	if len(validValues) > 0 {
		quotedValues := make([]string, len(validValues))
		for i, v := range validValues {
			quotedValues[i] = strconv.Quote(fmt.Sprint(v))
		}
		detail = "supported values: " + strings.Join(quotedValues, ", ")
	}
	return &Error{
		Type:     ErrorTypeNotSupported,
		Field:    field.String(),
		BadValue: value,
		Detail:   detail,
	}
}

// Forbidden 返回表示“禁止”的 *Error。
// 用于报告格式合法、在某些条件下可接受，但当前条件（如安全策略）不允许的值。
func Forbidden(field *Path, detail string) *Error {
	return &Error{
		Type:     ErrorTypeForbidden,
		Field:    field.String(),
		Detail:   detail,
		BadValue: "",
	}
}

// TooLong 返回表示“过长”的 *Error。
// 用于报告给定值过长。它类似 Invalid，但返回的错误不会包含过长的值。
// 若 maxLength 为负，则会在消息中包含该信息。value 参数未被使用。
func TooLong(field *Path, _ interface{}, maxLength int) *Error {
	var msg string
	if maxLength >= 0 {
		bs := "bytes"
		if maxLength == 1 {
			bs = "byte"
		}
		msg = fmt.Sprintf("may not be more than %d %s", maxLength, bs)
	} else {
		msg = "value is too long"
	}
	return &Error{
		Type:     ErrorTypeTooLong,
		Field:    field.String(),
		BadValue: "<value omitted>",
		Detail:   msg,
	}
}

// TooLongCharacters 返回表示“过长”的 *Error。
// 用于报告给定值按字符计（含多字节字符）过长。它类似 Invalid，但返回的错误
// 不会包含过长的值。若 maxLength 为负，则会在消息中包含该信息。value 参数未被使用。
func TooLongCharacters[T ~string](field *Path, _ T, maxLength int) *Error {
	var msg string
	if maxLength >= 0 {
		bs := "characters"
		if maxLength == 1 {
			bs = "character"
		}
		msg = fmt.Sprintf("may not be more than %d %s", maxLength, bs)
	} else {
		msg = "value is too long"
	}
	return &Error{
		Type:     ErrorTypeTooLong,
		Field:    field.String(),
		BadValue: "<value omitted>",
		Detail:   msg,
	}
}

// TooLongMaxLength 返回表示“过长”的 *Error。
// Deprecated: 请改用 TooLong。
func TooLongMaxLength(field *Path, value interface{}, maxLength int) *Error {
	return TooLong(field, "", maxLength)
}

// TooMany 返回表示“过多”的 *Error。
// 用于报告给定列表项过多。它类似 TooLong，但表示数量而非长度。
func TooMany(field *Path, actualQuantity, maxQuantity int) *Error {
	var msg string

	if maxQuantity >= 0 {
		is := "items"
		if maxQuantity == 1 {
			is = "item"
		}
		msg = fmt.Sprintf("must have at most %d %s", maxQuantity, is)
	} else {
		msg = "has too many items"
	}

	var actual interface{}
	// 实际数量未知（负数）时省略该值，避免输出误导性的 -1。
	if actualQuantity >= 0 {
		actual = actualQuantity
	} else {
		actual = omitValue
	}

	return &Error{
		Type:     ErrorTypeTooMany,
		Field:    field.String(),
		BadValue: actual,
		Detail:   msg,
	}
}

// InternalError 返回表示“内部错误”的 *Error。
// 用于表示发现的错误与用户输入无直接关系。err 参数必须非 nil。
func InternalError(field *Path, err error) *Error {
	return &Error{
		Type:     ErrorTypeInternal,
		Field:    field.String(),
		BadValue: err,
		Detail:   err.Error(),
	}
}

// TooShort 返回表示“过短”的 *Error。
// 用于报告给定值按字符计过短。它类似 Invalid。
// 若 minLength 非负，则会在消息中包含该信息。
func TooShort[T ~string](field *Path, value T, minLength int) *Error {
	var msg string
	if minLength >= 0 {
		bs := "characters"
		if minLength == 1 {
			bs = "character"
		}
		msg = fmt.Sprintf("must be at least %d %s", minLength, bs)
	} else {
		msg = "value is too short"
	}
	return &Error{
		Type:     ErrorTypeTooShort,
		Field:    field.String(),
		BadValue: value,
		Detail:   msg,
	}
}

// ErrorList 是一组 Error 的集合。未来可能在同一包中加入非字段错误，
// 但目前没有，因此可以保持简单，把 ErrorList 放在这里。
type ErrorList []*Error

// NewErrorTypeMatcher 返回一个 errors.Matcher：当给定错误是 Error 且
// ErrorType 匹配时返回 true。
func NewErrorTypeMatcher(t ErrorType) utilerrors.Matcher {
	return func(err error) bool {
		if e, ok := err.(*Error); ok {
			return e.Type == t
		}
		return false
	}
}

// WithOrigin 为列表中所有错误设置来源，并返回更新后的列表。
func (list ErrorList) WithOrigin(origin string) ErrorList {
	for _, err := range list {
		err.Origin = origin
	}
	return list
}

// MarkCoveredByDeclarative 把列表中所有错误标记为已被声明式校验覆盖。
func (list ErrorList) MarkCoveredByDeclarative() ErrorList {
	for _, err := range list {
		err.CoveredByDeclarative = true
	}
	return list
}

// PrefixDetail 为列表中所有错误的 Detail 添加前缀，并返回更新后的列表。
func (list ErrorList) PrefixDetail(prefix string) ErrorList {
	for _, err := range list {
		err.Detail = prefix + err.Detail
	}
	return list
}

// ToAggregate 把 ErrorList 转换为 errors.Aggregate，并按错误字符串去重。
func (list ErrorList) ToAggregate() utilerrors.Aggregate {
	if len(list) == 0 {
		return nil
	}
	errs := make([]error, 0, len(list))
	errorMsgs := sets.NewString()
	for _, err := range list {
		msg := fmt.Sprintf("%v", err)
		if errorMsgs.Has(msg) {
			continue
		}
		errorMsgs.Insert(msg)
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

// fromAggregate 把 Aggregate 还原为 ErrorList；要求其中元素均为 *Error。
func fromAggregate(agg utilerrors.Aggregate) ErrorList {
	errs := agg.Errors()
	list := make(ErrorList, len(errs))
	for i := range errs {
		list[i] = errs[i].(*Error)
	}
	return list
}

// Filter 从 ErrorList 中移除匹配任一 matcher 的项。
func (list ErrorList) Filter(fns ...utilerrors.Matcher) ErrorList {
	err := utilerrors.FilterOut(list.ToAggregate(), fns...)
	if err == nil {
		return nil
	}
	// FilterOut 接收 Aggregate 并返回 Aggregate。
	return fromAggregate(err.(utilerrors.Aggregate))
}

// ExtractCoveredByDeclarative 返回仅包含“应被声明式校验覆盖”错误的新 ErrorList。
func (list ErrorList) ExtractCoveredByDeclarative() ErrorList {
	newList := ErrorList{}
	for _, err := range list {
		if err.CoveredByDeclarative {
			newList = append(newList, err)
		}
	}
	return newList
}

// MarkAlpha 把错误标记为 alpha 级别校验错误。
func (e *Error) MarkAlpha() *Error {
	e.ValidationStabilityLevel = stabilityLevelAlpha
	return e
}

// MarkShortCircuitedInDV 标记该错误不会由声明式校验返回，因为声明式校验在
// 运行当前校验前就返回了。手写校验仍会返回它。
func (e *Error) MarkShortCircuitedInDV() *Error {
	e.ShortCircuitedInDeclarative = true
	return e
}

// MarkAlpha 把列表中的错误标记为 alpha 级别校验错误。
func (list ErrorList) MarkAlpha() ErrorList {
	for _, err := range list {
		err.ValidationStabilityLevel = stabilityLevelAlpha
	}
	return list
}

// MarkBeta 把错误标记为 beta 级别校验错误。
func (e *Error) MarkBeta() *Error {
	e.ValidationStabilityLevel = stabilityLevelBeta
	return e
}

// MarkBeta 把列表中的错误标记为 beta 级别校验错误。
func (list ErrorList) MarkBeta() ErrorList {
	for _, err := range list {
		err.ValidationStabilityLevel = stabilityLevelBeta
	}
	return list
}

// MarkFromImperative 把错误标记为来自手写（命令式）校验。
func (e *Error) MarkFromImperative() *Error {
	e.FromImperative = true
	return e
}

// MarkFromImperative 把列表中的错误标记为来自手写（命令式）校验。
func (list ErrorList) MarkFromImperative() ErrorList {
	for _, err := range list {
		err.FromImperative = true
	}
	return list
}

// MarkShortCircuit 把列表中的错误标记为短路错误。
func (list ErrorList) MarkShortCircuit() ErrorList {
	for _, err := range list {
		err.ShortCircuit = true
	}
	return list
}

// RemoveCoveredByDeclarative 返回仅包含“不应被声明式校验覆盖”错误的新 ErrorList。
func (list ErrorList) RemoveCoveredByDeclarative() ErrorList {
	newList := ErrorList{}
	for _, err := range list {
		if !err.CoveredByDeclarative {
			newList = append(newList, err)
		}
	}
	return newList
}

// TooFew 返回表示“过少”的 *Error。
// 用于报告给定列表项过少。它类似 TooLong，但表示数量而非长度。
func TooFew(field *Path, actualQuantity, minQuantity int) *Error {
	var msg string

	if minQuantity >= 0 {
		is := "items"
		if minQuantity == 1 {
			is = "item"
		}
		msg = fmt.Sprintf("must have at least %d %s", minQuantity, is)
	} else {
		msg = "has too few items"
	}

	return &Error{
		Type:     ErrorTypeTooFew,
		Field:    field.String(),
		BadValue: actualQuantity,
		Detail:   msg,
	}
}
