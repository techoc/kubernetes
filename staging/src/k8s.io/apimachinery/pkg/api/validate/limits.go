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
	"math"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/constraints"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MaxLength 校验 value 的字符数（rune 数，而非字节数）不超过 max。
// 采用两级快速路径避免对长字符串做昂贵的 rune 计数：
// 字节长度不超过 max 时必然合规；字节长度 /4 的最小字符数已超过 max 时必然不合规。
func MaxLength[T ~string](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T, max int) field.ErrorList {
	if value == nil {
		return nil
	}

	// 字节长度不超过 max 时，字符数必然也不超过 max（每个字符至少占 1 字节），
	// 无需再做 rune 计数。
	byteLength := len(*value)
	if byteLength <= max {
		return nil
	}

	// rune 最多占 4 字节，因此实际字符数至少是 len(value)/4（向上取整）。
	// 若这个下限已经超过 max，可以确定超长，从而跳过昂贵的 rune 计数。
	minimum := int(math.Ceil(float64(byteLength) / 4.0))
	if minimum > max || utf8.RuneCountInString(string(*value)) > max {
		return field.ErrorList{field.TooLongCharacters(fldPath, *value, max).WithOrigin("maxLength")}
	}
	return nil
}

// MaxBytes 校验 value 的字节长度不超过 max。value 为 nil 时视为未设置，直接通过。
func MaxBytes[T ~string](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T, max int) field.ErrorList {
	if value == nil {
		return nil
	}

	if len(*value) > max {
		return field.ErrorList{field.TooLong(fldPath, *value, max).WithOrigin("maxBytes")}
	}

	return nil
}

// MaxItems 校验切片元素个数不超过 max。nil 切片长度为 0，天然通过。
func MaxItems[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T, max int) field.ErrorList {
	if len(value) > max {
		return field.ErrorList{field.TooMany(fldPath, len(value), max).WithOrigin("maxItems")}
	}
	return nil
}

// MaxProperties 校验 map 的键数量不超过 max。nil map 视为未设置，直接通过。
func MaxProperties[K comparable, V any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]V, max int) field.ErrorList {
	if value == nil {
		return nil
	}

	if len(value) > max {
		return field.ErrorList{field.TooMany(fldPath, len(value), max).WithOrigin("maxProperties")}
	}
	return nil
}

// MinItems 校验切片元素个数不少于 min。
func MinItems[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T, min int) field.ErrorList {
	if len(value) < min {
		return field.ErrorList{field.TooFew(fldPath, len(value), min).WithOrigin("minItems")}
	}
	return nil
}

// MinProperties 校验 map 的键数量不少于 min。
func MinProperties[K comparable, V any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]V, min int) field.ErrorList {
	if len(value) < min {
		return field.ErrorList{field.TooFew(fldPath, len(value), min).WithOrigin("minProperties")}
	}
	return nil
}

// Minimum 校验整数 value 不小于 min。value 为 nil 时视为未设置，直接通过。
func Minimum[T constraints.Integer](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T, min T) field.ErrorList {
	if value == nil {
		return nil
	}
	if *value < min {
		return field.ErrorList{field.Invalid(fldPath, *value, content.MinError(min)).WithOrigin("minimum")}
	}
	return nil
}

// Maximum 校验整数 value 不大于 max。value 为 nil 时视为未设置，直接通过。
func Maximum[T constraints.Integer](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T, max T) field.ErrorList {
	if value == nil {
		return nil
	}
	if *value > max {
		return field.ErrorList{field.Invalid(fldPath, *value, content.MaxError(max)).WithOrigin("maximum")}
	}
	return nil
}

// MinLength 校验 value 的字符数（rune 数）不少于 min；value 为 nil 时直接通过。
// 同样使用两级快速路径：先看字节长度能否直接给出结论，必要时才做 rune 计数。
func MinLength[T ~string](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T, min int) field.ErrorList {
	if value == nil {
		return nil
	}

	byteLength := len(*value)

	// rune 最多占 4 字节，所以字符数至少是 len(value)/4（向上取整）。
	// 若该下限已不小于 min，可以确定合规，跳过 rune 计数。
	if int(math.Ceil(float64(byteLength)/4.0)) >= min {
		return nil
	}

	// 字节长度小于 min 时字符数必然也小于 min，可直接判定过短；
	// 否则再做 rune 计数确认字符数是否不足。
	if byteLength < min || utf8.RuneCountInString(string(*value)) < min {
		return field.ErrorList{field.TooShort(fldPath, *value, min).WithOrigin("minLength")}
	}
	return nil
}
