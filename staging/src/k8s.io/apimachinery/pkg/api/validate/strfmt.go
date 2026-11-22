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
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// 本文件集中实现各类"字符串格式"校验（名称、标签、UUID 等）。
// 这些校验只做格式检查，不负责必填/可选与长度语义之外的判断；
// 因此调用方若需要更精确的错误（如 required、maxLength），需自行补充校验。
const (
	uuidErrorMessage              = "must be a lowercase UUID in 8-4-4-4-12 format"
	defaultResourceRequestsPrefix = "requests."
	// Default namespace prefix.
	resourceDefaultNamespacePrefix = "kubernetes.io/"
	resourceDeviceMaxLength        = 32
)

// ShortName 校验 value 是否为合法的 "short name"（有时也称为 "DNS label"）：
//   - 不能为空
//   - 长度必须小于 64 个字符
//   - 必须以小写字母或数字开头和结尾
//   - 只能包含小写字母、数字或连字符
//
// 本函数返回的所有错误都是 "invalid" 类型。若调用方需要更精确的错误
// （如 required/optional、max-length），需自行补充校验。
func ShortName[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsDNS1123Label((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-short-name"))
	}
	return allErrs
}

// LongName 校验 value 是否为合法的 "long name"（有时也称为 "DNS subdomain"）：
//   - 不能为空
//   - 长度必须小于 254 个字符
//   - 以点分隔的每一段都必须以小写字母或数字开头和结尾
//   - 每一段只能包含小写字母、数字或连字符
//
// 本函数返回的所有错误都是 "invalid" 类型。若调用方需要更精确的错误
// （如 required/optional、max-length），需自行补充校验。
func LongName[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsDNS1123Subdomain((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-long-name"))
	}
	return allErrs
}

// LabelKey 校验 value 是否为合法的 label key。
// label key 由可选前缀与名称组成，二者以 '/' 分隔。
// 名称部分必填，且必须：
//   - 不超过 63 个字符
//   - 以字母或数字（[a-z0-9A-Z]）开头和结尾
//   - 只包含字母、数字、连字符（-）、下划线（_）或点（.）
//
// 前缀可选，且必须：
//   - 是合法的 DNS subdomain
//   - 不超过 253 个字符
func LabelKey[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsLabelKey((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-label-key"))
	}
	return allErrs
}

// PrefixedLabelKey 校验 value 是否为带域前缀的合法 label key。
// 带前缀的 label key 由前缀与名称组成，二者以 '/' 分隔。
// 名称部分必填，且必须：
//   - 不超过 63 个字符
//   - 以字母或数字（[a-z0-9A-Z]）开头和结尾
//   - 只包含字母、数字、连字符（-）、下划线（_）或点（.）
//
// 前缀必须：
//   - 是合法的 DNS subdomain
//   - 不超过 253 个字符
func PrefixedLabelKey[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsPrefixedLabelKey((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-prefixed-label-key"))
	}
	return allErrs
}

// LongNameCaseless 校验 value 是否为合法但大小写不敏感的 "long name"
// （有时也称为 "DNS subdomain"）：
//   - 不能为空
//   - 长度必须小于 254 个字符
//   - 以点分隔的每一段都必须以字母或数字开头和结尾
//   - 每一段只能包含字母、数字或连字符
//
// Deprecated: 大小写不敏感的名称容易产生歧义（例如 'Foo'、'FOO' 与 'foo' 都会被当作 foo 的合法名称），
// 因此不推荐使用；需要严格的小写校验请改用 LongName。
func LongNameCaseless[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsDNS1123SubdomainCaseless((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-long-name-caseless"))
	}
	return allErrs
}

// LabelValue 校验 value 是否为合法的 label value：
//   - 可以为空
//   - 不超过 63 个字符
//   - 以字母或数字开头和结尾
//   - 只包含字母、数字、连字符、下划线或点
func LabelValue[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsLabelValue((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-label-value"))
	}
	return allErrs
}

// PathSegmentName 校验 value 是否为合法的路径段名称。
// 合法的路径段名称可以安全地作为 URL 或文件路径中的一段进行编码。
//   - 不能恰好是 "." 或 ".."
//   - 不能包含 "/"（正斜杠）
//   - 不能包含 "%"（百分号）
//   - 可以包含其他任意字符，包括大小写混合字母、数字、点、连字符、下划线以及非 ASCII 字符
func PathSegmentName[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, msg := range content.IsPathSegmentName((string)(*value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, msg).WithOrigin("format=k8s-path-segment-name"))
	}
	return allErrs
}

// UUID 校验 value 是否为合法的 UUID（RFC 4122）：
//   - 长度必须为 36 个字符
//   - 必须采用规范形式 `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
//   - 只能使用小写十六进制字符
func UUID[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	val := (string)(*value)
	// 长度不符时无需逐字符检查，直接返回统一错误。
	if len(val) != 36 {
		return field.ErrorList{field.Invalid(fldPath, val, uuidErrorMessage).WithOrigin("format=k8s-uuid")}
	}
	for idx := 0; idx < len(val); idx++ {
		character := val[idx]
		switch idx {
		// 8-4-4-4-12 格式中连字符固定出现在这四个位置。
		case 8, 13, 18, 23:
			if character != '-' {
				return field.ErrorList{field.Invalid(fldPath, val, uuidErrorMessage).WithOrigin("format=k8s-uuid")}
			}
		default:
			// 其余位置必须是小写十六进制字符（大写会被拒绝，以保持规范化形式）。
			if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
				return field.ErrorList{field.Invalid(fldPath, val, uuidErrorMessage).WithOrigin("format=k8s-uuid")}
			}
		}
	}
	return nil
}

// ResourcePoolName 校验 value 是否为一个或多个合法 "long name" 段以 '/' 分隔组成，
// 且总长度不超过 253 个字符。
func ResourcePoolName[T ~string](ctx context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	val := (string)(*value)
	var allErrs field.ErrorList
	if len(val) > 253 {
		allErrs = append(allErrs, field.TooLong(fldPath, val, 253))
	}
	// 逐段复用 LongName 校验；空段单独报错，避免底层校验产生难以定位的通用信息。
	parts := strings.Split(val, "/")
	for i, part := range parts {
		if len(part) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, val, fmt.Sprintf("segment %d: must not be empty", i)))
			continue
		}
		// 注意：这里用带段号的前缀覆盖底层 LongName 校验留下的 origin，
		// 以便错误信息能指明是第几段出错。
		allErrs = append(allErrs, LongName(ctx, op, fldPath, &part, nil).PrefixDetail(fmt.Sprintf("segment %d: ", i))...)
	}
	return allErrs.WithOrigin("format=k8s-resource-pool-name")
}

// ExtendedResourceName 校验 value 是否为合法的扩展资源名。
// 扩展资源名是带域前缀的名称，且不得使用 "kubernetes.io" 或 "requests." 前缀。
// 它必须能作为 "requests." 前缀拼接后的合法 label key（配额计算即按此形式处理）。
//
//   - 必须包含斜杠分隔的域与名称
//   - 不能使用 "kubernetes.io" 域
//   - 不能使用 "requests." 前缀
//   - 名称不超过 63 个字符
//   - 拼接 "requests." 后必须是合法 label key（与配额场景一致）
//     -- 只能包含字母、数字、连字符、下划线或点
//     -- 必须以字母或数字结尾
func ExtendedResourceName[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	val := string(*value)
	allErrs := field.ErrorList{}
	// 必须带域前缀；同时禁止保留域，避免与内置资源或配额前缀冲突。
	if !strings.Contains(val, "/") {
		allErrs = append(allErrs, field.Invalid(fldPath, val, "a name must be a domain-prefixed path, such as 'example.com/my-prop'"))
	} else if strings.Contains(val, resourceDefaultNamespacePrefix) {
		allErrs = append(allErrs, field.Invalid(fldPath, val, fmt.Sprintf("must not have %q domain", resourceDefaultNamespacePrefix)))
	}
	// 确保扩展资源名不会被误当作配额资源（配额资源以 "requests." 为前缀）。
	if strings.HasPrefix(val, defaultResourceRequestsPrefix) {
		allErrs = append(allErrs, field.Invalid(fldPath, val, fmt.Sprintf("must not have %q prefix", defaultResourceRequestsPrefix)))
	}

	// 转换为配额资源名后必须满足 label key 规则，这样同一名称在配额系统中也能被接受。
	nameForQuota := fmt.Sprintf("%s%s", defaultResourceRequestsPrefix, val)
	for _, msg := range content.IsLabelKey(nameForQuota) {
		allErrs = append(allErrs, field.Invalid(fldPath, val, msg))
	}
	return allErrs.WithOrigin("format=k8s-extended-resource-name")
}

// resourcesQualifiedName 校验 value 是否为合法的 Kubernetes 资源限定名：
//   - 不能为空
//   - 由可选前缀与名称组成，以斜杠分隔（如 "prefix/name"）
//   - 若指定前缀，必须是 DNS subdomain
//   - 名称部分必须是 C 标识符
//   - 名称部分不超过 32 个字符
func resourcesQualifiedName[T ~string](ctx context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	s := string(*value)
	parts := strings.Split(s, "/")
	// TODO: This validation and the corresponding handwritten validation validateQualifiedName in
	// pkg/apis/resource/validation/validation.go are not validating whether there are more than 1
	// slash. This should be fixed in both places.
	// 仅接受"无前缀"或"一个前缀 + 名称"两种形式；多余斜杠会在下面通过长度分支被忽略，
	// 这是已知缺陷（见上方待办注释）。
	switch len(parts) {
	case 1:
		allErrs = append(allErrs, validateCIdentifier(parts[0], resourceDeviceMaxLength, fldPath)...)
	case 2:
		// 形如 "/name" 时前缀为空，需显式报错而不是交给 DNS 校验。
		if len(parts[0]) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "prefix must not be empty"))
		} else {
			if len(parts[0]) > 63 {
				allErrs = append(allErrs, field.TooLong(fldPath, parts[0], 63))
			}
			allErrs = append(allErrs, LongName(ctx, op, fldPath, &parts[0], nil).PrefixDetail("prefix: ")...)
		}
		// 形如 "prefix/" 时名称为空，同样单独报错。
		if len(parts[1]) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "name must not be empty"))
		} else {
			allErrs = append(allErrs, validateCIdentifier(parts[1], resourceDeviceMaxLength, fldPath)...)
		}
	}
	return allErrs
}

// ResourceFullyQualifiedName 校验 value 是否为合法的 Kubernetes 完全限定名：
//   - 不能为空
//   - 必须由前缀与名称组成，以斜杠分隔（如 "prefix/name"）
//   - 前缀必须是 DNS subdomain
//   - 名称部分必须是 C 标识符
//   - 名称部分不超过 32 个字符
func ResourceFullyQualifiedName[T ~string](ctx context.Context, op operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	var allErrs field.ErrorList
	s := string(*value)
	allErrs = append(allErrs, resourcesQualifiedName(ctx, op, fldPath, &s, nil)...)
	// 在共享校验基础上，完全限定名额外要求必须带前缀。
	if !strings.Contains(s, "/") {
		allErrs = append(allErrs, field.Invalid(fldPath, s, "a fully qualified name must be a domain and a name separated by a slash"))
	}
	return allErrs.WithOrigin("format=k8s-resource-fully-qualified-name")
}

// validateCIdentifier 校验 id 是否为不超过 length 个字符的 C 标识符。
// 长度检查与字符合法性检查彼此独立，因此两者可能同时报错。
func validateCIdentifier(id string, length int, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(id) > length {
		allErrs = append(allErrs, field.TooLong(fldPath, id, length))
	}
	for _, msg := range content.IsCIdentifier(id) {
		allErrs = append(allErrs, field.Invalid(fldPath, id, msg))
	}
	return allErrs
}
