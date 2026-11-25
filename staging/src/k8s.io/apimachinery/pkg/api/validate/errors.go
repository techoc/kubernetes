/*
Copyright The Kubernetes Authors.

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
	"errors"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// allDeclarativeEnforcedKeyType 是 allDeclarativeEnforcedKey 的空结构体类型。
// 使用空结构体而非字符串或整型作为 context 键，可以保证键值永不与任何外部值冲突，
// 避免校验逻辑被调用方意外注入的同名键干扰。
type allDeclarativeEnforcedKeyType struct{}

var allDeclarativeEnforcedKey = allDeclarativeEnforcedKeyType{}

// WithAllDeclarativeEnforcedForTest 返回父 context 的副本，其中 allDeclarativeEnforcedKey 被置为 true。
// 该函数用于测试场景：无论功能开关或成熟度级别如何，都暴露全部声明式校验错误，
// 并过滤掉所有被声明式校验覆盖的手写校验错误。
//
// 注意：此函数仅用于测试，不得在生产代码中使用。
func WithAllDeclarativeEnforcedForTest(ctx context.Context) context.Context {
	return context.WithValue(ctx, allDeclarativeEnforcedKey, true)
}

// AllDeclarativeEnforced 返回 true 表示 context 中 allDeclarativeEnforcedKey 被置为 true。
func AllDeclarativeEnforced(ctx context.Context) bool {
	// nil 的 context 没有承载任何值，直接判定为未开启，避免对 nil 调用 Value 的歧义。
	if ctx == nil {
		return false
	}
	return ctx.Value(allDeclarativeEnforcedKey) == true
}

// FilterCoveredHandwrittenErrors 在存在匹配的已生效 beta 声明式错误（按类型、字段与来源匹配）时，
// 移除被声明式校验覆盖（CoveredByDeclarative）的手写错误。
// 在 AllDeclarativeEnforced（仅测试）模式下，每一个被覆盖的手写错误都会被移除。
func FilterCoveredHandwrittenErrors(ctx context.Context, imperativeErrs, enforcedDeclarativeErrs field.ErrorList, betaEnabled bool, rules ...field.NormalizationRule) field.ErrorList {
	// 匹配器要求类型与来源一致，来源无效时必须提供来源；字段名按归一化规则比较，
	// 以便 foo-bar 与 foo_bar 这类书写差异不会让同一条错误被判为不匹配。
	matcher := field.ErrorMatcher{}.ByType().ByOrigin().RequireOriginWhenInvalid().ByFieldNormalized(rules)
	allDeclarativeEnforced := AllDeclarativeEnforced(ctx)
	return imperativeErrs.Filter(func(e error) bool {
		var fe *field.Error
		// 非 field.Error 或未标记为被声明式覆盖的错误必须保留，它们不受声明式校验约束。
		if !errors.As(e, &fe) || !fe.CoveredByDeclarative {
			return false
		}
		// 测试模式下的全量覆盖：无论声明式错误是否已生效，都移除手写错误，
		// 以便测试能观察到声明式校验的完整结果。
		if allDeclarativeEnforced {
			return true
		}
		for _, dErr := range enforcedDeclarativeErrs {
			// 只有已生效的 beta 声明式错误才能替代对应手写错误；beta 未开启时手写错误仍需保留，
			// 避免功能关闭后校验结果反而变宽松。
			if dErr.IsBeta() && matcher.Matches(fe, dErr) {
				return true
			}
		}
		return false
	})
}

// FilterEnforcedDeclarativeErrors 收集在当前模式下被生效（即会对用户暴露）的声明式错误。
// 满足以下任一条件时，声明式错误即为生效：
//   - AllDeclarativeEnforced 已设置（测试模式）：每一条声明式错误都生效。
//   - 它是内部错误：始终生效，与生命周期阶段无关。
//   - 它是 beta 错误且 BetaEnabled 为 true。
//   - 它是标准（无前缀）错误：始终生效。
//
// alpha 错误永不生效；它们始终被手写校验遮蔽。
func FilterEnforcedDeclarativeErrors(ctx context.Context, declarativeErrs field.ErrorList, betaEnabled bool) field.ErrorList {
	// 预分配容量，避免在逐条判定时反复扩容。
	enforcedDeclarativeErrs := make(field.ErrorList, 0, len(declarativeErrs))
	allDeclarativeEnforced := AllDeclarativeEnforced(ctx)
	for _, dvErr := range declarativeErrs {
		switch {
		// 测试模式：无条件放行，便于验证声明式校验的全部输出。
		case allDeclarativeEnforced:
			enforcedDeclarativeErrs = append(enforcedDeclarativeErrs, dvErr)
		// 内部错误由实现自身保证语义，必须始终暴露，否则会掩盖程序缺陷。
		case dvErr.Type == field.ErrorTypeInternal:
			enforcedDeclarativeErrs = append(enforcedDeclarativeErrs, dvErr)
		// beta 错误的可见性受功能开关控制，开关关闭时保持向后兼容的旧校验行为。
		case dvErr.IsBeta():
			if betaEnabled {
				enforcedDeclarativeErrs = append(enforcedDeclarativeErrs, dvErr)
			}
		// 标准错误不区分生命周期，始终暴露。
		case !dvErr.IsAlpha():
			enforcedDeclarativeErrs = append(enforcedDeclarativeErrs, dvErr) // 标准操作。
		}
	}
	return enforcedDeclarativeErrs
}
