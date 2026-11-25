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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestFilterCoveredHandwrittenErrors 验证"被声明式校验覆盖的手写校验错误"如何被过滤：
// 只有手写错误被标记为 covered、且存在对应的已启用（beta 或 allDeclarativeEnforced）声明式错误时，
// 才丢弃手写错误，避免同一问题重复报告。
func TestFilterCoveredHandwrittenErrors(t *testing.T) {
	originRule := "test-origin"

	// 手写错误：已标记由声明式校验覆盖。
	handwrittenCovered := field.Invalid(field.NewPath("spec", "name"), "invalid-value", "must be valid").WithOrigin(originRule).MarkCoveredByDeclarative()

	// 手写错误：未标记覆盖，应始终保留。
	handwrittenNotCovered := field.Invalid(field.NewPath("spec", "name"), "invalid-value", "must be valid").WithOrigin(originRule)

	// 对应同一 origin 的声明式错误，beta 级默认生效。
	matchingBetaDeclarative := field.Invalid(field.NewPath("spec", "name"), "invalid-value", "must be valid").WithOrigin(originRule).MarkBeta()

	// 对应同一 origin 的声明式错误，alpha 级默认不生效。
	matchingAlphaDeclarative := field.Invalid(field.NewPath("spec", "name"), "invalid-value", "must be valid").WithOrigin(originRule).MarkAlpha()

	tests := []struct {
		name                   string
		errs                   field.ErrorList
		enforcedDeclarative    field.ErrorList
		allDeclarativeEnforced bool
		betaEnabled            bool
		rules                  []field.NormalizationRule
		expectedLen            int
	}{
		{
			// 未标记覆盖的手写错误即使有对应声明式错误也必须保留。
			name:                   "uncovered handwritten error is preserved",
			errs:                   field.ErrorList{handwrittenNotCovered},
			enforcedDeclarative:    field.ErrorList{matchingBetaDeclarative},
			allDeclarativeEnforced: false,
			expectedLen:            1,
		},
		{
			// 有生效的 beta 声明式错误对应时，被覆盖的手写错误可以丢弃。
			name:                   "covered handwritten error filtered when matching beta declarative error exists",
			errs:                   field.ErrorList{handwrittenCovered},
			enforcedDeclarative:    field.ErrorList{matchingBetaDeclarative},
			allDeclarativeEnforced: false,
			expectedLen:            0,
		},
		{
			// 对应的声明式错误仍是 alpha（未生效），不能丢弃手写错误，否则校验会被削弱。
			name:                   "covered handwritten error not filtered when matching declarative is alpha",
			errs:                   field.ErrorList{handwrittenCovered},
			enforcedDeclarative:    field.ErrorList{matchingAlphaDeclarative},
			allDeclarativeEnforced: false,
			expectedLen:            1,
		},
		{
			// 在 allDeclarativeEnforced 模式下声明式校验被视为全面生效，无需逐个匹配即可丢弃手写错误。
			name:                   "covered handwritten error filtered in allDeclarativeEnforced mode regardless of matching declarative error",
			errs:                   field.ErrorList{handwrittenCovered},
			enforcedDeclarative:    nil,
			allDeclarativeEnforced: true,
			expectedLen:            0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 通过 context 打开"全部声明式校验生效"的测试开关。
			ctx := context.Background()
			if tt.allDeclarativeEnforced {
				ctx = WithAllDeclarativeEnforcedForTest(ctx)
			}
			res := FilterCoveredHandwrittenErrors(ctx, tt.errs, tt.enforcedDeclarative, tt.betaEnabled, tt.rules...)
			if len(res) != tt.expectedLen {
				t.Errorf("FilterCoveredHandwrittenErrors() returned %d errors, expected %d", len(res), tt.expectedLen)
			}
		})
	}
}

// TestFilterEnforcedDeclarativeErrors 验证按成熟度（alpha/beta）过滤声明式校验错误：
// beta 关闭时只保留标准与 internal 错误；beta 开启时额外保留 beta；alpha 始终不保留。
// allDeclarativeEnforced 模式则不做过滤。
func TestFilterEnforcedDeclarativeErrors(t *testing.T) {
	// 覆盖四类输入：内部错误、alpha、beta 与标准错误。
	internalErr := field.InternalError(field.NewPath("spec"), errors.New("internal error"))
	alphaErr := field.Invalid(field.NewPath("spec"), "val", "alpha").MarkAlpha()
	betaErr := field.Invalid(field.NewPath("spec"), "val", "beta").MarkBeta()
	stdErr := field.Invalid(field.NewPath("spec"), "val", "standard")

	tests := []struct {
		name                   string
		declarativeErrs        field.ErrorList
		betaEnabled            bool
		allDeclarativeEnforced bool
		expectedLen            int
	}{
		{
			// 全部生效模式下不过滤任何错误。
			name:                   "all errors enforced in allDeclarativeEnforced mode",
			declarativeErrs:        field.ErrorList{internalErr, alphaErr, betaErr, stdErr},
			betaEnabled:            false,
			allDeclarativeEnforced: true,
			expectedLen:            4,
		},
		{
			// beta 关闭时 alpha 与 beta 均被忽略。
			name:                   "beta disabled ignores alpha and beta",
			declarativeErrs:        field.ErrorList{internalErr, alphaErr, betaErr, stdErr},
			betaEnabled:            false,
			allDeclarativeEnforced: false,
			expectedLen:            2, // 内部操作 + 标准操作。
		},
		{
			// beta 开启时保留 beta，但 alpha 仍被排除。
			name:                   "beta enabled includes beta but excludes alpha",
			declarativeErrs:        field.ErrorList{internalErr, alphaErr, betaErr, stdErr},
			betaEnabled:            true,
			allDeclarativeEnforced: false,
			expectedLen:            3, // 内部操作 + beta 操作 + 标准操作。
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 通过 context 打开"全部声明式校验生效"的测试开关。
			ctx := context.Background()
			if tt.allDeclarativeEnforced {
				ctx = WithAllDeclarativeEnforcedForTest(ctx)
			}
			res := FilterEnforcedDeclarativeErrors(ctx, tt.declarativeErrs, tt.betaEnabled)
			if len(res) != tt.expectedLen {
				t.Errorf("FilterEnforcedDeclarativeErrors() returned %d errors, expected %d", len(res), tt.expectedLen)
			}
		})
	}
}
