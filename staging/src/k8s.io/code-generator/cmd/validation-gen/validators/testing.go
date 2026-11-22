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

package validators

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	// These tags return a fixed pass/fail state.
	validateTrueTagName  = "k8s:validateTrue"
	validateFalseTagName = "k8s:validateFalse"

	// This tag always returns an error from ExtractValidations.
	validateErrorTagName = "k8s:validateError"

	// validate true alpha/beta  test tags.
	validateAlphaTagName = "k8s:validateTrueAlpha"
	validateBetaTagName  = "k8s:validateTrueBeta"
)

func init() {
	RegisterTagValidator(fixedResultTagValidator{result: true})
	RegisterTagValidator(fixedResultTagValidator{result: false})
	RegisterTagValidator(fixedResultTagValidator{error: true})
	RegisterTagValidator(fixedResultTagValidator{result: true, stability: TagStabilityLevelAlpha})
	RegisterTagValidator(fixedResultTagValidator{result: true, stability: TagStabilityLevelBeta})
}

// fixedResultTagValidator 实现一组固定结果的测试标记：
// validateTrue/validateFalse 返回固定布尔结果，validateError 直接让生成失败，
// validateTrueAlpha/Beta 则是带稳定性级别的固定结果版本。
type fixedResultTagValidator struct {
	result    bool
	error     bool
	stability TagStabilityLevel
}

func (fixedResultTagValidator) Init(_ Config) {}

// TagName 根据构造时的 result/error/stability 组合返回对应标记名。
func (frtv fixedResultTagValidator) TagName() string {
	if frtv.error {
		return validateErrorTagName
	} else if frtv.result {
		switch frtv.stability {
		case TagStabilityLevelAlpha:
			return validateAlphaTagName
		case TagStabilityLevelBeta:
			return validateBetaTagName
		default:
			return validateTrueTagName
		}
	}
	return validateFalseTagName
}

var fixedResultTagValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

func (fixedResultTagValidator) ValidScopes() sets.Set[Scope] {
	return fixedResultTagValidScopes
}

// GetValidations 生成固定结果的校验函数；error 模式则直接返回错误。
func (frtv fixedResultTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	var result Validations

	// validateError 用于测试生成器对错误的处理，直接终止生成。
	if frtv.error {
		return result, fmt.Errorf("forced error: %q", tag.Value)
	}

	args, err := frtv.toFixedResultArgs(tag)
	if err != nil {
		return result, fmt.Errorf("can't decode tag payload: %w", err)
	}
	fn := Function(frtv.TagName(), args.flags, fixedResultValidator, frtv.result, args.msg).
		WithTypeArgs(args.typeArgs...)
	// 仅失败校验会产生错误，因此只有它需要声明错误来源。
	if !frtv.result {
		fn = fn.WithEmits(Emission{field.ErrorTypeInvalid, "validateFalse", ""})
	}
	fn.Cohort = args.cohort
	result.AddFunction(fn)

	return result, nil
}

var (
	fixedResultValidator = types.Name{Package: libValidationPkg, Name: "FixedResult"}
)

// fixedResultArgs 是从标记参数解析出的固定结果校验配置。
type fixedResultArgs struct {
	flags    FunctionFlags
	msg      string
	typeArgs []types.Name
	cohort   string
}

// toFixedResultArgs 解析 flags/typeArg/cohort 命名参数与字符串负载。
func (fixedResultTagValidator) toFixedResultArgs(in codetags.Tag) (fixedResultArgs, error) {
	result := fixedResultArgs{}
	for _, a := range in.Args {
		switch a.Name {
		case "flags":
			for _, fl := range strings.Split(a.Value, ",") {
				fl = strings.TrimSpace(fl)
				switch fl {
				case "ShortCircuit":
					result.flags |= ShortCircuit
				case "NonError":
					result.flags |= NonError
				default:
					return fixedResultArgs{}, fmt.Errorf("unknown flag: %q", fl)
				}
			}
		case "typeArg":
			if tn := a.Value; len(tn) > 0 {
				if !strings.HasPrefix(tn, "*") {
					tn = "*" + tn // We always need the pointer type.
				}
				result.typeArgs = []types.Name{{Package: "", Name: tn}}
			}
		case "cohort":
			result.cohort = a.Value
		}
	}
	if in.ValueType == codetags.ValueTypeString {
		result.msg = in.Value
	}
	return result, nil
}

// Docs 返回该标记的文档元数据；固定结果标记属于测试辅助，默认级别为 alpha。
func (frtv fixedResultTagValidator) Docs() TagDoc {
	tagStabilityLevel := TagStabilityLevelAlpha
	if frtv.stability != "" {
		tagStabilityLevel = frtv.stability
	}
	doc := TagDoc{
		Tag:            frtv.TagName(),
		StabilityLevel: tagStabilityLevel,
		Scopes:         sets.List(frtv.ValidScopes()),
	}
	doc.PayloadsType = codetags.ValueTypeString
	if frtv.error {
		doc.Description = "Always fails code generation (useful for testing)."
		doc.Payloads = []TagPayloadDoc{{
			Description: "<string>",
			Docs:        "This string will be included in the error message.",
		}}
	} else {
		// True and false have the same args and payload.
		doc.Payloads = []TagPayloadDoc{{
			Description: "<none>",
		}, {
			Description: "<string>",
			Docs:        "The generated code will include this string.",
		}}
		doc.Args = []TagArgDoc{{
			Name:        "flags",
			Description: "<comma-separated-list-of-flag-string>",
			Docs:        `values: ShortCircuit, NonError`,
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "typeArg",
			Description: "<string>",
			Docs:        "The type arg in generated code (must be the value-type, not pointer).",
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "cohort",
			Description: "<string>",
			Docs:        "An optional cohort name to group multiple validations.",
			Type:        codetags.ArgTypeString,
		}}
		if frtv.result {
			doc.Description = "Always passes validation (useful for testing)."
		} else {
			doc.Description = "Always fails validation (useful for testing)."
		}
	}
	return doc
}
