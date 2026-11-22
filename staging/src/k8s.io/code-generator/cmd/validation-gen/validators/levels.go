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

package validators

import (
	"fmt"
	"regexp"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gengo/v2/codetags"
)

// kubeVersionRegex 限定版本参数格式为 <major>.<minor>。
var kubeVersionRegex = regexp.MustCompile(`^\d+\.\d+$`)

const (
	alphaTagName = "k8s:alpha"
	betaTagName  = "k8s:beta"
)

func init() {
	// 同一个校验器类型用不同 tagName/level 注册两次，分别实现 alpha 与 beta。
	RegisterTagValidator(&levelTagValidator{tagName: alphaTagName, level: ValidationStabilityLevelAlpha})
	RegisterTagValidator(&levelTagValidator{tagName: betaTagName, level: ValidationStabilityLevelBeta})
}

// levelTagValidator 实现 +k8s:alpha / +k8s:beta：把负载校验标记为指定稳定性级别。
type levelTagValidator struct {
	validator TagValidationExtractor
	tagName   string
	level     ValidationStabilityLevel
}

func (ltv *levelTagValidator) Init(cfg Config) {
	ltv.validator = cfg.TagValidator
}

func (ltv *levelTagValidator) TagName() string {
	return ltv.tagName
}

var levelTagsValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

func (levelTagValidator) ValidScopes() sets.Set[Scope] {
	return levelTagsValidScopes
}

// GetValidations 解析可选的 since 版本参数，递归提取负载校验并统一改写其级别。
func (ltv *levelTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// 负载必须是一个校验标记。
	if tag.ValueType != codetags.ValueTypeTag || tag.ValueTag == nil {
		return Validations{}, fmt.Errorf("requires a validation tag as its value payload")
	}

	// 至多允许一个可选参数（since）。
	if len(tag.Args) > 1 {
		return Validations{}, fmt.Errorf("at most one optional kubernetes version argument is supported")
	}

	var version string
	if len(tag.Args) == 1 {
		arg := tag.Args[0]
		version = arg.Value
		if !kubeVersionRegex.MatchString(version) {
			return Validations{}, fmt.Errorf("invalid kubernetes version format, expected <major>.<minor>, got %s", version)
		}
	}

	// 先设置上下文的稳定性级别，再解析负载，使嵌套标记继承该级别。
	context.StabilityLevel = ltv.level
	validations, err := ltv.validator.ExtractTagValidations(context, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	// 递归改写所有函数级别；自行管理级别的函数保持不变。
	validations = WrapFunctions(validations, func(fn FunctionGen, scope DeferredScope) FunctionGen {
		if fn.StabilityLevelSelfManaged {
			return fn
		}
		fn.StabilityLevel = ltv.level
		return fn
	})
	return validations, nil
}

func (ltv *levelTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            ltv.TagName(),
		StabilityLevel: TagStabilityLevelBeta,
		Scopes:         sets.List(ltv.ValidScopes()),
		Description:    fmt.Sprintf("Marks the given payload validation as a %s validation of the handwritten validation code. An optional Kubernetes version can be specified.", ltv.level),
		Args: []TagArgDoc{{
			Description: "The Kubernetes version (e.g. `1.34`) at which this validation was added.",
			Type:        codetags.ArgTypeString,
			Name:        "since",
		}},
		Payloads: []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        fmt.Sprintf("The validation tag to evaluate as a %s validation.", ltv.level),
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
	return doc
}
