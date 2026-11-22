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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

// 这两个标签由同一个校验器实现，仅在 enabled 标志上不同。
const (
	ifEnabledTag  = "k8s:ifEnabled"
	ifDisabledTag = "k8s:ifDisabled"
)

// 注册两个实例，分别对应 ifEnabled 与 ifDisabled。
func init() {
	RegisterTagValidator(&ifTagValidator{true, nil})
	RegisterTagValidator(&ifTagValidator{false, nil})
}

// ifTagValidator 将内部校验包装成“仅在某个 option 开启/关闭时执行”。
// enabled 决定该实例响应哪个标签。
type ifTagValidator struct {
	enabled   bool
	validator TagValidationExtractor
}

// Init 保存配置中的 TagValidator，用于解析被包裹的 payload 标签。
func (itv *ifTagValidator) Init(cfg Config) {
	itv.validator = cfg.TagValidator
}

// TagName 返回当前实例对应的标签名。
func (itv ifTagValidator) TagName() string {
	if itv.enabled {
		return ifEnabledTag
	}
	return ifDisabledTag
}

// ifEnabledDisabledTagValidScopes 声明该标签可出现的所有作用域。
var ifEnabledDisabledTagValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal, ScopeConst)

// ValidScopes 返回支持的作用域集合。
func (ifTagValidator) ValidScopes() sets.Set[Scope] {
	return ifEnabledDisabledTagValidScopes
}

// ifOption 是生成代码中包装校验函数的运行时辅助函数。
var (
	ifOption = types.Name{Package: libValidationPkg, Name: "IfOption"}
)

// GetValidations 解析 "<option>" 位置参数与 payload 校验，并用
// IfOption 包装每个生成函数。ParentContext 作用域需使用父类型，
// 否则包装后的函数签名会与实际对象类型不符。
func (itv ifTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	optionArg, ok := tag.PositionalArg()
	if !ok {
		return Validations{}, fmt.Errorf("missing required option name positional argument")
	}

	validations, err := itv.validator.ExtractTagValidations(context, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	return WrapFunctions(validations, func(fn FunctionGen, scope DeferredScope) FunctionGen {
		objType := context.Type
		if scope == ParentContext {
			objType = context.ParentType
		}

		return Function(itv.TagName(), fn.Flags, ifOption, optionArg.Value, itv.enabled, WrapperFunction{Function: fn, ObjType: objType})
	}), nil
}

// Docs 返回该标签的文档元数据；payload 必填，因为标签本身不含校验逻辑。
func (itv ifTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            itv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Args: []TagArgDoc{{
			Description: "<option>",
			Type:        codetags.ArgTypeString,
			Required:    true,
		}},
		Scopes: sets.List(itv.ValidScopes()),
	}

	doc.PayloadsType = codetags.ValueTypeTag
	doc.PayloadsRequired = true
	if itv.enabled {
		doc.Description = "Declares a validation that only applies when an option is enabled."
		doc.Payloads = []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "This validation tag will be evaluated only if the validation option is enabled.",
		}}
	} else {
		doc.Description = "Declares a validation that only applies when an option is disabled."
		doc.Payloads = []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "This validation tag will be evaluated only if the validation option is disabled.",
		}}
	}
	return doc
}
