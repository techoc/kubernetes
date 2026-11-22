/*
Copyright 2021 The Kubernetes Authors.

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
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	enumTagName        = "k8s:enum"
	enumExcludeTagName = "k8s:enumExclude"
)

func init() {
	RegisterTagValidator(&enumTagValidator{})
	RegisterTagValidator(&enumExcludeTagValidator{})
}

// enumExcludeTagValidator 实现 +k8s:enumExclude：把某个常量从枚举集合中排除。
// 它本身不产生校验函数，只影响 enumTagValidator 收集常量时的结果。
type enumExcludeTagValidator struct {
}

func (*enumExcludeTagValidator) Init(_ Config) {
}

func (*enumExcludeTagValidator) TagName() string {
	return enumExcludeTagName
}

var enumExcludeValidScope = sets.New(ScopeConst)

func (*enumExcludeTagValidator) ValidScopes() sets.Set[Scope] {
	return enumExcludeValidScope
}

// GetValidations 返回空：排除逻辑在枚举收集阶段处理。
func (*enumExcludeTagValidator) GetValidations(_ Context, _ codetags.Tag) (Validations, error) {
	return Validations{}, nil
}

func (eetv *enumExcludeTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            eetv.TagName(),
		StabilityLevel: TagStabilityLevelAlpha,
		Scopes:         sets.List(eetv.ValidScopes()),
		Description: `Indicates that an constant value is not part of an enum, even if the constant's type is tagged with k8s:enum.
May be conditionally excluded via +k8s:ifEnabled(Option)=+k8s:enumExclude or +k8s:ifDisabled(Option)=+k8s:enumExclude.
If multiple +k8s:ifEnabled/+k8s:ifDisabled tags are used, the value is excluded if any of the exclude conditions are met.`,
	}
}

// enumTagValidator 实现 +k8s:enum：把类型的所有常量收集为一个枚举集合，
// 并生成 Enum 校验函数。
type enumTagValidator struct {
	validator TagValidationExtractor
}

func (etv *enumTagValidator) Init(cfg Config) {
	etv.validator = cfg.TagValidator
}

func (enumTagValidator) TagName() string {
	return enumTagName
}

var enumTagValidScopes = sets.New(ScopeType)

func (enumTagValidator) ValidScopes() sets.Set[Scope] {
	return enumTagValidScopes
}

var (
	enumValidator     = types.Name{Package: libValidationPkg, Name: "Enum"}
	enumExclusionType = types.Name{Package: libValidationPkg, Name: "EnumExclusion"}
	setsNew           = types.Name{Package: "k8s.io/apimachinery/pkg/util/sets", Name: "New"}
)

// GetValidations 收集类型的所有常量（按名称排序）并生成 Enum 校验。
func (etv *enumTagValidator) GetValidations(context Context, _ codetags.Tag) (Validations, error) {
	// 注意：指向指针的 typedef 不被支持，因此这里不应出现指针。
	// NOTE: typedefs to pointers are not supported, so we should never see a pointer here.
	if t := util.NativeType(context.Type); t != types.String {
		return Validations{}, fmt.Errorf("can only be used on string types (%s)", rootTypeString(context.Type, t))
	}

	enum := &enumType{Name: context.Type.Name}
	for _, c := range context.Constants {
		// 逐个常量检查是否有 enumExclude（无条件或条件式）。
		var exclusions []enumExclude
		isExcluded := false
		for _, tag := range c.Tags {
			switch tag.Name {
			case enumExcludeTagName:
				isExcluded = true
			case ifEnabledTag, ifDisabledTag:
				if tag.ValueTag != nil && tag.ValueTag.Name == enumExcludeTagName {
					if option, ok := tag.PositionalArg(); ok {
						exclusions = append(exclusions, enumExclude{
							excludeWhen: tag.Name == ifEnabledTag,
							option:      option.Value,
						})
					}
				}
			}
		}
		if isExcluded {
			continue
		}

		value := &enumValue{
			Name:       c.Constant.Name,
			Value:      *c.Constant.ConstValue,
			Comment:    strings.Join(c.Constant.CommentLines, " "),
			Exclusions: exclusions,
		}
		enum.addIfNotPresent(value)
	}

	// 排序，保证后续代码生成的输出稳定。
	// Sort the values for the codegen that happens later.
	slices.SortFunc(enum.Values, func(a, b *enumValue) int {
		return cmp.Compare(a.Name.Name, b.Name.Name)
	})
	for _, v := range enum.Values {
		slices.SortFunc(v.Exclusions, func(a, b enumExclude) int {
			if a.excludeWhen == b.excludeWhen {
				return cmp.Compare(a.option, b.option)
			}
			if a.excludeWhen {
				return 1
			}
			return -1
		})
	}

	var result Validations

	// TODO: Avoid the "local" here. This was added to avoid errors caused when the package is an empty string.
	//       The correct package would be the output package but is not known here. This does not show up in generated code.
	// TODO: Append a consistent hash suffix to avoid generated name conflicts?
	symbolsVarName := PrivateVar{Name: "SymbolsFor" + context.Type.Name.Name, Package: "local"}
	var allValues []any
	var exclusionRules []any
	for _, v := range enum.Values {
		allValues = append(allValues, Identifier(v.Name))
		for _, exclusion := range v.Exclusions {
			exclusionRules = append(exclusionRules, StructLiteral{
				Type:     enumExclusionType,
				TypeArgs: []*types.Type{context.Type},
				Fields: []StructLiteralField{
					{"Value", Identifier(v.Name)},
					{"Option", exclusion.option},
					{"ExcludeWhen", exclusion.excludeWhen},
				},
			})
		}
	}
	// 生成 sets.New(...) 初始化枚举集合；空集合时显式带类型参数。
	initFn := Function("setsNew", DefaultFlags, setsNew, allValues...)
	if len(allValues) == 0 {
		initFn = initFn.WithTypeArgs(enum.Name)
	}
	result.AddVariable(Variable(symbolsVarName, initFn))
	var exclusions any = Literal("nil")
	if len(exclusionRules) > 0 {
		// 有排除规则时额外生成一个排除规则切片变量。
		exclusionsVar := PrivateVar{Name: "ExclusionsFor" + context.Type.Name.Name, Package: "local"}
		result.AddVariable(Variable(exclusionsVar, SliceLiteral{
			ElementType:     enumExclusionType,
			ElementTypeArgs: []*types.Type{context.Type},
			Elements:        exclusionRules,
		}))
		exclusions = exclusionsVar
	}
	// 生成的校验会产出 NotSupported 错误。
	fn := Function(enumTagName, DefaultFlags, enumValidator, symbolsVarName, exclusions).
		WithEmits(Emission{field.ErrorTypeNotSupported, "", ""})
	result.AddFunction(fn)

	return result, nil
}

func (etv *enumTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            etv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(etv.ValidScopes()),
		Description:    "Indicates that a string type is an enum. All constant values of this type are considered values in the enum unless excluded using +k8s:enumExclude.",
	}
}

// ValueArgs 返回枚举值的标识符列表，供生成 sets.New 等调用使用。
func (et *enumType) ValueArgs() []any {
	var values []any
	for _, value := range et.SymbolConstants() {
		values = append(values, value)
	}
	return values
}

// SymbolConstants 返回按名称排序的枚举常量标识符，保证输出稳定。
func (et *enumType) SymbolConstants() []Identifier {
	var values []Identifier
	for _, value := range et.Values {
		values = append(values, Identifier(value.Name))
	}
	slices.SortFunc(values, func(a, b Identifier) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return values
}

// TODO: Everything below this comment is copied from kube-openapi's enum.go.

// enumType 表示一个字符串枚举：类型名 + 所有合法值。
type enumType struct {
	Name   types.Name
	Values []*enumValue
}

// enumValue 是一个枚举常量，包含其标识符、字面值、注释与条件排除规则。
type enumValue struct {
	Name       types.Name
	Value      string
	Comment    string
	Exclusions []enumExclude
}

// enumExclude 描述一条条件排除规则。
type enumExclude struct {
	// excludeWhen determines the condition for exclusion.
	// If true, the value is excluded if the option is present.
	// If false, the value is excluded if the option is NOT present.
	excludeWhen bool
	// option is the name of the feature option that controls the exclusion.
	option string
}

// ValueStrings 返回枚举所有可能值的字符串形式（带引号的 Go 字面量，已排序）。
// ValueStrings returns all possible values of the enum type as strings
// the results are sorted and quoted as Go literals.
func (et *enumType) ValueStrings() []string {
	var values []string
	for _, value := range et.Values {
		// use "%q" format to generate a Go literal of the string const value
		values = append(values, fmt.Sprintf("%q", value.Value))
	}
	sort.Strings(values)
	return values
}

// addIfNotPresent 添加枚举值；若已有相同字面值则按确定性规则择一保留。
func (et *enumType) addIfNotPresent(value *enumValue) {
	// 已有相同值则忽略新值。当枚举别名并重新导出常量时会出现这种情况。
	// If we already have an enum case with the same value, then ignore this new
	// one. This can happen if an enum aliases one from another package and
	// re-exports the cases.
	for i, existing := range et.Values {
		if existing.Value == value.Value {

			// 保留注释更长（信息更多）的版本；长度相同则按字典序取较大者，
			// 以保证多次生成结果一致。
			// Take the value of the longer comment (or some other deterministic tie breaker)
			if len(existing.Comment) < len(value.Comment) || (len(existing.Comment) == len(value.Comment) && existing.Comment > value.Comment) {
				et.Values[i] = value
			}

			return
		}
	}
	et.Values = append(et.Values, value)
}
