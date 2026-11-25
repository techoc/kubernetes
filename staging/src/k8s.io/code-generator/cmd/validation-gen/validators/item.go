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
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	itemTagName = "k8s:item"
)

// 注册时机上必须晚于 list 元数据收集完成：item 依赖 listMapKey 来定位目标元素。
func init() {
	// Processing item tags requires the list metadata.
	RegisterTagValidator(&itemTagValidator{listByPath: globalListMeta})
}

// keyValuePair 是 +k8s:item(name: "x", priority: 1) 中的一个选择条件。
// valueType 记录原始字面量类型，用于生成类型匹配的 Go 比较表达式。
type keyValuePair struct {
	key       string
	value     any
	valueType codetags.ValueType
}

// itemTagValidator 为 listType=map 列表中的特定元素声明校验。
// 选择条件必须覆盖全部 listMapKey，否则无法唯一定位元素。
type itemTagValidator struct {
	validator  TagValidationExtractor
	listByPath map[string]*listMetadata
}

// Init 注入子标签解析器；list 元数据由全局注册表在稍后阶段填充。
func (itv *itemTagValidator) Init(cfg Config) {
	itv.validator = cfg.TagValidator
}

// TagName 返回标签名。
func (itemTagValidator) TagName() string {
	return itemTagName
}

// item 可以出现在类型、字段以及列表/映射元素位置，但 GetValidations 会再校验必须是 slice。
var itemTagValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

// ValidScopes 返回允许的作用域集合。
func (itemTagValidator) ValidScopes() sets.Set[Scope] {
	return itemTagValidScopes
}

// 运行时库中的两种包装函数：元素是值还是指针决定生成的签名。
var (
	validateValSliceItem = types.Name{Package: libValidationPkg, Name: "ValSliceItem"}
	validatePtrSliceItem = types.Name{Package: libValidationPkg, Name: "PtrSliceItem"}
)

// GetValidations 把子标签的校验包成“仅对匹配元素生效”的校验。
// 由于 list 元数据在解析阶段尚未就绪，立即函数的生成被推迟到 ThisContext。
func (itv *itemTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// TODO: Support regular maps with syntax like:
	// +k8s:item("map-key")=+k8s:immutable

	// 该标签可用于值字段和指针字段，也可用于类型别名（类型别名绝不应是指针），
	// 因此需要检查具体类型。只有真实 slice 才有“元素”；元素本身还会被剥到 struct 再生成比较条件。
	nt := util.NonPointer(util.NativeType(context.Type))
	if nt.Kind != types.Slice {
		return Validations{}, fmt.Errorf("can only be used on list types")
	}
	elemT := util.NonPointer(util.NativeType(nt.Elem))

	// 指针元素用 PtrSliceItem（对 nil 元素报 Required），值元素用 ValSliceItem。
	validateFunc := validateValSliceItem
	if nt.Elem.Kind == types.Pointer {
		validateFunc = validatePtrSliceItem
	}
	if elemT.Kind != types.Struct {
		return Validations{}, fmt.Errorf("can only be used on lists of structs")
	}
	if tag.ValueType != codetags.ValueTypeTag || tag.ValueTag == nil {
		return Validations{}, fmt.Errorf("requires a validation tag as its value payload")
	}

	// Parse key-value pairs from named args.
	criteria, err := criteriaFromArgs(tag.Args)
	if err != nil {
		return Validations{}, err
	}
	if len(criteria) == 0 {
		return Validations{}, fmt.Errorf("no selection criteria was specified")
	}

	// 从存储的标签中提取校验定义：itemKey 是生成的 selector 字符串，同时用作 cohort 名，确保同一元素的
	// 多个校验连续输出且按稳定顺序执行。
	itemKey := selectorString(criteria)
	itemPath := context.Path.Key(itemKey)
	itemSelector := generateSelector(criteria)
	subContext := Context{
		Scope:          ScopeListVal,
		Type:           elemT,
		Path:           itemPath,
		ListSelector:   itemSelector,
		ParentPath:     context.Path,
		ParentType:     context.Type,
		StabilityLevel: context.StabilityLevel,
	}

	validations, err := itv.validator.ExtractTagValidations(subContext, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	result := validations.Clone()
	result.Functions = nil
	result.Deferred = nil

	// 1. Handle immediate functions. We need to defer them because listMetadata is not ready yet.
	if len(validations.Functions) > 0 {
		result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
			matchArg, equivArg, err := itv.prepareArgs(context, criteria, elemT)
			if err != nil {
				return Validations{}, err
			}
			deferredResult := Validations{}
			for _, vfn := range validations.Functions {
				f := Function(itemTagName, vfn.Flags, validateFunc, matchArg, equivArg, WrapperFunction{Function: vfn, ObjType: elemT})
				f.Cohort = itemKey
				vfn = f
				deferredResult.AddFunction(vfn)
			}
			return deferredResult, nil
		}))
	}

	// 2. Handle deferred validations from child tag.
	for _, d := range validations.Deferred {
		scope := d.Scope
		if scope == ParentContext {
			// A child tag (e.g. +k8s:union) returning a ParentContext validation intends
			// to evaluate the entire collection (the list) after its items are processed.
			// Because item tags are evaluated in the context of the list field, the "parent"
			// of a list element is conceptually the list itself, which maps to ThisContext
			// (the field phase).
			// If we left this as ParentContext, the framework would mistakenly defer it to
			// the struct containing the list. By mapping to ThisContext, we ensure the
			// deferred validation correctly runs at the end of the list field's phase, naturally
			// accumulating and running after all immediate item validations for this criteria.
			scope = ThisContext
		}
		result.AddDeferred(Deferred(scope, func() (Validations, error) {
			inner, err := d.Callback()
			if err != nil {
				return Validations{}, err
			}

			matchArg, equivArg, err := itv.prepareArgs(context, criteria, elemT)
			if err != nil {
				return Validations{}, err
			}

			return wrapFunctionsWithScope(inner, func(fn FunctionGen, s DeferredScope) FunctionGen {
				if s == ParentContext {
					// We reach here if the original deferred scope was ParentContext.
					// Because the parent of a list element is the list itself, the generated
					// function `fn` (e.g. a union validation) already expects the full list
					// as its argument. If we didn't skip wrapping, validateValSliceItem would
					// mistakenly attempt to pass individual list elements to it.
					return fn
				}
				f := Function(itemTagName, fn.Flags, validateFunc, matchArg, equivArg, WrapperFunction{Function: fn, ObjType: elemT})
				f.Cohort = itemKey
				return f
			}, d.Scope), nil
		}))
	}

	return result, nil
}

// prepareArgs 根据已收集的 list 元数据生成匹配函数与相等性比较参数。
// 必须等 list 标签处理完才能调用，因此它的调用点都在 deferred 回调里。
func (itv *itemTagValidator) prepareArgs(context Context, criteria []keyValuePair, elemT *types.Type) (matchArg any, equivArg any, err error) {
	// 列表元数据可能挂在字段路径或类型名上：字段继承类型上的 list 声明时后一种生效。
	listMeta, ok := itv.listByPath[context.Path.String()]
	if !ok && context.Scope == ScopeField {
		listMeta, ok = itv.listByPath[context.Type.String()]
	}
	if !ok || listMeta.semantic != semanticMap || len(listMeta.keyMembers) == 0 {
		return nil, nil, fmt.Errorf("found items with no list metadata - item tags require listType=map or unique=map with listMapKey")
	}

	// 参数数量必须与 listMapKey 完全一致，少一个就无法唯一定位元素。
	if len(criteria) != len(listMeta.keyNames) {
		return nil, nil, fmt.Errorf("number of arguments (%d) does not match number of listMapKey fields (%d)", len(criteria), len(listMeta.keyNames))
	}

	// sortedCriteria 按 listMapKey 顺序重排条件，使生成的匹配表达式顺序稳定，
	// 不受调用方书写参数顺序影响。
	foundKeys := make(map[string]bool)
	sortedCriteria := make([]keyValuePair, 0, len(listMeta.keyNames))
	for _, keyName := range listMeta.keyNames {
		for _, pair := range criteria {
			if pair.key == keyName {
				member := util.GetMemberByJSON(elemT, pair.key)
				if member == nil {
					return nil, nil, fmt.Errorf("no field with JSON name %q", pair.key)
				}
				// We don't need to validate type match here, as it's done during generateComparisonRHS
				foundKeys[keyName] = true
				sortedCriteria = append(sortedCriteria, pair)
				break
			}
		}
	}
	if len(foundKeys) != len(listMeta.keyNames) {
		missing := []string{}
		for _, k := range listMeta.keyNames {
			if !foundKeys[k] {
				missing = append(missing, k)
			}
		}
		return nil, nil, fmt.Errorf("missing required listMapKey fields: %v", missing)
	}
	matchArg, err = createMatchFn(elemT, sortedCriteria)
	if err != nil {
		return nil, nil, err
	}
	directComparable := util.IsDirectComparable(util.NonPointer(util.NativeType(elemT)))
	if directComparable {
		equivArg = Identifier(validateDirectEqual)
	} else {
		equivArg = Identifier(validateSemanticDeepEqual)
	}
	return matchArg, equivArg, nil
}

// criteriaFromArgs 把命名参数转换成条件列表，并拒绝重复键：
// 同一键出现两次会让匹配条件自相矛盾。
func criteriaFromArgs(args []codetags.Arg) ([]keyValuePair, error) {
	criteria := []keyValuePair{}
	keysSeen := sets.Set[string]{}

	for _, arg := range args {
		if arg.Name == "" {
			return nil, fmt.Errorf("all arguments must be named")
		}
		if keysSeen.Has(arg.Name) {
			return nil, fmt.Errorf("duplicate argument: %q", arg.Name)
		}
		keysSeen.Insert(arg.Name)

		parsedValue, valueType, err := parseTypedValue(arg.Value, arg.Type)
		if err != nil {
			return nil, fmt.Errorf("invalid value for argument %q: %w", arg.Name, err)
		}

		criteria = append(criteria, keyValuePair{
			key:       arg.Name,
			value:     parsedValue,
			valueType: valueType,
		})
	}
	return criteria, nil
}

// parseTypedValue 按检测到的类型解析取值；解析失败时带上字段名上下文，方便定位是哪个选择条件写错了。
func parseTypedValue(value string, argType codetags.ArgType) (any, codetags.ValueType, error) {
	switch argType {
	case codetags.ArgTypeString:
		return value, codetags.ValueTypeString, nil
	case codetags.ArgTypeInt:
		intVal, err := util.ParseInt(value)
		if err != nil {
			return nil, "", fmt.Errorf("invalid integer: %w", err)
		}
		return intVal, codetags.ValueTypeInt, nil
	case codetags.ArgTypeBool:
		boolVal, err := util.ParseBool(value)
		if err != nil {
			return nil, "", fmt.Errorf("invalid boolean: %w", err)
		}
		return boolVal, codetags.ValueTypeBool, nil
	default:
		return nil, "", fmt.Errorf("unsupported argument type: %q", argType)
	}
}

// Docs 返回标签文档，说明参数必须是 listMapKey 字段的 JSON 名。
func (itv itemTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            itv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(itv.ValidScopes()),
		Description: "Declares a validation for an item of a slice declared as a +k8s:listType=map. " +
			"The item to match is declared by providing field-value pair arguments. All key fields must be specified.",
		Usage: "+k8s:item(stringKey: \"value\", intKey: 42, boolKey: true)=<validation-tag>",
		Docs: "Arguments must be named with the JSON names of the list-map key fields. " +
			"Values can be strings, integers, or booleans. " +
			"For example: +k8s:item(name: \"myname\", priority: 10, enabled: true)=<chained-validation-tag>",
		AcceptsUnknownArgs: true,
		Payloads: []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "The tag to evaluate for the matching list item.",
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
	return doc
}

// buildMatchConditions 生成 "item.k1 == v1 && item.k2 == v2" 形式的表达式。
// 指针键需要额外判非 nil，避免对 nil 解引用。
func buildMatchConditions(elemT *types.Type, criteria []keyValuePair, itemRef string) (string, error) {
	var conditions []string

	for _, fld := range criteria {
		member := util.GetMemberByJSON(elemT, fld.key)
		if member == nil {
			return "", fmt.Errorf("no field with JSON name %q", fld.key)
		}
		// Generate the comparison based on the field's actual type
		rhs, err := generateComparisonRHS(member, fld.value)
		if err != nil {
			return "", err
		}
		if member.Type.Kind == types.Pointer {
			conditions = append(conditions, fmt.Sprintf("%s.%s != nil && *%s.%s == %s", itemRef, member.Name, itemRef, member.Name, rhs))
		} else {
			conditions = append(conditions, fmt.Sprintf("%s.%s == %s", itemRef, member.Name, rhs))
		}
	}

	return strings.Join(conditions, " && "), nil
}

// createMatchFn 把条件表达式包装成函数字面量；参数固定为 *elemT，
// 因为生成的包装函数总是接收元素指针。
func createMatchFn(elemT *types.Type, criteria []keyValuePair) (FunctionLiteral, error) {
	condition, err := buildMatchConditions(elemT, criteria, "item")
	if err != nil {
		return FunctionLiteral{}, err
	}

	return FunctionLiteral{
		Parameters: []ParamResult{{"item", types.PointerTo(elemT)}},
		Results:    []ParamResult{{"", types.Bool}},
		Body:       fmt.Sprintf("return %s", condition),
	}, nil
}

// generateComparisonRHS 把标签参数值渲染成与字段类型匹配的 Go 字面量。
// 只支持 string/bool/整数键：其他类型无法从注释中的字面量可靠地构造。
func generateComparisonRHS(member *types.Member, value any) (string, error) {
	memberType := util.NonPointer(util.NativeType(member.Type))
	switch {
	case memberType == types.String:
		strVal, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("type mismatch, field is string but value is not: %v", value)
		}
		return fmt.Sprintf("%q", strVal), nil

	case memberType == types.Bool:
		boolVal, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("type mismatch, field is bool but value is not: %v", value)
		}
		return fmt.Sprintf("%t", boolVal), nil

	case types.IsInteger(memberType):
		intVal, ok := value.(int)
		if !ok {
			return "", fmt.Errorf("type mismatch, field is int but value is not: %v", value)
		}
		return fmt.Sprintf("%d", intVal), nil

	default:
		return "", fmt.Errorf("unsupported type %s for field %s", memberType.String(), member.Name)
	}
}

// selectorString creates a field path for list map items using a JSON-like
// syntax. The format is {"key": "value", "key2": 42, "key3": true} with quoted
// keys and appropriately formatted values.
func selectorString(criteria []keyValuePair) string {
	var pairs []string
	for _, fld := range criteria {
		valueStr := formatValueForPath(fld.value)
		pairs = append(pairs, fmt.Sprintf("%q: %s", fld.key, valueStr))
	}
	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

// formatValueForPath 把值与 selectorString 的 JSON 风格格式对齐；
// 非 string/int/bool 只用 %v 兜底（正常不会出现）。
func formatValueForPath(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case int:
		return fmt.Sprintf("%d", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// generateSelector 把条件转换成结构化选择器，供错误路径与 ratcheting 识别元素。
func generateSelector(criteria []keyValuePair) []ListSelectorTerm {
	terms := make([]ListSelectorTerm, len(criteria))
	for i, pair := range criteria {
		terms[i] = ListSelectorTerm{
			Field: pair.key,
			Value: pair.value,
		}
	}
	return terms
}
