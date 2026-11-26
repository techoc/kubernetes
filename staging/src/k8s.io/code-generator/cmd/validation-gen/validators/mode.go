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
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/parser/tags"
	"k8s.io/gengo/v2/types"
)

const (
	modeDiscriminatorTagName = "k8s:modeDiscriminator"
	ifModeTagName            = "k8s:ifMode"
)

// validGroupNameRegex 限制 discriminator 组名必须是以字母开头、
// 只含字母数字与下划线的标识符。
var validGroupNameRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

func init() {
	RegisterTagValidator(&modeDiscriminatorTagValidator{discriminatorDefinitions})
	RegisterTagValidator(&ifModeTagValidator{discriminatorDefinitions, nil})
}

// discriminatorDefinitions 保存所有标签校验器发现的 discriminator 定义。
// key 是结构体的路径；用全局表是为了让 modeDiscriminator 与 ifMode 两个
// 独立标签校验器共享状态。
// discriminatorDefinitions 存储标签校验器发现的全部 discriminator 定义，键为结构体路径。
var discriminatorDefinitions = map[string]discriminatorGroups{}

// discriminatorGroups 是同一结构体下按组名索引的 discriminator 组。
type discriminatorGroups map[string]*discriminatorGroup

// discriminatorGroup 表示一个 discriminator 组：判别字段及其各成员的规则。
type discriminatorGroup struct {
	name                string
	discriminatorMember *types.Member
	// members 把字段名映射到该字段在本组中的规则。
	// members 把字段名映射到其在该 discriminator 组中的规则。
	members map[string]*fieldMemberRules
}

// fieldMemberRules 保存一个字段在某个 discriminator 组中的成员规则。
type fieldMemberRules struct {
	member *types.Member
	rules  []memberRule
}

// memberRule 是一条“当 discriminator 等于 value 时生效”的校验规则。
type memberRule struct {
	value          string
	validations    Validations
	stabilityLevel ValidationStabilityLevel
}

// getOrCreate 返回指定组名的组，不存在则创建；空名归一化为 "default"。
func (mg discriminatorGroups) getOrCreate(name string) *discriminatorGroup {
	if name == "" {
		name = "default"
	}
	g, ok := mg[name]
	if !ok {
		g = &discriminatorGroup{
			name:    name,
			members: make(map[string]*fieldMemberRules),
		}
		mg[name] = g
	}
	return g
}

// modeDiscriminatorTagValidator 实现 +k8s:modeDiscriminator：声明状态判别字段。
type modeDiscriminatorTagValidator struct {
	shared map[string]discriminatorGroups
}

func (mdtv *modeDiscriminatorTagValidator) Init(_ Config) {}

func (mdtv *modeDiscriminatorTagValidator) TagName() string {
	return modeDiscriminatorTagName
}

func (mdtv *modeDiscriminatorTagValidator) ValidScopes() sets.Set[Scope] {
	return sets.New(ScopeField)
}

// GetValidations 校验判别字段的类型并登记其所属组；实际校验由
// getDiscriminatorValidations 在延迟阶段统一生成。
func (mdtv *modeDiscriminatorTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	if util.NativeType(context.Type).Kind == types.Pointer {
		return Validations{}, fmt.Errorf("can only be used on non-pointer types")
	}

	// 判别值只能是 string 或 bool。
	if t := util.NonPointer(util.NativeType(context.Type)); t.Kind != types.Builtin || (t.Name.Name != "string" && t.Name.Name != "bool") {
		return Validations{}, fmt.Errorf("can only be used on string or bool types (%s)", rootTypeString(context.Type, t))
	}

	if mdtv.shared[context.ParentPath.String()] == nil {
		mdtv.shared[context.ParentPath.String()] = make(discriminatorGroups)
	}
	// 通过 modality 参数指定组名；缺省为 default 组。
	groupName := ""
	if nameArg, ok := tag.NamedArg("modality"); ok {
		groupName = nameArg.Value
	}
	if groupName != "" && !validGroupNameRegex.MatchString(groupName) {
		return Validations{}, fmt.Errorf("discriminator group name must match %s, got %q", validGroupNameRegex.String(), groupName)
	}
	if groupName == "default" {
		return Validations{}, fmt.Errorf("discriminator group name %q is reserved", groupName)
	}
	group := mdtv.shared[context.ParentPath.String()].getOrCreate(groupName)
	// 同一组只能有一个判别字段。
	if group.discriminatorMember != nil && group.discriminatorMember != context.Member {
		return Validations{}, fmt.Errorf("duplicate discriminator: %q", groupName)
	}
	group.discriminatorMember = context.Member

	// 这里只记录 discriminator 元数据，校验由延迟阶段统一生成。
	// 配置 discriminator 元数据，实际校验由 modeDiscriminatorTagValidator 输出。
	return Validations{}, nil
}

func (mdtv *modeDiscriminatorTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            mdtv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(mdtv.ValidScopes()),
		Description:    "Indicates that this field is a discriminator for state-based validation.",
		Args: []TagArgDoc{{
			Name:        "modality",
			Description: "<string>",
			Docs:        "the name of the discriminator group, if more than one exists",
			Type:        codetags.ArgTypeString,
		}},
	}
}

// ifModeTagValidator 实现 +k8s:ifMode：按 discriminator 取值条件性地施加校验。
type ifModeTagValidator struct {
	shared    map[string]discriminatorGroups
	validator TagValidationExtractor
}

func (imtv *ifModeTagValidator) Init(cfg Config) {
	imtv.validator = cfg.TagValidator
}

func (imtv *ifModeTagValidator) TagName() string {
	return ifModeTagName
}

func (imtv *ifModeTagValidator) ValidScopes() sets.Set[Scope] {
	return sets.New(ScopeField)
}

// GetValidations 记录“字段 + discriminator 值 + 校验”的规则，
// 并通过延迟生成统一产出 Discriminated 校验。
func (imtv *ifModeTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	if tag.ValueTag == nil {
		return Validations{}, fmt.Errorf("missing required payload")
	}

	groupName := ""
	if modeArg, ok := tag.NamedArg("modality"); ok {
		groupName = modeArg.Value
	}
	if groupName == "default" {
		return Validations{}, fmt.Errorf("discriminator group name %q is reserved", groupName)
	}

	// mode 可用命名参数或位置参数给出。
	value := ""
	if valArg, ok := tag.NamedArg("mode"); ok {
		value = valArg.Value
	} else if len(tag.Args) > 0 && tag.Args[0].Name == "" {
		// 位置参数。
		value = tag.Args[0].Value
	} else {
		return Validations{}, fmt.Errorf("missing required mode")
	}

	if imtv.shared[context.ParentPath.String()] == nil {
		imtv.shared[context.ParentPath.String()] = make(discriminatorGroups)
	}
	group := imtv.shared[context.ParentPath.String()].getOrCreate(groupName)

	fieldName := context.Member.Name
	if rules, ok := group.members[fieldName]; ok {
		// 同一字段多次出现时，member 必须是同一个，否则说明内部状态不一致。
		if rules.member != context.Member {
			return Validations{}, fmt.Errorf("internal error: member mismatch for field %q", fieldName)
		}
	} else {
		group.members[fieldName] = &fieldMemberRules{
			member: context.Member,
		}
	}

	payloadValidations, err := imtv.validator.ExtractTagValidations(context, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	group.members[fieldName].rules = append(group.members[fieldName].rules, memberRule{
		value:          value,
		validations:    payloadValidations,
		stabilityLevel: context.StabilityLevel,
	})

	// 延迟到父级上下文统一生成，确保所有成员规则都已收集完毕。
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ParentContext, func() (Validations, error) {
				return getDiscriminatorValidations(imtv.shared, context)
			}),
		},
	}, nil
}

func (imtv *ifModeTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            imtv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(imtv.ValidScopes()),
		Description:    "Indicates that this field's validation depends on a mode discriminator.",
		Args: []TagArgDoc{{
			Name:        "", // positional
			Description: "<string>",
			Docs:        "the value of the mode discriminator for which this validation applies",
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "modality",
			Description: "<string>",
			Docs:        "the name of the discriminator group",
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "mode",
			Description: "<string>",
			Docs:        "the mode value for which this validation applies",
			Type:        codetags.ArgTypeString,
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
}

// getDiscriminatorValidations 把已收集的 discriminator 组转换为 Discriminated 校验。
func getDiscriminatorValidations(shared map[string]discriminatorGroups, context Context) (Validations, error) {
	structPath := context.ParentPath.String()

	// 取尽可能具体的类型；非结构体（如 typedef 到结构体）则不做处理。
	// 尽可能取出最具体的类型。
	if k := util.NonPointer(util.NativeType(context.ParentType)).Kind; k != types.Struct {
		return Validations{}, nil
	}

	groups, ok := shared[structPath]
	if !ok || len(groups) == 0 {
		return Validations{}, nil
	}
	// 删除以避免同一结构体被处理两次。
	// 删除条目，确保同一结构体不被处理两次。
	delete(shared, structPath)

	var result Validations

	// 对组名排序，保证生成输出确定。
	// 对组名排序，保证输出确定。
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	slices.Sort(groupNames)

	for _, gn := range groupNames {
		group := groups[gn]
		if group.discriminatorMember == nil {
			// 有成员规则却没有判别字段，说明声明不完整。
			if len(group.members) > 0 {
				if gn == "default" {
					return Validations{}, fmt.Errorf("missing discriminator")
				}
				return Validations{}, fmt.Errorf("missing discriminator for group %q", gn)
			}
			continue
		}
		// 对字段名排序，保证输出稳定。
		fieldNames := make([]string, 0, len(group.members))
		for name := range group.members {
			fieldNames = append(fieldNames, name)
		}
		slices.Sort(fieldNames)

		for _, fn := range fieldNames {
			rules := group.members[fn]
			v, err := generateMemberFieldValidation(context.ParentType, group, rules)
			if err != nil {
				return Validations{}, err
			}
			result.Add(v)
		}
	}

	return result, nil
}

// generateMemberFieldValidation 为一个成员字段生成 Discriminated 校验：
// 按 discriminator 取值分桶，每个取值对应一组包装后的校验。
func generateMemberFieldValidation(structType *types.Type, group *discriminatorGroup, rules *fieldMemberRules) (Validations, error) {
	fieldType := rules.member.Type

	// 使用可空形式以处理缺省值：非可空类型统一取指针。
	// 用可空形式处理缺失值。
	nilableFieldType := fieldType
	fieldExprPrefix := ""
	if !util.IsNilableType(nilableFieldType) {
		nilableFieldType = types.PointerTo(nilableFieldType)
		fieldExprPrefix = "&"
	}

	// 取字段的 JSON 名称，错误路径与序列化保持一致。
	// 取字段的 JSON 名。
	jsonName := rules.member.Name
	if jt, ok := tags.LookupJSON(*rules.member); ok {
		jsonName = jt.Name
	}

	// Default-Forbidden 在 Discriminated 运行时中于 structPath.Child(jsonName) 执行，
	// 因此路径片段为 "." + jsonName。
	// Default-Forbidden 在 Discriminated 运行时的 structPath.Child(jsonName) 处执行，
	// 归属路径记为 "." + jsonName。
	defaultForbidden, err := getForbiddenValidation(fieldType, "."+jsonName)
	if err != nil {
		return Validations{}, err
	}

	// 准备 DiscriminatedRules：先按取值聚合规则，再按取值排序。
	// 在聚合前标记每条规则的稳定性级别，使同一取值的不同规则可携带不同级别。
	// 准备 DiscriminatedRules：按取值聚合前先给每条规则的校验函数打上稳定性级别，
	// 使同一取值的不同规则可携带不同级别。
	rulesByValue := make(map[string]*Validations)
	var values []string
	for _, rule := range rules.rules {
		// 按首次出现顺序记录唯一取值。
		// 按首次出现顺序记录不同的 discriminator 取值。
		if _, ok := rulesByValue[rule.value]; !ok {
			rulesByValue[rule.value] = &Validations{}
			values = append(values, rule.value)
		}
		// 合并前先标记每个校验函数的稳定性级别。
		// 合并前给每个校验函数打上稳定性级别。
		v := rule.validations
		if rule.stabilityLevel != "" {
			marked := make([]FunctionGen, len(v.Functions))
			for i, f := range v.Functions {
				marked[i] = f.WithStabilityLevel(rule.stabilityLevel)
			}
			v.Functions = marked
		}
		// 累加到相同 discriminator 取值的桶中。
		// 把该规则的校验与同一 discriminator 取值的其他规则累积到一起。
		rulesByValue[rule.value].Add(v)
	}
	slices.Sort(values)

	// 当所有规则共享同一稳定性级别时，default-forbidden（针对未识别的
	// discriminator 取值）也应标记为该级别，使错误带一致的注解。
	// 所有规则稳定性级别一致时，default-forbidden（针对无法识别的 discriminator 取值）
	// 也应标记为同一级别，使其错误带上相同的稳定性标注。
	if uniformLevel := uniformStabilityLevel(rules.rules); uniformLevel != "" {
		mwf, ok := defaultForbidden.(MultiWrapperFunction)
		if !ok {
			return Validations{}, fmt.Errorf("internal error: defaultForbidden is not a MultiWrapperFunction")
		}
		marked := make([]FunctionGen, len(mwf.Functions))
		for i, f := range mwf.Functions {
			marked[i] = f.WithStabilityLevel(uniformLevel)
		}
		mwf.Functions = marked
		defaultForbidden = mwf
	}

	discriminatorType := group.discriminatorMember.Type
	var discriminatedRules []any
	for _, val := range values {
		wrapper := MultiWrapperFunction{
			Functions: rulesByValue[val].Functions,
			ObjType:   nilableFieldType,
			// 每个 mode 的规则同样在 structPath.Child(jsonName) 执行。
			// per-mode 规则同样在 structPath.Child(jsonName) 处执行。
			PathFragment: "." + jsonName,
		}

		// 把字符串形式的 tag 值转换为 discriminator 类型对应的 Go 字面量。
		// 把字符串标签值转换为 discriminator 类型对应的 Go 字面量。
		typedValue, err := convertDiscriminatorValue(val, discriminatorType)
		if err != nil {
			return Validations{}, fmt.Errorf("invalid discriminator value %q: %w", val, err)
		}

		discriminatedRules = append(discriminatedRules, StructLiteral{
			Type:     types.Name{Package: libValidationPkg, Name: "DiscriminatedRule"},
			TypeArgs: []*types.Type{nilableFieldType, discriminatorType},
			Fields: []StructLiteralField{
				{Name: "Value", Value: typedValue},
				{Name: "Validation", Value: wrapper},
			},
		})
	}

	discriminatedValidator := types.Name{Package: libValidationPkg, Name: "Discriminated"}

	rulesSlice := SliceLiteral{
		ElementType:     types.Name{Package: libValidationPkg, Name: "DiscriminatedRule"},
		ElementTypeArgs: []*types.Type{nilableFieldType, discriminatorType},
		Elements:        discriminatedRules,
	}

	// getValue 提取被校验的成员字段。
	// getValue 提取器。
	getValue := FunctionLiteral{
		Parameters: []ParamResult{{Name: "obj", Type: types.PointerTo(structType)}},
		Results:    []ParamResult{{Type: nilableFieldType}},
		Body:       fmt.Sprintf("return %sobj.%s", fieldExprPrefix, rules.member.Name),
	}

	// getDiscriminator 提取判别字段。
	// getDiscriminator 提取器。
	getDiscriminator := FunctionLiteral{
		Parameters: []ParamResult{{Name: "obj", Type: types.PointerTo(structType)}},
		Results:    []ParamResult{{Type: discriminatorType}},
		Body:       fmt.Sprintf("return obj.%s", group.discriminatorMember.Name),
	}

	// directComparable 决定 ratcheting 时使用 "==" 还是语义 DeepEqual。
	// directComparable 判断在 ratcheting 中查找和比较关联的列表元素时，
	// 能否直接用 "=="，还是必须用语义化的 DeepEqual。
	var equivArg any
	if util.IsDirectComparable(util.NonPointer(util.NativeType(fieldType))) {
		equivArg = Identifier(validateDirectEqual)
	} else {
		equivArg = Identifier(validateSemanticDeepEqual)
	}

	fn := Function(modeDiscriminatorTagName, DefaultFlags, discriminatedValidator,
		Literal(fmt.Sprintf("%q", jsonName)),
		getValue,
		getDiscriminator,
		equivArg,
		defaultForbidden,
		rulesSlice,
	)
	// 稳定性级别已设置在被包裹的函数上，跳过上游的级别包装；
	// 否则上游会覆盖已设置的级别。
	// 稳定性级别已在被包装的校验函数上设置，上游不要再包一层：
	// 否则会覆盖被包装校验器的级别。
	fn.StabilityLevelSelfManaged = true
	return Validations{Functions: []FunctionGen{fn}}, nil
}

// uniformStabilityLevel 返回所有规则共同的稳定性级别；若不一致则返回 ""。
// uniformStabilityLevel 在所有规则级别一致时返回该级别，否则返回空串。
func uniformStabilityLevel(rules []memberRule) ValidationStabilityLevel {
	level := rules[0].stabilityLevel
	for i := 1; i < len(rules); i++ {
		if rules[i].stabilityLevel != level {
			return ""
		}
	}
	return level
}

// getForbiddenValidation 返回一个 MultiWrapperFunction，按 t 的 Kind 选择
// 合适的 validate.Forbidden* 运行时调用。pathFragment 是包装器的路径片段。
// getForbiddenValidation 返回 MultiWrapperFunction，包装与 t 类型匹配的
// validate.Forbidden* 调用；pathFragment 是包装器的 PathFragment。
func getForbiddenValidation(t *types.Type, pathFragment string) (any, error) {
	var forbidden types.Name
	nt := util.NativeType(t)
	switch nt.Kind {
	case types.Slice:
		forbidden = types.Name{Package: libValidationPkg, Name: "ForbiddenSlice"}
	case types.Map:
		forbidden = types.Name{Package: libValidationPkg, Name: "ForbiddenMap"}
	case types.Pointer:
		forbidden = types.Name{Package: libValidationPkg, Name: "ForbiddenPointer"}
	case types.Struct:
		// 结构体成员必须用指针表示，否则无法区分“未设置”。
		return nil, fmt.Errorf("discriminated member fields of struct type must be pointers")
	default:
		forbidden = types.Name{Package: libValidationPkg, Name: "ForbiddenValue"}
	}

	fg := Function(forbiddenTagName, DefaultFlags, forbidden).
		WithEmits(Emission{field.ErrorTypeForbidden, "", ""})

	// 使用可空形式以匹配标准校验函数签名。
	// 用可空形式匹配标准校验函数签名。
	wrapperObjType := t
	if !util.IsNilableType(t) {
		wrapperObjType = types.PointerTo(t)
	}

	return MultiWrapperFunction{
		Functions:    []FunctionGen{fg},
		ObjType:      wrapperObjType,
		PathFragment: pathFragment,
	}, nil
}

// convertDiscriminatorValue 把字符串 tag 值转换为 discriminator 类型对应的
// 有类型 Go 字面量；目前仅支持 string 与 bool。
// convertDiscriminatorValue 把字符串标签值转换为给定 discriminator 类型的 Go 字面量。
func convertDiscriminatorValue(val string, discType *types.Type) (any, error) {
	nt := util.NonPointer(util.NativeType(discType))
	if nt.Kind != types.Builtin {
		return nil, fmt.Errorf("unsupported discriminator type: %s", nt.Name.Name)
	}

	switch nt.Name.Name {
	case "string":
		return val, nil
	case "bool":
		b, err := util.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as bool: %w", val, err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported discriminator type: %s", nt.Name.Name)
	}
}
