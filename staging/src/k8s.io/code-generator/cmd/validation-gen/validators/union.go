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
	"fmt"
	"regexp"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/parser/tags"
	"k8s.io/gengo/v2/types"
)

var discriminatedUnionValidator = types.Name{Package: libValidationPkg, Name: "DiscriminatedUnion"}
var unionValidator = types.Name{Package: libValidationPkg, Name: "Union"}

var newDiscriminatedUnionMember = types.Name{Package: libValidationPkg, Name: "NewDiscriminatedUnionMember"}
var newDiscriminatedUnionMembership = types.Name{Package: libValidationPkg, Name: "NewDiscriminatedUnionMembership"}
var newUnionMember = types.Name{Package: libValidationPkg, Name: "NewUnionMember"}
var newUnionMembership = types.Name{Package: libValidationPkg, Name: "NewUnionMembership"}
var unionVariablePrefix = "unionMembershipFor"

// init 注册 union 相关的两个标记校验器。
// 一个 union 由多个标记共同描述，它们必须共享状态：
//   - 字段型 union：标记在结构体字段上，校验生成在结构体上；
//   - 列表项型 union：标记在列表元素上（经 +k8s:item），校验生成在列表上。
//
// 两个校验器通过同一个 shared 映射交换信息。
func init() {
	// "shared" 把字段路径字符串（键）映射到 union 定义（值）。
	// 键示例：
	//   - 结构体 union："MyStruct"（校验生成在结构体类型上）
	//   - 列表 union："Pipeline.Tasks"（校验生成在列表字段上）
	shared := map[string]unions{}
	RegisterTagValidator(unionDiscriminatorTagValidator{shared})
	RegisterTagValidator(unionMemberTagValidator{shared})
}

// getUnionValidations 在成员标记全部处理完后，把收集到的 union 定义翻译为校验。
// 它由 unionMemberTagValidator 的 Deferred（ParentContext）回调触发，
// 此时同一结构体/列表上的所有成员标记都已注册完毕。
func getUnionValidations(shared map[string]unions, context Context) (Validations, error) {
	// union 以 ParentPath 为键（字段作用域），其他作用域则用 Path。
	structPath := context.ParentPath
	parentType := context.ParentType

	// TODO: Add support for map items once map item validation is implemented

	// 取尽可能具体的类型：union 只能作用于结构体或切片。
	if k := util.NonPointer(util.NativeType(parentType)).Kind; k != types.Struct && k != types.Slice {
		return Validations{}, nil
	}

	unions := shared[structPath.String()]
	if len(unions) == 0 {
		return Validations{}, nil
	}
	// 消费后删除，避免同一路径的 union 被重复生成（例如包被扫描两次）。
	delete(shared, structPath.String())

	result, err := processUnionValidations(structPath, parentType, unions, unionVariablePrefix,
		unionMemberTagName, unionValidator, discriminatedUnionValidator,
		Emission{field.ErrorTypeInvalid, "union", ""})
	return result, err
}

const (
	unionDiscriminatorTagName = "k8s:unionDiscriminator"
	unionMemberTagName        = "k8s:unionMember"
)

// unionDiscriminatorTagValidator 处理 +k8s:unionDiscriminator。
type unionDiscriminatorTagValidator struct {
	shared map[string]unions
}

// Init 无状态，无需初始化。
func (unionDiscriminatorTagValidator) Init(_ Config) {}

// TagName 返回标记名。
func (unionDiscriminatorTagValidator) TagName() string {
	return unionDiscriminatorTagName
}

// unionTagValidScopes 由 discriminator 与 member 两个校验器共享：
// 二者都可出现在字段或列表元素上。
var unionTagValidScopes = sets.New(ScopeField, ScopeListVal)

// ValidScopes 返回允许的作用域。
func (unionDiscriminatorTagValidator) ValidScopes() sets.Set[Scope] {
	return unionTagValidScopes
}

// GetValidations 记录 discriminator 信息，本身不产生校验。
// 校验统一由成员校验器的延迟回调生成，以保证所有成员信息已收集完整。
func (udtv unionDiscriminatorTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	err := processDiscriminatorValidations(udtv.shared, context, tag)
	if err != nil {
		return Validations{}, err
	}
	// 为 union 配置 discriminator；校验由 union member 校验器发出。
	return Validations{}, nil
}

// Docs 返回 discriminator 标记的文档。
func (udtv unionDiscriminatorTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            udtv.TagName(),
		StabilityLevel: TagStabilityLevelBeta,
		Scopes:         sets.List(udtv.ValidScopes()),
		Description:    "Indicates that this field is the discriminator for a union.",
		Args: []TagArgDoc{{
			Name:        "union",
			Description: "<string>",
			Docs:        "the name of the union, if more than one exists",
			Type:        codetags.ArgTypeString,
		}},
	}
}

// unionMemberTagValidator 处理 +k8s:unionMember。
type unionMemberTagValidator struct {
	shared map[string]unions
}

// Init 无状态，无需初始化。
func (unionMemberTagValidator) Init(_ Config) {}

// TagName 返回标记名。
func (unionMemberTagValidator) TagName() string {
	return unionMemberTagName
}

// ValidScopes 返回允许的作用域。
func (unionMemberTagValidator) ValidScopes() sets.Set[Scope] {
	return unionTagValidScopes
}

// GetValidations 记录成员信息，并注册一个延迟校验：
// 延迟到父上下文再生成，是因为必须等同一 union 的所有成员都被看到。
func (umtv unionMemberTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	err := processMemberValidations(umtv.shared, context, tag)
	if err != nil {
		return Validations{}, err
	}
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ParentContext, func() (Validations, error) {
				return getUnionValidations(umtv.shared, context)
			}),
		},
	}, nil
}

// Docs 返回成员标记的文档。
func (umtv unionMemberTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            umtv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(umtv.ValidScopes()),
		Description:    "Indicates that this field is a member of a union.",
		Args: []TagArgDoc{{
			Name:        "union",
			Description: "<string>",
			Docs:        "the name of the union, if more than one exists",
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "memberName",
			Description: "<string>",
			Docs:        "the discriminator value for this member",
			Default:     "the field's name",
			Type:        codetags.ArgTypeString,
		}},
	}
}

// union 定义一次 union 校验的生成方式。union 可以由一组结构体字段
// （可带 discriminator）组成，也可以由一组列表项（以选择条件表示）组成。
type union struct {
	// members 提供 union 全部成员的信息。
	// 每项含 fieldName 与 discriminatorValue 对，其中 name 标识字段或
	// 选择器（用于错误消息），discriminatorValue 是带 discriminator 的
	// union 中用于命名该成员的值。
	members []unionMember

	// fieldMembers 描述结构体字段 union 的全部成员。
	// 与 itemMembers 互斥。
	fieldMembers []*types.Member

	// discriminator 是 discriminator 字段名。
	discriminator *string
	// discriminatorMember 描述 discriminator 字段。
	discriminatorMember *types.Member

	// itemMembers 保存列表项 union 全部成员的选择条件。
	// 与 fieldMembers 互斥。map 键是“字段名”
	// （如 `field[{"name": "succeeded"}]`），值是选择条件列表。
	itemMembers map[string][]ListSelectorTerm
}

// unionMember 是 union 的一个成员：错误消息中显示的字段名与可选的
// discriminator 值。
type unionMember struct {
	fieldName          string
	discriminatorValue string
}

// unions 表示某个 Go 结构体上的全部 union，以 union 名为键。
type unions map[string]*union

// newUnion 初始化一个 union 实例。
func newUnion() *union {
	return &union{
		// 切片字段可以为 nil。
		itemMembers: make(map[string][]ListSelectorTerm),
	}
}

// getOrCreate 按名字取 union，不存在则新建。
func (us unions) getOrCreate(name string) *union {
	var u *union
	var ok bool
	if u, ok = us[name]; !ok {
		u = newUnion()
		us[name] = u
	}
	return u
}

// processUnionValidations 把一组 union 定义转换为变量（成员集合）与函数（校验）。
// 变量先声明、函数再引用，避免在每次校验时重建成员表。
func processUnionValidations(structPath *field.Path, parentType *types.Type, unions unions, varPrefix string,
	tagName string, undiscriminatedValidator types.Name, discriminatedValidator types.Name, emits Emission,
) (Validations, error) {
	result := Validations{}

	// 对键排序以保证输出稳定。
	keys := make([]string, 0, len(unions))
	for k := range unions {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, unionName := range keys {
		u := unions[unionName]
		if len(u.fieldMembers) > 0 || u.discriminator != nil || len(u.itemMembers) > 0 {
			// 字段型与列表项型不能混用：二者的成员语义与错误路径完全不同。
			if len(u.fieldMembers) > 0 && len(u.itemMembers) > 0 {
				return Validations{}, fmt.Errorf("cannot have both field members and item members")
			}
			nativeType := util.NonPointer(util.NativeType(parentType))
			if nativeType.Kind == types.Struct && len(u.itemMembers) > 0 {
				return Validations{}, fmt.Errorf("struct type cannot have item members")
			}
			if nativeType.Kind == types.Slice && len(u.fieldMembers) > 0 {
				return Validations{}, fmt.Errorf("slice type cannot have field members")
			}

			// TODO: Avoid the "local" here. This was added to avoid errors caused when the package is an empty string.
			//       The correct package would be the output package but is not known here. This does not show up in generated code.
			// TODO: Append a consistent hash suffix to avoid generated name conflicts?
			varBaseName := sanitizeName(structPath.String() + "_" + unionName) // unionName 可以为 ""
			supportVarName := PrivateVar{Name: varPrefix + "_" + varBaseName, Package: "local"}

			var extractorArgs []any
			ptrType := types.PointerTo(parentType)

			// 处理字段型 union。
			for _, member := range u.fieldMembers {
				extractor := createMemberExtractor(ptrType, member)
				extractorArgs = append(extractorArgs, extractor)
			}

			// 处理列表型 union 的列表项。
			if nativeType.Kind == types.Slice && len(u.itemMembers) > 0 {
				elemType := util.NonPointer(nativeType.Elem)

				// 对键排序以保证输出稳定。
				keys := make([]string, 0, len(u.itemMembers))
				for key := range u.itemMembers {
					keys = append(keys, key)
				}
				slices.Sort(keys)

				for _, fullPath := range keys {
					selector := u.itemMembers[fullPath]
					extractor, err := createItemExtractor(parentType, elemType, selector)
					if err != nil {
						return Validations{}, err
					}
					extractorArgs = append(extractorArgs, extractor)
				}
			}

			if u.discriminator != nil {
				// 带 discriminator：成员按 discriminator 值匹配。
				supportVar := Variable(supportVarName,
					Function(tagName, DefaultFlags, newDiscriminatedUnionMembership,
						append([]any{*u.discriminator}, getMemberArgs(u, parentType, true)...)...))
				result.Variables = append(result.Variables, supportVar)

				// discriminator 提取器把字段值转换为 string，供运行时比较。
				discriminatorExtractor := FunctionLiteral{
					Parameters: []ParamResult{{Name: "obj", Type: ptrType}},
					Results:    []ParamResult{{Type: types.String}},
					Body:       fmt.Sprintf("if obj == nil {return \"\"}; return string(obj.%s)", u.discriminatorMember.Name), // Cast to string
				}

				extraArgs := append([]any{supportVarName, discriminatorExtractor}, extractorArgs...)
				fn := Function(tagName, DefaultFlags, discriminatedValidator, extraArgs...).
					WithEmits(emits)
				result.Functions = append(result.Functions, fn)
			} else {
				// 无 discriminator：按“至少/至多一个成员被设置”判断。
				supportVar := Variable(supportVarName, Function(tagName, DefaultFlags, newUnionMembership, getMemberArgs(u, parentType, false)...))
				result.Variables = append(result.Variables, supportVar)

				extraArgs := append([]any{supportVarName}, extractorArgs...)
				fn := Function(tagName, DefaultFlags, undiscriminatedValidator, extraArgs...).
					WithEmits(emits)
				result.Functions = append(result.Functions, fn)
			}
		}
	}

	return result, nil
}

// createMemberExtractor 为字段型 union 成员生成“是否被设置”的判定函数。
// 不同 Kind 的判定方式不同：指针判 nil，map/slice 判长度，内建类型与零值比较。
func createMemberExtractor(ptrType *types.Type, member *types.Member) FunctionLiteral {
	extractor := FunctionLiteral{
		Parameters: []ParamResult{{Name: "obj", Type: ptrType}},
		Results:    []ParamResult{{Type: types.Bool}},
	}
	nt := util.NativeType(member.Type)
	switch nt.Kind {
	case types.Pointer:
		extractor.Body = fmt.Sprintf("if obj == nil {return false}; return obj.%s != nil", member.Name)
	case types.Map, types.Slice:
		extractor.Body = fmt.Sprintf("if obj == nil {return false}; return len(obj.%s) != 0", member.Name)
	case types.Builtin:
		extractor.Body = fmt.Sprintf("if obj == nil {return false}; var z %s; return obj.%s != z", member.Type, member.Name)
	default:
		// 这种情况本应在更早阶段被拦截，这里兜底。
		extractor.Body = fmt.Sprintf("if obj == nil {return false}; return false /* unsupported union member kind: %s */", nt.Kind)
	}
	return extractor
}

// createItemExtractor 为列表项 union 成员生成提取器：
// 遍历列表，判断是否存在满足选择条件的元素。
func createItemExtractor(listType *types.Type, elemType *types.Type, selector []ListSelectorTerm) (FunctionLiteral, error) {
	var criteria []keyValuePair
	for _, term := range selector {
		criteria = append(criteria, keyValuePair{
			key:   term.Field,
			value: fmt.Sprint(term.Value),
		})
	}

	// 排序以保证输出稳定。
	slices.SortFunc(criteria, func(a, b keyValuePair) int {
		return strings.Compare(a.key, b.key)
	})

	condition, err := buildMatchConditions(elemType, criteria, "list[i]")
	if err != nil {
		return FunctionLiteral{}, err
	}

	extractor := FunctionLiteral{
		Parameters: []ParamResult{{Name: "list", Type: listType}},
		Results:    []ParamResult{{Type: types.Bool}},
		Body: fmt.Sprintf(
			`for i := range list {
				if %s {
					return true
				}
			 }
			 return false`, condition),
	}

	return extractor, nil
}

// processDiscriminatorValidations 处理 union discriminator 标记。
// 它写成自由函数而非方法，以便其他类似 union 的标记复用。
func processDiscriminatorValidations(shared map[string]unions, context Context, tag codetags.Tag) error {
	// 该标记可作用于值字段、指针字段以及 typedef（typedef 不应该是指针）。
	// 因此需要检查具体类型。
	if t := util.NonPointer(util.NativeType(context.Type)); t != types.String {
		return fmt.Errorf("can only be used on string types (%s)", rootTypeString(context.Type, t))
	}
	if shared[context.ParentPath.String()] == nil {
		shared[context.ParentPath.String()] = unions{}
	}
	unionArg, _ := tag.NamedArg("union") // 可选
	u := shared[context.ParentPath.String()].getOrCreate(unionArg.Value)

	var discriminatorFieldName string
	if jsonAnnotation, ok := tags.LookupJSON(*context.Member); ok {
		// 使用 JSON 名而不是 Go 字段名，保证错误消息与 API 字段路径一致。
		discriminatorFieldName = jsonAnnotation.Name
		u.discriminator = &discriminatorFieldName
		u.discriminatorMember = context.Member
	}

	return nil
}

// processMemberValidations 处理字段与列表项的 union 成员标记。
// 它写成自由函数而非方法，以便其他类似 union 的标记复用。
func processMemberValidations(shared map[string]unions, context Context, tag codetags.Tag) error {
	switch context.Scope {
	case ScopeField:
		return processFieldMemberValidations(shared, context, tag)
	case ScopeListVal:
		return processListMemberValidations(shared, context, tag)
	}
	return fmt.Errorf("can only be used on fields and list items: %v", context.Scope)
}

// processFieldMemberValidations 处理结构体字段的 union 成员标记。
// 它写成自由函数而非方法，以便其他类似 union 的标记复用。
func processFieldMemberValidations(shared map[string]unions, context Context, tag codetags.Tag) error {
	nt := util.NativeType(context.Member.Type)
	switch nt.Kind {
	case types.Pointer, types.Map, types.Slice, types.Builtin:
		// OK
	default:
		// 特别地，不支持非指针结构体（值类型无法表达“未设置”）。
		return fmt.Errorf("can only be used on nilable and primitive types (%s)", nt.Kind)
	}
	if context.Member == nil {
		return fmt.Errorf("struct-field union member has no member info in context")
	}

	jsonTag, ok := tags.LookupJSON(*context.Member)
	if !ok {
		return fmt.Errorf("field %q is a union member but has no JSON struct field tag", context.Member)
	}
	fieldName := jsonTag.Name
	if len(fieldName) == 0 {
		return fmt.Errorf("field %q is a union member but has no JSON name", context.Member)
	}

	if shared[context.ParentPath.String()] == nil {
		shared[context.ParentPath.String()] = unions{}
	}

	// 检查标记是否指定了成员名。
	memberName := context.Member.Name                        // 默认值
	if memberNameArg, ok := tag.NamedArg("memberName"); ok { // 可选
		memberName = memberNameArg.Value
	}

	unionArg, _ := tag.NamedArg("union") // 可选
	u := shared[context.ParentPath.String()].getOrCreate(unionArg.Value)
	u.members = append(u.members, unionMember{fieldName, memberName})

	u.fieldMembers = append(u.fieldMembers, context.Member)

	return nil
}

// processListMemberValidations 处理列表项的 union 成员标记。
// 它写成自由函数而非方法，以便其他类似 union 的标记复用。
func processListMemberValidations(shared map[string]unions, context Context, tag codetags.Tag) error {
	if context.ListSelector == nil {
		return fmt.Errorf("list-item union member has no list selector in context")
	}

	// 严格说不是“字段”，但接近。错误消息里已包含字段名，这里取一个
	// 便于人阅读的名字：`field[{"name": "succeeded"}]`。
	fieldName := lastPathElement(context.Path)

	if shared[context.ParentPath.String()] == nil {
		shared[context.ParentPath.String()] = unions{}
	}

	// 检查标记是否指定了成员名。
	memberName := ""
	if memberNameArg, ok := tag.NamedArg("memberName"); ok { // 可选
		memberName = memberNameArg.Value
	}

	unionArg, _ := tag.NamedArg("union") // 可选
	u := shared[context.ParentPath.String()].getOrCreate(unionArg.Value)
	u.members = append(u.members, unionMember{fieldName, memberName})

	// 同一列表项路径只能属于一个 union 成员，重复会导致生成的匹配条件歧义。
	if _, found := u.itemMembers[fieldName]; found {
		return fmt.Errorf("list-item union member %q already exists", fieldName)
	}
	u.itemMembers[fieldName] = context.ListSelector

	return nil
}

// lastPathElement 返回字段路径的最后一段。
func lastPathElement(path *field.Path) string {
	parts := strings.Split(path.String(), ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// getMemberArgs 构造用于初始化 union 成员的参数列表。
func getMemberArgs(u *union, _ *types.Type, discrim bool) []any {
	members := make([]any, 0, len(u.members))
	for _, f := range u.members {
		fieldName := f.fieldName
		memberName := f.discriminatorValue
		if discrim {
			members = append(members, Function("unused", 0, newDiscriminatedUnionMember, fieldName, memberName))
		} else {
			members = append(members, Function("unused", 0, newUnionMember, fieldName))
		}
	}
	return members
}

// sanitizeName 把任意字符串转换为合法的 Go 标识符。
func sanitizeName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	return re.ReplaceAllString(name, "_")
}
