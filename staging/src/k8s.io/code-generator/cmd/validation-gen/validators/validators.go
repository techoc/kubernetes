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
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"
)

// TagValidator 描述单个校验标签及其用法。要被
// validation-gen 发现，必须注册 TagValidator——见
// RegisterTagValidator。
//
// TagValidators 不应依赖其他 TagValidator 已执行
// 因为用户可以按任意顺序写标签。
//
// 不保证 TagValidators 的执行顺序。
// TagValidators 不应依赖标签顺序，而应
// 在内部累积信息，再用 DeferredGen 完成后续工作。
//
// 每个校验标签对应一个实现；生成器扫描标签时按注册表分派。
type TagValidator interface {
	// Init 初始化实现，且只调用一次。
	Init(cfg Config)

	// TagName 返回该标签的完整名称（不含 "marker" 前缀）。
	// 。
	TagName() string

	// ValidScopes 返回该标签可用的作用域集合。
	ValidScopes() sets.Set[Scope]

	// GetValidations 返回该标签描述的校验逻辑。
	//
	// 返回值可以是函数调用、变量、注释或 Deferred 回调；生成器再决定
	// 哪些真正落到代码里。
	GetValidations(context Context, tag codetags.Tag) (Validations, error)

	// Docs 返回面向用户的标签文档。
	Docs() TagDoc
}

// Config 携带供校验器使用的可选配置信息。
type Config struct {
	// GengoContext 提供 gengo 的生成器 Context，使校验器能
	// 查询各种其他信息。
	GengoContext *generator.Context

	// TagValidator 提供组合校验的方式。
	//
	// 例如，可利用注册表取出内嵌 "+format=IP" 的校验，
	// 从而定义形如 "+myValidator=+format=IP" 的校验，
	// 并用它构造
	// "+myValidator" 标签最终返回的 Validations。
	//
	// 该字段绝不能在 Init 阶段使用，因为届时其他校验器可能尚未
	// 初始化。
	TagValidator TagValidationExtractor

	// InputToCanonicalPkg 把每个输入（API 类型）包映射到其规范的
	// 生成校验包（跨包引用指向的那个包）。
	// 这与生成器定位生成代码时使用的映射一致：
	// Validate_<Type> 函数，使校验器能引用与生成代码同处的
	// 手写函数（如 +k8s:customValidation）。
	InputToCanonicalPkg map[string]string
}

// Scope 描述校验（或潜在校验）所在的位置。
//
// scope 决定标签允许出现的位置，也影响生成代码中路径与循环的构造方式。
type Scope string

// 注意：这些值都是字符串，可直接用于
// "may not be used in %s" 之类的错误消息。
const (
	// ScopeType 表示作用于类型定义的校验，适用于
	// 该类型的所有实例。
	ScopeType Scope = "type definitions"

	// ScopeField 表示作用于某个结构体字段的校验，
	// 只适用于该结构体的该字段。
	ScopeField Scope = "struct fields"

	// ScopeListVal 表示作用于列表所有元素的校验。
	// 列表字段或类型。
	ScopeListVal Scope = "list values"

	// ScopeMapKey 表示作用于 map 所有键的校验。
	// 字段或类型。
	ScopeMapKey Scope = "map keys"

	// ScopeMapVal 表示作用于 map 所有值的校验。
	// 字段或类型。
	ScopeMapVal Scope = "map values"

	// ScopeConst 表示只作用于常量值的校验。
	ScopeConst Scope = "constant values"

	// TODO: 尚不清楚是否需要区分字段的列表值
	// 与 typedef 的列表值。可以让 {type,field} 与
	// {scalar, list, list-value, map, map-key, map-value} 正交（甚至
	// 包括指针？），但这似乎是没必要的工作量，
	// 暂时如此。
)

// Context 描述标签的使用位置，以便校验作用域，
// 并让校验器按需区分不同情形。
//
// 同一份类型在不同位置使用时 Context 不同（Path/ParentPath 不同），
// 校验器据此区分“类型级”与“字段级”语义。
type Context struct {
	// Scope 表示当前在何处考虑该校验。
	Scope Scope

	// Type 描述被校验的类型。Scope 为
	// ScopeType 时是新定义的类型；为 ScopeField 时
	// 是字段类型（可能是指针、别名或两者）。
	// Scope 为 list-value/map-key/map-value 时是
	// 该键或值的类型（同样可能是指针、别名或两者）。
	// Scope 为 ScopeConst 时是常量的类型。
	Type *types.Type

	// Path 是被校验类型或字段的路径，
	// 用于精确定位上下文，例如在相关标签之间传递信息。
	// Scope 为 ScopeType 时是 Go 包路径
	// 与类型名（如 "k8s.io/api/core/v1.Pod"）；为 ScopeField 时
	// 是字段路径（如 "spec.containers[*].image"）。
	// Scope 为 list-value/map-key/map-value 时是
	// 上述类型或字段路径加上后缀，表明
	// 指向键还是值。ScopeConst 时为 nil。
	Path *field.Path

	// Scope 为 ScopeField 时，Member 描述结构体内的字段；
	// 其他 Scope 下为 nil。
	Member *types.Member

	// ListSelector 是一组键值对，表示从列表中
	// 选择元素的条件。Scope 为 ScopeListVal 时非 nil；
	// 空选择器表示
	// 选中列表内所有元素；其他 Scope 下为 nil。
	// Scope, this will be nil.
	ListSelector []ListSelectorTerm

	// ParentPath 是被校验对象的父类型或父字段路径（适用时），
	// 用于唯一标识同一类型在不同位置的校验上下文。
	// Scope 为 ScopeField 时是外层结构体类型或字段路径
	// （取决于标签写在何处）；
	// Scope 为 list-value/map-key/map-value 时是
	// 外层列表或 map 的类型/字段路径；
	// Scope 为 ScopeType 时为 nil。
	// specified). When Scope is ScopeType, this is nil.
	ParentPath *field.Path

	// ParentType 是被校验对象的父类型。
	// Scope 为 ScopeField 时是外层结构体类型；为
	// list-value/map-key/map-value 时是列表或 map 类型；
	// Scope 为 ScopeType 时为 nil。
	ParentType *types.Type

	// Constants 提供被校验类型的全部常量，
	// 仅 Scope 为 ScopeType 时设置。
	Constants []*Constant

	// StabilityLevel 表示相应校验的稳定性级别。
	StabilityLevel ValidationStabilityLevel
}

// Constant 表示一个常量。
type Constant struct {
	Constant *types.Type
	Tags     []codetags.Tag
}

// ListSelectorTerm 表示字段名与值的配对。
//
// 用于 listMap 的 item 选择：Field 是 JSON 名，Value 是待匹配的原生值。
type ListSelectorTerm struct {
	// Field 是待匹配字段的 JSON 名称。
	Field string
	// Value 是待匹配的值，必须是能作为 list-map 键的
	// 基本类型：string、int 或 bool。
	Value any
}

// TagStabilityLevel 表示校验标签的稳定性。
type TagStabilityLevel string

const (
	// TagStabilityLevelAlpha 表示标签语义今后可能变化。
	TagStabilityLevelAlpha TagStabilityLevel = "Alpha"
	// TagStabilityLevelBeta 表示标签语义在可预见的将来
	// 保持不变；用于标签转正为 stable 前的试运行。
	TagStabilityLevelBeta TagStabilityLevel = "Beta"
	// TagStabilityLevelStable 表示标签语义在可预见的将来
	// 保持不变。
	TagStabilityLevelStable TagStabilityLevel = "Stable"
)

// stabilityOrder 给稳定性级别定义全序，便于比较与排序。
var stabilityOrder = map[TagStabilityLevel]int{
	TagStabilityLevelAlpha:  0,
	TagStabilityLevelBeta:   1,
	TagStabilityLevelStable: 2,
}

// ValidationStabilityLevel 表示校验的稳定性级别。
type ValidationStabilityLevel string

const (
	// Alpha 表示声明式校验与手写校验同时运行，但以手写校验为准。
	ValidationStabilityLevelAlpha ValidationStabilityLevel = "Alpha"
	// Beta 表示声明式校验与手写校验同时运行，但以声明式校验为准。
	ValidationStabilityLevelBeta ValidationStabilityLevel = "Beta"
)

// Compare 比较两个稳定性级别并返回整数；任一级
// 别未知时返回错误。
func (s TagStabilityLevel) Compare(other TagStabilityLevel) (int, error) {
	sOrder, okS := stabilityOrder[s]
	if !okS {
		return 0, fmt.Errorf("unknown stability level %q", s)
	}
	otherOrder, okOther := stabilityOrder[other]
	if !okOther {
		return 0, fmt.Errorf("unknown stability level %q", other)
	}
	return sOrder - otherOrder, nil
}

// TagDoc 描述注释标签及其用法。
type TagDoc struct {
	// Tag 是标签名，不含开头的 '+'。
	Tag string
	// StabilityLevel 是标签的稳定性级别。
	StabilityLevel TagStabilityLevel
	// Args 列出该标签可带的参数。
	Args []TagArgDoc `json:",omitempty"`
	// Usage 说明标签的用法，含参数。
	Usage string
	// Description 简述该标签的用途。
	Description string
	// Docs 是面向人的标签说明。
	Docs string
	// Warning 是关于该标签的可选警告。
	Warning string `json:",omitempty"`
	// Scopes 列出该标签可用的位置。
	Scopes []Scope
	// Payloads 列出该标签允许的零个或多个取值形式。若标签
	// 从不带 payload，此列表应为空；若 payload 可选，
	// 则应包含 "<none>" 项。
	Payloads []TagPayloadDoc `json:",omitempty"`
	// PayloadsType 是 payload 的类型。
	PayloadsType codetags.ValueType `json:",omitempty"`
	// PayloadsRequired 为 true 表示 payload 必填。
	PayloadsRequired bool `json:",omitempty"`
	// AcceptsUnknownArgs 为 true 表示接受未知参数
	AcceptsUnknownArgs bool `json:",omitempty"`
}

// Arg 按名字查找参数文档。
func (td TagDoc) Arg(name string) (TagArgDoc, bool) {
	for _, arg := range td.Args {
		if arg.Name == name {
			return arg, true
		}
	}
	return TagArgDoc{}, false
}

// TagArgDoc 描述标签的一个参数。
//
// 例如：
//
//	`+tagName(arg)`
//	`+tagName(name1: arg1, name2: arg2)`
type TagArgDoc struct {
	// 参数名；位置参数不提供。
	Name string
	// Description 简述该参数（如 `<name>`）。
	Description string
	// Type 是参数类型。
	Type codetags.ArgType
	// Required 为 true 表示该参数必填。
	Required bool
	// Default 是未提供参数时的默认值。
	Default string
	// Docs 是面向人的参数说明。
	Docs string
}

// TagPayloadDoc 描述标签的取值（如 `+tagName=tagValue`）。部分
// 标签支持多种 payload，包括 <none>（如 `+tagName`）。
type TagPayloadDoc struct {
	// Description 简述该 payload（如 `<number>`）。
	Description string
	// Docs 是面向人的 payload 说明。
	Docs string
}

// Validations 定义要生成的函数调用与变量，用于执行
// 校验。
//
// Validations 是校验器输出的中间表示：生成器按作用域把这些片段拼装成
// 最终的 Go 代码，并处理 Opaque* 的短路语义。
type Validations struct {
	// Functions 保存需要生成的函数调用，用于执行
	// 校验。这些调用不一定按顺序执行，可能依
	// 据标志等条件排序。
	//
	// 每个函数的签名必须形如：
	//   func(
	//        // standard arguments
	// ctx context.Context
	//        op operation.Operation,
	// fldPath field.Path,
	// value, oldValue <ValueType>, // 始终可为 nil
	//        // additional arguments (optional)
	//        Args[0] <Args[0]Type>,
	//        Args[1] <Args[1]Type>,
	//        ...
	//        Args[N] <Args[N]Type>)
	//
	// 标准参数不在 FunctionGen.Args 列表中。
	Functions []FunctionGen

	// Variables 保存执行校验所需的变量。
	// validation.  Variables are not permitted in every context.
	Variables []VariableGen

	// Comments 保存要输出的注释（不含开头的 "//"）。
	Comments []string

	// OpaqueType 表示被校验的类型是不透明的，
	// 其上的校验不应生成。
	OpaqueType bool

	// OpaqueKeyType 表示被校验 map 的键类型不透明，
	// opaque, and that any validations defined on it should not be emitted.
	OpaqueKeyType bool

	// OpaqueValType 表示被校验 map 或 slice 的值类型
	// 不透明，其上的校验不应
	// 生成。
	OpaqueValType bool

	// Deferred 保存一组回调，在所有其他校验生成完成后执行。
	// 这让校验器可以把决策推迟到掌握更多信息之后
	// （例如其他校验器的情况）。
	// 延迟回调还可返回新的延迟校验，
	// 生成器会迭代处理直到队列耗尽。
	Deferred []DeferredGen
}

// Empty 报告是否既无可生成内容，也没有任何 opaque 标记。
func (v *Validations) Empty() bool {
	return !v.HasEmitable() &&
		!v.OpaqueType &&
		!v.OpaqueKeyType &&
		!v.OpaqueValType
}

// HasEmitable 报告是否存在需要写入生成代码的内容（含 Deferred）。
func (v *Validations) HasEmitable() bool {
	return len(v.Functions) > 0 || len(v.Variables) > 0 || len(v.Comments) > 0 || len(v.Deferred) > 0
}

// AddFunction 追加一个函数调用。
func (v *Validations) AddFunction(fn FunctionGen) {
	v.Functions = append(v.Functions, fn)
}

// AddVariable 追加一个变量声明。
func (v *Validations) AddVariable(vr VariableGen) {
	v.Variables = append(v.Variables, vr)
}

// AddDeferred 追加一个延迟回调。
func (v *Validations) AddDeferred(d DeferredGen) {
	v.Deferred = append(v.Deferred, d)
}

// AddComment 追加一行注释（不含 //）。
func (v *Validations) AddComment(comment string) {
	v.Comments = append(v.Comments, comment)
}

// Add 合并另一组 Validations；opaque 标记按逻辑或聚合，
// 任一方声明不透明即视为不透明。
func (v *Validations) Add(o Validations) {
	v.Functions = append(v.Functions, o.Functions...)
	v.Variables = append(v.Variables, o.Variables...)
	v.Comments = append(v.Comments, o.Comments...)
	v.Deferred = append(v.Deferred, o.Deferred...)
	v.OpaqueType = v.OpaqueType || o.OpaqueType
	v.OpaqueKeyType = v.OpaqueKeyType || o.OpaqueKeyType
	v.OpaqueValType = v.OpaqueValType || o.OpaqueValType
}

// Clone 复制 v，并为其 slice 字段重新分配底层数组。
//
// 深拷贝切片（元素本身是值或回调指针，无需再递归），
// 以便包装函数安全地修改副本而不影响原始 Validations。
func (v Validations) Clone() Validations {
	res := v
	if v.Functions != nil {
		res.Functions = make([]FunctionGen, len(v.Functions))
		copy(res.Functions, v.Functions)
	}
	if v.Variables != nil {
		res.Variables = make([]VariableGen, len(v.Variables))
		copy(res.Variables, v.Variables)
	}
	if v.Comments != nil {
		res.Comments = make([]string, len(v.Comments))
		copy(res.Comments, v.Comments)
	}
	if v.Deferred != nil {
		res.Deferred = make([]DeferredGen, len(v.Deferred))
		copy(res.Deferred, v.Deferred)
	}
	return res
}

// WrapFunctions 对 Validations 中的所有函数应用给定的包装函数，
// 并递归应用到所有 Deferred 校验，传入相应作用域。
// 便于把通用变换（如条件、稳定性级别）
// 应用到所有校验，包括延迟校验。
func WrapFunctions(v Validations, wrapFn func(FunctionGen, DeferredScope) FunctionGen) Validations {
	return wrapFunctionsWithScope(v, wrapFn, ThisContext)
}

// wrapFunctionsWithScope 是 WrapFunctions 的递归实现。
//
// 关键点：Deferred 回调里产生的 Validations 也必须被包装，
// 因此不能简单地在顶层遍历，而要在回调返回后递归调用自身。
func wrapFunctionsWithScope(v Validations, wrapFn func(FunctionGen, DeferredScope) FunctionGen, scope DeferredScope) Validations {
	result := v.Clone()
	result.Functions = nil
	result.Deferred = nil

	for _, fn := range v.Functions {
		result.AddFunction(wrapFn(fn, scope))
	}

	for _, d := range v.Deferred {
		result.AddDeferred(Deferred(d.Scope, func() (Validations, error) {
			inner, err := d.Callback()
			if err != nil {
				return Validations{}, err
			}
			return wrapFunctionsWithScope(inner, wrapFn, d.Scope), nil
		}))
	}

	return result
}

// FunctionFlags 定义校验器的可选属性。多数校验器
// 直接用 DefaultFlags 即可。
type FunctionFlags uint32

// IsSet 在所有目标标志都置位时返回 true。
func (ff FunctionFlags) IsSet(wanted FunctionFlags) bool {
	return (ff & wanted) == wanted
}

const (
	// DefaultFlags 为清晰起见而定义。
	DefaultFlags FunctionFlags = 0

	// ShortCircuit 表示当前校验失败时跳过后续校验。
	// 若有多个校验器都带此标志，它们都会执行；
	// 其中任一失败都会跳过所有非 ShortCircuit
	// 校验器。多数校验器不会致命中断。
	ShortCircuit FunctionFlags = 1 << iota

	// NonError 表示当前校验器失败不应
	// 计入错误，而应触发失败路径的其他效果
	// （如与 ShortCircuit 组合时提前返回）。
	NonError
)

// Conditions 定义资源被校验前必须满足的条件。
// 任一条件不成立时，资源不会被校验。
//
// 对应 +k8s:ifEnabled / +k8s:ifDisabled：生成代码里包一层 if 判断。
type Conditions struct {
	// OptionEnabled 指定条件成立所必须置为 true 的选项名。
	OptionEnabled string

	// OptionDisabled 指定条件成立所必须置为 false 的选项名。
	OptionDisabled string
}

// Empty 报告是否没有任何条件（即始终生效）。
func (c Conditions) Empty() bool {
	return len(c.OptionEnabled) == 0 && len(c.OptionDisabled) == 0
}

// Identifier 是生成器作为标识符输出的名字。
// 标识符用 RawNamer 策略生成。
type Identifier types.Name

// PrivateVar 是生成器作为私有标识符输出的变量名。
// 私有变量用 PrivateNamer 策略生成。
type PrivateVar types.Name

// Function 为给定函数名与额外参数创建 FunctionGen。
func Function(tagName string, flags FunctionFlags, function types.Name, extraArgs ...any) FunctionGen {
	return FunctionGen{
		TagName:  tagName,
		Flags:    flags,
		Function: function,
		Args:     extraArgs,
	}
}

// DeferredScope 表示校验应延迟到何时。
type DeferredScope string

const (
	// ThisContext 把校验延迟到当前上下文（字段或类型）结束。
	ThisContext DeferredScope = "ThisContext"
	// ParentContext 把校验延迟到父上下文结束，便于跨字段汇总数据。
	ParentContext DeferredScope = "ParentContext"
)

// Deferred 为给定回调创建 DeferredGen。
func Deferred(scope DeferredScope, callback func() (Validations, error)) DeferredGen {
	return DeferredGen{
		Scope:    scope,
		Callback: callback,
	}
}

// DeferredGen 描述延迟到
// 稍后再做。
type DeferredGen struct {
	Scope    DeferredScope
	Callback func() (Validations, error)
}

// Emission 描述运行时校验失败时产生的 field.Error。
// 必须与运行时函数通过 .WithOrigin(...) 报出的值一致。
type Emission struct {
	Type   field.ErrorType
	Origin string
	// PathFragment 非空时是由运行时校验器追加到 fldPath 的
	// 静态字段路径片段，报错前追加（如
	// "[*]"，Unique 用它报 field.Duplicate(fldPath.Index(i), ...)
	// 定位到出错元素）。工具遍历 FunctionGen 树重建字段路径时
	// 树以重建运行时会报出的路径。
	PathFragment string
}

// FunctionGen 描述需要生成的函数调用。
type FunctionGen struct {
	// TagName 是触发该函数的标签。
	TagName string

	// Cohort 表示一组相关函数，它们在
	// 一并处理。
	Cohort string

	// Flags 保存该校验函数的选项。
	Flags FunctionFlags

	// Function 是要调用的函数名。
	Function types.Name

	// Args 保存传给函数的参数，可包含：
	// - map、slice、string、int、float、bool 组成的数据字面量
	// - types.Type（引用 universe 中的任意类型）
	// - types.Member（引用当前值的成员）
	// - types.Identifier（引用 universe 中的任意标识符）
	// - validators.WrapperFunction（调用另一个校验函数）
	// - validators.Literal（传字面量）
	// - validators.FunctionLiteral（传函数字面量）
	// - validators.PrivateVar（引用变量）
	//
	// 细节见 toGolangSourceDataLiteral。
	Args []any

	// TypeArgs 为函数的类型参数赋值，用于
	// 需要显式类型实参的泛型函数调用。
	TypeArgs []types.Name

	// Conditions 保存字段被校验前必须成立的条件。
	// validated by this function.
	Conditions Conditions

	// Comments 保存要加到生成代码中的可选注释，
	// 代码（不含开头的 "//"）。
	Comments []string

	// StabilityLevel 表示相应校验的稳定性级别。
	StabilityLevel ValidationStabilityLevel

	// StabilityLevelSelfManaged 表示函数自身已处理稳定性级别，
	// 不应再被 levelTagValidator 包装。
	StabilityLevelSelfManaged bool

	// Emits 非空时声明运行时校验器在失败时
	// 产生的 field.Error。由 WithEmits 设置；包装器与非产出
	// 校验器为空。一次调用可能产生类型或路径片段不同的
	// 错误（如带 NoAddItem 和 NoRemoveItem 的 ValSliceUpdate），
	// 所以这里是切片。
	Emits []Emission
}

// WithTypeArgs 返回带类型实参的派生 FunctionGen。
func (fg FunctionGen) WithTypeArgs(typeArgs ...types.Name) FunctionGen {
	fg.TypeArgs = typeArgs
	return fg
}

// WithConditions 返回带条件的派生 FunctionGen。
func (fg FunctionGen) WithConditions(conditions Conditions) FunctionGen {
	fg.Conditions = conditions
	return fg
}

// WithComments 返回带注释的新 FunctionGen。
func (fg FunctionGen) WithComments(comments ...string) FunctionGen {
	fg.Comments = append(fg.Comments, comments...)
	return fg
}

// WithComment 返回带注释的新 FunctionGen。
func (fg FunctionGen) WithComment(comment string) FunctionGen {
	return fg.WithComments(comment)
}

// WithStabilityLevel 返回指定稳定性级别的 FunctionGen。
func (fg FunctionGen) WithStabilityLevel(level ValidationStabilityLevel) FunctionGen {
	fg.StabilityLevel = level
	return fg
}

// WithEmits 返回声明了运行时校验失败时产生的
// field.Error 的新 FunctionGen。一个函数可能产生多个
// 不同的 (type, path) 组合，需分别传入 Emission。
func (fg FunctionGen) WithEmits(emits ...Emission) FunctionGen {
	fg.Emits = emits
	return fg
}

// Variable 为给定变量名和初始值创建 VariableGen。
func Variable(variable PrivateVar, initializer any) VariableGen {
	return VariableGen{
		Variable:    variable,
		Initializer: initializer,
	}
}

// VariableGen 描述一个需要生成到校验函数中的局部变量。
type VariableGen struct {
	// Variable 保存变量标识符。
	Variable PrivateVar

	// Initializer 是变量的初始值。
	// Initializer 可以是 toGolangSourceDataLiteral 支持的任意函数调用或字面量类型。
	Initializer any
}

// WrapperFunction 描述具有普通校验函数签名
// (op, fldPath, obj, oldObj) 的函数字面量，并调用另一个
// 同签名的校验函数（必要时附带额外参数）。
type WrapperFunction struct {
	Function FunctionGen
	ObjType  *types.Type
	// PathFragment 非空时是由运行时校验器追加到 fldPath 的
	// 包装型 FunctionGen 在调用 Function 前追加到 fldPath 的内容（如
	// "[*]" 表示遍历 slice/map 值，"."+名字 表示子字段）。工具遍历
	// FunctionGen 树重建字段路径时会用到。
	PathFragment string
}

// MultiWrapperFunction 描述具有多个
// 同签名校验函数指纹的函数字面量，并调用
// 其他多个同签名的校验函数。
type MultiWrapperFunction struct {
	Functions []FunctionGen
	ObjType   *types.Type
	// PathFragment 非空时是由运行时校验器追加到 fldPath 的
	// 包装型 FunctionGen 在调用内部函数前追加到 fldPath 的内容
	// （判别模式校验器用 ".<jsonName>"）。工具遍历 FunctionGen 树
	// 重建字段路径时会用到。
	PathFragment string
}

// Literal 是作为校验器参数时
// 将原样输出，不做任何解释。使用时要小心，
// 它不会经过 Namers 处理。
type Literal string

// FunctionLiteral 描述可用作校验器参数的函数字面量。
// 用作校验器的参数。与 WrapperFunction 不同，它不
// 一定具有普通校验函数的签名。
type FunctionLiteral struct {
	Parameters []ParamResult
	Results    []ParamResult
	Body       string
}

// StructLiteral 描述可用作校验器参数的结构体字面量。
// 作为校验器的参数。
type StructLiteral struct {
	// Type 是要生成的结构体字面量的类型。
	Type types.Name
	// TypeArgs 是结构体类型的泛型实参。
	TypeArgs []*types.Type
	Fields   []StructLiteralField
}

// SliceLiteral 描述可用作校验器参数的 slice 字面量。
// 作为校验器的参数。
type SliceLiteral struct {
	// ElementType 是 slice 元素的类型。
	ElementType types.Name
	// ElementTypeArgs 是元素类型的泛型实参。
	ElementTypeArgs []*types.Type
	Elements        []any
}

// StructLiteralField 是 StructLiteral 中的一个字段初始化项。
type StructLiteralField struct {
	Name  string
	Value any
}

// ParamResult 表示函数的参数或返回值。
type ParamResult struct {
	Name string
	Type *types.Type
}

// typeCheck 检查标签的参数类型与值类型是否匹配
// 文档中的声明。
//
// 双向检查：文档声明但标签缺失的必填参数要报错，标签中出现但文档未声明
// 的参数也要报错（除非显式允许未知参数）。payload 类型同样校验。
func typeCheck(tag codetags.Tag, doc TagDoc) error {
	for _, docArg := range doc.Args {
		hasArg := false
		for _, tagArg := range tag.Args {
			if tagArg.Name == docArg.Name {
				hasArg = true
				if docArg.Type != tagArg.Type {
					return fmt.Errorf("argument %q has wrong type: got %s, want %s",
						tagArg, tagArg.Type, docArg.Type)
				}
				break
			}
		}
		if !hasArg && docArg.Required {
			if docArg.Name == "" {
				return fmt.Errorf("missing required positional argument of type %s", docArg.Type)
			} else {
				return fmt.Errorf("missing named argument %q of type %s", docArg.Name, docArg.Type)
			}
		}
	}

	for _, tagArg := range tag.Args {
		if _, ok := doc.Arg(tagArg.Name); !ok {
			if !doc.AcceptsUnknownArgs {
				return fmt.Errorf("unrecognized named argument %q", tagArg)
			}
		}
	}
	if tag.ValueType == codetags.ValueTypeNone {
		if doc.PayloadsRequired {
			return fmt.Errorf("missing required tag value of type %s", doc.PayloadsType)
		}
	} else if doc.PayloadsType != codetags.ValueTypeRaw && tag.ValueType != doc.PayloadsType {
		return fmt.Errorf("tag value has wrong type: got %s, want %s", tag.ValueType, doc.PayloadsType)
	}
	return nil
}
