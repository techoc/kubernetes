/*
Copyright 2022 The Kubernetes Authors.

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

package cel

import (
	"fmt"
	"math"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	noMaxLength = math.MaxInt
)

// NewListType 返回带有指定元素类型的参数化 list 类型。
func NewListType(elem *DeclType, maxItems int64) *DeclType {
	return &DeclType{
		name:         "list",
		ElemType:     elem,
		MaxElements:  maxItems,
		celType:      cel.ListType(elem.CelType()),
		defaultValue: NewListValue(),
		// list 在 JSON 中总可以表示为 []，因此最小序列化尺寸
		// 硬编码为 2
		MinSerializedSize: 2,
	}
}

// NewMapType 返回带有给定 key 类型和元素类型的参数化 map 类型。
func NewMapType(key, elem *DeclType, maxProperties int64) *DeclType {
	return &DeclType{
		name:         "map",
		KeyType:      key,
		ElemType:     elem,
		MaxElements:  maxProperties,
		celType:      cel.MapType(key.CelType(), elem.CelType()),
		defaultValue: NewMapValue(),
		// map 在 JSON 中总可以表示为 {}，因此最小序列化尺寸
		// 硬编码为 2
		MinSerializedSize: 2,
	}
}

// NewObjectType 创建一个对象类型，带限定名和一组字段声明。
func NewObjectType(name string, fields map[string]*DeclField) *DeclType {
	t := &DeclType{
		name:      name,
		Fields:    fields,
		celType:   cel.ObjectType(name),
		traitMask: traits.FieldTesterType | traits.IndexerType,
		// 对象的实际尺寸可能大于此处默认的最小值（{}），但若调用方
		// 往对象里添加属性，则依赖调用方自行相应地修改 MinSerializedSize
		MinSerializedSize: 2,
	}
	t.defaultValue = NewObjectValue(t)
	return t
}

// NewSimpleTypeWithMinSize 构造无结构的标量类型（bool/int/string 等）。
//
// minSize 用于 CEL 表达式的代价估算：估算过小会低估工作量，导致 CEL 成本限制失效，
// 因此每个标量都有一个保守的最小 JSON 序列化尺寸。
func NewSimpleTypeWithMinSize(name string, celType *cel.Type, zeroVal ref.Val, minSize int64) *DeclType {
	return &DeclType{
		name:              name,
		celType:           celType,
		defaultValue:      zeroVal,
		MinSerializedSize: minSize,
	}
}

// DeclType 是 OpenAPIv3 类型的通用类型描述符。
//
// 这是 schema 与 CEL 类型系统之间的桥梁：同时携带 CEL 类型、可访问字段、
// 以及在 JSON 中的最小序列化尺寸（后者服务于运行时代价预算）。
type DeclType struct {
	fmt.Stringer

	name string
	// Fields 是一个 map：key 为转义后的 CEL 标识符字段名，value 为对应的字段声明。
	Fields      map[string]*DeclField
	KeyType     *DeclType
	ElemType    *DeclType
	TypeParam   bool
	Metadata    map[string]string
	MaxElements int64
	// MinSerializedSize 表示该 DeclType 序列化到 JSON 时
	// 可能的最小字节数。
	MinSerializedSize int64

	celType      *cel.Type
	traitMask    int
	defaultValue ref.Val
}

// MaybeAssignTypeName 尝试在类型为 `object` 时把 DeclType 的 name 设为全限定名。
//
// DeclType 必须使 `IsObject` 返回 true，否则这次赋值会报错。
//
// 为匿名嵌套对象生成唯一限定名（如 spec.containers.@idx），否则不同位置的两个
// 结构相同的对象会被 CEL 视为同一类型，字段解析可能串味。
// 递归过程中若没有任何名字发生变化则返回原指针，避免无谓复制。
func (t *DeclType) MaybeAssignTypeName(name string) *DeclType {
	if t.IsObject() {
		objUpdated := false
		if t.name != "object" {
			name = t.name
		} else {
			objUpdated = true
		}
		fieldMap := make(map[string]*DeclField, len(t.Fields))
		for fieldName, field := range t.Fields {
			fieldType := field.Type
			fieldTypeName := fmt.Sprintf("%s.%s", name, fieldName)
			updated := fieldType.MaybeAssignTypeName(fieldTypeName)
			if updated == fieldType {
				fieldMap[fieldName] = field
				continue
			}
			objUpdated = true
			fieldMap[fieldName] = &DeclField{
				Name:         fieldName,
				Type:         updated,
				Required:     field.Required,
				enumValues:   field.enumValues,
				defaultValue: field.defaultValue,
			}
		}
		if !objUpdated {
			return t
		}
		return &DeclType{
			name:              name,
			Fields:            fieldMap,
			KeyType:           t.KeyType,
			ElemType:          t.ElemType,
			TypeParam:         t.TypeParam,
			Metadata:          t.Metadata,
			celType:           cel.ObjectType(name),
			traitMask:         t.traitMask,
			defaultValue:      t.defaultValue,
			MinSerializedSize: t.MinSerializedSize,
		}
	}
	if t.IsMap() {
		elemTypeName := fmt.Sprintf("%s.@elem", name)
		updated := t.ElemType.MaybeAssignTypeName(elemTypeName)
		if updated == t.ElemType {
			return t
		}
		return NewMapType(t.KeyType, updated, t.MaxElements)
	}
	if t.IsList() {
		elemTypeName := fmt.Sprintf("%s.@idx", name)
		updated := t.ElemType.MaybeAssignTypeName(elemTypeName)
		if updated == t.ElemType {
			return t
		}
		return NewListType(updated, t.MaxElements)
	}
	return t
}

// ExprType 返回该声明的 CEL 表达式类型；
// 供较旧的表达树（exprpb.Type）接口使用；新代码应优先用 CelType。
func (t *DeclType) ExprType() (*exprpb.Type, error) {
	return cel.TypeToExprType(t.celType)
}

// CelType 返回该声明对应的 CEL 类型。
func (t *DeclType) CelType() *cel.Type {
	return t.celType
}

// FindField 返回给定名称的 DeclField（若存在）。
func (t *DeclType) FindField(name string) (*DeclField, bool) {
	f, found := t.Fields[name]
	return f, found
}

// HasTrait 实现 CEL ref.Type 接口，使该类型声明可用于 CEL 求值器。
//
// traitMask 显式声明对象支持字段访问/下标；对非 DeclType 的默认值再委托其自身，
// 使标量类型也能正确回答是否可迭代等能力查询。
func (t *DeclType) HasTrait(trait int) bool {
	if t.traitMask&trait == trait {
		return true
	}
	if t.defaultValue == nil {
		return false
	}
	_, isDecl := t.defaultValue.Type().(*DeclType)
	if isDecl {
		return false
	}
	return t.defaultValue.Type().HasTrait(trait)
}

// IsList 返回该声明是否为 `list` 类型：定义了参数化的元素类型，
// 但没有参数化的 key 类型或字段。
//
// 三个 Is* 方法是互斥且完备的分类依据：有 Fields 是对象，有 Key+Elem 是 map，
// 仅 Elem 是 list。任何新形态都必须落进其中一类，否则类型提供者会静默忽略。
func (t *DeclType) IsList() bool {
	return t.KeyType == nil && t.ElemType != nil && t.Fields == nil
}

// IsMap 返回该声明是否为 'map' 类型：定义了参数化的 key 类型和元素类型，
// 但没有字段。
func (t *DeclType) IsMap() bool {
	return t.KeyType != nil && t.ElemType != nil && t.Fields == nil
}

// IsObject 返回该声明是否为 'object' 类型：定义了一组带类型的字段。
func (t *DeclType) IsObject() bool {
	return t.KeyType == nil && t.ElemType == nil && t.Fields != nil
}

// String 实现 fmt.Stringer 接口方法。
func (t *DeclType) String() string {
	return t.name
}

// TypeName 返回 DeclType 的全限定类型名。
func (t *DeclType) TypeName() string {
	return t.name
}

// DefaultValue 返回表示该对象类型默认值的 CEL ref.Val（若存在）。
func (t *DeclType) DefaultValue() ref.Val {
	return t.defaultValue
}

// FieldTypeMap 构造给定类型内部嵌套的字段与对象类型的映射。
//
// 生成的映射同时用于类型注册与嵌套类型的查找，键是对象路径。
func FieldTypeMap(path string, t *DeclType) map[string]*DeclType {
	if t.IsObject() && t.TypeName() != "object" {
		path = t.TypeName()
	}
	types := make(map[string]*DeclType)
	buildDeclTypes(path, t, types)
	return types
}

// buildDeclTypes 递归登记对象、map 与 list 的嵌套类型。
//
// list 与 map 的元素用 "@idx"/"@elem" 后缀区分，防止与同路径的对象类型冲突。
func buildDeclTypes(path string, t *DeclType, types map[string]*DeclType) {
	// 确保对象类型按其 schema 中出现的位置正确命名。
	if t.IsObject() {
		// 一个取巧的做法：保证名字唯一限定，并与类型解析步骤良好配合——
		// 字段解析要求使用全限定类型名才能正常工作。
		types[t.TypeName()] = t
		for name, field := range t.Fields {
			fieldPath := fmt.Sprintf("%s.%s", path, name)
			buildDeclTypes(fieldPath, field.Type, types)
		}
	}
	// 必要时把 map 的元素属性映射到类型名。
	if t.IsMap() {
		mapElemPath := fmt.Sprintf("%s.@elem", path)
		buildDeclTypes(mapElemPath, t.ElemType, types)
		types[path] = t
	}
	// list 的元素属性。
	if t.IsList() {
		listIdxPath := fmt.Sprintf("%s.@idx", path)
		buildDeclTypes(listIdxPath, t.ElemType, types)
		types[path] = t
	}
}

// DeclField 描述类型内部某个字段声明的名称、序号（ordinal）与是否必填。
type DeclField struct {
	Name         string
	Type         *DeclType
	Required     bool
	enumValues   []interface{}
	defaultValue interface{}
}

// NewDeclField 构造字段声明；enumValues 与 defaultValue 可为 nil。
func NewDeclField(name string, declType *DeclType, required bool, enumValues []interface{}, defaultValue interface{}) *DeclField {
	return &DeclField{
		Name:         name,
		Type:         declType,
		Required:     required,
		enumValues:   enumValues,
		defaultValue: defaultValue,
	}
}

// TypeName 返回该字段的字符串类型名。
func (f *DeclField) TypeName() string {
	return f.Type.TypeName()
}

// DefaultValue 返回与该字段关联的零值；
// 字段级默认值优先于类型默认值；后者是 schema 的类型零值。
func (f *DeclField) DefaultValue() ref.Val {
	if f.defaultValue != nil {
		return types.DefaultTypeAdapter.NativeToValue(f.defaultValue)
	}
	return f.Type.DefaultValue()
}

// EnumValues 返回该字段可取的值集合；
// 始终返回非 nil 切片，使调用方可以直接 range 而不必判空。
func (f *DeclField) EnumValues() []ref.Val {
	if f.enumValues == nil || len(f.enumValues) == 0 {
		return []ref.Val{}
	}
	ev := make([]ref.Val, len(f.enumValues))
	for i, e := range f.enumValues {
		ev[i] = types.DefaultTypeAdapter.NativeToValue(e)
	}
	return ev
}

// allTypesForDecl 把所有根类型展开成"类型名 -> 声明"的扁平映射，
// 供类型提供者在编译表达式时按名解析任意嵌套类型。
func allTypesForDecl(declTypes []*DeclType) map[string]*DeclType {
	if declTypes == nil {
		return nil
	}
	allTypes := map[string]*DeclType{}
	for _, declType := range declTypes {
		for k, t := range FieldTypeMap(declType.TypeName(), declType) {
			allTypes[k] = t
		}
	}

	return allTypes
}

// NewDeclTypeProvider 返回一个基于 Open API Schema、与 CEL 兼容的类型系统。
func NewDeclTypeProvider(rootTypes ...*DeclType) *DeclTypeProvider {
	// 注意：若 schema 表明其实际基于另一个 proto，则应优先采用 proto 定义。
	// 对于 proto 中的表达式，将需要新增一个字段注解来标明该表达式期望的
	// 环境与类型。
	allTypes := allTypesForDecl(rootTypes)
	return &DeclTypeProvider{
		registeredTypes: allTypes,
	}
}

// DeclTypeProvider 扩展了 CEL ref.TypeProvider 接口，提供基于 Open API Schema
// 的类型系统。
type DeclTypeProvider struct {
	registeredTypes             map[string]*DeclType
	typeProvider                types.Provider
	typeAdapter                 types.Adapter
	recognizeKeywordAsFieldName bool
}

// SetRecognizeKeywordAsFieldName 允许把 CEL 保留字当作字段名解析；
// 与之配套的字段在 schema 中会以 "__name__" 形式存在。
func (rt *DeclTypeProvider) SetRecognizeKeywordAsFieldName(recognize bool) {
	rt.recognizeKeywordAsFieldName = recognize
}

func (rt *DeclTypeProvider) EnumValue(enumName string) ref.Val {
	return rt.typeProvider.EnumValue(enumName)
}

func (rt *DeclTypeProvider) FindIdent(identName string) (ref.Val, bool) {
	return rt.typeProvider.FindIdent(identName)
}

// EnvOptions 返回一组 cel.EnvOption，包含声明集以及自定义的 ref.TypeProvider。
//
// 若 DeclTypeProvider 为 nil，则返回空的 []cel.EnvOption 集合。
func (rt *DeclTypeProvider) EnvOptions(tp types.Provider) ([]cel.EnvOption, error) {
	if rt == nil {
		return []cel.EnvOption{}, nil
	}
	rtWithTypes, err := rt.WithTypeProvider(tp)
	if err != nil {
		return nil, err
	}
	return []cel.EnvOption{
		cel.CustomTypeProvider(rtWithTypes),
		cel.CustomTypeAdapter(rtWithTypes),
	}, nil
}

// WithTypeProvider 返回一个新的 DeclTypeProvider，其中设置了给定的 TypeProvider。
// 若原 DeclTypeProvider 为 nil，返回的 DeclTypeProvider 仍然是 nil。
//
// 同时校验外部提供者中同名类型的定义是否与本 schema 一致：不一致意味着表达式在
// 编译期与运行期看到不同结构，必须立即报错而不是取其一。
func (rt *DeclTypeProvider) WithTypeProvider(tp types.Provider) (*DeclTypeProvider, error) {
	if rt == nil {
		return nil, nil
	}
	var ta types.Adapter = types.DefaultTypeAdapter
	tpa, ok := tp.(types.Adapter)
	if ok {
		ta = tpa
	}
	rtWithTypes := &DeclTypeProvider{
		typeProvider:                tp,
		typeAdapter:                 ta,
		registeredTypes:             rt.registeredTypes,
		recognizeKeywordAsFieldName: rt.recognizeKeywordAsFieldName,
	}
	for name, declType := range rt.registeredTypes {
		tpType, found := tp.FindStructType(name)
		// 把 celType 转换为 types.type

		expT := declType.CelType()
		if found && !expT.IsExactType(tpType) {
			return nil, fmt.Errorf(
				"type %s definition differs between CEL environment and type provider", name)
		}

	}
	return rtWithTypes, nil
}

// FindStructType 尝试解析来自规则的 rule-schema 的 typeName，若解析不到，
// 则回退到内嵌的 ref.TypeProvider。
//
// FindStructType 覆盖了内嵌 TypeProvider 默认的查找类型行为。
//
// 注意：当类型名基于 Open API Schema 时，名字将反映该类型定义所在的对象路径。
func (rt *DeclTypeProvider) FindStructType(typeName string) (*types.Type, bool) {
	if rt == nil {
		return nil, false
	}
	declType, found := rt.findDeclType(typeName)
	if found {
		expT := declType.CelType()
		return types.NewTypeTypeWithParam(expT), found
	}
	return rt.typeProvider.FindStructType(typeName)
}

// FindDeclType 返回可映射为 CEL 类型的 CPT 类型描述。
func (rt *DeclTypeProvider) FindDeclType(typeName string) (*DeclType, bool) {
	if rt == nil {
		return nil, false
	}
	return rt.findDeclType(typeName)
}

// FindStructFieldNames 返回该类型关联的字段名（若类型能找到）。
func (rt *DeclTypeProvider) FindStructFieldNames(typeName string) ([]string, bool) {
	return []string{}, false
}

// FindStructFieldType 在找到时根据类型名与字段名返回字段类型。
//
// 注意：Open API Schema 类型的类型名通常是其限定对象路径。
// 假如未来传入的是对象实例而非类型名，字段解析会更准确地反映期望的类型模型。
// 但在此处，为了与现有 CEL 接口对齐，做出了一些妥协。
func (rt *DeclTypeProvider) FindStructFieldType(typeName, fieldName string) (*types.FieldType, bool) {
	st, found := rt.findDeclType(typeName)
	if !found {
		return rt.typeProvider.FindStructFieldType(typeName, fieldName)
	}

	f, found := st.Fields[fieldName]
	// 保留字字段以 "__x__" 存储，按需做二次查找，使表达式可以写成 r.x 而不必写 __x__。
	if rt.recognizeKeywordAsFieldName && !found && celReservedSymbols.Has(fieldName) {
		f, found = st.Fields["__"+fieldName+"__"]
	}

	if found {
		ft := f.Type
		expT := ft.CelType()
		return &types.FieldType{
			Type: expT,
		}, true
	}
	// 这可能是一个动态 map。
	if st.IsMap() {
		et := st.ElemType
		expT := et.CelType()
		return &types.FieldType{
			Type: expT,
		}, true
	}
	return nil, false
}

// NativeToValue 是 ref.TypeAdapater 接口的实现，支持把规则值转换为
// CEL ref.Val 实例。
func (rt *DeclTypeProvider) NativeToValue(val interface{}) ref.Val {
	return rt.typeAdapter.NativeToValue(val)
}

// NewValue 目前委托给底层 typeProvider；对象实例化（变异准入需要）尚未实现。
func (rt *DeclTypeProvider) NewValue(typeName string, fields map[string]ref.Val) ref.Val {
	// TODO: 为 OpenAPI 类型实现该功能以支持 CEL 对象实例化，
	// 变异准入（mutating admission）需要此能力。
	return rt.typeProvider.NewValue(typeName, fields)
}

// TypeNames 返回 DeclTypeProvider 对象中声明的类型名列表。
func (rt *DeclTypeProvider) TypeNames() []string {
	// 预先分配长度，避免返回的切片在调用方 append 时产生共享底层数组的意外。
	typeNames := make([]string, len(rt.registeredTypes))
	i := 0
	for name := range rt.registeredTypes {
		typeNames[i] = name
		i++
	}
	return typeNames
}

// findDeclType 先查已注册类型，再尝试匹配内置标量类型。
func (rt *DeclTypeProvider) findDeclType(typeName string) (*DeclType, bool) {
	declType, found := rt.registeredTypes[typeName]
	if found {
		return declType, true
	}
	declType = findScalar(typeName)
	return declType, declType != nil
}

// findScalar 按类型名匹配内置标量；不支持的名字返回 nil，交由其他提供者处理。
func findScalar(typename string) *DeclType {
	switch typename {
	case BoolType.TypeName():
		return BoolType
	case BytesType.TypeName():
		return BytesType
	case DoubleType.TypeName():
		return DoubleType
	case DurationType.TypeName():
		return DurationType
	case IntType.TypeName():
		return IntType
	case NullType.TypeName():
		return NullType
	case StringType.TypeName():
		return StringType
	case TimestampType.TypeName():
		return TimestampType
	case UintType.TypeName():
		return UintType
	case ListType.TypeName():
		return ListType
	case MapType.TypeName():
		return MapType
	default:
		return nil
	}
}

var (
	// AnyType 等价于 CEL 的 'protobuf.Any' 类型，值可以是任意受支持的类型；
	// any/dyn 的最小尺寸取 1（JSON null），因为编译期无法确定实际类型。
	AnyType = NewSimpleTypeWithMinSize("any", cel.AnyType, nil, 1)

	// BoolType 等价于 CEL 的 'bool' 类型。
	BoolType = NewSimpleTypeWithMinSize("bool", cel.BoolType, types.False, MinBoolSize)

	// BytesType 等价于 CEL 的 'bytes' 类型。
	BytesType = NewSimpleTypeWithMinSize("bytes", cel.BytesType, types.Bytes([]byte{}), MinStringSize)

	// DoubleType 等价于 CEL 的 'double' 类型，即 64 位浮点值。
	DoubleType = NewSimpleTypeWithMinSize("double", cel.DoubleType, types.Double(0), MinNumberSize)

	// DurationType 等价于 CEL 的 'duration' 类型。
	DurationType = NewSimpleTypeWithMinSize("duration", cel.DurationType, types.Duration{Duration: time.Duration(0)}, MinDurationSizeJSON)

	// DateType 等价于 CEL 的 'date' 类型。
	DateType = NewSimpleTypeWithMinSize("date", cel.TimestampType, types.Timestamp{Time: time.Time{}}, JSONDateSize)

	// DynType 等价于 CEL 的 'dyn' 概念，表示类型将在运行时而非编译期确定。
	DynType = NewSimpleTypeWithMinSize("dyn", cel.DynType, nil, 1)

	// IntType 等价于 CEL 的 'int' 类型，即 64 位有符号整数。
	IntType = NewSimpleTypeWithMinSize("int", cel.IntType, types.IntZero, MinNumberSize)

	// NullType 等价于 CEL 的 'null_type'。
	NullType = NewSimpleTypeWithMinSize("null_type", cel.NullType, types.NullValue, 4)

	// StringType 等价于 CEL 的 'string' 类型，预期为 UTF-8 字符串。
	// StringType 的值可以是字符串字面量，也可以是表达式字符串。
	StringType = NewSimpleTypeWithMinSize("string", cel.StringType, types.String(""), MinStringSize)

	// TimestampType 对应 CEL 支持的知名类型 protobuf.Timestamp。
	// 注意：OpenAPI 的 date 和 date-time 类型都映射到 TimestampType，
	// 因此并非所有标记为 Timestamp 的类型的 MinSerializedSize 都相同。
	TimestampType = NewSimpleTypeWithMinSize("timestamp", cel.TimestampType, types.Timestamp{Time: time.Time{}}, JSONDateSize)

	// QuantityDeclType 包装了一个 [QuantityType]，使其可用于期望
	// [DeclType] 的函数。
	QuantityDeclType = NewSimpleTypeWithMinSize("quantity", QuantityType, Quantity{Quantity: resource.NewQuantity(0, resource.DecimalSI)}, 8)

	// UintType 等价于 CEL 的 'uint' 类型。
	UintType = NewSimpleTypeWithMinSize("uint", cel.UintType, types.Uint(0), 1)

	// ListType 等价于 CEL 的 'list' 类型。
	ListType = NewListType(AnyType, noMaxLength)

	// MapType 等价于 CEL 的 'map' 类型。
	MapType = NewMapType(AnyType, AnyType, noMaxLength)
)
