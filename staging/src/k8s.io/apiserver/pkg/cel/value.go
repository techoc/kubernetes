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
	"reflect"
	"sync"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// 用于记录原始 YAML/JSON 中的排版风格，序列化回文本时尽量保持用户书写习惯。
type EncodeStyle int

const (
	// BlockValueStyle 是默认的字符串编码风格，保留空白字符和换行。
	BlockValueStyle EncodeStyle = iota

	// FlowValueStyle 表示该字符串是复杂类型的行内（inline）表示形式。
	FlowValueStyle

	// FoldedValueStyle 是多行字符串，其空白字符与换行会被裁剪为单个
	// 空白字符；但连续的换行只会被替换为单个换行，而不是单个空白字符。
	FoldedValueStyle

	// LiteralStyle 是多行字符串，保留换行，但把其他所有空白字符
	// 裁剪为单个字符。
	LiteralStyle
)

// NewEmptyDynValue 返回零值 DynValue。
func NewEmptyDynValue() *DynValue {
	// 注意：0 不是合法的 parse node identifier。
	dv, _ := NewDynValue(0, nil)
	return dv
}

// NewDynValue 返回与某个 parse node id 和值对应的 DynValue。
func NewDynValue(id int64, val interface{}) (*DynValue, error) {
	dv := &DynValue{ID: id}
	err := dv.SetValue(val)
	return dv, err
}

// DynValue 是动态类型值，用于描述非结构化内容。
// 值是否符合期望的类型，取决于它在 Instance 或 Template 中被使用的位置，
// 以及是否存在施加更严格类型定义的 schema。
//
// value 保存 Go 原生值，exprValue 是其 CEL 表示，declType 是声明类型。
// 三者由 SetValue 同步更新，避免出现类型与值不一致的中间状态。
type DynValue struct {
	ID          int64
	EncodeStyle EncodeStyle
	value       interface{}
	exprValue   ref.Val
	declType    *DeclType
}

// DeclType 返回该 dyn 值对应的策略模型类型。
func (dv *DynValue) DeclType() *DeclType {
	return dv.declType
}

// ConvertToNative 是 CEL ref.Val 方法的实现，用于在 CEL 类型与
// Go 原生类型之间做适配。
//
// 该方法的默认行为是先转换为 CEL 类型（CEL 类型有一套明确定义的
// 转换规则），再代理调用该类型的 CEL ConvertToNative 方法。
func (dv *DynValue) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	ev := dv.ExprValue()
	// 求值阶段的错误优先返回，避免在错误值上继续做类型转换。
	if types.IsError(ev) {
		return nil, ev.(*types.Err)
	}
	return ev.ConvertToNative(typeDesc)
}

// Equal 返回该 dyn 值是否与给定的 CEL 值相等。
func (dv *DynValue) Equal(other ref.Val) ref.Val {
	dvType := dv.Type()
	otherType := other.Type()
	// CEL 规定不同类型之间不可比较；这里不返回 false 而是“无此重载”错误。
	if dvType.TypeName() != otherType.TypeName() {
		return types.MaybeNoSuchOverloadErr(other)
	}
	switch v := dv.value.(type) {
	case ref.Val:
		return v.Equal(other)
	case PlainTextValue:
		return celBool(string(v) == other.Value().(string))
	case *MultilineStringValue:
		// 比较解析后的 Value 而非 Raw，使排版差异不影响语义相等。
		return celBool(v.Value == other.Value().(string))
	case time.Duration:
		otherDuration := other.Value().(time.Duration)
		return celBool(v == otherDuration)
	case time.Time:
		otherTimestamp := other.Value().(time.Time)
		return celBool(v.Equal(otherTimestamp))
	default:
		return celBool(reflect.DeepEqual(v, other.Value()))
	}
}

// ExprValue 将 DynValue 转换为 CEL 值。
func (dv *DynValue) ExprValue() ref.Val {
	return dv.exprValue
}

// Value 返回该引用持有的底层值。
func (dv *DynValue) Value() interface{} {
	return dv.value
}

// SetValue 更新该引用持有的底层值：同时重建 CEL 值与声明类型；失败时保留原始 value 但返回错误，调用方不应继续使用。
func (dv *DynValue) SetValue(value interface{}) error {
	dv.value = value
	var err error
	dv.exprValue, dv.declType, err = exprValue(value)
	return err
}

// Type 返回给定值对应的 CEL 类型。
func (dv *DynValue) Type() ref.Type {
	return dv.ExprValue().Type()
}

// exprValue 把 Go 原生值映射为 CEL 值与声明类型。
// 仅支持策略模型允许的类型集合，其他类型直接报错，防止未定义语义的值进入求值。
func exprValue(value interface{}) (ref.Val, *DeclType, error) {
	switch v := value.(type) {
	case bool:
		return types.Bool(v), BoolType, nil
	case []byte:
		return types.Bytes(v), BytesType, nil
	case float64:
		return types.Double(v), DoubleType, nil
	case int64:
		return types.Int(v), IntType, nil
	case string:
		return types.String(v), StringType, nil
	case uint64:
		return types.Uint(v), UintType, nil
	case time.Duration:
		return types.Duration{Duration: v}, DurationType, nil
	case time.Time:
		return types.Timestamp{Time: v}, TimestampType, nil
	case types.Null:
		return v, NullType, nil
	case *ListValue:
		return v, ListType, nil
	case *MapValue:
		return v, MapType, nil
	case *ObjectValue:
		return v, v.objectType, nil
	default:
		return nil, unknownType, fmt.Errorf("unsupported type: (%T)%v", v, v)
	}
}

// PlainTextValue 是纯文本字符串字面量，不得当作表达式处理：与普通 string 区分，避免被误当作 CEL 表达式再次求值。
type PlainTextValue string

// MultilineStringValue 是多行字符串值：Value 是按省略空白的方式解析后的形式，
// Raw 则是保留空白的原始形式。
type MultilineStringValue struct {
	Value string
	Raw   string
}

// newStructValue 构造内部共享的字段容器；ObjectValue 与 MapValue 复用同一实现。
func newStructValue() *structValue {
	return &structValue{
		Fields:   []*Field{},
		fieldMap: map[string]*Field{},
	}
}

// structValue 同时维护有序字段列表与按名索引，前者保证遍历稳定，后者保证 O(1) 查找。
type structValue struct {
	Fields   []*Field
	fieldMap map[string]*Field
}

// 重复添加同名字段会覆盖索引但保留列表中的旧项；调用方应避免重复。
func (sv *structValue) AddField(field *Field) {
	sv.Fields = append(sv.Fields, field)
	sv.fieldMap[field.Name] = field
}

// ConvertToNative 把 MapValue 类型转换为 Go 原生类型。
func (sv *structValue) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if typeDesc.Kind() != reflect.Map &&
		typeDesc.Kind() != reflect.Struct &&
		typeDesc.Kind() != reflect.Pointer &&
		typeDesc.Kind() != reflect.Interface {
		return nil, fmt.Errorf("type conversion error from object to '%v'", typeDesc)
	}

	// 解包指针并跟踪其使用情况：只允许一层指针，更深层指针在 CEL 中没有对应语义。
	isPtr := false
	if typeDesc.Kind() == reflect.Pointer {
		tk := typeDesc
		typeDesc = typeDesc.Elem()
		if typeDesc.Kind() == reflect.Pointer {
			return nil, fmt.Errorf("unsupported type conversion to '%v'", tk)
		}
		isPtr = true
	}

	if typeDesc.Kind() == reflect.Map {
		keyType := typeDesc.Key()
		if keyType.Kind() != reflect.String && keyType.Kind() != reflect.Interface {
			return nil, fmt.Errorf("object fields cannot be converted to type '%v'", keyType)
		}
		elemType := typeDesc.Elem()
		sz := len(sv.fieldMap)
		ntvMap := reflect.MakeMapWithSize(typeDesc, sz)
		for name, val := range sv.fieldMap {
			refVal, err := val.Ref.ConvertToNative(elemType)
			if err != nil {
				return nil, err
			}
			ntvMap.SetMapIndex(reflect.ValueOf(name), reflect.ValueOf(refVal))
		}
		return ntvMap.Interface(), nil
	}

	if typeDesc.Kind() == reflect.Struct {
		ntvObjPtr := reflect.New(typeDesc)
		ntvObj := ntvObjPtr.Elem()
		for name, val := range sv.fieldMap {
			// 按 Go 字段名而非 JSON 名匹配，与调用方传入的 typeDesc 约定一致。
			f := ntvObj.FieldByName(name)
			if !f.IsValid() {
				return nil, fmt.Errorf("type conversion error, no such field %s in type %v",
					name, typeDesc)
			}
			fv, err := val.Ref.ConvertToNative(f.Type())
			if err != nil {
				return nil, err
			}
			f.Set(reflect.ValueOf(fv))
		}
		if isPtr {
			return ntvObjPtr.Interface(), nil
		}
		return ntvObj.Interface(), nil
	}
	return nil, fmt.Errorf("type conversion error from object to '%v'", typeDesc)
}

// GetField 按名称返回 MapField（若存在）。
func (sv *structValue) GetField(name string) (*Field, bool) {
	field, found := sv.fieldMap[name]
	return field, found
}

// IsSet 返回某个已定义的字段是否也已被赋值。
func (sv *structValue) IsSet(key ref.Val) ref.Val {
	k, ok := key.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(key)
	}
	name := string(k)
	_, found := sv.fieldMap[name]
	return celBool(found)
}

// NewObjectValue 创建一个带 schema 类型的 struct 值，并返回空的 ObjectValue。
func NewObjectValue(sType *DeclType) *ObjectValue {
	return &ObjectValue{
		structValue: newStructValue(),
		objectType:  sType,
	}
}

// ObjectValue 是一个 struct，带有自定义 schema 类型，用于说明该结构
// 关联的字段及其类型。
type ObjectValue struct {
	*structValue
	objectType *DeclType
}

// ConvertToType 是 CEL ref.Val 接口方法的实现。
func (o *ObjectValue) ConvertToType(t ref.Type) ref.Val {
	if t == types.TypeType {
		return types.NewObjectTypeValue(o.objectType.TypeName())
	}
	if t.TypeName() == o.objectType.TypeName() {
		return o
	}
	return types.NewErr("type conversion error from '%s' to '%s'", o.Type(), t)
}

// Equal 当且仅当两个对象类型相同且各字段值相等时返回 true。
func (o *ObjectValue) Equal(other ref.Val) ref.Val {
	// 保持 CEL 的同类（homogeneous）相等语义。
	if o.objectType.TypeName() != other.Type().TypeName() {
		return types.MaybeNoSuchOverloadErr(other)
	}
	o2 := other.(traits.Indexer)
	for name := range o.objectType.Fields {
		k := types.String(name)
		v := o.Get(k)
		ov := o2.Get(k)
		vEq := v.Equal(ov)
		// 字段比较出错（而非不相等）时向上传播错误，便于用户定位问题。
		if vEq != types.True {
			return vEq
		}
	}
	return types.True
}

// Get 返回指定字段的值。
//
// 若字段已设置，返回其值；若字段未设置，则返回该字段的默认值，
// 从而支持安全遍历（safe-traversal），并为基于 Open API Schema 的类型
// 保留类 proto 的字段访问语义。
func (o *ObjectValue) Get(name ref.Val) ref.Val {
	n, ok := name.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(n)
	}
	nameStr := string(n)
	field, found := o.fieldMap[nameStr]
	if found {
		return field.Ref.ExprValue()
	}
	fieldDef, found := o.objectType.Fields[nameStr]
	if !found {
		return types.NewErr("no such field: %s", nameStr)
	}
	// 未设置的字段回退到 schema 默认值，实现 proto 风格的安全链式访问。
	defValue := fieldDef.DefaultValue()
	if defValue != nil {
		return defValue
	}
	return types.NewErr("no default for type: %s", fieldDef.TypeName())
}

// Type 返回该对象的 CEL 类型值。
func (o *ObjectValue) Type() ref.Type {
	return o.objectType
}

// Value 返回该对象的 Go 原生表示。
func (o *ObjectValue) Value() interface{} {
	return o
}

// NewMapValue 返回一个空的 MapValue。
func NewMapValue() *MapValue {
	return &MapValue{
		structValue: newStructValue(),
	}
}

// MapValue 声明一个对象，其一组具名字段的值是动态类型的。
type MapValue struct {
	*structValue
}

// ConvertToObject 由 MapValue 生成带有关联 schema 类型的 ObjectValue。
//
// 该转换是浅拷贝，Object 与 Map 共享内存，因为对 map 的所有引用
// 预期都会被替换为对该 Object 的引用。
func (m *MapValue) ConvertToObject(declType *DeclType) *ObjectValue {
	return &ObjectValue{
		structValue: m.structValue,
		objectType:  declType,
	}
}

// Contains 返回给定 key 是否存在于 MapValue 中。
func (m *MapValue) Contains(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}
	return celBool(found)
}

// ConvertToType 在可能的前提下把 MapValue 转换为其他 CEL 类型。
func (m *MapValue) ConvertToType(t ref.Type) ref.Val {
	switch t {
	case types.MapType:
		return m
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", m.Type(), t)
}

// Equal 当且仅当两个 map 大小相同、key 集合相同，且每个 map 的
// key-value 都相等时返回 true。
func (m *MapValue) Equal(other ref.Val) ref.Val {
	oMap, isMap := other.(traits.Mapper)
	if !isMap {
		return types.MaybeNoSuchOverloadErr(other)
	}
	if m.Size() != oMap.Size() {
		return types.False
	}
	for name, field := range m.fieldMap {
		k := types.String(name)
		ov, found := oMap.Find(k)
		if !found {
			return types.False
		}
		v := field.Ref.ExprValue()
		vEq := v.Equal(ov)
		if vEq != types.True {
			return vEq
		}
	}
	return types.True
}

// Find 返回 map 中该 key 对应的值（若找到）。
func (m *MapValue) Find(name ref.Val) (ref.Val, bool) {
	// 目前仅支持字符串 key 的 map：这与 JSON 最为契合，
	// 实现起来也简单得多。
	n, ok := name.(types.String)
	if !ok {
		// 非字符串 key 返回错误且 found=true，使调用方区分“类型错误”与“不存在”。
		return types.MaybeNoSuchOverloadErr(n), true
	}
	nameStr := string(n)
	field, found := m.fieldMap[nameStr]
	if found {
		return field.Ref.ExprValue(), true
	}
	return nil, false
}

// Get 返回 map 中该 key 对应的值；若未找到则返回错误。
func (m *MapValue) Get(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if found {
		return v
	}
	return types.ValOrErr(key, "no such key: %v", key)
}

// Iterator 生成一个遍历 map key 的 traits.Iterator。
//
// 该 Iterator 常在 comprehension（推导式）中使用。
func (m *MapValue) Iterator() traits.Iterator {
	// 先快照 key 列表，避免迭代期间 map 变更导致结果不确定。
	keys := make([]ref.Val, len(m.fieldMap))
	i := 0
	for k := range m.fieldMap {
		keys[i] = types.String(k)
		i++
	}
	return &baseMapIterator{
		baseVal: &baseVal{},
		keys:    keys,
	}
}

// Size 返回 map 中 key 的数量。
func (m *MapValue) Size() ref.Val {
	return types.Int(len(m.Fields))
}

// Type 返回该 map 的 CEL ref.Type。
func (m *MapValue) Type() ref.Type {
	return types.MapType
}

// Value 返回 MapValue 的 Go 原生表示。
func (m *MapValue) Value() interface{} {
	return m
}

// baseMapIterator 以预先生成的 key 列表为基准进行遍历。
type baseMapIterator struct {
	*baseVal
	keys []ref.Val
	idx  int
}

// HasNext 实现 traits.Iterator 接口方法。
func (it *baseMapIterator) HasNext() ref.Val {
	if it.idx < len(it.keys) {
		return types.True
	}
	return types.False
}

// Next 实现 traits.Iterator 接口方法。
func (it *baseMapIterator) Next() ref.Val {
	key := it.keys[it.idx]
	it.idx++
	return key
}

// Type 实现 CEL ref.Val 接口方法。
func (it *baseMapIterator) Type() ref.Type {
	return types.IteratorType
}

// NewField 返回一个 MapField 实例，其 DynValue 为空，指向指定的
// parse node id 与字段名。
func NewField(id int64, name string) *Field {
	return &Field{
		ID:   id,
		Name: name,
		Ref:  NewEmptyDynValue(),
	}
}

// Field 指定一个字段名，以及一个指向动态值的引用。
type Field struct {
	ID   int64
	Name string
	Ref  *DynValue
}

// NewListValue 返回一个空的 ListValue 实例。
func NewListValue() *ListValue {
	return &ListValue{
		Entries: []*DynValue{},
	}
}

// ListValue 包含一个动态类型条目的列表。
// valueSet 是惰性构建的哈希集合，用于加速 Contains；Append 会使其失效。
type ListValue struct {
	Entries      []*DynValue
	initValueSet sync.Once
	valueSet     map[ref.Val]struct{}
}

// Add 把两个 list 拼接起来，生成一个新的 CEL list 值。
func (lv *ListValue) Add(other ref.Val) ref.Val {
	oArr, isArr := other.(traits.Lister)
	if !isArr {
		return types.MaybeNoSuchOverloadErr(other)
	}
	szRight := len(lv.Entries)
	szLeft := int(oArr.Size().(types.Int))
	sz := szRight + szLeft
	combo := make([]ref.Val, sz)
	for i := 0; i < szRight; i++ {
		combo[i] = lv.Entries[i].ExprValue()
	}
	for i := 0; i < szLeft; i++ {
		combo[i+szRight] = oArr.Get(types.Int(i))
	}
	return types.DefaultTypeAdapter.NativeToValue(combo)
}

// Append 向 ListValue 追加一个条目。
func (lv *ListValue) Append(entry *DynValue) {
	lv.Entries = append(lv.Entries, entry)
	// 追加会重置所有已建立的索引：sync.Once 无法复位，只能整体替换为零值以允许下次重建。
	lv.initValueSet = sync.Once{}
}

// Contains 返回输入的 `val` 是否等于 list 中的某个元素。
//
// 若输入值与某个列表元素之间的任意一次逐对比较产生错误，
// 该操作将返回错误。
func (lv *ListValue) Contains(val ref.Val) ref.Val {
	if types.IsUnknownOrError(val) {
		return val
	}
	// 元素全为标量时走哈希快路径；否则退回逐项比较。
	lv.initValueSet.Do(lv.finalizeValueSet)
	if lv.valueSet != nil {
		_, found := lv.valueSet[val]
		if found {
			return types.True
		}
		// 这里不直接返回 false，而是继续走逐对相等比较，
		// 让 CEL 的异构（heterogeneous）相等约束由比较结果决定。
	}
	var err ref.Val
	sz := len(lv.Entries)
	for i := 0; i < sz; i++ {
		elem := lv.Entries[i]
		cmp := elem.Equal(val)
		b, ok := cmp.(types.Bool)
		// 类型不匹配的比较会产生非 bool 结果，记录第一个错误但继续寻找相等项。
		if !ok && err == nil {
			err = types.MaybeNoSuchOverloadErr(cmp)
		}
		if b == types.True {
			return types.True
		}
	}
	if err != nil {
		return err
	}
	return types.False
}

// ConvertToNative 是 CEL ref.Val 方法的实现，用于在 CEL 类型与
// Go 原生的数组类类型之间做适配。
func (lv *ListValue) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	// 非 list 的转换请求。
	if typeDesc.Kind() != reflect.Slice &&
		typeDesc.Kind() != reflect.Array &&
		typeDesc.Kind() != reflect.Interface {
		return nil, fmt.Errorf("type conversion error from list to '%v'", typeDesc)
	}

	// 若该 list 已可赋值给目标类型，直接返回。
	if reflect.TypeOf(lv).AssignableTo(typeDesc) {
		return lv, nil
	}

	// list 的转换。
	otherElem := typeDesc.Elem()

	// 由元素的 ConvertToNative() 函数决定转换是否可行。
	sz := len(lv.Entries)
	nativeList := reflect.MakeSlice(typeDesc, int(sz), int(sz))
	for i := 0; i < sz; i++ {
		elem := lv.Entries[i]
		nativeElemVal, err := elem.ConvertToNative(otherElem)
		if err != nil {
			return nil, err
		}
		nativeList.Index(int(i)).Set(reflect.ValueOf(nativeElemVal))
	}
	return nativeList.Interface(), nil
}

// ConvertToType 把 ListValue 转换为另一个 CEL 类型。
func (lv *ListValue) ConvertToType(t ref.Type) ref.Val {
	switch t {
	case types.ListType:
		return lv
	case types.TypeType:
		return types.ListType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", ListType, t)
}

// Equal 当且仅当两个 list 大小相同、且每个下标处的值也相等时
// 返回 true。
func (lv *ListValue) Equal(other ref.Val) ref.Val {
	oArr, isArr := other.(traits.Lister)
	if !isArr {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(len(lv.Entries))
	if sz != oArr.Size() {
		return types.False
	}
	for i := types.Int(0); i < sz; i++ {
		cmp := lv.Get(i).Equal(oArr.Get(i))
		if cmp != types.True {
			return cmp
		}
	}
	return types.True
}

// Get 返回给定下标处的值。
//
// 若下标为负数或大于等于 list 的大小，则返回错误。
func (lv *ListValue) Get(idx ref.Val) ref.Val {
	iv, isInt := idx.(types.Int)
	if !isInt {
		return types.ValOrErr(idx, "unsupported index: %v", idx)
	}
	i := int(iv)
	if i < 0 || i >= len(lv.Entries) {
		return types.NewErr("index out of bounds: %v", idx)
	}
	return lv.Entries[i].ExprValue()
}

// Iterator 生成适用于 CEL comprehension 宏的 traits.Iterator。
func (lv *ListValue) Iterator() traits.Iterator {
	return &baseListIterator{
		getter: lv.Get,
		sz:     len(lv.Entries),
	}
}

// Size 返回 list 中元素的数量。
func (lv *ListValue) Size() ref.Val {
	return types.Int(len(lv.Entries))
}

// Type 返回该 list 的 CEL ref.Type。
func (lv *ListValue) Type() ref.Type {
	return types.ListType
}

// Value 返回 Go 原生值。
func (lv *ListValue) Value() interface{} {
	return lv
}

// finalizeValueSet 检查 ListValue 的条目，在所有条目确定后用于内部优化。
// 只有全部为可哈希的标量时才建立集合；出现复合类型则放弃优化（valueSet 保持 nil）。
func (lv *ListValue) finalizeValueSet() {
	valueSet := make(map[ref.Val]struct{})
	for _, e := range lv.Entries {
		switch e.value.(type) {
		case bool, float64, int64, string, uint64, types.Null, PlainTextValue:
			valueSet[e.ExprValue()] = struct{}{}
		default:
			lv.valueSet = nil
			return
		}
	}
	lv.valueSet = valueSet
}

// baseVal 为迭代器提供默认的“不支持”实现，避免每个迭代器重复定义。
type baseVal struct{}

// ConvertToNative 不支持。
func (*baseVal) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	return nil, fmt.Errorf("unsupported native conversion to: %v", typeDesc)
}

// ConvertToType 不支持。
func (*baseVal) ConvertToType(t ref.Type) ref.Val {
	return types.NewErr("unsupported type conversion to: %v", t)
}

// Equal 不支持比较。
func (*baseVal) Equal(other ref.Val) ref.Val {
	return types.NewErr("unsupported equality test between instances")
}

// Value 无原生值。
func (v *baseVal) Value() interface{} {
	return nil
}

// baseListIterator 通过 getter 回调按下标取值，避免依赖具体列表实现。
type baseListIterator struct {
	*baseVal
	getter func(idx ref.Val) ref.Val
	sz     int
	idx    int
}

// HasNext 判断是否还有元素。
func (it *baseListIterator) HasNext() ref.Val {
	if it.idx < it.sz {
		return types.True
	}
	return types.False
}

// Next 返回当前元素并前进。
func (it *baseListIterator) Next() ref.Val {
	v := it.getter(types.Int(it.idx))
	it.idx++
	return v
}

// Type 返回 iterator 类型。
func (it *baseListIterator) Type() ref.Type {
	return types.IteratorType
}

// celBool 把 Go bool 转为 CEL bool。
func celBool(pred bool) ref.Val {
	if pred {
		return types.True
	}
	return types.False
}

// unknownType 用于无法映射到已知声明类型的值；其名字仅用于错误信息。
var unknownType = &DeclType{name: "unknown", MinSerializedSize: 1}
