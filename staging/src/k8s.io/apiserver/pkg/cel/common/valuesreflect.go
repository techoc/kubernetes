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

package common

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"sigs.k8s.io/structured-merge-diff/v6/value"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apiserver/pkg/cel"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// TypedToVal wraps "typed" Go value as CEL ref.Val types using reflection.
// "typed" values must be values declared by Kubernetes API types.go definitions.
//
// 中文说明：反射包装只在缺少生成代码或需要通用路径时使用；它依赖 schema 提供
// 子字段类型，因此 schema 与值结构必须一致，否则返回 CEL error 而非 panic。
// 指针逐层解引用并在遇到 nil 时返回 NullValue，保证 CEL 的 null 语义。
func TypedToVal(val interface{}, schema Schema) ref.Val {
	if val == nil {
		return types.NullValue
	}
	v := reflect.ValueOf(val)
	if !v.IsValid() {
		return types.NewErr("invalid data, got invalid reflect value: %v", v)
	}
	// 多层指针（如 **T）在 API 类型中少见但合法，统一解到最内层。
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return types.NullValue
		}
		v = v.Elem()
	}
	val = v.Interface()

	// 先按具体类型匹配：这些类型有明确的 CEL 映射，不能交给 kind 分支
	// 处理（例如 metav1.Time 是结构体，但应映射为 timestamp）。
	switch typedVal := val.(type) {
	case bool:
		return types.Bool(typedVal)
	case int:
		return types.Int(typedVal)
	case int32:
		return types.Int(typedVal)
	case int64:
		return types.Int(typedVal)
	case float32:
		return types.Double(typedVal)
	case float64:
		return types.Double(typedVal)
	case string:
		return types.String(typedVal)
	case []byte:
		if typedVal == nil {
			return types.NullValue
		}
		return types.Bytes(typedVal)
	case metav1.Time:
		return types.Timestamp{Time: typedVal.Time}
	case metav1.MicroTime:
		return types.Timestamp{Time: typedVal.Time}
	case metav1.Duration:
		return types.Duration{Duration: typedVal.Duration}
	case intstr.IntOrString:
		switch typedVal.Type {
		case intstr.Int:
			return types.Int(typedVal.IntVal)
		case intstr.String:
			return types.String(typedVal.StrVal)
		}
	case resource.Quantity:
		// For compatibility with CRD Validation rules, represent quantity as a plain string.
		// 中文说明：不能映射为数字，因为 Quantity 支持 m、Mi 等后缀，转为数字会丢失精度与语义。
		return types.String(typedVal.String())
	case json.Marshaler:
		// All JSON marshaled types must be mapped to a CEL type in the above switch.
		// This ensures that all types are purposefully mapped to CEL types.
		// 中文说明：显式报错而非回退到反射，是为了强制新增 API 类型时审慎选择 CEL 映射，
		// 避免出现隐式、不可控的表示。
		return types.NewErr("unsupported Go type for CEL: %T", typedVal)
	default:
		// continue on to the next switch
	}

	// 再按 kind 处理容器与基本类型；容器需要 schema 才能继续递归。
	switch v.Kind() {
	case reflect.Slice:
		if schema.Items() == nil {
			return types.NewErr("invalid schema for slice type: %v", schema)
		}
		typedList := typedList{value: v, itemsSchema: schema.Items()}
		// list-type 决定 CEL 中 `==` 与 `+` 的语义（有序、按 key、集合）。
		listType := schema.XListType()
		if listType != "" {
			switch listType {
			case "map":
				mapKeys := schema.XListMapKeys()
				return &typedMapList{typedList: typedList, keyProps: mapKeys, escapedKeyProps: escapeKeyProps(mapKeys)}
			case "set":
				return &typedSetList{typedList: typedList}
			case "atomic":
				return &typedList
			default:
				return types.NewErr("invalid x-kubernetes-list-type, expected 'map', 'set' or 'atomic' but got %s", listType)
			}
		}
		return &typedList
	case reflect.Map:
		if schema.AdditionalProperties() == nil || schema.AdditionalProperties().Schema() == nil {
			return types.NewErr("invalid schema for map type: %v", schema)
		}
		return &typedMap{value: v, valuesSchema: schema.AdditionalProperties().Schema()}
	case reflect.Struct:
		if schema.Properties() == nil {
			return types.NewErr("invalid schema for struct type: %v", schema)
		}
		return &typedStruct{
			value: v,
			propSchema: func(key string) (Schema, bool) {
				if schema, ok := schema.Properties()[key]; ok {
					return schema, true
				}
				return nil, false
			},
		}
	// Match type aliases to primitives by kind
	// 中文说明：如 PodPhase 这类字符串别名没有独立的 CEL 类型，按底层 kind 处理。
	case reflect.Bool:
		return types.Bool(v.Bool())
	case reflect.String:
		return types.String(v.String())
	case reflect.Int, reflect.Int32, reflect.Int64:
		return types.Int(v.Int())
	case reflect.Float32, reflect.Float64:
		return types.Double(v.Float())
	default:
		return types.NewErr("unsupported Go type for CEL: %v", v.Type())
	}
}

// typedStruct wraps a struct as a CEL ref.Val and provides lazy access to fields via reflection.
//
// 中文说明：字段在访问时才转换，避免把整棵对象树一次性展开；对于只读取少数
// 字段的策略，这能显著减少反射与分配开销。
type typedStruct struct {
	value reflect.Value // Kind is required to be: reflect.Struct

	// propSchema finds the schema for a given key.
	// The key is an unescaped JSON name, not an escaped CEL identifier.
	propSchema func(key string) (Schema, bool)
}

func (s *typedStruct) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	// 只允许转换为同一具体类型，防止把结构体静默转成不兼容的 Go 类型。
	if s.value.Type().AssignableTo(typeDesc) {
		return s.value.Interface(), nil
	}
	return nil, fmt.Errorf("type conversion error from struct type %v to %v", s.value.Type(), typeDesc)
}

// ConvertToType 支持 struct 到 map 的视图转换，便于 CEL 以 map 方式访问字段。
func (s *typedStruct) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case s.Type():
		return s
	case types.MapType:
		return s
	case types.TypeType:
		return s.objType()
	}
	return types.NewErr("type conversion error from struct %s to %s", s.Type().TypeName(), typeValue.TypeName())
}

// Equal 使用 Kubernetes 语义相等，忽略仅由 API 默认值造成的差异。
//
// 中文说明：不能直接用 reflect.DeepEqual，否则零值与 nil 切片会被判定不等，
// 与 API 的语义比较约定冲突。
func (s *typedStruct) Equal(other ref.Val) ref.Val {
	otherStruct, ok := other.(*typedStruct)
	if ok {
		return types.Bool(apiequality.Semantic.DeepEqual(s.value.Interface(), otherStruct.value.Interface()))
	}
	return types.MaybeNoSuchOverloadErr(other)
}

func (s *typedStruct) Type() ref.Type {
	return s.objType()
}

// objType 用"包路径.类型名"构造 CEL 对象类型，避免不同包中同名类型冲突。
func (s *typedStruct) objType() *types.Type {
	typeName := s.value.Type().Name()
	if pkgPath := s.value.Type().PkgPath(); pkgPath != "" {
		typeName = pkgPath + "." + typeName
	}
	return types.NewObjectType(typeName)
}

func (s *typedStruct) Value() interface{} {
	return s.value.Interface()
}

// IsSet returns true if the field is set. escapedField must be a types.String containing the
// escaped CEL identifier. For example, "__if__" for JSON name "if".
//
// 中文说明：区分"字段不存在"与"字段值为 null/错误"；后者需原样向上传播，
// 否则会把错误吞掉变成 false。
func (s *typedStruct) IsSet(escapedField ref.Val) ref.Val {
	v, found := s.lookupField(escapedField)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}
	return types.Bool(found)
}

// Get returns the value of a field. escapedKey must be a types.String containing the
// escaped CEL identifier. For example, "__if__" for JSON name "if".
func (s *typedStruct) Get(escapedKey ref.Val) ref.Val {
	v, found := s.lookupField(escapedKey)
	if !found {
		return types.NewErr("no such key: %v", escapedKey)
	}
	return v
}

// lookupField finds a field by name. escapedKey must be a types.String containing the escaped
// CEL identifier of the field name. For example, "__if__" for JSON name "if".
//
// 中文说明：先反转义再查字段缓存，是因为 CEL 会把 Go 关键字等冲突名改写为
// __xxx__；缓存按未转义的 JSON 名索引。
func (s *typedStruct) lookupField(escapedKey ref.Val) (ref.Val, bool) {
	escapedKeyStr, ok := escapedKey.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(escapedKey), true
	}
	unescapedFieldName, ok := cel.Unescape(escapedKeyStr.Value().(string))
	if !ok {
		return nil, false
	}

	cacheEntry := value.TypeReflectEntryOf(s.value.Type())
	fieldCache, ok := cacheEntry.Fields()[unescapedFieldName]
	if !ok {
		return nil, false
	}

	// CanOmit 用于跳过 omitempty 且为零值的字段，使 CEL 的 has() 与
	// JSON 序列化后的可见性保持一致。
	if e := fieldCache.GetFrom(s.value); !fieldCache.CanOmit(e) {
		if propSchema, ok := s.propSchema(unescapedFieldName); ok {
			v := TypedToVal(e.Interface(), propSchema)
			// 显式 null 在 CEL 中视为字段不存在，has() 返回 false。
			if v == types.NullValue {
				return nil, false
			}
			return v, true
		}
	}
	return nil, false
}

// typedList 表示普通（atomic 或未声明 list-type）切片。
//
// 中文说明：保持元素顺序，相等性按位置比较——这是 atomic 语义的核心。
type typedList struct {
	value reflect.Value // Kind is required to be: reflect.Slice

	itemsSchema Schema
}

func (t *typedList) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc.Kind() {
	case reflect.Slice:
		return t.value.Interface(), nil
	default:
		return nil, fmt.Errorf("type conversion error from '%s' to '%s'", t.Type(), typeDesc)
	}
}

func (t *typedList) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.ListType:
		return t
	case types.TypeType:
		return types.ListType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", t.Type(), typeValue.TypeName())
}

// Equal 逐位置比较，任一元素不等立即返回。
//
// 中文说明：返回元素的比较结果而非统一返回 false，是为了让错误（如类型
// 不匹配）能够向上传播。
func (t *typedList) Equal(other ref.Val) ref.Val {
	oList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(t.value.Len())
	if sz != oList.Size() {
		return types.False
	}
	for i := range types.Int(sz) {
		eq := t.Get(i).Equal(oList.Get(i))
		if eq != types.True {
			return eq // either false or error
		}
	}
	return types.True
}

func (t *typedList) Type() ref.Type {
	return types.ListType
}

func (t *typedList) Value() interface{} {
	return t.value.Interface()
}

// Add 实现 CEL 的列表拼接：先放本列表全部元素，再追加另一列表。
//
// 中文说明：结果为新列表，不修改原值，符合 CEL 表达式不可变的语义。
func (t *typedList) Add(other ref.Val) ref.Val {
	oList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := t.value.Len()
	otherSz, _ := oList.Size().(types.Int)
	elements := make([]ref.Val, 0, sz+int(otherSz))
	for i := range sz {
		elements = append(elements, t.Get(types.Int(i)))
	}
	for it := oList.Iterator(); it.HasNext() == types.True; {
		elements = append(elements, it.Next())
	}
	return types.NewRefValList(types.DefaultTypeAdapter, elements)
}

// Contains 判断列表中是否存在与 val 相等的元素。
//
// 中文说明：比较过程中若出现类型错误，先记住错误继续扫描，命中相等元素时
// 仍返回 true——存在性判断不应因无关元素类型不符而失败。
func (t *typedList) Contains(val ref.Val) ref.Val {
	if types.IsUnknownOrError(val) {
		return val
	}
	var err ref.Val
	sz := t.value.Len()
	for i := range sz {
		elem := TypedToVal(t.value.Index(i).Interface(), t.itemsSchema)
		cmp := elem.Equal(val)
		b, ok := cmp.(types.Bool)
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

// Get 按索引取元素，索引必须是整数且在范围内。
func (t *typedList) Get(idx ref.Val) ref.Val {
	iv, isInt := idx.(types.Int)
	if !isInt {
		return types.ValOrErr(idx, "unsupported index: %v", idx)
	}
	i := int(iv)
	if i < 0 || i >= t.value.Len() {
		return types.NewErr("index out of bounds: %v", idx)
	}
	return TypedToVal(t.value.Index(i).Interface(), t.itemsSchema)
}

// Iterator 预先物化全部元素，保证迭代期间底层切片被修改也不会影响结果。
func (t *typedList) Iterator() traits.Iterator {
	elements := make([]ref.Val, t.value.Len())
	sz := t.value.Len()
	for i := range sz {
		elements[i] = TypedToVal(t.value.Index(i).Interface(), t.itemsSchema)
	}
	return &sliceIter{typedList: t, elements: elements}
}

func (t *typedList) Size() ref.Val {
	return types.Int(t.value.Len())
}

// sliceIter 是物化列表上的单向迭代器。
//
// 中文说明：不支持回退，符合 CEL 迭代协议；越界后继续 Next 返回错误。
type sliceIter struct {
	*typedList
	elements []ref.Val
	idx      int
}

func (it *sliceIter) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.elements))
}

func (it *sliceIter) Next() ref.Val {
	if it.idx >= len(it.elements) {
		return types.NewErr("iterator exhausted")
	}
	elem := it.elements[it.idx]
	it.idx++
	return elem
}

// typedMapList 表示 x-kubernetes-list-type=map 的切片：相等性忽略顺序，
// 以 keyProps 标识元素。
//
// 中文说明：mapOfList 用 sync.Once 惰性构建，因为多数表达式只做字段访问，
// 不触发相等比较；构建后即可 O(1) 定位。
type typedMapList struct {
	typedList
	// keyProps are unescaped JSON property names
	keyProps []string

	// escapedKeyProps are escaped CEL identifiers
	escapedKeyProps []string

	sync.Once // for lazy load of mapOfList since it is only needed if Equals is called
	mapOfList map[interface{}]interface{}
}

// getMap 惰性构建 key 到元素的索引。
func (t *typedMapList) getMap() map[interface{}]interface{} {
	t.Do(func() {
		sz := t.value.Len()
		t.mapOfList = make(map[interface{}]interface{}, sz)
		for i := range types.Int(sz) {
			v := t.Get(i)
			e := reflect.ValueOf(v.Value())
			t.mapOfList[t.toMapKey(e)] = e.Interface()
		}
	})
	return t.mapOfList
}

// toMapKey returns a valid golang map key for the given element of the map list.
// element must be a valid map list entry where all map key props are scalar types (which are comparable in go
// and valid for use in a golang map key).
//
// 中文说明：1-3 个 key 用数组（可直接比较），更多时退化为字符串序列化，
// 避免为通用情况引入不可比较类型。
func (t *typedMapList) toMapKey(element reflect.Value) interface{} {
	if element.Kind() != reflect.Struct {
		return types.NewErr("unexpected data format for element of array with x-kubernetes-list-type=map: %T", element)
	}
	cacheEntry := value.TypeReflectEntryOf(element.Type())
	var fieldEntries []*value.FieldCacheEntry
	for i := range len(t.keyProps) {
		if ce, ok := cacheEntry.Fields()[t.keyProps[i]]; !ok { // Fields() is keyed by unescaped JSON property names.
			return types.NewErr("unexpected data format for element of array with x-kubernetes-list-type=map: %T", element)
		} else {
			fieldEntries = append(fieldEntries, ce)
		}
	}

	getKeyValue := func(fieldEntry *value.FieldCacheEntry) interface{} {
		v := fieldEntry.GetFrom(element)
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
		}
		raw := v.Interface()
		if raw != nil && !reflect.TypeOf(raw).Comparable() {
			// %#v includes type information and quotes strings, so the
			// serialized form cannot collide with a comparable raw value.
			return fmt.Sprintf("%#v", raw)
		}
		return raw
	}

	// Arrays are comparable in go and may be used as map keys, but maps and slices are not.
	// So we can special case small numbers of key props as arrays and fall back to serialization
	// for larger numbers of key props
	if len(fieldEntries) == 1 {
		return getKeyValue(fieldEntries[0])
	}
	if len(fieldEntries) == 2 {
		return [2]interface{}{getKeyValue(fieldEntries[0]), getKeyValue(fieldEntries[1])}
	}
	if len(fieldEntries) == 3 {
		return [3]interface{}{getKeyValue(fieldEntries[0]), getKeyValue(fieldEntries[1]), getKeyValue(fieldEntries[2])}
	}

	key := make([]interface{}, len(fieldEntries))
	for i := range fieldEntries {
		key[i] = getKeyValue(fieldEntries[i])
	}
	// Serialize to a string for more than 3 keys. %#v quotes strings.
	return fmt.Sprintf("%#v", key)
}

// Equal on a map list ignores list element order.
//
// 中文说明：用 seen 计数确保两侧元素一一对应——仅判断"每个元素都能在对方
// 找到"不足够，重复 key 可能掩盖长度相同的假相等。
func (t *typedMapList) Equal(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(t.value.Len())
	if sz != oMapList.Size() {
		return types.False
	}
	tMap := t.getMap()
	seen := make(map[interface{}]struct{}, len(tMap))
	for it := oMapList.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		k := t.toMapKey(reflect.ValueOf(v.Value()))
		tVal, ok := tMap[k]
		if !ok {
			return types.False
		}
		eq := TypedToVal(tVal, t.itemsSchema).Equal(v)
		if eq != types.True {
			return eq // either false or error
		}
		seen[k] = struct{}{}
	}
	return types.Bool(len(seen) == len(tMap))
}

// Add for a map list `X + Y` performs a merge where the array positions of all keys in `X` are preserved but the values
// are overwritten by values in `Y` when the key sets of `X` and `Y` intersect. Elements in `Y` with
// non-intersecting keys are appended, retaining their partial order.
//
// 中文说明：与 SSA 的 merge key 语义一致，使 CEL 中的 `+` 能直接表达
// "按 key 合并"的期望。
func (t *typedMapList) Add(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := t.value.Len()
	elements := make([]ref.Val, sz)
	for i := range sz {
		elements[i] = t.Get(types.Int(i))
	}
	return addToMapList(elements, oMapList, t.escapedKeyProps)
}

// addToMapList merges the elements of other into the given map list elements
// according to x-kubernetes-list-type=map semantics: elements of other with
// intersecting keys overwrite the matching entries of elements in place, and
// elements with non-intersecting keys are appended. elements may be mutated in
// place and may be appended to, so callers must not retain or reuse elements
// after the call.
//
// escapedKeyProps must contain the escaped CEL identifiers keys.
//
// 中文说明：调用方需注意 elements 会被原地修改或追加，因此必须先复制；
// 该约定避免了每次合并都重新分配整个切片。
func addToMapList(elements []ref.Val, other traits.Lister, escapedKeyProps []string) ref.Val {
	keyToIdx := make(map[interface{}]int, len(elements))
	for i, e := range elements {
		keyToIdx[refValMapKey(e, escapedKeyProps)] = i
	}
	for it := other.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		k := refValMapKey(v, escapedKeyProps)
		if overwritePosition, ok := keyToIdx[k]; ok {
			elements[overwritePosition] = v
		} else {
			elements = append(elements, v)
		}
	}
	return &refValMapList{
		Lister:          types.NewRefValList(types.DefaultTypeAdapter, elements),
		escapedKeyProps: escapedKeyProps,
	}
}

// refValMapKey returns a valid golang map key for the given element of a map list.
// escapedKeyProps must contain the escaped CEL identifiers of the keys.
//
// 中文说明：与 typedMapList.toMapKey 逻辑对应，但作用于已转换的 ref.Val；
// 同时兼容 map 与带字段测试的对象两种访问方式。
func refValMapKey(element ref.Val, escapedKeyProps []string) interface{} {
	get := func(escapedProp string) interface{} {
		key := types.String(escapedProp)
		var v ref.Val
		var found bool
		switch e := element.(type) {
		case traits.Mapper:
			v, found = e.Find(key)
		case traits.Indexer:
			if tester, ok := element.(traits.FieldTester); ok {
				found = tester.IsSet(key) == types.True
			}
			if found {
				v = e.Get(key)
			}
		}
		if !found || v == nil || types.IsUnknownOrError(v) {
			return nil
		}
		raw := v.Value()
		if raw != nil && !reflect.TypeOf(raw).Comparable() {
			// %#v includes type information and quotes strings, so the
			// serialized form cannot collide with a comparable raw value.
			return fmt.Sprintf("%#v", raw)
		}
		return raw
	}

	if len(escapedKeyProps) == 1 {
		return get(escapedKeyProps[0])
	}
	if len(escapedKeyProps) == 2 {
		return [2]interface{}{get(escapedKeyProps[0]), get(escapedKeyProps[1])}
	}
	if len(escapedKeyProps) == 3 {
		return [3]interface{}{get(escapedKeyProps[0]), get(escapedKeyProps[1]), get(escapedKeyProps[2])}
	}

	key := make([]interface{}, len(escapedKeyProps))
	for i, kf := range escapedKeyProps {
		key[i] = get(kf)
	}
	// Serialize to a string for more than 3 keys. %#v quotes strings.
	return fmt.Sprintf("%#v", key)
}

// refValMapList 是合并结果：元素为 ref.Val，但仍需保留 map-list 的相等与
// 再次合并语义。
type refValMapList struct {
	traits.Lister
	escapedKeyProps []string
}

func (l *refValMapList) Add(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz, _ := l.Size().(types.Int)
	elements := make([]ref.Val, int(sz))
	for i := range types.Int(sz) {
		elements[i] = l.Get(i)
	}
	return addToMapList(elements, oMapList, l.escapedKeyProps)
}

func (l *refValMapList) Equal(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz, _ := l.Size().(types.Int)
	if sz != oMapList.Size() {
		return types.False
	}
	keyToElement := make(map[interface{}]ref.Val, int(sz))
	for i := range types.Int(sz) {
		e := l.Get(i)
		keyToElement[refValMapKey(e, l.escapedKeyProps)] = e
	}
	seen := make(map[interface{}]struct{}, int(sz))
	for it := oMapList.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		k := refValMapKey(v, l.escapedKeyProps)
		e, ok := keyToElement[k]
		if !ok {
			return types.False
		}
		eq := e.Equal(v)
		if eq != types.True {
			return eq
		}
		seen[k] = struct{}{}
	}
	return types.Bool(len(seen) == len(keyToElement))
}

// typedSetList 表示 x-kubernetes-list-type=set 的切片：相等性忽略顺序，
// 元素必须可比较（标量）。
//
// 中文说明：setOfList 同样惰性构建；遇到非标量元素时缓存错误，避免重复扫描。
type typedSetList struct {
	typedList

	sync.Once // for lazy load of setOfList since it is only needed if Equals is called
	set       map[interface{}]struct{}
	setErr    ref.Val
}

// getSet 惰性构建集合，并缓存首次遇到的错误。
func (t *typedSetList) getSet() (map[interface{}]struct{}, ref.Val) {
	t.Do(func() {
		sz := t.value.Len()
		t.set = make(map[interface{}]struct{}, sz)
		for i := range types.Int(sz) {
			k, err := setElementKey(t.Get(i))
			if err != nil {
				t.set, t.setErr = nil, err
				return
			}
			t.set[k] = struct{}{}
		}
	})
	return t.set, t.setErr
}

// Equal on a set list ignores list element order.
//
// 中文说明：与 map-list 相同，用 seen 计数保证元素数量与对应关系都一致。
func (t *typedSetList) Equal(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(t.value.Len())
	if sz != oSetList.Size() {
		return types.False
	}
	tSet, err := t.getSet()
	if err != nil {
		return err
	}
	seen := make(map[interface{}]struct{}, len(tSet))
	for it := oSetList.Iterator(); it.HasNext() == types.True; {
		k, err := setElementKey(it.Next())
		if err != nil {
			return err
		}
		if _, ok := tSet[k]; !ok {
			return types.False
		}
		seen[k] = struct{}{}
	}
	return types.Bool(len(seen) == len(tSet))
}

// Add for a set list `X + Y` performs a union where the array positions of all elements in `X` are preserved and
// non-intersecting elements in `Y` are appended, retaining their partial order.
//
// 中文说明：这是集合并集，不产生重复元素。
func (t *typedSetList) Add(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := t.value.Len()
	elements := make([]ref.Val, sz)
	for i := range sz {
		elements[i] = t.Get(types.Int(i))
	}
	return addToSetList(elements, oSetList)
}

// addToSetList unions the elements of other into the given set list elements
// according to x-kubernetes-list-type=set semantics: the array positions of
// elements are preserved, and elements of other that are not already present
// are appended to maintain partial order. elements may be appended to,
// so callers must not retain or reuse elements after the call.
//
// 中文说明：非标量元素直接返回错误，因为集合语义要求元素可作为 map key。
func addToSetList(elements []ref.Val, other traits.Lister) ref.Val {
	set := make(map[interface{}]struct{}, len(elements))
	for _, e := range elements {
		k, err := setElementKey(e)
		if err != nil {
			return err
		}
		set[k] = struct{}{}
	}
	for it := other.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		k, err := setElementKey(v)
		if err != nil {
			return err
		}
		if _, exists := set[k]; !exists {
			set[k] = struct{}{}
			elements = append(elements, v)
		}
	}
	return &refValSetList{Lister: types.NewRefValList(types.DefaultTypeAdapter, elements)}
}

// setElementKey returns a valid golang map key for the given element of a set list.
// Set elements are expected to be scalars; if the element is a non-scalar an error is returned.
//
// 中文说明：不可比较的原始值通过 %#v 序列化为字符串，保证字符串结果带引号，
// 不会与真正的字符串值碰撞。
func setElementKey(element ref.Val) (value interface{}, err ref.Val) {
	switch element.(type) {
	case traits.Lister, traits.Mapper:
		return nil, types.NewErr("listSet operations are only supported on lists of scalar values")
	}
	raw := element.Value()
	if raw != nil && !reflect.TypeOf(raw).Comparable() {
		// %#v includes type information and quotes strings, so the
		// serialized form cannot collide with a comparable raw value.
		return fmt.Sprintf("%#v", raw), nil
	}
	return raw, nil
}

// refValSetList 是集合合并结果，保留 set 语义以便继续合并或比较。
type refValSetList struct {
	traits.Lister
}

func (l *refValSetList) Add(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz, _ := l.Size().(types.Int)
	elements := make([]ref.Val, int(sz))
	for i := range sz {
		elements[i] = l.Get(i)
	}
	return addToSetList(elements, oSetList)
}

// Equal on a set list ignores list element order.
func (l *refValSetList) Equal(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz, _ := l.Size().(types.Int)
	if sz != oSetList.Size() {
		return types.False
	}
	set := make(map[interface{}]struct{}, int(sz))
	for i := range sz {
		k, err := setElementKey(l.Get(i))
		if err != nil {
			return err
		}
		set[k] = struct{}{}
	}
	seen := make(map[interface{}]struct{}, len(set))
	for it := oSetList.Iterator(); it.HasNext() == types.True; {
		k, err := setElementKey(it.Next())
		if err != nil {
			return err
		}
		if _, exists := set[k]; !exists {
			return types.False
		}
		seen[k] = struct{}{}
	}
	return types.Bool(len(seen) == len(set))
}

// typedMap 表示 Go map，键为字符串，值类型由 additionalProperties schema 描述。
//
// 中文说明：map 的键不经过 CEL 标识符转义，与结构体字段的访问方式不同。
type typedMap struct {
	value reflect.Value // Kind is required to be: reflect.Map

	valuesSchema Schema
}

func (t *typedMap) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc.Kind() {
	case reflect.Map:
		return t.value.Interface(), nil
	default:
		return nil, fmt.Errorf("type conversion error from '%s' to '%s'", t.Type(), typeDesc)
	}
}

func (t *typedMap) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.MapType:
		return t
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", t.Type(), typeValue.TypeName())
}

// Equal 逐键比较，键集合大小不同直接判定不等。
func (t *typedMap) Equal(other ref.Val) ref.Val {
	oMap, isMap := other.(traits.Mapper)
	if !isMap {
		return types.MaybeNoSuchOverloadErr(other)
	}
	if types.Int(t.value.Len()) != oMap.Size() {
		return types.False
	}
	for it := t.value.MapRange(); it.Next(); {
		key := it.Key()
		value := it.Value()
		ov, found := oMap.Find(types.String(key.String()))
		if !found {
			return types.False
		}
		v := TypedToVal(value.Interface(), t.valuesSchema)
		vEq := v.Equal(ov)
		if vEq != types.True {
			return vEq // either false or error
		}
	}
	return types.True
}

func (t *typedMap) Type() ref.Type {
	return types.MapType
}

func (t *typedMap) Value() interface{} {
	return t.value.Interface()
}

// Contains 实现 CEL 的 `in` 运算；值转换出错时向上传播错误。
func (t *typedMap) Contains(key ref.Val) ref.Val {
	v, found := t.Find(key)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}

	return types.Bool(found)
}

func (t *typedMap) Get(key ref.Val) ref.Val {
	v, found := t.Find(key)
	if found {
		return v
	}
	return types.ValOrErr(key, "no such key: %v", key)
}

func (t *typedMap) Size() ref.Val {
	return types.Int(t.value.Len())
}

// Find returns the value of the map entry for the given key, if present. The map keys
// are normal strings, not CEL identifiers, and are not escaped.
//
// 中文说明：返回 (nil, false) 表示键不存在，与"键存在但值为空"区分开。
func (t *typedMap) Find(key ref.Val) (ref.Val, bool) {
	keyStr, ok := key.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(key), true
	}
	k := keyStr.Value().(string)
	if v := t.value.MapIndex(reflect.ValueOf(k)); v.IsValid() {
		return TypedToVal(v.Interface(), t.valuesSchema), true
	}
	return nil, false
}

// Iterator 预先收集键，迭代顺序为 Go map 的随机顺序。
//
// 中文说明：map 本身无序，CEL 对 map 的迭代不应依赖顺序；需要有序语义的
// 调用方应使用列表。
func (t *typedMap) Iterator() traits.Iterator {
	keys := make([]ref.Val, t.value.Len())
	for i, k := range t.value.MapKeys() {
		keys[i] = types.String(k.String())
	}
	return &mapIter{typedMap: t, keys: keys}
}

// mapIter 遍历键集合，由调用方再用 Find 取值。
type mapIter struct {
	*typedMap
	keys []ref.Val
	idx  int
}

func (it *mapIter) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.keys))
}

func (it *mapIter) Next() ref.Val {
	key := it.keys[it.idx]
	it.idx++
	return key
}
