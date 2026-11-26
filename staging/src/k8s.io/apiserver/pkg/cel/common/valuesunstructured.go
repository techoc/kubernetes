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

package common

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	"k8s.io/kube-openapi/pkg/validation/strfmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apiserver/pkg/cel"
)

// UnstructuredToVal 把 Kubernetes 非结构化数据元素转换为 CEL Val。
// 自定义资源 schema 的根 schema 应包含 type meta 与 object meta 的 schema。
// 若嵌入资源缺少这两部分 schema，会自动补上。
//
// 转换严格以 OpenAPI schema 为准：数据与 schema 不符时返回 CEL error 值而不是 panic，
// 让策略求值失败而不是让 apiserver 崩溃。
func UnstructuredToVal(unstructured interface{}, schema Schema) ref.Val {
	if unstructured == nil {
		// nullable=false 的字段出现 null 属于数据/schema 不一致，显式报错。
		if schema.Nullable() {
			return types.NullValue
		}
		return types.NewErr("invalid data, got null for schema with nullable=false")
	}
	if schema.IsXIntOrString() {
		switch v := unstructured.(type) {
		case string:
			return types.String(v)
		case int:
			return types.Int(v)
		case int32:
			return types.Int(v)
		case int64:
			return types.Int(v)
		}
		return types.NewErr("invalid data, expected XIntOrString value to be either a string or integer")
	}
	if schema.Type() == "object" {
		m, ok := unstructured.(map[string]interface{})
		if !ok {
			return types.NewErr("invalid data, expected a map for the provided schema with type=object")
		}
		if schema.IsXEmbeddedResource() || schema.Properties() != nil {
			if schema.IsXEmbeddedResource() {
				// 内嵌资源自动补上 apiVersion/kind/metadata，使其在 CEL 中表现与顶层资源一致。
				schema = schema.WithTypeAndObjectMeta()
			}
			return &unstructuredMap{
				value:  m,
				schema: schema,
				propSchema: func(key string) (Schema, bool) {
					if schema, ok := schema.Properties()[key]; ok {
						return schema, true
					}
					return nil, false
				},
			}
		}
		if schema.AdditionalProperties() != nil && schema.AdditionalProperties().Schema() != nil {
			return &unstructuredMap{
				value:  m,
				schema: schema,
				propSchema: func(key string) (Schema, bool) {
					return schema.AdditionalProperties().Schema(), true
				},
			}
		}

		// properties 与 additionalProperties 互斥，但两者都缺失的情况
		// 也无法阻止。
		// 满足 (1) 无 properties (2) 无 additionalProperties 或其值为 false 的对象
		// 视为空对象。
		// additionalProperties == true 的对象视为非结构化 map。
		// 设置了 x-kubernetes-preserve-unknown-field 扩展的对象也视为非结构化 map。
		// 空对象与非结构化 map 由 unstructuredMap 实现结合 schema 加以区分，
		// 但最终结果保持一致。
		return &unstructuredMap{
			value:  m,
			schema: schema,
			propSchema: func(key string) (Schema, bool) {
				return nil, false
			},
		}
	}

	if schema.Type() == "array" {
		l, ok := unstructured.([]interface{})
		if !ok {
			return types.NewErr("invalid data, expected an array for the provided schema with type=array")
		}
		if schema.Items() == nil {
			return types.NewErr("invalid array type, expected Items with a non-empty Schema")
		}
		typedList := unstructuredList{elements: l, itemsSchema: schema.Items()}
		// list-type 决定相等性与合并语义：map 按 key、set 按元素、atomic 按整体顺序。
		listType := schema.XListType()
		if listType != "" {
			switch listType {
			case "map":
				mapKeys := schema.XListMapKeys()
				return &unstructuredMapList{unstructuredList: typedList, escapedKeyProps: escapeKeyProps(mapKeys)}
			case "set":
				return &unstructuredSetList{unstructuredList: typedList}
			case "atomic":
				return &typedList
			default:
				return types.NewErr("invalid x-kubernetes-list-type, expected 'map', 'set' or 'atomic' but got %s", listType)
			}
		}
		return &typedList
	}

	if schema.Type() == "string" {
		str, ok := unstructured.(string)
		if !ok {
			return types.NewErr("invalid data, expected string, got %T", unstructured)
		}
		// format 决定字符串在 CEL 中的实际类型，使 duration/date 等可直接参与比较。
		switch schema.Format() {
		case "duration":
			d, err := strfmt.ParseDuration(str)
			if err != nil {
				return types.NewErr("Invalid duration %s: %v", str, err)
			}
			return types.Duration{Duration: d}
		case "date":
			d, err := time.Parse(strfmt.RFC3339FullDate, str) // strfmt uses this format for OpenAPIv3 value validation
			if err != nil {
				return types.NewErr("Invalid date formatted string %s: %v", str, err)
			}
			return types.Timestamp{Time: d}
		case "date-time":
			d, err := strfmt.ParseDateTime(str)
			if err != nil {
				return types.NewErr("Invalid date-time formatted string %s: %v", str, err)
			}
			return types.Timestamp{Time: time.Time(d)}
		case "byte":
			base64 := strfmt.Base64{}
			err := base64.UnmarshalText([]byte(str))
			if err != nil {
				return types.NewErr("Invalid byte formatted string %s: %v", str, err)
			}
			return types.Bytes(base64)
		}

		return types.String(str)
	}
	if schema.Type() == "number" {
		switch v := unstructured.(type) {
		// 整数的小数表示（如 1.0、0.0）在 yaml 转 json 时可能变为整数表示（如 1、0），
		// 随后被解析为 int64。
		case int:
			return types.Double(v)
		case int32:
			return types.Double(v)
		case int64:
			return types.Double(v)

		case float32:
			return types.Double(v)
		case float64:
			return types.Double(v)
		default:
			return types.NewErr("invalid data, expected float, got %T", unstructured)
		}
	}
	if schema.Type() == "integer" {
		switch v := unstructured.(type) {
		case int:
			return types.Int(v)
		case int32:
			return types.Int(v)
		case int64:
			return types.Int(v)
		default:
			return types.NewErr("invalid data, expected int, got %T", unstructured)
		}
	}
	if schema.Type() == "boolean" {
		b, ok := unstructured.(bool)
		if !ok {
			return types.NewErr("invalid data, expected bool, got %T", unstructured)
		}
		return types.Bool(b)
	}

	// 无类型但保留未知字段：以不透明值处理，仅支持相等比较。
	if schema.IsXPreserveUnknownFields() {
		return &unknownPreserved{u: unstructured}
	}

	return types.NewErr("invalid type, expected object, array, number, integer, boolean or string, or no type with x-kubernetes-int-or-string or x-kubernetes-preserve-unknown-fields is true, got %s", schema.Type())
}

// unknownPreserved 表示通过 x-kubernetes-preserve-unknown-fields 保留在自定义资源中的未知数据。
// 它在运行时保留数据，不假定其具体类型，且只支持相等比较。
// unknownPreserved 只用于 CEL 表达式中无法直接访问的值，即那些
// 没有对应 CEL 类型声明的数据。
type unknownPreserved struct {
	u interface{}
}

// ConvertToNative 不支持转换：该值没有 CEL 类型声明，转换语义未定义。
func (t *unknownPreserved) ConvertToNative(refType reflect.Type) (interface{}, error) {
	return nil, fmt.Errorf("type conversion to '%s' not supported for values preserved by x-kubernetes-preserve-unknown-fields", refType)
}

// ConvertToType 同样不支持显式类型转换。
func (t *unknownPreserved) ConvertToType(typeValue ref.Type) ref.Val {
	return types.NewErr("type conversion to '%s' not supported for values preserved by x-kubernetes-preserve-unknown-fields", typeValue.TypeName())
}

// Equal 用语义相等（而非 Go ==）比较，以便正确处理 map/slice 等不可比较类型。
func (t *unknownPreserved) Equal(other ref.Val) ref.Val {
	return types.Bool(equality.Semantic.DeepEqual(t.u, other.Value()))
}

// Type 返回 UnknownType，表示 CEL 不应尝试对其做类型化操作。
func (t *unknownPreserved) Type() ref.Type {
	return types.UnknownType
}

// Value 返回原始数据，供 Equal 使用。
func (t *unknownPreserved) Value() interface{} {
	return t.u // used by Equal checks
}

// unstructuredMapList 表示带 x-kubernetes-list-type=map 的 OpenAPI 数组的非结构化实例。
type unstructuredMapList struct {
	unstructuredList
	escapedKeyProps []string

	sync.Once // for for lazy load of mapOfList since it is only needed if Equals is called
	mapOfList map[interface{}]interface{}
}

// getMap 懒构建 key->元素 索引；只有在做相等比较时才需要，避免每次求值都付出 O(n) 代价。
func (t *unstructuredMapList) getMap() map[interface{}]interface{} {
	t.Do(func() {
		t.mapOfList = make(map[interface{}]interface{}, len(t.elements))
		for _, e := range t.elements {
			t.mapOfList[t.toMapKey(e)] = e
		}
	})
	return t.mapOfList
}

// toMapKey 为 map list 的给定元素构造合法的 Go map key。
// element 必须是合法的 map list 条目，其所有 map key 属性均为标量类型
// （在 Go 中可比较，因而可作 map key）。
func (t *unstructuredMapList) toMapKey(element interface{}) interface{} {
	eObj, ok := element.(map[string]interface{})
	if !ok {
		return types.NewErr("unexpected data format for element of array with x-kubernetes-list-type=map: %T", element)
	}
	// Go 中数组可比较、可作 map key，而 map 与 slice 不行。
	// 因此少量 key 属性用数组特化处理，更多时
	// 回退到序列化。
	if len(t.escapedKeyProps) == 1 {
		return eObj[t.escapedKeyProps[0]]
	}
	if len(t.escapedKeyProps) == 2 {
		return [2]interface{}{eObj[t.escapedKeyProps[0]], eObj[t.escapedKeyProps[1]]}
	}
	if len(t.escapedKeyProps) == 3 {
		return [3]interface{}{eObj[t.escapedKeyProps[0]], eObj[t.escapedKeyProps[1]], eObj[t.escapedKeyProps[2]]}
	}

	key := make([]interface{}, len(t.escapedKeyProps))
	for i, kf := range t.escapedKeyProps {
		key[i] = eObj[kf]
	}
	// 超过 3 个 key 时序列化为字符串。%#v 会为字符串加引号。
	return fmt.Sprintf("%#v", key)
}

// map list 上的 Equal 忽略列表元素顺序。
func (t *unstructuredMapList) Equal(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(len(t.elements))
	if sz != oMapList.Size() {
		return types.False
	}
	tMap := t.getMap()
	seen := make(map[interface{}]struct{}, len(tMap))
	for it := oMapList.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		k := t.toMapKey(v.Value())
		tVal, ok := tMap[k]
		if !ok {
			return types.False
		}
		eq := UnstructuredToVal(tVal, t.itemsSchema).Equal(v)
		if eq != types.True {
			return eq // either false or error
		}
		seen[k] = struct{}{}
	}
	// seen 与 tMap 大小一致才能排除重复 key 造成的假阳性。
	return types.Bool(len(seen) == len(tMap))
}

// map list 的 `X + Y` 执行合并：`X` 中所有 key 的数组位置保留，
// 当 `X` 与 `Y` 的 key 集合相交时，值被 `Y` 中的值覆盖。
// `Y` 中 key 不相交的元素追加到末尾，保持其偏序。
func (t *unstructuredMapList) Add(other ref.Val) ref.Val {
	oMapList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	elements := make([]ref.Val, len(t.elements))
	for i := range t.elements {
		elements[i] = t.Get(types.Int(i))
	}
	return addToMapList(elements, oMapList, t.escapedKeyProps)
}

// escapeKeyProps 返回对每个标识符应用 Escape 后的结果。
// 无法转义的标识符保持原样，CEL 程序虽无法访问它们，
// 但内部进行相等比较时仍需保留。
func escapeKeyProps(idents []string) []string {
	result := make([]string, len(idents))
	for i, prop := range idents {
		if escaped, ok := cel.Escape(prop); ok {
			result[i] = escaped
		} else {
			result[i] = prop
		}
	}
	return result
}

// unstructuredSetList 表示带 x-kubernetes-list-type=set 的 OpenAPI 数组的非结构化实例。
type unstructuredSetList struct {
	unstructuredList

	sync.Once // for lazy load of setOfList since it is only needed if Equals is called
	set       map[interface{}]struct{}
	setErr    ref.Val
}

// getSet 懒构建集合；任一元素无法转成 key 时缓存错误值，保证重复调用结果一致。
func (t *unstructuredSetList) getSet() (map[interface{}]struct{}, ref.Val) {
	t.Do(func() {
		t.set = make(map[interface{}]struct{}, len(t.elements))
		for i := range t.elements {
			k, err := setElementKey(t.Get(types.Int(i)))
			if err != nil {
				t.set, t.setErr = nil, err
				return
			}
			t.set[k] = struct{}{}
		}
	})
	return t.set, t.setErr
}

// set list 上的 Equal 忽略列表元素顺序。
func (t *unstructuredSetList) Equal(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(len(t.elements))
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

// set list 的 `X + Y` 执行并集：保留 `X` 中所有元素的数组位置，
// 追加 `Y` 中不相交的元素，并保持其偏序。
func (t *unstructuredSetList) Add(other ref.Val) ref.Val {
	oSetList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	elements := make([]ref.Val, len(t.elements))
	for i := range t.elements {
		elements[i] = t.Get(types.Int(i))
	}
	return addToSetList(elements, oSetList)
}

// unstructuredList 表示带 x-kubernetes-list-type=atomic（默认）的 OpenAPI 数组的非结构化实例。
type unstructuredList struct {
	elements    []interface{}
	itemsSchema Schema
}

var _ = traits.Lister(&unstructuredList{})

// ConvertToNative 把列表转为 Go 切片；元素类型为 string 时逐项断言，
// 以便与 cel-go 期望的 []string 互操作。
func (t *unstructuredList) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc.Kind() {
	case reflect.Slice:
		switch t.itemsSchema.Type() {
		// 在 cel-go 中确定期望行为（见 https://github.com/google/cel-go/issues/688）之前，
		// 这是针对 https://github.com/kubernetes/kubernetes/issues/117590 的权宜之计。
		case "string":
			var result []string
			for _, e := range t.elements {
				s, ok := e.(string)
				if !ok {
					return nil, fmt.Errorf("expected all elements to be of type string, but got %T", e)
				}
				result = append(result, s)
			}
			return result, nil
		default:
			return t.elements, nil
		}
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%s'", t.Type(), typeDesc)
}

// ConvertToType 支持转为 list 自身或 list 类型对象。
func (t *unstructuredList) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.ListType:
		return t
	case types.TypeType:
		return types.ListType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", t.Type(), typeValue.TypeName())
}

// Equal 对 atomic 列表按位置逐项比较。
func (t *unstructuredList) Equal(other ref.Val) ref.Val {
	oList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := types.Int(len(t.elements))
	if sz != oList.Size() {
		return types.False
	}
	for i := types.Int(0); i < sz; i++ {
		eq := t.Get(i).Equal(oList.Get(i))
		if eq != types.True {
			return eq // either false or error
		}
	}
	return types.True
}

// Type 返回 CEL list 类型。
func (t *unstructuredList) Type() ref.Type {
	return types.ListType
}

// Value 返回底层元素切片。
func (t *unstructuredList) Value() interface{} {
	return t.elements
}

// Add 实现列表拼接，返回新的 refValList，不修改原列表。
func (t *unstructuredList) Add(other ref.Val) ref.Val {
	oList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	otherSz, _ := oList.Size().(types.Int)
	elements := make([]ref.Val, 0, len(t.elements)+int(otherSz))
	for i := range t.elements {
		elements = append(elements, t.Get(types.Int(i)))
	}
	for it := oList.Iterator(); it.HasNext() == types.True; {
		elements = append(elements, it.Next())
	}
	return types.NewRefValList(types.DefaultTypeAdapter, elements)
}

// Contains 逐元素比较；若某次比较返回 error，则继续寻找 true，全部失败才返回该 error。
func (t *unstructuredList) Contains(val ref.Val) ref.Val {
	if types.IsUnknownOrError(val) {
		return val
	}
	var err ref.Val
	sz := len(t.elements)
	for i := 0; i < sz; i++ {
		elem := UnstructuredToVal(t.elements[i], t.itemsSchema)
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

// Get 按索引取值，越界返回 CEL error。
func (t *unstructuredList) Get(idx ref.Val) ref.Val {
	iv, isInt := idx.(types.Int)
	if !isInt {
		return types.ValOrErr(idx, "unsupported index: %v", idx)
	}
	i := int(iv)
	if i < 0 || i >= len(t.elements) {
		return types.NewErr("index out of bounds: %v", idx)
	}
	return UnstructuredToVal(t.elements[i], t.itemsSchema)
}

// Iterator 预先把元素转成 CEL 值，避免迭代过程中重复转换。
func (t *unstructuredList) Iterator() traits.Iterator {
	items := make([]ref.Val, len(t.elements))
	for i, item := range t.elements {
		itemCopy := item
		items[i] = UnstructuredToVal(itemCopy, t.itemsSchema)
	}
	return &listIterator{unstructuredList: t, items: items}
}

type listIterator struct {
	*unstructuredList
	items []ref.Val
	idx   int
}

// HasNext 报告是否还有未读元素。
func (it *listIterator) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.items))
}

// Next 返回当前元素并前进；调用方需先用 HasNext 判断。
func (it *listIterator) Next() ref.Val {
	item := it.items[it.idx]
	it.idx++
	return item
}

// Size 返回元素个数。
func (t *unstructuredList) Size() ref.Val {
	return types.Int(len(t.elements))
}

// unstructuredMap 表示 OpenAPI 对象的非结构化数据实例。
type unstructuredMap struct {
	value  map[string]interface{}
	schema Schema
	// propSchema 查找特定 map key 应使用的 schema。
	propSchema func(key string) (Schema, bool)
}

var _ = traits.Mapper(&unstructuredMap{})

// ConvertToNative 只支持转为 Go map。
func (t *unstructuredMap) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc.Kind() {
	case reflect.Map:
		return t.value, nil
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%s'", t.Type(), typeDesc)
}

// ConvertToType 支持转为 map 自身或 map 类型对象。
func (t *unstructuredMap) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.MapType:
		return t
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", t.Type(), typeValue.TypeName())
}

// Equal 对已知字段按 schema 递归比较；遇到未知字段时退化为非结构化深比较，
// 保证与 x-kubernetes-preserve-unknown-fields 的数据也能正确判等。
func (t *unstructuredMap) Equal(other ref.Val) ref.Val {
	oMap, isMap := other.(traits.Mapper)
	if !isMap {
		return types.MaybeNoSuchOverloadErr(other)
	}
	if t.Size() != oMap.Size() {
		return types.False
	}

	for key, value := range t.value {
		if _, ok := t.propSchema(key); ok {
			v, found := t.Find(types.String(key))
			ov, oFound := oMap.Find(types.String(key))
			if found != oFound {
				return types.False
			}
			if !found {
				continue
			}
			vEq := v.Equal(ov)
			if vEq != types.True {
				return vEq // either false or error
			}
		} else {
			// 必须是带 properties 的对象。
			// 既然遇到未知字段，就回退到非结构化相等比较。
			ouMap, ok := other.(*unstructuredMap)
			if !ok {
				// 编译器保证相等比较双方是同一对象类型，因此这里不应可达。
				return types.MaybeNoSuchOverloadErr(other)
			}
			if oValue, ok := ouMap.value[key]; ok {
				if !equality.Semantic.DeepEqual(value, oValue) {
					return types.False
				}
			}
		}
	}
	return types.True
}

// Type 返回 CEL map 类型。
func (t *unstructuredMap) Type() ref.Type {
	return types.MapType
}

// Value 返回底层 map。
func (t *unstructuredMap) Value() interface{} {
	return t.value
}

// Contains 实现 CEL 的 in 操作符。
func (t *unstructuredMap) Contains(key ref.Val) ref.Val {
	v, found := t.Find(key)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}

	return types.Bool(found)
}

// Get 按键取值；缺失时返回 CEL error，与 cel-go 的 map 语义一致。
func (t *unstructuredMap) Get(key ref.Val) ref.Val {
	v, found := t.Find(key)
	if found {
		return v
	}
	return types.ValOrErr(key, "no such key: %v", key)
}

// Iterator 只遍历 schema 声明过的键，并对属性名做 CEL 转义。
func (t *unstructuredMap) Iterator() traits.Iterator {
	isObject := t.schema.Properties() != nil
	keys := make([]ref.Val, len(t.value))
	i := 0
	for k := range t.value {
		if _, ok := t.propSchema(k); ok {
			mapKey := k
			if isObject {
				if escaped, ok := cel.Escape(k); ok {
					mapKey = escaped
				}
			}
			keys[i] = types.String(mapKey)
			i++
		}
	}
	return &mapIterator{unstructuredMap: t, keys: keys}
}

type mapIterator struct {
	*unstructuredMap
	keys []ref.Val
	idx  int
}

// HasNext 报告是否还有未读键。
func (it *mapIterator) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.keys))
}

// Next 返回当前键并前进。
func (it *mapIterator) Next() ref.Val {
	key := it.keys[it.idx]
	it.idx++
	return key
}

// Size 返回 map 条目数。
func (t *unstructuredMap) Size() ref.Val {
	return types.Int(len(t.value))
}

// Find 按键查找并转换为 CEL 值；属性名需先反转义。
// 对带 properties 的对象，值为 null 的可空字段视同缺失，与 CEL 可选字段语义保持一致。
func (t *unstructuredMap) Find(key ref.Val) (ref.Val, bool) {
	isObject := t.schema.Properties() != nil
	keyStr, ok := key.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(key), true
	}
	k := keyStr.Value().(string)
	if isObject {
		k, ok = cel.Unescape(k)
		if !ok {
			return nil, false
		}
	}
	if v, ok := t.value[k]; ok {
		// 若是带 properties 的对象而非带 additionalProperties 的对象，
		// 则为 null 的 nullable 字段与缺失的 optional 字段视为相同。
		if isObject && v == nil {
			return nil, false
		}
		if propSchema, ok := t.propSchema(k); ok {
			return UnstructuredToVal(v, propSchema), true
		}
	}

	return nil, false
}
