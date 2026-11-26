/*
Copyright The Kubernetes Authors.

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
	"encoding/base64"
	"fmt"
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/structured-merge-diff/v6/value"
)

// SchemalessTypedToVal 把 Go 值包装成 CEL ref.Val。
// 它模拟值经 JSON 序列化后转为 unstructured map 的形态，但通过反射与
// structured-merge-diff 的 value 包惰性遍历，避免完整转换的分配开销。
//
// 语义必须与 runtime.DefaultUnstructuredConverter.ToUnstructured 完全一致，
// 任何差异都是 bug。返回值内部为惰性反射做了缓存，非线程安全，
// 只能在单次 CEL 求值的同步上下文中使用。
//
// SchemalessTypedToVal 是“无 schema 的 typed 对象”进入 CEL 的入口。关键不变量：
// 结果必须与先 ToUnstructured 再 NativeToValue 完全一致，否则 CEL 表达式的
// 求值结果会随对象来自 typed 还是 unstructured 路径而不同。
// 返回值内部带惰性缓存，只能在单次 CEL 求值的单 goroutine 内使用。
func SchemalessTypedToVal(val interface{}) ref.Val {
	if val == nil {
		return types.NullValue
	}

	// 快路径：直接处理基本类型，绕开 reflect.ValueOf 的开销。
	// 热点路径：绝大多数标量直接映射，避免反射开销。
	switch typedVal := val.(type) {
	case bool:
		return types.Bool(typedVal)
	case string:
		return types.String(typedVal)
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
	case []byte:
		// 与 JSON 序列化保持一致：[]byte 在 unstructured 中表现为 base64 字符串。
		if typedVal == nil {
			return types.NullValue
		}
		return types.String(base64.StdEncoding.EncodeToString(typedVal))
	case intstr.IntOrString:
		// IntOrString 序列化后是 int 或 string，取决于其 Type 字段。
		switch typedVal.Type {
		case intstr.Int:
			return types.Int(typedVal.IntVal)
		case intstr.String:
			return types.String(typedVal.StrVal)
		}
	}

	v := reflect.ValueOf(val)
	if !v.IsValid() {
		return types.NewErr("invalid data, got invalid reflect value: %v", v)
	}

	// nil pointer/map/slice 在 JSON 中都是 null，CEL 侧统一映射为 NullValue。
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return types.NullValue
		}
	}

	// 若类型具备 ToUnstructured 能力，优先走 structured-merge-diff 的转换器，
	// 以保证 JSON tag、omitempty、自定义序列化等语义与真实转换完全一致。
	cacheEntry := value.TypeReflectEntryOf(v.Type())
	if cacheEntry.CanConvertToUnstructured() {
		if !v.CanAddr() {
			// ToUnstructured 要求可寻址的值（可能取指针方法集）。
			ptr := reflect.New(v.Type())
			ptr.Elem().Set(v)
			v = ptr.Elem()
		}
		unstr, err := cacheEntry.ToUnstructured(v)
		if err != nil {
			return types.NewErr("failed to convert to unstructured: %v", err)
		}
		if unstr == nil {
			return types.NullValue
		}
		return types.DefaultTypeAdapter.NativeToValue(unstr)
	}
	// 无法整体转换时，按 kind 递归地惰性包装。
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return types.NullValue
		}
		return SchemalessTypedToVal(v.Elem().Interface())
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 { // byte, relection return slice of Uint8 for bytes.
			return types.String(base64.StdEncoding.EncodeToString(v.Bytes()))
		}
		return &reflectSchemalessTypedList{value: v}
	case reflect.Map:
		return &reflectSchemalessTypedMap{value: v}
	case reflect.Struct:
		return &reflectSchemalessTypedStruct{value: v}
	// 按 kind 把类型别名映射到基本类型。
	case reflect.Bool:
		return types.Bool(v.Bool())
	case reflect.String:
		return types.String(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return types.Int(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uVal := v.Uint()
		// CEL 只有 int64；超出范围的 uint64 无法无损表示，宁可报错也不静默截断。
		if uVal > 9223372036854775807 { // math.MaxInt64
			return types.NewErr("unsigned value %d does not fit into int64 (overflow)", uVal)
		}
		return types.Int(uVal)
	case reflect.Float32, reflect.Float64:
		return types.Double(v.Float())
	default:
		return types.NewErr("unsupported Go type for CEL: %v", v.Type())
	}
}

var _ traits.Lister = &reflectSchemalessTypedList{}

// reflectSchemalessTypedList 把 Go 切片/数组惰性包装成 CEL Lister。
// fieldCache 非线程安全：CEL-Go 的同步求值契约保证同一个包装值只会被单个 goroutine 访问。
//
// 惰性包装的收益是：表达式只访问少数下标时无需转换整个切片。
type reflectSchemalessTypedList struct {
	value      reflect.Value
	fieldCache []ref.Val
}

// ConvertToNative 仅在目标为切片时允许，直接返回原始 Go 值。
func (l *reflectSchemalessTypedList) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc.Kind() {
	case reflect.Slice:
		return l.value.Interface(), nil
	default:
		return nil, fmt.Errorf("type conversion error from '%s' to '%s'", l.Type(), typeDesc)
	}
}

// ConvertToType 只支持转换为 list 或 type，其他转换一律报错，与 CEL 标准语义一致。
func (l *reflectSchemalessTypedList) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.ListType:
		return l
	case types.TypeType:
		return types.ListType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", l.Type(), typeValue.TypeName())
}

// Equal 逐元素比较；先比长度可快速排除不相等的情况。
func (l *reflectSchemalessTypedList) Equal(other ref.Val) ref.Val {
	otherList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := l.Size().(types.Int)
	if sz != otherList.Size().(types.Int) {
		return types.False
	}
	for i := range sz {
		v1 := l.Get(i)
		v2 := otherList.Get(i)
		if v1.Equal(v2) != types.True {
			return types.False
		}
	}
	return types.True
}

// Type 在 CEL 中始终表现为 list，即便底层是 Go 数组。
func (l *reflectSchemalessTypedList) Type() ref.Type {
	return types.ListType
}

// Value 暴露底层 Go 值，供需要原始类型的扩展函数使用。
func (l *reflectSchemalessTypedList) Value() interface{} {
	return l.value.Interface()
}

// Contains 线性扫描；CEL 列表没有哈希索引，复杂度为 O(n)。
func (l *reflectSchemalessTypedList) Contains(val ref.Val) ref.Val {
	sz := l.Size().(types.Int)
	for i := range sz {
		v := l.Get(i)
		if v.Equal(val) == types.True {
			return types.True
		}
	}
	return types.False
}

// Add 实现 CEL 的 `+`：结果退化为普通 RefValList，不再保持惰性包装。
func (l *reflectSchemalessTypedList) Add(other ref.Val) ref.Val {
	otherList, ok := other.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	sz := l.value.Len()
	otherSz, _ := otherList.Size().(types.Int)
	elements := make([]ref.Val, 0, sz+int(otherSz))
	for i := range sz {
		elements = append(elements, l.Get(types.Int(i)))
	}
	it := otherList.Iterator()
	for it.HasNext() == types.True {
		elements = append(elements, it.Next())
	}
	return types.NewRefValList(types.DefaultTypeAdapter, elements)
}

// Get 按需转换单个元素并缓存；下标越界返回 CEL 错误而非 panic。
func (l *reflectSchemalessTypedList) Get(index ref.Val) ref.Val {
	idx, ok := index.(types.Int)
	if !ok {
		return types.ValOrErr(index, "unsupported index type: %T", index)
	}
	if idx < 0 || int(idx) >= l.value.Len() {
		return types.NewErr("index out of bounds: %d", idx)
	}
	i := int(idx)
	if l.fieldCache == nil {
		l.fieldCache = make([]ref.Val, l.value.Len())
	}
	if cached := l.fieldCache[i]; cached != nil {
		return cached
	}
	v := SchemalessTypedToVal(l.value.Index(i).Interface())
	l.fieldCache[i] = v
	return v
}

// Iterator 返回惰性迭代器；迭代同样走 Get，因此共享缓存。
func (l *reflectSchemalessTypedList) Iterator() traits.Iterator {
	return &reflectSchemalessTypedListIterator{reflectSchemalessTypedList: l, idx: 0}
}

// Size 返回底层切片的长度。
func (l *reflectSchemalessTypedList) Size() ref.Val {
	return types.Int(l.value.Len())
}

// reflectSchemalessTypedListIterator 记录当前下标，遍历时无需复制元素。
type reflectSchemalessTypedListIterator struct {
	*reflectSchemalessTypedList
	idx int
}

// HasNext 判断是否还有未访问的元素。
func (it *reflectSchemalessTypedListIterator) HasNext() ref.Val {
	return types.Bool(it.idx < it.reflectSchemalessTypedList.value.Len())
}

// Next 返回当前元素并前进；越界时返回 CEL 错误。
func (it *reflectSchemalessTypedListIterator) Next() ref.Val {
	if it.idx >= it.reflectSchemalessTypedList.value.Len() {
		return types.NewErr("no more elements")
	}
	val := it.reflectSchemalessTypedList.Get(types.Int(it.idx))
	it.idx++
	return val
}

var _ traits.Mapper = &reflectSchemalessTypedMap{}

// reflectSchemalessTypedMap 把 Go map 惰性包装成 CEL Mapper。
// fieldCache 非线程安全：CEL-Go 的同步求值契约保证同一个包装值只会被单个 goroutine 访问。
//
// fieldCache 同时承担“未命中”的缓存：值为 nil 表示该 key 不存在，避免重复查找。
type reflectSchemalessTypedMap struct {
	value      reflect.Value
	fieldCache map[string]ref.Val
}

// ConvertToNative 仅当目标类型可赋值时返回原值。
func (m *reflectSchemalessTypedMap) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if m.value.Type().AssignableTo(typeDesc) {
		return m.value.Interface(), nil
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%s'", m.Type(), typeDesc)
}

// ConvertToType 支持 map 与 type 两种目标类型。
func (m *reflectSchemalessTypedMap) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.MapType:
		return m
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", m.Type(), typeValue.TypeName())
}

// Equal 先比大小再逐 key 比较；大小不同必然不相等，可提前返回。
func (m *reflectSchemalessTypedMap) Equal(other ref.Val) ref.Val {
	otherMap, ok := other.(traits.Mapper)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	if m.Size() != otherMap.Size() {
		return types.False
	}
	it := m.Iterator()
	for it.HasNext() == types.True {
		key := it.Next()
		v1, _ := m.Find(key)
		v2, found := otherMap.Find(key)
		if !found || v1.Equal(v2) != types.True {
			return types.False
		}
	}
	return types.True
}

// Type 在 CEL 中表现为 map。
func (m *reflectSchemalessTypedMap) Type() ref.Type {
	return types.MapType
}

// Value 暴露底层 Go map。
func (m *reflectSchemalessTypedMap) Value() interface{} {
	return m.value.Interface()
}

// Contains 判断 key 是否存在；查找过程本身出错时优先返回错误。
func (m *reflectSchemalessTypedMap) Contains(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if v != nil && types.IsError(v) {
		return v
	}
	return types.Bool(found)
}

// Find 按字符串 key 查找并缓存结果（含未命中）。
// 非 string 的 key 在 JSON 语义下无法表示，返回错误且 found=false。
func (m *reflectSchemalessTypedMap) Find(key ref.Val) (ref.Val, bool) {
	keyVal, ok := key.(types.String)
	if !ok {
		return types.ValOrErr(key, "unsupported map key type: %T", key), false
	}
	fieldName := string(keyVal)
	if m.fieldCache != nil {
		if cached, ok := m.fieldCache[fieldName]; ok {
			return cached, cached != nil
		}
	}

	var result ref.Val
	found := false

	keyType := m.value.Type().Key()
	if keyType.Kind() == reflect.String {
		reflectKey := reflect.ValueOf(fieldName).Convert(keyType)
		val := m.value.MapIndex(reflectKey)
		if val.IsValid() {
			result = SchemalessTypedToVal(val.Interface())
			found = true
		}
	}

	if m.fieldCache == nil {
		m.fieldCache = make(map[string]ref.Val)
	}
	m.fieldCache[fieldName] = result
	return result, found
}

// Get 与 Find 的区别在于缺失时返回错误（对应 CEL 的索引访问语义）。
func (m *reflectSchemalessTypedMap) Get(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if !found {
		if v != nil && types.IsError(v) {
			return v
		}
		return types.ValOrErr(key, "no such key: %v", key)
	}
	return v
}

// Iterator 在创建时快照 key 列表；迭代期间修改 map 不会影响本次遍历。
func (m *reflectSchemalessTypedMap) Iterator() traits.Iterator {
	return &reflectSchemalessTypedMapIterator{reflectSchemalessTypedMap: m, keys: m.value.MapKeys(), idx: 0}
}

// Size 返回 map 元素个数。
func (m *reflectSchemalessTypedMap) Size() ref.Val {
	return types.Int(m.value.Len())
}

// reflectSchemalessTypedMapIterator 遍历 key；值仍按需从 map 中取。
type reflectSchemalessTypedMapIterator struct {
	*reflectSchemalessTypedMap
	keys []reflect.Value
	idx  int
}

// HasNext 判断是否还有 key 未访问。
func (it *reflectSchemalessTypedMapIterator) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.keys))
}

// Next 返回下一个 key 的字符串形式。
func (it *reflectSchemalessTypedMapIterator) Next() ref.Val {
	if it.idx >= len(it.keys) {
		return types.NewErr("no more elements")
	}
	val := types.String(it.keys[it.idx].String())
	it.idx++
	return val
}

var _ traits.Mapper = &reflectSchemalessTypedStruct{}

// reflectSchemalessTypedStruct 把 Go 结构体惰性包装成 CEL Mapper。
// fieldCache 非线程安全：CEL-Go 的同步求值契约保证同一个包装值只会被单个 goroutine 访问。
//
// 结构体在 CEL 中伪装成 map，字段名使用 JSON 名称（由 structured-merge-diff 的
// 反射缓存提供），从而与 unstructured 视图保持一致。
type reflectSchemalessTypedStruct struct {
	value      reflect.Value
	fieldCache map[string]ref.Val
}

// ConvertToNative 仅当目标类型可赋值时返回原结构体。
func (s *reflectSchemalessTypedStruct) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if s.value.Type().AssignableTo(typeDesc) {
		return s.value.Interface(), nil
	}
	return nil, fmt.Errorf("type conversion error from struct type %v to %v", s.value.Type(), typeDesc)
}

// ConvertToType 支持 map 与 type；结构体对外表现为 map。
func (s *reflectSchemalessTypedStruct) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue {
	case types.MapType:
		return s
	case types.TypeType:
		return types.MapType
	}
	return types.NewErr("type conversion error from struct to %s", typeValue.TypeName())
}

// Equal 对同类型结构体直接用语义深比较（更快且避免逐字段转换）；
// 与其他 Mapper 比较时退化为 map 语义。
func (s *reflectSchemalessTypedStruct) Equal(other ref.Val) ref.Val {
	otherStruct, ok := other.(*reflectSchemalessTypedStruct)
	if ok {
		return types.Bool(apiequality.Semantic.DeepEqual(s.value.Interface(), otherStruct.value.Interface()))
	}
	otherMap, ok := other.(traits.Mapper)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	if s.Size() != otherMap.Size() {
		return types.False
	}
	it := s.Iterator()
	for it.HasNext() == types.True {
		key := it.Next()
		v1, _ := s.Find(key)
		v2, found := otherMap.Find(key)
		if !found {
			return types.False
		}
		eq := v1.Equal(v2)
		if eq != types.True {
			return eq
		}
	}
	return types.True
}

// Type 返回 MapType：Unstructured 视图下结构体就是 map。
func (s *reflectSchemalessTypedStruct) Type() ref.Type {
	return types.MapType // Unstructured behaves exactly as a Map
}

// Value 暴露底层 Go 结构体。
func (s *reflectSchemalessTypedStruct) Value() interface{} {
	return s.value.Interface()
}

// IsSet 实现 CEL 的 `has()` 语义；unknown/error 需要原样向上传播。
func (s *reflectSchemalessTypedStruct) IsSet(field ref.Val) ref.Val {
	v, found := s.lookupField(field)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}
	return types.Bool(found)
}

// Find 是 lookupField 的导出版本，供 CEL Mapper 接口使用。
func (s *reflectSchemalessTypedStruct) Find(key ref.Val) (ref.Val, bool) {
	return s.lookupField(key)
}

// Get 在字段不存在时返回 CEL 错误。
func (s *reflectSchemalessTypedStruct) Get(key ref.Val) ref.Val {
	v, found := s.lookupField(key)
	if !found {
		return types.ValOrErr(key, "no such key: %v", key)
	}
	return v
}

// Contains 判断字段是否存在（考虑 omitempty 后的可见性）。
func (s *reflectSchemalessTypedStruct) Contains(key ref.Val) ref.Val {
	v, found := s.Find(key)
	if v != nil && types.IsError(v) {
		return v
	}
	return types.Bool(found)
}

// lookupField 按 JSON 字段名查找并缓存。CanOmit 用于实现 omitempty 语义：
// 可省略且为空值的字段在 unstructured 中不存在，因此这里也视为未找到。
func (s *reflectSchemalessTypedStruct) lookupField(key ref.Val) (ref.Val, bool) {
	keyStr, ok := key.(types.String)
	if !ok {
		return types.ValOrErr(key, "unsupported map key type: %T", key), true
	}
	fieldName := keyStr.Value().(string)

	if s.fieldCache != nil {
		if cached, ok := s.fieldCache[fieldName]; ok {
			return cached, cached != nil
		}
	}

	var result ref.Val
	found := false

	cacheEntry := value.TypeReflectEntryOf(s.value.Type())
	fieldCache, ok := cacheEntry.Fields()[fieldName]
	if ok {
		if e := fieldCache.GetFrom(s.value); !fieldCache.CanOmit(e) {
			result = SchemalessTypedToVal(e.Interface())
			found = true
		}
	}

	if s.fieldCache == nil {
		s.fieldCache = make(map[string]ref.Val)
	}
	s.fieldCache[fieldName] = result
	return result, found
}

// Size 只统计在 unstructured 视图中可见的字段，与 JSON 序列化结果一致。
func (s *reflectSchemalessTypedStruct) Size() ref.Val {
	cacheEntry := value.TypeReflectEntryOf(s.value.Type())
	count := 0
	for _, fieldCache := range cacheEntry.Fields() {
		if e := fieldCache.GetFrom(s.value); !fieldCache.CanOmit(e) {
			count++
		}
	}
	return types.Int(count)
}

// Iterator 生成可见字段名列表；字段顺序不保证稳定，与 map 迭代语义一致。
func (s *reflectSchemalessTypedStruct) Iterator() traits.Iterator {
	cacheEntry := value.TypeReflectEntryOf(s.value.Type())
	var keys []string
	for fieldName, fieldCache := range cacheEntry.Fields() {
		if e := fieldCache.GetFrom(s.value); !fieldCache.CanOmit(e) {
			keys = append(keys, string(fieldName))
		}
	}
	return &reflectSchemalessTypedStructIterator{reflectSchemalessTypedStruct: s, keys: keys, idx: 0}
}

// reflectSchemalessTypedStructIterator 遍历预先生成的字段名列表。
type reflectSchemalessTypedStructIterator struct {
	*reflectSchemalessTypedStruct
	keys []string
	idx  int
}

// HasNext 判断是否还有字段未访问。
func (it *reflectSchemalessTypedStructIterator) HasNext() ref.Val {
	return types.Bool(it.idx < len(it.keys))
}

// Next 返回下一个字段名。
func (it *reflectSchemalessTypedStructIterator) Next() ref.Val {
	if it.idx >= len(it.keys) {
		return types.NewErr("no more elements")
	}
	val := types.String(it.keys[it.idx])
	it.idx++
	return val
}
