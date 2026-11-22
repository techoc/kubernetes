/*
Copyright 2017 The Kubernetes Authors.

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

package runtime

import (
	encodingjson "encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/structured-merge-diff/v6/value"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"k8s.io/klog/v2"
)

// UnstructuredConverter is an interface for converting between interface{}
// and map[string]interface representation.
type UnstructuredConverter interface {
	ToUnstructured(obj interface{}) (map[string]interface{}, error)
	FromUnstructured(u map[string]interface{}, obj interface{}) error
}

// structField 作为字段缓存键：结构体类型 + 字段下标，足以唯一标识一个字段。
type structField struct {
	structType reflect.Type
	field      int
}

// fieldInfo 缓存单个字段的 JSON 元数据，避免每次转换都重新解析 struct tag。
type fieldInfo struct {
	name      string
	nameValue reflect.Value
	omitempty bool
	omitzero  func(dv reflect.Value) bool
}

// fieldsCacheMap 是字段元数据的只读快照；写入时整体替换，读路径无需加锁。
type fieldsCacheMap map[structField]*fieldInfo

// fieldsCache 采用 copy-on-write + atomic.Value：读操作无锁，写操作串行化。
// 这种设计让热点转换路径避免了 mutex 竞争。
type fieldsCache struct {
	sync.Mutex
	value atomic.Value
}

func newFieldsCache() *fieldsCache {
	cache := &fieldsCache{}
	cache.value.Store(make(fieldsCacheMap))
	return cache
}

var (
	mapStringInterfaceType = reflect.TypeOf(map[string]interface{}{})
	stringType             = reflect.TypeOf(string(""))
	fieldCache             = newFieldsCache()

	// DefaultUnstructuredConverter performs unstructured to Go typed object conversions.
	//
	// 比较函数把 time.Time 按 UTC 归一化，因为同一时刻可能因时区不同而
	// DeepEqual 失败。KUBE_PATCH_CONVERSION_DETECTOR 可开启测试用的双重校验。
	DefaultUnstructuredConverter = &unstructuredConverter{
		mismatchDetection: parseBool(os.Getenv("KUBE_PATCH_CONVERSION_DETECTOR")),
		comparison: conversion.EqualitiesOrDie(
			func(a, b time.Time) bool {
				return a.UTC() == b.UTC()
			},
		),
	}
)

// parseBool 解析环境变量开关；非法值只记录错误并按 false 处理，避免启动失败。
func parseBool(key string) bool {
	if len(key) == 0 {
		return false
	}
	value, err := strconv.ParseBool(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't parse '%s' as bool for unstructured mismatch detection", key))
	}
	return value
}

// unstructuredConverter knows how to convert between interface{} and
// Unstructured in both ways.
type unstructuredConverter struct {
	// If true, we will be additionally running conversion via json
	// to ensure that the result is true.
	// This is supposed to be set only in tests.
	mismatchDetection bool
	// comparison is the default test logic used to compare
	// the results of reflection-based and json-based conversions.
	comparison conversion.Equalities
}

// NewTestUnstructuredConverter creates an UnstructuredConverter that accepts JSON typed maps and translates them
// to Go types via reflection. It performs mismatch detection automatically and is intended for use by external
// test tools. Use DefaultUnstructuredConverter if you do not explicitly need mismatch detection.
func NewTestUnstructuredConverter(comparison conversion.Equalities) UnstructuredConverter {
	return NewTestUnstructuredConverterWithValidation(comparison)
}

// NewTestUnstrucutredConverterWithValidation allows for access to
// FromUnstructuredWithValidation from within tests.
//
// 函数名中的拼写错误是历史遗留，为保持兼容不修改；返回具体类型以便调用
// FromUnstructuredWithValidation。
func NewTestUnstructuredConverterWithValidation(comparison conversion.Equalities) *unstructuredConverter {
	return &unstructuredConverter{
		mismatchDetection: true,
		comparison:        comparison,
	}
}

// fromUnstructuredContext provides options for informing the converter
// the state of its recursive walk through the conversion process.
type fromUnstructuredContext struct {
	// isInlined indicates whether the converter is currently in
	// an inlined field or not to determine whether it should
	// validate the matchedKeys yet or only collect them.
	// This should only be set from `structFromUnstructured`
	isInlined bool
	// matchedKeys is a stack of the set of all fields that exist in the
	// concrete go type of the object being converted into.
	// This should only be manipulated via `pushMatchedKeyTracker`,
	// `recordMatchedKey`, or `popAndVerifyMatchedKeys`
	matchedKeys []map[string]struct{}
	// parentPath collects the path that the conversion
	// takes as it traverses the unstructured json map.
	// It is used to report the full path to any unknown
	// fields that the converter encounters.
	parentPath []string
	// returnUnknownFields indicates whether or not
	// unknown field errors should be collected and
	// returned to the caller
	returnUnknownFields bool
	// unknownFieldErrors are the collection of
	// the full path to each unknown field in the
	// object.
	unknownFieldErrors []error
}

// pushMatchedKeyTracker adds a placeholder set for tracking
// matched keys for the given level. This should only be
// called from `structFromUnstructured`.
//
// 每个结构体层级压入一个占位集合；未知字段检测关闭时完全跳过以省分配。
func (c *fromUnstructuredContext) pushMatchedKeyTracker() {
	if !c.returnUnknownFields {
		return
	}

	c.matchedKeys = append(c.matchedKeys, nil)
}

// recordMatchedKey initializes the last element of matchedKeys
// (if needed) and sets 'key'. This should only be called from
// `structFromUnstructured`.
func (c *fromUnstructuredContext) recordMatchedKey(key string) {
	if !c.returnUnknownFields {
		return
	}

	// 惰性初始化：只有真的出现字段匹配时才分配 map。
	last := len(c.matchedKeys) - 1
	if c.matchedKeys[last] == nil {
		c.matchedKeys[last] = map[string]struct{}{}
	}
	c.matchedKeys[last][key] = struct{}{}
}

// popAndVerifyMatchedKeys pops the last element of matchedKeys,
// checks the matched keys against the data, and adds unknown
// field errors for any matched keys.
// `mapValue` is the value of sv containing all of the keys that exist at this level
// (ie. sv.MapKeys) in the source data.
// `matchedKeys` are all the keys found for that level in the destination object.
// This should only be called from `structFromUnstructured`.
func (c *fromUnstructuredContext) popAndVerifyMatchedKeys(mapValue reflect.Value) {
	if !c.returnUnknownFields {
		return
	}

	last := len(c.matchedKeys) - 1
	curMatchedKeys := c.matchedKeys[last]
	c.matchedKeys[last] = nil
	c.matchedKeys = c.matchedKeys[:last]
	// 源数据中未被目标结构体消费的 key 即未知字段。
	for _, key := range mapValue.MapKeys() {
		if _, ok := curMatchedKeys[key.String()]; !ok {
			c.recordUnknownField(key.String())
		}
	}
}

// recordUnknownField 生成带完整路径的未知字段错误；路径通过临时压入 key 再
// 回退得到，避免复制 parentPath。
func (c *fromUnstructuredContext) recordUnknownField(field string) {
	if !c.returnUnknownFields {
		return
	}

	pathLen := len(c.parentPath)
	c.pushKey(field)
	errPath := strings.Join(c.parentPath, "")
	c.parentPath = c.parentPath[:pathLen]
	c.unknownFieldErrors = append(c.unknownFieldErrors, fmt.Errorf(`unknown field "%s"`, errPath))
}

// pushIndex 把数组下标以 "[i]" 形式追加到路径中。
func (c *fromUnstructuredContext) pushIndex(index int) {
	if !c.returnUnknownFields {
		return
	}

	c.parentPath = append(c.parentPath, "[", strconv.Itoa(index), "]")
}

// pushKey 把字段名追加到路径中，非首段前加 "." 分隔。
func (c *fromUnstructuredContext) pushKey(key string) {
	if !c.returnUnknownFields {
		return
	}

	if len(c.parentPath) > 0 {
		c.parentPath = append(c.parentPath, ".")
	}
	c.parentPath = append(c.parentPath, key)

}

// FromUnstructuredWithValidation converts an object from map[string]interface{} representation into a concrete type.
// It uses encoding/json/Unmarshaler if object implements it or reflection if not.
// It takes a validationDirective that indicates how to behave when it encounters unknown fields.
func (c *unstructuredConverter) FromUnstructuredWithValidation(u map[string]interface{}, obj interface{}, returnUnknownFields bool) error {
	t := reflect.TypeOf(obj)
	value := reflect.ValueOf(obj)
	if t.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("FromUnstructured requires a non-nil pointer to an object, got %v", t)
	}

	fromUnstructuredContext := &fromUnstructuredContext{
		returnUnknownFields: returnUnknownFields,
	}
	err := fromUnstructured(reflect.ValueOf(u), value.Elem(), fromUnstructuredContext)
	if c.mismatchDetection {
		// 测试模式：再用标准库 JSON 路径转换一次，两条路径的结果必须一致。
		newObj := reflect.New(t.Elem()).Interface()
		newErr := fromUnstructuredViaJSON(u, newObj)
		if (err != nil) != (newErr != nil) {
			//nolint:logcheck // Should not be reached.
			klog.Fatalf("FromUnstructured unexpected error for %v: error: %v", u, err)
		}
		if err == nil && !c.comparison.DeepEqual(obj, newObj) {
			//nolint:logcheck // Should not be reached.
			klog.Fatalf("FromUnstructured mismatch\nobj1: %#v\nobj2: %#v", obj, newObj)
		}
	}
	if err != nil {
		return err
	}
	if returnUnknownFields && len(fromUnstructuredContext.unknownFieldErrors) > 0 {
		// 排序保证错误输出稳定，便于测试断言与用户阅读。
		sort.Slice(fromUnstructuredContext.unknownFieldErrors, func(i, j int) bool {
			return fromUnstructuredContext.unknownFieldErrors[i].Error() <
				fromUnstructuredContext.unknownFieldErrors[j].Error()
		})
		return NewStrictDecodingError(fromUnstructuredContext.unknownFieldErrors)
	}
	return nil
}

// FromUnstructured converts an object from map[string]interface{} representation into a concrete type.
// It uses encoding/json/Unmarshaler if object implements it or reflection if not.
func (c *unstructuredConverter) FromUnstructured(u map[string]interface{}, obj interface{}) error {
	return c.FromUnstructuredWithValidation(u, obj, false)
}

// fromUnstructuredViaJSON 是测试对照路径：走标准库 JSON 编解码完成同样的转换。
func fromUnstructuredViaJSON(u map[string]interface{}, obj interface{}) error {
	data, err := json.Marshal(u)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, obj)
}

// fromUnstructured 把源值 sv 递归转换为目标类型 dv。它刻意模仿 encoding/json
// 的转换规则（而非 Go 的 ConvertibleTo），因为 wire 格式只允许 JSON 支持的转换。
func fromUnstructured(sv, dv reflect.Value, ctx *fromUnstructuredContext) error {
	sv = unwrapInterface(sv)
	if !sv.IsValid() {
		// 源值缺失（nil interface）时把目标置零。
		dv.Set(reflect.Zero(dv.Type()))
		return nil
	}
	st, dt := sv.Type(), dv.Type()

	switch dt.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Struct, reflect.Interface:
		// Those require non-trivial conversion.
	default:
		// This should handle all simple types.
		if st.AssignableTo(dt) {
			dv.Set(sv)
			return nil
		}
		// We cannot simply use "ConvertibleTo", as JSON doesn't support conversions
		// between those four groups: bools, integers, floats and string. We need to
		// do the same.
		if st.ConvertibleTo(dt) {
			switch st.Kind() {
			case reflect.String:
				switch dt.Kind() {
				case reflect.String:
					dv.Set(sv.Convert(dt))
					return nil
				}
			case reflect.Bool:
				switch dt.Kind() {
				case reflect.Bool:
					dv.Set(sv.Convert(dt))
					return nil
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				switch dt.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					dv.Set(sv.Convert(dt))
					return nil
				case reflect.Float32, reflect.Float64:
					dv.Set(sv.Convert(dt))
					return nil
				}
			case reflect.Float32, reflect.Float64:
				switch dt.Kind() {
				case reflect.Float32, reflect.Float64:
					dv.Set(sv.Convert(dt))
					return nil
				}
				// 浮点转整数仅在小数部分为 0 时允许，与 JSON 解码语义一致。
				if sv.Float() == math.Trunc(sv.Float()) {
					dv.Set(sv.Convert(dt))
					return nil
				}
			}
			return fmt.Errorf("cannot convert %s to %s", st.String(), dt.String())
		}
	}

	// Check if the object has a custom JSON marshaller/unmarshaller.
	// 自定义转换（structured-merge-diff 的 TypeReflectEntry）优先于反射逻辑。
	entry := value.TypeReflectEntryOf(dv.Type())
	if entry.CanConvertFromUnstructured() {
		return entry.FromUnstructured(sv, dv)
	}

	switch dt.Kind() {
	case reflect.Map:
		return mapFromUnstructured(sv, dv, ctx)
	case reflect.Slice:
		return sliceFromUnstructured(sv, dv, ctx)
	case reflect.Pointer:
		return pointerFromUnstructured(sv, dv, ctx)
	case reflect.Struct:
		return structFromUnstructured(sv, dv, ctx)
	case reflect.Interface:
		return interfaceFromUnstructured(sv, dv)
	default:
		return fmt.Errorf("unrecognized type: %v", dt.Kind())
	}

}

// fieldInfoFromField 解析并缓存字段的 json tag 信息。缓存采用读无锁、
// 写时复制的方式，避免在并发转换热点路径上争抢锁。
func fieldInfoFromField(structType reflect.Type, field int) *fieldInfo {
	fieldCacheMap := fieldCache.value.Load().(fieldsCacheMap)
	if info, ok := fieldCacheMap[structField{structType, field}]; ok {
		return info
	}

	// Cache miss - we need to compute the field name.
	info := &fieldInfo{}
	typeField := structType.Field(field)
	jsonTag, exists := typeField.Tag.Lookup("json")
	if !exists || len(jsonTag) == 0 {
		if !typeField.Anonymous {
			// match stdlib behavior for naming fields that don't specify a json tag name
			info.name = typeField.Name
		}
	} else {
		items := strings.Split(jsonTag, ",")
		info.name = items[0]
		if isInlinedFromTag(typeField, items[0], items[1:]) {
			// match stdlib behavior when controlled by tag
			// 空名 + 内联选项表示该字段的键应展开到父对象。
			info.name = ""
		} else if len(info.name) == 0 && !typeField.Anonymous {
			// match stdlib behavior for naming fields that don't specify a json tag name
			info.name = typeField.Name
		}

		for i := range items {
			if i > 0 && items[i] == "omitempty" {
				info.omitempty = true
			}
			if i > 0 && items[i] == "omitzero" {
				info.omitzero = value.OmitZeroFunc(typeField.Type)
			}
		}
	}
	info.nameValue = reflect.ValueOf(info.name)

	// 写路径加锁并基于最新快照复制，保证已发布的 map 不会被并发修改。
	fieldCache.Lock()
	defer fieldCache.Unlock()
	fieldCacheMap = fieldCache.value.Load().(fieldsCacheMap)
	newFieldCacheMap := make(fieldsCacheMap)
	for k, v := range fieldCacheMap {
		newFieldCacheMap[k] = v
	}
	newFieldCacheMap[structField{structType, field}] = info
	fieldCache.value.Store(newFieldCacheMap)
	return info
}

// unwrapInterface 剥掉所有 interface 包装，得到具体的动态值。
func unwrapInterface(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

// mapFromUnstructured 把 map 源值复制到目标 map；键需可赋值或可转换，
// 值为 nil interface 时写入零值。
func mapFromUnstructured(sv, dv reflect.Value, ctx *fromUnstructuredContext) error {
	st, dt := sv.Type(), dv.Type()
	if st.Kind() != reflect.Map {
		return fmt.Errorf("cannot restore map from %v", st.Kind())
	}

	if !st.Key().AssignableTo(dt.Key()) && !st.Key().ConvertibleTo(dt.Key()) {
		return fmt.Errorf("cannot copy map with non-assignable keys: %v %v", st.Key(), dt.Key())
	}

	if sv.IsNil() {
		// nil map 与空 map 语义不同，必须保留 nil。
		dv.Set(reflect.Zero(dt))
		return nil
	}
	dv.Set(reflect.MakeMap(dt))
	for _, key := range sv.MapKeys() {
		value := reflect.New(dt.Elem()).Elem()
		if val := unwrapInterface(sv.MapIndex(key)); val.IsValid() {
			if err := fromUnstructured(val, value, ctx); err != nil {
				return err
			}
		} else {
			// 源值为 nil interface 时，目标元素置零。
			value.Set(reflect.Zero(dt.Elem()))
		}
		if st.Key().AssignableTo(dt.Key()) {
			dv.SetMapIndex(key, value)
		} else {
			dv.SetMapIndex(key.Convert(dt.Key()), value)
		}
	}
	return nil
}

// sliceFromUnstructured 处理切片转换，并兼容 wire 上 []byte 被表示为 string
// 的特殊情况。
func sliceFromUnstructured(sv, dv reflect.Value, ctx *fromUnstructuredContext) error {
	st, dt := sv.Type(), dv.Type()
	if st.Kind() == reflect.String && dt.Elem().Kind() == reflect.Uint8 {
		// We store original []byte representation as string.
		// This conversion is allowed, but we need to be careful about
		// marshaling data appropriately.
		// []byte 在 JSON 中是 base64 字符串，先 Marshal 再 Unmarshal 才能正确还原。
		if len(sv.Interface().(string)) > 0 {
			marshalled, err := json.Marshal(sv.Interface())
			if err != nil {
				return fmt.Errorf("error encoding %s to json: %v", st, err)
			}
			// TODO: Is this Unmarshal needed?
			var data []byte
			err = json.Unmarshal(marshalled, &data)
			if err != nil {
				return fmt.Errorf("error decoding from json: %v", err)
			}
			dv.SetBytes(data)
		} else {
			dv.Set(reflect.MakeSlice(dt, 0, 0))
		}
		return nil
	}
	if st.Kind() != reflect.Slice {
		return fmt.Errorf("cannot restore slice from %v", st.Kind())
	}

	if sv.IsNil() {
		dv.Set(reflect.Zero(dt))
		return nil
	}
	// 预分配与源相同容量，避免逐元素扩容。
	dv.Set(reflect.MakeSlice(dt, sv.Len(), sv.Cap()))

	pathLen := len(ctx.parentPath)
	defer func() {
		// 无论成功失败都恢复路径，保证错误路径不会污染后续兄弟节点。
		ctx.parentPath = ctx.parentPath[:pathLen]
	}()
	for i := 0; i < sv.Len(); i++ {
		ctx.pushIndex(i)
		if err := fromUnstructured(sv.Index(i), dv.Index(i), ctx); err != nil {
			return err
		}
		ctx.parentPath = ctx.parentPath[:pathLen]
	}
	return nil
}

// pointerFromUnstructured 为指针目标分配新值后递归转换；源为 nil 指针时
// 目标也置 nil。
func pointerFromUnstructured(sv, dv reflect.Value, ctx *fromUnstructuredContext) error {
	st, dt := sv.Type(), dv.Type()

	if st.Kind() == reflect.Pointer && sv.IsNil() {
		dv.Set(reflect.Zero(dt))
		return nil
	}
	dv.Set(reflect.New(dt.Elem()))
	switch st.Kind() {
	case reflect.Pointer, reflect.Interface:
		return fromUnstructured(sv.Elem(), dv.Elem(), ctx)
	default:
		return fromUnstructured(sv, dv.Elem(), ctx)
	}
}

// structFromUnstructured 按字段名匹配源 map。内联字段（json tag 名为空）
// 复用同一层的匹配集合；非内联字段则各层独立记录，用于未知字段检测。
func structFromUnstructured(sv, dv reflect.Value, ctx *fromUnstructuredContext) error {
	st, dt := sv.Type(), dv.Type()
	if st.Kind() != reflect.Map {
		return fmt.Errorf("cannot restore struct from: %v", st.Kind())
	}

	pathLen := len(ctx.parentPath)
	svInlined := ctx.isInlined
	defer func() {
		ctx.parentPath = ctx.parentPath[:pathLen]
		ctx.isInlined = svInlined
	}()
	if !svInlined {
		ctx.pushMatchedKeyTracker()
	}
	for i := 0; i < dt.NumField(); i++ {
		fieldInfo := fieldInfoFromField(dt, i)
		fv := dv.Field(i)

		if len(fieldInfo.name) == 0 {
			// This field is inlined, recurse into fromUnstructured again
			// with the same set of matched keys.
			// 内联字段的键直接属于当前层，因此沿用同一匹配集合。
			ctx.isInlined = true
			if err := fromUnstructured(sv, fv, ctx); err != nil {
				return err
			}
			ctx.isInlined = svInlined
		} else {
			// This field is not inlined so we recurse into
			// child field of sv corresponding to field i of
			// dv, with a new set of matchedKeys and updating
			// the parentPath to indicate that we are one level
			// deeper.
			ctx.recordMatchedKey(fieldInfo.name)
			value := unwrapInterface(sv.MapIndex(fieldInfo.nameValue))
			if value.IsValid() {
				ctx.isInlined = false
				ctx.pushKey(fieldInfo.name)
				if err := fromUnstructured(value, fv, ctx); err != nil {
					return err
				}
				ctx.parentPath = ctx.parentPath[:pathLen]
				ctx.isInlined = svInlined
			} else {
				// 源数据缺少该字段时显式置零，避免复用对象时残留旧值。
				fv.Set(reflect.Zero(fv.Type()))
			}
		}
	}
	if !svInlined {
		ctx.popAndVerifyMatchedKeys(sv)
	}
	return nil
}

// interfaceFromUnstructured 直接把源值赋给 interface{} 目标。
func interfaceFromUnstructured(sv, dv reflect.Value) error {
	// TODO: Is this conversion safe?
	dv.Set(sv)
	return nil
}

// ToUnstructured converts an object into map[string]interface{} representation.
// It uses encoding/json/Marshaler if object implements it or reflection if not.
//
// 已是 Unstructured 的对象直接复用其内容（不做拷贝）；其余对象走反射转换。
func (c *unstructuredConverter) ToUnstructured(obj interface{}) (map[string]interface{}, error) {
	var u map[string]interface{}
	var err error
	if unstr, ok := obj.(Unstructured); ok {
		u = unstr.UnstructuredContent()
	} else {
		t := reflect.TypeOf(obj)
		value := reflect.ValueOf(obj)
		if t.Kind() != reflect.Pointer || value.IsNil() {
			return nil, fmt.Errorf("ToUnstructured requires a non-nil pointer to an object, got %v", t)
		}
		u = map[string]interface{}{}
		err = toUnstructured(value.Elem(), reflect.ValueOf(&u).Elem())
	}
	if c.mismatchDetection {
		// 测试模式：与标准库 JSON 路径交叉验证。
		newUnstr := map[string]interface{}{}
		newErr := toUnstructuredViaJSON(obj, &newUnstr)
		if (err != nil) != (newErr != nil) {
			//nolint:logcheck // Should not be reached.
			klog.Fatalf("ToUnstructured unexpected error for %v: error: %v; newErr: %v", obj, err, newErr)
		}
		if err == nil && !c.comparison.DeepEqual(u, newUnstr) {
			//nolint:logcheck // Should not be reached.
			klog.Fatalf("ToUnstructured mismatch\nobj1: %#v\nobj2: %#v", u, newUnstr)
		}
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// DeepCopyJSON deep copies the passed value, assuming it is a valid JSON representation i.e. only contains
// types produced by json.Unmarshal() and also int64.
// bool, int64, float64, string, []interface{}, map[string]interface{}, json.Number and nil
func DeepCopyJSON(x map[string]interface{}) map[string]interface{} {
	return DeepCopyJSONValue(x).(map[string]interface{})
}

// DeepCopyJSONValue deep copies the passed value, assuming it is a valid JSON representation i.e. only contains
// types produced by json.Unmarshal() and also int64.
// bool, int64, float64, string, []interface{}, map[string]interface{}, json.Number and nil
//
// 只复制 JSON 可表达的类型；遇到其他类型直接 panic，因为那说明调用方
// 传入了非法的 unstructured 内容。
func DeepCopyJSONValue(x interface{}) interface{} {
	switch x := x.(type) {
	case map[string]interface{}:
		if x == nil {
			// Typed nil - an interface{} that contains a type map[string]interface{} with a value of nil
			return x
		}
		clone := make(map[string]interface{}, len(x))
		for k, v := range x {
			clone[k] = DeepCopyJSONValue(v)
		}
		return clone
	case []interface{}:
		if x == nil {
			// Typed nil - an interface{} that contains a type []interface{} with a value of nil
			return x
		}
		clone := make([]interface{}, len(x))
		for i, v := range x {
			clone[i] = DeepCopyJSONValue(v)
		}
		return clone
	case string, int64, bool, float64, nil, encodingjson.Number:
		// 标量不可变，直接返回。
		return x
	default:
		panic(fmt.Errorf("cannot deep copy %T", x))
	}
}

// toUnstructuredViaJSON 是测试对照路径：走标准库 JSON 编解码完成转换。
func toUnstructuredViaJSON(obj interface{}, u *map[string]interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, u)
}

// toUnstructured 把 Go 值递归转换为 JSON 兼容的 interface{} 值。
func toUnstructured(sv, dv reflect.Value) error {
	// Check if the object has a custom string converter.
	// 自定义转换优先于反射，保证特殊类型（如 resource.Quantity）的语义。
	entry := value.TypeReflectEntryOf(sv.Type())
	if entry.CanConvertToUnstructured() {
		v, err := entry.ToUnstructured(sv)
		if err != nil {
			return err
		}
		if v != nil {
			dv.Set(reflect.ValueOf(v))
		}
		return nil
	}
	st := sv.Type()
	switch st.Kind() {
	case reflect.String:
		dv.Set(reflect.ValueOf(sv.String()))
		return nil
	case reflect.Bool:
		dv.Set(reflect.ValueOf(sv.Bool()))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// 所有有符号整数统一提升为 int64，这是 unstructured 的约定。
		dv.Set(reflect.ValueOf(sv.Int()))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val, err := uintToUnstructuredHelper(sv.Uint())
		if err != nil {
			return err
		}
		dv.Set(reflect.ValueOf(val))
		return nil
	case reflect.Float32, reflect.Float64:
		dv.Set(reflect.ValueOf(sv.Float()))
		return nil
	case reflect.Map:
		return mapToUnstructured(sv, dv)
	case reflect.Slice:
		return sliceToUnstructured(sv, dv)
	case reflect.Pointer:
		return pointerToUnstructured(sv, dv)
	case reflect.Struct:
		return structToUnstructured(sv, dv)
	case reflect.Interface:
		return interfaceToUnstructured(sv, dv)
	default:
		return fmt.Errorf("unrecognized type: %v", st.Kind())
	}
}

// mapToUnstructured 转换 map；当目标是空接口时自动选择 map[string]interface{}。
func mapToUnstructured(sv, dv reflect.Value) error {
	st, dt := sv.Type(), dv.Type()
	if sv.IsNil() {
		dv.Set(reflect.Zero(dt))
		return nil
	}
	if dt.Kind() == reflect.Interface && dv.NumMethod() == 0 {
		if st.Key().Kind() == reflect.String {
			dv.Set(reflect.MakeMap(mapStringInterfaceType))
			dv = dv.Elem()
			dt = dv.Type()
		}
	}
	if dt.Kind() != reflect.Map {
		return fmt.Errorf("cannot convert map to: %v", dt.Kind())
	}

	if !st.Key().AssignableTo(dt.Key()) && !st.Key().ConvertibleTo(dt.Key()) {
		return fmt.Errorf("cannot copy map with non-assignable keys: %v %v", st.Key(), dt.Key())
	}

	for _, key := range sv.MapKeys() {
		value := reflect.New(dt.Elem()).Elem()
		if err := toUnstructured(sv.MapIndex(key), value); err != nil {
			return err
		}
		if st.Key().AssignableTo(dt.Key()) {
			dv.SetMapIndex(key, value)
		} else {
			dv.SetMapIndex(key.Convert(dt.Key()), value)
		}
	}
	return nil
}

// sliceToUnstructured 转换切片；[]byte 按 JSON 约定编码为 base64 字符串。
func sliceToUnstructured(sv, dv reflect.Value) error {
	st, dt := sv.Type(), dv.Type()
	if sv.IsNil() {
		dv.Set(reflect.Zero(dt))
		return nil
	}
	if st.Elem().Kind() == reflect.Uint8 {
		// []byte 在 JSON 中不是数组，而是 base64 字符串；先经 json 转换。
		dv.Set(reflect.New(stringType))
		data, err := json.Marshal(sv.Bytes())
		if err != nil {
			return err
		}
		var result string
		if err = json.Unmarshal(data, &result); err != nil {
			return err
		}
		dv.Set(reflect.ValueOf(result))
		return nil
	}
	if dt.Kind() == reflect.Interface && dv.NumMethod() == 0 {
		dv.Set(reflect.MakeSlice(reflect.SliceOf(dt), sv.Len(), sv.Cap()))
		dv = dv.Elem()
		dt = dv.Type()
	}
	if dt.Kind() != reflect.Slice {
		return fmt.Errorf("cannot convert slice to: %v", dt.Kind())
	}
	for i := 0; i < sv.Len(); i++ {
		if err := toUnstructured(sv.Index(i), dv.Index(i)); err != nil {
			return err
		}
	}
	return nil
}

// pointerToUnstructured 解引用指针；nil 指针不写入任何键，保持“未设置”语义。
func pointerToUnstructured(sv, dv reflect.Value) error {
	if sv.IsNil() {
		// We're done - we don't need to store anything.
		return nil
	}
	return toUnstructured(sv.Elem(), dv)
}

// isEmpty 实现 encoding/json 的 omitempty 语义。
func isEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Map, reflect.Slice:
		// TODO: It seems that 0-len maps are ignored in it.
		// 与 encoding/json 一致：nil 或长度为 0 都算空。
		return v.IsNil() || v.Len() == 0
	case reflect.Pointer, reflect.Interface:
		return v.IsNil()
	}
	return false
}

// structToUnstructured 按 json tag 把结构体字段写入目标 map，规则与
// encoding/json 对齐：忽略 "-"、omitempty、omitzero，并展开内联字段。
func structToUnstructured(sv, dv reflect.Value) error {
	st, dt := sv.Type(), dv.Type()
	if dt.Kind() == reflect.Interface && dv.NumMethod() == 0 {
		dv.Set(reflect.MakeMapWithSize(mapStringInterfaceType, st.NumField()))
		dv = dv.Elem()
		dt = dv.Type()
	}
	if dt.Kind() != reflect.Map {
		return fmt.Errorf("cannot convert struct to: %v", dt.Kind())
	}
	realMap := dv.Interface().(map[string]interface{})

	for i := 0; i < st.NumField(); i++ {
		fieldInfo := fieldInfoFromField(st, i)
		fv := sv.Field(i)

		if fieldInfo.name == "-" {
			// This field should be skipped.
			continue
		}
		if fieldInfo.omitempty && isEmpty(fv) {
			// omitempty fields should be ignored.
			continue
		}
		if fieldInfo.omitzero != nil && fieldInfo.omitzero(fv) {
			// omitzero fields should be ignored
			continue
		}
		if len(fieldInfo.name) == 0 {
			// This field is inlined.
			// 内联字段的键直接写入当前 map。
			if err := toUnstructured(fv, dv); err != nil {
				return err
			}
			continue
		}
		// 基本类型走快速路径，避免反射递归；其他类型递归处理。
		switch fv.Type().Kind() {
		case reflect.String:
			realMap[fieldInfo.name] = fv.String()
		case reflect.Bool:
			realMap[fieldInfo.name] = fv.Bool()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			realMap[fieldInfo.name] = fv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := uintToUnstructuredHelper(fv.Uint())
			if err != nil {
				return err
			}
			realMap[fieldInfo.name] = val
		case reflect.Float32, reflect.Float64:
			realMap[fieldInfo.name] = fv.Float()
		default:
			subv := reflect.New(dt.Elem()).Elem()
			if err := toUnstructured(fv, subv); err != nil {
				return err
			}
			dv.SetMapIndex(fieldInfo.nameValue, subv)
		}
	}
	return nil
}

// interfaceToUnstructured 解包 interface 后递归转换；nil 接口置零。
func interfaceToUnstructured(sv, dv reflect.Value) error {
	if !sv.IsValid() || sv.IsNil() {
		dv.Set(reflect.Zero(dv.Type()))
		return nil
	}
	return toUnstructured(sv.Elem(), dv)
}

// uintToUnstructuredHelper 把无符号整数提升为 int64；超出范围时报错而不是
// 静默截断，因为 unstructured 数值统一用 int64 表示。
func uintToUnstructuredHelper(uVal uint64) (int64, error) {
	if uVal > math.MaxInt64 {
		return 0, fmt.Errorf("unsigned value %d does not fit into int64 (overflow)", uVal)
	}
	return int64(uVal), nil
}
