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

// UnstructuredConverter 在 interface{} 与 map[string]interface{} 表示之间转换。
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

	// DefaultUnstructuredConverter 执行 unstructured 到 Go 强类型对象的转换。
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

// unstructuredConverter 负责 interface{} 与 Unstructured 的双向转换。
type unstructuredConverter struct {
	// 开启后额外走一遍 JSON 转换做交叉校验，仅用于测试。
	mismatchDetection bool
	// comparison 是默认的测试比较逻辑，用于对比基于反射和基于 JSON 的转换结果。
	comparison conversion.Equalities
}

// 创建用于测试的 UnstructuredConverter。它通过反射把 JSON 风格的 map 转换为 Go 类型，并自动检测反射转换与 JSON 转换之间的不一致；普通业务代码优先使用 DefaultUnstructuredConverter。
func NewTestUnstructuredConverter(comparison conversion.Equalities) UnstructuredConverter {
	return NewTestUnstructuredConverterWithValidation(comparison)
}

// 创建可访问 FromUnstructuredWithValidation 的测试转换器。返回具体实现类型，是为了让测试代码能够调用额外的未知字段校验能力。
func NewTestUnstructuredConverterWithValidation(comparison conversion.Equalities) *unstructuredConverter {
	return &unstructuredConverter{
		mismatchDetection: true,
		comparison:        comparison,
	}
}

// fromUnstructuredContext 记录转换器递归遍历过程中的状态。
type fromUnstructuredContext struct {
	// isInlined 表示当前是否处于内联字段中；内联字段要延后校验 matchedKeys，
	// 只在本层收集。该状态只能由 `structFromUnstructured` 设置。
	isInlined bool
	// matchedKeys 是栈结构，记录目标 Go 类型中存在的字段集合。
	// 只能通过 `pushMatchedKeyTracker`、`recordMatchedKey` 和
	// `popAndVerifyMatchedKeys` 操作。
	matchedKeys []map[string]struct{}
	// parentPath 记录遍历 unstructured JSON map 时的路径，用于报告
	// 未知字段的完整位置。
	parentPath []string
	// returnUnknownFields 决定是否收集未知字段错误并返回给调用方。
	returnUnknownFields bool
	// unknownFieldErrors 收集对象里每个未知字段的完整路径。
	unknownFieldErrors []error
}

// pushMatchedKeyTracker 为当前层级压入一个占位集合，用于跟踪已匹配的 key。
// 只应从 `structFromUnstructured` 调用；未知字段检测关闭时完全跳过以省分配。
func (c *fromUnstructuredContext) pushMatchedKeyTracker() {
	if !c.returnUnknownFields {
		return
	}

	c.matchedKeys = append(c.matchedKeys, nil)
}

// recordMatchedKey 在 matchedKeys 栈顶记录 'key'（必要时先初始化）。
// 只能由 `structFromUnstructured` 调用。
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

// popAndVerifyMatchedKeys 弹出 matchedKeys 栈顶，与源数据核对后为不匹配的键
// 记录未知字段错误。
// `mapValue` 是 sv 中当前层的所有键（即 sv.MapKeys）；`matchedKeys` 是目标
// 对象中当前层的全部字段。只能由 `structFromUnstructured` 调用。
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

// 将 map[string]interface{} 形式的非结构化对象转换为具体 Go 类型。若目标类型实现 json.Unmarshaler，则优先使用其自定义反序列化逻辑；否则使用反射转换。参数用于决定遇到未知字段时是报错还是忽略。
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

// 将非结构化 map 转换为具体 Go 对象。目标类型实现 json.Unmarshaler 时走自定义反序列化，否则通过反射逐字段赋值。
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
		// 这些类型需要额外的转换逻辑。
	default:
		// 简单类型走这条通用路径即可。
		if st.AssignableTo(dt) {
			dv.Set(sv)
			return nil
		}
		// 不能直接用 "ConvertibleTo"：JSON 不支持布尔、整数、浮点和字符串
		// 四组之间的相互转换，这里必须保持一致。
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

	// 缓存未命中，需要计算字段名。
	info := &fieldInfo{}
	typeField := structType.Field(field)
	jsonTag, exists := typeField.Tag.Lookup("json")
	if !exists || len(jsonTag) == 0 {
		if !typeField.Anonymous {
			// 与标准库对未指定 json tag 名称的字段的命名行为保持一致。
			info.name = typeField.Name
		}
	} else {
		items := strings.Split(jsonTag, ",")
		info.name = items[0]
		if isInlinedFromTag(typeField, items[0], items[1:]) {
			// 与标准库行为一致（由 tag 控制时）：空名 + 内联选项表示该字段的键应展开到父对象。
			info.name = ""
		} else if len(info.name) == 0 && !typeField.Anonymous {
			// 与标准库对未指定 json tag 名称的字段的命名行为保持一致。
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
		// 原始 []byte 表示被存为 string。该转换是允许的，但序列化数据要谨慎处理：
		// []byte 在 JSON 中是 base64 字符串，先 Marshal 再 Unmarshal 才能正确还原。
		if len(sv.Interface().(string)) > 0 {
			marshalled, err := json.Marshal(sv.Interface())
			if err != nil {
				return fmt.Errorf("error encoding %s to json: %v", st, err)
			}
			// TODO: 这里是否真的需要 Unmarshal？
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
			// 该字段是内联的，用同一组已匹配 key 再次递归调用 fromUnstructured；
			// 内联字段的键直接属于当前层，因此沿用同一匹配集合。
			ctx.isInlined = true
			if err := fromUnstructured(sv, fv, ctx); err != nil {
				return err
			}
			ctx.isInlined = svInlined
		} else {
			// 该字段未内联，需要递归处理 sv 中与 dv 第 i 个字段对应的
			// 子字段，并使用新的 matchedKeys，同时把 parentPath 加深一层。
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
	// TODO: 这种转换是否安全？
	dv.Set(sv)
	return nil
}

// 将 Go 对象转换为 map[string]interface{}。若对象实现 json.Marshaler，则使用其自定义序列化结果；否则通过反射遍历字段。输入已经是 Unstructured 时直接返回其内容，不额外复制。
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

// 深拷贝符合 JSON 数据模型的 map。支持 JSON 基本类型、[]interface{}、map[string]interface{}、json.Number、nil，以及 Kubernetes 额外允许的 int64。
func DeepCopyJSON(x map[string]interface{}) map[string]interface{} {
	return DeepCopyJSONValue(x).(map[string]interface{})
}

// 递归深拷贝符合 JSON 数据模型的值。遇到不属于支持集合的类型会 panic，因为这意味着传入内容不符合 Unstructured 的数据约束。
func DeepCopyJSONValue(x interface{}) interface{} {
	switch x := x.(type) {
	case map[string]interface{}:
		if x == nil {
			// 带类型的 nil：interface{} 中装着值为 nil 的 map[string]interface{}。
			return x
		}
		clone := make(map[string]interface{}, len(x))
		for k, v := range x {
			clone[k] = DeepCopyJSONValue(v)
		}
		return clone
	case []interface{}:
		if x == nil {
			// 带类型的 nil：interface{} 中装着值为 nil 的 []interface{}。
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
		// 无需存储任何内容，直接结束。
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
		// TODO: 看起来长度为 0 的 map 会被忽略。
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
			// 跳过该字段。
			continue
		}
		if fieldInfo.omitempty && isEmpty(fv) {
			// 忽略 omitempty 字段。
			continue
		}
		if fieldInfo.omitzero != nil && fieldInfo.omitzero(fv) {
			// 忽略 omitzero 字段。
			continue
		}
		if len(fieldInfo.name) == 0 {
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
