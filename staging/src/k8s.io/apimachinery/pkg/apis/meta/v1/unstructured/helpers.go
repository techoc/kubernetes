/*
Copyright 2015 The Kubernetes Authors.

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

package unstructured

import (
	gojson "encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog/v2"
)

// NestedFieldCopy 返回嵌套字段值的深拷贝。
// 字段缺失时返回 false；字段为 nil 不视为错误。
//
// 注意：fields 被当作对象内的键使用，不支持数组/切片下标语法。
func NestedFieldCopy(obj map[string]interface{}, fields ...string) (interface{}, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	return runtime.DeepCopyJSONValue(val), true, nil
}

// NestedFieldNoCopy 返回嵌套字段的引用（不拷贝）。
// 字段不存在时返回 false；无法遍历 obj（途中遇到非 map 节点）时返回错误。
//
// 注意：fields 被当作对象内的键使用，不支持数组/切片下标语法。
func NestedFieldNoCopy(obj map[string]interface{}, fields ...string) (interface{}, bool, error) {
	var val interface{} = obj

	for i, field := range fields {
		if val == nil {
			// 中途为 nil，无法继续下钻，按“未找到”处理。
			return nil, false, nil
		}
		if m, ok := val.(map[string]interface{}); ok {
			val, ok = m[field]
			if !ok {
				// 键不存在属于正常缺失，不是错误。
				return nil, false, nil
			}
		} else {
			// 路径上出现非 map 值说明调用方给出的路径与数据结构不符，报错并给出出错前缀。
			return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected map[string]interface{}", jsonPath(fields[:i+1]), val, val)
		}
	}
	return val, true, nil
}

// NestedString 返回嵌套字段的字符串值。
// 字段不存在时返回 false；值不是字符串时返回错误。
func NestedString(obj map[string]interface{}, fields ...string) (string, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return "", found, err
	}
	s, ok := val.(string)
	if !ok {
		return "", false, fmt.Errorf("%v accessor error: %v is of the type %T, expected string", jsonPath(fields), val, val)
	}
	return s, true, nil
}

// NestedBool 返回嵌套字段的布尔值。
// 字段不存在时返回 false；值不是布尔时返回错误。
func NestedBool(obj map[string]interface{}, fields ...string) (bool, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return false, found, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected bool", jsonPath(fields), val, val)
	}
	return b, true, nil
}

// NestedFloat64 返回嵌套字段的 float64 值。
// 字段不存在时返回 false；值不是 float64 时返回错误。
func NestedFloat64(obj map[string]interface{}, fields ...string) (float64, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}
	f, ok := val.(float64)
	if !ok {
		return 0, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected float64", jsonPath(fields), val, val)
	}
	return f, true, nil
}

// NestedInt64 返回嵌套字段的 int64 值。
// 字段不存在时返回 false；值不是 int64 时返回错误。
func NestedInt64(obj map[string]interface{}, fields ...string) (int64, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}
	i, ok := val.(int64)
	if !ok {
		return 0, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected int64", jsonPath(fields), val, val)
	}
	return i, true, nil
}

// NestedNumberAsFloat64 返回嵌套字段的 float64 值。
// 如果字段是 float64 直接返回；如果是 int64 且能无损转换为 float64，则转换后返回。
// 字段不存在时返回 false；值既不是 float64 也不是可精确表示的 int64 时返回错误。
func NestedNumberAsFloat64(obj map[string]interface{}, fields ...string) (float64, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}
	switch x := val.(type) {
	case int64:
		// 用 big.Float 检查转换精度，避免大整数在 float64 中丢精度。
		f, accuracy := big.NewInt(x).Float64()
		if accuracy != big.Exact {
			return 0, false, fmt.Errorf("%v accessor error: int64 value %v cannot be losslessly converted to float64", jsonPath(fields), x)
		}
		return f, true, nil
	case float64:
		return x, true, nil
	default:
		return 0, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected float64 or int64", jsonPath(fields), val, val)
	}
}

// NestedStringSlice 返回嵌套字段 []string 值的拷贝。
// 字段不存在、不是 []interface{} 或其中含非字符串元素时返回错误。
func NestedStringSlice(obj map[string]interface{}, fields ...string) ([]string, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	m, ok := val.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected []interface{}", jsonPath(fields), val, val)
	}
	strSlice := make([]string, 0, len(m))
	for _, v := range m {
		if str, ok := v.(string); ok {
			strSlice = append(strSlice, str)
		} else {
			return nil, false, fmt.Errorf("%v accessor error: contains non-string key in the slice: %v is of the type %T, expected string", jsonPath(fields), v, v)
		}
	}
	return strSlice, true, nil
}

// NestedSlice 返回嵌套字段 []interface{} 值的深拷贝。
// 字段不存在或不是 []interface{} 时返回错误。
func NestedSlice(obj map[string]interface{}, fields ...string) ([]interface{}, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	_, ok := val.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected []interface{}", jsonPath(fields), val, val)
	}
	return runtime.DeepCopyJSONValue(val).([]interface{}), true, nil
}

// NestedStringMap 返回嵌套字段 map[string]string 值的拷贝。
// 字段不存在、不是 map[string]interface{} 或含非字符串值时返回错误。
func NestedStringMap(obj map[string]interface{}, fields ...string) (map[string]string, bool, error) {
	m, found, err := nestedMapNoCopy(obj, false, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	strMap := make(map[string]string, len(m))
	for k, v := range m {
		if str, ok := v.(string); ok {
			strMap[k] = str
		} else {
			return nil, false, fmt.Errorf("%v accessor error: contains non-string value in the map under key %q: %v is of the type %T, expected string", jsonPath(fields), k, v, v)
		}
	}
	return strMap, true, nil
}

// NestedNullCoercingStringMap 返回嵌套字段 map[string]string 值的拷贝。
// 若值存在且显式为 null，返回 `nil, true, nil`。
// 若值不是 map 也不是 null，或 map 中含非字符串非 null 值，返回 `nil, false, err`。
// map 中的 null 值会被强制转为 ""，与 JSON 解码行为保持一致。
func NestedNullCoercingStringMap(obj map[string]interface{}, fields ...string) (map[string]string, bool, error) {
	m, found, err := nestedMapNoCopy(obj, true, fields...)
	if !found || err != nil || m == nil {
		return nil, found, err
	}
	strMap := make(map[string]string, len(m))
	for k, v := range m {
		if str, ok := v.(string); ok {
			strMap[k] = str
		} else if v == nil {
			// JSON 中 null 的反序列化结果为空字符串，这里保持同样语义。
			strMap[k] = ""
		} else {
			return nil, false, fmt.Errorf("%v accessor error: contains non-string value in the map under key %q: %v is of the type %T, expected string", jsonPath(fields), k, v, v)
		}
	}
	return strMap, true, nil
}

// NestedMap 返回嵌套字段 map[string]interface{} 值的深拷贝。
// 字段不存在或不是 map[string]interface{} 时返回错误。
func NestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	m, found, err := nestedMapNoCopy(obj, false, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	return runtime.DeepCopyJSON(m), true, nil
}

// nestedMapNoCopy 返回嵌套字段的 map[string]interface{} 值（不拷贝）。
// tolerateNil 为 true 时，显式 null 值也算有效结果（返回 nil, true, nil）。
func nestedMapNoCopy(obj map[string]interface{}, tolerateNil bool, fields ...string) (map[string]interface{}, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	if val == nil && tolerateNil {
		return nil, true, nil
	}
	m, ok := val.(map[string]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected map[string]interface{}", jsonPath(fields), val, val)
	}
	return m, true, nil
}

// SetNestedField 把嵌套字段设置为传入值的深拷贝，调用方后续修改 value 不会影响 obj。
// 若某一层嵌套不是 map[string]interface{}，则返回错误。
func SetNestedField(obj map[string]interface{}, value interface{}, fields ...string) error {
	return setNestedFieldNoCopy(obj, runtime.DeepCopyJSONValue(value), fields...)
}

// setNestedFieldNoCopy 逐层下钻并写入值；缺失的中间层会被自动补建，
// 从而支持在空对象上设置任意深度的字段。
func setNestedFieldNoCopy(obj map[string]interface{}, value interface{}, fields ...string) error {
	m := obj

	for i, field := range fields[:len(fields)-1] {
		if val, ok := m[field]; ok {
			if valMap, ok := val.(map[string]interface{}); ok {
				m = valMap
			} else {
				// 中间层已存在但不是 map，无法继续下钻。
				return fmt.Errorf("value cannot be set because %v is not a map[string]interface{}", jsonPath(fields[:i+1]))
			}
		} else {
			// 中间层缺失，创建空 map 补全路径。
			newVal := make(map[string]interface{})
			m[field] = newVal
			m = newVal
		}
	}
	m[fields[len(fields)-1]] = value
	return nil
}

// SetNestedStringSlice 设置嵌套字段为 []string 值。
// 若某一层嵌套不是 map[string]interface{}，则返回错误。
func SetNestedStringSlice(obj map[string]interface{}, value []string, fields ...string) error {
	m := make([]interface{}, 0, len(value)) // 将 []string 转换为 []interface{}。
	for _, v := range value {
		m = append(m, v)
	}
	return setNestedFieldNoCopy(obj, m, fields...)
}

// SetNestedSlice 设置嵌套字段的切片值。
// 若某一层嵌套不是 map[string]interface{}，则返回错误。
func SetNestedSlice(obj map[string]interface{}, value []interface{}, fields ...string) error {
	return SetNestedField(obj, value, fields...)
}

// SetNestedStringMap 设置嵌套字段为 map[string]string 值。
// 若某一层嵌套不是 map[string]interface{}，则返回错误。
func SetNestedStringMap(obj map[string]interface{}, value map[string]string, fields ...string) error {
	m := make(map[string]interface{}, len(value)) // 将 map[string]string 转换为 map[string]interface{}。
	for k, v := range value {
		m[k] = v
	}
	return setNestedFieldNoCopy(obj, m, fields...)
}

// SetNestedMap 设置嵌套字段的 map[string]interface{} 值。
// 若某一层嵌套不是 map[string]interface{}，则返回错误。
func SetNestedMap(obj map[string]interface{}, value map[string]interface{}, fields ...string) error {
	return SetNestedField(obj, value, fields...)
}

// RemoveNestedField 从 obj 中删除嵌套字段。
// 路径中途遇到非 map 值时静默放弃（字段本就不存在），删除操作不返回错误。
func RemoveNestedField(obj map[string]interface{}, fields ...string) {
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if x, ok := m[field].(map[string]interface{}); ok {
			m = x
		} else {
			return
		}
	}
	delete(m, fields[len(fields)-1])
}

// getNestedString 是 NestedString 的宽松版本：缺失或类型错误时返回空字符串。
func getNestedString(obj map[string]interface{}, fields ...string) string {
	val, found, err := NestedString(obj, fields...)
	if !found || err != nil {
		return ""
	}
	return val
}

// getNestedInt64Pointer 是 NestedInt64 的宽松版本：缺失或类型错误时返回 nil。
// 需要区分“未设置”与“设为 0”的字段使用它。
func getNestedInt64Pointer(obj map[string]interface{}, fields ...string) *int64 {
	val, found, err := NestedInt64(obj, fields...)
	if !found || err != nil {
		return nil
	}
	return &val
}

// jsonPath 把字段列表拼成 ".a.b.c" 形式的路径，用于错误信息定位。
func jsonPath(fields []string) string {
	return "." + strings.Join(fields, ".")
}

// extractOwnerReference 从原始 map 中提取 OwnerReference。
// 注意：字段虽定义为 *bool，但从 JSON 解码后是 bool，因此需要手动转回指针。
func extractOwnerReference(v map[string]interface{}) metav1.OwnerReference {
	// 该字段虽定义为 *bool，但从 JSON 解码后为 bool。
	var controllerPtr *bool
	if controller, found, err := NestedBool(v, "controller"); err == nil && found {
		controllerPtr = &controller
	}
	var blockOwnerDeletionPtr *bool
	if blockOwnerDeletion, found, err := NestedBool(v, "blockOwnerDeletion"); err == nil && found {
		blockOwnerDeletionPtr = &blockOwnerDeletion
	}
	return metav1.OwnerReference{
		Kind:               getNestedString(v, "kind"),
		Name:               getNestedString(v, "name"),
		APIVersion:         getNestedString(v, "apiVersion"),
		UID:                types.UID(getNestedString(v, "uid")),
		Controller:         controllerPtr,
		BlockOwnerDeletion: blockOwnerDeletionPtr,
	}
}

// UnstructuredJSONScheme 能把 JSON 数据转换为 Unstructured 类型，
// 从而无需预定义 scheme 即可通用地访问对象。
var UnstructuredJSONScheme runtime.Codec = unstructuredJSONScheme{}

type unstructuredJSONScheme struct{}

// unstructuredJSONSchemeIdentifier 是编码器缓存标识，序列化结果只取决于该 scheme。
const unstructuredJSONSchemeIdentifier runtime.Identifier = "unstructuredJSON"

// Decode 解码数据；obj 非 nil 时解码到 obj 中，否则自行探测是列表还是单个对象。
// 解出的对象必须带有 Kind（列表还应有 apiVersion），否则返回 MissingKind 错误。
func (s unstructuredJSONScheme) Decode(data []byte, _ *schema.GroupVersionKind, obj runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	var err error
	if obj != nil {
		err = s.decodeInto(data, obj)
	} else {
		obj, err = s.decode(data)
	}

	if err != nil {
		return nil, nil, err
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	if len(gvk.Kind) == 0 {
		return nil, &gvk, runtime.NewMissingKindErr(string(data))
	}
	// TODO(109023): 此处也要求提供 apiVersion。

	return obj, &gvk, nil
}

// Encode 把对象编码为 JSON。CacheableObject 走缓存编码路径，避免重复序列化。
func (s unstructuredJSONScheme) Encode(obj runtime.Object, w io.Writer) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(s.Identifier(), s.doEncode, w)
	}
	return s.doEncode(obj, w)
}

// doEncode 按具体类型编码：Unstructured 直接编码其内容；
// 列表把 Items 展开到 "items" 键；Unknown 写出原始字节。
func (unstructuredJSONScheme) doEncode(obj runtime.Object, w io.Writer) error {
	switch t := obj.(type) {
	case *Unstructured:
		return json.NewEncoder(w).Encode(t.Object)
	case *UnstructuredList:
		items := make([]interface{}, 0, len(t.Items))
		for _, i := range t.Items {
			items = append(items, i.Object)
		}
		listObj := make(map[string]interface{}, len(t.Object)+1)
		for k, v := range t.Object { // 进行浅复制。
			listObj[k] = v
		}
		listObj["items"] = items
		return json.NewEncoder(w).Encode(listObj)
	case *runtime.Unknown:
		// TODO: Unstructured 需要处理 ContentType。
		_, err := w.Write(t.Raw)
		return err
	default:
		return json.NewEncoder(w).Encode(t)
	}
}

// Identifier 实现 runtime.Encoder 接口。
func (unstructuredJSONScheme) Identifier() runtime.Identifier {
	return unstructuredJSONSchemeIdentifier
}

// decode 先探测顶层是否含 "items" 字段，以区分列表与单对象。
func (s unstructuredJSONScheme) decode(data []byte) (runtime.Object, error) {
	type detector struct {
		Items gojson.RawMessage `json:"items"`
	}
	var det detector
	if err := json.Unmarshal(data, &det); err != nil {
		return nil, err
	}

	if det.Items != nil {
		list := &UnstructuredList{}
		err := s.decodeToList(data, list)
		return list, err
	}

	// 没有 items 字段，按单个对象处理。
	unstruct := &Unstructured{}
	err := s.decodeToUnstructured(data, unstruct)
	return unstruct, err
}

// decodeInto 把数据解码到调用方指定的具体类型中。
func (s unstructuredJSONScheme) decodeInto(data []byte, obj runtime.Object) error {
	switch x := obj.(type) {
	case *Unstructured:
		return s.decodeToUnstructured(data, x)
	case *UnstructuredList:
		return s.decodeToList(data, x)
	default:
		return json.Unmarshal(data, x)
	}
}

// decodeToUnstructured 把 JSON 解码为通用 map。
func (unstructuredJSONScheme) decodeToUnstructured(data []byte, unstruct *Unstructured) error {
	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	unstruct.Object = m

	return nil
}

// decodeToList 解码列表：先取出 items 的原始 JSON 逐个解码，
// 再为缺失 GVK 的条目补上从列表类型推断出的 Kind 与 apiVersion。
func (s unstructuredJSONScheme) decodeToList(data []byte, list *UnstructuredList) error {
	type decodeList struct {
		Items []gojson.RawMessage `json:"items"`
	}

	var dList decodeList
	if err := json.Unmarshal(data, &dList); err != nil {
		return err
	}

	if err := json.Unmarshal(data, &list.Object); err != nil {
		return err
	}

	// 类型化列表（如 PodList）中每个条目通常不带 APIVersion/Kind，需要在此补齐。
	listAPIVersion := list.GetAPIVersion()
	listKind := list.GetKind()
	itemKind := strings.TrimSuffix(listKind, "List")

	delete(list.Object, "items")
	list.Items = make([]Unstructured, 0, len(dList.Items))
	for _, i := range dList.Items {
		unstruct := &Unstructured{}
		if err := s.decodeToUnstructured([]byte(i), unstruct); err != nil {
			return err
		}
		// 仅当条目自身没有 GVK 时才按列表类型推断，避免覆盖条目上已有的信息。
		if len(unstruct.GetKind()) == 0 && len(unstruct.GetAPIVersion()) == 0 {
			unstruct.SetKind(itemKind)
			unstruct.SetAPIVersion(listAPIVersion)
		}
		list.Items = append(list.Items, *unstruct)
	}
	return nil
}

// jsonFallbackEncoder 包装一个 encoder：当目标类型未注册时，
// 对 Unstructured 回退到 JSON 编码，而不是直接报错。
type jsonFallbackEncoder struct {
	encoder    runtime.Encoder
	identifier runtime.Identifier
}

// NewJSONFallbackEncoder 构造带回退的编码器，其标识由回退名与底层编码器标识共同组成，
// 以保证缓存键的唯一性。
func NewJSONFallbackEncoder(encoder runtime.Encoder) runtime.Encoder {
	result := map[string]string{
		"name": "fallback",
		"base": string(encoder.Identifier()),
	}
	identifier, err := gojson.Marshal(result)
	if err != nil {
		//nolint:logcheck // Should not be reached.
		klog.Fatalf("Failed marshaling identifier for jsonFallbackEncoder: %v", err)
	}
	return &jsonFallbackEncoder{
		encoder:    encoder,
		identifier: runtime.Identifier(identifier),
	}
}

// Encode 先走底层编码器；只有“类型未注册”错误且对象为 Unstructured 时才回退。
func (c *jsonFallbackEncoder) Encode(obj runtime.Object, w io.Writer) error {
	// 无需处理 CacheableObject：这里只负责回退到其他编码器。
	err := c.encoder.Encode(obj, w)
	if runtime.IsNotRegisteredError(err) {
		switch obj.(type) {
		case *Unstructured, *UnstructuredList:
			return UnstructuredJSONScheme.Encode(obj, w)
		}
	}
	return err
}

// Identifier 实现 runtime.Encoder 接口。
func (c *jsonFallbackEncoder) Identifier() runtime.Identifier {
	return c.identifier
}
