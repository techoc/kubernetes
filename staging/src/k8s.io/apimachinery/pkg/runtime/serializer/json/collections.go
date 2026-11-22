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

package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// streamEncodeCollections 尝试把列表（List）类型以流式方式直接写出，避免先把整个列表
// 组装成中间结构。返回值表示是否已处理；未处理的类型由调用方回退到普通编码路径。
func streamEncodeCollections(obj runtime.Object, w io.Writer) (bool, error) {
	// UnstructuredList 的字段全部在 map 中，走专门路径保留键顺序并处理 items。
	list, ok := obj.(*unstructured.UnstructuredList)
	if ok {
		return true, newStreamEncoder(w).encodeUnstructuredList(list)
	}
	// 自带 Marshaler 的对象有自己的 JSON 表示，不能按结构体字段拆解，交回调用方。
	if _, ok := obj.(json.Marshaler); ok {
		return false, nil
	}
	// 其他类型尝试按“TypeMeta + ListMeta + items”三字段的约定识别为列表。
	typeMeta, listMeta, items, err := getListMeta(obj)
	if err == nil {
		return true, newStreamEncoder(w).encodeList(typeMeta, listMeta, items)
	}
	return false, nil
}

// getListMeta 实现 json 流式序列化所需的列表字段提取逻辑。
//
// 之所以不直接用 meta 包的访问器，而是自定义逻辑：
//   - 需要校验 json tag，防止与标准 json 包的编码结果不兼容。
//   - ListMetaAccessor 无法区分空值与 nil 值，而流式输出需要保留这一差异。
//   - TypeAccessor 会重新解析 "apiVersion"，并以 "{group}/{version}" 形式重新序列化，
//     可能改变原始字符串。
func getListMeta(list runtime.Object) (metav1.TypeMeta, metav1.ListMeta, []runtime.Object, error) {
	listValue, err := conversion.EnforcePtr(list)
	if err != nil {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, err
	}
	// 列表类型必须严格是三字段结构（内嵌 TypeMeta、ListMeta、items），否则无法安全拆解。
	listType := listValue.Type()
	if listType.NumField() != 3 {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected ListType to have 3 fields")
	}
	// TypeMeta
	typeMeta, ok := listValue.Field(0).Interface().(metav1.TypeMeta)
	if !ok {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected TypeMeta field to have TypeMeta type")
	}
	// TypeMeta 必须内嵌，这样其字段才会被内联进列表的 JSON 对象。
	if !listType.Field(0).Anonymous {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected TypeMeta json field tag to be embedded`)
	}
	if jsonTag, jsonTagExists := listType.Field(0).Tag.Lookup("json"); !jsonTagExists {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected TypeMeta json field tag`)
	} else if jsonTag != "" && jsonTag != ",inline" {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected TypeMeta json field tag to be "" or ",inline"`)
	}
	// ListMeta
	listMeta, ok := listValue.Field(1).Interface().(metav1.ListMeta)
	if !ok {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected ListMeta field to have ListMeta type")
	}
	// 固定 tag 是流式编码按 "metadata" 键输出 ListMeta 的前提。
	if listType.Field(1).Tag.Get("json") != "metadata,omitempty" {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected ListMeta json field tag to be "metadata,omitempty"`)
	}
	// Items
	// ExtractList 通过 meta 访问器拿到元素切片，兼容各种具体列表实现。
	items, err := meta.ExtractList(list)
	if err != nil {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, err
	}
	if listType.Field(2).Tag.Get("json") != "items" {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected Items json field tag to be "items"`)
	}
	return typeMeta, listMeta, items, nil
}

// streamEncoder 把 JSON 值写到 w，并在多次编码之间复用内部缓冲区，
// 避免像 json.Marshal 那样每次调用都分配新的输出缓冲。
type streamEncoder struct {
	w    io.Writer
	buf  bytes.Buffer
	json *json.Encoder
}

// newStreamEncoder 创建绑定到 w 的流式编码器，json.Encoder 复用同一缓冲区。
func newStreamEncoder(w io.Writer) *streamEncoder {
	e := &streamEncoder{w: w}
	e.json = json.NewEncoder(&e.buf)
	return e
}

// encodeList 按 JSON 对象顺序输出 kind、apiVersion、metadata、items，
// 与标准 List 的 json.Marshal 输出保持字节级兼容。
func (e *streamEncoder) encodeList(typeMeta metav1.TypeMeta, listMeta metav1.ListMeta, items []runtime.Object) error {
	// 起始
	if _, err := e.w.Write([]byte(`{`)); err != nil {
		return err
	}

	// TypeMeta
	// 空值字段直接省略，与 omitempty 的行为一致。
	if typeMeta.Kind != "" {
		if err := e.encodeKeyValuePair("kind", typeMeta.Kind, []byte(",")); err != nil {
			return err
		}
	}
	if typeMeta.APIVersion != "" {
		if err := e.encodeKeyValuePair("apiVersion", typeMeta.APIVersion, []byte(",")); err != nil {
			return err
		}
	}

	// ListMeta
	if err := e.encodeKeyValuePair("metadata", listMeta, []byte(",")); err != nil {
		return err
	}

	// Items
	if err := e.encodeItemsObjectSlice(items); err != nil {
		return err
	}

	// 结束
	_, err := e.w.Write([]byte("}\n"))
	return err
}

// encodeItemsObjectSlice 输出 "items" 数组；items 为 nil 时输出 null，
// 以保留 nil 与空数组之间的语义差异。
func (e *streamEncoder) encodeItemsObjectSlice(items []runtime.Object) (err error) {
	if items == nil {
		err := e.encodeKeyValuePair("items", nil, nil)
		return err
	}
	_, err = e.w.Write([]byte(`"items":[`))
	if err != nil {
		return err
	}
	// 逐元素写出，除最后一个外都追加逗号，从而无需知道元素总数也能拼出合法数组。
	suffix := []byte(",")
	for i, item := range items {
		if i == len(items)-1 {
			suffix = nil
		}
		err := e.encodeValue(item, suffix)
		if err != nil {
			return err
		}
	}
	_, err = e.w.Write([]byte("]"))
	if err != nil {
		return err
	}
	return err
}

// encodeUnstructuredList 输出 unstructured 列表。其字段保存在 map 中，
// 这里按键名排序后输出以保证结果稳定，并确保 items 始终被写出。
func (e *streamEncoder) encodeUnstructuredList(list *unstructured.UnstructuredList) error {
	_, err := e.w.Write([]byte(`{`))
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(list.Object)+1)
	for key := range list.Object {
		keys = append(keys, key)
	}
	// UnstructuredList 的 items 可能不在 Object map 中，需要补一个键占位。
	if _, exists := list.Object["items"]; !exists {
		keys = append(keys, "items")
	}
	sort.Strings(keys)

	suffix := []byte(",")
	for i, key := range keys {
		if i == len(keys)-1 {
			suffix = nil
		}
		if key == "items" {
			err = e.encodeItemsUnstructuredSlice(list.Items, suffix)
		} else {
			err = e.encodeKeyValuePair(key, list.Object[key], suffix)
		}
		if err != nil {
			return err
		}
	}
	_, err = e.w.Write([]byte("}\n"))
	return err
}

// encodeItemsUnstructuredSlice 输出 unstructured 的 items 数组，并追加外层对象
// 所需的 suffix（逗号或空）。
func (e *streamEncoder) encodeItemsUnstructuredSlice(items []unstructured.Unstructured, suffix []byte) (err error) {
	_, err = e.w.Write([]byte(`"items":[`))
	if err != nil {
		return err
	}
	comma := []byte(",")
	for i, item := range items {
		if i == len(items)-1 {
			comma = nil
		}
		err := e.encodeValue(item.Object, comma)
		if err != nil {
			return err
		}
	}
	_, err = e.w.Write([]byte("]"))
	if err != nil {
		return err
	}
	// suffix 为 nil 时表示这是对象的最后一个键，不再补逗号。
	if len(suffix) > 0 {
		_, err = e.w.Write(suffix)
	}
	return err
}

// encodeKeyValuePair 输出一个 JSON 键值对，值后面追加 suffix。
func (e *streamEncoder) encodeKeyValuePair(key string, value any, suffix []byte) (err error) {
	err = e.encodeValue(key, []byte(":"))
	if err != nil {
		return err
	}
	err = e.encodeValue(value, suffix)
	if err != nil {
		return err
	}
	return err
}

// encodeValue 编码单个 JSON 值并追加 suffix。复用缓冲区以减少分配。
func (e *streamEncoder) encodeValue(value any, suffix []byte) error {
	e.buf.Reset()
	if err := e.json.Encode(value); err != nil {
		return err
	}
	// json.Encoder 在值后追加换行符；截掉它并替换为 suffix，
	// 使输出与 json.Marshal 的结果逐字节一致。
	e.buf.Truncate(e.buf.Len() - 1)
	e.buf.Write(suffix)
	_, err := e.w.Write(e.buf.Bytes())
	return err
}
