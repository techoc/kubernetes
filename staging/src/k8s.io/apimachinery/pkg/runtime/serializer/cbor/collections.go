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

package cbor

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"reflect"
	"slices"
	"sort"

	"k8s.io/apimachinery/pkg/conversion"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"

	"github.com/fxamacker/cbor/v2"
)

// streamEncodeCollections 尝试对 list 类对象走流式编码，返回 (是否已处理, error)。
//
// 只有标准形态的 List（3 个字段、特定 tag 布局）才适合流式编码；自定义
// Marshaler 或不符合布局的类型返回 false，交由通用编码器处理。
func streamEncodeCollections(obj runtime.Object, w io.Writer, mode modes.EncMode) (bool, error) {
	list, ok := obj.(*unstructured.UnstructuredList)
	if ok {
		return true, streamingEncodeUnstructuredList(w, list, mode)
	}
	// 自定义 CBOR/JSON Marshaler 的语义由类型自己决定，不能绕过。
	if _, ok := obj.(cbor.Marshaler); ok {
		return false, nil
	}
	if _, ok := obj.(json.Marshaler); ok {
		return false, nil
	}
	typeMeta, listMeta, items, err := getListMeta(obj)
	if err == nil {
		return true, streamingEncodeList(w, typeMeta, listMeta, items, mode)
	}
	return false, nil
}

// getListMeta 实现 CBOR 流式序列化时的列表提取逻辑。
//
// 不复用 meta 包 accessor 的原因：流式编码需要严格校验字段布局，且必须
// 区分 nil 与空 Items。
func getListMeta(list runtime.Object) (metav1.TypeMeta, metav1.ListMeta, []interface{}, error) {
	listValue, err := conversion.EnforcePtr(list)
	if err != nil {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, err
	}
	listType := listValue.Type()
	if listType.NumField() != 3 {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected ListType to have 3 fields")
	}
	// 流式编码器按 json struct tag 还原字段名与布局（kind、apiVersion、metadata、items）。
	// CBOR 编码器却优先采用 "cbor" tag：这些字段上的 cbor tag 可能重命名键、改变
	// 选项（如 keyasint）或取消 TypeMeta 的内联，从而与流式输出不一致。
	// 遇到这种类型时拒绝流式编码，退回通用编码器以正确处理 cbor tag。
	//
	// 流式编码按 json tag 的布局手写字节；一旦存在 cbor tag，通用编码器会
	// 以 cbor tag 为准，两者结果可能不同，因此拒绝流式并回退。
	for i := 0; i < listType.NumField(); i++ {
		if _, ok := listType.Field(i).Tag.Lookup("cbor"); ok {
			return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected list field %d to have no cbor tag", i)
		}
	}
	// TypeMeta
	typeMeta, ok := listValue.Field(0).Interface().(metav1.TypeMeta)
	if !ok {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf("expected TypeMeta field to have TypeMeta type")
	}
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
	if listType.Field(1).Tag.Get("json") != "metadata,omitempty" {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected ListMeta json field tag to be "metadata,omitempty"`)
	}
	// Items
	if listType.Field(2).Tag.Get("json") != "items" {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, fmt.Errorf(`expected Items json field tag to be "items"`)
	}
	items, err := getListItems(listValue.Field(2))
	if err != nil {
		return metav1.TypeMeta{}, metav1.ListMeta{}, nil, err
	}
	return typeMeta, listMeta, items, nil
}

// getListItems 以可寻址的值返回列表 Items 字段的元素。直接序列化每个元素
// （而不是经过 meta.ExtractList）能保留元素自身的类型和自定义 marshaler：
// 例如 Items 为装着 CBOR 字节的 runtime.RawExtension 时，会由
// runtime.RawExtension.MarshalCBOR 原样编码，而不会被压平成 runtime.Unknown
// （后者没有 MarshalCBOR，会被错误地当作结构体编码或导致 JSON 转码失败）。
//
// Items 为 nil 时返回 nil 结果，与空但非 nil 的切片区分开，
// 让编码器输出 CBOR null 而不是空数组。
//
// 返回元素地址而非值：保留元素自身的类型与自定义 marshaler。
func getListItems(itemsValue reflect.Value) ([]interface{}, error) {
	if itemsValue.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected Items field to be a slice, got %s", itemsValue.Kind())
	}
	if itemsValue.IsNil() {
		// 区分 nil 与空切片：前者编码为 CBOR null。
		return nil, nil
	}
	items := make([]interface{}, itemsValue.Len())
	for i := range items {
		items[i] = itemsValue.Index(i).Addr().Interface()
	}
	return items, nil
}

// cborMapEntry 表示一个待写出的 map 键及其延迟编码函数，
// 便于在非确定性模式下打乱写出顺序。
type cborMapEntry struct {
	key   string
	write func() error
}

// streamingEncodeList 手写 CBOR map 头并按 List 的字段布局逐项编码，
// 避免通用反射编码的中间分配。
func streamingEncodeList(w io.Writer, typeMeta metav1.TypeMeta, listMeta metav1.ListMeta, items []interface{}, mode modes.EncMode) error {
	var entries []cborMapEntry

	// 字段写出顺序与 json 布局一致：kind, items, metadata, apiVersion。
	// 空字符串的 kind/apiVersion 会被省略，与 omitempty 语义一致。
	if typeMeta.Kind != "" {
		entries = append(entries, cborMapEntry{
			key: "kind",
			write: func() error {
				return encodeKeyValuePair(w, "kind", typeMeta.Kind, mode)
			},
		})
	}
	entries = append(entries, cborMapEntry{
		key: "items",
		write: func() error {
			if err := mode.MarshalTo("items", w); err != nil {
				return err
			}
			if items == nil {
				// nil 与空数组语义不同，必须编码为 CBOR null。
				_, err := w.Write([]byte{0xf6}) // CBOR null
				return err
			}
			if err := writeArrayHead(w, len(items)); err != nil {
				return err
			}
			for _, item := range items {
				if err := mode.MarshalTo(item, w); err != nil {
					return err
				}
			}
			return nil
		},
	})
	entries = append(entries, cborMapEntry{
		key: "metadata",
		write: func() error {
			return encodeKeyValuePair(w, "metadata", listMeta, mode)
		},
	})
	if typeMeta.APIVersion != "" {
		entries = append(entries, cborMapEntry{
			key: "apiVersion",
			write: func() error {
				return encodeKeyValuePair(w, "apiVersion", typeMeta.APIVersion, mode)
			},
		})
	}

	// entries 按固定顺序构建（kind、items、metadata、apiVersion）。
	// streamingEncodeUnstructuredList 的键来自 Go 随机的 map 遍历顺序，这里
	// 本身没有随机性，因此非确定性模式（SortFastShuffle）下用随机起始偏移
	// 轮换编码循环。
	//
	// 非确定性模式需要模拟“随机顺序”，通过随机起点轮转实现。
	start := 0
	if !mode.IsDeterministic() && len(entries) > 0 {
		start = rand.Intn(len(entries))
	}

	if err := writeMapHead(w, len(entries)); err != nil {
		return err
	}

	for i := 0; i < len(entries); i++ {
		entry := entries[(start+i)%len(entries)]
		if err := entry.write(); err != nil {
			return err
		}
	}
	return nil
}

// streamingEncodeUnstructuredList 流式编码 UnstructuredList：先写非 items 的
// 原始键值，再把 Items 数组展开到 "items" 键。
func streamingEncodeUnstructuredList(w io.Writer, list *unstructured.UnstructuredList, mode modes.EncMode) error {
	keys := slices.Collect(maps.Keys(list.Object))
	if _, exists := list.Object["items"]; !exists {
		// 即使内容中没有 items，也要输出（可能为 nil 的）items 字段。
		keys = append(keys, "items")
	}
	// keys 初始顺序是 Go 随机的 map 遍历顺序。确定性模式（SortBytewiseLexical）
	// 会排序：先按长度，再按内容字典序。非确定性模式（SortFastShuffle）保持
	// 原顺序即可，它本身每次调用都不同（与 SortFastShuffle 处理结构体的方式
	// 类似），无需额外打乱。
	//
	// 确定性模式按“长度优先、再字典序”排序，与 CBOR 规范对 map 键的推荐顺序一致。
	if mode.IsDeterministic() {
		sort.Slice(keys, func(i, j int) bool {
			if len(keys[i]) != len(keys[j]) {
				return len(keys[i]) < len(keys[j])
			}
			return keys[i] < keys[j]
		})
	}

	if err := writeMapHead(w, len(keys)); err != nil {
		return err
	}

	for _, key := range keys {
		if err := mode.MarshalTo(key, w); err != nil {
			return err
		}
		if key == "items" {
			// Items 是结构化元素，需要逐个编码其 Object map。
			if err := writeArrayHead(w, len(list.Items)); err != nil {
				return err
			}
			for _, item := range list.Items {
				if err := mode.MarshalTo(item.Object, w); err != nil {
					return err
				}
			}
		} else {
			if err := mode.MarshalTo(list.Object[key], w); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeKeyValuePair 先写键再写值；两者必须使用同一编码模式。
func encodeKeyValuePair(w io.Writer, key string, value interface{}, mode modes.EncMode) error {
	if err := mode.MarshalTo(key, w); err != nil {
		return err
	}
	if err := mode.MarshalTo(value, w); err != nil {
		return err
	}
	return nil
}

// CBOR 主类型前缀字节（类型占高 3 位，附加信息清零），遵循 RFC 8949 第 3.1 节。
//
// 高 3 位是 major type：4=array、5=map；低 5 位留给长度/附加信息。
const (
	cborTypeArray byte = 0x80 // major type 4
	cborTypeMap   byte = 0xa0 // major type 5
)

// writeMapHead 为含 n 个表项的 map 写入 CBOR map 头。
func writeMapHead(w io.Writer, n int) error {
	return writeCollectionHead(w, cborTypeMap, int64(n))
}

// writeArrayHead 为含 n 个元素的数组写入 CBOR 数组头。
func writeArrayHead(w io.Writer, n int) error {
	return writeCollectionHead(w, cborTypeArray, int64(n))
}

// writeCollectionHead 写入 CBOR 集合（数组或 map）头，按 RFC 8949 第 3 节的
// 附加信息规则编码元素个数 n：
//
//   - base：集合类型的前缀字节。
//     map 用 cborTypeMap (0xa0)，数组用 cborTypeArray (0x80)。
//
// 扩展形式的前缀由 base 按位或得到：
//   - base|24：后跟 1 字节长度（附加信息 24）
//   - base|25：后跟 2 字节长度（附加信息 25）
//   - base|26：后跟 4 字节长度（附加信息 26）
//   - base|27：后跟 8 字节长度（附加信息 27）
//
// 编码表（以 map 为例，base=0xa0）：
//
//	n <= 23:         1 byte  — 0xa0|n
//	n <= 0xFF:        2 bytes — 0xb8 (0xa0|24), n
//	n <= 0xFFFF:      3 bytes — 0xb9 (0xa0|25), n>>8, n
//	n <= 0xFFFFFFFF:  5 bytes — 0xba (0xa0|26), n>>24..n
//	n > 0xFFFFFFFF:   9 bytes — 0xbb (0xa0|27), n>>56..n
//
// 按最短形式（canonical）选择长度编码，避免同一长度有多种表示。
func writeCollectionHead(w io.Writer, base byte, n int64) error {
	switch {
	case n <= 23:
		// 附加信息 0–23：长度直接编码在低 5 位。
		_, err := w.Write([]byte{base + byte(n)})
		return err
	case n <= 0xFF:
		// 附加信息 24：用 1 个附加字节表示长度。
		_, err := w.Write([]byte{base | 24, byte(n)})
		return err
	case n <= 0xFFFF:
		// 附加信息 25：用 2 个附加字节表示长度（大端）。
		_, err := w.Write([]byte{base | 25, byte(n >> 8), byte(n)})
		return err
	case n <= 0xFFFFFFFF:
		// 附加信息 26：用 4 个附加字节表示长度（大端）。
		_, err := w.Write([]byte{base | 26, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
		return err
	default:
		// 附加信息 27：用 8 个附加字节表示长度（大端）。
		_, err := w.Write([]byte{
			base | 27, byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16),
			byte(n >> 8), byte(n),
		})
		return err
	}
}
