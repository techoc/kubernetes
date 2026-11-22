//go:build fieldsv1string

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

package v1

import (
	"strings"
	"unique"
)

// FieldsV1 以类似 Trie 的 JSON 数据结构存储字段集合。
//
// 每个键要么是 '.'（表示字段本身，且总映射到空集合），要么是表示子字段或
// 列表项的字符串。该字符串遵循以下四种格式之一：
// 'f:<name>'，其中 <name> 是结构体字段名或 map 键
// 'v:<value>'，其中 <value> 是列表项精确的 JSON 格式值
// 'i:<index>'，其中 <index> 是列表项在列表中的位置
// 'k:<keys>'，其中 <keys> 是列表项键字段到其唯一值的映射
// 若某个键映射到空的 Fields 值，则该键所表示的字段属于该集合。
//
// 具体格式定义见 sigs.k8s.io/structured-merge-diff
// +k8s:deepcopy-gen=false
// +protobuf.options.marshal=false
// +protobuf.options.(gogoproto.goproto_stringer)=false
type FieldsV1 struct {
	// unique.Handle[string] 的零值内部指针未初始化，调用 .Value() 会 panic。
	// 必须在各访问方法中显式检查该未初始化状态（f.handle == unique.Handle[string]{}），
	// 以便安全支持未初始化的 metav1.FieldsV1{} 对象。
	// 相关 Go 讨论见：https://github.com/golang/go/issues/73344
	handle unique.Handle[string]
}

// String 返回 FieldsV1 的原始 JSON 字符串；零值返回空串。
func (f FieldsV1) String() string {
	if f.handle == (unique.Handle[string]{}) {
		return ""
	}
	return f.handle.Value()
}

// Equal 判断两个 FieldsV1 是否相等。
func (f FieldsV1) Equal(f2 FieldsV1) bool {
	if f.handle == f2.handle {
		return true
	}
	// 未初始化的 FieldsV1 与显式空 FieldsV1（unique.Make("")）会在上面的
	// handle 比较中判为不等。这里再比较字符串内容，以保持与旧实现
	// bytes.Equal(nil, []byte{}) == true 的行为一致。
	return f.GetRawString() == f2.GetRawString()
}

// GetRawReader 返回读取原始数据的 Reader；nil 或零值返回空 Reader。
func (f *FieldsV1) GetRawReader() FieldsV1Reader {
	if f == nil || f.handle == (unique.Handle[string]{}) {
		return strings.NewReader("")
	}
	return strings.NewReader(f.handle.Value())
}

// GetRawBytes 返回原始字节。
// 返回值可能是也可能不是底层字节的副本。
// 若需要修改底层字节，可修改返回值后传给 SetRawBytes()；
// 若不需要修改，请自行复制返回值。
func (f *FieldsV1) GetRawBytes() []byte {
	if f == nil || f.handle == (unique.Handle[string]{}) {
		return nil
	}
	return []byte(f.handle.Value())
}

// GetRawString 以字符串返回原始数据。
func (f *FieldsV1) GetRawString() string {
	if f == nil || f.handle == (unique.Handle[string]{}) {
		return ""
	}
	return f.handle.Value()
}

// SetRawBytes 设置原始字节；不会保留传入的字节切片（会复制到唯一字符串）。
func (f *FieldsV1) SetRawBytes(b []byte) {
	if f != nil {
		f.handle = unique.Make(string(b))
	}
}

// SetRawString 从字符串设置原始数据。
func (f *FieldsV1) SetRawString(s string) {
	if f != nil {
		f.handle = unique.Make(s)
	}
}

// NewFieldsV1 用给定的原始字符串构造 FieldsV1。
func NewFieldsV1(raw string) *FieldsV1 {
	return &FieldsV1{handle: unique.Make(raw)}
}

// DeepCopyInto 把 f 复制到 out。由于 handle 是不可变字符串句柄，浅拷贝即可。
func (f *FieldsV1) DeepCopyInto(out *FieldsV1) {
	*out = *f
}
