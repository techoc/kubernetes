//go:build !fieldsv1string

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
	"bytes"
)

// FieldsV1 以类似 Trie 的 JSON 结构存储一组字段。
//
// 每个键要么是 '.'，表示字段自身且总映射到空集合；
// 要么是表示子字段或列表项的字符串，格式为以下四种之一：
// 'f:<name>'，<name> 为结构体字段名或 map 键
// 'v:<value>'，<value> 为列表项的确切 JSON 值
// 'i:<index>'，<index> 为列表项的位置
// 'k:<keys>'，<keys> 为列表项各关键字段到其唯一值的映射
// 若某键映射到空的 Fields 值，则该键所代表的字段属于该集合。
//
// 具体格式由 sigs.k8s.io/structured-merge-diff 定义。
// +k8s:deepcopy-gen=false
// +protobuf.options.marshal=false
// +protobuf.options.(gogoproto.goproto_stringer)=false
type FieldsV1 struct {
	// Raw 是此对象的底层序列化结果。
	//
	// Deprecated: 直接访问该字段已废弃。请改用 GetRawBytes、GetRawString、SetRawBytes、SetRawString、GetRawReader、NewFieldsV1。
	Raw []byte `json:"-" protobuf:"bytes,1,opt,name=Raw"`
}

// String 返回字段集合的原始 JSON 字符串。
func (f FieldsV1) String() string {
	return string(f.Raw)
}

// Equal 按原始字节逐字节比较两个字段集合是否相同。
func (f FieldsV1) Equal(f2 FieldsV1) bool {
	return bytes.Equal(f.Raw, f2.Raw)
}

// GetRawReader 返回读取原始字节的 Reader。
// 空值或 nil 接收者返回一个读到 EOF 的空 Reader，调用方无需判空。
func (f *FieldsV1) GetRawReader() FieldsV1Reader {
	if f == nil || len(f.Raw) == 0 {
		return bytes.NewReader(nil)
	}
	return bytes.NewReader(f.Raw)
}

// GetRawBytes 返回原始字节。
// 返回值可能是底层字节的副本，也可能直接引用底层字节。
// 若希望修改底层字节，可修改返回的字节后传给 SetRawBytes()；
// 若不希望修改，请自行复制返回的字节。
func (f *FieldsV1) GetRawBytes() []byte {
	if f == nil {
		return nil
	}
	return f.Raw
}

// GetRawString 以字符串形式返回原始数据。
func (f *FieldsV1) GetRawString() string {
	if f == nil {
		return ""
	}
	return string(f.Raw)
}

// SetRawBytes 设置原始字节。不会保留传入的字节切片（内部会克隆）。
func (f *FieldsV1) SetRawBytes(b []byte) {
	if f != nil {
		f.Raw = bytes.Clone(b)
	}
}

// SetRawString 用字符串设置原始数据。
func (f *FieldsV1) SetRawString(s string) {
	if f != nil {
		f.Raw = []byte(s)
	}
}

// NewFieldsV1 用给定的原始 JSON 字符串构造 FieldsV1。
func NewFieldsV1(raw string) *FieldsV1 {
	return &FieldsV1{Raw: []byte(raw)}
}

// DeepCopyInto 把接收者深拷贝到 out。Raw 为字节切片，必须复制而非共享底层数组。
func (f *FieldsV1) DeepCopyInto(out *FieldsV1) {
	*out = *f
	if f.Raw != nil {
		in, out := &f.Raw, &out.Raw
		*out = make([]byte, len(*in))
		copy(*out, *in)
	}
}
