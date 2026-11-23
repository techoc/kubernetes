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

import "encoding/json"

// MarshalJSON() preserves pre-1.34 JSON encoding of value as a string (not base64),
// stomping non-utf-8 data with the utf8 replacement character.
//
// MarshalJSON 为 KeyValue 实现自定义 JSON 序列化，以保持 Kubernetes 1.34 之前的行为：
//   - Value 字段按普通字符串编码，而不是 protobuf 默认的 base64 编码；
//   - 若 Value 中含有非法 UTF-8 字节，转换为字符串时会被替换为 UTF-8 替代字符（U+FFFD）。
//
// 这样做是为了兼容旧版本写入/读取的 JSON 数据格式，避免升级后字段名或编码方式发生变化。
func (k *KeyValue) MarshalJSON() ([]byte, error) {
	// 先构造一个与旧版本结构一致的临时对象：Key 为字符串，Value 由 []byte 强转为 string。
	// 强转过程中，非法 UTF-8 字节会被替换为替代字符。
	return json.Marshal(stringKeyValue{
		Key:   k.GetKey(),
		Value: string(k.GetValue()),
	})
}

// UnmarshalJSON preserves pre-1.34 JSON decoding of value as a string (not base64),
// stomping non-utf-8 data with the utf8 replacement character.
//
// UnmarshalJSON 为 KeyValue 实现自定义 JSON 反序列化，与上面的 MarshalJSON 成对出现：
//   - 把 JSON 中的 value 字符串按旧版本方式解析，而不是当作 base64 解码；
//   - 非法 UTF-8 字节在解析阶段被替换为替代字符。
//
// 处理流程：
//  1. 先反序列化到中间结构 stringKeyValue；
//  2. 解析失败则原样返回错误；
//  3. 解析成功后把字段逐一写回 KeyValue，其中 Value 再从 string 转回 []byte。
func (k *KeyValue) UnmarshalJSON(data []byte) error {
	v := stringKeyValue{}
	// 使用中间结构体接收 JSON 数据，解析出错则直接返回给调用方。
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	// 将中间结构体的值回填到 protobuf 生成的 KeyValue 对象中。
	k.Key = v.Key
	k.Value = []byte(v.Value)
	return nil
}

// stringKeyValue matches the structure used to json-encode pre-1.34.
// Non-UTF-8 characters in Value are coerced to the replacement character on encode/decode.
//
// stringKeyValue 是一个内部辅助结构，字段名和 json tag 都与 1.34 之前的 KeyValue 编码结构保持一致。
// 它的 Value 使用 string 而非 []byte，从而在编解码时触发 Go 标准库对非法 UTF-8 的替换行为。
type stringKeyValue struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}
