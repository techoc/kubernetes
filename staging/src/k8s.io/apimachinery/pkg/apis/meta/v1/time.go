/*
Copyright 2014 The Kubernetes Authors.

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
	"encoding/json"
	"time"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
)

// Time 是对 time.Time 的包装，支持正确的 YAML 与 JSON 序列化。
// 并提供了 time 包中众多工厂方法的包装版本。
//
// +protobuf.options.marshal=false
// +protobuf.as=Timestamp
// +protobuf.options.(gogoproto.goproto_stringer)=false
type Time struct {
	time.Time `protobuf:"-"`
}

// DeepCopyInto 深拷贝 Time 值。底层 time.Time 在 time API 中实际不可变，
// 因此尽管包含未导出的指针字段，按值赋值也是安全的。
func (t *Time) DeepCopyInto(out *Time) {
	*out = *t
}

// NewTime 返回包装了给定 time 的实例
func NewTime(time time.Time) Time {
	return Time{time}
}

// Date 包装 time.Date，返回由给定参数确定的 Time。
func Date(year int, month time.Month, day, hour, min, sec, nsec int, loc *time.Location) Time {
	return Time{time.Date(year, month, day, hour, min, sec, nsec, loc)}
}

// Now 返回当前本地时间。
func Now() Time {
	return Time{time.Now()}
}

// IsZero 在值为 nil 或时间为零值时返回 true。
func (t *Time) IsZero() bool {
	if t == nil {
		return true
	}
	return t.Time.IsZero()
}

// Before 报告时间点 t 是否早于 u；任一为 nil 时返回 false。
func (t *Time) Before(u *Time) bool {
	if t != nil && u != nil {
		return t.Time.Before(u.Time)
	}
	return false
}

// Equal 报告时间点 t 与 u 是否相等；两者均为 nil 视为相等。
func (t *Time) Equal(u *Time) bool {
	if t == nil && u == nil {
		return true
	}
	if t != nil && u != nil {
		return t.Time.Equal(u.Time)
	}
	return false
}

// Unix 包装 time.Unix，返回给定 Unix 时间对应的本地时间。
func Unix(sec int64, nsec int64) Time {
	return Time{time.Unix(sec, nsec)}
}

// Rfc3339Copy 返回截断到秒级精度的 Time 副本。
func (t Time) Rfc3339Copy() Time {
	copied, _ := time.Parse(time.RFC3339, t.Format(time.RFC3339))
	return Time{copied}
}

// UnmarshalJSON 实现 json.Unmarshaller 接口。
// null 会清零时间；否则按 RFC3339 解析并转换为本地时区。
func (t *Time) UnmarshalJSON(b []byte) error {
	if len(b) == 4 && string(b) == "null" {
		t.Time = time.Time{}
		return nil
	}

	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return err
	}

	pt, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return err
	}

	t.Time = pt.Local()
	return nil
}

// UnmarshalCBOR 从 CBOR 字符串解码时间；null 清零，否则按 RFC3339 解析并转本地时区。
func (t *Time) UnmarshalCBOR(b []byte) error {
	var s *string
	if err := cbor.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == nil {
		t.Time = time.Time{}
		return nil
	}

	parsed, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		return err
	}

	t.Time = parsed.Local()
	return nil
}

// UnmarshalQueryParameter 把 URL 查询参数值转换为对象。
// 空串视为未设置；同时容忍老客户端以 JSON 序列化产生的 "null"。
func (t *Time) UnmarshalQueryParameter(str string) error {
	if len(str) == 0 {
		t.Time = time.Time{}
		return nil
	}
	// 兼容老客户端：它们曾用 JSON 序列化构造查询参数
	if len(str) == 4 && str == "null" {
		t.Time = time.Time{}
		return nil
	}

	pt, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return err
	}

	t.Time = pt.Local()
	return nil
}

// MarshalJSON 实现 json.Marshaler 接口。
// 零值输出 JSON 的 "null"，其余输出 UTC 的 RFC3339 字符串。
func (t Time) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		// 未设置/空对象编码为 JSON 的 "null"。
		return []byte("null"), nil
	}
	buf := make([]byte, 0, len(time.RFC3339)+2)
	buf = append(buf, '"')
	// time 不会包含无法转义的 JSON 字符
	buf = t.UTC().AppendFormat(buf, time.RFC3339)
	buf = append(buf, '"')
	return buf, nil
}

// MarshalCBOR 把时间编码为 CBOR 字符串；零值编码为 null。
func (t Time) MarshalCBOR() ([]byte, error) {
	if t.IsZero() {
		return cbor.Marshal(nil)
	}

	return cbor.Marshal(t.UTC().Format(time.RFC3339))
}

// ToUnstructured 实现 value.UnstructuredConverter 接口：
// 零值返回 nil，其余返回 UTC 的 RFC3339 字符串。
func (t Time) ToUnstructured() interface{} {
	if t.IsZero() {
		return nil
	}
	buf := make([]byte, 0, len(time.RFC3339))
	buf = t.UTC().AppendFormat(buf, time.RFC3339)
	return string(buf)
}

// OpenAPISchemaType 供 kube-openapi 生成器在构造该类型的 OpenAPI spec 时使用。
//
// See: https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ Time) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat 供 kube-openapi 生成器在构造该类型的 OpenAPI spec 时使用。
func (_ Time) OpenAPISchemaFormat() string { return "date-time" }

// MarshalQueryParameter 转换为 URL 查询参数值；
// 零值编码为空字符串，其余为 UTC 的 RFC3339 字符串。
func (t Time) MarshalQueryParameter() (string, error) {
	if t.IsZero() {
		// 未设置/空对象编码为空字符串
		return "", nil
	}

	return t.UTC().Format(time.RFC3339), nil
}
