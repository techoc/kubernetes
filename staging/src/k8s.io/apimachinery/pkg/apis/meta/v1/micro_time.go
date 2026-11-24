/*
Copyright 2016 The Kubernetes Authors.

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

// RFC3339Micro 是 MicroTime 的线格式：RFC3339 加 6 位小数秒。
// 固定 6 位小数保证编码结果稳定、可逐字节比较。
const RFC3339Micro = "2006-01-02T15:04:05.000000Z07:00"

// MicroTime 是 Time 的微秒精度版本，适用于需要比秒更细、但不需要纳秒精度的 API 时间字段。
//
// +protobuf.options.marshal=false
// +protobuf.as=Timestamp
// +protobuf.options.(gogoproto.goproto_stringer)=false
//
// MicroTime 以 time.Time 内嵌，但序列化时只保留微秒精度；protobuf 指令让
// 生成代码把它当作 Timestamp 处理，而不是普通结构体。
type MicroTime struct {
	time.Time `protobuf:"-"`
}

// DeepCopyInto 将 MicroTime 复制到 out。time.Time 在 time API 中实际上不可变，
// 因此即使内部包含未导出的指针字段，直接按值赋值也是安全的。
//
// time.Time 在 API 语义上不可变，按值复制即可，无需深拷贝其内部字段。
func (t *MicroTime) DeepCopyInto(out *MicroTime) {
	*out = *t
}

// NewMicroTime 将给定 time.Time 包装为 MicroTime。
func NewMicroTime(time time.Time) MicroTime {
	return MicroTime{time}
}

// DateMicro 包装 time.Date，根据给定年月日、时分秒、纳秒和时区构造 MicroTime。
func DateMicro(year int, month time.Month, day, hour, min, sec, nsec int, loc *time.Location) MicroTime {
	return MicroTime{time.Date(year, month, day, hour, min, sec, nsec, loc)}
}

// NowMicro 返回当前本地时间，并包装为 MicroTime。
func NowMicro() MicroTime {
	return MicroTime{time.Now()}
}

// IsZero returns true if the value is nil or time is zero.
//
// nil 接收者视为零值，便于在指针字段未设置时直接调用。
func (t *MicroTime) IsZero() bool {
	if t == nil {
		return true
	}
	return t.Time.IsZero()
}

// Before 判断时间点 t 是否早于 u。
//
// 任一侧为 nil 都返回 false：nil 代表“未设置”，不参与时间比较。
func (t *MicroTime) Before(u *MicroTime) bool {
	if t != nil && u != nil {
		return t.Time.Before(u.Time)
	}
	return false
}

// Equal 判断时间点 t 与 u 是否相等。
//
// 两个 nil 视为相等；一 nil 一非 nil 视为不等。
func (t *MicroTime) Equal(u *MicroTime) bool {
	if t == nil && u == nil {
		return true
	}
	if t != nil && u != nil {
		return t.Time.Equal(u.Time)
	}
	return false
}

// BeforeTime 判断微秒精度的 t 是否早于秒精度的 u。
func (t *MicroTime) BeforeTime(u *Time) bool {
	if t != nil && u != nil {
		return t.Time.Before(u.Time)
	}
	return false
}

// EqualTime 判断微秒精度的 t 是否与秒精度的 u 表示同一时刻。
func (t *MicroTime) EqualTime(u *Time) bool {
	if t == nil && u == nil {
		return true
	}
	if t != nil && u != nil {
		return t.Time.Equal(u.Time)
	}
	return false
}

// UnixMicro 包装 time.Unix，根据 Unix 秒数和纳秒偏移构造 MicroTime。
func UnixMicro(sec int64, nsec int64) MicroTime {
	return MicroTime{time.Unix(sec, nsec)}
}

// UnmarshalJSON implements the json.Unmarshaller interface.
//
// JSON null 被规范化为零值 time.Time，而不是保留 nil 指针语义。
func (t *MicroTime) UnmarshalJSON(b []byte) error {
	if len(b) == 4 && string(b) == "null" {
		t.Time = time.Time{}
		return nil
	}

	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return err
	}

	pt, err := time.Parse(RFC3339Micro, str)
	if err != nil {
		return err
	}

	// 统一转成本地时区，保证同一时刻在不同时区解析后 DeepEqual 一致。
	t.Time = pt.Local()
	return nil
}

// UnmarshalCBOR 反序列化 CBOR 编码的 MicroTime；null 同样映射为零值。
// 该方法由 fxamacker/cbor 按接口约定自动调用。
func (t *MicroTime) UnmarshalCBOR(b []byte) error {
	var s *string
	if err := cbor.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == nil {
		t.Time = time.Time{}
		return nil
	}

	parsed, err := time.Parse(RFC3339Micro, *s)
	if err != nil {
		return err
	}

	t.Time = parsed.Local()
	return nil
}

// UnmarshalQueryParameter 将 URL 查询参数字符串解析为 MicroTime。
//
// 空字符串与字面量 "null" 都视为未设置，兼容旧客户端用 JSON 拼 query 的行为。
func (t *MicroTime) UnmarshalQueryParameter(str string) error {
	if len(str) == 0 {
		t.Time = time.Time{}
		return nil
	}
	// 兼容旧客户端：它们曾使用 JSON 序列化结果构造 URL 查询参数。
	if len(str) == 4 && str == "null" {
		t.Time = time.Time{}
		return nil
	}

	pt, err := time.Parse(RFC3339Micro, str)
	if err != nil {
		return err
	}

	t.Time = pt.Local()
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
//
// 零值输出 JSON null；非零值统一转为 UTC 后按固定微秒格式输出。
func (t MicroTime) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		// 未设置或零值时间统一编码为 JSON null。
		return []byte("null"), nil
	}

	return json.Marshal(t.UTC().Format(RFC3339Micro))
}

// MarshalCBOR 输出与 MarshalJSON 对应的 CBOR 表示：零值为 null，否则为字符串。
func (t MicroTime) MarshalCBOR() ([]byte, error) {
	if t.IsZero() {
		return cbor.Marshal(nil)
	}
	return cbor.Marshal(t.UTC().Format(RFC3339Micro))
}

// OpenAPISchemaType 供 kube-openapi 生成器构建该类型的 OpenAPI Schema 时调用。
//
// See: https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ MicroTime) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat 供 kube-openapi 生成器构建该类型的 OpenAPI Schema 时调用。
func (_ MicroTime) OpenAPISchemaFormat() string { return "date-time" }

// MarshalQueryParameter converts to a URL query parameter value
//
// 零值编码为空串（而不是 "null"），与 UnmarshalQueryParameter 的空串约定对称。
func (t MicroTime) MarshalQueryParameter() (string, error) {
	if t.IsZero() {
		// 未设置或零值时间编码为空字符串，与查询参数解码规则保持一致。
		return "", nil
	}

	return t.UTC().Format(RFC3339Micro), nil
}
