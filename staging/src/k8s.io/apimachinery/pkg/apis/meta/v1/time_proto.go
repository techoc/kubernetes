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

package v1

import (
	"time"
)

// Timestamp 是与 Time 对应的 protobuf 中间结构，仅用于 protobuf 编解码。
// 生成后的序列化格式与 Time 保持一致；不要在普通 Go 业务结构体中直接使用。
type Timestamp struct {
	// 表示自 Unix epoch（1970-01-01T00:00:00Z）起累计的 UTC 秒数。
	// 有效范围为 0001-01-01T00:00:00Z 至 9999-12-31T23:59:59Z，包含边界。
	Seconds int64 `json:"seconds" protobuf:"varint,1,opt,name=seconds"`
	// 秒以下的非负纳秒部分。即使 Seconds 为负数，Nanos 仍必须非负，并表示向前推进的时间。
	// 有效范围为 0 至 999,999,999，包含边界；实际精度可能受使用场景限制。
	Nanos int32 `json:"nanos" protobuf:"varint,2,opt,name=nanos"`
}

// ProtoTime 将 Time 转换为新的 Timestamp。这里有意丢弃纳秒：Time 的 JSON 格式使用秒精度 RFC3339，
// 如果 protobuf 额外输出纳秒，protobuf 客户端看到的状态就会与 JSON 客户端不同，破坏往返一致性。
func (m *Time) ProtoTime() *Timestamp {
	if m == nil {
		return &Timestamp{}
	}
	return &Timestamp{
		Seconds: m.Time.Unix(),
		// 保留这段注释说明原因：JSON 只处理秒精度；如果 protobuf 客户端写入纳秒，JSON 客户端读取不到，
		// 就会出现意外的字段变更，并导致校验和相等性判断失败。
	}
}

// Size 实现 protobuf 编码接口，返回编码所需的字节数。
func (m *Time) Size() (n int) {
	if m == nil || m.Time.IsZero() {
		return 0
	}
	return m.ProtoTime().Size()
}

// Unmarshal 实现 protobuf 解码接口，将字节数据写入 Time。
func (m *Time) Unmarshal(data []byte) error {
	// 空 payload 表示零时间，不视为解码错误。
	if len(data) == 0 {
		m.Time = time.Time{}
		return nil
	}
	p := Timestamp{}
	if err := p.Unmarshal(data); err != nil {
		return err
	}
	// 出于与 ProtoTime 相同的兼容性原因，忽略 Nanos：JSON 只携带秒精度，
	// protobuf 输入中的纳秒不能改变 JSON 客户端无法观察到的状态。
	m.Time = time.Unix(p.Seconds, int64(0)).Local()
	return nil
}

// Marshal 实现 protobuf 编码接口。
func (m *Time) Marshal() (data []byte, err error) {
	if m == nil || m.Time.IsZero() {
		return nil, nil
	}
	return m.ProtoTime().Marshal()
}

// MarshalTo 将 protobuf 编码结果写入调用方提供的缓冲区。
func (m *Time) MarshalTo(data []byte) (int, error) {
	if m == nil || m.Time.IsZero() {
		return 0, nil
	}
	return m.ProtoTime().MarshalTo(data)
}

// MarshalToSizedBuffer 实现 protobuf 反向缓冲区编码接口。
func (m *Time) MarshalToSizedBuffer(data []byte) (int, error) {
	if m == nil || m.Time.IsZero() {
		return 0, nil
	}
	return m.ProtoTime().MarshalToSizedBuffer(data)
}
