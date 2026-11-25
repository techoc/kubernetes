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
	"time"
)

// Timestamp 的结构定义位于 time_proto.go，本文件只提供 MicroTime 的转换与 protobuf 方法。

// ProtoMicroTime 将 MicroTime 转换为新的 Timestamp，并截断到微秒精度。
// MicroTime 的 JSON 格式只保留微秒，因此 protobuf 转换也必须丢弃微秒以下的纳秒，
// 确保 JSON 与 protobuf 两种格式往返转换后得到相同结果。
func (m *MicroTime) ProtoMicroTime() *Timestamp {
	if m == nil {
		return &Timestamp{}
	}

	// 截断到微秒精度，与 JSON 编解码行为保持一致。
	truncatedNanoseconds := time.Duration(m.Time.Nanosecond()).Truncate(time.Microsecond)
	return &Timestamp{
		Seconds: m.Time.Unix(),
		Nanos:   int32(truncatedNanoseconds),
	}
}

// Size 实现 protobuf 编码接口，返回编码所需的字节数。
func (m *MicroTime) Size() (n int) {
	if m == nil || m.Time.IsZero() {
		return 0
	}
	return m.ProtoMicroTime().Size()
}

// Unmarshal 实现 protobuf 解码接口，将字节数据写入 MicroTime。
func (m *MicroTime) Unmarshal(data []byte) error {
	// 空 payload 表示零时间，不视为解码错误。
	if len(data) == 0 {
		m.Time = time.Time{}
		return nil
	}
	p := Timestamp{}
	if err := p.Unmarshal(data); err != nil {
		return err
	}

	// 截断到微秒精度，与 JSON 编解码行为保持一致。
	// 调用 Local() 与 Time.UnmarshalJSON 保持一致，确保两条解码路径得到相同的时间值。
	truncatedNanoseconds := time.Duration(p.Nanos).Truncate(time.Microsecond)
	m.Time = time.Unix(p.Seconds, int64(truncatedNanoseconds)).Local()
	return nil
}

// Marshal 实现 protobuf 编码接口。
func (m *MicroTime) Marshal() (data []byte, err error) {
	if m == nil || m.Time.IsZero() {
		return nil, nil
	}
	return m.ProtoMicroTime().Marshal()
}

// MarshalTo 将 protobuf 编码结果写入调用方提供的缓冲区。
func (m *MicroTime) MarshalTo(data []byte) (int, error) {
	if m == nil || m.Time.IsZero() {
		return 0, nil
	}
	return m.ProtoMicroTime().MarshalTo(data)
}

// MarshalToSizedBuffer 实现 protobuf 序列化接口。
func (m *MicroTime) MarshalToSizedBuffer(data []byte) (int, error) {
	if m == nil || m.Time.IsZero() {
		return 0, nil
	}
	return m.ProtoMicroTime().MarshalToSizedBuffer(data)
}
