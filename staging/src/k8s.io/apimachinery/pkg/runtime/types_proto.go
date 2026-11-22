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

package runtime

import (
	"fmt"
	"io"
)

// ProtobufMarshaller 是向后兼容的 protobuf 编组接口：调用方可预先计算大小，
// 并从头开始填充给定缓冲区。
type ProtobufMarshaller interface {
	// Size 返回调用 MarshalTo 将写入的字节数。
	Size() int
	// MarshalTo 从 data 缓冲区起始处开始编组（缓冲区至少要有 Size() 字节），
	// 返回写入的字节数，必须与 Size() 的返回值一致。
	MarshalTo(data []byte) (int, error)
}

// ProtobufReverseMarshaller 可预先计算大小，并从给定缓冲区的末尾向前编组。
// 这种“反向”写法的优点是一次预分配即可，无需先算长度再写，避免多余分配。
type ProtobufReverseMarshaller interface {
	// Size 返回调用 MarshalToSizedBuffer 将写入的字节数。
	Size() int
	// MarshalToSizedBuffer 从 data 缓冲区末尾向前编组（缓冲区至少要有 Size() 字节），
	// 返回写入的字节数，必须与 Size() 的返回值一致。
	MarshalToSizedBuffer(data []byte) (int, error)
}

const (
	// 以下 tag 是 Unknown 结构体各字段在 protobuf wire 格式中的字段号与 wire type 组合。
	typeMetaTag        = 0xa
	rawTag             = 0x12
	contentEncodingTag = 0x1a
	contentTypeTag     = 0x22

	// uint64 的 varint 最长 10 字节
	maxUint64VarIntLength = 10
)

// MarshalToWriter 允许调用方提供流式 writer 来输出 raw 字节，而不是先把它们填充进 Unknown 结构体，
// 从而避免为可能很大的原始负载分配整块内存。
// rawSize 是成功情况下 rawWriter 将写入的字节数；writeRaw 在需要写入 raw 字节时被调用，
// 它必须返回 rawSize, nil 或一个错误。
func (m *Unknown) MarshalToWriter(w io.Writer, rawSize int, writeRaw func(io.Writer) (int, error)) (int, error) {
	size := 0

	// 复用一个缓冲区来编组 varint，避免每次写字段都分配。
	varintBuffer := make([]byte, maxUint64VarIntLength)
	writeVarint := func(i int) (int, error) {
		offset := encodeVarintGenerated(varintBuffer, len(varintBuffer), uint64(i))
		return w.Write(varintBuffer[offset:])
	}

	// TypeMeta 字段
	{
		n, err := w.Write([]byte{typeMetaTag})
		size += n
		if err != nil {
			return size, err
		}

		typeMetaBytes, err := m.TypeMeta.Marshal()
		if err != nil {
			return size, err
		}

		n, err = writeVarint(len(typeMetaBytes))
		size += n
		if err != nil {
			return size, err
		}

		n, err = w.Write(typeMetaBytes)
		size += n
		if err != nil {
			return size, err
		}
	}

	// Raw 字段，写入委托给 writeRaw()
	{
		n, err := w.Write([]byte{rawTag})
		size += n
		if err != nil {
			return size, err
		}

		n, err = writeVarint(rawSize)
		size += n
		if err != nil {
			return size, err
		}

		n, err = writeRaw(w)
		size += n
		if err != nil {
			return size, err
		}
		// 实际写入量与声明的 rawSize 必须一致，否则 wire 格式里的长度前缀就是错的。
		if n != int(rawSize) {
			return size, fmt.Errorf("the size value was %d, but encoding wrote %d bytes to data", rawSize, n)
		}
	}

	// ContentEncoding 字段
	{
		n, err := w.Write([]byte{contentEncodingTag})
		size += n
		if err != nil {
			return size, err
		}

		n, err = writeVarint(len(m.ContentEncoding))
		size += n
		if err != nil {
			return size, err
		}

		n, err = w.Write([]byte(m.ContentEncoding))
		size += n
		if err != nil {
			return size, err
		}
	}

	// ContentType 字段
	{
		n, err := w.Write([]byte{contentTypeTag})
		size += n
		if err != nil {
			return size, err
		}

		n, err = writeVarint(len(m.ContentType))
		size += n
		if err != nil {
			return size, err
		}

		n, err = w.Write([]byte(m.ContentType))
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

// NestedMarshalTo 允许调用方在序列化“嵌套了一个实现了 ProtobufMarshaller 或
// ProtobufReverseMarshaller 的对象”的 Unknown 时避免额外分配。
// 由于 protobuf 的长度前缀依赖总长度，这里先用 Size() 预计算，再从缓冲区末尾反向写入。
func (m *Unknown) NestedMarshalTo(data []byte, b ProtobufMarshaller, size uint64) (int, error) {
	// 计算整条消息的完整大小。
	msgSize := m.Size()
	if b != nil {
		msgSize += int(size) + sovGenerated(size) + 1
	}

	// 反向编组 m 的各字段。
	i := msgSize
	i -= len(m.ContentType)
	copy(data[i:], m.ContentType)
	i = encodeVarintGenerated(data, i, uint64(len(m.ContentType)))
	i--
	data[i] = contentTypeTag
	i -= len(m.ContentEncoding)
	copy(data[i:], m.ContentEncoding)
	i = encodeVarintGenerated(data, i, uint64(len(m.ContentEncoding)))
	i--
	data[i] = contentEncodingTag
	if b != nil {
		// 优先使用反向编组接口；它能把嵌套对象直接写到当前空位之前。
		if r, ok := b.(ProtobufReverseMarshaller); ok {
			n1, err := r.MarshalToSizedBuffer(data[:i])
			if err != nil {
				return 0, err
			}
			i -= int(size)
			if uint64(n1) != size {
				// 程序错误：protobuf 的 Size() 与 MarshalToSizedBuffer 结果不一致，会生成错误的消息。
				return 0, fmt.Errorf("the Size() value of %T was %d, but NestedMarshalTo wrote %d bytes to data", b, size, n1)
			}
		} else {
			i -= int(size)
			n1, err := b.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			if uint64(n1) != size {
				// 程序错误：protobuf 的 Size() 与 MarshalTo 结果不一致，会生成错误的消息。
				return 0, fmt.Errorf("the Size() value of %T was %d, but NestedMarshalTo wrote %d bytes to data", b, size, n1)
			}
		}
		i = encodeVarintGenerated(data, i, size)
		i--
		data[i] = rawTag
	}
	n2, err := m.TypeMeta.MarshalToSizedBuffer(data[:i])
	if err != nil {
		return 0, err
	}
	i -= n2
	i = encodeVarintGenerated(data, i, uint64(n2))
	i--
	data[i] = typeMetaTag
	return msgSize - i, nil
}
