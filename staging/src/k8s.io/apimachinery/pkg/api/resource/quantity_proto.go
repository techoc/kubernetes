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

// 本文件是 protobuf 生成代码的手工变体：Quantity 只以单个字符串字段
// 上线，因此必须把生成代码改成调用 String()/ParseQuantity()，而不是
// 读写普通结构体字段。保持手写（而非重新生成）可以让规范字符串形式
// 继续作为线格式，从而保证 protobuf 兼容性与输出稳定。

package resource

import (
	"fmt"
	"io"
	"math/bits"
)

// Marshal 分配一个恰好 Size() 字节的缓冲区并填充，用于满足 protobuf
// Marshaler 接口；与生成代码一样返回写入的字节数。
func (m *Quantity) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(data[:size])
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// MarshalTo 写入调用方提供的缓冲区。先按 Size() 截断，是因为调用方应
// 已按 Size() 准备容量；这与生成代码“从切片末尾写入”的语义一致。
func (m *Quantity) MarshalTo(data []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(data[:size])
}

// MarshalToSizedBuffer 是单字符串字段结构体的定制 protobuf 编码实现。
func (m *Quantity) MarshalToSizedBuffer(data []byte) (int, error) {
	i := len(data)
	_ = i
	var l int
	_ = l

	// 唯一的 protobuf 字段是字段 1（string），wire type 为 2
	// （长度分隔）。从缓冲区末尾向前写是 gogo/protobuf 的约定，
	// 这样调用方可通过返回的起点偏移得知数据位置。
	// 序列化规范字符串形式，而不是内部的 (amount, scale) 表示；
	// ParseQuantity 是唯一支持的解码入口。
	out := m.String()
	i -= len(out)
	copy(data[i:], out)
	i = encodeVarintGenerated(data, i, uint64(len(out)))
	// 字段 1、wire type 2 的 tag：(1 << 3) | 2 == 0xa。
	i--
	data[i] = 0xa

	return len(data) - i, nil
}

// encodeVarintGenerated 把 v 编码为以 offset 结尾的 base-128 varint，
// 并返回编码起始下标。反向写入与 MarshalToSizedBuffer 从尾部填充的
// 方向一致。
func encodeVarintGenerated(data []byte, offset int, v uint64) int {
	offset -= sovGenerated(v)
	base := offset
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return base
}

// Size 返回 Quantity 的编码长度：1 字节 tag、varint 编码的字符串长度
// 以及字符串本身。它必须与 MarshalToSizedBuffer 保持一致，否则
// Marshal 会截断输出。
func (m *Quantity) Size() (n int) {
	var l int
	_ = l

	// CUSTOM SIZE：线上传输的是规范字符串，因此按字符串长度计算，
	// 而不是按结构体字段。
	l = len(m.String())
	// END CUSTOM SIZE

	n += 1 + l + sovGenerated(uint64(l))
	return n
}

// sovGenerated 返回 varint 编码 x 所需的字节数。`x|1` 用来省掉零值的
// 特判：零值仍然需要 1 字节。
func sovGenerated(x uint64) (n int) {
	return (bits.Len64(x|1) + 6) / 7
}

// Unmarshal 是单字符串字段结构体的定制 protobuf 解码实现。
func (m *Quantity) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		// 读取字段键：一个 (字段号 << 3) | wire type 的 varint。
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		// wire type 4（end-group）只能作为 group 的结束标记出现，
		// 而本消息从不包含 group。
		if wireType == 4 {
			return fmt.Errorf("proto: Quantity: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Quantity: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			// 字段 1 是规范字符串形式，wire type 为 2。
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field String_", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			// 拒绝会溢出 int 的长度，避免恶意构造的大值导致切片越界 panic。
			if intStringLen < 0 {
				return ErrInvalidLengthGenerated
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(data[iNdEx:postIndex])

			// CUSTOM DECODE：通过 ParseQuantity 重建，使线格式与内部
			// 表示保持解耦。
			p, err := ParseQuantity(s)
			if err != nil {
				return err
			}
			*m = p
			// END CUSTOM DECODE

			iNdEx = postIndex
		default:
			// 未知字段直接跳过，以保证向前兼容。
			iNdEx = preIndex
			skippy, err := skipGenerated(data[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthGenerated
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// skipGenerated 从 data[0] 开始跳过一个 protobuf 字段，返回其占用字节数
// （含嵌套 group）。它使未知字段无需解码即可被忽略。
func skipGenerated(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowGenerated
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		// 不同 wire type 的字段长度要么固定，要么带长度前缀。
		switch wireType {
		case 0:
			// Varint：一直读取，直到某个字节的续位为 0。
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if data[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			// 64 位定长。
			iNdEx += 8
			return iNdEx, nil
		case 2:
			// 长度分隔：先读长度，再跳过相应字节。
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowGenerated
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthGenerated
			}
			return iNdEx, nil
		case 3:
			// Group：递归跳过，直到在当前嵌套层遇到匹配的 end-group
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowGenerated
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipGenerated(data[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			// End-group：调用方不再消费任何字节。
			return iNdEx, nil
		case 5:
			// 32 位定长。
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	// 进入循环后没有字段可处理，说明输入为空但未满足上面的返回条件；
	// 走到这里表示内部不变量被破坏。
	panic("unreachable")
}

var (
	// ErrInvalidLengthGenerated 表示字段长度为负或溢出。
	ErrInvalidLengthGenerated = fmt.Errorf("proto: negative length found during unmarshaling")
	// ErrIntOverflowGenerated 表示 varint 超出 64 位。
	ErrIntOverflowGenerated = fmt.Errorf("proto: integer overflow")
)
