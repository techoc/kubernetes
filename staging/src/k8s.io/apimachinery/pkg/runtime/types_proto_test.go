/*
Copyright 2025 The Kubernetes Authors.

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
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestVarint 将 MaxUint64 的编码长度固定为 varint 的最大长度；
// 以此守护生成的 marshaller 所使用的长度计算。
func TestVarint(t *testing.T) {
	varintBuffer := make([]byte, maxUint64VarIntLength)
	offset := encodeVarintGenerated(varintBuffer, len(varintBuffer), math.MaxUint64)
	used := len(varintBuffer) - offset
	if used != maxUint64VarIntLength {
		t.Fatalf("expected encodeVarintGenerated to use %d bytes to encode MaxUint64, got %d", maxUint64VarIntLength, used)
	}
}

// TestNestedMarshalToWriter 验证 Unknown 的三种编码方式——内联 Raw 的普通
// Marshal、NestedMarshalTo 以及流式 MarshalToWriter——产生逐字节相同的
// 输出，空载荷也不例外。
func TestNestedMarshalToWriter(t *testing.T) {
	testcases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "zero-length",
			raw:  []byte{},
		},
		{
			name: "simple",
			raw:  []byte{0x00, 0x01, 0x02, 0x03},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u := &Unknown{
				ContentType:     "ct",
				ContentEncoding: "ce",
				TypeMeta: TypeMeta{
					APIVersion: "v1",
					Kind:       "k",
				},
			}

			// 内联 Raw 进行普通 Marshal
			u.Raw = tc.raw
			marshalData, err := u.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			u.Raw = nil

			// 使用 NestedMarshalTo 进行 Marshal
			nestedMarshalData := make([]byte, len(marshalData))
			n, err := u.NestedMarshalTo(nestedMarshalData, copyMarshaler(tc.raw), uint64(len(tc.raw)))
			if err != nil {
				t.Fatal(err)
			}
			if n != len(marshalData) {
				t.Errorf("NestedMarshalTo returned %d, expected %d", n, len(marshalData))
			}
			if e, a := marshalData, nestedMarshalData; !bytes.Equal(e, a) {
				t.Errorf("NestedMarshalTo and Marshal differ:\n%s", cmp.Diff(e, a))
			}

			// 使用 MarshalToWriter 进行流式 Marshal
			buf := bytes.NewBuffer(nil)
			n, err = u.MarshalToWriter(buf, len(tc.raw), func(w io.Writer) (int, error) {
				return w.Write(tc.raw)
			})
			if err != nil {
				t.Fatal(err)
			}
			if n != len(marshalData) {
				t.Errorf("MarshalToWriter returned %d, expected %d", n, len(marshalData))
			}
			if e, a := marshalData, buf.Bytes(); !bytes.Equal(e, a) {
				t.Errorf("MarshalToWriter and Marshal differ:\n%s", cmp.Diff(e, a))
			}
		})
	}
}

// copyMarshaler 是一个简单的 Marshaler，用于在不涉及生成代码的情况下
// 将原始字节送入嵌套 marshal 路径。
type copyMarshaler []byte

func (c copyMarshaler) Size() int {
	return len(c)
}

func (c copyMarshaler) MarshalTo(dest []byte) (int, error) {
	n := copy(dest, []byte(c))
	return n, nil
}
