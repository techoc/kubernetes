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

package v1_test

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"unsafe"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 本文件仅在构建标签 fieldsv1string 下编译，用于测试 FieldsV1 的字符串驻留（interning）实现。
// 驻留可以显著减少大量相同 FieldsV1 负载带来的内存占用，因此这里直接用 unsafe
// 校验两个字符串是否指向同一块内存，而不是仅仅比较内容。

// requireInterned 在字符串不指向同一内存地址时让测试失败。
func requireInterned(t *testing.T, a, b string) {
	t.Helper()
	ptrA := unsafe.StringData(a)
	ptrB := unsafe.StringData(b)
	if ptrA != ptrB {
		t.Fatalf("Expected strings to be interned (same memory address) but pointers differ: %p != %p", ptrA, ptrB)
	}
}

// TestFieldsV1_String 验证 String() 对零值、已初始化的空句柄与有效负载的输出。
func TestFieldsV1_String(t *testing.T) {
	for _, tc := range []struct {
		name     string
		f        metav1.FieldsV1
		expected string
	}{
		{
			name:     "zero value handle",
			f:        metav1.FieldsV1{},
			expected: "",
		},
		{
			name:     "initialized empty handle",
			f:        *metav1.NewFieldsV1(""),
			expected: "",
		},
		{
			name:     "valid payload",
			f:        *metav1.NewFieldsV1(`{"f:app":{}}`),
			expected: `{"f:app":{}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.f.String(); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

// TestFieldsV1_Equal 验证 Equal 的语义：零值与空句柄相等，内容相同即相等，并检查其可交换性。
func TestFieldsV1_Equal(t *testing.T) {
	valid := *metav1.NewFieldsV1(`{"f:app":{}}`)
	validClone := *metav1.NewFieldsV1(`{"f:app":{}}`)
	different := *metav1.NewFieldsV1(`{"f:other":{}}`)

	for _, tc := range []struct {
		name     string
		a        metav1.FieldsV1
		b        metav1.FieldsV1
		expected bool
	}{
		{"both zero value", metav1.FieldsV1{}, metav1.FieldsV1{}, true},
		{"zero value and initialized empty handle", metav1.FieldsV1{}, *metav1.NewFieldsV1(""), true},
		{"identical valid payloads", valid, validClone, true},
		{"different payloads", valid, different, false},
		{"zero value and valid payload", metav1.FieldsV1{}, valid, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.expected {
				t.Errorf("expected Equal() to be %v, got %v", tc.expected, got)
			}
			// Equal 必须满足可交换性：a.Equal(b) 与 b.Equal(a) 结果一致。
			if got := tc.b.Equal(tc.a); got != tc.expected {
				t.Errorf("expected Equal() commutative to be %v, got %v", tc.expected, got)
			}
		})
	}
}

// TestFieldsV1_GetRawReader 验证 GetRawReader 在 nil 接收者、零值与有效负载下都返回可读内容。
func TestFieldsV1_GetRawReader(t *testing.T) {
	for _, tc := range []struct {
		name     string
		f        *metav1.FieldsV1
		expected []byte
	}{
		{"nil receiver", nil, []byte("")},
		{"zero value handle", &metav1.FieldsV1{}, []byte("")},
		{"initialized empty handle", metav1.NewFieldsV1(""), []byte("")},
		{"valid payload", metav1.NewFieldsV1(`{"f:app":{}}`), []byte(`{"f:app":{}}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.f.GetRawReader()
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("unexpected error reading from RawReader: %v", err)
			}
			if !bytes.Equal(got, tc.expected) {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

// TestFieldsV1_GetRawBytes 验证 GetRawBytes 的返回值：
// nil 接收者与零值必须返回严格 nil（而非空切片），且返回值不得与内部存储共享内存。
func TestFieldsV1_GetRawBytes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		f        *metav1.FieldsV1
		expected []byte
	}{
		{"nil receiver", nil, nil},
		{"zero value handle", &metav1.FieldsV1{}, nil},
		{"initialized empty handle", metav1.NewFieldsV1(""), []byte{}},
		{"valid payload", metav1.NewFieldsV1(`{"f:app":{}}`), []byte(`{"f:app":{}}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.f.GetRawBytes()
			if !bytes.Equal(got, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
			// bytes.Equal 认为 nil 与空切片相等，因此需要单独断言"严格 nil"这一契约。
			if tc.expected == nil && got != nil {
				t.Errorf("expected strict nil, got %v", got)
			}
		})
	}

	// 修改返回的字节切片不得影响内部驻留字符串，否则调用方会意外破坏共享数据。
	t.Run("mutation safety", func(t *testing.T) {
		f := metav1.NewFieldsV1(`{"f:app":{}}`)
		b := f.GetRawBytes()

		// 修改返回的字节切片。
		b[2] = 'X'

		// 原始驻留字符串必须保持不变。
		if got := f.GetRawString(); got != `{"f:app":{}}` {
			t.Errorf("GetRawBytes returned an unisolated slice! Mutating it corrupted the handle. Got: %s", got)
		}
	})
}

// TestFieldsV1_GetRawString 验证 GetRawString 在各种状态下都返回空串或原始负载。
func TestFieldsV1_GetRawString(t *testing.T) {
	for _, tc := range []struct {
		name     string
		f        *metav1.FieldsV1
		expected string
	}{
		{"nil receiver", nil, ""},
		{"zero value handle", &metav1.FieldsV1{}, ""},
		{"initialized empty handle", metav1.NewFieldsV1(""), ""},
		{"valid payload", metav1.NewFieldsV1(`{"f:app":{}}`), `{"f:app":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.f.GetRawString(); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

// TestFieldsV1_SetRawBytes 验证 SetRawBytes 的边界行为：nil 接收者不 panic、
// nil/空切片都归一化为空串，且内容相同的不同切片最终驻留到同一地址。
func TestFieldsV1_SetRawBytes(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var f *metav1.FieldsV1        // nil
		f.SetRawBytes([]byte("test")) // Should not panic
	})

	t.Run("empty slice input", func(t *testing.T) {
		f := &metav1.FieldsV1{}
		f.SetRawBytes([]byte{})
		if f.GetRawString() != "" {
			t.Errorf("Expected empty string, got %q", f.GetRawString())
		}
	})

	t.Run("nil slice input", func(t *testing.T) {
		f := &metav1.FieldsV1{}
		f.SetRawBytes(nil)
		if f.GetRawString() != "" {
			t.Errorf("Expected empty string, got %q", f.GetRawString())
		}
	})

	t.Run("interning behavior", func(t *testing.T) {
		payload := []byte(`{"f:app":{}}`)

		// 构造两个内容相同但底层数组不同的切片，确保驻留不是靠共享底层数组实现的。
		b1 := append([]byte(nil), payload...)
		b2 := append([]byte(nil), payload...)

		f1 := &metav1.FieldsV1{}
		f1.SetRawBytes(b1)

		f2 := &metav1.FieldsV1{}
		f2.SetRawBytes(b2)

		if f1.GetRawString() != string(payload) {
			t.Errorf("Expected GetRawString to match payload, got %q", f1.GetRawString())
		}

		requireInterned(t, f1.GetRawString(), f2.GetRawString())
	})
}

// TestFieldsV1_SetRawString 验证 SetRawString 对 nil 接收者的容错，以及相同内容的驻留行为。
func TestFieldsV1_SetRawString(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var f *metav1.FieldsV1 // nil
		f.SetRawString("test") // Should not panic
	})

	t.Run("interning behavior", func(t *testing.T) {
		payload := `{"f:app":{}}`

		// 通过 append 强制产生新的字符串副本，避免编译器把相同字面量合并为同一静态字符串，
		// 否则测试无法证明是 SetRawString 在驻留。
		s1 := string(append([]byte(nil), payload...))
		s2 := string(append([]byte(nil), payload...))

		f1 := &metav1.FieldsV1{}
		f1.SetRawString(s1)

		f2 := &metav1.FieldsV1{}
		f2.SetRawString(s2)

		requireInterned(t, f1.GetRawString(), f2.GetRawString())
	})
}

// TestFieldsV1_NewFieldsV1 验证构造函数同样会对相同内容做驻留。
func TestFieldsV1_NewFieldsV1(t *testing.T) {
	t.Run("interning behavior", func(t *testing.T) {
		payload := `{"f:app":{}}`
		s1 := string(append([]byte(nil), payload...))
		s2 := string(append([]byte(nil), payload...))

		f1 := metav1.NewFieldsV1(s1)
		f2 := metav1.NewFieldsV1(s2)

		requireInterned(t, f1.GetRawString(), f2.GetRawString())
	})
}

// TestFieldsV1_DeepCopyInto 验证深拷贝保留驻留（拷贝不复制字符串内容），
// 零值拷贝正确，且拷贝后修改副本不影响原对象。
func TestFieldsV1_DeepCopyInto(t *testing.T) {
	t.Run("preserves interning", func(t *testing.T) {
		orig := metav1.NewFieldsV1(`{"f:app":{}}`)
		var clone metav1.FieldsV1
		orig.DeepCopyInto(&clone)

		if orig.GetRawString() != clone.GetRawString() {
			t.Fatalf("Expected strings to match after DeepCopyInto, got %q and %q", orig.GetRawString(), clone.GetRawString())
		}

		requireInterned(t, orig.GetRawString(), clone.GetRawString())
	})

	t.Run("zero value deep copy", func(t *testing.T) {
		var orig metav1.FieldsV1
		var clone metav1.FieldsV1
		orig.DeepCopyInto(&clone)

		if clone.GetRawString() != "" {
			t.Errorf("Expected empty string from cloned zero-value, got %q", clone.GetRawString())
		}
		if !orig.Equal(clone) {
			t.Errorf("Expected original zero-value and its clone to be equal")
		}
	})

	t.Run("independence after deep copy", func(t *testing.T) {
		orig := metav1.NewFieldsV1(`{"f:app":{}}`)
		var clone metav1.FieldsV1
		orig.DeepCopyInto(&clone)

		// 修改副本。
		clone.SetRawString(`{"f:new":{}}`)

		// 原对象必须完全不受影响，否则说明深拷贝没有正确隔离。
		if got := orig.GetRawString(); got != `{"f:app":{}}` {
			t.Errorf("DeepCopyInto failed to isolate clone! Modifying clone corrupted original. Got: %s", got)
		}
	})
}

// TestFieldsV1_UnmarshalInterning 验证 JSON、protobuf 与 CBOR 三条反序列化路径
// 都会把相同内容驻留到同一地址——这是内存优化的关键契约。
func TestFieldsV1_UnmarshalInterning(t *testing.T) {
	jsonPayload := []byte(`{"f:metadata":{"f:labels":{"f:app":{}}}}`)
	orig := metav1.NewFieldsV1(string(jsonPayload))

	protoPayload, err := orig.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal protobuf payload during setup: %v", err)
	}

	cborPayload, err := orig.MarshalCBOR()
	if err != nil {
		t.Fatalf("Failed to marshal CBOR payload during setup: %v", err)
	}

	tests := []struct {
		name      string
		payload   []byte
		unmarshal func(t *testing.T, f *metav1.FieldsV1, payload []byte)
	}{
		{
			name:    "json unmarshal",
			payload: jsonPayload,
			unmarshal: func(t *testing.T, f *metav1.FieldsV1, payload []byte) {
				if err := json.Unmarshal(payload, f); err != nil {
					t.Fatalf("JSON Unmarshal failed: %v", err)
				}
			},
		},
		{
			name:    "protobuf unmarshal",
			payload: protoPayload,
			unmarshal: func(t *testing.T, f *metav1.FieldsV1, payload []byte) {
				if err := f.Unmarshal(payload); err != nil {
					t.Fatalf("Protobuf Unmarshal failed: %v", err)
				}
			},
		},
		{
			name:    "cbor unmarshal",
			payload: cborPayload,
			unmarshal: func(t *testing.T, f *metav1.FieldsV1, payload []byte) {
				if err := f.UnmarshalCBOR(payload); err != nil {
					t.Fatalf("CBOR Unmarshal failed: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 使用相互独立的底层数组，确保驻留效果来自反序列化逻辑而非输入共享。
			b1 := append([]byte(nil), tc.payload...)
			b2 := append([]byte(nil), tc.payload...)

			var f1 metav1.FieldsV1
			var f2 metav1.FieldsV1

			tc.unmarshal(t, &f1, b1)
			tc.unmarshal(t, &f2, b2)

			requireInterned(t, f1.GetRawString(), f2.GetRawString())
		})
	}
}
