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

package util

import (
	"reflect"
	"testing"

	"k8s.io/gengo/v2/types"
)

// TestGetMemberByJSON 验证按 json 标签名查字段，而非按 Go 字段名。
func TestGetMemberByJSON(t *testing.T) {
	tests := []struct {
		name     string
		t        *types.Type
		jsonTag  string
		want     *types.Member
		wantBool bool
	}{{
		name: "exact match",
		t: &types.Type{
			Members: []types.Member{
				{Name: "Field0", Tags: `json:"field0"`},
				{Name: "Field1", Tags: `json:"field1"`},
				{Name: "Field2", Tags: `json:"field2"`},
			},
		},
		jsonTag:  "field1",
		want:     &types.Member{Name: "Field1", Tags: `json:"field1"`},
		wantBool: true,
	}, {
		name: "no match",
		t: &types.Type{
			Members: []types.Member{
				{Name: "Field0", Tags: `json:"field0"`},
				{Name: "Field1", Tags: `json:"field1"`},
			},
		},
		jsonTag:  "field2",
		want:     nil,
		wantBool: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetMemberByJSON(tt.t, tt.jsonTag)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMemberByJSON() got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsNilableType 验证可空类型判定：指针、map、slice、interface 及其 alias
// 为 true，struct/builtin 及其 alias 为 false。
func TestIsNilableType(t *testing.T) {
	tStruct := &types.Type{
		Name: types.Name{Name: "MyStruct"},
		Kind: types.Struct,
	}

	tests := []struct {
		name string
		t    *types.Type
		want bool
	}{{
		name: "pointer",
		t: &types.Type{
			Kind: types.Pointer,
			Elem: tStruct,
		},
		want: true,
	}, {
		name: "alias to pointer",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Pointer,
				Elem: tStruct,
			},
		},
		want: true,
	}, {
		name: "map",
		t: &types.Type{
			Kind: types.Map,
		},
		want: true,
	}, {
		name: "alias to map",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Map,
			},
		},
		want: true,
	}, {
		name: "slice",
		t: &types.Type{
			Kind: types.Slice,
		},
		want: true,
	}, {
		name: "alias to slice",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Slice,
			},
		},
		want: true,
	}, {
		name: "interface",
		t: &types.Type{
			Kind: types.Interface,
		},
		want: true,
	}, {
		name: "alias to interface",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Interface,
			},
		},
		want: true,
	}, {
		name: "struct",
		t: &types.Type{
			Kind: types.Struct,
		},
		want: false,
	}, {
		name: "alias to struct",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Struct,
			},
		},
		want: false,
	}, {
		name: "builtin",
		t: &types.Type{
			Kind: types.Builtin,
		},
		want: false,
	}, {
		name: "alias to builtin",
		t: &types.Type{
			Kind: types.Alias,
			Underlying: &types.Type{
				Kind: types.Builtin,
			},
		},
		want: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNilableType(tt.t); got != tt.want {
				t.Errorf("IsNilableType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNativeType 验证 alias 解引用后保留指针层级：
// alias 只影响类型名，不改变指针结构；但指针下的 alias 会被完全展开。
func TestNativeType(t *testing.T) {
	tStruct := &types.Type{
		Name: types.Name{Name: "MyStruct"},
		Kind: types.Struct,
	}
	pStruct := &types.Type{
		Name: types.Name{Name: "*MyStruct"},
		Kind: types.Pointer,
		Elem: tStruct,
	}

	tests := []struct {
		name string
		t    *types.Type
		want *types.Type
	}{{
		name: "struct",
		t:    tStruct,
		want: tStruct,
	}, {
		name: "pointer to struct",
		t:    pStruct,
		want: pStruct,
	}, {
		name: "alias to struct",
		t: &types.Type{
			Name:       types.Name{Name: "Alias"},
			Kind:       types.Alias,
			Underlying: tStruct,
		},
		want: tStruct,
	}, {
		name: "pointer to alias to struct",
		t: &types.Type{
			Name: types.Name{Name: "*Alias"},
			Kind: types.Pointer,
			Elem: &types.Type{
				Kind:       types.Alias,
				Underlying: tStruct,
			},
		},
		want: pStruct,
	}, {
		name: "alias of pointer to struct",
		t: &types.Type{
			Name:       types.Name{Name: "AliasP"},
			Kind:       types.Alias,
			Underlying: pStruct,
		},
		want: pStruct,
	}, {
		name: "pointer to alias of pointer to struct",
		t: &types.Type{
			Name: types.Name{Name: "*AliasP"},
			Kind: types.Pointer,
			Elem: &types.Type{
				Name:       types.Name{Name: "AliasP"},
				Kind:       types.Alias,
				Underlying: pStruct,
			},
		},
		want: &types.Type{
			Name: types.Name{Name: "**MyStruct"},
			Kind: types.Pointer,
			Elem: pStruct,
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if want, got := tt.want.String(), NativeType(tt.t).String(); want != got {
				t.Errorf("NativeType() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNonPointer 验证剥离指针层级的行为：
// 只剥外层指针，遇到 alias 即停止，保留 alias 本身。
func TestNonPointer(t *testing.T) {
	tStruct := &types.Type{
		Name: types.Name{Name: "MyStruct"},
		Kind: types.Struct,
	}

	tests := []struct {
		name string
		t    *types.Type
		want *types.Type
	}{{
		name: "value",
		t:    tStruct,
		want: tStruct,
	}, {
		name: "pointer",
		t: &types.Type{
			Name: types.Name{Name: "*MyStruct"},
			Kind: types.Pointer,
			Elem: tStruct,
		},
		want: tStruct,
	}, {
		name: "pointer pointer",
		t: &types.Type{
			Name: types.Name{Name: "**MyStruct"},
			Kind: types.Pointer,
			Elem: &types.Type{
				Kind: types.Pointer,
				Elem: tStruct,
			},
		},
		want: tStruct,
	}, {
		name: "pointer alias pointer",
		t: &types.Type{
			Name: types.Name{Name: "*AliasP"},
			Kind: types.Pointer,
			Elem: &types.Type{
				Name: types.Name{Name: "AliasP"},
				Kind: types.Alias,
				Underlying: &types.Type{
					Name: types.Name{Name: "*MyStruct"},
					Kind: types.Pointer,
					Elem: tStruct,
				},
			},
		},
		want: &types.Type{
			Name: types.Name{Name: "AliasP"},
			Kind: types.Alias,
			Underlying: &types.Type{
				Name: types.Name{Name: "*MyStruct"},
				Kind: types.Pointer,
				Elem: tStruct,
			},
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NonPointer(tt.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NonPointer() = %v, want %v", got, tt.want)
			}
		})
	}
}

// gengo 只提供 `PointerTo()`，其余没有，为保持一致留在这里。
//
// 以下构造函数用于在测试里快速拼出各种类型形状；
// 显式设置 Name 便于失败信息中可读。
func ptrTo(t *types.Type) *types.Type {
	return &types.Type{
		Name: types.Name{
			Package: "",
			Name:    "*" + t.Name.String(),
		},
		Kind: types.Pointer,
		Elem: t,
	}
}

// sliceOf 构造元素类型为 t 的 slice。
func sliceOf(t *types.Type) *types.Type {
	return &types.Type{
		Name: types.Name{
			Package: "",
			Name:    "[]" + t.Name.String(),
		},
		Kind: types.Slice,
		Elem: t,
	}
}

// mapOf 构造 map[string]t；key 固定为 string 足以覆盖可比较性判定。
func mapOf(t *types.Type) *types.Type {
	return &types.Type{
		Name: types.Name{
			Package: "",
			Name:    "map[string]" + t.Name.String(),
		},
		Kind: types.Map,
		Key:  types.String,
		Elem: t,
	}
}

// arrayOf 构造定长数组 [2]t，用于验证数组的可比较性由元素类型决定。
func arrayOf(t *types.Type) *types.Type {
	return &types.Type{
		Name: types.Name{
			Package: "",
			Name:    "[2]" + t.Name.String(),
		},
		Kind: types.Array,
		Len:  2,
		Elem: t,
	}
}

// aliasOf 构造命名 alias；名字前缀固定，避免与内建类型混淆。
func aliasOf(name string, t *types.Type) *types.Type {
	return &types.Type{
		Name: types.Name{
			Package: "",
			Name:    "Alias_" + name,
		},
		Kind:       types.Alias,
		Underlying: t,
	}
}

// TestIsDirectComparable 验证 Go 的“可直接用 == 比较”规则：
// 指针/slice/map 不可比，数组与结构体递归取决于元素/字段类型。
func TestIsDirectComparable(t *testing.T) {
	cases := []struct {
		in     *types.Type
		expect bool
	}{
		{
			in:     types.String,
			expect: true,
		}, {
			in:     ptrTo(types.String),
			expect: false,
		}, {
			in:     sliceOf(types.String),
			expect: false,
		}, {
			in:     mapOf(types.String),
			expect: false,
		}, {
			in:     aliasOf("s", types.String),
			expect: true,
		}, {
			in: &types.Type{
				Name: types.Name{
					Package: "",
					Name:    "struct_comparable_member",
				},
				Kind: types.Struct,
				Members: []types.Member{
					{
						Name: "s",
						Type: types.String,
					},
				},
			},
			expect: true,
		}, {
			in: &types.Type{
				Name: types.Name{
					Package: "",
					Name:    "struct_uncomparable_member",
				},
				Kind: types.Struct,
				Members: []types.Member{
					{
						Name: "s",
						Type: ptrTo(types.String),
					},
				},
			},
			expect: false,
		}, {
			in:     arrayOf(types.String),
			expect: true,
		}, {
			in:     arrayOf(aliasOf("s", types.String)),
			expect: true,
		}, {
			in:     arrayOf(ptrTo(types.String)),
			expect: false,
		}, {
			in:     arrayOf(mapOf(types.String)),
			expect: false,
		},
	}

	for _, tc := range cases {
		if got, want := IsDirectComparable(tc.in), tc.expect; got != want {
			t.Errorf("%q: expected %v, got %v", tc.in, want, got)
		}
	}
}

// TestParseInt 验证整数解析只接受规范形式（无前导零、无一元 +）,
// 保证标签字面量与生成代码中的常量写法一致。
func TestParseInt(t *testing.T) {
	type testcase struct {
		name          string
		in            string
		expectedOut   int
		expectedError bool
	}

	testcases := []testcase{
		{
			name:        "valid canonical positive integer string",
			in:          "100",
			expectedOut: 100,
		},
		{
			name:        "valid canonical negative integer string",
			in:          "-100",
			expectedOut: -100,
		},
		{
			name:          "empty string",
			in:            "",
			expectedError: true,
		},
		{
			name:          "invalid unary positive integer string",
			in:            "+100",
			expectedError: true,
		},
		{
			name:          "invalid canonical integer string, not an integer at all",
			in:            "notanint",
			expectedError: true,
		},
		{
			name:          "invalid canonical integer string, spurious leading zeros",
			in:            "00100",
			expectedError: true,
		},
		{
			name:          "invalid canonical integer string, octal value",
			in:            "0o123",
			expectedError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ParseInt(tc.in)
			switch {
			case tc.expectedError && err == nil:
				t.Error("expected an error but did not receive one")
			case !tc.expectedError && err != nil:
				t.Errorf("received an unexpected error: %v", err)
			}

			if out != tc.expectedOut {
				t.Errorf("expected an output value of %d but got %d", tc.expectedOut, out)
			}
		})
	}
}

// TestParseSignedInt 覆盖 int8/16/32/64 的边界与溢出，并验证非规范形式、
// 未知位宽被拒绝。边界值同时检查“恰好可表示”和“越界一位”。
func TestParseSignedInt(t *testing.T) {
	type testcase struct {
		name          string
		in            string
		bitSize       int
		expectedOut   int64
		expectedError bool
	}

	testcases := []testcase{
		// --- int32 合法边界 ---
		{
			name:        "int32 exact minimum boundary",
			in:          "-2147483648",
			bitSize:     32,
			expectedOut: -2147483648,
		},
		{
			name:        "int32 exact maximum boundary",
			in:          "2147483647",
			bitSize:     32,
			expectedOut: 2147483647,
		},
		{
			name:        "int32 zero",
			in:          "0",
			bitSize:     32,
			expectedOut: 0,
		},
		{
			name:        "int32 positive value",
			in:          "100",
			bitSize:     32,
			expectedOut: 100,
		},
		{
			name:        "int32 negative value",
			in:          "-1",
			bitSize:     32,
			expectedOut: -1,
		},
		// --- int32 溢出 ---
		{
			name:          "int32 one below minimum overflows",
			in:            "-2147483649",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "int32 one above maximum overflows",
			in:            "2147483648",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:        "int64 accepts value that overflows int32",
			in:          "2147483648",
			bitSize:     64,
			expectedOut: 2147483648,
		},
		// --- int64 合法边界 ---
		{
			name:        "int64 exact minimum boundary",
			in:          "-9223372036854775808",
			bitSize:     64,
			expectedOut: -9223372036854775808,
		},
		{
			name:        "int64 exact maximum boundary",
			in:          "9223372036854775807",
			bitSize:     64,
			expectedOut: 9223372036854775807,
		},
		// --- int64 溢出 ---
		{
			name:          "int64 one above maximum overflows",
			in:            "9223372036854775808",
			bitSize:       64,
			expectedError: true,
		},
		{
			name:          "int64 one below minimum overflows",
			in:            "-9223372036854775809",
			bitSize:       64,
			expectedError: true,
		},
		// --- int16 边界 ---
		{
			name:        "int16 exact minimum boundary",
			in:          "-32768",
			bitSize:     16,
			expectedOut: -32768,
		},
		{
			name:        "int16 exact maximum boundary",
			in:          "32767",
			bitSize:     16,
			expectedOut: 32767,
		},
		{
			name:          "int16 one above maximum overflows",
			in:            "32768",
			bitSize:       16,
			expectedError: true,
		},
		{
			name:          "int16 one below minimum overflows",
			in:            "-32769",
			bitSize:       16,
			expectedError: true,
		},
		// --- int8 边界 ---
		{
			name:        "int8 exact minimum boundary",
			in:          "-128",
			bitSize:     8,
			expectedOut: -128,
		},
		{
			name:        "int8 exact maximum boundary",
			in:          "127",
			bitSize:     8,
			expectedOut: 127,
		},
		{
			name:          "int8 one above maximum overflows",
			in:            "128",
			bitSize:       8,
			expectedError: true,
		},
		{
			name:          "int8 one below minimum overflows",
			in:            "-129",
			bitSize:       8,
			expectedError: true,
		},
		// --- 规范形式拒绝 ---
		{
			name:          "leading zeros rejected",
			in:            "0100",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "unary plus rejected",
			in:            "+1",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "octal notation rejected",
			in:            "0o77",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "hex notation rejected",
			in:            "0xFF",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "empty string rejected",
			in:            "",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "non-numeric string rejected",
			in:            "abc",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "floating point rejected",
			in:            "1.5",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "whitespace rejected",
			in:            " 1",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "negative zero canonical form rejected",
			in:            "-0",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "unknown bit size returns error",
			in:            "1",
			bitSize:       7,
			expectedError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ParseSignedInt(tc.in, tc.bitSize)
			switch {
			case tc.expectedError && err == nil:
				t.Errorf("expected an error for input %q with bitSize %d but did not receive one (got %d)", tc.in, tc.bitSize, out)
			case !tc.expectedError && err != nil:
				t.Errorf("received an unexpected error for input %q with bitSize %d: %v", tc.in, tc.bitSize, err)
			}

			if out != tc.expectedOut {
				t.Errorf("expected output %d but got %d", tc.expectedOut, out)
			}
		})
	}
}

// TestParseUnsignedInt 覆盖 uint8/16/32/64 的上界、溢出、负值拒绝
// 以及规范形式校验；无下界所以只测最大值。
func TestParseUnsignedInt(t *testing.T) {
	type testcase struct {
		name          string
		in            string
		bitSize       int
		expectedOut   uint64
		expectedError bool
	}

	testcases := []testcase{
		// --- uint64 合法边界 ---
		{
			name:        "uint64 maximum boundary",
			in:          "18446744073709551615",
			bitSize:     64,
			expectedOut: 18446744073709551615,
		},
		{
			name:        "uint64 zero",
			in:          "0",
			bitSize:     64,
			expectedOut: 0,
		},
		// --- uint64 溢出 ---
		{
			name:          "uint64 one above maximum overflows",
			in:            "18446744073709551616",
			bitSize:       64,
			expectedError: true,
		},
		// --- uint32 合法边界 ---
		{
			name:        "uint32 maximum boundary",
			in:          "4294967295",
			bitSize:     32,
			expectedOut: 4294967295,
		},
		{
			name:        "uint32 zero",
			in:          "0",
			bitSize:     32,
			expectedOut: 0,
		},
		{
			name:          "uint32 one above maximum overflows",
			in:            "4294967296",
			bitSize:       32,
			expectedError: true,
		},
		// --- uint16 合法边界 ---
		{
			name:        "uint16 maximum boundary",
			in:          "65535",
			bitSize:     16,
			expectedOut: 65535,
		},
		{
			name:          "uint16 one above maximum overflows",
			in:            "65536",
			bitSize:       16,
			expectedError: true,
		},
		// --- uint8 合法边界 ---
		{
			name:        "uint8 maximum boundary",
			in:          "255",
			bitSize:     8,
			expectedOut: 255,
		},
		{
			name:          "uint8 one above maximum overflows",
			in:            "256",
			bitSize:       8,
			expectedError: true,
		},
		// --- 无符号类型拒绝负值 ---
		{
			name:          "negative value rejected",
			in:            "-1",
			bitSize:       64,
			expectedError: true,
		},
		{
			name:        "uint64 accepts value that overflows uint32",
			in:          "4294967296",
			bitSize:     64,
			expectedOut: 4294967296,
		},
		// --- 规范形式拒绝 ---
		{
			name:          "leading zeros rejected",
			in:            "0100",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "unary plus rejected",
			in:            "+1",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "empty string rejected",
			in:            "",
			bitSize:       64,
			expectedError: true,
		},
		{
			name:          "hex notation rejected",
			in:            "0xFF",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "floating point rejected",
			in:            "1.0",
			bitSize:       32,
			expectedError: true,
		},
		{
			name:          "unknown bit size returns error",
			in:            "1",
			bitSize:       7,
			expectedError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ParseUnsignedInt(tc.in, tc.bitSize)
			switch {
			case tc.expectedError && err == nil:
				t.Errorf("expected an error for input %q with bitSize %d but did not receive one (got %d)", tc.in, tc.bitSize, out)
			case !tc.expectedError && err != nil:
				t.Errorf("received an unexpected error for input %q with bitSize %d: %v", tc.in, tc.bitSize, err)
			}

			if out != tc.expectedOut {
				t.Errorf("expected output %d but got %d", tc.expectedOut, out)
			}
		})
	}
}

// TestParseBool 验证 bool 只接受小写 "true"/"false"，
// 拒绝大写、数字和 YAML 风格写法，避免标签语义产生歧义。
func TestParseBool(t *testing.T) {
	type testcase struct {
		name          string
		in            string
		expectedOut   bool
		expectedError bool
	}

	testcases := []testcase{
		{
			name:        "valid canonical true string",
			in:          "true",
			expectedOut: true,
		},
		{
			name:        "valid canonical false string",
			in:          "false",
			expectedOut: false,
		},
		{
			name:          "empty string",
			in:            "",
			expectedError: true,
		},
		{
			name:          "invalid canonical boolean string, not a bool at all",
			in:            "notabool",
			expectedError: true,
		},
		{
			name:          "invalid canonical boolean string, capitalized",
			in:            "True",
			expectedError: true,
		},
		{
			name:          "invalid canonical boolean string, numeric",
			in:            "1",
			expectedError: true,
		},
		{
			name:          "invalid canonical boolean string, YAML",
			in:            "yes",
			expectedError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ParseBool(tc.in)
			switch {
			case tc.expectedError && err == nil:
				t.Error("expected an error but did not receive one")
			case !tc.expectedError && err != nil:
				t.Errorf("received an unexpected error: %v", err)
			}

			if out != tc.expectedOut {
				t.Errorf("expected an output value of %v but got %v", tc.expectedOut, out)
			}
		})
	}
}
