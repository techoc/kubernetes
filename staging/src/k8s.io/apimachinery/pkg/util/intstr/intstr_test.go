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

package intstr

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"sigs.k8s.io/yaml"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"
)

// TestFromInt 验证构造函数把整数标记为 Int 类型。
func TestFromInt(t *testing.T) {
	i := FromInt(93)
	if i.Type != Int || i.IntVal != 93 {
		t.Errorf("Expected IntVal=93, got %+v", i)
	}
}

// TestFromInt32 验证 int32 输入同样产生 Int 类型且不丢失数值。
func TestFromInt32(t *testing.T) {
	i := FromInt32(93)
	if i.Type != Int || i.IntVal != 93 {
		t.Errorf("Expected IntVal=93, got %+v", i)
	}
}

// TestFromString 验证字符串输入始终标记为 String 类型，即使内容是数字。
func TestFromString(t *testing.T) {
	i := FromString("76")
	if i.Type != String || i.StrVal != "76" {
		t.Errorf("Expected StrVal=\"76\", got %+v", i)
	}
}

// IntOrStringHolder 把 IntOrString 包在结构体字段中，避免顶层值走
// encoding/json 的特殊处理，从而测试真实的字段编解码路径。
type IntOrStringHolder struct {
	IOrS IntOrString `json:"val"`
}

// TestIntOrStringUnmarshalJSON 验证 JSON 数字解码为 Int，JSON 字符串解码为
// String——两者不能混同，否则百分比等语义会丢失。
func TestIntOrStringUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result IntOrString
	}{
		{"{\"val\": 123}", FromInt32(123)},
		{"{\"val\": \"123\"}", FromString("123")},
	}

	for _, c := range cases {
		var result IntOrStringHolder
		if err := json.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input '%v': %v", c.input, err)
		}
		if result.IOrS != c.result {
			t.Errorf("Failed to unmarshal input '%v': expected %+v, got %+v", c.input, c.result, result)
		}
	}
}

// TestIntOrStringMarshalJSON 验证序列化保持原始类型：Int 输出为数字，
// String 输出为带引号的字符串。
func TestIntOrStringMarshalJSON(t *testing.T) {
	cases := []struct {
		input  IntOrString
		result string
	}{
		{FromInt32(123), "{\"val\":123}"},
		{FromString("123"), "{\"val\":\"123\"}"},
	}

	for _, c := range cases {
		input := IntOrStringHolder{c.input}
		result, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input '%v': %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input '%v': expected: %+v, got %q", input, c.result, string(result))
		}
	}
}

// TestIntOrStringMarshalJSONUnmarshalYAML 验证 JSON 编码结果可被 YAML 解析
// 并还原为等价值——YAML 是 JSON 的超集，这条路径保证跨格式兼容。
func TestIntOrStringMarshalJSONUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input IntOrString
	}{
		{FromInt32(123)},
		{FromString("123")},
	}

	for _, c := range cases {
		input := IntOrStringHolder{c.input}
		jsonMarshalled, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("1: Failed to marshal input: '%v': %v", input, err)
		}

		var result IntOrStringHolder
		err = yaml.Unmarshal(jsonMarshalled, &result)
		if err != nil {
			t.Errorf("2: Failed to unmarshal '%+v': %v", string(jsonMarshalled), err)
		}

		if !reflect.DeepEqual(input, result) {
			t.Errorf("3: Failed to marshal input '%+v': got %+v", input, result)
		}
	}
}

// TestGetIntFromIntOrString 覆盖取值的宽松解析：Int 直接返回；String 仅在以
// "%" 结尾且前缀为合法整数时视为百分比。注意 "200"（纯数字字符串）会报错，
// 因为字符串形式只允许百分比。
func TestGetIntFromIntOrString(t *testing.T) {
	tests := []struct {
		input      IntOrString
		expectErr  bool
		expectVal  int
		expectPerc bool
	}{
		{
			input:      FromInt32(200),
			expectErr:  false,
			expectVal:  200,
			expectPerc: false,
		},
		{
			input:      FromString("200"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("30%0"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("40%"),
			expectErr:  false,
			expectVal:  40,
			expectPerc: true,
		},
		{
			input:      FromString("%"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("a%"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("a"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("40#"),
			expectErr:  true,
			expectPerc: false,
		},
		{
			input:      FromString("40%%"),
			expectErr:  true,
			expectPerc: false,
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			value, isPercent, err := getIntOrPercentValueSafely(&test.input)
			if test.expectVal != value {
				t.Fatalf("expected value does not match, expected: %d, got: %d", test.expectVal, value)
			}
			if test.expectPerc != isPercent {
				t.Fatalf("expected percent does not match, expected: %t, got: %t", test.expectPerc, isPercent)
			}
			if test.expectErr != (err != nil) {
				t.Fatalf("expected error does not match, expected error: %v, got: %v", test.expectErr, err)
			}
		})
	}

}

// TestGetIntFromIntOrPercent 验证百分比按总量缩放时的取整方向：
// 90% of 95 向上取整为 86，向下取整为 85；非法格式与纯数字字符串均报错。
func TestGetIntFromIntOrPercent(t *testing.T) {
	tests := []struct {
		input     IntOrString
		total     int
		roundUp   bool
		expectErr bool
		expectVal int
	}{
		{
			input:     FromInt32(123),
			expectErr: false,
			expectVal: 123,
		},
		{
			input:     FromString("90%"),
			total:     100,
			roundUp:   true,
			expectErr: false,
			expectVal: 90,
		},
		{
			input:     FromString("90%"),
			total:     95,
			roundUp:   true,
			expectErr: false,
			expectVal: 86,
		},
		{
			input:     FromString("90%"),
			total:     95,
			roundUp:   false,
			expectErr: false,
			expectVal: 85,
		},
		{
			input:     FromString("%"),
			expectErr: true,
		},
		{
			input:     FromString("90#"),
			expectErr: true,
		},
		{
			input:     FromString("#%"),
			expectErr: true,
		},
		{
			input:     FromString("90"),
			expectErr: true,
		},
	}

	for i, test := range tests {
		t.Logf("test case %d", i)
		value, err := GetScaledValueFromIntOrPercent(&test.input, test.total, test.roundUp)
		if test.expectErr && err == nil {
			t.Errorf("expected error, but got none")
			continue
		}
		if !test.expectErr && err != nil {
			t.Errorf("unexpected err: %v", err)
			continue
		}
		if test.expectVal != value {
			t.Errorf("expected %v, but got %v", test.expectVal, value)
		}
	}
}

// TestGetValueFromIntOrPercentNil 验证 nil 输入必须报错而非 panic。
func TestGetValueFromIntOrPercentNil(t *testing.T) {
	_, err := GetScaledValueFromIntOrPercent(nil, 0, false)
	if err == nil {
		t.Errorf("expected error got none")
	}
}

// TestParse 验证 Parse 只把落在 int32 范围内的十进制解析为 Int，超出范围
// （包括 int64 范围）一律保留为 String，避免不同平台 int 宽度差异导致
// 语义漂移。
func TestParse(t *testing.T) {
	tests := []struct {
		input  string
		output IntOrString
	}{
		{
			input:  "0",
			output: IntOrString{Type: Int, IntVal: 0},
		},
		{
			input:  "2147483647", // math.MaxInt32
			output: IntOrString{Type: Int, IntVal: 2147483647},
		},
		{
			input:  "-2147483648", // math.MinInt32
			output: IntOrString{Type: Int, IntVal: -2147483648},
		},
		{
			input:  "2147483648", // math.MaxInt32+1
			output: IntOrString{Type: String, StrVal: "2147483648"},
		},
		{
			input:  "-2147483649", // math.MinInt32-1
			output: IntOrString{Type: String, StrVal: "-2147483649"},
		},
		{
			input:  "9223372036854775807", // math.MaxInt64
			output: IntOrString{Type: String, StrVal: "9223372036854775807"},
		},
		{
			input:  "-9223372036854775808", // math.MinInt64
			output: IntOrString{Type: String, StrVal: "-9223372036854775808"},
		},
		{
			input:  "9223372036854775808", // math.MaxInt64+1
			output: IntOrString{Type: String, StrVal: "9223372036854775808"},
		},
		{
			input:  "-9223372036854775809", // math.MinInt64-1
			output: IntOrString{Type: String, StrVal: "-9223372036854775809"},
		},
	}

	for i, test := range tests {
		t.Logf("test case %d", i)
		value := Parse(test.input)
		if test.output.Type != value.Type {
			t.Errorf("expected type %d (%v), but got %d (%v)", test.output.Type, test.output, value.Type, value)
			continue
		}
		if value.Type == Int && test.output.IntVal != value.IntVal {
			t.Errorf("expected int value %d (%v), but got %d (%v)", test.output.IntVal, test.output, value.IntVal, value)
			continue
		}
		if value.Type == String && test.output.StrVal != value.StrVal {
			t.Errorf("expected string value %q (%v), but got %q (%v)", test.output.StrVal, test.output, value.StrVal, value)
		}
	}
}

// TestMarshalCBOR 验证 CBOR 编码使用规范的最短整数编码：Int 按数值大小选择
// 1/2/3/5 字节表示，String 按 UTF-8 长度编码；非法 Type 必须报错。
func TestMarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		in            IntOrString
		want          []byte
		assertOnError func(*testing.T, error)
	}{
		{
			in: IntOrString{Type: 42},
			assertOnError: func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected non-nil error")
				}
				const want = "impossible IntOrString.Type"
				if got := err.Error(); got != want {
					t.Fatalf("want error message %q, got %q", want, got)
				}
			},
		},
		{
			in:   FromString(""),
			want: []byte{0x40},
		},
		{
			in:   FromString("abc"),
			want: []byte{0x43, 'a', 'b', 'c'},
		},
		{
			in:   FromInt32(0), // min positive integer representable in one byte
			want: []byte{0x00},
		},
		{
			in:   FromInt32(23), // max positive integer representable in one byte
			want: []byte{0x17},
		},
		{
			in:   FromInt32(24), // min positive integer representable in two bytes
			want: []byte{0x18, 0x18},
		},
		{
			in:   FromInt32(math.MaxUint8), // max positive integer representable in two bytes
			want: []byte{0x18, 0xff},
		},
		{
			in:   FromInt32(math.MaxUint8 + 1), // min positive integer representable in three bytes
			want: []byte{0x19, 0x01, 0x00},
		},
		{
			in:   FromInt32(math.MaxUint16), // max positive integer representable in three bytes
			want: []byte{0x19, 0xff, 0xff},
		},
		{
			in:   FromInt32(math.MaxUint16 + 1), // min positive integer representable in five bytes
			want: []byte{0x1a, 0x00, 0x01, 0x00, 0x00},
		},
		{
			in:   FromInt32(math.MaxInt32), // max positive integer representable by Go int32
			want: []byte{0x1a, 0x7f, 0xff, 0xff, 0xff},
		},
		{
			in:   FromInt32(-1), // max negative integer representable in one byte
			want: []byte{0x20},
		},
		{
			in:   FromInt32(-24), // min negative integer representable in one byte
			want: []byte{0x37},
		},
		{
			in:   FromInt32(-1 - 24), // max negative integer representable in two bytes
			want: []byte{0x38, 0x18},
		},
		{
			in:   FromInt32(-1 - math.MaxUint8), // min negative integer representable in two bytes
			want: []byte{0x38, 0xff},
		},
		{
			in:   FromInt32(-2 - math.MaxUint8), // max negative integer representable in three bytes
			want: []byte{0x39, 0x01, 0x00},
		},
		{
			in:   FromInt32(-1 - math.MaxUint16), // min negative integer representable in three bytes
			want: []byte{0x39, 0xff, 0xff},
		},
		{
			in:   FromInt32(-2 - math.MaxUint16), // max negative integer representable in five bytes
			want: []byte{0x3a, 0x00, 0x01, 0x00, 0x00},
		},
		{
			in:   FromInt32(math.MinInt32), // min negative integer representable by Go int32
			want: []byte{0x3a, 0x7f, 0xff, 0xff, 0xff},
		},
	} {
		t.Run(fmt.Sprintf("{Type:%d,IntVal:%d,StrVal:%q}", tc.in.Type, tc.in.IntVal, tc.in.StrVal), func(t *testing.T) {
			got, err := tc.in.MarshalCBOR()
			if tc.assertOnError != nil {
				tc.assertOnError(t, err)
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("unexpected difference between expected and actual output:\n%s", diff)
			}
		})
	}
}

// TestUnmarshalCBOR 是 MarshalCBOR 的逆过程：整数解码为 Int、文本字符串解码
// 为 String，map 等不支持的输入必须报错。
func TestUnmarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		in            []byte
		want          IntOrString
		assertOnError func(*testing.T, error)
	}{
		{
			in: []byte{0xa0}, // {}
			assertOnError: func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected non-nil error")
				}
				const want = "cbor: cannot unmarshal map into Go value of type int32"
				if got := err.Error(); got != want {
					t.Fatalf("want error message %q, got %q", want, got)
				}

			},
		},
		{
			in:   []byte{0x40},
			want: FromString(""),
		},
		{
			in:   []byte{0x43, 'a', 'b', 'c'},
			want: FromString("abc"),
		},
		{
			in:   []byte{0x00},
			want: FromInt32(0), // min positive integer representable in one byte
		},
		{
			in:   []byte{0x17},
			want: FromInt32(23), // max positive integer representable in one byte
		},
		{
			in:   []byte{0x18, 0x18},
			want: FromInt32(24), // min positive integer representable in two bytes
		},
		{
			in:   []byte{0x18, 0xff},
			want: FromInt32(math.MaxUint8), // max positive integer representable in two bytes
		},
		{
			in:   []byte{0x19, 0x01, 0x00},
			want: FromInt32(math.MaxUint8 + 1), // min positive integer representable in three bytes
		},
		{
			in:   []byte{0x19, 0xff, 0xff},
			want: FromInt32(math.MaxUint16), // max positive integer representable in three bytes
		},
		{
			in:   []byte{0x1a, 0x00, 0x01, 0x00, 0x00},
			want: FromInt32(math.MaxUint16 + 1), // min positive integer representable in five bytes
		},
		{
			in:   []byte{0x1a, 0x7f, 0xff, 0xff, 0xff},
			want: FromInt32(math.MaxInt32), // max positive integer representable by Go int32
		},
		{
			in:   []byte{0x20},
			want: FromInt32(-1), // max negative integer representable in one byte
		},
		{
			in:   []byte{0x37},
			want: FromInt32(-24), // min negative integer representable in one byte
		},
		{
			in:   []byte{0x38, 0x18},
			want: FromInt32(-1 - 24), // max negative integer representable in two bytes
		},
		{
			in:   []byte{0x38, 0xff},
			want: FromInt32(-1 - math.MaxUint8), // min negative integer representable in two bytes
		},
		{
			in:   []byte{0x39, 0x01, 0x00},
			want: FromInt32(-2 - math.MaxUint8), // max negative integer representable in three bytes
		},
		{
			in:   []byte{0x39, 0xff, 0xff},
			want: FromInt32(-1 - math.MaxUint16), // min negative integer representable in three bytes
		},
		{
			in:   []byte{0x3a, 0x00, 0x01, 0x00, 0x00},
			want: FromInt32(-2 - math.MaxUint16), // max negative integer representable in five bytes
		},
		{
			in:   []byte{0x3a, 0x7f, 0xff, 0xff, 0xff},
			want: FromInt32(math.MinInt32), // min negative integer representable by Go int32
		},
	} {
		t.Run(fmt.Sprintf("{Type:%d,IntVal:%d,StrVal:%q}", tc.want.Type, tc.want.IntVal, tc.want.StrVal), func(t *testing.T) {
			var got IntOrString
			err := got.UnmarshalCBOR(tc.in)
			if tc.assertOnError != nil {
				tc.assertOnError(t, err)
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("unexpected difference between expected and actual output:\n%s", diff)
			}
		})
	}
}

// TestIntOrStringRoundtripCBOR 随机生成 IntOrString 并做 CBOR 往返，验证
// 编码/解码在任意合法值上都保持完全相等；失败时输出诊断编码以便定位。
func TestIntOrStringRoundtripCBOR(t *testing.T) {
	fuzzer := randfill.New()
	for i := 0; i < 500; i++ {
		var initial, final IntOrString
		fuzzer.Fill(&initial)
		b, err := cbor.Marshal(initial)
		if err != nil {
			t.Errorf("error encoding %v: %v", initial, err)
			continue
		}
		err = cbor.Unmarshal(b, &final)
		if err != nil {
			t.Errorf("%v: error decoding %v: %v", initial, string(b), err)
		}
		if diff := cmp.Diff(initial, final); diff != "" {
			diag, err := cbor.Diagnose(b)
			if err != nil {
				t.Logf("failed to produce diagnostic encoding of 0x%x: %v", b, err)
			}
			t.Errorf("unexpected diff:\n%s\ncbor: %s", diff, diag)
		}
	}
}
