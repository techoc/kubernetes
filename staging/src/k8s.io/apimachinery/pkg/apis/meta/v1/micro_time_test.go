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
	"fmt"
	"reflect"
	"testing"
	"time"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"sigs.k8s.io/yaml"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"
)

// MicroTimeHolder 是带 MicroTime 字段的测试包装，用于验证 JSON/YAML 编解码。
type MicroTimeHolder struct {
	T MicroTime `json:"t"`
}

// TestMicroTimeMarshalYAML 验证 MicroTime 的 YAML 输出：零值编码为 null，
// 非零值统一转成 UTC 并保留六位小数（微秒精度）。
func TestMicroTimeMarshalYAML(t *testing.T) {
	cases := []struct {
		input  MicroTime
		result string
	}{
		{MicroTime{}, "t: null\n"},
		{DateMicro(1998, time.May, 5, 1, 5, 5, 50, time.FixedZone("test", -4*60*60)), "t: \"1998-05-05T05:05:05.000000Z\"\n"},
		{DateMicro(1998, time.May, 5, 5, 5, 5, 0, time.UTC), "t: \"1998-05-05T05:05:05.000000Z\"\n"},
	}

	for _, c := range cases {
		input := MicroTimeHolder{c.input}
		result, err := yaml.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: '%v': %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: '%v': expected %+v, got %q", input, c.result, string(result))
		}
	}
}

// TestMicroTimeUnmarshalYAML 验证 YAML 反序列化：null 得到零值，
// RFC3339 时间戳解析后转换为本地时区。
func TestMicroTimeUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input  string
		result MicroTime
	}{
		{"t: null\n", MicroTime{}},
		{"t: 1998-05-05T05:05:05.000000Z\n", MicroTime{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},
	}

	for _, c := range cases {
		var result MicroTimeHolder
		if err := yaml.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input '%v': %v", c.input, err)
		}
		if result.T != c.result {
			t.Errorf("Failed to unmarshal input '%v': expected %+v, got %+v", c.input, c.result, result)
		}
	}
}

// TestMicroTimeMarshalJSON 验证 JSON 输出：零值为 null，非零值以 RFC3339 格式（UTC、微秒精度）输出。
func TestMicroTimeMarshalJSON(t *testing.T) {
	cases := []struct {
		input  MicroTime
		result string
	}{
		{MicroTime{}, "{\"t\":null}"},
		{DateMicro(1998, time.May, 5, 5, 5, 5, 50, time.UTC), "{\"t\":\"1998-05-05T05:05:05.000000Z\"}"},
		{DateMicro(1998, time.May, 5, 5, 5, 5, 0, time.UTC), "{\"t\":\"1998-05-05T05:05:05.000000Z\"}"},
	}

	for _, c := range cases {
		input := MicroTimeHolder{c.input}
		result, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: '%v': %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: '%v': expected %+v, got %q", input, c.result, string(result))
		}
	}
}

// TestMicroTimeUnmarshalJSON 验证 JSON 反序列化：null 得到零值，RFC3339 时间戳转为本地时区。
func TestMicroTimeUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result MicroTime
	}{
		{"{\"t\":null}", MicroTime{}},
		{"{\"t\":\"1998-05-05T05:05:05.000000Z\"}", MicroTime{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},
	}

	for _, c := range cases {
		var result MicroTimeHolder
		if err := json.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input '%v': %v", c.input, err)
		}
		if result.T != c.result {
			t.Errorf("Failed to unmarshal input '%v': expected %+v, got %+v", c.input, c.result, result)
		}
	}
}

// TestMicroTimeMarshalCBOR 验证 CBOR 编码：零值为 null，时间字符串固定六位小数，
// 纳秒部分按微秒截断。
func TestMicroTimeMarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   MicroTime
		out  []byte
	}{
		{name: "zero value", in: MicroTime{}, out: []byte{0xf6}},                                                                                       // null
		{name: "no fractional seconds", in: DateMicro(1998, time.May, 5, 5, 5, 5, 0, time.UTC), out: []byte("\x58\x1b1998-05-05T05:05:05.000000Z")},    // '1998-05-05T05:05:05.000000Z'
		{name: "nanoseconds truncated", in: DateMicro(1998, time.May, 5, 5, 5, 5, 5050, time.UTC), out: []byte("\x58\x1b1998-05-05T05:05:05.000005Z")}, // '1998-05-05T05:05:05.000005Z'
	} {
		t.Run(fmt.Sprintf("%+v", tc.in), func(t *testing.T) {
			got, err := tc.in.MarshalCBOR()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.out, got); diff != "" {
				t.Errorf("unexpected output:\n%s", diff)
			}
		})
	}
}

// TestMicroTimeUnmarshalCBOR 验证 CBOR 解码：null、合法时间戳与非法输入（类型错误、格式错误）的错误信息。
func TestMicroTimeUnmarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		name       string
		in         []byte
		out        MicroTime
		errMessage string
	}{
		{name: "null", in: []byte{0xf6}, out: MicroTime{}}, // null
		{name: "valid", in: []byte("\x58\x1b1998-05-05T05:05:05.000000Z"), out: MicroTime{Time: Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},                                    // '1998-05-05T05:05:05.000000Z'
		{name: "invalid cbor type", in: []byte{0x07}, out: MicroTime{}, errMessage: "cbor: cannot unmarshal positive integer into Go value of type string"},                                // 7
		{name: "malformed timestamp", in: []byte("\x45hello"), out: MicroTime{}, errMessage: `parsing time "hello" as "2006-01-02T15:04:05.000000Z07:00": cannot parse "hello" as "2006"`}, // 'hello'
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got MicroTime
			err := got.UnmarshalCBOR(tc.in)
			if err != nil {
				if tc.errMessage == "" {
					t.Fatalf("want nil error, got: %v", err)
				} else if gotMessage := err.Error(); tc.errMessage != gotMessage {
					t.Fatalf("want error: %q, got: %q", tc.errMessage, gotMessage)
				}
			} else if tc.errMessage != "" {
				t.Fatalf("got nil error, want: %s", tc.errMessage)
			}
			if diff := cmp.Diff(tc.out, got); diff != "" {
				t.Errorf("unexpected output:\n%s", diff)
			}
		})
	}
}

// TestMicroTimeProto 验证 protobuf 编解码往返的幂等性。
func TestMicroTimeProto(t *testing.T) {
	cases := []struct {
		input MicroTime
	}{
		{MicroTime{}},
		{DateMicro(1998, time.May, 5, 1, 5, 5, 1000, time.Local)},
		{DateMicro(1998, time.May, 5, 5, 5, 5, 0, time.Local)},
	}

	for _, c := range cases {
		input := c.input
		data, err := input.Marshal()
		if err != nil {
			t.Fatalf("Failed to marshal input: '%v': %v", input, err)
		}
		time := MicroTime{}
		if err := time.Unmarshal(data); err != nil {
			t.Fatalf("Failed to unmarshal output: '%v': %v", input, err)
		}
		if !reflect.DeepEqual(input, time) {
			t.Errorf("Marshal->Unmarshal is not idempotent: '%v' vs '%v'", input, time)
		}
	}
}

// TestMicroTimeEqual 验证 Equal 对 nil/nil、同指针与 nil/非 nil 组合的语义。
func TestMicroTimeEqual(t *testing.T) {
	t1 := NewMicroTime(time.Now())
	cases := []struct {
		name   string
		x      *MicroTime
		y      *MicroTime
		result bool
	}{
		{"nil =? nil", nil, nil, true},
		{"!nil =? !nil", &t1, &t1, true},
		{"nil =? !nil", nil, &t1, false},
		{"!nil =? nil", &t1, nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.x.Equal(c.y)
			if result != c.result {
				t.Errorf("Failed equality test for '%v', '%v': expected %+v, got %+v", c.x, c.y, c.result, result)
			}
		})
	}
}

// TestMicroTimeEqualTime 验证 MicroTime 与 Time 跨类型比较的 nil 语义与相等判断。
func TestMicroTimeEqualTime(t *testing.T) {
	t1 := NewMicroTime(time.Now())
	t2 := NewTime(t1.Time)
	cases := []struct {
		name   string
		x      *MicroTime
		y      *Time
		result bool
	}{
		{"nil =? nil", nil, nil, true},
		{"!nil =? !nil", &t1, &t2, true},
		{"nil =? !nil", nil, &t2, false},
		{"!nil =? nil", &t1, nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.x.EqualTime(c.y)
			if result != c.result {
				t.Errorf("Failed equality test for '%v', '%v': expected %+v, got %+v", c.x, c.y, c.result, result)
			}
		})
	}
}

// TestMicroTimeBefore 验证 Before 在任一侧为 nil 或两者相等时均返回 false。
func TestMicroTimeBefore(t *testing.T) {
	t1 := NewMicroTime(time.Now())
	cases := []struct {
		name string
		x    *MicroTime
		y    *MicroTime
	}{
		{"nil <? nil", nil, nil},
		{"!nil <? !nil", &t1, &t1},
		{"nil <? !nil", nil, &t1},
		{"!nil <? nil", &t1, nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.x.Before(c.y)
			if result {
				t.Errorf("Failed before test for '%v', '%v': expected false, got %+v", c.x, c.y, result)
			}
		})
	}
}

// TestMicroTimeBeforeTime 验证 MicroTime 与 Time 跨类型 Before 比较的 nil 语义。
func TestMicroTimeBeforeTime(t *testing.T) {
	t1 := NewMicroTime(time.Now())
	t2 := NewTime(t1.Time)
	cases := []struct {
		name string
		x    *MicroTime
		y    *Time
	}{
		{"nil <? nil", nil, nil},
		{"!nil <? !nil", &t1, &t2},
		{"nil <? !nil", nil, &t2},
		{"!nil <? nil", &t1, nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.x.BeforeTime(c.y)
			if result {
				t.Errorf("Failed before test for '%v', '%v': expected false, got %+v", c.x, c.y, result)
			}
		})
	}
}

// TestMicroTimeIsZero 验证 nil 与零时间都视为 zero，非零时间返回 false。
func TestMicroTimeIsZero(t *testing.T) {
	t1 := NewMicroTime(time.Now())
	cases := []struct {
		name   string
		x      *MicroTime
		result bool
	}{
		{"nil =? 0", nil, true},
		{"!nil =? 0", &t1, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.x.IsZero()
			if result != c.result {
				t.Errorf("Failed equality test for '%v': expected %+v, got %+v", c.x, c.result, result)
			}
		})
	}
}

// TestMicroTimeUnmarshalJSONAndProtoEqual 验证同一 MicroTime 经 JSON 与 proto 两条路径
// 编解码后结果相等，包括纳秒被截断到微秒的情形。
func TestMicroTimeUnmarshalJSONAndProtoEqual(t *testing.T) {
	cases := []struct {
		name   string
		input  MicroTime
		result bool
	}{
		{"nanosecond level precision", UnixMicro(123, 123123123), true},
		{"microsecond level precision", UnixMicro(123, 123123000), true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			jsonData, err := c.input.MarshalJSON()
			if err != nil {
				t.Fatalf("Failed to marshal input to JSON: '%v': %v", c.input, err)
			}

			protoData, err := c.input.Marshal()
			if err != nil {
				t.Fatalf("Failed to marshal input to proto: '%v': %v", c.input, err)
			}

			var tJSON, tProto MicroTime
			if err = tJSON.UnmarshalJSON(jsonData); err != nil {
				t.Fatalf("Failed to unmarshal JSON: '%v': %v", jsonData, err)
			}
			if err = tProto.Unmarshal(protoData); err != nil {
				t.Fatalf("Failed to unmarshal proto: '%v': %v", protoData, err)
			}

			result := tJSON.Equal(&tProto)
			if result != c.result {
				t.Errorf("Failed equality test for '%v': expected %+v, got %+v", c.input, c.result, result)
			}
		})
	}
}

// TestMicroTimeProtoUnmarshalRaw 用预先生成的 protobuf Timestamp 字节验证解码，
// 包括纳秒精度输入被截断为微秒的情况。
func TestMicroTimeProtoUnmarshalRaw(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		expected MicroTime
	}{
		// 输入由 Timestamp{123, 123123123}.Marshal() 生成。
		{"nanosecond level precision", []byte{8, 123, 16, 179, 235, 218, 58}, UnixMicro(123, 123123000)},
		// 输入由 Timestamp{123, 123123000}.Marshal() 生成。
		{"microsecond level precision", []byte{8, 123, 16, 184, 234, 218, 58}, UnixMicro(123, 123123000)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var actual MicroTime
			if err := actual.Unmarshal(c.input); err != nil {
				t.Fatalf("Failed to unmarshal proto: '%v': %v", c.input, err)
			}

			if !actual.Equal(&c.expected) {
				t.Errorf("Failed unmarshal from nanosecond-precise raw for '%v': expected %+v, got %+v", c.input, c.expected, actual)
			}
		})
	}

}

// TestMicroTimeRoundtripCBOR 用随机值做 CBOR 往返，验证编码/解码保持相等；
// 失败时输出 CBOR 诊断编码以便定位问题。
func TestMicroTimeRoundtripCBOR(t *testing.T) {
	fuzzer := randfill.New()
	for i := 0; i < 500; i++ {
		var initial, final MicroTime
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
		if !final.Equal(&initial) {
			diag, err := cbor.Diagnose(b)
			if err != nil {
				t.Logf("failed to produce diagnostic encoding of 0x%x: %v", b, err)
			}
			t.Errorf("expected equal: %v, %v (cbor was '%s')", initial, final, diag)
		}
	}
}
