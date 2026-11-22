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

// TimeHolder 是测试用的包装结构，让 Time 通过结构体字段参与 JSON/YAML 编解码。
type TimeHolder struct {
	T Time `json:"t"`
}

// TestTimeMarshalYAML 验证 Time 序列化为 YAML 时：零值输出 null，
// 非 UTC 时区的时间会转换为 UTC（RFC3339 格式）。
func TestTimeMarshalYAML(t *testing.T) {
	cases := []struct {
		input  Time
		result string
	}{
		{Time{}, "t: null\n"},
		{Date(1998, time.May, 5, 1, 5, 5, 50, time.FixedZone("test", -4*60*60)), "t: \"1998-05-05T05:05:05Z\"\n"},
		{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC), "t: \"1998-05-05T05:05:05Z\"\n"},
	}

	for _, c := range cases {
		input := TimeHolder{c.input}
		result, err := yaml.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: '%v': %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: '%v': expected %+v, got %q", input, c.result, string(result))
		}
	}
}

// TestTimeUnmarshalYAML 验证 YAML 中的 null 解析为零值 Time，
// 而带时区偏移的时间会被转换到本地时区存储（Time 内部保留 Location 信息）。
func TestTimeUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input  string
		result Time
	}{
		{"t: null\n", Time{}},
		{"t: 1998-05-05T05:05:05Z\n", Time{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},
	}

	for _, c := range cases {
		var result TimeHolder
		if err := yaml.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input '%v': %v", c.input, err)
		}
		if result.T != c.result {
			t.Errorf("Failed to unmarshal input '%v': expected %+v, got %+v", c.input, c.result, result)
		}
	}
}

// TestTimeMarshalJSON 验证 Time 的 JSON 输出为 RFC3339 UTC 字符串，
// 零值序列化为 null（与 omitempty 语义配合）。
func TestTimeMarshalJSON(t *testing.T) {
	cases := []struct {
		input  Time
		result string
	}{
		{Time{}, "{\"t\":null}"},
		{Date(1998, time.May, 5, 5, 5, 5, 50, time.UTC), "{\"t\":\"1998-05-05T05:05:05Z\"}"},
		{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC), "{\"t\":\"1998-05-05T05:05:05Z\"}"},
	}

	for _, c := range cases {
		input := TimeHolder{c.input}
		result, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: '%v': %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: '%v': expected %+v, got %q", input, c.result, string(result))
		}
	}
}

// TestTimeUnmarshalJSON 验证 JSON 解析保留纳秒精度（RFC3339Nano），
// 且解析结果转换为本地时区。
func TestTimeUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result Time
	}{
		{"{\"t\":null}", Time{}},
		{"{\"t\":\"1998-05-05T05:05:05Z\"}", Time{Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},
		{"{\"t\":\"1998-05-05T05:05:05.123456789Z\"}", Time{Date(1998, time.May, 5, 5, 5, 5, 123456789, time.UTC).Local()}},
	}

	for _, c := range cases {
		var result TimeHolder
		if err := json.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input '%v': %v", c.input, err)
		}
		if result.T != c.result {
			t.Errorf("Failed to unmarshal input '%v': expected %+v, got %+v", c.input, c.result, result)
		}
	}
}

// TestTimeMarshalJSONUnmarshalYAML 验证跨格式往返：JSON 序列化后再用 YAML 解析，
// 时区名称/偏移与 UnixNano 都应保持不变。
func TestTimeMarshalJSONUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input Time
	}{
		{Time{}},
		{Date(1998, time.May, 5, 5, 5, 5, 50, time.Local).Rfc3339Copy()},
		{Date(1998, time.May, 5, 5, 5, 5, 0, time.Local).Rfc3339Copy()},
	}

	for i, c := range cases {
		input := TimeHolder{c.input}
		jsonMarshalled, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("%d-1: Failed to marshal input: '%v': %v", i, input, err)
		}

		var result TimeHolder
		err = yaml.Unmarshal(jsonMarshalled, &result)
		if err != nil {
			t.Errorf("%d-2: Failed to unmarshal '%+v': %v", i, string(jsonMarshalled), err)
		}

		iN, iO := input.T.Zone()
		oN, oO := result.T.Zone()
		if iN != oN || iO != oO {
			t.Errorf("%d-3: Time zones differ before and after serialization %s:%d %s:%d", i, iN, iO, oN, oO)
		}

		if input.T.UnixNano() != result.T.UnixNano() {
			t.Errorf("%d-4: Failed to marshal input '%#v': got %#v", i, input, result)
		}
	}
}

// TestTimeMarshalCBOR 验证 CBOR 编码：零值为 null（0xf6），
// 非零值编码为 RFC3339 字符串且纳秒被截断。
func TestTimeMarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   Time
		out  []byte
	}{
		{name: "zero value", in: Time{}, out: []byte{0xf6}},                                                                                        // null
		{name: "no fractional seconds", in: Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC), out: []byte("\x541998-05-05T05:05:05Z")},                // '1998-05-05T05:05:05Z'
		{name: "fractional seconds truncated", in: Date(1998, time.May, 5, 5, 5, 5, 123456789, time.UTC), out: []byte("\x541998-05-05T05:05:05Z")}, // '1998-05-05T05:05:05Z'
		{name: "epoch", in: Time{Time: time.Unix(0, 0)}, out: []byte("\x541970-01-01T00:00:00Z")},                                                  // '1970-01-01T00:00:00Z'
		{name: "pre-epoch", in: Date(1960, time.January, 1, 0, 0, 0, 0, time.UTC), out: []byte("\x541960-01-01T00:00:00Z")},                        // '1960-01-01T00:00:00Z'
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

// TestTimeUnmarshalCBOR 验证 CBOR 解码：接受 null、秒级与纳秒级时间戳，
// 对错误类型与格式错误的时间字符串返回明确错误。
func TestTimeUnmarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		name       string
		in         []byte
		out        Time
		errMessage string
	}{
		{name: "null", in: []byte{0xf6}, out: Time{}}, // null
		{name: "no fractional seconds", in: []byte("\x58\x141998-05-05T05:05:05Z"), out: Time{Time: Date(1998, time.May, 5, 5, 5, 5, 0, time.UTC).Local()}},                    // '1998-05-05T05:05:05Z'
		{name: "fractional seconds", in: []byte("\x58\x1e1998-05-05T05:05:05.123456789Z"), out: Time{Time: Date(1998, time.May, 5, 5, 5, 5, 123456789, time.UTC).Local()}},     // '1998-05-05T05:05:05.123456789Z'
		{name: "invalid cbor type", in: []byte{0x07}, out: Time{}, errMessage: "cbor: cannot unmarshal positive integer into Go value of type string"},                         // 7
		{name: "malformed timestamp", in: []byte("\x45hello"), out: Time{}, errMessage: `parsing time "hello" as "2006-01-02T15:04:05Z07:00": cannot parse "hello" as "2006"`}, // 'hello'
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got Time
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

// TestTimeProto 验证 protobuf 编解码往返后与原始值完全一致（reflect.DeepEqual）。
func TestTimeProto(t *testing.T) {
	cases := []struct {
		input Time
	}{
		{Time{}},
		{Date(1998, time.May, 5, 1, 5, 5, 0, time.Local)},
		{Date(1998, time.May, 5, 5, 5, 5, 0, time.Local)},
	}

	for _, c := range cases {
		input := c.input
		data, err := input.Marshal()
		if err != nil {
			t.Fatalf("Failed to marshal input: '%v': %v", input, err)
		}
		time := Time{}
		if err := time.Unmarshal(data); err != nil {
			t.Fatalf("Failed to unmarshal output: '%v': %v", input, err)
		}
		if !reflect.DeepEqual(input, time) {
			t.Errorf("Marshal->Unmarshal is not idempotent: '%v' vs '%v'", input, time)
		}
	}
}

// TestTimeEqual 验证 nil 接收者与 nil 参数的安全比较：两个 nil 视为相等，
// nil 与非 nil 视为不等。
func TestTimeEqual(t *testing.T) {
	t1 := NewTime(time.Now())
	cases := []struct {
		name   string
		x      *Time
		y      *Time
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

// TestTimeBefore 验证 Before 对 nil 输入一律返回 false（包括同一时刻与 nil 组合），
// 避免调用方需要额外的 nil 判断。
func TestTimeBefore(t *testing.T) {
	t1 := NewTime(time.Now())
	cases := []struct {
		name string
		x    *Time
		y    *Time
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
				t.Errorf("Failed equality test for '%v', '%v': expected false, got %+v", c.x, c.y, result)
			}
		})
	}
}

// TestTimeIsZero 验证 IsZero：nil 接收者视为零值，非 nil 实例返回 false。
func TestTimeIsZero(t *testing.T) {
	t1 := NewTime(time.Now())
	cases := []struct {
		name   string
		x      *Time
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

// TestTimeRoundtripCBOR 用随机填充的 Time 做 500 次 CBOR 往返，
// 确保任意取值（含边界时间）解码后都等于原值。
func TestTimeRoundtripCBOR(t *testing.T) {
	fuzzer := randfill.New()
	for i := 0; i < 500; i++ {
		var initial, final Time
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
