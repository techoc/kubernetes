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
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

// DurationHolder 用结构体包裹 Duration，验证它作为字段时的编解码行为，
// 而不是依赖顶层值。
type DurationHolder struct {
	D Duration `json:"d"`
}

// TestDurationMarshalYAML 覆盖 Duration 在 YAML 中的规范输出形式：始终使用
// Go 的 duration 语法，零值单位会被省略（如 2m0s）。
func TestDurationMarshalYAML(t *testing.T) {
	cases := []struct {
		input  Duration
		result string
	}{
		{Duration{5 * time.Second}, "d: 5s\n"},
		{Duration{2 * time.Minute}, "d: 2m0s\n"},
		{Duration{time.Hour + 3*time.Millisecond}, "d: 1h0m0.003s\n"},
	}

	for _, c := range cases {
		input := DurationHolder{c.input}
		result, err := yaml.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: %q: %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: %q: expected %q, got %q", input, c.result, string(result))
		}
	}
}

func TestDurationUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input  string
		result Duration
	}{
		{"d: 0s\n", Duration{}},
		{"d: 5s\n", Duration{5 * time.Second}},
		{"d: 2m0s\n", Duration{2 * time.Minute}},
		{"d: 1h0m0.003s\n", Duration{time.Hour + 3*time.Millisecond}},

		// Units with zero values can optionally be dropped
		{"d: 2m\n", Duration{2 * time.Minute}},
		{"d: 1h0.003s\n", Duration{time.Hour + 3*time.Millisecond}},
	}

	// TestDurationUnmarshalYAML 验证解析既能接受规范形式，也能接受省略零值
	// 单位的写法（2m 等价于 2m0s）；后者是用户手写 YAML 时的常见形式，
	// 必须保持兼容。
	for _, c := range cases {
		var result DurationHolder
		if err := yaml.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input %q: %v", c.input, err)
		}
		if result.D != c.result {
			t.Errorf("Failed to unmarshal input %q: expected %q, got %q", c.input, c.result, result)
		}
	}
}

// TestDurationMarshalJSON 确认 JSON 输出与 YAML 一致，都是字符串形式的
// duration（而不是纳秒整数）。
func TestDurationMarshalJSON(t *testing.T) {
	cases := []struct {
		input  Duration
		result string
	}{
		{Duration{5 * time.Second}, `{"d":"5s"}`},
		{Duration{2 * time.Minute}, `{"d":"2m0s"}`},
		{Duration{time.Hour + 3*time.Millisecond}, `{"d":"1h0m0.003s"}`},
	}

	for _, c := range cases {
		input := DurationHolder{c.input}
		result, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input: %q: %v", input, err)
		}
		if string(result) != c.result {
			t.Errorf("Failed to marshal input: %q: expected %q, got %q", input, c.result, string(result))
		}
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result Duration
	}{
		{`{"d":"0s"}`, Duration{}},
		{`{"d":"5s"}`, Duration{5 * time.Second}},
		{`{"d":"2m0s"}`, Duration{2 * time.Minute}},
		{`{"d":"1h0m0.003s"}`, Duration{time.Hour + 3*time.Millisecond}},

		// Units with zero values can optionally be dropped
		{`{"d":"2m"}`, Duration{2 * time.Minute}},
		{`{"d":"1h0.003s"}`, Duration{time.Hour + 3*time.Millisecond}},
	}

	// TestDurationUnmarshalJSON 与 YAML 用例对应，额外确认省略零值单位的
	// 字符串同样可被解析。
	for _, c := range cases {
		var result DurationHolder
		if err := json.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("Failed to unmarshal input %q: %v", c.input, err)
		}
		if result.D != c.result {
			t.Errorf("Failed to unmarshal input %q: expected %q, got %q", c.input, c.result, result)
		}
	}
}

// TestDurationMarshalJSONUnmarshalYAML 验证 JSON 与 YAML 之间的互通：先用
// JSON 序列化再交给 YAML 解析，最终值必须与原始值相等。这保证两种格式
// 表达的是同一种 duration 语义。
func TestDurationMarshalJSONUnmarshalYAML(t *testing.T) {
	cases := []struct {
		input Duration
	}{
		{Duration{}},
		{Duration{5 * time.Second}},
		{Duration{2 * time.Minute}},
		{Duration{time.Hour + 3*time.Millisecond}},
	}

	for i, c := range cases {
		input := DurationHolder{c.input}
		jsonMarshalled, err := json.Marshal(&input)
		if err != nil {
			t.Errorf("%d-1: Failed to marshal input: '%v': %v", i, input, err)
		}

		var result DurationHolder
		if err := yaml.Unmarshal(jsonMarshalled, &result); err != nil {
			t.Errorf("%d-2: Failed to unmarshal '%+v': %v", i, string(jsonMarshalled), err)
		}

		if input.D != result.D {
			t.Errorf("%d-4: Failed to marshal input '%#v': got %#v", i, input, result)
		}
	}
}
