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

package v1

import (
	gojson "encoding/json"
	"reflect"
	"testing"

	utiljson "k8s.io/apimachinery/pkg/util/json"
)

// GroupVersionHolder 把 GroupVersion 包在结构体字段里，
// 以便在 JSON 编解码时走 GroupVersion 自定义的 Marshal/Unmarshal 方法，
// 而不是把 GroupVersion 当作普通结构体展开。
type GroupVersionHolder struct {
	GV GroupVersion `json:"val"`
}

// TestGroupVersionUnmarshalJSON 验证 GroupVersion 能从 "group/version" 与纯 "version"
// 两种字符串形式反序列化，且标准库 encoding/json 与 util/json 两个解码器结果一致。
//
// util/json 基于 jsoniter，是 apiserver 实际使用的解码器，两者行为必须对齐，
// 否则不同代码路径会得到不同的 GroupVersion。
func TestGroupVersionUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  []byte
		expect GroupVersion
	}{
		// 无组名时 group 为空字符串，仅版本号。
		{[]byte(`{"val": "v1"}`), GroupVersion{"", "v1"}},
		{[]byte(`{"val": "apps/v1"}`), GroupVersion{"apps", "v1"}},
	}

	for _, c := range cases {
		var result GroupVersionHolder
		// 先用标准库解码，作为基准语义。
		if err := gojson.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("JSON codec failed to unmarshal input '%v': %v", c.input, err)
		}
		if !reflect.DeepEqual(result.GV, c.expect) {
			t.Errorf("JSON codec failed to unmarshal input '%s': expected %+v, got %+v", c.input, c.expect, result.GV)
		}
		// 再用 util/json 解码，确保高性能解码器与标准库行为一致。
		if err := utiljson.Unmarshal(c.input, &result); err != nil {
			t.Errorf("util/json codec failed to unmarshal input '%v': %v", c.input, err)
		}
		if !reflect.DeepEqual(result.GV, c.expect) {
			t.Errorf("util/json codec failed to unmarshal input '%s': expected %+v, got %+v", c.input, c.expect, result.GV)
		}
	}
}

// TestGroupVersionMarshalJSON 验证 GroupVersion 序列化为 "group/version" 字符串，
// 且无组名时只输出版本号，不产生前导斜杠。
func TestGroupVersionMarshalJSON(t *testing.T) {
	cases := []struct {
		input  GroupVersion
		expect []byte
	}{
		{GroupVersion{"", "v1"}, []byte(`{"val":"v1"}`)},
		{GroupVersion{"apps", "v1"}, []byte(`{"val":"apps/v1"}`)},
	}

	for _, c := range cases {
		input := GroupVersionHolder{c.input}
		result, err := gojson.Marshal(&input)
		if err != nil {
			t.Errorf("Failed to marshal input '%v': %v", input, err)
		}
		if !reflect.DeepEqual(result, c.expect) {
			t.Errorf("Failed to marshal input '%+v': expected: %s, got: %s", input, c.expect, result)
		}
	}
}
