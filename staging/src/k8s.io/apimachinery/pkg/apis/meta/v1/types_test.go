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
	gojson "encoding/json"
	"reflect"
	"testing"

	utiljson "k8s.io/apimachinery/pkg/util/json"
)

// TestVerbsMarshalJSON 验证 Verbs 的序列化行为：
// nil 与空切片必须区分（null 与 []），否则依赖字段是否存在判断的客户端会出错。
func TestVerbsMarshalJSON(t *testing.T) {
	cases := []struct {
		input  APIResource
		result string
	}{
		{APIResource{}, `{"name":"","singularName":"","namespaced":false,"kind":"","verbs":null}`},
		{APIResource{Verbs: Verbs([]string{})}, `{"name":"","singularName":"","namespaced":false,"kind":"","verbs":[]}`},
		{APIResource{Verbs: Verbs([]string{"delete"})}, `{"name":"","singularName":"","namespaced":false,"kind":"","verbs":["delete"]}`},
	}

	for i, c := range cases {
		result, err := gojson.Marshal(&c.input)
		if err != nil {
			t.Errorf("[%d] Failed to marshal input: '%v': %v", i, c.input, err)
		}
		if string(result) != c.result {
			t.Errorf("[%d] Failed to marshal input: '%v': expected '%v', got '%v'", i, c.input, c.result, string(result))
		}
	}
}

// TestVerbsJsonIterUnmarshalJSON 使用基于 jsoniter 的 util/json 反序列化，
// 确认 "verbs":null 与缺失字段得到相同的零值，而 "verbs":[] 得到非 nil 的空切片。
func TestVerbsJsonIterUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result APIResource
	}{
		{`{}`, APIResource{}},
		{`{"verbs":null}`, APIResource{}},
		{`{"verbs":[]}`, APIResource{Verbs: Verbs([]string{})}},
		{`{"verbs":["delete"]}`, APIResource{Verbs: Verbs([]string{"delete"})}},
	}

	for i, c := range cases {
		var result APIResource
		if err := utiljson.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("[%d] Failed to unmarshal input '%v': %v", i, c.input, err)
		}
		if !reflect.DeepEqual(result, c.result) {
			t.Errorf("[%d] Failed to unmarshal input '%v': expected %+v, got %+v", i, c.input, c.result, result)
		}
	}
}

// TestMarshalJSONWithOmit 确认带 omitempty 的切片字段在 nil 与空切片时都被省略，
// 防止历史回归：空切片曾被错误地序列化为 "matchExpressions":[]。
// 注意：包含一个零值元素的切片不是空切片，仍会被序列化。
func TestMarshalJSONWithOmit(t *testing.T) {
	cases := []struct {
		input  LabelSelector
		result string
	}{
		{LabelSelector{}, `{}`},
		{LabelSelector{MatchExpressions: []LabelSelectorRequirement{}}, `{}`},
		{LabelSelector{MatchExpressions: []LabelSelectorRequirement{{}}}, `{"matchExpressions":[{"key":"","operator":""}]}`},
	}

	for i, c := range cases {
		result, err := gojson.Marshal(&c.input)
		if err != nil {
			t.Errorf("[%d] Failed to marshal input: '%v': %v", i, c.input, err)
		}
		if string(result) != c.result {
			t.Errorf("[%d] Failed to marshal input: '%v': expected '%v', got '%v'", i, c.input, c.result, string(result))
		}
	}
}

// TestVerbsUnmarshalJSON 使用标准库 encoding/json 验证 Verbs 的反序列化，
// 与 jsoniter 版本保持一致的语义。
func TestVerbsUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		result APIResource
	}{
		{`{}`, APIResource{}},
		{`{"verbs":null}`, APIResource{}},
		{`{"verbs":[]}`, APIResource{Verbs: Verbs([]string{})}},
		{`{"verbs":["delete"]}`, APIResource{Verbs: Verbs([]string{"delete"})}},
	}

	for i, c := range cases {
		var result APIResource
		if err := gojson.Unmarshal([]byte(c.input), &result); err != nil {
			t.Errorf("[%d] Failed to unmarshal input '%v': %v", i, c.input, err)
		}
		if !reflect.DeepEqual(result, c.result) {
			t.Errorf("[%d] Failed to unmarshal input '%v': expected %+v, got %+v", i, c.input, c.result, result)
		}
	}
}

// TestVerbsProto 验证 APIResource 的 protobuf 往返（Marshal -> Unmarshal）保持幂等，
// 覆盖 nil、空切片与有值切片三种情况。
func TestVerbsProto(t *testing.T) {
	cases := []APIResource{
		{},
		{Verbs: Verbs([]string{})},
		{Verbs: Verbs([]string{"delete"})},
	}

	for _, input := range cases {
		data, err := input.Marshal()
		if err != nil {
			t.Fatalf("Failed to marshal input: '%v': %v", input, err)
		}
		resource := APIResource{}
		if err := resource.Unmarshal(data); err != nil {
			t.Fatalf("Failed to unmarshal output: '%v': %v", input, err)
		}
		if !reflect.DeepEqual(input, resource) {
			t.Errorf("Marshal->Unmarshal is not idempotent: '%v' vs '%v'", input, resource)
		}
	}
}
