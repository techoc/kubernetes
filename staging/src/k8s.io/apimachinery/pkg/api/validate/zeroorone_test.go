/*
Copyright 2024 The Kubernetes Authors.

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

package validate

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestZeroOrOneOfUnion 覆盖 ZeroOrOneOfUnion 的成员计数逻辑：
// 0 个或 1 个成员合法，2 个及以上报错，错误中列出已设置与全部成员。
func TestZeroOrOneOfUnion(t *testing.T) {
	testCases := []struct {
		name        string
		fields      []string
		fieldValues []bool
		expected    field.ErrorList
	}{
		{
			name:        "one member set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, false, false, true},
			expected:    nil,
		},
		{
			name:        "two members set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, true, false, true},
			expected:    field.ErrorList{field.Invalid(nil, "{b, d}", "must specify at most one of: `a`, `b`, `c`, `d`").WithOrigin("zeroOrOneOf")},
		},
		{
			name:        "all members set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{true, true, true, true},
			expected:    field.ErrorList{field.Invalid(nil, "{a, b, c, d}", "must specify at most one of: `a`, `b`, `c`, `d`").WithOrigin("zeroOrOneOf")},
		},
		{
			name:        "no member set - allowed for ZeroOrOneOf",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, false, false, false},
			expected:    nil, // 与 OneOf 的关键区别：零个成员是允许的。
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			members := []UnionMember{}
			for _, f := range tc.fields {
				members = append(members, NewUnionMember(f))
			}

			// 构造返回预置布尔值的 mock extractor，避免真正从对象中取值。
			extractors := make([]ExtractorFn[*testMember, bool], len(tc.fieldValues))
			for i, val := range tc.fieldValues {
				extractors[i] = func(_ *testMember) bool { return val }
			}

			got := ZeroOrOneOfUnion(context.Background(), operation.Operation{}, nil, &testMember{}, nil,
				NewUnionMembership(members...), extractors...)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}

// TestZeroOrOneOfUnionRatcheting 验证更新时的 ratcheting 行为：
// 若新旧对象都存在同样的多个成员（历史遗留非法数据），不重复报错；
// 只有从合法状态变为非法状态时才报错。
func TestZeroOrOneOfUnionRatcheting(t *testing.T) {
	testCases := []struct {
		name      string
		oldStruct *testStruct
		newStruct *testStruct
		expected  field.ErrorList
	}{
		{
			name:      "both nil",
			oldStruct: nil,
			newStruct: nil,
		},
		{
			name:      "both empty struct - allowed for ZeroOrOneOf",
			oldStruct: &testStruct{},
			newStruct: &testStruct{},
		},
		{
			// 旧值已非法时允许保持原样（ratcheting）。
			name: "both have more than one member",
			oldStruct: &testStruct{
				M1: &m1{},
				M2: &m2{},
			},
			newStruct: &testStruct{
				M1: &m1{},
				M2: &m2{},
			},
		},
		{
			// 从单个成员变为两个成员，必须报错。
			name: "change to invalid",
			oldStruct: &testStruct{
				M1: &m1{},
			},
			newStruct: &testStruct{
				M1: &m1{},
				M2: &m2{},
			},
			expected: field.ErrorList{
				field.Invalid(nil, "{m1, m2}", "must specify at most one of: `m1`, `m2`, `m3`, `m4`").WithOrigin("zeroOrOneOf"),
			},
		},
		{
			name:      "change from empty to one member - allowed",
			oldStruct: &testStruct{},
			newStruct: &testStruct{
				M1: &m1{},
			},
			expected: nil,
		},
		{
			name: "change from one member to empty - allowed",
			oldStruct: &testStruct{
				M1: &m1{},
			},
			newStruct: &testStruct{},
			expected:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			members := []UnionMember{NewUnionMember("m1"), NewUnionMember("m2"), NewUnionMember("m3"), NewUnionMember("m4")}
			got := ZeroOrOneOfUnion(context.Background(), operation.Operation{Type: operation.Update}, nil, tc.newStruct, tc.oldStruct,
				NewUnionMembership(members...), extractors...)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}
