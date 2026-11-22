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

// testMember 是一个空结构，用于配合 mock extractor 只测试 Union 逻辑本身。
type testMember struct{}

// TestUnion 验证非判别式 union 的约束：恰好设置一个成员。
// 每个用例都分别跑指针和值两种泛型实例，确认两条实现路径一致。
func TestUnion(t *testing.T) {
	testCases := []struct {
		name        string
		fields      []string
		fieldValues []bool
		expected    field.ErrorList
	}{
		{
			// 恰好一个成员被设置，合法。
			name:        "one member set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, false, false, true},
			expected:    nil,
		},
		{
			// 多个成员同时设置：错误信息列出所有已设置的字段名。
			name:        "two members set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, true, false, true},
			expected:    field.ErrorList{field.Invalid(nil, "{b, d}", "must specify exactly one of: `a`, `b`, `c`, `d`")}.WithOrigin("union"),
		},
		{
			name:        "all members set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{true, true, true, true},
			expected:    field.ErrorList{field.Invalid(nil, "{a, b, c, d}", "must specify exactly one of: `a`, `b`, `c`, `d`")}.WithOrigin("union"),
		},
		{
			// 一个都不设置：错误信息是 "must specify one of"，与"多选"的措辞不同。
			name:        "no member set",
			fields:      []string{"a", "b", "c", "d"},
			fieldValues: []bool{false, false, false, false},
			expected:    field.ErrorList{field.Invalid(nil, "", "must specify one of: `a`, `b`, `c`, `d`")}.WithOrigin("union"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			members := []UnionMember{}
			for _, f := range tc.fields {
				members = append(members, NewUnionMember(f))
			}

			t.Run("pointer", func(t *testing.T) {
				// Create mock extractors that return predefined values instead of
				// actually extracting from the object.
				// 依赖 Go 1.22 起 range 变量按迭代创建，闭包各自捕获自己的 val。
				extractors := make([]ExtractorFn[*testMember, bool], len(tc.fieldValues))
				for i, val := range tc.fieldValues {
					extractors[i] = func(_ *testMember) bool { return val }
				}

				got := Union(context.Background(), operation.Operation{}, nil, &testMember{}, nil,
					NewUnionMembership(members...), extractors...)
				if !reflect.DeepEqual(got, tc.expected) {
					t.Errorf("got %v want %v", got, tc.expected)
				}
			})
			t.Run("value", func(t *testing.T) {
				// Create mock extractors that return predefined values instead of
				// actually extracting from the object.
				extractors := make([]ExtractorFn[testMember, bool], len(tc.fieldValues))
				for i, val := range tc.fieldValues {
					extractors[i] = func(_ testMember) bool { return val }
				}

				got := Union(context.Background(), operation.Operation{}, nil, testMember{}, testMember{},
					NewUnionMembership(members...), extractors...)
				if !reflect.DeepEqual(got, tc.expected) {
					t.Errorf("got %v want %v", got, tc.expected)
				}
			})
		})
	}
}

// TestDiscriminatedUnion 验证判别式 union：由 discriminator 字段决定唯一允许的成员，
// 其他成员一旦设置即报错；discriminator 指向的成员缺失也会报错。
func TestDiscriminatedUnion(t *testing.T) {
	testCases := []struct {
		name               string
		discriminatorField string
		fields             [][2]string
		discriminatorValue string
		fieldValues        []bool
		expected           field.ErrorList
	}{
		{
			name:               "valid discriminated union A",
			discriminatorField: "d",
			fields:             [][2]string{{"a", "A"}, {"b", "B"}, {"c", "C"}, {"d", "D"}},
			discriminatorValue: "A",
			fieldValues:        []bool{true, false, false, false},
		},
		{
			name:               "valid discriminated union C",
			discriminatorField: "d",
			fields:             [][2]string{{"a", "A"}, {"b", "B"}, {"c", "C"}, {"d", "D"}},
			discriminatorValue: "C",
			fieldValues:        []bool{false, false, true, false},
		},
		{
			// discriminator 为 C，但设置的是 b：既报 b 不该设置，也报 c 必须设置。
			name:               "invalid, discriminator not set to member that is specified",
			discriminatorField: "type",
			fields:             [][2]string{{"a", "A"}, {"b", "B"}, {"c", "C"}, {"d", "D"}},
			discriminatorValue: "C",
			fieldValues:        []bool{false, true, false, false},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("b"), "", "may only be specified when `type` is \"B\""),
				field.Invalid(field.NewPath("c"), "", "must be specified when `type` is \"C\""),
			}.WithOrigin("union"),
		},
		{
			// discriminator 本身正确（C），但 b、d 也被设置；c 合法所以不报 c。
			name:               "invalid, discriminator correct, multiple members set",
			discriminatorField: "type",
			fields:             [][2]string{{"a", "A"}, {"b", "B"}, {"c", "C"}, {"d", "D"}},
			discriminatorValue: "C",
			fieldValues:        []bool{false, true, true, true},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("b"), "", "may only be specified when `type` is \"B\""),
				field.Invalid(field.NewPath("d"), "", "may only be specified when `type` is \"D\""),
			}.WithOrigin("union"),
		},
	}

	for _, tc := range testCases {
		members := []UnionMember{}
		for _, f := range tc.fields {
			members = append(members, NewDiscriminatedUnionMember(f[0], f[1]))
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Run("pointer", func(t *testing.T) {
				discriminatorExtractor := func(_ *testMember) string { return tc.discriminatorValue }

				// Create mock extractors that return predefined values instead of
				// actually extracting from the object.
				extractors := make([]ExtractorFn[*testMember, bool], len(tc.fieldValues))
				for i, val := range tc.fieldValues {
					extractors[i] = func(_ *testMember) bool { return val }
				}

				got := DiscriminatedUnion(context.Background(), operation.Operation{}, nil, &testMember{}, nil,
					NewDiscriminatedUnionMembership(tc.discriminatorField, members...), discriminatorExtractor, extractors...)
				if !reflect.DeepEqual(got, tc.expected) {
					t.Errorf("got %v want %v", got.ToAggregate(), tc.expected.ToAggregate())
				}
			})
			t.Run("value", func(t *testing.T) {
				discriminatorExtractor := func(_ testMember) string { return tc.discriminatorValue }

				// Create mock extractors that return predefined values instead of
				// actually extracting from the object.
				extractors := make([]ExtractorFn[testMember, bool], len(tc.fieldValues))
				for i, val := range tc.fieldValues {
					extractors[i] = func(_ testMember) bool { return val }
				}

				got := DiscriminatedUnion(context.Background(), operation.Operation{}, nil, testMember{}, testMember{},
					NewDiscriminatedUnionMembership(tc.discriminatorField, members...), discriminatorExtractor, extractors...)
				if !reflect.DeepEqual(got, tc.expected) {
					t.Errorf("got %v want %v", got.ToAggregate(), tc.expected.ToAggregate())
				}
			})
		})
	}
}

// testStruct 用四种不同形状的成员（指针、指针、切片、map）覆盖 extractor 的
// "是否设置"判定：长度为零的切片/map 视为未设置。
type testStruct struct {
	M1 *m1               `json:"m1"`
	M2 *m2               `json:"m2"`
	M3 []string          `json:"m3"`
	M4 map[string]string `json:"m4"`
}

type m1 struct{}
type m2 struct{}

var extractors = []ExtractorFn[*testStruct, bool]{
	func(s *testStruct) bool {
		if s == nil {
			return false
		}
		return s.M1 != nil
	},
	func(s *testStruct) bool {
		if s == nil {
			return false
		}
		return s.M2 != nil
	},
	func(s *testStruct) bool {
		if s == nil {
			return false
		}
		return len(s.M3) != 0
	},
	func(s *testStruct) bool {
		if s == nil {
			return false
		}
		return len(s.M4) != 0
	},
}

// TestUnionRatcheting 验证 update 时的 ratcheting：
// 旧对象已存在时，原本就违反 union 约束的字段不会被重新判为错误；
// 只有在新旧状态之间"变成非法"才会报错。
func TestUnionRatcheting(t *testing.T) {
	testCases := []struct {
		name      string
		oldStruct *testStruct
		newStruct *testStruct
		expected  field.ErrorList
	}{
		{
			// 旧对象为 nil 无法 ratchet，空对象依旧违反"恰好一个"。
			name:      "old nil - no ratcheting",
			oldStruct: nil,
			newStruct: &testStruct{},
			expected: field.ErrorList{
				field.Invalid(nil, "", "must specify one of: `m1`, `m2`, `m3`, `m4`"),
			}.WithOrigin("union"),
		},
		{
			name:      "both empty struct",
			oldStruct: &testStruct{},
			newStruct: &testStruct{},
		},
		{
			// 旧状态本来就多选，保持多选不算新错误。
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
			// 从合法（单成员）变为非法（多成员），必须报错。
			name: "change to invalid",
			oldStruct: &testStruct{
				M1: &m1{},
			},
			newStruct: &testStruct{
				M1: &m1{},
				M2: &m2{},
			},
			expected: field.ErrorList{
				field.Invalid(nil, "{m1, m2}", "must specify exactly one of: `m1`, `m2`, `m3`, `m4`"),
			}.WithOrigin("union"),
		},
		{
			// 切片内容变化但"是否设置"不变（都非空），不算新的 union 违规。
			name:      "slice member ratcheting: unchanged membership",
			oldStruct: &testStruct{M3: []string{"a"}},
			newStruct: &testStruct{M3: []string{"b"}},
		},
		{
			name:      "map member ratcheting: unchanged membership",
			oldStruct: &testStruct{M4: map[string]string{"k": "v1"}},
			newStruct: &testStruct{M4: map[string]string{"k": "v2"}},
		},
		{
			// 空切片不算"已设置"，因此仍然是"一个都没选"。
			name:      "empty slice is not set",
			oldStruct: nil,
			newStruct: &testStruct{M3: []string{}},
			expected: field.ErrorList{
				field.Invalid(nil, "", "must specify one of: `m1`, `m2`, `m3`, `m4`"),
			}.WithOrigin("union"),
		},
		{
			// 空 map 与空切片同理。
			name:      "empty map is not set",
			oldStruct: nil,
			newStruct: &testStruct{M4: map[string]string{}},
			expected: field.ErrorList{
				field.Invalid(nil, "", "must specify one of: `m1`, `m2`, `m3`, `m4`"),
			}.WithOrigin("union"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			members := []UnionMember{NewUnionMember("m1"), NewUnionMember("m2"), NewUnionMember("m3"), NewUnionMember("m4")}
			got := Union(context.Background(), operation.Operation{Type: operation.Update}, nil, tc.newStruct, tc.oldStruct,
				NewUnionMembership(members...), extractors...)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}

// testDiscriminatedStruct 在 testStruct 基础上增加判别字段 D，
// 成员名与判别值一一对应，方便复用同一组 extractor 语义。
type testDiscriminatedStruct struct {
	D  string            `json:"d"`
	M1 *m1               `json:"m1"`
	M2 *m2               `json:"m2"`
	M3 []string          `json:"m3"`
	M4 map[string]string `json:"m4"`
}

var testDiscriminatorExtractor = func(s *testDiscriminatedStruct) string {
	if s != nil {
		return s.D
	}
	return ""
}
var testDiscriminatedExtractors = []ExtractorFn[*testDiscriminatedStruct, bool]{
	func(s *testDiscriminatedStruct) bool {
		if s == nil {
			return false
		}
		return s.M1 != nil
	},
	func(s *testDiscriminatedStruct) bool {
		if s == nil {
			return false
		}
		return s.M2 != nil
	},
	func(s *testDiscriminatedStruct) bool {
		if s == nil {
			return false
		}
		return len(s.M3) != 0
	},
	func(s *testDiscriminatedStruct) bool {
		if s == nil {
			return false
		}
		return len(s.M4) != 0
	},
}

// TestDiscriminatedUnionRatcheting 覆盖判别式 union 的 ratcheting：
// 旧对象已存在但已违规时保持违规状态可通过；改变判别值导致新的不匹配则报错。
func TestDiscriminatedUnionRatcheting(t *testing.T) {
	testCases := []struct {
		name      string
		oldStruct *testDiscriminatedStruct
		newStruct *testDiscriminatedStruct
		expected  field.ErrorList
	}{
		{
			name: "pass with both nil",
		},
		{
			name:      "pass with both empty struct",
			oldStruct: &testDiscriminatedStruct{},
			newStruct: &testDiscriminatedStruct{},
		},
		{
			// 旧状态已违规（D=m1 却设置了 m2），保持不变不算新错误。
			name: "pass with both not set to member that is specified",
			oldStruct: &testDiscriminatedStruct{
				D:  "m1",
				M2: &m2{},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m1",
				M2: &m2{},
			},
		},
		{
			name: "pass with both set to more than one member",
			oldStruct: &testDiscriminatedStruct{
				D:  "m1",
				M1: &m1{},
				M2: &m2{},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m1",
				M1: &m1{},
				M2: &m2{},
			},
		},
		{
			// 判别值 m1 合法，新增的 m2 是新违规，必须报错。
			name: "fail on changing to invalid with both set",
			oldStruct: &testDiscriminatedStruct{
				D:  "m1",
				M1: &m1{},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m1",
				M1: &m1{},
				M2: &m2{},
			},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("m2"), "", "may only be specified when `d` is \"m2\""),
			}.WithOrigin("union"),
		},
		{
			// 判别值从 m1 改为 m2：旧成员 m1 变成多余，新成员 m2 又缺失，两处都要报。
			name: "fail on changing the discriminator",
			oldStruct: &testDiscriminatedStruct{
				D:  "m1",
				M1: &m1{},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m2",
				M1: &m1{},
			},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("m1"), "", "may only be specified when `d` is \"m1\""),
				field.Invalid(field.NewPath("m2"), "", "must be specified when `d` is \"m2\""),
			}.WithOrigin("union"),
		},
		{
			name: "slice member ratcheting: unchanged membership",
			oldStruct: &testDiscriminatedStruct{
				D:  "m3",
				M3: []string{"a"},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m3",
				M3: []string{"b"},
			},
		},
		{
			name: "map member ratcheting: unchanged membership",
			oldStruct: &testDiscriminatedStruct{
				D:  "m4",
				M4: map[string]string{"k": "v1"},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m4",
				M4: map[string]string{"k": "v2"},
			},
		},
		{
			// 从"已设置"变为空切片：设置状态丢失，判为必须设置。
			name: "empty slice is not set",
			oldStruct: &testDiscriminatedStruct{
				D:  "m3",
				M3: []string{"a"},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m3",
				M3: []string{},
			},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("m3"), "", "must be specified when `d` is \"m3\""),
			}.WithOrigin("union"),
		},
		{
			// 同上，map 版本。
			name: "empty map is not set",
			oldStruct: &testDiscriminatedStruct{
				D:  "m4",
				M4: map[string]string{"k": "v"},
			},
			newStruct: &testDiscriminatedStruct{
				D:  "m4",
				M4: map[string]string{},
			},
			expected: field.ErrorList{
				field.Invalid(field.NewPath("m4"), "", "must be specified when `d` is \"m4\""),
			}.WithOrigin("union"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			members := []UnionMember{NewDiscriminatedUnionMember("m1", "m1"), NewDiscriminatedUnionMember("m2", "m2"), NewDiscriminatedUnionMember("m3", "m3"), NewDiscriminatedUnionMember("m4", "m4")}
			got := DiscriminatedUnion(context.Background(), operation.Operation{Type: operation.Update}, nil, tc.newStruct, tc.oldStruct,
				NewDiscriminatedUnionMembership("d", members...), testDiscriminatorExtractor, testDiscriminatedExtractors...)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}
