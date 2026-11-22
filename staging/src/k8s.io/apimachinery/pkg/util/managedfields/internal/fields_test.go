/*
Copyright 2018 The Kubernetes Authors.

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

package internal

import (
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// TestFieldsRoundTrip 验证 fields trie（FieldsV1）与 path set 之间可以无损往返。
func TestFieldsRoundTrip(t *testing.T) {
	// 一个非空 trie 与一个空 Fields 都参与往返。
	tests := []metav1.FieldsV1{
		*metav1.NewFieldsV1(`{"f:metadata":{".":{},"f:name":{}}}`),
		EmptyFields,
	}

	for _, test := range tests {
		set, err := FieldsToSet(test)
		if err != nil {
			t.Fatalf("Failed to create path set: %v", err)
		}
		output, err := SetToFields(set)
		if err != nil {
			t.Fatalf("Failed to create fields trie from path set: %v", err)
		}
		if !reflect.DeepEqual(test, output) {
			t.Fatalf("Expected round-trip:\ninput: %v\noutput: %v", test, output)
		}
	}
}

// TestFieldsToSetError 验证 FieldsToSet 会把非法的字段路径（这里是非法的
// k:{...} 键）作为错误返回，而不是静默忽略。
func TestFieldsToSetError(t *testing.T) {
	tests := []struct {
		fields    metav1.FieldsV1
		errString string
	}{
		{
			fields: *metav1.NewFieldsV1(
				`{"k:{invalid json}":{"f:name":{},".":{}}}`,
			),
			errString: "ReadObjectCB",
		},
	}

	for _, test := range tests {
		_, err := FieldsToSet(test.fields)
		if err == nil || !strings.Contains(err.Error(), test.errString) {
			t.Fatalf("Expected error to contain %q but got: %v", test.errString, err)
		}
	}
}

// TestSetToFieldsError 验证 SetToFields 遇到不含任何标识（既无字段名也无
// 键值）的非法 PathElement 时返回错误。
func TestSetToFieldsError(t *testing.T) {
	validName := "ok"
	invalidPath := fieldpath.Path([]fieldpath.PathElement{{}, {FieldName: &validName}})

	tests := []struct {
		set       fieldpath.Set
		errString string
	}{
		{
			set:       *fieldpath.NewSet(invalidPath),
			errString: "invalid PathElement",
		},
	}

	for _, test := range tests {
		_, err := SetToFields(test.set)
		if err == nil || !strings.Contains(err.Error(), test.errString) {
			t.Fatalf("Expected error to contain %q but got: %v", test.errString, err)
		}
	}
}

// BenchmarkSetToFields 测量把包含列表索引与键字段的 path set 转换为 fields trie 的开销。
func BenchmarkSetToFields(b *testing.B) {
	set := fieldpath.NewSet(
		fieldpath.MakePathOrDie("foo", 0, "bar", "baz"),
		fieldpath.MakePathOrDie("foo", 0, "bar", "zot"),
		fieldpath.MakePathOrDie("foo", 0, "bar"),
		fieldpath.MakePathOrDie("foo", 0),
		fieldpath.MakePathOrDie("foo", 1, "bar", "baz"),
		fieldpath.MakePathOrDie("foo", 1, "bar"),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "first")),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "first"), "bar"),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "second"), "bar"),
	)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := SetToFields(*set)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFieldsToSet 测量反向转换（fields trie -> path set）的开销。
// trie 的构造在计时开始前完成，只测量 FieldsToSet。
func BenchmarkFieldsToSet(b *testing.B) {
	set := fieldpath.NewSet(
		fieldpath.MakePathOrDie("foo", 0, "bar", "baz"),
		fieldpath.MakePathOrDie("foo", 0, "bar", "zot"),
		fieldpath.MakePathOrDie("foo", 0, "bar"),
		fieldpath.MakePathOrDie("foo", 0),
		fieldpath.MakePathOrDie("foo", 1, "bar", "baz"),
		fieldpath.MakePathOrDie("foo", 1, "bar"),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "first")),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "first"), "bar"),
		fieldpath.MakePathOrDie("qux", fieldpath.KeyByFields("name", "second"), "bar"),
	)
	fields, err := SetToFields(*set)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := FieldsToSet(fields)
		if err != nil {
			b.Fatal(err)
		}
	}
}
