/*
Copyright 2020 The Kubernetes Authors.

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

package defaulting

import (
	"bytes"
	"reflect"
	"testing"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/util/json"
)

// TestPruneNonNullableNullsWithoutDefaults 验证：没有默认值且不可为空的字段，
// 其显式 null 会被删除；可为空或有默认值的字段则保留。
func TestPruneNonNullableNullsWithoutDefaults(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		schema   *structuralschema.Structural
		expected string
	}{
		{"empty", "null", nil, "null"},
		{"scalar", "4", &structuralschema.Structural{
			Generic: structuralschema.Generic{
				Default: structuralschema.JSON{Object: "foo"},
			},
		}, "4"},
		{"scalar array", "[1,null]", nil, "[1,null]"},
		{"object array", `[{"a":null},{"b":null},{"c":null},{"d":null},{"e":null}]`, &structuralschema.Structural{
			Items: &structuralschema.Structural{
				Properties: map[string]structuralschema.Structural{
					"a": {
						Generic: structuralschema.Generic{
							Default: structuralschema.JSON{Object: "A"},
						},
					},
					"b": {
						Generic: structuralschema.Generic{
							Nullable: true,
						},
					},
					"c": {
						Generic: structuralschema.Generic{
							Default:  structuralschema.JSON{Object: "C"},
							Nullable: true,
						},
					},
					"d": {
						Generic: structuralschema.Generic{},
					},
				},
			},
		}, `[{"a":null},{"b":null},{"c":null},{},{"e":null}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 输入输出都走 JSON 往返，保证比较的是反序列化后的等价对象而非字符串。
			var in interface{}
			if err := json.Unmarshal([]byte(tt.json), &in); err != nil {
				t.Fatal(err)
			}

			var expected interface{}
			if err := json.Unmarshal([]byte(tt.expected), &expected); err != nil {
				t.Fatal(err)
			}

			PruneNonNullableNullsWithoutDefaults(in, tt.schema)
			if !reflect.DeepEqual(in, expected) {
				// 仅在失败时做一次带缩进的序列化，方便定位差异。
				var buf bytes.Buffer
				enc := json.NewEncoder(&buf)
				enc.SetIndent("", "  ")
				err := enc.Encode(in)
				if err != nil {
					t.Fatalf("unexpected result mashalling error: %v", err)
				}
				t.Errorf("expected: %s\ngot: %s", tt.expected, buf.String())
			}
		})
	}
}
