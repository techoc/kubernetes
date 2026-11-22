/*
Copyright 2017 The Kubernetes Authors.

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

package v1beta1

import (
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
)

// TestJSONConversion 验证内部 JSONSchemaProps 的 Default 转换到 v1beta1 后的原始 JSON。
func TestJSONConversion(t *testing.T) {
	nilJSON := apiextensions.JSON(nil)
	nullJSON := apiextensions.JSON("null")
	stringJSON := apiextensions.JSON("foo")
	boolJSON := apiextensions.JSON(true)
	sliceJSON := apiextensions.JSON([]string{"foo", "bar", "baz"})

	testCases := map[string]struct {
		input    *apiextensions.JSONSchemaProps
		expected *JSONSchemaProps
	}{
		"nil": {
			input: &apiextensions.JSONSchemaProps{
				Default: nil,
			},
			expected: &JSONSchemaProps{},
		},
		"aliased nil": {
			input: &apiextensions.JSONSchemaProps{
				Default: &nilJSON,
			},
			expected: &JSONSchemaProps{},
		},
		"null": {
			input: &apiextensions.JSONSchemaProps{
				Default: &nullJSON,
			},
			expected: &JSONSchemaProps{
				Default: &JSON{
					Raw: []byte(`"null"`),
				},
			},
		},
		"string": {
			input: &apiextensions.JSONSchemaProps{
				Default: &stringJSON,
			},
			expected: &JSONSchemaProps{
				Default: &JSON{
					Raw: []byte(`"foo"`),
				},
			},
		},
		"bool": {
			input: &apiextensions.JSONSchemaProps{
				Default: &boolJSON,
			},
			expected: &JSONSchemaProps{
				Default: &JSON{
					Raw: []byte(`true`),
				},
			},
		},
		"slice": {
			input: &apiextensions.JSONSchemaProps{
				Default: &sliceJSON,
			},
			expected: &JSONSchemaProps{
				Default: &JSON{
					Raw: []byte(`["foo","bar","baz"]`),
				},
			},
		},
	}

	scheme := runtime.NewScheme()

	// 注册内部与外部类型，以便 scheme 执行转换
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	for k, tc := range testCases {
		external := &JSONSchemaProps{}
		if err := scheme.Convert(tc.input, external, nil); err != nil {
			t.Errorf("%s: unexpected error: %v", k, err)
		}

		if !reflect.DeepEqual(external, tc.expected) {
			t.Errorf("%s: expected\n\t%#v, got \n\t%#v", k, tc.expected, external)
		}
	}
}

// TestJSONRoundTrip 验证含 null 的 JSON 在 v1beta1 → 内部 → v1beta1 往返后与预期一致。
func TestJSONRoundTrip(t *testing.T) {
	testcases := []struct {
		name string
		in   string
		out  string
	}{
		{
			name: "nulls",
			in:   `{"default":null,"enum":null,"example":null}`,
			out:  `{}`,
		},
		{
			name: "null values",
			in:   `{"default":{"test":null},"enum":[null],"example":{"test":null}}`,
			out:  `{"default":{"test":null},"enum":[null],"example":{"test":null}}`,
		},
	}

	scheme := runtime.NewScheme()
	// 注册内部与外部类型，以便 scheme 执行转换
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			external := &JSONSchemaProps{}
			if err := json.Unmarshal([]byte(tc.in), external); err != nil {
				t.Fatal(err)
			}

			internal := &apiextensions.JSONSchemaProps{}
			if err := scheme.Convert(external, internal, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			roundtripped := &JSONSchemaProps{}
			if err := scheme.Convert(internal, roundtripped, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out, err := json.Marshal(roundtripped)
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != string(tc.out) {
				t.Fatalf("expected\n%s\ngot\n%s", string(tc.out), string(out))
			}
		})
	}
}
