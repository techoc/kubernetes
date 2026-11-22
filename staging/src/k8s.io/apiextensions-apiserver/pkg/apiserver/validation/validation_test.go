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

package validation

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	kjson "sigs.k8s.io/json"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/utils/ptr"

	kubeopenapispec "k8s.io/kube-openapi/pkg/validation/spec"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsfuzzer "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/fuzzer"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// TestRoundTrip 检查到 go-openapi 类型的转换：
// 内部 -> go-openapi -> JSON -> 外部 -> 内部。
func TestRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)

	// 将内部与外部类型注册到 scheme
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	seed := int64(time.Now().Nanosecond())
	// 可用 TEST_RAND_SEED 复现某次随机失败
	if override := os.Getenv("TEST_RAND_SEED"); len(override) > 0 {
		overrideSeed, err := strconv.Atoi(override)
		if err != nil {
			t.Fatal(err)
		}
		seed = int64(overrideSeed)
		t.Logf("using overridden seed: %d", seed)
	} else {
		t.Logf("seed (override with TEST_RAND_SEED if desired): %d", seed)
	}
	fuzzerFuncs := fuzzer.MergeFuzzerFuncs(apiextensionsfuzzer.Funcs)
	f := fuzzer.FuzzerFor(fuzzerFuncs, rand.NewSource(seed), codecs)

	for i := 0; i < 50; i++ {
		// 对内部类型做模糊填充
		internal := &apiextensions.JSONSchemaProps{}
		f.Fill(internal)

		// 内部 -> go-openapi
		openAPITypes := &kubeopenapispec.Schema{}
		if err := ConvertJSONSchemaProps(internal, openAPITypes); err != nil {
			t.Fatal(err)
		}

		// go-openapi -> JSON 序列化
		openAPIJSON, err := json.Marshal(openAPITypes)
		if err != nil {
			t.Fatal(err)
		}

		// JSON -> 内存中的 JSON => convertNullTypeToNullable => JSON
		var j interface{}
		if strictErrs, err := kjson.UnmarshalStrict(openAPIJSON, &j); err != nil {
			t.Fatal(err)
		} else if len(strictErrs) > 0 {
			t.Fatal(strictErrs)
		}
		j = stripIntOrStringType(j)
		openAPIJSON, err = json.Marshal(j)
		if err != nil {
			t.Fatal(err)
		}

		// JSON -> 外部
		external := &apiextensionsv1.JSONSchemaProps{}
		if strictErrs, err := kjson.UnmarshalStrict(openAPIJSON, external); err != nil {
			t.Fatal(err)
		} else if len(strictErrs) > 0 {
			t.Fatal(strictErrs)
		}

		// 外部 -> 内部
		internalRoundTripped := &apiextensions.JSONSchemaProps{}
		if err := scheme.Convert(external, internalRoundTripped, nil); err != nil {
			t.Fatal(err)
		}

		if !apiequality.Semantic.DeepEqual(internal, internalRoundTripped) {
			t.Log(string(openAPIJSON))
			t.Fatalf("%d: unexpected diff\n\t%s", i, cmp.Diff(internal, internalRoundTripped))
		}
	}
}

// stripIntOrStringType 移除 XIntOrString 字段上多余的 "type": ["integer","string"]，
// 使序列化后的 JSON 与内部表示的预期形式一致。
func stripIntOrStringType(x interface{}) interface{} {
	switch x := x.(type) {
	case map[string]interface{}:
		if t, found := x["type"]; found {
			switch t := t.(type) {
			case []interface{}:
				if len(t) == 2 && t[0] == "integer" && t[1] == "string" && x["x-kubernetes-int-or-string"] == true {
					delete(x, "type")
				}
			}
		}
		for k := range x {
			x[k] = stripIntOrStringType(x[k])
		}
		return x
	case []interface{}:
		for i := range x {
			x[i] = stripIntOrStringType(x[i])
		}
		return x
	default:
		return x
	}
}

// failingObject 描述一个预期校验失败的对象及其期望错误信息。
type failingObject struct {
	object     interface{}
	oldObject  interface{}
	expectErrs []string
}

// TestValidateCustomResource 覆盖结构校验与 CEL 校验在可空、int-or-string、
// 必填、枚举、过渡规则等场景下的行为。
func TestValidateCustomResource(t *testing.T) {
	tests := []struct {
		name           string
		compatVersion  *version.Version
		schema         apiextensions.JSONSchemaProps
		objects        []interface{}
		oldObjects     []interface{}
		failingObjects []failingObject
	}{
		{name: "!nullable",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:     "object",
						Nullable: false,
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": map[string]interface{}{}},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": "foo"}, expectErrs: []string{`field: Invalid value: "string": field in body must be of type object: "string"`}},
				{object: map[string]interface{}{"field": 42}, expectErrs: []string{`field: Invalid value: "integer": field in body must be of type object: "integer"`}},
				{object: map[string]interface{}{"field": true}, expectErrs: []string{`field: Invalid value: "boolean": field in body must be of type object: "boolean"`}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{`field: Invalid value: "number": field in body must be of type object: "number"`}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{`field: Invalid value: "array": field in body must be of type object: "array"`}},
				{object: map[string]interface{}{"field": nil}, expectErrs: []string{`field: Invalid value: "null": field in body must be of type object: "null"`}},
			},
		},
		{name: "nullable",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:     "object",
						Nullable: true,
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": map[string]interface{}{}},
				map[string]interface{}{"field": nil},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": "foo"}, expectErrs: []string{`field: Invalid value: "string": field in body must be of type object: "string"`}},
				{object: map[string]interface{}{"field": 42}, expectErrs: []string{`field: Invalid value: "integer": field in body must be of type object: "integer"`}},
				{object: map[string]interface{}{"field": true}, expectErrs: []string{`field: Invalid value: "boolean": field in body must be of type object: "boolean"`}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{`field: Invalid value: "number": field in body must be of type object: "number"`}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{`field: Invalid value: "array": field in body must be of type object: "array"`}},
			},
		},
		{name: "nullable and no type",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Nullable: true,
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": map[string]interface{}{}},
				map[string]interface{}{"field": nil},
				map[string]interface{}{"field": "foo"},
				map[string]interface{}{"field": 42},
				map[string]interface{}{"field": true},
				map[string]interface{}{"field": 1.2},
				map[string]interface{}{"field": []interface{}{}},
			},
		},
		{name: "x-kubernetes-int-or-string",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						XIntOrString: true,
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": 42},
				map[string]interface{}{"field": "foo"},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": nil}, expectErrs: []string{`field: Invalid value: "null": field in body must be of type integer,string: "null"`}},
				{object: map[string]interface{}{"field": true}, expectErrs: []string{`field: Invalid value: "boolean": field in body must be of type integer,string: "boolean"`}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{`field: Invalid value: "number": field in body must be of type integer,string: "number"`}},
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{`field: Invalid value: "object": field in body must be of type integer,string: "object"`}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{`field: Invalid value: "array": field in body must be of type integer,string: "array"`}},
			},
		},
		{name: "nullable and x-kubernetes-int-or-string",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Nullable:     true,
						XIntOrString: true,
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": 42},
				map[string]interface{}{"field": "foo"},
				map[string]interface{}{"field": nil},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": true}, expectErrs: []string{`field: Invalid value: "boolean": field in body must be of type integer,string: "boolean"`}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{`field: Invalid value: "number": field in body must be of type integer,string: "number"`}},
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{`field: Invalid value: "object": field in body must be of type integer,string: "object"`}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{`field: Invalid value: "array": field in body must be of type integer,string: "array"`}},
			},
		},
		{name: "nullable, x-kubernetes-int-or-string and user-provided anyOf",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Nullable:     true,
						XIntOrString: true,
						AnyOf: []apiextensions.JSONSchemaProps{
							{Type: "integer"},
							{Type: "string"},
						},
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": nil},
				map[string]interface{}{"field": 42},
				map[string]interface{}{"field": "foo"},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": true}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "boolean": field in body must be of type integer,string: "boolean"`,
					`field: Invalid value: "boolean": field in body must be of type integer: "boolean"`,
				}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "number": field in body must be of type integer,string: "number"`,
					`field: Invalid value: "number": field in body must be of type integer: "number"`,
					`<nil>: Invalid value: "": Checked value must be of type integer (default format) in field`,
				}},
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "object": field in body must be of type integer,string: "object"`,
					`field: Invalid value: "object": field in body must be of type integer: "object"`,
				}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "array": field in body must be of type integer,string: "array"`,
					`field: Invalid value: "array": field in body must be of type integer: "array"`,
				}},
			},
		},
		{name: "nullable, x-kubernetes-int-or-string and user-provider allOf",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Nullable:     true,
						XIntOrString: true,
						AllOf: []apiextensions.JSONSchemaProps{
							{
								AnyOf: []apiextensions.JSONSchemaProps{
									{Type: "integer"},
									{Type: "string"},
								},
							},
						},
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{},
				map[string]interface{}{"field": nil},
				map[string]interface{}{"field": 42},
				map[string]interface{}{"field": "foo"},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": true}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate all the schemas (allOf). None validated`,
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "boolean": field in body must be of type integer,string: "boolean"`,
					`field: Invalid value: "boolean": field in body must be of type integer: "boolean"`,
				}},
				{object: map[string]interface{}{"field": 1.2}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate all the schemas (allOf). None validated`,
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "number": field in body must be of type integer,string: "number"`,
					`field: Invalid value: "number": field in body must be of type integer: "number"`,
					`<nil>: Invalid value: "": Checked value must be of type integer (default format) in field`,
				}},
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate all the schemas (allOf). None validated`,
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "object": field in body must be of type integer,string: "object"`,
					`field: Invalid value: "object": field in body must be of type integer: "object"`,
				}},
				{object: map[string]interface{}{"field": []interface{}{}}, expectErrs: []string{
					`<nil>: Invalid value: "": "field" must validate all the schemas (allOf). None validated`,
					`<nil>: Invalid value: "": "field" must validate at least one schema (anyOf)`,
					`field: Invalid value: "array": field in body must be of type integer,string: "array"`,
					`field: Invalid value: "array": field in body must be of type integer: "array"`,
				}},
			},
		},
		{name: "invalid regex",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:    "string",
						Pattern: "+",
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": "foo"}, expectErrs: []string{"field: Invalid value: \"foo\": field in body should match '+, but pattern is invalid: error parsing regexp: missing argument to repetition operator: `+`'"}},
			},
		},
		{name: "required field",
			schema: apiextensions.JSONSchemaProps{
				Type:     "object",
				Required: []string{"field"},
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:     "object",
						Required: []string{"nested"},
						Properties: map[string]apiextensions.JSONSchemaProps{
							"nested": {},
						},
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"test": "a"}, expectErrs: []string{`field: Required value`}},
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{`field.nested: Required value`}},
			},
		},
		{name: "enum",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:     "object",
						Required: []string{"nestedint", "nestedstring"},
						Properties: map[string]apiextensions.JSONSchemaProps{
							"nestedint": {
								Type: "integer",
								Enum: []apiextensions.JSON{1, 2},
							},
							"nestedstring": {
								Type: "string",
								Enum: []apiextensions.JSON{"a", "b"},
							},
						},
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"field": map[string]interface{}{}}, expectErrs: []string{
					`field.nestedint: Required value`,
					`field.nestedstring: Required value`,
				}},
				{object: map[string]interface{}{"field": map[string]interface{}{"nestedint": "x", "nestedstring": true}}, expectErrs: []string{
					`field.nestedint: Invalid value: "string": field.nestedint in body must be of type integer: "string"`,
					`field.nestedint: Unsupported value: "x": supported values: "1", "2"`,
					`field.nestedstring: Invalid value: "boolean": field.nestedstring in body must be of type string: "boolean"`,
					`field.nestedstring: Unsupported value: true: supported values: "a", "b"`,
				}},
			},
		},
		{name: "immutability transition rule",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type: "string",
						XValidations: []apiextensions.ValidationRule{
							{
								Rule: "self == oldSelf",
							},
						},
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{"field": "x"},
			},
			oldObjects: []interface{}{
				map[string]interface{}{"field": "x"},
			},
			failingObjects: []failingObject{
				{
					object:    map[string]interface{}{"field": "y"},
					oldObject: map[string]interface{}{"field": "x"},
					expectErrs: []string{
						`field: Invalid value: "y": failed rule: self == oldSelf`,
					}},
			},
		},
		{name: "correlatable transition rule",
			// 确保 "listMap" 下的过渡规则得到支持。
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type:         "array",
						XListType:    &listMapType,
						XListMapKeys: []string{"k1", "k2"},
						Items: &apiextensions.JSONSchemaPropsOrArray{
							Schema: &apiextensions.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensions.JSONSchemaProps{
									"k1": {
										Type: "string",
									},
									"k2": {
										Type: "string",
									},
									"v1": {
										Type: "number",
										XValidations: []apiextensions.ValidationRule{
											{
												Rule: "self >= oldSelf",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{"field": []interface{}{map[string]interface{}{"k1": "a", "k2": "b", "v1": 1.2}}},
			},
			oldObjects: []interface{}{
				map[string]interface{}{"field": []interface{}{map[string]interface{}{"k1": "a", "k2": "b", "v1": 1.0}}},
			},
			failingObjects: []failingObject{
				{
					object:    map[string]interface{}{"field": []interface{}{map[string]interface{}{"k1": "a", "k2": "b", "v1": 0.9}}},
					oldObject: map[string]interface{}{"field": []interface{}{map[string]interface{}{"k1": "a", "k2": "b", "v1": 1.0}}},
					expectErrs: []string{
						`field[0].v1: Invalid value: 0.9: failed rule: self >= oldSelf`,
					}},
			},
		},
		{name: "validation rule under non-correlatable field",
			// 数组使嵌套字符串上的规则无法用于过渡规则关联。
			// 本测试确保一条不使用 oldSelf（不是过渡规则）的规则，
			// 在不可关联字段下仍按预期工作。
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"field": {
						Type: "array",
						Items: &apiextensions.JSONSchemaPropsOrArray{
							Schema: &apiextensions.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensions.JSONSchemaProps{
									"x": {
										Type: "string",
										XValidations: []apiextensions.ValidationRule{
											{
												Rule: "self == 'x'",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			objects: []interface{}{
				map[string]interface{}{"field": []interface{}{map[string]interface{}{"x": "x"}}},
			},
			failingObjects: []failingObject{
				{
					object: map[string]interface{}{"field": []interface{}{map[string]interface{}{"x": "y"}}},
					expectErrs: []string{
						`field[0].x: Invalid value: "y": failed rule: self == 'x'`,
					}},
			},
		},
		{name: "maxProperties",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"fieldX": {
						Type:          "object",
						MaxProperties: ptr.To[int64](2),
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"fieldX": map[string]interface{}{"a": true, "b": true, "c": true}}, expectErrs: []string{
					`fieldX: Too many: 3: must have at most 2 items`,
				}},
			},
		},
		{name: "maxItems",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"fieldX": {
						Type:     "array",
						MaxItems: ptr.To[int64](2),
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"fieldX": []interface{}{"a", "b", "c"}}, expectErrs: []string{
					`fieldX: Too many: 3: must have at most 2 items`,
				}},
			},
		},
		{name: "maxLength",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"fieldX": {
						Type:      "string",
						MaxLength: ptr.To[int64](2),
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"fieldX": "abc"}, expectErrs: []string{
					`fieldX: Too long: may not be more than 2 bytes`,
				}},
			},
		},
		{name: "k8sLongName",
			compatVersion: version.MajorMinor(1, 34),
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"fieldX": {
						Type:   "string",
						Format: "k8s-long-name",
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"fieldX": "a.-"}, expectErrs: []string{
					`fieldX: Invalid value: "a.-": fieldX in body must be of type k8s-long-name: "a.-"`,
				}},
			},
		},
		{name: "k8sShortName",
			compatVersion: version.MajorMinor(1, 34),
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"fieldX": {
						Type:   "string",
						Format: "k8s-short-name",
					},
				},
			},
			failingObjects: []failingObject{
				{object: map[string]interface{}{"fieldX": "a-"}, expectErrs: []string{
					`fieldX: Invalid value: "a-": fieldX in body must be of type k8s-short-name: "a-"`,
				}},
			},
		},
		{name: "numeric formats valid",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"intThirtyTwo":  {Type: "integer", Format: "int32"},
					"intSixtyFour":  {Type: "integer", Format: "int64"},
					"floatThreeTwo": {Type: "number", Format: "float"},
					"floatSixFour":  {Type: "number", Format: "double"},
				},
			},
			objects: []interface{}{
				map[string]interface{}{
					"intThirtyTwo":  int64(math.MinInt32),
					"intSixtyFour":  int64(math.MinInt64),
					"floatThreeTwo": float64(-math.MaxFloat32),
					"floatSixFour":  float64(-math.MaxFloat64),
				},
				map[string]interface{}{
					"intThirtyTwo":  int64(0),
					"intSixtyFour":  int64(0),
					"floatThreeTwo": float64(0),
					"floatSixFour":  float64(0),
				},
				map[string]interface{}{
					"intThirtyTwo":  int64(math.MaxInt32),
					"intSixtyFour":  int64(math.MaxInt64),
					"floatThreeTwo": float64(math.MaxFloat32),
					"floatSixFour":  float64(math.MaxFloat64),
				},
			},
		},
		{name: "numeric formats invalid",
			schema: apiextensions.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensions.JSONSchemaProps{
					"intThirtyTwo":  {Type: "integer", Format: "int32"},
					"intSixtyFour":  {Type: "integer", Format: "int64"},
					"floatThreeTwo": {Type: "number", Format: "float"},
					"floatSixFour":  {Type: "number", Format: "double"},
				},
			},
			failingObjects: []failingObject{
				{
					object: map[string]interface{}{"intThirtyTwo": int64(math.MaxInt32 + 1)},
					expectErrs: []string{
						`<nil>: Invalid value: "": Checked value must be of type integer with format int32 in intThirtyTwo`,
					},
				},
				{
					object: map[string]interface{}{"intThirtyTwo": int64(math.MinInt32 - 1)},
					expectErrs: []string{
						`<nil>: Invalid value: "": Checked value must be of type integer with format int32 in intThirtyTwo`,
					},
				},
				// int64 输入本身不会溢出，但可以用 float64 触发该路径
				{
					object: map[string]interface{}{"intSixtyFour": float64(math.MaxInt64) * 1.1},
					expectErrs: []string{
						`intSixtyFour: Invalid value: "float64": intSixtyFour in body must be of type int64: "float64"`,
						`<nil>: Invalid value: "": Checked value must be of type integer with format int64 in intSixtyFour`,
					},
				},
				{
					object: map[string]interface{}{"intSixtyFour": float64(math.MinInt64) * 1.1},
					expectErrs: []string{
						`intSixtyFour: Invalid value: "float64": intSixtyFour in body must be of type int64: "float64"`,
						`<nil>: Invalid value: "": Checked value must be of type integer with format int64 in intSixtyFour`,
					},
				},
				{
					object: map[string]interface{}{"floatThreeTwo": float64(math.MaxFloat32 * 1.1)},
					expectErrs: []string{
						`<nil>: Invalid value: "": Checked value must be of type number with format float in floatThreeTwo`,
					},
				},
				{
					object: map[string]interface{}{"floatThreeTwo": float64(-math.MaxFloat32 * 1.1)},
					expectErrs: []string{
						`<nil>: Invalid value: "": Checked value must be of type number with format float in floatThreeTwo`,
					},
				},
				// double（float64）溢出由 JSON 解析处理，但可以尝试传入可能触发它的值。
				// 不过标准 JSON 反序列化通常会在 float64 上限处截断或报错。
				// 校验器本身会检查范围；由于 Go 的 float64 与 double 等宽，不借助自定义数值类型很难表示真正的溢出。
				// 这里跳过显式的 double 溢出测试：它们往往产生 +Inf/-Inf，可能被不同地处理或直接解析失败。
			},
		},
	}
	for _, tt := range tests {
		compatVersion := tt.compatVersion
		if compatVersion == nil {
			// 未显式指定时使用默认兼容版本
			compatVersion = environment.DefaultCompatibilityVersion()
		}

		t.Run(tt.name, func(t *testing.T) {
			validator, _, err := NewSchemaValidatorForVersion(&tt.schema, compatVersion)
			if err != nil {
				t.Fatal(err)
			}
			structural, err := structuralschema.NewStructural(&tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			celValidator := cel.NewValidator(structural, false, celconfig.PerCallLimit)
			for i, obj := range tt.objects {
				var oldObject interface{}
				// 仅当新旧对象列表长度一致时才逐项配对，避免越界
				if len(tt.oldObjects) == len(tt.objects) {
					oldObject = tt.oldObjects[i]
				}
				if errs := ValidateCustomResource(nil, obj, validator); len(errs) > 0 {
					t.Errorf("unexpected validation error for %v: %v", obj, errs)
				}
				errs, _ := celValidator.Validate(context.TODO(), nil, structural, obj, oldObject, celconfig.RuntimeCELCostBudget)
				if len(errs) > 0 {
					t.Error(errs.ToAggregate().Error())
				}
			}
			for i, failingObject := range tt.failingObjects {
				errs := ValidateCustomResource(nil, failingObject.object, validator)
				celErrs, _ := celValidator.Validate(context.TODO(), nil, structural, failingObject.object, failingObject.oldObject, celconfig.RuntimeCELCostBudget)
				errs = append(errs, celErrs...)
				if len(errs) == 0 {
					t.Errorf("missing error for %v", failingObject.object)
				} else {
					// 用集合比较实际错误与期望错误，可同时报告多余和缺失项
					sawErrors := sets.NewString()
					for _, err := range errs {
						sawErrors.Insert(err.Error())
					}
					expectErrs := sets.NewString(failingObject.expectErrs...)
					for _, unexpectedError := range sawErrors.Difference(expectErrs).List() {
						t.Errorf("%d: unexpected error: %s", i, unexpectedError)
					}
					for _, missingError := range expectErrs.Difference(sawErrors).List() {
						t.Errorf("%d: missing error:    %s", i, missingError)
					}
				}
			}
		})
	}
}

// TestItemsProperty 验证名为 "items" 的普通属性不会被误当作数组的 items 关键字。
func TestItemsProperty(t *testing.T) {
	type args struct {
		schema apiextensions.JSONSchemaProps
		object interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"items in object", args{
			apiextensions.JSONSchemaProps{
				Properties: map[string]apiextensions.JSONSchemaProps{
					"spec": {
						Properties: map[string]apiextensions.JSONSchemaProps{
							"replicas": {
								Type: "integer",
							},
						},
					},
				},
			},
			map[string]interface{}{"spec": map[string]interface{}{"replicas": 1, "items": []string{"1", "2"}}},
		}, false},
		{"items in array", args{
			apiextensions.JSONSchemaProps{
				Properties: map[string]apiextensions.JSONSchemaProps{
					"secrets": {
						Type: "array",
						Items: &apiextensions.JSONSchemaPropsOrArray{
							Schema: &apiextensions.JSONSchemaProps{
								Type: "string",
							},
						},
					},
				},
			},
			map[string]interface{}{"secrets": []string{"1", "2"}},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator, _, err := NewSchemaValidator(&tt.args.schema)
			if err != nil {
				t.Fatal(err)
			}
			if errs := ValidateCustomResource(nil, tt.args.object, validator); (len(errs) > 0) != tt.wantErr {
				if len(errs) == 0 {
					t.Error("expected error, but didn't get one")
				} else {
					t.Errorf("unexpected validation error: %v", errs)
				}
			}
		})
	}
}

// listMapType 供测试使用的 x-kubernetes-list-type 取值。
var listMapType = "map"
