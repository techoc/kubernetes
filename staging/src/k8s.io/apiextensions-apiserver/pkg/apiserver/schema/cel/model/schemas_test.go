/*
Copyright 2022 The Kubernetes Authors.

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

package model

import (
	"reflect"
	"testing"

	"github.com/google/cel-go/common/types"

	"google.golang.org/protobuf/proto"

	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/utils/ptr"
)

// TestSchemaDeclType 验证 SchemaDeclType 生成的 CEL 类型与 schema 的字段、
// 默认值、枚举以及 maxLength 换算出的 maxElements 一致，且必填字段被正确标记。
func TestSchemaDeclType(t *testing.T) {
	ts := testSchema()
	cust := SchemaDeclType(ts, false)
	if cust.TypeName() != "object" {
		t.Errorf("incorrect type name, got %v, wanted object", cust.TypeName())
	}
	if len(cust.Fields) != 5 {
		t.Errorf("incorrect number of fields, got %d, wanted 5", len(cust.Fields))
	}
	for _, f := range cust.Fields {
		prop, found := ts.Properties[f.Name]
		if !found {
			t.Errorf("type field not found in schema, field: %s", f.Name)
		}
		fdv := f.DefaultValue()
		if prop.Default.Object != nil {
			pdv := types.DefaultTypeAdapter.NativeToValue(prop.Default.Object)
			if !reflect.DeepEqual(fdv, pdv) {
				t.Errorf("field and schema do not agree on default value for field: %s, field value: %v, schema default: %v", f.Name, fdv, pdv)
			}
		}
		if (prop.ValueValidation == nil || len(prop.ValueValidation.Enum) == 0) && len(f.EnumValues()) != 0 {
			t.Errorf("field had more enum values than the property. field: %s", f.Name)
		}
		if prop.ValueValidation != nil {
			fevs := f.EnumValues()
			for _, fev := range fevs {
				found := false
				for _, pev := range prop.ValueValidation.Enum {
					celpev := types.DefaultTypeAdapter.NativeToValue(pev.Object)
					if reflect.DeepEqual(fev, celpev) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf(
						"could not find field enum value in property definition. field: %s, enum: %v",
						f.Name, fev)
				}
			}
		}
		if prop.ValueValidation != nil && prop.ValueValidation.MaxLength != nil {
			if f.Type.MaxElements != 4*(*prop.ValueValidation.MaxLength) {
				// 将 maxLength 换算为 maxElements 时按字节数计算。
				// 最坏情况一个 rune 占 4 字节，因此 maxElements 应为 maxLength 的 4 倍。
				t.Errorf("field maxElements does not match property 4x maxLength. field: %s, maxElements: %d, maxLength: %d", f.Name, f.Type.MaxElements, *prop.ValueValidation.MaxLength)
			}
		}
	}
	if ts.ValueValidation != nil {
		for _, name := range ts.ValueValidation.Required {
			df, found := cust.FindField(name)
			if !found {
				t.Errorf("custom type missing required field. field=%s", name)
			}
			if !df.Required {
				t.Errorf("field marked as required in schema, but optional in type. field=%s", df.Name)
			}
		}
	}
}

// TestSchemaDeclTypes 验证 SchemaDeclType 在 object 模式下产出的嵌套对象类型映射正确。
func TestSchemaDeclTypes(t *testing.T) {
	ts := testSchema()
	cust := SchemaDeclType(ts, true).MaybeAssignTypeName("CustomObject")
	typeMap := apiservercel.FieldTypeMap("CustomObject", cust)
	nested, _ := cust.FindField("nested")
	metadata, _ := cust.FindField("metadata")
	expectedObjTypeMap := map[string]*apiservercel.DeclType{
		"CustomObject":          cust,
		"CustomObject.nested":   nested.Type,
		"CustomObject.metadata": metadata.Type,
	}
	objTypeMap := map[string]*apiservercel.DeclType{}
	for name, t := range typeMap {
		if t.IsObject() {
			objTypeMap[name] = t
		}
	}
	if len(objTypeMap) != len(expectedObjTypeMap) {
		t.Errorf("got different type set. got=%v, wanted=%v", objTypeMap, expectedObjTypeMap)
	}
	for exp, expType := range expectedObjTypeMap {
		actType, found := objTypeMap[exp]
		if !found {
			t.Errorf("missing type in rule types: %s", exp)
			continue
		}
		expT, err := expType.ExprType()
		if err != nil {
			t.Errorf("fail to get cel type: %s", err)
		}
		actT, err := actType.ExprType()
		if err != nil {
			t.Errorf("fail to get cel type: %s", err)
		}
		if !proto.Equal(expT, actT) {
			t.Errorf("incompatible CEL types. got=%v, wanted=%v", expT, actT)
		}
	}
}

// TestWithTypeAndObjectMetaNameLength 验证 WithTypeAndObjectMeta 为
// metadata.name/generateName 设置的长度上限：默认值、保留作者值、超限截断，
// 并确认该上限会流入 CEL 成本估算。
func TestWithTypeAndObjectMetaNameLength(t *testing.T) {
	const (
		defaultMax             = nameMaxLength
		defaultGenerateNameMax = nameMaxLength - 1 // 生成的后缀至少占 1 字节
	)

	maxLengthOf := func(s *schema.Structural, field string) *int64 {
		md, ok := s.Properties["metadata"]
		if !ok || md.Properties == nil {
			return nil
		}
		f, ok := md.Properties[field]
		if !ok || f.ValueValidation == nil {
			return nil
		}
		return f.ValueValidation.MaxLength
	}

	cases := []struct {
		name             string
		input            *schema.Structural
		wantName         int64
		wantGenerateName int64
	}{
		{
			name:             "metadata absent, defaults applied to both fields",
			input:            &schema.Structural{Generic: schema.Generic{Type: "object"}},
			wantName:         defaultMax,
			wantGenerateName: defaultGenerateNameMax,
		},
		{
			name: "author maxLength on name preserved, generateName defaulted",
			input: &schema.Structural{
				Generic: schema.Generic{Type: "object"},
				Properties: map[string]schema.Structural{
					"metadata": {
						Generic: schema.Generic{Type: "object"},
						Properties: map[string]schema.Structural{
							"name": {
								Generic:         schema.Generic{Type: "string"},
								ValueValidation: &schema.ValueValidation{MaxLength: ptr.To[int64](10)},
							},
						},
					},
				},
			},
			wantName:         10,
			wantGenerateName: defaultGenerateNameMax,
		},
		{
			name: "author maxLength on generateName smaller than default is preserved",
			input: &schema.Structural{
				Generic: schema.Generic{Type: "object"},
				Properties: map[string]schema.Structural{
					"metadata": {
						Generic: schema.Generic{Type: "object"},
						Properties: map[string]schema.Structural{
							"generateName": {
								Generic:         schema.Generic{Type: "string"},
								ValueValidation: &schema.ValueValidation{MaxLength: ptr.To[int64](100)},
							},
						},
					},
				},
			},
			wantName:         defaultMax,
			wantGenerateName: 100,
		},
		{
			name: "author maxLength larger than default is capped to the default",
			input: &schema.Structural{
				Generic: schema.Generic{Type: "object"},
				Properties: map[string]schema.Structural{
					"metadata": {
						Generic: schema.Generic{Type: "object"},
						Properties: map[string]schema.Structural{
							"name": {
								Generic:         schema.Generic{Type: "string"},
								ValueValidation: &schema.ValueValidation{MaxLength: ptr.To[int64](1000)},
							},
							"generateName": {
								Generic:         schema.Generic{Type: "string"},
								ValueValidation: &schema.ValueValidation{MaxLength: ptr.To[int64](1000)},
							},
						},
					},
				},
			},
			wantName:         defaultMax,
			wantGenerateName: defaultGenerateNameMax,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := WithTypeAndObjectMeta(tc.input)
			if ml := maxLengthOf(got, "name"); ml == nil || *ml != tc.wantName {
				t.Errorf("metadata.name maxLength = %v, want %d", ml, tc.wantName)
			}
			if ml := maxLengthOf(got, "generateName"); ml == nil || *ml != tc.wantGenerateName {
				t.Errorf("metadata.generateName maxLength = %v, want %d", ml, tc.wantGenerateName)
			}

			// 该上限必须流入 CEL 成本估算：declType 中 name 和 generateName 的
			// MaxElements 反映有界长度（每个 rune 4 字节），而非请求大小。
			root := SchemaDeclType(tc.input, true)
			meta, ok := root.FindField("metadata")
			if !ok {
				t.Fatalf("metadata field missing from declType")
			}
			for _, f := range []struct {
				field string
				want  int64
			}{
				{"name", tc.wantName},
				{"generateName", tc.wantGenerateName},
			} {
				field, ok := meta.Type.FindField(f.field)
				if !ok {
					t.Fatalf("metadata.%s field missing from declType", f.field)
				}
				if got := field.Type.MaxElements; got != f.want*4 {
					t.Errorf("metadata.%s declType MaxElements = %d, want %d", f.field, got, f.want*4)
				}
				if field.Type.MaxElements >= apiservercel.DefaultMaxRequestSizeBytes-2 {
					t.Errorf("metadata.%s declType MaxElements = %d, expected it bounded well below the request size", f.field, field.Type.MaxElements)
				}
			}
		})
	}
}

// TestWithTypeAndObjectMetaIdempotent 确保重复应用 WithTypeAndObjectMeta
// （schema 被多次处理时会发生）时，长度上限保持稳定。
func TestWithTypeAndObjectMetaIdempotent(t *testing.T) {
	once := WithTypeAndObjectMeta(&schema.Structural{Generic: schema.Generic{Type: "object"}})
	twice := WithTypeAndObjectMeta(once)
	if twice != once {
		t.Errorf("expected the already-bounded schema to be returned unchanged")
	}
	for _, f := range []struct {
		field string
		want  int64
	}{
		{"name", nameMaxLength},
		{"generateName", nameMaxLength - 1},
	} {
		ml := twice.Properties["metadata"].Properties[f.field].ValueValidation.MaxLength
		if ml == nil || *ml != f.want {
			t.Errorf("metadata.%s maxLength = %v, want %d", f.field, ml, f.want)
		}
	}
}

// TestWithTypeAndObjectMetaDoesNotMutateInput 验证 maxLength 上限只应用于
// 返回的（仅用于 CEL 的）schema，绝不写回调用方的 schema。
// 输入 schema 还会用于裁剪、默认值填充和构建值校验器，这些都不应获得
// metadata.name 的 maxLength 约束。
func TestWithTypeAndObjectMetaDoesNotMutateInput(t *testing.T) {
	input := &schema.Structural{
		Generic: schema.Generic{Type: "object"},
		Properties: map[string]schema.Structural{
			"metadata": {
				Generic: schema.Generic{Type: "object"},
				Properties: map[string]schema.Structural{
					"name": {
						Generic:         schema.Generic{Type: "string"},
						ValueValidation: &schema.ValueValidation{Format: "k8s-short-name"},
					},
				},
			},
		},
	}

	result := WithTypeAndObjectMeta(input)

	// 输入中作者指定的 metadata.name 保持不变：未添加 maxLength，
	// 且其 format 被保留。
	inName := input.Properties["metadata"].Properties["name"]
	if inName.ValueValidation == nil || inName.ValueValidation.MaxLength != nil {
		t.Errorf("input metadata.name must not gain a maxLength, got %+v", inName.ValueValidation)
	}
	if inName.ValueValidation == nil || inName.ValueValidation.Format != "k8s-short-name" {
		t.Errorf("input metadata.name format must be preserved, got %+v", inName.ValueValidation)
	}

	// 返回的 schema 仅用于 CEL 成本/类型建模，带有该上限。
	outName := result.Properties["metadata"].Properties["name"]
	if outName.ValueValidation == nil || outName.ValueValidation.MaxLength == nil || *outName.ValueValidation.MaxLength != nameMaxLength {
		t.Errorf("result metadata.name maxLength = %+v, want %d", outName.ValueValidation, nameMaxLength)
	}
}

func testSchema() *schema.Structural {
	// testSchema 手工构造具有以下定义的 schema，供类型映射测试使用：
	//
	// schema:
	//   type: object
	//   metadata:
	//     custom_type: "CustomObject"
	//   required:
	//     - name
	//     - value
	//   properties:
	//     name:
	//       type: string
	//       maxLength: 256
	//     nested:
	//       type: object
	//       properties:
	//         subname:
	//           type: string
	//         flags:
	//           type: object
	//           additionalProperties:
	//             type: boolean
	//         dates:
	//           type: array
	//           items:
	//             type: string
	//             format: date-time
	//      metadata:
	//        type: object
	//        additionalProperties:
	//          type: object
	//          properties:
	//            key:
	//              type: string
	//            values:
	//              type: array
	//              items: string
	//     value:
	//       type: integer
	//       format: int64
	//       default: 1
	//       enum: [1,2,3]
	//     intOrString:
	//       x-kubernetes-int-or-string: true
	//       anyOf:
	//       - type: "integer"
	//		 - type: "string"
	//       maxLength: 20
	ts := &schema.Structural{
		Generic: schema.Generic{
			Type: "object",
		},
		Properties: map[string]schema.Structural{
			"name": {
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					MaxLength: ptr.To[int64](256),
				},
			},
			"value": {
				Generic: schema.Generic{
					Type:    "integer",
					Default: schema.JSON{Object: int64(1)},
				},
				ValueValidation: &schema.ValueValidation{
					Format: "int64",
					Enum:   []schema.JSON{{Object: int64(1)}, {Object: int64(2)}, {Object: int64(3)}},
				},
			},
			"nested": {
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"subname": {
						Generic: schema.Generic{
							Type: "string",
						},
					},
					"flags": {
						Generic: schema.Generic{
							Type: "object",
						},
						AdditionalProperties: &schema.StructuralOrBool{
							Structural: &schema.Structural{
								Generic: schema.Generic{
									Type: "boolean",
								},
							},
						},
					},
					"dates": {
						Generic: schema.Generic{
							Type: "array",
						},
						Items: &schema.Structural{
							Generic: schema.Generic{
								Type: "string",
							},
							ValueValidation: &schema.ValueValidation{
								Format: "date-time",
							},
						},
					},
				},
			},
			"metadata": {
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"name": {
						Generic: schema.Generic{
							Type: "string",
						},
					},
					"value": {
						Generic: schema.Generic{
							Type: "array",
						},
						Items: &schema.Structural{
							Generic: schema.Generic{
								Type: "string",
							},
						},
					},
				},
			},
			"intOrString": {
				Extensions: schema.Extensions{
					XIntOrString: true,
				},
				ValueValidation: &schema.ValueValidation{
					MaxLength: ptr.To[int64](20),
					AnyOf: []schema.NestedValueValidation{
						{
							ForbiddenGenerics: schema.Generic{
								Type: "integer",
							},
						},
						{
							ForbiddenGenerics: schema.Generic{
								Type: "string",
							},
						},
					},
				},
			},
		},
	}
	return ts
}

// arraySchema 构造给定元素类型、格式和 maxItems 的数组 schema。
func arraySchema(arrayType, format string, maxItems *int64) *schema.Structural {
	return &schema.Structural{
		Generic: schema.Generic{
			Type: "array",
		},
		Items: &schema.Structural{
			Generic: schema.Generic{
				Type: arrayType,
			},
			ValueValidation: &schema.ValueValidation{
				Format: format,
			},
		},
		ValueValidation: &schema.ValueValidation{
			MaxItems: maxItems,
		},
	}
}

// TestEstimateMaxLengthJSON 验证 SchemaDeclType 对不同数组/字符串/对象形态
// 估算出的 MaxElements。
func TestEstimateMaxLengthJSON(t *testing.T) {
	type maxLengthTest struct {
		Name                string
		InputSchema         *schema.Structural
		ExpectedMaxElements int64
		ExpectNilType       bool
	}
	tests := []maxLengthTest{
		{
			Name:        "booleanArray",
			InputSchema: arraySchema("boolean", "", nil),
			// 预期 JSON 为 [true,true,...]，因此长度应为 (maxRequestSizeBytes - 2) / 5
			ExpectedMaxElements: 629145,
		},
		{
			Name:        "durationArray",
			InputSchema: arraySchema("string", "duration", nil),
			// 预期 JSON 为 ["0","0",...]，因此长度应为 (maxRequestSizeBytes - 2) / 4
			ExpectedMaxElements: 786431,
		},
		{
			Name:        "datetimeArray",
			InputSchema: arraySchema("string", "date-time", nil),
			// 预期 JSON 为 ["2000-01-01T01:01:01","2000-01-01T01:01:01",...]，因此长度应为 (maxRequestSizeBytes - 2) / 22
			ExpectedMaxElements: 142987,
		},
		{
			Name:        "dateArray",
			InputSchema: arraySchema("string", "date", nil),
			// 预期 JSON 为 ["2000-01-01","2000-01-02",...]，因此长度应为 (maxRequestSizeBytes - 2) / 13
			ExpectedMaxElements: 241978,
		},
		{
			Name:        "numberArray",
			InputSchema: arraySchema("integer", "", nil),
			// 预期 JSON 为 [0,0,...]，因此长度应为 (maxRequestSizeBytes - 2) / 2
			ExpectedMaxElements: 1572863,
		},
		{
			Name:        "stringArray",
			InputSchema: arraySchema("string", "", nil),
			// 预期 JSON 为 ["","",...]，因此长度应为 (maxRequestSizeBytes - 2) / 3
			ExpectedMaxElements: 1048575,
		},
		{
			Name: "stringMap",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				AdditionalProperties: &schema.StructuralOrBool{Structural: &schema.Structural{
					Generic: schema.Generic{
						Type: "string",
					},
				}},
			},
			// 预期 JSON 为 {"":"","":"",...}，因此长度应为 (3000000 - 2) / 6
			ExpectedMaxElements: 393215,
		},
		{
			Name: "objectOptionalPropertyArray",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "array",
				},
				Items: &schema.Structural{
					Generic: schema.Generic{
						Type: "object",
					},
					Properties: map[string]schema.Structural{
						"required": {
							Generic: schema.Generic{
								Type: "string",
							},
						},
						"optional": {
							Generic: schema.Generic{
								Type: "string",
							},
						},
					},
					ValueValidation: &schema.ValueValidation{
						Required: []string{"required"},
					},
				},
			},
			// 预期 JSON 为 [{"required":"",},{"required":"",},...]，因此长度应为 (maxRequestSizeBytes - 2) / 17
			ExpectedMaxElements: 185042,
		},
		{
			Name:        "arrayWithLength",
			InputSchema: arraySchema("integer", "int64", ptr.To[int64](10)),
			// 由 MaxItems 手动设置
			ExpectedMaxElements: 10,
		},
		{
			Name: "stringWithLength",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					MaxLength: ptr.To[int64](20),
				},
			},
			// 由 MaxLength 手动设置，但相比原始输入预期放大 4 倍：
			// OpenAPIv3 的 maxLength 以码点计，而 DeclType 以字节计。
			ExpectedMaxElements: 80,
		},
		{
			Name: "mapWithLength",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				AdditionalProperties: &schema.StructuralOrBool{Structural: &schema.Structural{
					Generic: schema.Generic{
						Type: "string",
					},
				}},
				ValueValidation: &schema.ValueValidation{
					Format:        "string",
					MaxProperties: ptr.To[int64](15),
				},
			},
			// 由 MaxProperties 手动设置
			ExpectedMaxElements: 15,
		},
		{
			Name: "durationMaxSize",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Format: "duration",
				},
			},
			// 应恰好等于 maxDurationSizeJSON
			ExpectedMaxElements: apiservercel.MaxDurationSizeJSON,
		},
		{
			Name: "dateSize",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Format: "date",
				},
			},
			// 应恰好等于 dateSizeJSON
			ExpectedMaxElements: apiservercel.JSONDateSize,
		},
		{
			Name: "maxdatetimeSize",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Format: "date-time",
				},
			},
			// 应恰好等于 maxDatetimeSizeJSON
			ExpectedMaxElements: apiservercel.MaxDatetimeSizeJSON,
		},
		{
			Name: "maxintOrStringSize",
			InputSchema: &schema.Structural{
				Extensions: schema.Extensions{
					XIntOrString: true,
				},
			},
			// 应恰好等于 maxRequestSizeBytes - 2（为字符串两侧的引号留出空间）
			ExpectedMaxElements: apiservercel.DefaultMaxRequestSizeBytes - 2,
		},
		{
			Name: "objectDefaultFieldArray",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "array",
				},
				Items: &schema.Structural{
					Generic: schema.Generic{
						Type: "object",
					},
					Properties: map[string]schema.Structural{
						"field": {
							Generic: schema.Generic{
								Type:    "string",
								Default: schema.JSON{Object: "default"},
							},
						},
					},
					ValueValidation: &schema.ValueValidation{
						Required: []string{"field"},
					},
				},
			},
			// 预期 JSON 为 [{},{},...]，因此长度应为 (maxRequestSizeBytes - 2) / 3
			ExpectedMaxElements: 1048575,
		},
		{
			Name: "byteStringSize",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Format: "byte",
				},
			},
			// 预期 JSON 为 ""，因此长度应为 (maxRequestSizeBytes - 2)
			ExpectedMaxElements: 3145726,
		},
		{
			Name: "byteStringSetMaxLength",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Format:    "byte",
					MaxLength: ptr.To[int64](20),
				},
			},
			// 注意与普通字符串不同，这里无需考虑 unicode，
			// 因此预期最大长度恰好等于用户指定的值
			ExpectedMaxElements: 20,
		},
		{
			Name: "Property under array",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "array",
				},
				Properties: map[string]schema.Structural{
					"field": {
						Generic: schema.Generic{
							Type:    "string",
							Default: schema.JSON{Object: "default"},
						},
					},
				},
			},
			// delType 为 nil
			ExpectedMaxElements: 0,
			ExpectNilType:       true,
		},
		{
			Name: "Items under object",
			InputSchema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Items: &schema.Structural{
					Generic: schema.Generic{
						Type: "array",
					},
					Properties: map[string]schema.Structural{
						"field": {
							Generic: schema.Generic{
								Type:    "string",
								Default: schema.JSON{Object: "default"},
							},
						},
					},
					ValueValidation: &schema.ValueValidation{
						Required: []string{"field"},
					},
				},
			},
			// 类型转换时跳过 object 下的 items。
			ExpectedMaxElements: 0,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.Name, func(t *testing.T) {
			decl := SchemaDeclType(testCase.InputSchema, false)
			if decl != nil && decl.MaxElements != testCase.ExpectedMaxElements {
				t.Errorf("wrong maxElements (got %d, expected %d)", decl.MaxElements, testCase.ExpectedMaxElements)
			}
			if testCase.ExpectNilType && decl != nil {
				t.Errorf("expected nil type, got %v", decl)
			}
		})
	}
}

// genNestedSchema 生成深度为 depth 的嵌套 object schema。
func genNestedSchema(depth int) *schema.Structural {
	var generator func(d int) schema.Structural
	generator = func(d int) schema.Structural {
		nodeTemplate := schema.Structural{
			Generic: schema.Generic{
				Type: "object",
			},
			AdditionalProperties: &schema.StructuralOrBool{},
		}
		if d == 1 {
			return nodeTemplate
		} else {
			mapType := generator(d - 1)
			nodeTemplate.AdditionalProperties.Structural = &mapType
			return nodeTemplate
		}
	}
	schema := generator(depth)
	return &schema
}

// BenchmarkDeeplyNestedSchemaDeclType 测量深度嵌套 schema 的类型推导开销。
func BenchmarkDeeplyNestedSchemaDeclType(b *testing.B) {
	benchmarkSchema := genNestedSchema(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SchemaDeclType(benchmarkSchema, false)
	}
}
