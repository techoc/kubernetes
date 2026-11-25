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

package templates_test

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	v2 "k8s.io/kubectl/pkg/explain/v2"
)

var (
	//go:embed plaintext.tmpl
	plaintextSource string

	//go:embed apiextensions.k8s.io_v1.json
	apiextensionsJSON string

	//go:embed batch.k8s.io_v1.json
	batchJSON string

	apiExtensionsV1OpenAPI map[string]interface{} = func() map[string]interface{} {
		var res map[string]interface{}
		utilruntime.Must(json.Unmarshal([]byte(apiextensionsJSON), &res))
		return res
	}()

	apiExtensionsV1OpenAPIWithoutListVerb map[string]interface{} = func() map[string]interface{} {
		var res map[string]interface{}
		utilruntime.Must(json.Unmarshal([]byte(apiextensionsJSON), &res))
		paths := res["paths"].(map[string]interface{})
		delete(paths, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
		return res
	}()

	apiExtensionsV1OpenAPISpec spec3.OpenAPI = func() spec3.OpenAPI {
		var res spec3.OpenAPI
		utilruntime.Must(json.Unmarshal([]byte(apiextensionsJSON), &res))
		return res
	}()

	batchV1OpenAPI map[string]interface{} = func() map[string]interface{} {
		var res map[string]interface{}
		utilruntime.Must(json.Unmarshal([]byte(batchJSON), &res))
		return res
	}()

	batchV1OpenAPIWithoutListVerb map[string]interface{} = func() map[string]interface{} {
		var res map[string]interface{}
		utilruntime.Must(json.Unmarshal([]byte(batchJSON), &res))
		paths := res["paths"].(map[string]interface{})
		delete(paths, "/apis/batch/v1/jobs")
		delete(paths, "/apis/batch/v1/namespaces/{namespace}/jobs")

		delete(paths, "/apis/batch/v1/cronjobs")
		delete(paths, "/apis/batch/v1/namespaces/{namespace}/cronjobs/{name}")
		return res
	}()

	nestedWidgetGVR = schema.GroupVersionResource{
		Group:    "example.com",
		Version:  "v1",
		Resource: "widgets",
	}

	nestedWidgetOpenAPI map[string]interface{} = map[string]interface{}{
		"paths": map[string]interface{}{
			"/apis/example.com/v1/widgets": map[string]interface{}{
				"get": map[string]interface{}{
					"x-kubernetes-group-version-kind": map[string]interface{}{
						"group":   "example.com",
						"version": "v1",
						"kind":    "Widget",
					},
				},
			},
		},
		"components": map[string]interface{}{
			"schemas": map[string]interface{}{
				"com.example.v1.Widget": map[string]interface{}{
					"type": "object",
					"x-kubernetes-group-version-kind": []map[string]interface{}{
						{"group": "example.com", "version": "v1", "kind": "Widget"},
					},
					"properties": map[string]interface{}{
						"name": map[string]interface{}{"type": "string"},
						"meta": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"id": map[string]interface{}{"type": "string"},
								"tags": map[string]interface{}{
									"type": "object",
									"properties": map[string]interface{}{
										"key": map[string]interface{}{"type": "string"},
									},
								},
							},
						},
					},
				},
			},
		},
	}
)

type testCase struct {
	// 测试用例名称
	Name string
	// 若为空则使用主模板
	Subtemplate string
	// 用于渲染的上下文
	Context any
	// 对渲染输出执行的检查
	Checks []check
}

type check interface {
	doCheck(output string, err error) error
}

type checkError string

func (c checkError) doCheck(output string, err error) error {
	if !strings.Contains(err.Error(), "error: "+string(c)) {
		return fmt.Errorf("expected error: '%v' in string:\n%v", string(c), err)
	}
	return nil
}

type checkContains string

func (c checkContains) doCheck(output string, err error) error {
	if !strings.Contains(output, string(c)) {
		return fmt.Errorf("expected substring: '%v' in string:\n%v", string(c), output)
	}
	return nil
}

type checkEquals string

func (c checkEquals) doCheck(output string, err error) error {
	if output != string(c) {
		return fmt.Errorf("output is not equal to expectation:\n%v", cmp.Diff(string(c), output))
	}
	return nil
}

func MapDict[K comparable, V any, N any](accum map[K]V, mapper func(V) N) map[K]N {
	res := make(map[K]N, len(accum))
	for k, v := range accum {
		res[k] = mapper(v)
	}
	return res
}

func ReduceDict[K comparable, V any, N any](val map[K]V, accum N, mapper func(N, K, V) N) N {
	for k, v := range val {
		accum = mapper(accum, k, v)
	}
	return accum
}

func TestPlaintext(t *testing.T) {
	testcases := []testCase{
		{
			// 测试用例：待渲染的资源在 OpenAPI schema 中不存在
			Name: "ResourceNotFound",
			Context: v2.TemplateContext{
				Document:  apiExtensionsV1OpenAPI,
				GVR:       schema.GroupVersionResource{},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkError("GVR (/, Resource=) not found in OpenAPI schema"),
			},
		},
		{
			// 测试查找 GVR 并打印其描述的基本能力
			Name: "SchemaFound",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("CustomResourceDefinition represents a resource that should be exposed"),
			},
		},
		{
			// 测试查找带命名空间的 GVR 并打印其描述的基本能力
			Name: "SchemaFoundNamespaced",
			Context: v2.TemplateContext{
				Document: batchV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "batch",
					Version:  "v1",
					Resource: "jobs",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("Job represents the configuration of a single job"),
			},
		},
		{
			// 测试查找没有 list verb 的 GVR 并打印其描述的基本能力
			Name: "SchemaFoundWithoutListVerb",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPIWithoutListVerb,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("CustomResourceDefinition represents a resource that should be exposed"),
			},
		},
		{
			// 测试查找带命名空间且没有 list verb 的 GVR 并打印其描述的基本能力
			Name: "SchemaFoundNamespacedWithoutListVerb",
			Context: v2.TemplateContext{
				Document: batchV1OpenAPIWithoutListVerb,
				GVR: schema.GroupVersionResource{
					Group:    "batch",
					Version:  "v1",
					Resource: "jobs",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("Job represents the configuration of a single job"),
			},
		},
		{
			// 测试查找带命名空间且没有顶层 list verb 的 GVR 并打印其描述的基本能力
			Name: "SchemaFoundNamespacedWithoutTopLevelListVerb",
			Context: v2.TemplateContext{
				Document: batchV1OpenAPIWithoutListVerb,
				GVR: schema.GroupVersionResource{
					Group:    "batch",
					Version:  "v1",
					Resource: "cronjobs",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("CronJob represents the configuration of a single cron job"),
			},
		},
		{
			// 测试用例：尝试在已有 schema 中查找一个不存在的
			// 字段路径
			Name: "SchemaFieldPathNotFound",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"does", "not", "exist"},
				Recursive: false,
			},
			Checks: []check{
				checkError(`field "exist" does not exist`),
			},
		},
		{
			// 测试遍历单层的标量字段路径
			Name: "SchemaFieldPathShallow",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"kind"},
				Recursive: false,
			},
			Checks: []check{
				checkContains("FIELD: kind <string>"),
			},
		},
		{
			// 测试遍历多层的标量字段路径
			Name: "SchemaFieldPathDeep",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "names", "singular"},
				Recursive: false,
			},
			Checks: []check{
				checkContains("FIELD: singular <string>"),
			},
		},
		{
			// 测试遍历多层的标量字段路径
			// 穿过数组字段
			Name: "SchemaFieldPathViaList",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "versions", "name"},
				Recursive: false,
			},
			Checks: []check{
				checkContains("FIELD: name <string>"),
			},
		},
		{
			// 测试遍历多层的标量字段路径
			// 穿过 map[string]T 字段。
			Name: "SchemaFieldPathViaMap",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "versions", "schema", "openAPIV3Schema", "properties", "default"},
				Recursive: false,
			},
			Checks: []check{
				checkContains("default is a default value for undefined object fields"),
			},
		},
		{
			// 表明遍历递归定义的 schema 完全没问题！
			Name: "SchemaFieldPathRecursive",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "versions", "schema", "openAPIV3Schema", "properties", "properties", "properties", "properties", "properties", "default"},
				Recursive: false,
			},
			Checks: []check{
				checkContains("default is a default value for undefined object fields"),
			},
		},
		{
			// 表明所有字段均已包含
			Name: "SchemaAllFields",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "versions", "schema"},
				Recursive: false,
			},
			Checks: ReduceDict(apiExtensionsV1OpenAPISpec.Components.Schemas["io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceValidation"].Properties, []check{}, func(checks []check, k string, v spec.Schema) []check {
				return append(checks, checkContains(k), checkContains(v.Description))
			}),
		},
		{
			// 表明所有字段均已包含
			Name: "SchemaAllFieldsRecursive",
			Context: v2.TemplateContext{
				Document: apiExtensionsV1OpenAPI,
				GVR: schema.GroupVersionResource{
					Group:    "apiextensions.k8s.io",
					Version:  "v1",
					Resource: "customresourcedefinitions",
				},
				FieldPath: []string{"spec", "versions", "schema"},
				Recursive: true,
			},
			Checks: ReduceDict(apiExtensionsV1OpenAPISpec.Components.Schemas["io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceValidation"].Properties, []check{}, func(checks []check, k string, v spec.Schema) []check {
				return append(checks, checkContains(k))
			}),
		},
		{
			// 表明 typeguess 模板适用于标量类型
			Name:        "Scalar",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"type": "string",
				},
			},
			Checks: []check{
				checkEquals("string"),
			},
		},
		{
			// 表明 typeguess 模板适用于布尔型 additionalProperties
			Name:        "True additionalProperties",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"type":                 "string",
					"additionalProperties": true,
				},
			},
			Checks: []check{
				checkEquals("string"),
			},
		},
		{
			// 表明指向基本类型的 $ref 使用被引用类型的类型
			Name:        "PrimitiveRef",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"$ref":        "#/components/schemas/v1.Time",
				},
				"Document": map[string]any{
					"components": map[string]any{
						"schemas": map[string]any{
							"v1.Time": map[string]any{
								"type":   "string",
								"format": "date-time",
							},
						},
					},
				},
			},
			Checks: []check{
				checkEquals("string"),
			},
		},
		{
			// 表明 typeguess 模板在给定 items 类型未知的
			// 数组时行为正确
			Name:        "ArrayUnknown",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "array",
				},
			},
			Checks: []check{
				checkEquals("array"),
			},
		},
		{
			// 表明 typeguess 将 Object 类型以首字母大写形式输出
			Name:        "ObjectTitle",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "object",
				},
			},
			Checks: []check{
				checkEquals("Object"),
			},
		},
		{
			// 表明 typeguess 模板适用于标量类型
			Name:        "ArrayOfScalar",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "array",
					"items": map[string]any{
						"type": "number",
					},
				},
			},
			Checks: []check{
				checkEquals("[]number"),
			},
		},
		{
			// 表明 typeguess 模板适用于这样的数组：其元素由
			// 仅含单个指向 $ref 的 allOf 元素的 schema
			// 指定
			Name:        "ArrayOfAllOfRef",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "array",
					"items": map[string]any{
						"type": "object",
						"allOf": []map[string]any{
							{
								"$ref": "io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceValidation",
							},
						},
					},
				},
			},
			Checks: []check{
				checkEquals("[]CustomResourceValidation"),
			},
		},
		{
			// 表明 typeguess 模板适用于这样的数组：其元素为
			// 指向 $ref 的 schema
			Name:        "ArrayOfRef",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "array",
					"items": map[string]any{
						"type": "object",
						"$ref": "io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceValidation",
					},
				},
			},
			Checks: []check{
				checkEquals("[]CustomResourceValidation"),
			},
		},
		{
			// 表明 typeguess 模板适用于标量 map 组成的数组
			Name:        "ArrayOfMap",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "array",
					"items": map[string]any{
						"type": "object",
						"additionalProperties": map[string]any{
							"type": "string",
						},
					},
				},
			},
			Checks: []check{
				checkEquals("[]map[string]string"),
			},
		},
		{
			// 表明 typeguess 模板适用于标量数组组成的 map
			Name:        "MapOfArrayOfScalar",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "object",
					"additionalProperties": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "string",
						},
					},
				},
			},
			Checks: []check{
				checkEquals("map[string][]string"),
			},
		},
		{
			// 表明 typeguess 模板适用于引用类型组成的 map
			Name:        "MapOfRef",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
					"type":        "object",
					"additionalProperties": map[string]any{
						"type": "string",
						"allOf": []map[string]any{
							{
								"$ref": "io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceValidation",
							},
						},
					},
				},
			},
			Checks: []check{
				checkEquals("map[string]CustomResourceValidation"),
			},
		},
		{
			// 表明当完全没有类型信息时，typeguess 模板会输出
			// `Object`
			Name:        "Unknown",
			Subtemplate: "typeGuess",
			Context: map[string]any{
				"schema": map[string]any{
					"description": "a cool field",
				},
			},
			Checks: []check{
				checkEquals("Object"),
			},
		},
		{
			Name:        "Required",
			Subtemplate: "fieldDetail",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "object",
					"description": "a description that should not be printed",
					"properties": map[string]any{
						"thefield": map[string]any{
							"type":        "string",
							"description": "a description that should not be printed",
						},
					},
					"required": []string{"thefield"},
				},
				"name":  "thefield",
				"short": true,
			},
			Checks: []check{
				checkEquals("thefield\t<string> -required-\n"),
			},
		},
		{
			Name:        "Description",
			Subtemplate: "fieldDetail",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "object",
					"description": "a description that should not be printed",
					"properties": map[string]any{
						"thefield": map[string]any{
							"type":        "string",
							"description": "a description that should be printed",
						},
					},
					"required": []string{"thefield"},
				},
				"name":  "thefield",
				"short": false,
			},
			Checks: []check{
				checkEquals(`thefield	<string> -required-
  a description that should be printed

`),
			},
		},
		{
			Name:        "Indent",
			Subtemplate: "fieldDetail",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "object",
					"description": "a description that should not be printed",
					"properties": map[string]any{
						"thefield": map[string]any{
							"type":        "string",
							"description": "a description that should not be printed",
						},
					},
					"required": []string{"thefield"},
				},
				"name":  "thefield",
				"short": true,
				"level": 5,
			},
			Checks: []check{
				checkEquals("          thefield\t<string> -required-\n"),
			},
		},
		{
			// 表明 fieldDetail 在 description 之后渲染 externalDocs
			Name:        "FieldExternalDocs",
			Subtemplate: "fieldDetail",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "object",
					"description": "parent description",
					"properties": map[string]any{
						"thefield": map[string]any{
							"type":        "string",
							"description": "a description that should be printed",
							"externalDocs": map[string]any{
								"description": "Find more info here",
								"url":         "https://example.com/docs",
							},
						},
					},
				},
				"name":  "thefield",
				"short": false,
			},
			Checks: []check{
				checkContains("a description that should be printed"),
				checkContains("EXTERNAL DOCS:"),
				checkContains("Find more info here"),
				checkContains("URL: https://example.com/docs"),
			},
		},
		{
			// 表明 fieldDetail 在 short 模式下不渲染 externalDocs
			Name:        "FieldExternalDocsShort",
			Subtemplate: "fieldDetail",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "object",
					"description": "parent description",
					"properties": map[string]any{
						"thefield": map[string]any{
							"type":        "string",
							"description": "a description",
							"externalDocs": map[string]any{
								"description": "Find more info here",
								"url":         "https://example.com/docs",
							},
						},
					},
				},
				"name":  "thefield",
				"short": true,
			},
			Checks: []check{
				checkEquals("thefield\t<string>\n"),
			},
		},
		{
			// 表明 extractEnum 可以跳过空的 enum 切片
			Name:        "extractEmptyEnum",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{},
				},
			},
			Checks: []check{
				checkEquals(""),
			},
		},
		{
			// 表明 extractEnum 可以提取任意 enum 切片并以大写形式排版
			Name:        "extractEnumSimpleForm",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{0, 1, 2, 3},
				},
				"isLongView": true,
			},
			Checks: []check{
				checkEquals(`ENUM:
    0
    1
    2
    3`),
			},
		},
		{
			// 表明 extractEnum 可以提取任意 enum 切片并以小写形式排版
			Name:        "extractEnumLongFormWithIndent",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{0, 1, 2, 3},
				},
				"isLongView":   false,
				"indentAmount": 2,
			},
			Checks: []check{
				checkEquals(`
  enum: 0, 1, 2, 3`),
			},
		},
		{
			// 表明 extractEnum 可以提取任意 enum 切片并在枚举被截断时排版
			Name:        "extractEnumLongFormWithLimitAndIndent",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{0, 1, 2, 3},
				},
				"isLongView":   false,
				"limit":        2,
				"indentAmount": 2,
			},
			Checks: []check{
				checkEquals(`
  enum: 0, 1, ....`),
			},
		},
		{
			// 表明 extractEnum 可以提取任意 enum 切片并在枚举被截断时排版
			Name:        "extractEnumSimpleFormWithLimitAndIndent",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{0, 1, 2, 3},
				},
				"isLongView":   true,
				"limit":        2,
				"indentAmount": 2,
			},
			Checks: []check{
				checkEquals(`ENUM:
    0
    1, ....`),
			},
		},
		{
			// 表明 extractEnum 可以提取任意 enum 切片并处理空字符串
			Name:        "extractEnumSimpleFormEmptyString",
			Subtemplate: "extractEnum",
			Context: map[string]any{
				"schema": map[string]any{
					"type":        "string",
					"description": "a description that should not be printed",
					"enum":        []any{"Block", "File", ""},
				},
				"isLongView": true,
			},
			Checks: []check{
				checkEquals("ENUM:\n    Block\n    File\n    \"\""),
			},
		},
		{
			// 表明 externalDocs 会同时渲染 description 和 URL
			Name:        "ExternalDocsDescriptionAndURL",
			Subtemplate: "externalDocs",
			Context: map[string]any{
				"schema": map[string]any{
					"externalDocs": map[string]any{
						"description": "Find more info here",
						"url":         "https://example.com/docs",
					},
				},
			},
			Checks: []check{
				checkContains("EXTERNAL DOCS:"),
				checkContains("Find more info here"),
				checkContains("URL: https://example.com/docs"),
			},
		},
		{
			// 表明当 description 缺失时，externalDocs 仍会渲染 URL
			Name:        "ExternalDocsURLOnly",
			Subtemplate: "externalDocs",
			Context: map[string]any{
				"schema": map[string]any{
					"externalDocs": map[string]any{
						"url": "https://example.com/docs",
					},
				},
			},
			Checks: []check{
				checkContains("EXTERNAL DOCS:"),
				checkContains("URL: https://example.com/docs"),
			},
		},
		{
			// 表明 externalDocs 缺失时不渲染任何内容
			Name:        "ExternalDocsAbsent",
			Subtemplate: "externalDocs",
			Context: map[string]any{
				"schema": map[string]any{
					"type": "object",
				},
			},
			Checks: []check{
				checkEquals(""),
			},
		},
		{
			// 集成测试：表明 externalDocs 出现在完整输出中
			Name: "SchemaWithExternalDocs",
			Context: v2.TemplateContext{
				Document: map[string]any{
					"paths": map[string]any{
						"/apis/example.com/v1/widgets": map[string]any{
							"get": map[string]any{
								"x-kubernetes-group-version-kind": map[string]any{
									"group":   "example.com",
									"version": "v1",
									"kind":    "Widget",
								},
							},
						},
					},
					"components": map[string]any{
						"schemas": map[string]any{
							"com.example.v1.Widget": map[string]any{
								"type":        "object",
								"description": "A widget is a thing.",
								"x-kubernetes-group-version-kind": []map[string]any{
									{
										"group":   "example.com",
										"version": "v1",
										"kind":    "Widget",
									},
								},
								"externalDocs": map[string]any{
									"description": "Find more info here",
									"url":         "https://example.com/docs/widgets",
								},
								"properties": map[string]any{
									"apiVersion": map[string]any{
										"type": "string",
									},
									"kind": map[string]any{
										"type": "string",
									},
								},
							},
						},
					},
				},
				GVR: schema.GroupVersionResource{
					Group:    "example.com",
					Version:  "v1",
					Resource: "widgets",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("DESCRIPTION:"),
				checkContains("A widget is a thing."),
				checkContains("EXTERNAL DOCS:"),
				checkContains("Find more info here"),
				checkContains("URL: https://example.com/docs/widgets"),
			},
		},
		{
			// 集成测试：表明子字段上的 externalDocs 出现在字段列表中
			Name: "SchemaWithFieldExternalDocs",
			Context: v2.TemplateContext{
				Document: map[string]any{
					"paths": map[string]any{
						"/apis/example.com/v1/widgets": map[string]any{
							"get": map[string]any{
								"x-kubernetes-group-version-kind": map[string]any{
									"group":   "example.com",
									"version": "v1",
									"kind":    "Widget",
								},
							},
						},
					},
					"components": map[string]any{
						"schemas": map[string]any{
							"com.example.v1.Widget": map[string]any{
								"type":        "object",
								"description": "A widget is a thing.",
								"x-kubernetes-group-version-kind": []map[string]any{
									{
										"group":   "example.com",
										"version": "v1",
										"kind":    "Widget",
									},
								},
								"properties": map[string]any{
									"name": map[string]any{
										"type":        "string",
										"description": "name of the widget",
										"externalDocs": map[string]any{
											"description": "Widget naming conventions",
											"url":         "https://example.com/docs/naming",
										},
									},
									"size": map[string]any{
										"type":        "integer",
										"description": "size of the widget",
									},
								},
							},
						},
					},
				},
				GVR: schema.GroupVersionResource{
					Group:    "example.com",
					Version:  "v1",
					Resource: "widgets",
				},
				FieldPath: nil,
				Recursive: false,
			},
			Checks: []check{
				checkContains("FIELDS:"),
				checkContains("name of the widget"),
				checkContains("EXTERNAL DOCS:"),
				checkContains("Widget naming conventions"),
				checkContains("URL: https://example.com/docs/naming"),
				checkContains("size of the widget"),
			},
		},
		{
			Name: "MaxDepthZeroIsUnlimited",
			Context: v2.TemplateContext{
				Document:  nestedWidgetOpenAPI,
				GVR:       nestedWidgetGVR,
				FieldPath: nil,
				Recursive: true,
				MaxDepth:  0,
			},
			Checks: []check{
				// 非递归的字段详情会用空行分隔描述，
				// 而根模板会输出最后的结尾换行符。
				checkEquals(`GROUP:      example.com
KIND:       Widget
VERSION:    v1

DESCRIPTION:
    <empty>
FIELDS:
  meta	<Object>
    id	<string>
    tags	<Object>
      key	<string>
  name	<string>

`),
			},
		},
		{
			Name: "MaxDepthOneStopsAtTopLevelFields",
			Context: v2.TemplateContext{
				Document:  nestedWidgetOpenAPI,
				GVR:       nestedWidgetGVR,
				FieldPath: nil,
				Recursive: true,
				MaxDepth:  1,
			},
			Checks: []check{
				checkEquals(`GROUP:      example.com
KIND:       Widget
VERSION:    v1

DESCRIPTION:
    <empty>
FIELDS:
  meta	<Object>
  name	<string>

`),
			},
		},
		{
			Name: "MaxDepthTwoExpandsOneLevelDeeper",
			Context: v2.TemplateContext{
				Document:  nestedWidgetOpenAPI,
				GVR:       nestedWidgetGVR,
				FieldPath: nil,
				Recursive: true,
				MaxDepth:  2,
			},
			Checks: []check{
				checkEquals(`GROUP:      example.com
KIND:       Widget
VERSION:    v1

DESCRIPTION:
    <empty>
FIELDS:
  meta	<Object>
    id	<string>
    tags	<Object>
  name	<string>

`),
			},
		},
		{
			Name: "MaxDepthLargerThanSchemaEqualsUnlimited",
			Context: v2.TemplateContext{
				Document:  nestedWidgetOpenAPI,
				GVR:       nestedWidgetGVR,
				FieldPath: nil,
				Recursive: true,
				MaxDepth:  100,
			},
			Checks: []check{
				checkEquals(`GROUP:      example.com
KIND:       Widget
VERSION:    v1

DESCRIPTION:
    <empty>
FIELDS:
  meta	<Object>
    id	<string>
    tags	<Object>
      key	<string>
  name	<string>

`),
			},
		},
		{
			Name: "MaxDepthIgnoredWhenRecursiveFalse",
			Context: v2.TemplateContext{
				Document:  nestedWidgetOpenAPI,
				GVR:       nestedWidgetGVR,
				FieldPath: nil,
				Recursive: false,
				MaxDepth:  5,
			},
			Checks: []check{
				checkEquals(`GROUP:      example.com
KIND:       Widget
VERSION:    v1

DESCRIPTION:
    <empty>
FIELDS:
  meta	<Object>
    <no description>

  name	<string>
    <no description>


`),
			},
		},
	}

	tmpl, err := v2.WithBuiltinTemplateFuncs(template.New("")).Parse(plaintextSource)
	require.NoError(t, err)

	for _, tcase := range testcases {
		testName := tcase.Name
		if len(tcase.Subtemplate) > 0 {
			testName = tcase.Subtemplate + "/" + testName
		}

		t.Run(testName, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)

			var outputErr error
			if len(tcase.Subtemplate) == 0 {
				outputErr = tmpl.Execute(buf, tcase.Context)
			} else {
				outputErr = tmpl.ExecuteTemplate(buf, tcase.Subtemplate, tcase.Context)
			}

			output := buf.String()
			for _, check := range tcase.Checks {
				err = check.doCheck(output, outputErr)

				if err != nil {
					t.Log("test failed on output:\n" + output)
					require.NoError(t, err)
				}
			}
		})
	}
}
