/*
Copyright 2019 The Kubernetes Authors.

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

package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/utils/ptr"
)

// TestNewBuilder 验证内部构建器为 CRD 生成的 schema 与 list schema。
func TestNewBuilder(t *testing.T) {
	tests := []struct {
		name string

		schema string

		wantedSchema      string
		wantedItemsSchema string

		v2                bool // 是否生成 OpenAPIv2
		includeSelectable bool // 是否包含可选字段
		version           string
	}{
		{
			"nil",
			"",
			`{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`, `{"$ref":"#/definitions/io.k8s.bar.v1.Foo"}`,
			true,
			false,
			"v1",
		},
		{"with properties",
			`{"type":"object","properties":{"spec":{"type":"object"},"status":{"type":"object"}}}`,
			`{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"spec":{"type":"object"},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			`{"$ref":"#/definitions/io.k8s.bar.v1.Foo"}`,
			true,
			false,
			"v1",
		},
		{"type only",
			`{"type":"object"}`,
			`{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			`{"$ref":"#/definitions/io.k8s.bar.v1.Foo"}`,
			true,
			false,
			"v1",
		},
		{"preserve unknown at root v2",
			`{"type":"object","x-kubernetes-preserve-unknown-fields":true}`,
			`{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			`{"$ref":"#/definitions/io.k8s.bar.v1.Foo"}`,
			true,
			false,
			"v1",
		},
		{"with extensions",
			`
{
  "type":"object",
  "properties": {
    "int-or-string-1": {
      "x-kubernetes-int-or-string": true,
      "anyOf": [
        {"type":"integer"},
        {"type":"string"}
      ]
    },
    "int-or-string-2": {
      "x-kubernetes-int-or-string": true,
      "allOf": [{
        "anyOf": [
          {"type":"integer"},
          {"type":"string"}
        ]
      }, {
        "anyOf": [
          {"minimum": 42.0}
        ]
      }]
    },
    "int-or-string-3": {
      "x-kubernetes-int-or-string": true,
      "anyOf": [
        {"type":"integer"},
        {"type":"string"}
      ],
      "allOf": [{
        "anyOf": [
          {"minimum": 42.0}
        ]
      }]
    },
    "int-or-string-4": {
      "x-kubernetes-int-or-string": true,
      "anyOf": [
        {"minimum": 42.0}
      ]
    },
    "int-or-string-5": {
      "x-kubernetes-int-or-string": true,
      "anyOf": [
        {"minimum": 42.0}
      ],
      "allOf": [
        {"minimum": 42.0}
      ]
    },
    "int-or-string-6": {
      "x-kubernetes-int-or-string": true
    },
    "preserve-unknown-fields": {
      "x-kubernetes-preserve-unknown-fields": true
    },
    "embedded-object": {
      "x-kubernetes-embedded-resource": true,
      "x-kubernetes-preserve-unknown-fields": true,
      "type": "object"
    }
  }
}`,
			`
{
  "type":"object",
  "properties": {
    "apiVersion": {"type":"string"},
    "kind": {"type":"string"},
    "metadata": {"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
    "int-or-string-1": {
      "x-kubernetes-int-or-string": true
    },
    "int-or-string-2": {
      "x-kubernetes-int-or-string": true
    },
    "int-or-string-3": {
      "x-kubernetes-int-or-string": true
    },
    "int-or-string-4": {
      "x-kubernetes-int-or-string": true
    },
    "int-or-string-5": {
      "x-kubernetes-int-or-string": true
    },
    "int-or-string-6": {
      "x-kubernetes-int-or-string": true
    },
    "preserve-unknown-fields": {
      "x-kubernetes-preserve-unknown-fields": true
    },
    "embedded-object": {
      "x-kubernetes-embedded-resource": true,
      "x-kubernetes-preserve-unknown-fields": true
    }
  },
  "x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]
}`,
			`{"$ref":"#/definitions/io.k8s.bar.v1.Foo"}`,
			true,
			false,
			"v1",
		},
		{
			"include selectable fields with different version",
			`{"type":"object","properties":{"spec":{"type":"object"},"status":{"type":"object"}}}`,
			`{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"spec":{"type":"object"},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v2"}]}`,
			`{"$ref":"#/definitions/io.k8s.bar.v2.Foo"}`,
			true,
			true,
			"v2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var schema *structuralschema.Structural
			if len(tt.schema) > 0 {
				v1beta1Schema := &apiextensionsv1.JSONSchemaProps{}
				if err := json.Unmarshal([]byte(tt.schema), &v1beta1Schema); err != nil {
					t.Fatal(err)
				}
				internalSchema := &apiextensionsinternal.JSONSchemaProps{}
				apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v1beta1Schema, internalSchema, nil)
				var err error
				schema, err = structuralschema.NewStructural(internalSchema)
				if err != nil {
					t.Fatalf("structural schema error: %v", err)
				}
				if errs := structuralschema.ValidateStructural(nil, schema); len(errs) > 0 {
					t.Fatalf("structural schema validation error: %v", errs.ToAggregate())
				}
				// 展开复用定义，否则构建器看到的仍是未折叠结构
				schema = schema.Unfold()
			}

			got := newBuilder(&apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Group: "bar.k8s.io",
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{
							Name: "v1",
						},
					},
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Plural:   "foos",
						Singular: "foo",
						Kind:     "Foo",
						ListKind: "FooList",
					},
					Scope: apiextensionsv1.NamespaceScoped,
				},
			}, tt.version, schema, Options{V2: tt.v2, IncludeSelectableFields: tt.includeSelectable})

			var wantedSchema, wantedItemsSchema spec.Schema
			if err := json.Unmarshal([]byte(tt.wantedSchema), &wantedSchema); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tt.wantedItemsSchema), &wantedItemsSchema); err != nil {
				t.Fatal(err)
			}

			gotProperties := properties(got.schema.Properties)
			wantedProperties := properties(wantedSchema.Properties)
			if !gotProperties.Equal(wantedProperties) {
				t.Fatalf("unexpected properties, got: %s, expected: %s", gotProperties.List(), wantedProperties.List())
			}

			// 抹掉 TypeMeta/ObjectMeta 的内容及其大段描述，这里信任这些字段一致。
			for _, metaField := range []string{"kind", "apiVersion", "metadata"} {
				if _, found := got.schema.Properties["kind"]; found {
					prop := got.schema.Properties[metaField]
					prop.Description = ""
					got.schema.Properties[metaField] = prop
				}
			}

			if !reflect.DeepEqual(&wantedSchema, got.schema) {
				t.Errorf("unexpected schema: %s\nwant = %#v\ngot = %#v", schemaDiff(&wantedSchema, got.schema), &wantedSchema, got.schema)
			}

			gotListProperties := properties(got.listSchema.Properties)
			if want := sets.NewString("apiVersion", "kind", "metadata", "items"); !gotListProperties.Equal(want) {
				t.Fatalf("unexpected list properties, got: %s, expected: %s", gotListProperties.List(), want.List())
			}

			if e, a := (spec.StringOrArray{"string"}), got.listSchema.Properties["apiVersion"].Type; !reflect.DeepEqual(e, a) {
				t.Errorf("expected %#v, got %#v", e, a)
			}
			if e, a := (spec.StringOrArray{"string"}), got.listSchema.Properties["kind"].Type; !reflect.DeepEqual(e, a) {
				t.Errorf("expected %#v, got %#v", e, a)
			}
			listRef := got.listSchema.Properties["metadata"].Ref
			if e, a := "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ListMeta", (&listRef).String(); e != a {
				t.Errorf("expected %q, got %q", e, a)
			}

			gotListSchema := got.listSchema.Properties["items"].Items.Schema
			if !reflect.DeepEqual(&wantedItemsSchema, gotListSchema) {
				t.Errorf("unexpected list schema:\n%s", schemaDiff(&wantedItemsSchema, gotListSchema))
			}
		})
	}
}

// TestCRDRouteParameterBuilder 验证命名空间/集群作用域的 CRD 路由参数与受支持动作。
func TestCRDRouteParameterBuilder(t *testing.T) {
	testCRDKind := "Foo"
	testCRDGroup := "foo-group"
	testCRDVersion := "foo-version"
	testCRDResourceName := "foos"

	testCases := []struct {
		scope apiextensionsv1.ResourceScope
		paths map[string]struct {
			expectNamespaceParam bool
			expectNameParam      bool
			expectedActions      sets.String
		}
	}{
		{
			scope: apiextensionsv1.NamespaceScoped,
			paths: map[string]struct {
				expectNamespaceParam bool
				expectNameParam      bool
				expectedActions      sets.String
			}{
				"/apis/foo-group/foo-version/foos":                                      {expectNamespaceParam: false, expectNameParam: false, expectedActions: sets.NewString("list")},
				"/apis/foo-group/foo-version/namespaces/{namespace}/foos":               {expectNamespaceParam: true, expectNameParam: false, expectedActions: sets.NewString("post", "list", "deletecollection")},
				"/apis/foo-group/foo-version/namespaces/{namespace}/foos/{name}":        {expectNamespaceParam: true, expectNameParam: true, expectedActions: sets.NewString("get", "put", "patch", "delete")},
				"/apis/foo-group/foo-version/namespaces/{namespace}/foos/{name}/scale":  {expectNamespaceParam: true, expectNameParam: true, expectedActions: sets.NewString("get", "patch", "put")},
				"/apis/foo-group/foo-version/namespaces/{namespace}/foos/{name}/status": {expectNamespaceParam: true, expectNameParam: true, expectedActions: sets.NewString("get", "patch", "put")},
			},
		},
		{
			scope: apiextensionsv1.ClusterScoped,
			paths: map[string]struct {
				expectNamespaceParam bool
				expectNameParam      bool
				expectedActions      sets.String
			}{
				"/apis/foo-group/foo-version/foos":               {expectNamespaceParam: false, expectNameParam: false, expectedActions: sets.NewString("post", "list", "deletecollection")},
				"/apis/foo-group/foo-version/foos/{name}":        {expectNamespaceParam: false, expectNameParam: true, expectedActions: sets.NewString("get", "put", "patch", "delete")},
				"/apis/foo-group/foo-version/foos/{name}/scale":  {expectNamespaceParam: false, expectNameParam: true, expectedActions: sets.NewString("get", "patch", "put")},
				"/apis/foo-group/foo-version/foos/{name}/status": {expectNamespaceParam: false, expectNameParam: true, expectedActions: sets.NewString("get", "patch", "put")},
			},
		},
	}

	for _, testCase := range testCases {
		testNamespacedCRD := &apiextensionsv1.CustomResourceDefinition{
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Scope: testCase.scope,
				Group: testCRDGroup,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Kind:   testCRDKind,
					Plural: testCRDResourceName,
				},
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name: testCRDVersion,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
							Scale:  &apiextensionsv1.CustomResourceSubresourceScale{},
						},
					},
				},
			},
		}
		swagger, err := BuildOpenAPIV2(testNamespacedCRD, testCRDVersion, Options{V2: true})
		require.NoError(t, err)
		require.Equal(t, len(testCase.paths), len(swagger.Paths.Paths), testCase.scope)
		for path, expected := range testCase.paths {
			t.Run(path, func(t *testing.T) {
				path, ok := swagger.Paths.Paths[path]
				if !ok {
					t.Errorf("unexpected path %v", path)
				}

				hasNamespaceParam := false
				hasNameParam := false
				for _, param := range path.Parameters {
					if strings.HasPrefix(param.Ref.String(), "#/parameters/namespace-") {
						hasNamespaceParam = true
					}
					if param.In == "path" && param.Name == "name" {
						hasNameParam = true
					}
				}
				assert.Equal(t, expected.expectNamespaceParam, hasNamespaceParam)
				assert.Equal(t, expected.expectNameParam, hasNameParam)

				actions := sets.NewString()
				for _, operation := range []*spec.Operation{path.Get, path.Post, path.Put, path.Patch, path.Delete} {
					if operation != nil {
						action, ok := operation.VendorExtensible.Extensions.GetString(endpoints.RouteMetaAction)
						if ok {
							actions.Insert(action)
						}
						if action == "patch" {
							expected := []string{"application/json-patch+json", "application/merge-patch+json", "application/apply-patch+yaml"}
							assert.Equal(t, expected, operation.Consumes)
						} else {
							assert.Equal(t, []string{"application/json", "application/yaml"}, operation.Consumes)
						}
					}
				}
				assert.Equal(t, expected.expectedActions, actions)
			})
		}
	}
}

// properties 返回 schema 属性名的集合，用于比较属性键而忽略值。
func properties(p map[string]spec.Schema) sets.String {
	ret := sets.NewString()
	for k := range p {
		ret.Insert(k)
	}
	return ret
}

// schemaDiff 比较两个 schema 并返回差异。
func schemaDiff(a, b *spec.Schema) string {
	// 该选项允许比较所有字段，包括未导出字段。
	return cmp.Diff(a, b, cmp.Exporter(func(reflect.Type) bool { return true }))
}

// TestBuildOpenAPIV2 验证 BuildOpenAPIV2 的 schema 生成及 selectable fields 行为。
func TestBuildOpenAPIV2(t *testing.T) {
	tests := []struct {
		name                  string
		schema                string
		preserveUnknownFields *bool
		wantedSchema          string
		opts                  Options
		selectableFields      []apiextensionsv1.SelectableField
	}{
		{
			name:         "nil",
			wantedSchema: `{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:         Options{V2: true},
		},
		{
			name:         "with properties",
			schema:       `{"type":"object","properties":{"spec":{"type":"object"},"status":{"type":"object"}}}`,
			wantedSchema: `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"spec":{"type":"object"},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:         Options{V2: true},
		},
		{
			name:         "with invalid-typed properties",
			schema:       `{"type":"object","properties":{"spec":{"type":"bug"},"status":{"type":"object"}}}`,
			wantedSchema: `{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:         Options{V2: true},
		},
		{
			name:         "with non-structural schema",
			schema:       `{"type":"object","properties":{"foo":{"type":"array"}}}`,
			wantedSchema: `{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:         Options{V2: true},
		},
		{
			name:                  "with spec.preseveUnknownFields=true",
			schema:                `{"type":"object","properties":{"foo":{"type":"string"}}}`,
			preserveUnknownFields: ptr.To(true),
			wantedSchema:          `{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:                  Options{V2: true},
		},
		{
			name:         "v2",
			schema:       `{"type":"object","properties":{"foo":{"type":"string","oneOf":[{"pattern":"a"},{"pattern":"b"}]}}}`,
			wantedSchema: `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"foo":{"type":"string"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:         Options{V2: true},
		},
		{
			name:             "with selectable fields enabled",
			schema:           `{"type":"object","properties":{"foo":{"type":"string"}}}`,
			wantedSchema:     `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"foo":{"type":"string"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}], "x-kubernetes-selectable-fields": [{"fieldPath":"foo"}]}`,
			opts:             Options{V2: true, IncludeSelectableFields: true},
			selectableFields: []apiextensionsv1.SelectableField{{JSONPath: "foo"}},
		},
		{
			name:             "with selectable fields disabled",
			schema:           `{"type":"object","properties":{"foo":{"type":"string"}}}`,
			wantedSchema:     `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"$ref":"#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},"foo":{"type":"string"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			opts:             Options{V2: true},
			selectableFields: []apiextensionsv1.SelectableField{{JSONPath: "foo"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var validation *apiextensionsv1.CustomResourceValidation
			if len(tt.schema) > 0 {
				v1Schema := &apiextensionsv1.JSONSchemaProps{}
				if err := json.Unmarshal([]byte(tt.schema), &v1Schema); err != nil {
					t.Fatal(err)
				}
				validation = &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: v1Schema,
				}
			}
			if tt.preserveUnknownFields != nil && *tt.preserveUnknownFields {
				validation.OpenAPIV3Schema.XPreserveUnknownFields = ptr.To(true)
			}

			// TODO: 大部分复制自上一个测试，重构以消除重复
			got, err := BuildOpenAPIV2(&apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Group: "bar.k8s.io",
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{
							Name:             "v1",
							Schema:           validation,
							SelectableFields: tt.selectableFields,
						},
					},
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Plural:   "foos",
						Singular: "foo",
						Kind:     "Foo",
						ListKind: "FooList",
					},
					Scope: apiextensionsv1.NamespaceScoped,
				},
			}, "v1", tt.opts)
			if err != nil {
				t.Fatal(err)
			}

			var wantedSchema spec.Schema
			if err := json.Unmarshal([]byte(tt.wantedSchema), &wantedSchema); err != nil {
				t.Fatal(err)
			}

			gotSchema := got.Definitions["io.k8s.bar.v1.Foo"]
			gotProperties := properties(gotSchema.Properties)
			wantedProperties := properties(wantedSchema.Properties)
			if !gotProperties.Equal(wantedProperties) {
				t.Fatalf("unexpected properties, got: %s, expected: %s", gotProperties.List(), wantedProperties.List())
			}

			// 抹掉 TypeMeta/ObjectMeta 的内容及其大段描述，这里信任这些字段一致。
			for _, metaField := range []string{"kind", "apiVersion", "metadata"} {
				if _, found := gotSchema.Properties["kind"]; found {
					prop := gotSchema.Properties[metaField]
					prop.Description = ""
					gotSchema.Properties[metaField] = prop
				}
			}

			if !reflect.DeepEqual(&wantedSchema, &gotSchema) {
				t.Errorf("unexpected schema: %s\nwant = %#v\ngot = %#v", schemaDiff(&wantedSchema, &gotSchema), &wantedSchema, &gotSchema)
			}
		})
	}
}

// TestBuildOpenAPIV3 验证 BuildOpenAPIV3 的 schema 生成、nullable/default 及引用前缀。
func TestBuildOpenAPIV3(t *testing.T) {
	tests := []struct {
		name                  string
		schema                string
		preserveUnknownFields *bool
		wantedSchema          string
		opts                  Options
		selectableFields      []apiextensionsv1.SelectableField
	}{
		{
			name:         "nil",
			wantedSchema: `{"type":"object","x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
		},
		{
			name:         "with properties",
			schema:       `{"type":"object","properties":{"spec":{"type":"object"},"status":{"type":"object"}}}`,
			wantedSchema: `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"allOf":[{"$ref":"#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]},"spec":{"type":"object"},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
		},
		{
			name:         "with v3 nullable field",
			schema:       `{"type":"object","properties":{"spec":{"type":"object", "nullable": true},"status":{"type":"object"}}}`,
			wantedSchema: `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"allOf":[{"$ref":"#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]},"spec":{"type":"object", "nullable": true},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
		},
		{
			name:         "with default not pruned for v3",
			schema:       `{"type":"object","properties":{"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}}}`,
			wantedSchema: `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"allOf":[{"$ref":"#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]},"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
		},
		{
			name:             "with selectable fields enabled",
			schema:           `{"type":"object","properties":{"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}}}`,
			wantedSchema:     `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"allOf":[{"$ref":"#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]},"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}], "x-kubernetes-selectable-fields": [{"fieldPath":"spec.field"}]}`,
			opts:             Options{IncludeSelectableFields: true},
			selectableFields: []apiextensionsv1.SelectableField{{JSONPath: "spec.field"}},
		},
		{
			name:             "with selectable fields disabled",
			schema:           `{"type":"object","properties":{"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}}}`,
			wantedSchema:     `{"type":"object","properties":{"apiVersion":{"type":"string"},"kind":{"type":"string"},"metadata":{"allOf":[{"$ref":"#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]},"spec":{"type":"object","properties":{"field":{"type":"string","default":"foo"}}},"status":{"type":"object"}},"x-kubernetes-group-version-kind":[{"group":"bar.k8s.io","kind":"Foo","version":"v1"}]}`,
			selectableFields: []apiextensionsv1.SelectableField{{JSONPath: "spec.field"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var validation *apiextensionsv1.CustomResourceValidation
			if len(tt.schema) > 0 {
				v1Schema := &apiextensionsv1.JSONSchemaProps{}
				if err := json.Unmarshal([]byte(tt.schema), &v1Schema); err != nil {
					t.Fatal(err)
				}
				validation = &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: v1Schema,
				}
			}
			if tt.preserveUnknownFields != nil && *tt.preserveUnknownFields {
				validation.OpenAPIV3Schema.XPreserveUnknownFields = ptr.To(true)
			}

			got, err := BuildOpenAPIV3(&apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Group: "bar.k8s.io",
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{
							Name:             "v1",
							Schema:           validation,
							SelectableFields: tt.selectableFields,
						},
					},
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Plural:   "foos",
						Singular: "foo",
						Kind:     "Foo",
						ListKind: "FooList",
					},
					Scope: apiextensionsv1.NamespaceScoped,
				},
			}, "v1", tt.opts)
			if err != nil {
				t.Fatal(err)
			}

			var wantedSchema spec.Schema
			if err := json.Unmarshal([]byte(tt.wantedSchema), &wantedSchema); err != nil {
				t.Fatal(err)
			}

			gotSchema := *got.Components.Schemas["io.k8s.bar.v1.Foo"]
			listSchemaRef := got.Components.Schemas["io.k8s.bar.v1.FooList"].Properties["items"].Items.Schema.Ref.String()
			if strings.Contains(listSchemaRef, "#/definitions/") || !strings.Contains(listSchemaRef, "#/components/schemas/") {
				t.Errorf("Expected list schema ref to contain #/components/schemas/ prefix. Got %s", listSchemaRef)
			}
			gotProperties := properties(gotSchema.Properties)
			wantedProperties := properties(wantedSchema.Properties)
			if !gotProperties.Equal(wantedProperties) {
				t.Fatalf("unexpected properties, got: %s, expected: %s", gotProperties.List(), wantedProperties.List())
			}

			// 抹掉 TypeMeta/ObjectMeta 的内容及其大段描述，这里信任这些字段一致。
			for _, metaField := range []string{"kind", "apiVersion", "metadata"} {
				if _, found := gotSchema.Properties["kind"]; found {
					prop := gotSchema.Properties[metaField]
					prop.Description = ""
					gotSchema.Properties[metaField] = prop
				}
			}

			if !reflect.DeepEqual(&wantedSchema, &gotSchema) {
				t.Errorf("unexpected schema: %s\nwant = %#v\ngot = %#v", schemaDiff(&wantedSchema, &gotSchema), &wantedSchema, &gotSchema)
			}
		})
	}
}

// TestGetDefinitionRefPrefix 测试 getDefinition 的引用构造在 v2 与 v3 操作下是否正确使用对应前缀。
// 此前未暴露该 bug，是因为至今只查询不使用引用的类型。
func TestGetDefinitionRefPrefix(t *testing.T) {
	// 该 bug 在为某一版本生成缓存定义映射后，又在另一版本上查找时触发；
	// 映射在首次调用 getDefinition 时生成。

	// ManagedFieldsEntry 的 Time 字段已知会使用引用
	managedFieldsTypePath := "io.k8s.apimachinery.pkg.apis.meta.v1.ManagedFieldsEntry"

	v2Ref := getDefinition(managedFieldsTypePath, true).SchemaProps.Properties["time"].SchemaProps.Ref
	v3Ref := getDefinition(managedFieldsTypePath, false).SchemaProps.Properties["time"].SchemaProps.Ref

	v2String := v2Ref.String()
	v3String := v3Ref.String()

	if !strings.HasPrefix(v3String, v3DefinitionPrefix) {
		t.Errorf("v3 ref (%v) does not have the correct prefix (%v)", v3String, v3DefinitionPrefix)
	}

	if !strings.HasPrefix(v2String, definitionPrefix) {
		t.Errorf("v2 ref (%v) does not have the correct prefix (%v)", v2String, definitionPrefix)
	}
}
