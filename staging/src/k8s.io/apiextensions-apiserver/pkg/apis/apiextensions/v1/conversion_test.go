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

package v1

import (
	"encoding/json"
	fmt "fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// TestConversion 验证内部类型与 v1 之间的字段迁移和合并逻辑。
func TestConversion(t *testing.T) {
	testcases := []struct {
		Name      string
		In        runtime.Object
		Out       runtime.Object
		ExpectOut runtime.Object
		ExpectErr string
	}{
		// 版本
		{
			Name:      "internal to v1, no versions",
			In:        &apiextensions.CustomResourceDefinition{},
			Out:       &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{},
		},
		{
			Name: "internal to v1, top-level version",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{{Name: "v1", Served: true, Storage: true}},
				},
			},
		},
		{
			Name: "internal to v1, multiple versions",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: false, Storage: false},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: false, Storage: false},
					},
				},
			},
		},
		{
			Name: "v1 to internal, no versions",
			In:   &CustomResourceDefinition{},
			Out:  &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, single version",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{{Name: "v1", Served: true, Storage: true}},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version:               "v1",
					Versions:              []apiextensions.CustomResourceDefinitionVersion{{Name: "v1", Served: true, Storage: true}},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, multiple versions",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: false, Storage: false},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: false, Storage: false},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		// 校验
		{
			Name: "internal to v1, top-level validation moves to per-version",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version:    "v1",
					Validation: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Type: "object"}},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Type: "object"}}},
					},
				},
			},
		},
		{
			Name: "internal to v1, per-version validation is preserved",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "v1", Type: "object"}}},
						{Name: "v2", Served: false, Storage: false, Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "v2", Type: "object"}}},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Description: "v1", Type: "object"}}},
						{Name: "v2", Served: false, Storage: false, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Description: "v2", Type: "object"}}},
					},
				},
			},
		},
		{
			Name: "v1 to internal, identical validation moves to top-level",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Type: "object"}}},
						{Name: "v2", Served: true, Storage: false, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Type: "object"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: true, Storage: false},
					},
					Validation:            &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Type: "object"}},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, distinct validation remains per-version",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Description: "v1", Type: "object"}}},
						{Name: "v2", Served: true, Storage: false, Schema: &CustomResourceValidation{OpenAPIV3Schema: &JSONSchemaProps{Description: "v2", Type: "object"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "v1", Type: "object"}}},
						{Name: "v2", Served: true, Storage: false, Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Description: "v2", Type: "object"}}},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		// 子资源
		{
			Name: "internal to v1, top-level subresources moves to per-version",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version:      "v1",
					Subresources: &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas"}},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas"}}},
					},
				},
			},
		},
		{
			Name: "internal to v1, per-version subresources is preserved",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas1"}}},
						{Name: "v2", Served: false, Storage: false, Subresources: &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas2"}}},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas1"}}},
						{Name: "v2", Served: false, Storage: false, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas2"}}},
					},
				},
			},
		},
		{
			Name: "v1 to internal, identical subresources moves to top-level",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas"}}},
						{Name: "v2", Served: true, Storage: false, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: true, Storage: false},
					},
					Subresources:          &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas"}},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, distinct subresources remains per-version",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas1"}}},
						{Name: "v2", Served: true, Storage: false, Subresources: &CustomResourceSubresources{Scale: &CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas2"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, Subresources: &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas1"}}},
						{Name: "v2", Served: true, Storage: false, Subresources: &apiextensions.CustomResourceSubresources{Scale: &apiextensions.CustomResourceSubresourceScale{SpecReplicasPath: "spec.replicas2"}}},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		// 附加打印列
		{
			Name: "internal to v1, top-level printer columns moves to per-version",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version:                  "v1",
					AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column1"}},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column1"}}},
					},
				},
			},
		},
		{
			Name: "internal to v1, per-version printer columns is preserved",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column1"}}},
						{Name: "v2", Served: false, Storage: false, AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column2"}}},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column1"}}},
						{Name: "v2", Served: false, Storage: false, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column2"}}},
					},
				},
			},
		},
		{
			Name: "v1 to internal, identical printer columns moves to top-level",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column1"}}},
						{Name: "v2", Served: true, Storage: false, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column1"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: true, Storage: false},
					},
					AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column1"}},
					PreserveUnknownFields:    ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, distinct printer columns remains per-version",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column1"}}},
						{Name: "v2", Served: true, Storage: false, AdditionalPrinterColumns: []CustomResourceColumnDefinition{{Name: "column2"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column1"}}},
						{Name: "v2", Served: true, Storage: false, AdditionalPrinterColumns: []apiextensions.CustomResourceColumnDefinition{{Name: "column2"}}},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		// 可选字段
		{
			Name: "internal to v1, top-level selectable fields moves to per-version",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version:          "v1",
					SelectableFields: []apiextensions.SelectableField{{JSONPath: ".spec.x"}},
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
					},
				},
			},
		},
		{
			Name: "internal to v1, per-version selectable fields is preserved",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []apiextensions.SelectableField{{JSONPath: ".spec.x"}}},
						{Name: "v2", Served: false, Storage: false, SelectableFields: []apiextensions.SelectableField{{JSONPath: ".spec.y"}}},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
						{Name: "v2", Served: false, Storage: false, SelectableFields: []SelectableField{{JSONPath: ".spec.y"}}},
					},
				},
			},
		},
		{
			Name: "v1 to internal, identical selectable fields moves to top-level",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
						{Name: "v2", Served: true, Storage: false, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
						{Name: "v2", Served: true, Storage: false},
					},
					SelectableFields:      []apiextensions.SelectableField{{JSONPath: ".spec.x"}},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, single selectable field moves to top-level",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
					},
					SelectableFields:      []apiextensions.SelectableField{{JSONPath: ".spec.x"}},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, distinct selectable fields remains per-version",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Versions: []CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []SelectableField{{JSONPath: ".spec.x"}}},
						{Name: "v2", Served: true, Storage: false, SelectableFields: []SelectableField{{JSONPath: ".spec.y"}}},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Version: "v1",
					Versions: []apiextensions.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true, SelectableFields: []apiextensions.SelectableField{{JSONPath: ".spec.x"}}},
						{Name: "v2", Served: true, Storage: false, SelectableFields: []apiextensions.SelectableField{{JSONPath: ".spec.y"}}},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		// webhook 转换配置
		{
			Name: "internal to v1, no webhook client config",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion: &apiextensions.CustomResourceConversion{},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{},
				},
			},
		},
		{
			Name: "internal to v1, webhook client config",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion: &apiextensions.CustomResourceConversion{
						WebhookClientConfig: &apiextensions.WebhookClientConfig{URL: ptr.To("http://example.com")},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{
						Webhook: &WebhookConversion{
							ClientConfig: &WebhookClientConfig{URL: ptr.To("http://example.com")},
						},
					},
				},
			},
		},
		{
			Name: "internal to v1, webhook versions",
			In: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion: &apiextensions.CustomResourceConversion{
						ConversionReviewVersions: []string{"v1"},
					},
				},
			},
			Out: &CustomResourceDefinition{},
			ExpectOut: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{
						Webhook: &WebhookConversion{
							ConversionReviewVersions: []string{"v1"},
						},
					},
				},
			},
		},
		{
			Name: "v1 to internal, no webhook client config",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion:            &apiextensions.CustomResourceConversion{},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, webhook client config",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{
						Webhook: &WebhookConversion{
							ClientConfig: &WebhookClientConfig{URL: ptr.To("http://example.com")},
						},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion: &apiextensions.CustomResourceConversion{
						WebhookClientConfig: &apiextensions.WebhookClientConfig{URL: ptr.To("http://example.com")},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
		{
			Name: "v1 to internal, webhook versions",
			In: &CustomResourceDefinition{
				Spec: CustomResourceDefinitionSpec{
					Conversion: &CustomResourceConversion{
						Webhook: &WebhookConversion{
							ConversionReviewVersions: []string{"v1"},
						},
					},
				},
			},
			Out: &apiextensions.CustomResourceDefinition{},
			ExpectOut: &apiextensions.CustomResourceDefinition{
				Spec: apiextensions.CustomResourceDefinitionSpec{
					Conversion: &apiextensions.CustomResourceConversion{
						ConversionReviewVersions: []string{"v1"},
					},
					PreserveUnknownFields: ptr.To(false),
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	// 注册内部与外部类型
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			err := scheme.Convert(tc.In, tc.Out, nil)
			if err != nil {
				if len(tc.ExpectErr) == 0 {
					t.Fatalf("unexpected error %v", err)
				}
				if !strings.Contains(err.Error(), tc.ExpectErr) {
					t.Fatalf("expected error %s, got %v", tc.ExpectErr, err)
				}
				return
			}
			if len(tc.ExpectErr) > 0 {
				t.Fatalf("expected error %s, got none", tc.ExpectErr)
			}
			if !reflect.DeepEqual(tc.Out, tc.ExpectOut) {
				t.Fatalf("unexpected result:\n %s", cmp.Diff(tc.ExpectOut, tc.Out))
			}
		})
	}
}

// TestJSONConversion 验证 JSON 默认值在转换为外部类型时的序列化结果。
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

	// 注册内部与外部类型
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

// TestJSONRoundTrip 验证 JSON schema 经过内部类型往返后保留 null 相关语义。
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
	// 注册内部与外部类型
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

// TestMemoryEqual 校验内部与外部类型的反射结构在内存布局上一致。
func TestMemoryEqual(t *testing.T) {
	testcases := []struct {
		a interface{}
		b interface{}
	}{
		{apiextensions.JSONSchemaProps{}.XValidations, JSONSchemaProps{}.XValidations},
	}

	for _, tc := range testcases {
		aType := reflect.TypeOf(tc.a)
		bType := reflect.TypeOf(tc.b)
		t.Run(aType.String(), func(t *testing.T) {
			assertEqualTypes(t, nil, aType, bType)
		})
	}
}

// assertEqualTypes 递归比较两个反射类型，确保字段名、偏移、匿名性和索引一致。
func assertEqualTypes(t *testing.T, path []string, a, b reflect.Type) {
	if a == b {
		return
	}

	if a.Kind() != b.Kind() {
		fatalTypeError(t, path, a, b, "mismatched Kind")
	}

	switch a.Kind() {
	case reflect.Struct:
		aFields := a.NumField()
		bFields := b.NumField()
		if aFields != bFields {
			fatalTypeError(t, path, a, b, "mismatched field count")
		}
		for i := 0; i < aFields; i++ {
			aField := a.Field(i)
			bField := b.Field(i)
			if aField.Name != bField.Name {
				fatalTypeError(t, path, a, b, fmt.Sprintf("mismatched field name %d: %s %s", i, aField.Name, bField.Name))
			}
			if aField.Offset != bField.Offset {
				fatalTypeError(t, path, a, b, fmt.Sprintf("mismatched field offset %d: %v %v", i, aField.Offset, bField.Offset))
			}
			if aField.Anonymous != bField.Anonymous {
				fatalTypeError(t, path, a, b, fmt.Sprintf("mismatched field anonymous %d: %v %v", i, aField.Anonymous, bField.Anonymous))
			}
			if !reflect.DeepEqual(aField.Index, bField.Index) {
				fatalTypeError(t, path, a, b, fmt.Sprintf("mismatched field index %d: %v %v", i, aField.Index, bField.Index))
			}
			path = append(path, aField.Name)
			assertEqualTypes(t, path, aField.Type, bField.Type)
			path = path[:len(path)-1]
		}

	case reflect.Pointer, reflect.Slice:
		aElemType := a.Elem()
		bElemType := b.Elem()
		assertEqualTypes(t, path, aElemType, bElemType)
	case reflect.String:
		// 字符串类型相等

	default:
		fatalTypeError(t, path, a, b, "unhandled kind")
	}
}

// fatalTypeError 报告类型不匹配的致命错误。
func fatalTypeError(t *testing.T, path []string, a, b reflect.Type, message string) {
	t.Helper()
	t.Fatalf("%s: %s: %s %s", strings.Join(path, "."), message, a, b)
}
