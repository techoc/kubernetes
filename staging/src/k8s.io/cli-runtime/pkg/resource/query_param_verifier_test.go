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

package resource

import (
	"path/filepath"
	"testing"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	"k8s.io/apimachinery/pkg/runtime/schema"
	openapitesting "k8s.io/kube-openapi/pkg/util/proto/testing"
)

// 使用仓库内的 OpenAPI 文档做离线测试，分别覆盖参数内联与 shared parameters 两种写法。
var fakeSchema = openapitesting.Fake{Path: filepath.Join("..", "..", "artifacts", "openapi", "swagger.json")}
var fakeSchemaSharedParams = openapitesting.Fake{Path: filepath.Join("..", "..", "artifacts", "openapi", "swagger-with-shared-parameters.json")}

// TestSupportsQueryParam 验证 supportsQueryParam 能从两种 OpenAPI 文档结构中正确判断
// 某 GVK 是否支持 fieldValidation 参数，以及未知类型、不支持类型的行为。
func TestSupportsQueryParam(t *testing.T) {
	docInlineParams, err := fakeSchema.OpenAPISchema()
	if err != nil {
		t.Fatalf("Failed to get OpenAPI Schema: %v", err)
	}
	docSharedParams, err := fakeSchemaSharedParams.OpenAPISchema()
	if err != nil {
		t.Fatalf("Failed to get OpenAPI Schema: %v", err)
	}

	tests := []struct {
		gvk        schema.GroupVersionKind
		success    bool
		supports   bool
		queryParam VerifiableQueryParam
	}{
		// Pod 支持 fieldValidation。
		{
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			},
			success:    true,
			supports:   true,
			queryParam: QueryParamFieldValidation,
		},
		// 未知类型：既找不到定义，也判定为不支持。
		{
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "UnknownKind",
			},
			success:    false,
			supports:   false,
			queryParam: QueryParamFieldValidation,
		},
		// NodeProxyOptions 存在但不支持 fieldValidation。
		{
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "NodeProxyOptions",
			},
			success:    true,
			supports:   false,
			queryParam: QueryParamFieldValidation,
		},
		// List 是聚合类型，本身不承载可验证字段。
		{
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "List",
			},
			success:    false,
			supports:   false,
			queryParam: QueryParamFieldValidation,
		},
	}

	for name, doc := range map[string]*openapi_v2.Document{
		"inline parameters": docInlineParams,
		"shared parameters": docSharedParams,
	} {
		t.Run(name, func(t *testing.T) {
			for _, test := range tests {
				supports, err := supportsQueryParam(doc, test.gvk, test.queryParam)
				if supports != test.supports || ((err == nil) != test.success) {
					errStr := "nil"
					if test.success == false {
						errStr = "err"
					}
					t.Errorf("SupportsQueryParam(doc, %v, %v) = (%v, %v), expected (%v, %v)",
						test.gvk, test.queryParam,
						supports, err,
						test.supports, errStr,
					)
				}
			}
		})
	}
}

// TestFieldValidationVerifier 验证 QueryParamVerifier 结合 CRD 发现后的判定：
// 已知内置类型按 OpenAPI 判断，CRD 天生支持 fieldValidation，未知类型则报错。
func TestFieldValidationVerifier(t *testing.T) {
	fieldValidationVerifier := QueryParamVerifier{
		// 模拟发现到两个 CRD，用于验证 CRD 分支。
		finder: NewCRDFinder(func() ([]schema.GroupKind, error) {
			return []schema.GroupKind{
				{
					Group: "crd.com",
					Kind:  "MyCRD",
				},
				{
					Group: "crd.com",
					Kind:  "MyNewCRD",
				},
			}, nil
		}),
		openAPIGetter: &fakeSchema,
		queryParam:    QueryParamFieldValidation,
	}

	err := fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NodeProxyOptions"})
	if err == nil {
		t.Fatalf("NodeProxyOptions doesn't support fieldValidation, yet no error found")
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	if err != nil {
		t.Fatalf("Pod should support fieldValidation: %v", err)
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "crd.com", Version: "v1", Kind: "MyCRD"})
	if err != nil {
		t.Fatalf("MyCRD should support fieldValidation: %v", err)
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "crd.com", Version: "v1", Kind: "Random"})
	if err == nil {
		t.Fatalf("Random doesn't support fieldValidation, yet no error found")
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "List"})
	if err == nil {
		t.Fatalf("List does not support fieldValidation, yet no error found")
	}
}

// EmptyOpenAPI 返回空文档，模拟服务端未暴露 OpenAPI 信息的老集群。
type EmptyOpenAPI struct{}

func (EmptyOpenAPI) OpenAPISchema() (*openapi_v2.Document, error) {
	return &openapi_v2.Document{}, nil
}

// TestFieldValidationVerifierNoOpenAPI 验证拿不到 OpenAPI 文档时的保守行为：
// 除 CRD 外一律判定为不支持，宁可少用参数也不要发出可能被拒绝的请求。
func TestFieldValidationVerifierNoOpenAPI(t *testing.T) {
	fieldValidationVerifier := QueryParamVerifier{
		finder: NewCRDFinder(func() ([]schema.GroupKind, error) {
			return []schema.GroupKind{
				{
					Group: "crd.com",
					Kind:  "MyCRD",
				},
				{
					Group: "crd.com",
					Kind:  "MyNewCRD",
				},
			}, nil
		}),
		openAPIGetter: EmptyOpenAPI{},
		queryParam:    QueryParamFieldValidation,
	}

	err := fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
	if err == nil {
		t.Fatalf("Pod doesn't support fieldValidation, yet no error found")
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "crd.com", Version: "v1", Kind: "MyCRD"})
	if err == nil {
		t.Fatalf("MyCRD doesn't support fieldValidation, yet no error found")
	}

	err = fieldValidationVerifier.HasSupport(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "List"})
	if err == nil {
		t.Fatalf("List does not support fieldValidation, yet no error found")
	}
}
