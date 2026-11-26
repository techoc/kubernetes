/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/openapi"
	"k8s.io/client-go/openapi3"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var _ Verifier = &queryParamVerifierV3{}

// 返回实现 Verifier 的 queryParamVerifierV3。
// OpenAPI V3 的缓存行为取决于传入的 oapiClient：
// 内存缓存需事先用 cached.NewClient(oapiClient) 包装；
// 磁盘缓存则由创建 oapiClient 的发现客户端决定。
//
// 注意：缓存策略完全由调用方提供的 oapiClient 决定，本函数不做缓存包装；
// 每次 GVSpec 调用都可能触发远端请求，热路径上应先包一层 cached client。
func NewQueryParamVerifierV3(dynamicClient dynamic.Interface, oapiClient openapi.Client, queryParam VerifiableQueryParam) Verifier {
	return &queryParamVerifierV3{
		finder:     NewCRDFinder(CRDFromDynamic(dynamicClient)),
		root:       openapi3.NewRoot(oapiClient),
		queryParam: queryParam,
	}
}

// 封装判断给定 GVK 的 Patch 端点是否支持某查询参数所需的信息。
type queryParamVerifierV3 struct {
	finder     CRDFinder
	root       openapi3.Root
	queryParam VerifiableQueryParam
}

// namespaceGVK 作为 CRD 能力推断的参照类型，理由与 V2 实现一致。
var namespaceGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}

// GVK 的 Patch 端点支持所存参数（通常是 "fieldValidation"）时返回 nil；
// 不支持或发生其他错误时返回错误。
// 找不到 CRD 的 OpenAPI V3 文档时，改用 Namespace 的文档判断。
func (v *queryParamVerifierV3) HasSupport(gvk schema.GroupVersionKind) error {
	if (gvk == schema.GroupVersionKind{Version: "v1", Kind: "List"}) {
		return NewParamUnsupportedError(gvk, v.queryParam)
	}
	gvSpec, err := v.root.GVSpec(gvk.GroupVersion())
	if err == nil {
		return supportsQueryParamV3(gvSpec, gvk, v.queryParam)
	}
	// 只有“该 GroupVersion 不存在”才走 CRD 回退；其他错误（网络等）原样返回，
	// 避免把真实故障误报为“不支持该参数”。
	if _, isErr := err.(*openapi3.GroupVersionNotFoundError); !isErr {
		return err
	}
	// 找不到 GVK 的文档时先判断它是否为 CRD。
	// CRD 则用 Namespace 的 OpenAPI V3 文档判断。
	if found, _ := v.finder.HasCRD(gvk.GroupKind()); found {
		namespaceSpec, err := v.root.GVSpec(namespaceGVK.GroupVersion())
		if err != nil {
			// 取 Namespace 文档出错时原样返回错误。
			return err
		}
		return supportsQueryParamV3(namespaceSpec, namespaceGVK, v.queryParam)
	}
	return NewParamUnsupportedError(gvk, v.queryParam)
}

// 扩展信息中包含给定 GVK 时返回 true，否则返回 false。
func hasGVKExtensionV3(extensions spec.Extensions, gvk schema.GroupVersionKind) bool {
	var oapiGVK map[string]string
	err := extensions.GetObject("x-kubernetes-group-version-kind", &oapiGVK)
	if err != nil {
		return false
	}
	if oapiGVK["group"] == gvk.Group &&
		oapiGVK["version"] == gvk.Version &&
		oapiGVK["kind"] == gvk.Kind {
		return true
	}
	return false
}

// 从 OpenAPI 中检查 GVK 的 PATCH 端点是否支持指定查询参数；
// 支持返回 nil，否则返回 "paramUnsupportedError"（文档无效时返回解析错误）。
// 非 PATCH 操作跳过；只检查目标 GVK 的 PATCH 操作，
// 在请求参数和全局参数中查找该查询参数。
func supportsQueryParamV3(doc *spec3.OpenAPI, gvk schema.GroupVersionKind, queryParam VerifiableQueryParam) error {
	// 不完整的文档直接报错，而不是当作“不支持”，以便调用方区分配置问题与能力问题。
	if doc == nil || doc.Paths == nil {
		return fmt.Errorf("Invalid OpenAPI V3 document")
	}
	for _, path := range doc.Paths.Paths {
		// 非 PATCH 操作跳过。
		if path == nil {
			continue
		}
		op := path.PathProps.Patch
		if op == nil {
			continue
		}
		// 这是目标 GVK 的 PATCH 操作吗？
		if !hasGVKExtensionV3(op.VendorExtensible.Extensions, gvk) {
			continue
		}
		// 在该 PATCH 操作的参数中查找目标查询参数。
		for _, param := range op.OperationProps.Parameters {
			if param.ParameterProps.Name == string(queryParam) && param.In == "query" {
				return nil
			}

			// 查找全局参数。
			if ref := param.Refable.Ref.Ref.String(); strings.HasPrefix(ref, "#/parameters/") && doc.Components != nil {
				k := strings.TrimPrefix(ref, "#/parameters/")
				if globalParam, ok := doc.Components.Parameters[k]; ok && globalParam != nil {
					if globalParam.In == "query" && globalParam.Name == string(queryParam) {
						return nil
					}
				}
			}
		}
		// 已定位到该 GVK 的 PATCH 端点但没有目标参数，明确不支持。
		return NewParamUnsupportedError(gvk, queryParam)
	}
	return fmt.Errorf("Path not found for GVK (%s) in OpenAPI V3 doc", gvk)
}
