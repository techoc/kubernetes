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
	"errors"
	"fmt"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	yaml "go.yaml.in/yaml/v2"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// NewQueryParamVerifier 构造基于 OpenAPI V2 的校验器；它内部惰性加载 CRD 列表，
// 只有当 OpenAPI 中找不到目标类型时才去查询 CRD，以尽量省下额外的网络往返。
func NewQueryParamVerifier(dynamicClient dynamic.Interface, openAPIGetter discovery.OpenAPISchemaInterface, queryParam VerifiableQueryParam) *QueryParamVerifier {
	return &QueryParamVerifier{
		finder:        NewCRDFinder(CRDFromDynamic(dynamicClient)),
		openAPIGetter: openAPIGetter,
		queryParam:    queryParam,
	}
}

// QueryParamVerifier 针对当前服务端检查给定 GVK 是否支持指定查询参数。
//
// 目前支持的参数为 fieldValidation。
//
// 必须检查该参数：是否执行服务端或客户端 schema 校验取决于服务端是否支持它。
//
// 检查通过读取 OpenAPI 完成。找不到 GVK 时，假定 CRD 与 "namespaces" 支持程度相同，
// 非 CRD 则不支持；但 CRD 的检查尽量延后，因为需要额外一次服务端往返。
//
// 设计假设：CRD 由 apiserver 的扩展机制提供，其查询参数支持程度与
// namespace（一个长期存在、能力稳定的内置类型）保持一致；内置类型则
// 直接读 OpenAPI 判定。fieldValidation 用于决定由客户端还是服务端做校验。
type QueryParamVerifier struct {
	finder        CRDFinder
	openAPIGetter discovery.OpenAPISchemaInterface
	queryParam    VerifiableQueryParam
}

// Verifier 是测试 QueryParamVerifier 时使用的通用接口。
type Verifier interface {
	HasSupport(gvk schema.GroupVersionKind) error
}

// VerifiableQueryParam 指可通过特定 GVK 的 OpenAPI 判断apiserver 是否支持的参数。
type VerifiableQueryParam string

const (
	// QueryParamFieldValidation 是 PATCH 请求上的字段校验参数名。
	QueryParamFieldValidation VerifiableQueryParam = "fieldValidation"
)

// HasSupport 检查给定 GVK 是否支持 v 上配置的查询参数。
func (v *QueryParamVerifier) HasSupport(gvk schema.GroupVersionKind) error {
	// List 不直接对应某个类型的 PATCH 端点，任何字段校验参数对它都无意义。
	if (gvk == schema.GroupVersionKind{Version: "v1", Kind: "List"}) {
		return NewParamUnsupportedError(gvk, v.queryParam)
	}

	// 检查两种来源：OpenAPI 中的 PATCH 参数定义，以及按 namespace 规则推断 CRD。
	oapi, err := v.openAPIGetter.OpenAPISchema()
	if err != nil {
		return fmt.Errorf("failed to download openapi: %v", err)
	}
	supports, err := supportsQueryParam(oapi, gvk, v.queryParam)
	if err != nil {
		// 假定找不到该类型，转而检查 namespace：
		supports, _ = supportsQueryParam(oapi, schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, v.queryParam)
		// 若 namespace 支持该参数，则只为 CRD 认定支持。
		// 只有目标是 CRD 时才把 namespace 的能力套用过来；内置类型
		// 未在 OpenAPI 中找到 PATCH 定义即视为不支持。
		if supports {
			supports, err = v.finder.HasCRD(gvk.GroupKind())
			if err != nil {
				return fmt.Errorf("failed to check CRD: %v", err)
			}
		}
	}
	if !supports {
		return NewParamUnsupportedError(gvk, v.queryParam)
	}
	return nil
}

// paramUnsupportedError 表达“该 GVK 的 PATCH 端点不支持某查询参数”，
// 与“网络/OpenAPI 加载失败”这类普通错误区分开，便于命令层决定
// 是否回退到客户端兼容行为。
type paramUnsupportedError struct {
	gvk   schema.GroupVersionKind
	param VerifiableQueryParam
}

// NewParamUnsupportedError 构造一个不支持的参数错误。
func NewParamUnsupportedError(gvk schema.GroupVersionKind, param VerifiableQueryParam) error {
	return &paramUnsupportedError{
		gvk:   gvk,
		param: param,
	}
}

func (e *paramUnsupportedError) Error() string {
	return fmt.Sprintf("%v doesn't support %s", e.gvk, e.param)
}

// IsParamUnsupportedError 判断 err 是否为不支持参数错误。
func IsParamUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*paramUnsupportedError)
	return ok
}

// hasGVKExtension 检查 OpenAPI 的 x-kubernetes-group-version-kind vendor 扩展
// 是否匹配给定的 GVK；不匹配或解析失败都返回 false。
func hasGVKExtension(extensions []*openapi_v2.NamedAny, gvk schema.GroupVersionKind) bool {
	for _, extension := range extensions {
		if extension.GetValue().GetYaml() == "" ||
			extension.GetName() != "x-kubernetes-group-version-kind" {
			continue
		}
		var value map[string]string
		err := yaml.Unmarshal([]byte(extension.GetValue().GetYaml()), &value)
		if err != nil {
			continue
		}

		if value["group"] == gvk.Group && value["kind"] == gvk.Kind && value["version"] == gvk.Version {
			return true
		}
		return false
	}
	return false
}

// 从 OpenAPI 中检查给定 GVK 是否在 PATCH 端点支持指定查询参数。
//
// 全局参数（#/parameters/<name>）会被解析成 map 以便按引用查找。
func supportsQueryParam(doc *openapi_v2.Document, gvk schema.GroupVersionKind, queryParam VerifiableQueryParam) (bool, error) {
	globalParams := map[string]*openapi_v2.NamedParameter{}
	for _, p := range doc.GetParameters().GetAdditionalProperties() {
		globalParams["#/parameters/"+p.GetName()] = p
	}

	for _, path := range doc.GetPaths().GetPath() {
		// 这段描述的是我们要找的 GVK 吗？
		if !hasGVKExtension(path.GetValue().GetPatch().GetVendorExtension(), gvk) {
			continue
		}
		for _, param := range path.GetValue().GetPatch().GetParameters() {
			if param.GetParameter().GetNonBodyParameter().GetQueryParameterSubSchema().GetName() == string(queryParam) {
				return true, nil
			}

			// 查找全局参数。
			if ref := param.GetJsonReference().GetXRef(); ref != "" {
				if globalParam, ok := globalParams[ref]; ok && globalParam != nil && globalParam.GetValue().GetNonBodyParameter().GetQueryParameterSubSchema().GetName() == string(queryParam) {
					return true, nil
				}
			}
		}
		// 已找到该 GVK 对应的 PATCH 端点但参数不在其中，说明确实不支持。
		return false, nil
	}

	// 遍历完所有 path 都没有该 GVK，交回调用方走 namespace 推断逻辑。
	return false, errors.New("couldn't find GVK in openapi")
}
