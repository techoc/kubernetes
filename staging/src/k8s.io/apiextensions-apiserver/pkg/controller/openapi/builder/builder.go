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

// Package builder 根据 CRD 定义构建供 kube-openapi 消费的 OpenAPI v2/v3 文档。
package builder

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/emicklei/go-restful/v3"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	v1 "k8s.io/api/autoscaling/v1"
	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	openapiv2 "k8s.io/apiextensions-apiserver/pkg/controller/openapi/v2"
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	utilopenapi "k8s.io/apiserver/pkg/util/openapi"
	"k8s.io/client-go/kubernetes/scheme"
	openapibuilder "k8s.io/kube-openapi/pkg/builder"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/common/restfuladapter"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

const (
	// 内置 metadata 的引用与 Go 类型
	objectMetaSchemaRef = "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
	listMetaSchemaRef   = "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ListMeta"

	typeMetaType   = "io.k8s.apimachinery.pkg.apis.meta.v1.TypeMeta"
	objectMetaType = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"

	definitionPrefix   = "#/definitions/"
	v3DefinitionPrefix = "#/components/schemas/"
)

var (
	swaggerPartialObjectMetadataDescriptions     = metav1beta1.PartialObjectMetadata{}.SwaggerDoc()
	swaggerPartialObjectMetadataListDescriptions = metav1beta1.PartialObjectMetadataList{}.SwaggerDoc()

	nameToken      = "{name}"
	namespaceToken = "{namespace}"
)

// OpenAPI v2 与 v3 的 definitions 路径不同，必要时需要转换。
// 传入的 schemaRef 使用 v2 前缀；当 v2 为 false 时转换为 v3 前缀。
func refForOpenAPIVersion(schemaRef string, v2 bool) string {
	if v2 {
		return schemaRef
	}
	return strings.Replace(schemaRef, definitionPrefix, v3DefinitionPrefix, 1)
}

var definitions map[string]common.OpenAPIDefinition
var definitionsV3 map[string]common.OpenAPIDefinition
var buildDefinitions sync.Once
var namer *openapi.DefinitionNamer

// Options 包含 builder 的构建选项。
type Options struct {
	// 转换为 OpenAPI v2。
	V2 bool

	// 去除值校验。
	StripValueValidation bool

	// 去除 nullable。
	StripNullable bool

	// AllowNonStructural 表示 schema 虽可归入 structural 类型但不满足全部 structural 不变量时，仍允许为其构建 swagger。
	AllowNonStructural bool

	// IncludeSelectableFields 表示是否在输出中加上可选字段（selectableFields）扩展信息。
	IncludeSelectableFields bool
}

// generateBuilder 为指定版本的 CRD 构造 builder，并按需把 OpenAPI v3 schema 转换为 structural schema。
func generateBuilder(crd *apiextensionsv1.CustomResourceDefinition, version string, opts Options) (*builder, error) {
	var schema *structuralschema.Structural
	s, err := apiextensionshelpers.GetSchemaForVersion(crd, version)
	if err != nil {
		return nil, err
	}

	if s != nil && s.OpenAPIV3Schema != nil {
		internalCRDSchema := &apiextensionsinternal.CustomResourceValidation{}
		if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(s, internalCRDSchema, nil); err != nil {
			return nil, fmt.Errorf("failed converting CRD validation to internal version: %v", err)
		}
		if !validation.SchemaHasInvalidTypes(internalCRDSchema.OpenAPIV3Schema) {
			if ss, err := structuralschema.NewStructural(internalCRDSchema.OpenAPIV3Schema); err == nil {
				// 除非明确要求从非 structural schema 生成 swagger，否则跳过这类 schema
				if opts.AllowNonStructural || len(structuralschema.ValidateStructural(nil, ss)) == 0 {
					schema = ss

					// 这里会加入 ValueValidation 字段（anyOf、allOf）；若 opts.StripValueValidation 为 true 则随后移除。
					schema = schema.Unfold()

					if opts.StripValueValidation {
						schema = schema.StripValueValidations()
					}
					if opts.StripNullable {
						schema = schema.StripNullable()
					}
				}
			}
		}
	}

	// TODO(roycaihw): 移除下面的 WebService 模板化逻辑。这段逻辑来自
	// k8s.io/apiserver 中的 registerResourceHandlers()。
	// 可选方案：要么（理想情况下）重构 registerResourceHandlers() 以复用这段代码，
	// 要么伪造一个 CR 的 APIInstaller 交给 registerResourceHandlers()。
	b := newBuilder(crd, version, schema, opts)

	// 用于构建 web service 的示例响应类型
	sample := &CRDCanonicalTypeNamer{
		group:   b.group,
		version: b.version,
		kind:    b.kind,
	}
	sampleList := &CRDCanonicalTypeNamer{
		group:   b.group,
		version: b.version,
		kind:    b.listKind,
	}
	status := &metav1.Status{}
	patch := &metav1.Patch{}
	scale := &v1.Scale{}

	routes := make([]*restful.RouteBuilder, 0)
	root := fmt.Sprintf("/apis/%s/%s/%s", b.group, b.version, b.plural)

	if b.namespaced {
		routes = append(routes, b.buildRoute(root, "", "GET", "list", "list", sampleList).Operation("list"+b.kind+"ForAllNamespaces"))
		root = fmt.Sprintf("/apis/%s/%s/namespaces/{namespace}/%s", b.group, b.version, b.plural)
	}
	routes = append(routes, b.buildRoute(root, "", "GET", "list", "list", sampleList))
	routes = append(routes, b.buildRoute(root, "", "POST", "post", "create", sample).Reads(sample))
	routes = append(routes, b.buildRoute(root, "", "DELETE", "deletecollection", "deletecollection", status))

	routes = append(routes, b.buildRoute(root, "/{name}", "GET", "get", "read", sample))
	routes = append(routes, b.buildRoute(root, "/{name}", "PUT", "put", "replace", sample).Reads(sample))
	routes = append(routes, b.buildRoute(root, "/{name}", "DELETE", "delete", "delete", status))
	routes = append(routes, b.buildRoute(root, "/{name}", "PATCH", "patch", "patch", sample).Reads(patch))

	subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, version)
	if err != nil {
		return nil, err
	}
	if subresources != nil && subresources.Status != nil {
		routes = append(routes, b.buildRoute(root, "/{name}/status", "GET", "get", "read", sample))
		routes = append(routes, b.buildRoute(root, "/{name}/status", "PUT", "put", "replace", sample).Reads(sample))
		routes = append(routes, b.buildRoute(root, "/{name}/status", "PATCH", "patch", "patch", sample).Reads(patch))
	}
	if subresources != nil && subresources.Scale != nil {
		routes = append(routes, b.buildRoute(root, "/{name}/scale", "GET", "get", "read", scale))
		routes = append(routes, b.buildRoute(root, "/{name}/scale", "PUT", "put", "replace", scale).Reads(scale))
		routes = append(routes, b.buildRoute(root, "/{name}/scale", "PATCH", "patch", "patch", scale).Reads(patch))
	}

	for _, route := range routes {
		b.ws.Route(route)
	}
	return b, nil
}

// BuildOpenAPIV3 为给定版本的 CRD 构建 OpenAPI v3 文档。
func BuildOpenAPIV3(crd *apiextensionsv1.CustomResourceDefinition, version string, opts Options) (*spec3.OpenAPI, error) {
	b, err := generateBuilder(crd, version, opts)
	if err != nil {
		return nil, err
	}

	return builder3.BuildOpenAPISpecFromRoutes(restfuladapter.AdaptWebServices([]*restful.WebService{b.ws}), b.getOpenAPIV3Config())
}

// BuildOpenAPIV2 为给定版本的 CRD 构建 OpenAPI v2。
func BuildOpenAPIV2(crd *apiextensionsv1.CustomResourceDefinition, version string, opts Options) (*spec.Swagger, error) {
	b, err := generateBuilder(crd, version, opts)
	if err != nil {
		return nil, err
	}

	return openapibuilder.BuildOpenAPISpecFromRoutes(restfuladapter.AdaptWebServices([]*restful.WebService{b.ws}), b.getOpenAPIConfig())
}

// 确认 CRDCanonicalTypeNamer 实现了 CanonicalTypeNamer 接口
var _ = util.OpenAPICanonicalTypeNamer(&CRDCanonicalTypeNamer{})

// CRDCanonicalTypeNamer 为 CRD 实现 CanonicalTypeNamer 接口，
// 以便在没有 Go 类型的情况下为 kube-openapi 提供规范类型名。
type CRDCanonicalTypeNamer struct {
	group   string
	version string
	kind    string
}

// OpenAPICanonicalTypeName 返回给定 CRD 的规范类型名。
func (c *CRDCanonicalTypeNamer) OpenAPICanonicalTypeName() string {
	return gvkToModelName(c.group, c.version, c.kind)
}

// builder 持有某个版本 CRD 的校验 schema 与基础命名信息。
// 它的作用是构建 kube-openapi 可以消费的 WebService。
type builder struct {
	schema     *spec.Schema
	listSchema *spec.Schema
	ws         *restful.WebService

	group    string
	version  string
	kind     string
	listKind string
	plural   string

	namespaced bool
}

// subresource 是获取子资源名的便捷方法。合法输入如下：
//
//	输入                        输出
//	""                        ""
//	"/"                       ""
//	"/{name}"                 ""
//	"/{name}/scale"           "scale"
//	"/{name}/scale/foo"       非法输入
func subresource(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) <= 2 {
		return ""
	}
	if len(parts) == 3 {
		return parts[2]
	}
	// 以 panic 提示编程错误
	panic("failed to parse subresource; invalid path")
}

func (b *builder) descriptionFor(path, operationVerb string) string {
	var article string
	switch operationVerb {
	case "list":
		article = " objects of kind "
	case "read", "replace":
		article = " the specified "
	case "patch":
		article = " the specified "
	case "create", "delete":
		article = endpoints.GetArticleForNoun(b.kind, " ")
	default:
		article = ""
	}

	var description string
	sub := subresource(path)
	if len(sub) > 0 {
		sub = " " + sub + " of"
	}
	switch operationVerb {
	case "patch":
		description = "partially update" + sub + article + b.kind
	case "deletecollection":
		// 与内置 API 的文案保持一致
		if len(sub) > 0 {
			sub = sub + " a"
		}
		description = "delete collection of" + sub + " " + b.kind
	default:
		description = operationVerb + sub + article + b.kind
	}

	return description
}

// buildRoute 返回供 WebService 消费的 RouteBuilder，并在 swagger 中构建路径。
//
//	action 可选值：GET、PUT、PATCH、POST、DELETE；
//	verb 可选值：list、read、replace、patch、create、delete、deletecollection；
//	sample 是响应类型对应的示例 Go 类型。
func (b *builder) buildRoute(root, path, httpMethod, actionVerb, operationVerb string, sample interface{}) *restful.RouteBuilder {
	var namespaced string
	if b.namespaced {
		namespaced = "Namespaced"
	}
	route := b.ws.Method(httpMethod).
		Path(root+path).
		To(func(req *restful.Request, res *restful.Response) {}).
		Doc(b.descriptionFor(path, operationVerb)).
		Param(b.ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
		Operation(operationVerb+namespaced+b.kind+cases.Title(language.English).String(subresource(path))).
		Metadata(endpoints.RouteMetaGVK, metav1.GroupVersionKind{
			Group:   b.group,
			Version: b.version,
			Kind:    b.kind,
		}).
		Metadata(endpoints.RouteMetaAction, actionVerb).
		Produces("application/json", "application/yaml").
		Returns(http.StatusOK, "OK", sample).
		Writes(sample)
	if strings.Contains(root, namespaceToken) || strings.Contains(path, namespaceToken) {
		route.Param(b.ws.PathParameter("namespace", "object name and auth scope, such as for teams and projects").DataType("string"))
	}
	if strings.Contains(root, nameToken) || strings.Contains(path, nameToken) {
		route.Param(b.ws.PathParameter("name", "name of the "+b.kind).DataType("string"))
	}

	// 构建请求体支持的媒体类型
	if httpMethod == "PATCH" {
		supportedTypes := []string{
			string(types.JSONPatchType),
			string(types.MergePatchType),
			string(types.ApplyPatchType),
		}
		route.Consumes(supportedTypes...)
	} else {
		route.Consumes(runtime.ContentTypeJSON, runtime.ContentTypeYAML)
	}

	// 构建可选参数
	switch actionVerb {
	case "get":
		endpoints.AddObjectParams(b.ws, route, &metav1.GetOptions{})
	case "list", "deletecollection":
		endpoints.AddObjectParams(b.ws, route, &metav1.ListOptions{})
	case "put":
		endpoints.AddObjectParams(b.ws, route, &metav1.UpdateOptions{})
	case "patch":
		endpoints.AddObjectParams(b.ws, route, &metav1.PatchOptions{})
	case "post":
		endpoints.AddObjectParams(b.ws, route, &metav1.CreateOptions{})
	case "delete":
		endpoints.AddObjectParams(b.ws, route, &metav1.DeleteOptions{})
		route.Reads(&metav1.DeleteOptions{}).ParameterNamed("body").Required(false)
	}

	// 构建响应
	switch actionVerb {
	case "post":
		route.Returns(http.StatusAccepted, "Accepted", sample)
		route.Returns(http.StatusCreated, "Created", sample)
	case "delete":
		route.Returns(http.StatusAccepted, "Accepted", sample)
	case "put":
		route.Returns(http.StatusCreated, "Created", sample)
	}

	return route
}

// buildKubeNative 结合 Kubernetes 原生的 object meta、type meta 与扩展字段构建输入 schema。
func (b *builder) buildKubeNative(crd *apiextensionsv1.CustomResourceDefinition, schema *structuralschema.Structural, opts Options, crdPreserveUnknownFields bool) (ret *spec.Schema) {
	// 只有在存在 schema 时才添加属性。否则 kubectl 会（错误地）假定 additionalProperties=false，
	// 并禁止 apiVersion、kind、metadata 之外的任何字段。必须修复 kubectl 才能停止这种行为，例如
	// 为 additionalProperties=true 增加显式允许额外字段的支持。
	// TODO: 让 kubectl 理解 additionalProperties=true
	if schema == nil {
		ret = &spec.Schema{
			SchemaProps: spec.SchemaProps{Type: []string{"object"}},
		}
	} else if opts.V2 && (schema.XPreserveUnknownFields || crdPreserveUnknownFields) {
		ret = &spec.Schema{
			SchemaProps: spec.SchemaProps{Type: []string{"object"}},
		}
		// 这里不能再添加更多属性，连 TypeMeta/ObjectMeta 都不行，否则 kubectl 会对其他字段报未知字段错误。
	} else {
		if opts.V2 {
			schema = openapiv2.ToStructuralOpenAPIV2(schema)
		}

		ret = schema.ToKubeOpenAPI()
		ret.SetProperty("metadata", *spec.RefSchema(refForOpenAPIVersion(objectMetaSchemaRef, opts.V2)).WithDescription(swaggerPartialObjectMetadataDescriptions["metadata"]))
		addTypeMetaProperties(ret, opts.V2)
		addEmbeddedProperties(ret, opts)
	}
	ret.AddExtension(endpoints.RouteMetaGVK, []interface{}{
		map[string]interface{}{
			"group":   b.group,
			"version": b.version,
			"kind":    b.kind,
		},
	})

	if opts.IncludeSelectableFields {
		if selectableFields := buildSelectableFields(crd, b.version); selectableFields != nil {
			ret.AddExtension(endpoints.RouteMetaSelectableFields, selectableFields)
		}
	}

	return ret
}

// addEmbeddedProperties 递归地为带 x-kubernetes-embedded-resource 标记的嵌套 schema 补上 apiVersion、kind、metadata。
func addEmbeddedProperties(s *spec.Schema, opts Options) {
	if s == nil {
		return
	}

	for k := range s.Properties {
		v := s.Properties[k]
		addEmbeddedProperties(&v, opts)
		s.Properties[k] = v
	}
	if s.Items != nil {
		addEmbeddedProperties(s.Items.Schema, opts)
	}
	if s.AdditionalProperties != nil {
		addEmbeddedProperties(s.AdditionalProperties.Schema, opts)
	}

	if isTrue, ok := s.VendorExtensible.Extensions.GetBool("x-kubernetes-preserve-unknown-fields"); ok && isTrue && opts.V2 {
		// 发布到 OpenAPI v2 且允许未知字段时，不添加 metadata 属性。
		// 添加这些 metadata 属性会让 kubectl 拒绝校验未知字段。
		return
	}
	if isTrue, ok := s.VendorExtensible.Extensions.GetBool("x-kubernetes-embedded-resource"); ok && isTrue {
		s.SetProperty("apiVersion", withDescription(getDefinition(typeMetaType, opts.V2).SchemaProps.Properties["apiVersion"],
			"apiVersion defines the versioned schema of this representation of an object. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
		))
		s.SetProperty("kind", withDescription(getDefinition(typeMetaType, opts.V2).SchemaProps.Properties["kind"],
			"kind is a string value representing the type of this object. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
		))
		s.SetProperty("metadata", *spec.RefSchema(refForOpenAPIVersion(objectMetaSchemaRef, opts.V2)).WithDescription(swaggerPartialObjectMetadataDescriptions["metadata"]))

		req := sets.NewString(s.Required...)
		if !req.Has("kind") {
			s.Required = append(s.Required, "kind")
		}
		if !req.Has("apiVersion") {
			s.Required = append(s.Required, "apiVersion")
		}
	}
}

// getDefinition 返回给定 Kubernetes 类型的定义。该函数摘自 kube-openapi 的 builder 逻辑。
func getDefinition(name string, v2 bool) spec.Schema {
	buildDefinitions.Do(generateBuildDefinitionsFunc)

	if v2 {
		return definitions[name].Schema
	}
	return definitionsV3[name].Schema
}

// withDescription 返回带描述的新 schema 副本，避免直接改动共享的定义。
func withDescription(s spec.Schema, desc string) spec.Schema {
	return *s.WithDescription(desc)
}

// generateBuildDefinitionsFunc 初始化 v2/v3 两套定义与命名器，只执行一次。
func generateBuildDefinitionsFunc() {
	namer = openapi.NewDefinitionNamer(scheme.Scheme)
	definitionsV3 = utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions)(func(name string) spec.Ref {
		defName, _ := namer.GetDefinitionName(name)
		prefix := v3DefinitionPrefix
		return spec.MustCreateRef(prefix + common.EscapeJsonPointer(defName))
	})

	definitions = utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions)(func(name string) spec.Ref {
		defName, _ := namer.GetDefinitionName(name)
		prefix := definitionPrefix
		return spec.MustCreateRef(prefix + common.EscapeJsonPointer(defName))
	})
}

// addTypeMetaProperties 向输入 schema 添加 Kubernetes 特有的 type meta 属性：
//
//	apiVersion 与 kind
func addTypeMetaProperties(s *spec.Schema, v2 bool) {
	s.SetProperty("apiVersion", getDefinition(typeMetaType, v2).SchemaProps.Properties["apiVersion"])
	s.SetProperty("kind", getDefinition(typeMetaType, v2).SchemaProps.Properties["kind"])
}

// buildListSchema 为 CRD 构建 list kind 的 schema。
func (b *builder) buildListSchema(crd *apiextensionsv1.CustomResourceDefinition, opts Options) *spec.Schema {
	name := definitionPrefix + gvkToModelName(b.group, b.version, b.kind)
	doc := fmt.Sprintf("List of %s. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md", b.plural)
	s := new(spec.Schema).
		Typed("object", "").
		WithDescription(fmt.Sprintf("%s is a list of %s", b.listKind, b.kind)).
		WithRequired("items").
		SetProperty("items", *spec.ArrayProperty(spec.RefSchema(refForOpenAPIVersion(name, opts.V2))).WithDescription(doc)).
		SetProperty("metadata", *spec.RefSchema(refForOpenAPIVersion(listMetaSchemaRef, opts.V2)).WithDescription(swaggerPartialObjectMetadataListDescriptions["metadata"]))

	addTypeMetaProperties(s, opts.V2)
	s.AddExtension(endpoints.RouteMetaGVK, []map[string]string{
		{
			"group":   b.group,
			"version": b.version,
			"kind":    b.listKind,
		},
	})
	if opts.IncludeSelectableFields {
		if selectableFields := buildSelectableFields(crd, b.version); selectableFields != nil {
			s.AddExtension(endpoints.RouteMetaSelectableFields, selectableFields)
		}
	}
	return s
}

// gvkToModelName 把 group/version/kind 组合成 kube-openapi 的模型名，group 各段需要反转。
func gvkToModelName(g, v, k string) string {
	groupParts := strings.Split(g, ".")
	slices.Reverse(groupParts)
	g = strings.Join(groupParts, ".")
	return fmt.Sprintf("%s.%s.%s", g, v, k)
}

// getOpenAPIConfig 构建配置，把生成的各类型定义接入 kube-openapi。
func (b *builder) getOpenAPIConfig() *common.Config {
	return &common.Config{
		ProtocolList: []string{"https"},
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title:   "Kubernetes CRD Swagger",
				Version: "v0.1.0",
			},
		},
		CommonResponses: map[int]spec.Response{
			401: {
				ResponseProps: spec.ResponseProps{
					Description: "Unauthorized",
				},
			},
		},
		GetOperationIDAndTags: openapi.GetOperationIDAndTags,
		GetDefinitionName: func(name string) (string, spec.Extensions) {
			buildDefinitions.Do(generateBuildDefinitionsFunc)
			return namer.GetDefinitionName(name)
		},
		GetDefinitions: func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
			def := utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions)(ref)
			def[gvkToModelName(b.group, b.version, b.kind)] = common.OpenAPIDefinition{
				Schema:       *b.schema,
				Dependencies: []string{objectMetaType},
			}
			def[gvkToModelName(b.group, b.version, b.listKind)] = common.OpenAPIDefinition{
				Schema: *b.listSchema,
			}
			return def
		},
	}
}

// getOpenAPIV3Config 构建 OpenAPI v3 的配置，把生成的各类型定义接入 kube-openapi。
func (b *builder) getOpenAPIV3Config() *common.OpenAPIV3Config {
	return &common.OpenAPIV3Config{
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title:   "Kubernetes CRD Swagger",
				Version: "v0.1.0",
			},
		},
		CommonResponses: map[int]*spec3.Response{
			401: {
				ResponseProps: spec3.ResponseProps{
					Description: "Unauthorized",
				},
			},
		},
		GetOperationIDAndTags: openapi.GetOperationIDAndTags,
		GetDefinitionName: func(name string) (string, spec.Extensions) {
			buildDefinitions.Do(generateBuildDefinitionsFunc)
			return namer.GetDefinitionName(name)
		},
		GetDefinitions: func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
			def := utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions)(ref)
			def[gvkToModelName(b.group, b.version, b.kind)] = common.OpenAPIDefinition{
				Schema:       *b.schema,
				Dependencies: []string{objectMetaType},
			}
			def[gvkToModelName(b.group, b.version, b.listKind)] = common.OpenAPIDefinition{
				Schema: *b.listSchema,
			}
			return def
		},
	}
}

// newBuilder 组装 builder，并预先构建带有 Kubernetes 原生属性的 schema 与 list schema。
func newBuilder(crd *apiextensionsv1.CustomResourceDefinition, version string, schema *structuralschema.Structural, opts Options) *builder {
	b := &builder{
		schema: &spec.Schema{
			SchemaProps: spec.SchemaProps{Type: []string{"object"}},
		},
		listSchema: &spec.Schema{},
		ws:         &restful.WebService{},

		group:    crd.Spec.Group,
		version:  version,
		kind:     crd.Spec.Names.Kind,
		listKind: crd.Spec.Names.ListKind,
		plural:   crd.Spec.Names.Plural,
	}
	if crd.Spec.Scope == apiextensionsv1.NamespaceScoped {
		b.namespaced = true
	}

	// 预先构建带有 Kubernetes 原生属性的 schema
	b.schema = b.buildKubeNative(crd, schema, opts, crd.Spec.PreserveUnknownFields)
	b.listSchema = b.buildListSchema(crd, opts)

	return b
}

// buildSelectableFields 提取指定版本声明的可选字段，转成 OpenAPI 扩展所需的格式；未声明时返回 nil。
func buildSelectableFields(crd *apiextensionsv1.CustomResourceDefinition, version string) any {
	var specVersion *apiextensionsv1.CustomResourceDefinitionVersion
	for _, v := range crd.Spec.Versions {
		if v.Name == version {
			specVersion = &v
			break
		}
	}
	if specVersion == nil || len(specVersion.SelectableFields) == 0 {
		return nil
	}
	selectableFields := make([]any, len(specVersion.SelectableFields))
	for i, sf := range specVersion.SelectableFields {
		props := map[string]any{
			"fieldPath": strings.TrimPrefix(sf.JSONPath, "."),
		}
		selectableFields[i] = props
	}
	return selectableFields
}
