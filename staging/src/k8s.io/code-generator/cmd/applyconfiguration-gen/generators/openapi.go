/*
Copyright 2021 The Kubernetes Authors.

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

package generators

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	openapiv2 "github.com/google/gnostic-models/openapiv2"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/gengo/v2/types"
	"k8s.io/kube-openapi/pkg/util"
	utilproto "k8s.io/kube-openapi/pkg/util/proto"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// typeModels 封装 OpenAPI schema 的解析结果：可供 Extract* 使用的模型，
// 以及 GVK 到 OpenAPI 类型名的映射（Extract* 需要把 Kind 映射回 definitions 条目）。
type typeModels struct {
	models           utilproto.Models
	gvkToOpenAPIType map[clientgentypes.GroupVersionKind]string
}

// newTypeModels 读取并裁剪 OpenAPI schema，只保留 apply 相关的类型及其引用闭包。
// 空闲路径（未提供 schema 文件）返回 emptyModels，对应不生成 Extract* 函数的模式。
func newTypeModels(openAPISchemaFilePath string, pkgTypes map[string]*types.Package) (*typeModels, error) {
	if len(openAPISchemaFilePath) == 0 {
		return emptyModels, nil // No Extract<type>() functions will be generated.
	}

	rawOpenAPISchema, err := os.ReadFile(openAPISchemaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read openapi-schema file: %w", err)
	}

	// Read in the provided openAPI schema.
	openAPISchema := &spec.Swagger{}
	err = json.Unmarshal(rawOpenAPISchema, openAPISchema)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal typeModels JSON: %w", err)
	}

	// Build a mapping from openAPI type name to GVK.
	// 找出 client-go apply 所需的根类型：
	// 只有同时具备 +genclient 与 apply/applyStatus verb 的类型才是 apply 根对象，
	// 其余类型通过引用闭包被间接纳入。
	gvkToOpenAPIType := map[clientgentypes.GroupVersionKind]string{}
	rootDefs := map[string]spec.Schema{}
	for _, p := range pkgTypes {
		gv, err := groupVersion(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse comments of package %s: %w", p.Name, err)
		}
		for _, t := range p.Types {
			tags := genclientTags(t)
			hasApply := tags.HasVerb("apply") || tags.HasVerb("applyStatus")
			if tags.GenerateClient && hasApply {
				// ToRESTFriendlyName 把 Go 类型名转成 OpenAPI definitions 中使用的名字。
				openAPIType := util.ToRESTFriendlyName(typeName(t))
				gvk := gv.WithKind(clientgentypes.Kind(t.Name.Name))
				rootDefs[openAPIType] = openAPISchema.Definitions[openAPIType]
				gvkToOpenAPIType[gvk] = openAPIType
			}
		}
	}

	// 把 schema 裁剪到只保留 client-go apply 所需的类型：
	// 裁剪可缩小序列化/校验成本，也避免未引用的外部 schema 造成校验失败。
	requiredDefs := make(map[string]spec.Schema)
	for name, def := range rootDefs {
		requiredDefs[name] = def
		findReferenced(&def, openAPISchema.Definitions, requiredDefs)
	}
	openAPISchema.Definitions = requiredDefs

	// Convert the openAPI schema to the models format and validate it.
	models, err := toValidatedModels(openAPISchema)
	if err != nil {
		return nil, err
	}
	return &typeModels{models: models, gvkToOpenAPIType: gvkToOpenAPIType}, nil
}

// emptyModels 在无 schema 时使用，始终非 nil，简化调用方判空。
var emptyModels = &typeModels{
	models:           &utilproto.Definitions{},
	gvkToOpenAPIType: map[clientgentypes.GroupVersionKind]string{},
}

// toValidatedModels 把 spec.Swagger 转成 kube-openapi 的模型格式。
// 走 openapiv2.ParseDocument 是为了复用 Its 引用校验；代价是一次多余的 JSON 往返。
func toValidatedModels(openAPISchema *spec.Swagger) (utilproto.Models, error) {
	// openapi_v2.ParseDocument only accepts a []byte of the JSON or YAML file to be parsed.
	// so we do an inefficient marshal back to json and then read it back in as yaml
	// but get the benefit of running the models through utilproto.NewOpenAPIData to
	// validate all the references between types
	rawMinimalOpenAPISchema, err := json.Marshal(openAPISchema)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal openAPI as JSON: %w", err)
	}

	document, err := openapiv2.ParseDocument(rawMinimalOpenAPISchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI document for file: %w", err)
	}
	// Construct the models and validate all references are valid.
	models, err := utilproto.NewOpenAPIData(document)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenAPI models for file: %w", err)
	}
	return models, nil
}

// findReferenced recursively finds all schemas referenced from the given def.
// toValidatedModels 确保不会漏掉任何引用。
// 递归覆盖 OpenAPI schema 中所有可能嵌套 schema 的位置；漏掉任一分支都会导致
// 裁剪后的文档出现悬空 $ref，从而在模型校验阶段报错。
func findReferenced(def *spec.Schema, allSchemas, referencedOut map[string]spec.Schema) {
	// follow $ref, if any
	refPtr := def.Ref.GetPointer()
	if refPtr != nil && !refPtr.IsEmpty() {
		name := refPtr.String()
		// 只处理本文件的 #/definitions/ 引用，外部引用保持原样。
		if !strings.HasPrefix(name, "/definitions/") {
			return
		}
		name = strings.TrimPrefix(name, "/definitions/")
		schema, ok := allSchemas[name]
		if !ok {
			panic(fmt.Sprintf("allSchemas schema is missing referenced type: %s", name))
		}
		// 已收录的引用不再下钻，既防环也避免重复工作。
		if _, ok := referencedOut[name]; !ok {
			referencedOut[name] = schema
			findReferenced(&schema, allSchemas, referencedOut)
		}
	}

	// follow any nested schemas
	if def.Items != nil {
		if def.Items.Schema != nil {
			findReferenced(def.Items.Schema, allSchemas, referencedOut)
		}
		for _, item := range def.Items.Schemas {
			findReferenced(&item, allSchemas, referencedOut)
		}
	}
	if def.AllOf != nil {
		for _, s := range def.AllOf {
			findReferenced(&s, allSchemas, referencedOut)
		}
	}
	if def.AnyOf != nil {
		for _, s := range def.AnyOf {
			findReferenced(&s, allSchemas, referencedOut)
		}
	}
	if def.OneOf != nil {
		for _, s := range def.OneOf {
			findReferenced(&s, allSchemas, referencedOut)
		}
	}
	if def.Not != nil {
		findReferenced(def.Not, allSchemas, referencedOut)
	}
	if def.Properties != nil {
		for _, prop := range def.Properties {
			findReferenced(&prop, allSchemas, referencedOut)
		}
	}
	if def.AdditionalProperties != nil && def.AdditionalProperties.Schema != nil {
		findReferenced(def.AdditionalProperties.Schema, allSchemas, referencedOut)
	}
	if def.PatternProperties != nil {
		for _, s := range def.PatternProperties {
			findReferenced(&s, allSchemas, referencedOut)
		}
	}
	if def.Dependencies != nil {
		for _, d := range def.Dependencies {
			if d.Schema != nil {
				findReferenced(d.Schema, allSchemas, referencedOut)
			}
		}
	}
	if def.AdditionalItems != nil && def.AdditionalItems.Schema != nil {
		findReferenced(def.AdditionalItems.Schema, allSchemas, referencedOut)
	}
	if def.Definitions != nil {
		for _, s := range def.Definitions {
			findReferenced(&s, allSchemas, referencedOut)
		}
	}
}
