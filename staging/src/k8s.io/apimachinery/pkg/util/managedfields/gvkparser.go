/*
Copyright 2018 The Kubernetes Authors.

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

package managedfields

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/schemaconv"
	"k8s.io/kube-openapi/pkg/util/proto"
	smdschema "sigs.k8s.io/structured-merge-diff/v6/schema"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
)

// groupVersionKindExtensionKey is the key used to lookup the
// GroupVersionKind value for an object definition from the
// definition's "extensions" map.
const groupVersionKindExtensionKey = "x-kubernetes-group-version-kind"

// GvkParser contains a Parser that allows introspecting the schema.
//
// 它把 GVK 映射到 OpenAPI 模型名，再由 structured-merge-diff 的 parser 提供
// 类型化的字段路径操作；managed fields 的合并与比较都依赖这一映射。
type GvkParser struct {
	gvks   map[schema.GroupVersionKind]string
	parser typed.Parser
}

// Type returns a helper which can produce objects of the given type. Any
// errors are deferred until a further function is called.
//
// 未知 GVK 返回 nil；调用方必须检查，否则会在后续字段操作时 panic。
func (p *GvkParser) Type(gvk schema.GroupVersionKind) *typed.ParseableType {
	typeName, ok := p.gvks[gvk]
	if !ok {
		return nil
	}
	t := p.parser.Type(typeName)
	return &t
}

// NewGVKParser builds a GVKParser from a proto.Models. This
// will automatically find the proper version of the object, and the
// corresponding schema information.
//
// 同一个 GVK 出现在多个模型中属于 OpenAPI 定义错误，直接返回错误而不是
// 后者覆盖前者，以免静默地选错 schema。
func NewGVKParser(models proto.Models, preserveUnknownFields bool) (*GvkParser, error) {
	typeSchema, err := schemaconv.ToSchemaWithPreserveUnknownFields(models, preserveUnknownFields)
	if err != nil {
		return nil, fmt.Errorf("failed to convert models to schema: %v", err)
	}
	parser := GvkParser{
		gvks: map[schema.GroupVersionKind]string{},
	}
	parser.parser = typed.Parser{Schema: smdschema.Schema{Types: typeSchema.Types}}
	for _, modelName := range models.ListModels() {
		model := models.LookupModel(modelName)
		if model == nil {
			panic(fmt.Sprintf("ListModels returns a model that can't be looked-up for: %v", modelName))
		}
		gvkList := parseGroupVersionKind(model)
		for _, gvk := range gvkList {
			if len(gvk.Kind) > 0 {
				_, ok := parser.gvks[gvk]
				if ok {
					return nil, fmt.Errorf("duplicate entry for %v", gvk)
				}
				parser.gvks[gvk] = modelName
			}
		}
	}
	return &parser, nil
}

// Get and parse GroupVersionKind from the extension. Returns empty if it doesn't have one.
//
// 容错解析：任何结构不符合预期的条目都被跳过而不是整体报错，因为扩展只是
// 元数据，不应阻止 schema 的正常使用。
func parseGroupVersionKind(s proto.Schema) []schema.GroupVersionKind {
	extensions := s.GetExtensions()

	gvkListResult := []schema.GroupVersionKind{}

	// Get the extensions
	gvkExtension, ok := extensions[groupVersionKindExtensionKey]
	if !ok {
		return []schema.GroupVersionKind{}
	}

	// gvk extension must be a list of at least 1 element.
	gvkList, ok := gvkExtension.([]interface{})
	if !ok {
		return []schema.GroupVersionKind{}
	}

	for _, gvk := range gvkList {
		// gvk extension list must be a map with group, version, and
		// kind fields
		gvkMap, ok := gvk.(map[interface{}]interface{})
		if !ok {
			continue
		}
		group, ok := gvkMap["group"].(string)
		if !ok {
			continue
		}
		version, ok := gvkMap["version"].(string)
		if !ok {
			continue
		}
		kind, ok := gvkMap["kind"].(string)
		if !ok {
			continue
		}

		gvkListResult = append(gvkListResult, schema.GroupVersionKind{
			Group:   group,
			Version: version,
			Kind:    kind,
		})
	}

	return gvkListResult
}
