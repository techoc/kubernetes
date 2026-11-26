/*
Copyright 2017 The Kubernetes Authors.

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

package openapi

import (
	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/util/proto"
	"sigs.k8s.io/yaml"
)

// 返回 OpenAPI V2 资源定义的函数，
// 用于延迟加载这些定义。
type OpenAPIResourcesGetter interface {
	OpenAPISchema() (Resources, error)
}

// Resources 接口描述能按 GVK 提供资源的提供者。
type Resources interface {
	LookupResource(gvk schema.GroupVersionKind) proto.Schema
	GetConsumes(gvk schema.GroupVersionKind, operation string) []string
}

// 从对象定义的 "extensions" map 中取出 GVK 所用的键。
const groupVersionKindExtensionKey = "x-kubernetes-group-version-kind"

// 在 openapi Schema 中查找资源的 Resources 实现。
type document struct {
	// 把 GVK 映射到模型名。
	resources map[schema.GroupVersionKind]string
	models    proto.Models
	doc       *openapi_v2.Document
}

var _ Resources = &document{}

// 从 openapi 文档创建 Resources。
func NewOpenAPIData(doc *openapi_v2.Document) (Resources, error) {
	models, err := proto.NewOpenAPIData(doc)
	if err != nil {
		return nil, err
	}

	resources := map[schema.GroupVersionKind]string{}
	for _, modelName := range models.ListModels() {
		model := models.LookupModel(modelName)
		if model == nil {
			panic("ListModels returns a model that can't be looked-up.")
		}
		gvkList := parseGroupVersionKind(model)
		for _, gvk := range gvkList {
			if len(gvk.Kind) > 0 {
				resources[gvk] = modelName
			}
		}
	}

	return &document{
		resources: resources,
		models:    models,
		doc:       doc,
	}, nil
}

func (d *document) LookupResource(gvk schema.GroupVersionKind) proto.Schema {
	modelName, found := d.resources[gvk]
	if !found {
		return nil
	}
	return d.models.LookupModel(modelName)
}

func (d *document) GetConsumes(gvk schema.GroupVersionKind, operation string) []string {
	for _, path := range d.doc.GetPaths().GetPath() {
		for _, ex := range path.GetValue().GetPatch().GetVendorExtension() {
			if ex.GetValue().GetYaml() == "" ||
				ex.GetName() != "x-kubernetes-group-version-kind" {
				continue
			}

			var value map[string]string
			err := yaml.Unmarshal([]byte(ex.GetValue().GetYaml()), &value)
			if err != nil {
				continue
			}

			if value["group"] == gvk.Group && value["kind"] == gvk.Kind && value["version"] == gvk.Version {
				switch operation {
				case "GET":
					return path.GetValue().GetGet().GetConsumes()
				case "PATCH":
					return path.GetValue().GetPatch().GetConsumes()
				case "HEAD":
					return path.GetValue().GetHead().GetConsumes()
				case "PUT":
					return path.GetValue().GetPut().GetConsumes()
				case "POST":
					return path.GetValue().GetPost().GetConsumes()
				case "OPTIONS":
					return path.GetValue().GetOptions().GetConsumes()
				case "DELETE":
					return path.GetValue().GetDelete().GetConsumes()
				}
			}
		}
	}

	return nil
}

// 从扩展信息中解析 GVK，不存在时返回空值。
func parseGroupVersionKind(s proto.Schema) []schema.GroupVersionKind {
	extensions := s.GetExtensions()

	gvkListResult := []schema.GroupVersionKind{}

	// 取扩展信息。
	gvkExtension, ok := extensions[groupVersionKindExtensionKey]
	if !ok {
		return []schema.GroupVersionKind{}
	}

	// gvk 扩展必须是至少含一个元素的列表。
	gvkList, ok := gvkExtension.([]interface{})
	if !ok {
		return []schema.GroupVersionKind{}
	}

	for _, gvk := range gvkList {
		// gvk 扩展列表的元素必须是含 group、version、kind
		// 字段的 map。
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
