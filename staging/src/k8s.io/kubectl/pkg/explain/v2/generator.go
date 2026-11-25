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

package v2

import (
	"fmt"
	"io"
	"text/template"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Generator interface {
	AddTemplate(name string, contents string) error

	Render(
		// 用于渲染的模板
		templateName string,
		// 自包含的 OpenAPI 文档，包含 $ref 所使用的所有 schema
		// 仅支持 OpenAPI V3 文档
		document map[string]interface{},
		// OpenAPI 文档中要为其渲染 explain schema 的资源
		gvr schema.GroupVersionResource,
		// 要将输出聚焦到的资源子字段路径
		fieldSelector []string,
		// 布尔值，指示字段是否应递归/深度渲染
		recursive bool,
		// recursive 为 true 时的最大递归深度。0 表示无限制。
		maxDepth int,
		// 输出写入器
		writer io.Writer,
	) error
}

type TemplateContext struct {
	GVR       schema.GroupVersionResource
	Document  map[string]interface{}
	Recursive bool
	MaxDepth  int
	FieldPath []string
}

type generator struct {
	templates map[string]*template.Template
}

func NewGenerator() Generator {
	return &generator{
		templates: make(map[string]*template.Template),
	}
}

func (g *generator) AddTemplate(name string, contents string) error {
	compiled, err := WithBuiltinTemplateFuncs(template.New(name)).Parse(contents)

	if err != nil {
		return err
	}

	g.templates[name] = compiled
	return nil
}

func (g *generator) Render(
	// 用于渲染的模板
	templateName string,
	// 自包含的 OpenAPI 文档，包含 $ref 所使用的所有 schema
	// 仅支持 OpenAPI V3 文档
	document map[string]interface{},
	// OpenAPI 文档中要为其渲染 explain schema 的资源
	gvr schema.GroupVersionResource,
	// 要将输出聚焦到的资源子字段路径
	fieldSelector []string,
	// 布尔值，指示字段是否应递归/深度渲染
	recursive bool,
	// recursive 为 true 时的最大递归深度。0 表示无限制。
	maxDepth int,
	// 输出写入器
	writer io.Writer,
) error {
	compiledTemplate, ok := g.templates[templateName]
	if !ok {
		return fmt.Errorf("unrecognized format: %s", templateName)
	}

	err := compiledTemplate.Execute(writer, TemplateContext{
		Document:  document,
		Recursive: recursive,
		MaxDepth:  maxDepth,
		FieldPath: fieldSelector,
		GVR:       gvr,
	})
	return err
}
