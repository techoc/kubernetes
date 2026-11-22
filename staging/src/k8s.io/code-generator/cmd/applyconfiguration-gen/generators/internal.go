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
	"io"

	yaml "go.yaml.in/yaml/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/kube-openapi/pkg/schemaconv"
)

// internalGenerator 生成 internal 子包中的 Parser() 与内嵌 schema。
// 该 schema 是生成代码执行 Extract/Apply 时定位 apply configuration 类型的
// 依据，因此必须与本次生成使用的 OpenAPI 模型完全一致。
type internalGenerator struct {
	generator.GoGenerator
	outputPackage string
	imports       namer.ImportTracker
	typeModels    *typeModels
	filtered      bool
}

var _ generator.Generator = &internalGenerator{}

// Filter 让整个生成器只运行一次：schema 是全局的，不需要按类型重复生成。
func (g *internalGenerator) Filter(*generator.Context, *types.Type) bool {
	// generate file exactly once
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 提供 raw（带 import 管理）与 singularKind 两套命名系统，
// 后者用于生成 ForKind 接口所需的单数 Kind 名称。
func (g *internalGenerator) Namers(*generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw":          namer.NewRawNamer(g.outputPackage, g.imports),
		"singularKind": namer.NewPublicNamer(0),
	}
}

// Imports 返回本文件已收集的 import 行。
func (g *internalGenerator) Imports(*generator.Context) (imports []string) {
	return g.imports.ImportLines()
}

// GenerateType 将 OpenAPI 模型序列化为 YAML 并写入生成的 schema 常量。
// 序列化失败意味着模型自身不一致，应中断生成而不是产出无法解析的代码。
func (g *internalGenerator) GenerateType(c *generator.Context, _ *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	schema, err := schemaconv.ToSchema(g.typeModels.models)
	if err != nil {
		return err
	}
	schemaYAML, err := yaml.Marshal(schema)
	if err != nil {
		return err
	}
	sw.Do(schemaBlock, map[string]interface{}{
		"schemaYAML":   string(schemaYAML),
		"smdParser":    smdParser,
		"smdNewParser": smdNewParser,
		"fmtSprintf":   fmtSprintf,
		"syncOnce":     syncOnce,
		"yamlObject":   yamlObject,
	})

	return sw.Error()
}

// schemaBlock 是生成代码的模板。注意其内容会被原样写入目标文件：
// parserOnce 保证 schema 只解析一次，解析失败直接 panic 是刻意的——
// 这属于生成产物与运行时库版本不匹配，应尽早暴露。
var schemaBlock = `
func Parser() *{{.smdParser|raw}} {
	parserOnce.Do(func() {
		var err error
		parser, err = {{.smdNewParser|raw}}(schemaYAML)
		if err != nil {
			panic({{.fmtSprintf|raw}}("Failed to parse schema: %v", err))
		}
	})
	return parser
}

var parserOnce {{.syncOnce|raw}}
var parser *{{.smdParser|raw}}
var schemaYAML = {{.yamlObject|raw}}(` + "`{{.schemaYAML}}`" + `)
`
