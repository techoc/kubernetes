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
	"path"
	"sort"
	"strings"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// utilGenerator 生成 ForKind() 工具函数。
// 同时生成 NewTypeConverter：前者按 GVK 返回空的 apply configuration 实例，
// 后者结合内嵌 schema 在运行期完成对象到 apply configuration 的转换。
type utilGenerator struct {
	generator.GoGenerator
	outputPackage        string
	imports              namer.ImportTracker
	groupVersions        map[string]clientgentypes.GroupVersions
	groupGoNames         map[string]string
	typesForGroupVersion map[clientgentypes.GroupVersion][]applyConfig
	filtered             bool
	typeModels           *typeModels
}

var _ generator.Generator = &utilGenerator{}

// Filter 保证 utils.go 整体只生成一次。
func (g *utilGenerator) Filter(*generator.Context, *types.Type) bool {
	// 工具文件与具体类型无关，整个生成过程只输出一次。
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 提供 raw 与 singularKind 命名系统。
func (g *utilGenerator) Namers(*generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw":          namer.NewRawNamer(g.outputPackage, g.imports),
		"singularKind": namer.NewPublicNamer(0),
	}
}

// Imports 返回收集到的 import 行。
func (g *utilGenerator) Imports(*generator.Context) (imports []string) {
	return g.imports.ImportLines()
}

// group 描述一个 API 群组及其版本，供模板按群组/版本分组输出。
type group struct {
	GroupGoName string
	Name        string
	Versions    []*version
}

// groupSort 按群组名（忽略大小写）排序，保证生成顺序稳定。
type groupSort []group

func (g groupSort) Len() int { return len(g) }
func (g groupSort) Less(i, j int) bool {
	return strings.ToLower(g[i].Name) < strings.ToLower(g[j].Name)
}
func (g groupSort) Swap(i, j int) { g[i], g[j] = g[j], g[i] }

// version 描述一个群组版本及其资源列表。
type version struct {
	Name      string
	GoName    string
	Resources []applyConfig
}

// versionSort 按版本名（忽略大小写）排序。
type versionSort []*version

func (v versionSort) Len() int { return len(v) }
func (v versionSort) Less(i, j int) bool {
	return strings.ToLower(v[i].Name) < strings.ToLower(v[j].Name)
}
func (v versionSort) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

// applyConfig 关联原始 API 类型、对应的 apply configuration 类型
// 以及 OpenAPI 模型中的名称。
type applyConfig struct {
	Type               *types.Type
	ApplyConfiguration *types.Type
	OpenAPIName        string
}

// applyConfigSort 按类型名排序，稳定输出。
type applyConfigSort []applyConfig

func (v applyConfigSort) Len() int { return len(v) }
func (v applyConfigSort) Less(i, j int) bool {
	return strings.ToLower(v[i].Type.Name.Name) < strings.ToLower(v[j].Type.Name.Name)
}
func (v applyConfigSort) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

// GenerateType 汇总所有群组/版本/资源，输出 ForKind 与 NewTypeConverter。
func (g *utilGenerator) GenerateType(c *generator.Context, _ *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	var groups []group
	schemeGVs := make(map[*version]*types.Type)

	for groupPackageName, groupVersions := range g.groupVersions {
		group := group{
			GroupGoName: g.groupGoNames[groupPackageName],
			Name:        groupVersions.Group.NonEmpty(),
			Versions:    []*version{},
		}
		for _, v := range groupVersions.Versions {
			gv := clientgentypes.GroupVersion{Group: groupVersions.Group, Version: v.Version}
			version := &version{
				Name:      v.Version.NonEmpty(),
				GoName:    namer.IC(v.Version.NonEmpty()),
				Resources: g.typesForGroupVersion[gv],
			}
			// 记录该版本对应的 SchemeGroupVersion 符号，供模板生成
			// gv.WithKind(...) 形式的 case 分支。资源列表非空由上游保证。
			schemeGVs[version] = c.Universe.Variable(types.Name{
				Package: g.typesForGroupVersion[gv][0].Type.Name.Package,
				Name:    "SchemeGroupVersion",
			})
			group.Versions = append(group.Versions, version)
		}
		sort.Sort(versionSort(group.Versions))
		groups = append(groups, group)
	}
	sort.Sort(groupSort(groups))

	m := map[string]interface{}{
		"applyConfiguration":     applyConfiguration,
		"groups":                 groups,
		"internalParser":         types.Ref(path.Join(g.outputPackage, "internal"), "Parser"),
		"runtimeScheme":          runtimeScheme,
		"schemeGVs":              schemeGVs,
		"schemaGroupVersionKind": groupVersionKind,
		"typeConverter":          typeConverter,
		"newSchemeTypeConverter": newSchemeTypeConverter,
	}
	sw.Do(forKindFunc, m)
	sw.Do(newTypeConverterFunc, m)

	return sw.Error()
}

// newTypeConverterFunc 模板：将内嵌 schema 与 scheme 组合成转换器。
var newTypeConverterFunc = `
func NewTypeConverter(scheme *{{.runtimeScheme|raw}}) {{.typeConverter|raw}} {
	return {{.newSchemeTypeConverter|raw}}(scheme, {{.internalParser|raw}}())
}
`

var forKindFunc = `
// ForKind returns an apply configuration type for the given GroupVersionKind, or nil if no
// apply configuration type exists for the given GroupVersionKind.
func ForKind(kind {{.schemaGroupVersionKind|raw}}) interface{} {
	switch kind {
		{{range $group := .groups -}}{{$GroupGoName := .GroupGoName -}}
			{{range $version := .Versions -}}
	// Group={{$group.Name}}, Version={{.Name}}
				{{range .Resources -}}
	case {{index $.schemeGVs $version|raw}}.WithKind("{{.Type|singularKind}}"):
		return &{{.ApplyConfiguration|raw}}{}
				{{end}}
			{{end}}
		{{end -}}
	}
	return nil
}
`
