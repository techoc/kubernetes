/*
Copyright 2016 The Kubernetes Authors.

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
	"sort"
	"strings"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	codegennamer "k8s.io/code-generator/pkg/namer"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// // genericGenerator 生成 generic informer。
//
// generic informer 允许调用方按 GroupVersionResource 动态获取 informer，
// 无需在编译期知道具体类型；ForResource 的 switch 由本生成器展开。
type genericGenerator struct {
	generator.GoGenerator
	outputPackage        string
	imports              namer.ImportTracker
	groupVersions        map[string]clientgentypes.GroupVersions
	groupGoNames         map[string]string
	pluralExceptions     map[string]string
	typesForGroupVersion map[clientgentypes.GroupVersion][]*types.Type
	// filtered 保证只生成一次，本文件与具体类型无关。
	filtered bool
}

var _ generator.Generator = &genericGenerator{}

// Filter 仅第一次返回 true，避免每个类型都重复生成同一文件。
func (g *genericGenerator) Filter(c *generator.Context, t *types.Type) bool {
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 注册模板中使用的命名系统。
// resource 使用 +resourceName 覆盖，与 client-gen 保持一致，
// 否则 ForResource 的 switch 分支会与实际 REST 资源名不匹配。
func (g *genericGenerator) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw":                namer.NewRawNamer(g.outputPackage, g.imports),
		"allLowercasePlural": namer.NewAllLowercasePluralNamer(g.pluralExceptions),
		"publicPlural":       namer.NewPublicPluralNamer(g.pluralExceptions),
		"resource":           codegennamer.NewTagOverrideNamer("resourceName", namer.NewAllLowercasePluralNamer(g.pluralExceptions)),
	}
}

// Imports 输出模板实际用到的 import 行。
func (g *genericGenerator) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	return
}

// group 是模板渲染用的中间结构，代表一个 API group 及其所有版本。
type group struct {
	GroupGoName string
	Name        string
	Versions    []*version
}

// groupSort 按 group 名（忽略大小写）排序，保证生成文件中 case 分支顺序稳定。
type groupSort []group

func (g groupSort) Len() int { return len(g) }
func (g groupSort) Less(i, j int) bool {
	return strings.ToLower(g[i].Name) < strings.ToLower(g[j].Name)
}
func (g groupSort) Swap(i, j int) { g[i], g[j] = g[j], g[i] }

// version 是模板渲染用的中间结构，代表一个版本及其资源类型。
type version struct {
	Name      string
	GoName    string
	Resources []*types.Type
}

// versionSort 按版本名（忽略大小写）排序。
type versionSort []*version

func (v versionSort) Len() int { return len(v) }
func (v versionSort) Less(i, j int) bool {
	return strings.ToLower(v[i].Name) < strings.ToLower(v[j].Name)
}
func (v versionSort) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

// GenerateType 构造 group/version/resource 三层结构并渲染 GenericInformer 与 ForResource。
//
// 排序是必须的：group 与 version 的遍历来自 map，若不排序，生成的 switch
// 分支顺序会随运行变化，破坏代码生成的幂等性。
func (g *genericGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	groups := []group{}
	// schemeGVs 把每个 version 映射到其包内的 SchemeGroupVersion 变量，
	// 模板里用它构造 GroupVersionResource 做 switch 匹配。
	schemeGVs := make(map[*version]*types.Type)

	orderer := namer.Orderer{Namer: namer.NewPrivateNamer(0)}
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
				Resources: orderer.OrderTypes(g.typesForGroupVersion[gv]),
			}
			// 取该 group-version 首个类型的包路径来定位 SchemeGroupVersion；
			// 该变量由 client-gen 的 scheme 生成器产出。
			func() {
				schemeGVs[version] = c.Universe.Variable(types.Name{Package: g.typesForGroupVersion[gv][0].Name.Package, Name: "SchemeGroupVersion"})
			}()
			group.Versions = append(group.Versions, version)
		}
		sort.Sort(versionSort(group.Versions))
		groups = append(groups, group)
	}
	sort.Sort(groupSort(groups))

	m := map[string]interface{}{
		"cacheGenericLister":         c.Universe.Type(cacheGenericLister),
		"cacheNewGenericLister":      c.Universe.Function(cacheNewGenericLister),
		"cacheSharedIndexInformer":   c.Universe.Type(cacheSharedIndexInformer),
		"fmtErrorf":                  c.Universe.Type(fmtErrorfFunc),
		"groups":                     groups,
		"schemeGVs":                  schemeGVs,
		"schemaGroupResource":        c.Universe.Type(schemaGroupResource),
		"schemaGroupVersionResource": c.Universe.Type(schemaGroupVersionResource),
	}

	sw.Do(genericInformer, m)
	sw.Do(forResource, m)

	return sw.Error()
}

var genericInformer = `
// GenericInformer is type of SharedIndexInformer which will locate and delegate to other
// sharedInformers based on type
type GenericInformer interface {
	Informer() {{.cacheSharedIndexInformer|raw}}
	Lister() {{.cacheGenericLister|raw}}
}

type genericInformer struct {
	informer {{.cacheSharedIndexInformer|raw}}
	resource {{.schemaGroupResource|raw}}
}

// Informer returns the SharedIndexInformer.
func (f *genericInformer) Informer() {{.cacheSharedIndexInformer|raw}} {
	return f.informer
}

// Lister returns the GenericLister.
func (f *genericInformer) Lister() {{.cacheGenericLister|raw}} {
	return {{.cacheNewGenericLister|raw}}(f.Informer().GetIndexer(), f.resource)
}
`

var forResource = `
// ForResource gives generic access to a shared informer of the matching type
// TODO extend this to unknown resources with a client pool
func (f *sharedInformerFactory) ForResource(resource {{.schemaGroupVersionResource|raw}}) (GenericInformer, error) {
	switch resource {
		{{range $group := .groups -}}{{$GroupGoName := .GroupGoName -}}
			{{range $version := .Versions -}}
	// Group={{$group.Name}}, Version={{.Name}}
				{{range .Resources -}}
	case {{index $.schemeGVs $version|raw}}.WithResource("{{.|resource}}"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.{{$GroupGoName}}().{{$version.GoName}}().{{.|publicPlural}}().Informer()}, nil
				{{end}}
			{{end}}
		{{end -}}
	}

	return nil, {{.fmtErrorf|raw}}("no informer found for %v", resource)
}
`
