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

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
)

// 该文件为每个 group-version 提供 Interface 接口和 version 实现，
// 是共享 factory 按版本访问 informer 的入口。
type versionInterfaceGenerator struct {
	generator.GoGenerator
	outputPackage string
	imports       namer.ImportTracker
	types         []*types.Type
	// filtered 保证整个 target 只渲染一次。
	filtered                  bool
	internalInterfacesPackage string
}

var _ generator.Generator = &versionInterfaceGenerator{}

// Filter 仅第一次返回 true，之后跳过。
func (g *versionInterfaceGenerator) Filter(c *generator.Context, t *types.Type) bool {
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 只注册 raw 命名系统。
func (g *versionInterfaceGenerator) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 输出模板实际用到的 import 行。
func (g *versionInterfaceGenerator) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	return
}

// GenerateType 先渲染接口定义，再为每个类型渲染访问方法。
//
// 方法返回 Typed*Informer，与 informer.go 中生成的类型保持一致；
// namespaced 取自 genclient 标签，决定是否把 namespace 传入构造。
func (g *versionInterfaceGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	m := map[string]interface{}{
		"interfacesTweakListOptionsFunc":  c.Universe.Type(types.Name{Package: g.internalInterfacesPackage, Name: "TweakListOptionsFunc"}),
		"interfacesSharedInformerFactory": c.Universe.Type(types.Name{Package: g.internalInterfacesPackage, Name: "SharedInformerFactory"}),
		"types":                           g.types,
	}

	sw.Do(versionTemplate, m)
	for _, typeDef := range g.types {
		tags, err := util.ParseClientGenTags(append(typeDef.SecondClosestCommentLines, typeDef.CommentLines...))
		if err != nil {
			return err
		}
		m["namespaced"] = !tags.NonNamespaced
		m["type"] = typeDef
		sw.Do(versionFuncTemplate, m)
	}

	return sw.Error()
}

var versionTemplate = `
// Interface provides access to all the informers in this group version.
type Interface interface {
	$range .types -$
		// $.|publicPlural$ returns a $.|public$Informer.
		$.|publicPlural$() Typed$.|public$Informer
	$end$
}

type version struct {
	factory $.interfacesSharedInformerFactory|raw$
	namespace string
	tweakListOptions $.interfacesTweakListOptionsFunc|raw$
}

// New returns a new Interface.
func New(f $.interfacesSharedInformerFactory|raw$, namespace string, tweakListOptions $.interfacesTweakListOptionsFunc|raw$) Interface {
	return &version{factory: f, namespace: namespace, tweakListOptions: tweakListOptions}
}
`

var versionFuncTemplate = `
// $.type|publicPlural$ returns a Typed$.type|public$Informer.
func (v *version) $.type|publicPlural$() Typed$.type|public$Informer {
	return &$.type|private$Informer{factory: v.factory$if .namespaced$, namespace: v.namespace$end$, tweakListOptions: v.tweakListOptions}
}
`
