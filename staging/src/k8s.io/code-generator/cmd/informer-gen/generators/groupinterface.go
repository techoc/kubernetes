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
	"path"
	"strings"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// groupInterfaceGenerator 生成 group 级接口文件。
// 输出 <group>/interface.go：一个 group 对应一个文件，内含该组所有版本访问器。
type groupInterfaceGenerator struct {
	generator.GoGenerator
	outputPackage             string
	imports                   namer.ImportTracker
	groupVersions             clientgentypes.GroupVersions
	filtered                  bool // 组级生成器只应触发一次，用它保证 Filter 幂等
	internalInterfacesPackage string
}

var _ generator.Generator = &groupInterfaceGenerator{}

// Filter 首次调用返回 true，之后恒为 false：文件级生成器需要恰好一次输出机会，
// 而 gengo 的 Filter 是对每个类型调用的，因此用状态位模拟“只生成一次”。
func (g *groupInterfaceGenerator) Filter(c *generator.Context, t *types.Type) bool {
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 注册 raw 命名系统。
func (g *groupInterfaceGenerator) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 返回生成文件所需的导入行。
func (g *groupInterfaceGenerator) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	return
}

// versionData 描述模板中单个版本的访问器：名称、返回接口以及构造函数。
type versionData struct {
	Name      string
	Interface *types.Type
	New       *types.Type
}

// GenerateType 生成组级 Interface 及其实现。
// 各版本的 Interface/New 通过 Universe 按“输出包/版本”路径引用，
// 因此生成顺序无关，只要同目录下对应版本的 informer 文件存在即可编译。
func (g *groupInterfaceGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	versions := make([]versionData, 0, len(g.groupVersions.Versions))
	for _, version := range g.groupVersions.Versions {
		gv := clientgentypes.GroupVersion{Group: g.groupVersions.Group, Version: version.Version}
		versionPackage := path.Join(g.outputPackage, strings.ToLower(gv.Version.NonEmpty()))
		iface := c.Universe.Type(types.Name{Package: versionPackage, Name: "Interface"})
		versions = append(versions, versionData{
			// 版本号在 Go 标识符里需要合法化（如 v1beta1 → V1beta1）。
			Name:      namer.IC(version.Version.NonEmpty()),
			Interface: iface,
			New:       c.Universe.Function(types.Name{Package: versionPackage, Name: "New"}),
		})
	}
	m := map[string]interface{}{
		"interfacesTweakListOptionsFunc":  c.Universe.Type(types.Name{Package: g.internalInterfacesPackage, Name: "TweakListOptionsFunc"}),
		"interfacesSharedInformerFactory": c.Universe.Type(types.Name{Package: g.internalInterfacesPackage, Name: "SharedInformerFactory"}),
		"versions":                        versions,
	}

	sw.Do(groupTemplate, m)

	return sw.Error()
}

var groupTemplate = `
// Interface provides access to each of this group's versions.
type Interface interface {
	$range .versions -$
		// $.Name$ provides access to shared informers for resources in $.Name$.
		$.Name$() $.Interface|raw$
	$end$
}

type group struct {
	factory $.interfacesSharedInformerFactory|raw$
	namespace string
	tweakListOptions  $.interfacesTweakListOptionsFunc|raw$
}

// New returns a new Interface.
func New(f $.interfacesSharedInformerFactory|raw$, namespace string, tweakListOptions $.interfacesTweakListOptionsFunc|raw$) Interface {
	return &group{factory: f, namespace: namespace, tweakListOptions: tweakListOptions}
}

$range .versions$
// $.Name$ returns a new $.Interface|raw$.
func (g *group) $.Name$() $.Interface|raw$ {
	return $.New|raw$(g.factory, g.namespace, g.tweakListOptions)
}
$end$
`
