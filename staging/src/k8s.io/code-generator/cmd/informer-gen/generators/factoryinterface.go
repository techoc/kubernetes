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

	"k8s.io/klog/v2"
)

// factoryInterfaceGenerator 生成用于打破 informer 注册依赖环的接口文件。
//
// 生成的 factory 接口不依赖具体 group 的 typed informer，因此能被低层包引用，
// 从而打破 informer 与 clientset 之间的循环导入。
type factoryInterfaceGenerator struct {
	generator.GoGenerator
	outputPackage    string
	imports          namer.ImportTracker
	clientSetPackage string
	// filtered 保证整个 target 只生成一次：gengo 会按类型调用 Filter，
	// 而本文件与具体类型无关。
	filtered bool
}

var _ generator.Generator = &factoryInterfaceGenerator{}

// Filter 仅在第一次调用时返回 true，之后一律跳过。
func (g *factoryInterfaceGenerator) Filter(c *generator.Context, t *types.Type) bool {
	if !g.filtered {
		g.filtered = true
		return true
	}
	return false
}

// Namers 只注册 raw 命名系统，模板中所有引用都走 import tracker 解析。
func (g *factoryInterfaceGenerator) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 输出模板实际用到的 import 行。
func (g *factoryInterfaceGenerator) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	return
}

// GenerateType 把模板渲染到输出文件。
// 传入的 t 仅用于触发一次生成，模板内容与它无关。
func (g *factoryInterfaceGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	klog.V(5).Infof("processing type %v", t)

	// 模板变量统一使用类型而非字符串，确保 import 由 tracker 正确管理。
	m := map[string]interface{}{
		"cacheIndexers":            c.Universe.Type(cacheIndexers),
		"cacheInformerName":        c.Universe.Type(cacheInformerName),
		"cacheSharedIndexInformer": c.Universe.Type(cacheSharedIndexInformer),
		"clientSetPackage":         c.Universe.Type(types.Name{Package: g.clientSetPackage, Name: "Interface"}),
		"runtimeObject":            c.Universe.Type(runtimeObject),
		"timeDuration":             c.Universe.Type(timeDuration),
		"v1ListOptions":            c.Universe.Type(v1ListOptions),
	}

	sw.Do(externalSharedInformerFactoryInterface, m)

	return sw.Error()
}

var externalSharedInformerFactoryInterface = `
// NewInformerFunc takes {{.clientSetPackage|raw}} and {{.timeDuration|raw}} to return a SharedIndexInformer.
type NewInformerFunc func({{.clientSetPackage|raw}}, {{.timeDuration|raw}}) cache.SharedIndexInformer

// SharedInformerFactory a small interface to allow for adding an informer without an import cycle
type SharedInformerFactory interface {
	Start(stopCh <-chan struct{})
	InformerFor(obj {{.runtimeObject|raw}}, newFunc NewInformerFunc) {{.cacheSharedIndexInformer|raw}}
	InformerName() *{{.cacheInformerName|raw}}
}

// TweakListOptionsFunc is a function that transforms a {{.v1ListOptions|raw}}.
type TweakListOptionsFunc func(*{{.v1ListOptions|raw}})

// InformerOptions holds the options for creating an informer.
type InformerOptions struct {
	// ResyncPeriod is the resync period for this informer.
	// If not set, defaults to 0 (no resync).
	ResyncPeriod {{.timeDuration|raw}}

	// Indexers are the indexers for this informer.
	Indexers {{.cacheIndexers|raw}}

	// InformerName is used to uniquely identify this informer for metrics.
	// If not set, metrics will not be published for this informer.
	// Use cache.NewInformerName() to create an InformerName at startup.
	InformerName *{{.cacheInformerName|raw}}

	// TweakListOptions is an optional function to modify the list options.
	TweakListOptions TweakListOptionsFunc
}
`
