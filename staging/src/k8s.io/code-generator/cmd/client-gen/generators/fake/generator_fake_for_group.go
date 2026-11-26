/*
Copyright 2015 The Kubernetes Authors.

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

package fake

import (
	"fmt"
	"io"
	"path"
	"strings"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
)

// // genFakeForGroup 为一个 group 生成聚合客户端文件（如 extension group 的 ExtensionsClient）。
//
// 它输出的是“每组一个”的聚合类型 Fake<Group><Version>，其中嵌入 client-go/testing
// 的 Fake，并暴露该组内每个类型的 getter；每类型的具体实现由 genFakeForType 生成。
type genFakeForGroup struct {
	generator.GoGenerator
	outputPackage     string // 必须是 Go import path
	realClientPackage string // 必须是 Go import path
	version           string
	groupGoName       string
	// // 该 group 下的所有类型
	types   []*types.Type
	imports namer.ImportTracker
	// // 是否已生成过；group client 只需生成一次
	called bool
}

var _ generator.Generator = &genFakeForGroup{}

// // Filter 保证每组只调用一次 GenerateType：首次调用后置位 called。
// 通过首次调用后置位 called 实现“整组只生成一次”；Filter 由 gengo 对每个类型调用。
func (g *genFakeForGroup) Filter(c *generator.Context, t *types.Type) bool {
	if !g.called {
		g.called = true
		return true
	}
	return false
}

// Namers 只注册 raw namer：模板里所有类型都按完整限定名书写并依赖 import 追踪。
func (g *genFakeForGroup) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 除模板引用的包外，还需要真实客户端包——getter 的返回类型是
// 真实客户端的 <Type>Interface，别名取其包路径最后一段的小写形式。
func (g *genFakeForGroup) Imports(c *generator.Context) (imports []string) {
	imports = g.imports.ImportLines()
	if len(g.types) != 0 {
		imports = append(imports, fmt.Sprintf("%s \"%s\"", strings.ToLower(path.Base(g.realClientPackage)), g.realClientPackage))
	}
	return imports
}

// GenerateType 写出组级 Fake 类型以及各类型的构造 getter，最后补上 RESTClient 桩方法。
func (g *genFakeForGroup) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	// 模板参数集中于此；具体类型的 getter 需要每个类型自己的 tags 来决定是否带 namespace。
	m := map[string]interface{}{
		"GroupGoName":         g.groupGoName,
		"Version":             namer.IC(g.version),
		"Fake":                c.Universe.Type(types.Name{Package: "k8s.io/client-go/testing", Name: "Fake"}),
		"RESTClientInterface": c.Universe.Type(types.Name{Package: "k8s.io/client-go/rest", Name: "Interface"}),
		"RESTClient":          c.Universe.Type(types.Name{Package: "k8s.io/client-go/rest", Name: "RESTClient"}),
		"FakeClient":          c.Universe.Type(types.Name{Package: "k8s.io/client-go/gentype", Name: "FakeClient"}),
		"NewFakeClient":       c.Universe.Function(types.Name{Package: "k8s.io/client-go/gentype", Name: "NewFakeClient"}),
	}

	sw.Do(groupClientTemplate, m)
	for _, t := range g.types {
		// 命名空间感知来自类型注释，与真实客户端生成器使用同一份 tags 解析逻辑。
		tags, err := util.ParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
		if err != nil {
			return err
		}
		wrapper := map[string]interface{}{
			"type":              t,
			"GroupGoName":       g.groupGoName,
			"Version":           namer.IC(g.version),
			"realClientPackage": strings.ToLower(path.Base(g.realClientPackage)),
		}
		if tags.NonNamespaced {
			sw.Do(getterImplNonNamespaced, wrapper)
			continue
		}
		sw.Do(getterImplNamespaced, wrapper)
	}
	sw.Do(getRESTClient, m)
	return sw.Error()
}

// groupClientTemplate 生成 Fake<Group><Version>，嵌入 testing.Fake 以复用 action 记录能力。
var groupClientTemplate = `
type Fake$.GroupGoName$$.Version$ struct {
	*$.Fake|raw$
}
`

// getterImplNamespaced 生成带 namespace 的 getter，返回真实客户端接口以保持签名一致。
var getterImplNamespaced = `
func (c *Fake$.GroupGoName$$.Version$) $.type|publicPlural$(namespace string) $.realClientPackage$.$.type|public$Interface {
	return newFake$.type|publicPlural$(c, namespace)
}
`

// getterImplNonNamespaced 是不带 namespace 的版本；选用哪个由 +genclient:nonNamespaced 决定。
var getterImplNonNamespaced = `
func (c *Fake$.GroupGoName$$.Version$) $.type|publicPlural$() $.realClientPackage$.$.type|public$Interface {
	return newFake$.type|publicPlural$(c)
}
`

// getRESTClient 返回 nil *rest.RESTClient 作为接口值；fake 客户端没有真实连接，
// 但必须满足接口，调用方通常不会真正使用它。
var getRESTClient = `
// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *Fake$.GroupGoName$$.Version$) RESTClient() $.RESTClientInterface|raw$ {
	var ret *$.RESTClient|raw$
	return ret
}
`
