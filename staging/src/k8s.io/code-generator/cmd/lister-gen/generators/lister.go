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
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/cmd/lister-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// // NameSystems 返回本包生成器使用的命名系统。
//
// 除 raw/public 等通用系统外，这里注册了 publicPlural、allLowercasePlural 和
// lowercaseSingular：lister 的接口名、资源名和构造函数名分别依赖它们，
// 复数特例（如 endpoints）由调用方通过 pluralExceptions 注入。
func NameSystems(pluralExceptions map[string]string) namer.NameSystems {
	return namer.NameSystems{
		"public":             namer.NewPublicNamer(0),
		"private":            namer.NewPrivateNamer(0),
		"raw":                namer.NewRawNamer("", nil),
		"publicPlural":       namer.NewPublicPluralNamer(pluralExceptions),
		"allLowercasePlural": namer.NewAllLowercasePluralNamer(pluralExceptions),
		"lowercaseSingular":  &lowercaseSingularNamer{},
	}
}

// lowercaseSingularNamer 实现 Namer 接口。
type lowercaseSingularNamer struct{}

// Name 返回类型名的小写形式。
func (n *lowercaseSingularNamer) Name(t *types.Type) string {
	return strings.ToLower(t.Name.Name)
}

// DefaultNameSystem 指定遍历类型时使用的默认命名系统。
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 构造 lister 的生成目标。
//
// 只为同时具备 list 和 get 动词的类型生成 lister：lister 的接口正是围绕
// List/Get 两个操作设计的。每个 GroupVersion 输出到 <group>/<version> 子目录。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, "", gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	var targetList []generator.Target
	for _, inputPkg := range context.Inputs {
		p := context.Universe.Package(inputPkg)

		// 是否生成完全由包级识别结果决定（+k8s:lister-gen 或类型级 opt-in）。
		info, err := apidefinitions.Identify(p, apidefinitions.Lister, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			continue
		}

		objectMeta, internal, err := objectMetaForPackage(p)
		if err != nil {
			klog.Fatal(err)
		}
		if objectMeta == nil {
			// // 包内没有带 genclient 的类型，不生成 lister
			continue
		}

		var gv clientgentypes.GroupVersion
		var internalGVPkg string

		// 内部版本没有版本号目录，group 名取自最后一段；外部版本最后两段是 group/version，
		// internalGVPkg 指回上一层的内部包，供 lister 引用 internal 类型。
		if internal {
			lastSlash := strings.LastIndex(p.Path, "/")
			if lastSlash == -1 {
				klog.Fatalf("error constructing internal group version for package %q", p.Path)
			}
			gv.Group = clientgentypes.Group(p.Path[lastSlash+1:])
			internalGVPkg = p.Path
		} else {
			parts := strings.Split(p.Path, "/")
			gv.Group = clientgentypes.Group(parts[len(parts)-2])
			gv.Version = clientgentypes.Version(parts[len(parts)-1])

			internalGVPkg = strings.Join(parts[0:len(parts)-1], "/")
		}
		groupPackageName := strings.ToLower(gv.Group.NonEmpty())

		// // +groupName 注释（如 +groupName=somegroup.foo.bar.io）可覆盖 group 名；
		// // 只取点号前的第一段，与 client-gen 的目录命名保持一致。
		// 生成时取首段作为 group 名。
		// 只取点号前的第一段，与 client-gen 的 group 目录命名保持一致。
		override, ok, err := apidefinitions.GroupNameForPackage(p.Comments)
		if err != nil {
			klog.Fatalf("error resolving group name: %v", err)
		}
		if ok {
			gv.Group = clientgentypes.Group(strings.SplitN(override, ".", 2)[0])
		}

		// lister 只对可 list、可 get 的资源有意义；排序保证生成顺序稳定。
		var typesToGenerate []*types.Type
		for _, t := range p.Types {
			tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
			if !tags.GenerateClient || !tags.HasVerb("list") || !tags.HasVerb("get") {
				continue
			}
			typesToGenerate = append(typesToGenerate, t)
		}
		if len(typesToGenerate) == 0 {
			continue
		}
		orderer := namer.Orderer{Namer: namer.NewPrivateNamer(0)}
		typesToGenerate = orderer.OrderTypes(typesToGenerate)

		// 输出目录与 import 路径都按 group/version 分层，与真实客户端包布局一致。
		subdir := []string{groupPackageName, strings.ToLower(gv.Version.NonEmpty())}
		outputDir := filepath.Join(args.OutputDir, filepath.Join(subdir...))
		outputPkg := path.Join(args.OutputPkg, path.Join(subdir...))
		targetList = append(targetList, &generator.SimpleTarget{
			PkgName:       strings.ToLower(gv.Version.NonEmpty()),
			PkgPath:       outputPkg,
			PkgDir:        outputDir,
			HeaderComment: boilerplate,
			FilterFunc: func(c *generator.Context, t *types.Type) bool {
				tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
				return tags.GenerateClient && tags.HasVerb("list") && tags.HasVerb("get")
			},
			GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
				// 每个 GroupVersion 一个 expansion 文件；每个类型一个独立 lister 文件。
				generators = append(generators, &expansionGenerator{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "expansion_generated.go",
					},
					outputPath: outputDir,
					types:      typesToGenerate,
				})

				for _, t := range typesToGenerate {
					generators = append(generators, &listerGenerator{
						GoGenerator: generator.GoGenerator{
							OutputFilename: strings.ToLower(t.Name.Name) + ".go",
						},
						outputPackage:  outputPkg,
						groupVersion:   gv,
						internalGVPkg:  internalGVPkg,
						typeToGenerate: t,
						imports:        generator.NewImportTrackerForPackage(outputPkg),
						objectMeta:     objectMeta,
					})
				}
				return generators
			},
		})
	}

	return targetList
}

// // objectMetaForPackage 返回包 p 使用的 ObjectMeta 类型。
//
// 第二个返回值表示该 ObjectMeta 是否来自 internal 版本；调用方据此决定 lister
// 引用内部类型还是版本化类型。包内有 genclient 类型却找不到 ObjectMeta 时视为错误。
func objectMetaForPackage(p *types.Package) (*types.Type, bool, error) {
	generatingForPackage := false
	for _, t := range p.Types {
		// // 过滤掉没有 genclient 的类型。
		if !util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...)).GenerateClient {
			continue
		}
		generatingForPackage = true
		for _, member := range t.Members {
			if member.Name == "ObjectMeta" {
				return member.Type, isInternal(member), nil
			}
		}
	}
	if generatingForPackage {
		return nil, false, fmt.Errorf("unable to find ObjectMeta for any types in package %s", p.Path)
	}
	return nil, false, nil
}

// // isInternal 通过成员缺少 json tag 判断其为内部版本类型。
//
// 内部版本类型的字段没有 json tag；这是区分 internal 与版本化 ObjectMeta 的简便判据。
func isInternal(m types.Member) bool {
	return !strings.Contains(m.Tags, "json")
}

// listerGenerator 为给定 GroupVersion 与类型生成 lister 文件。
type listerGenerator struct {
	generator.GoGenerator
	outputPackage  string
	groupVersion   clientgentypes.GroupVersion
	internalGVPkg  string
	typeToGenerate *types.Type
	imports        namer.ImportTracker
	objectMeta     *types.Type
}

var _ generator.Generator = &listerGenerator{}

// Filter 只放行构造时绑定的类型，保证一个文件只包含一个类型的 lister。
func (g *listerGenerator) Filter(c *generator.Context, t *types.Type) bool {
	return t == g.typeToGenerate
}

// Namers 注册 raw namer，模板中的类型按 outputPackage 解析并登记 import。
func (g *listerGenerator) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 返回模板引用到的 import。
func (g *listerGenerator) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	return
}

// GenerateType 生成 lister 接口、实现结构体和构造函数；命名空间资源额外生成
// 并生成 NamespaceLister。
func (g *listerGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	klog.V(5).Infof("processing type %v", t)
	// 模板参数：Resource 用于按资源名建立索引，objectMeta 决定可选字段的访问方式。
	m := map[string]interface{}{
		"Resource":               c.Universe.Function(types.Name{Package: t.Name.Package, Name: "Resource"}),
		"labelsSelector":         c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/labels", Name: "Selector"}),
		"listersResourceIndexer": c.Universe.Function(types.Name{Package: "k8s.io/client-go/listers", Name: "ResourceIndexer"}),
		"listersNew":             c.Universe.Function(types.Name{Package: "k8s.io/client-go/listers", Name: "New"}),
		"listersNewNamespaced":   c.Universe.Function(types.Name{Package: "k8s.io/client-go/listers", Name: "NewNamespaced"}),
		"cacheIndexer":           c.Universe.Type(types.Name{Package: "k8s.io/client-go/tools/cache", Name: "Indexer"}),
		"type":                   t,
		"objectMeta":             g.objectMeta,
	}

	tags, err := util.ParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
	if err != nil {
		return err
	}

	// 非命名空间资源没有 NamespaceLister，提前返回以免生成不可用的方法。
	if tags.NonNamespaced {
		sw.Do(typeListerInterfaceNonNamespaced, m)
	} else {
		sw.Do(typeListerInterface, m)
	}

	sw.Do(typeListerStruct, m)
	sw.Do(typeListerConstructor, m)

	if tags.NonNamespaced {
		return sw.Error()
	}

	sw.Do(typeListerNamespaceLister, m)
	sw.Do(namespaceListerInterface, m)
	sw.Do(namespaceListerStruct, m)

	return sw.Error()
}

// typeListerInterface 是命名空间资源的顶层 lister：List 全部 + 按 namespace 下钻。
var typeListerInterface = `
// $.type|public$Lister helps list $.type|publicPlural$.
// All objects returned here must be treated as read-only.
type $.type|public$Lister interface {
	// List lists all $.type|publicPlural$ in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector $.labelsSelector|raw$) (ret []*$.type|raw$, err error)
	// $.type|publicPlural$ returns an object that can list and get $.type|publicPlural$.
	$.type|publicPlural$(namespace string) $.type|public$NamespaceLister
	$.type|public$ListerExpansion
}
`

// typeListerInterfaceNonNamespaced 用于 cluster-scoped 资源：没有 namespace 参数，
// 直接按 name Get。
var typeListerInterfaceNonNamespaced = `
// $.type|public$Lister helps list $.type|publicPlural$.
// All objects returned here must be treated as read-only.
type $.type|public$Lister interface {
	// List lists all $.type|publicPlural$ in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector $.labelsSelector|raw$) (ret []*$.type|raw$, err error)
	// Get retrieves the $.type|public$ from the index for a given name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*$.type|raw$, error)
	$.type|public$ListerExpansion
}
`

// 这里嵌入带类型信息的资源索引器而非取别名，使生成的结构体
// 能作为 expansion 接口中专用方法的接收者。
var typeListerStruct = `
// $.type|private$Lister implements the $.type|public$Lister interface.
type $.type|private$Lister struct {
	$.listersResourceIndexer|raw$[*$.type|raw$]
}
`

// typeListerConstructor 以资源名建立带类型的索引，资源名取自 <Type>Resource()，
// 保证与 informer 使用的索引键一致。
var typeListerConstructor = `
// New$.type|public$Lister returns a new $.type|public$Lister.
func New$.type|public$Lister(indexer $.cacheIndexer|raw$) $.type|public$Lister {
	return &$.type|private$Lister{$.listersNew|raw$[*$.type|raw$](indexer, $.Resource|raw$("$.type|lowercaseSingular$"))}
}
`

// typeListerNamespaceLister 是由顶层 lister 返回的 per-namespace 视图。
var typeListerNamespaceLister = `
// $.type|publicPlural$ returns an object that can list and get $.type|publicPlural$.
func (s *$.type|private$Lister) $.type|publicPlural$(namespace string) $.type|public$NamespaceLister {
	return $.type|private$NamespaceLister{$.listersNewNamespaced|raw$[*$.type|raw$](s.ResourceIndexer, namespace)}
}
`

// namespaceListerInterface 仅暴露某个 namespace 内的 List/Get。
var namespaceListerInterface = `
// $.type|public$NamespaceLister helps list and get $.type|publicPlural$.
// All objects returned here must be treated as read-only.
type $.type|public$NamespaceLister interface {
	// List lists all $.type|publicPlural$ in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector $.labelsSelector|raw$) (ret []*$.type|raw$, err error)
	// Get retrieves the $.type|public$ from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*$.type|raw$, error)
	$.type|public$NamespaceListerExpansion
}
`

// 这里嵌入带类型信息的 namespaced 资源索引器而非取别名，使生成的结构体
// 能作为 expansion 接口中专用方法的接收者。
var namespaceListerStruct = `
// $.type|private$NamespaceLister implements the $.type|public$NamespaceLister
// interface.
type $.type|private$NamespaceLister struct {
	$.listersResourceIndexer|raw$[*$.type|raw$]
}
`
