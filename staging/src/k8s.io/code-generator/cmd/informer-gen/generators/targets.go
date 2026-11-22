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
	"path"
	"path/filepath"
	"strings"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/cmd/informer-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	genutil "k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// NameSystems 返回本包生成器使用的命名系统。
// publicPlural/allLowercasePlural 决定生成文件名与共享 informer 名（如 deployments）；
// lowercaseSingular 用于 SharedInformerFactory 中按资源名生成访问器方法。
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

// lowercaseSingularNamer 实现 Namer，输出全小写的类型名（单数）。
type lowercaseSingularNamer struct{}

// Name 返回 t 的全小写名称。
func (n *lowercaseSingularNamer) Name(t *types.Type) string {
	return strings.ToLower(t.Name.Name)
}

// DefaultNameSystem 返回按类型排序处理时使用的命名系统。
func DefaultNameSystem() string {
	return "public"
}

// objectMetaForPackage 返回包 p 中使用的 ObjectMeta 类型。
// 若包内没有任何 genclient 类型，返回 nil；若存在 genclient 类型却找不到
// ObjectMeta 字段，则报错——这说明输入不符合 informer 生成的预期结构。
func objectMetaForPackage(p *types.Package) (*types.Type, bool, error) {
	generatingForPackage := false
	for _, t := range p.Types {
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

// isInternal 判断成员标签是否不含 json tag。
// 内部版本类型没有 json 标签，据此区分 internal 与 external 版本，
// 二者生成的 informer 输出目录与 client 包都不同。
func isInternal(m types.Member) bool {
	return !strings.Contains(m.Tags, "json")
}

const subdirForInternalInterfaces = "internalinterfaces"

// GetTargets 构造 informer 生成目标。
// 输出的目录布局（除 SingleDirectory 模式外）为：
//
//	<output>/internalversion/<group>/<version>/...  内部版本
//	<output>/externalversions/<group>/<version>/... 外部版本
//	<output>/{internal,external}versions/internalinterfaces/
//
// 外部与内部版本各自独立生成，互不混用 client 包。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, "", gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	internalVersionOutputDir := args.OutputDir
	internalVersionOutputPkg := args.OutputPkg
	externalVersionOutputDir := args.OutputDir
	externalVersionOutputPkg := args.OutputPkg
	// SingleDirectory 模式把所有内容写在同一个包里，供测试或特殊场景使用。
	if !args.SingleDirectory {
		internalVersionOutputDir = filepath.Join(internalVersionOutputDir, "internalversion")
		internalVersionOutputPkg = path.Join(internalVersionOutputPkg, "internalversion")
		externalVersionOutputDir = filepath.Join(externalVersionOutputDir, "externalversions")
		externalVersionOutputPkg = path.Join(externalVersionOutputPkg, "externalversions")
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	var targetList []generator.Target
	// typesForGroupVersion 供 generic.go 生成 SharedInformerFactory 的
	// ForResource 映射，因此必须收集所有版本的类型，而不只是当前包。
	typesForGroupVersion := make(map[clientgentypes.GroupVersion][]*types.Type)

	externalGroupVersions := make(map[string]clientgentypes.GroupVersions)
	internalGroupVersions := make(map[string]clientgentypes.GroupVersions)
	groupGoNames := make(map[string]string)
	for _, inputPkg := range context.Inputs {
		p := context.Universe.Package(inputPkg)

		info, err := apidefinitions.Identify(p, apidefinitions.Informer, idOpts...)
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
			// 该包中没有带 genclient 的类型。
			continue
		}

		var gv clientgentypes.GroupVersion
		var targetGroupVersions map[string]clientgentypes.GroupVersions

		// 内部版本的路径形如 .../<group>，外部版本形如 .../<group>/<version>，
		// 因此解析方式不同。
		if internal {
			lastSlash := strings.LastIndex(p.Path, "/")
			if lastSlash == -1 {
				klog.Fatalf("error constructing internal group version for package %q", p.Path)
			}
			gv.Group = clientgentypes.Group(p.Path[lastSlash+1:])
			targetGroupVersions = internalGroupVersions
		} else {
			parts := strings.Split(p.Path, "/")
			gv.Group = clientgentypes.Group(parts[len(parts)-2])
			gv.Version = clientgentypes.Version(parts[len(parts)-1])
			targetGroupVersions = externalGroupVersions
		}
		groupPackageName := gv.Group.NonEmpty()
		gvPackage := path.Clean(p.Path)

		// 若存在 "// +groupName=somegroup" 或
		// "// +groupName=somegroup.foo.bar.io" 注释，则用其第一段
		// （somegroup）作为生成时的 group 名。
		override, ok, err := apidefinitions.GroupNameForPackage(p.Comments)
		if err != nil {
			klog.Fatalf("error resolving group name: %v", err)
		}
		if ok {
			gv.Group = clientgentypes.Group(override)
		}

		// 若存在 "// +groupGoName=SomeUniqueShortName" 注释，用它作为
		// CamelCase 的 Go group 标识符；否则从 group 名推导。
		groupGoNames[groupPackageName] = namer.IC(strings.Split(gv.Group.NonEmpty(), ".")[0])
		goName, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{"groupGoName"}, p.Comments)
		if err != nil {
			klog.Fatalf("error extracting groupGoName tags: %v", err)
		}
		if goName["groupGoName"] != nil {
			groupGoNames[groupPackageName] = namer.IC(goName["groupGoName"][0])
		}

		var typesToGenerate []*types.Type
		for _, t := range p.Types {
			tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
			// informer 依赖 list/watch，缺少任一动词或显式 NoVerbs 的类型不能生成 informer。
			if !tags.GenerateClient || tags.NoVerbs || !tags.HasVerb("list") || !tags.HasVerb("watch") {
				continue
			}

			typesToGenerate = append(typesToGenerate, t)

			if _, ok := typesForGroupVersion[gv]; !ok {
				typesForGroupVersion[gv] = []*types.Type{}
			}
			typesForGroupVersion[gv] = append(typesForGroupVersion[gv], t)
		}
		if len(typesToGenerate) == 0 {
			continue
		}

		groupVersionsEntry, ok := targetGroupVersions[groupPackageName]
		if !ok {
			groupVersionsEntry = clientgentypes.GroupVersions{
				PackageName: groupPackageName,
				Group:       gv.Group,
			}
		}
		groupVersionsEntry.Versions = append(groupVersionsEntry.Versions, clientgentypes.PackageVersion{Version: gv.Version, Package: gvPackage})
		targetGroupVersions[groupPackageName] = groupVersionsEntry

		// 按私有名排序，使同一 group 内各版本的类型处理顺序确定。
		orderer := namer.Orderer{Namer: namer.NewPrivateNamer(0)}
		typesToGenerate = orderer.OrderTypes(typesToGenerate)

		if internal {
			targetList = append(targetList,
				versionTarget(
					internalVersionOutputDir, internalVersionOutputPkg,
					groupPackageName, gv, groupGoNames[groupPackageName],
					boilerplate, typesToGenerate,
					args.InternalClientSetPackage, args.ListersPackage))
		} else {
			targetList = append(targetList,
				versionTarget(
					externalVersionOutputDir, externalVersionOutputPkg,
					groupPackageName, gv, groupGoNames[groupPackageName],
					boilerplate, typesToGenerate,
					args.VersionedClientSetPackage, args.ListersPackage))
		}
	}

	// 工厂与 group 接口只有在存在对应版本时才生成，避免产出无意义的空包。
	if len(externalGroupVersions) != 0 {
		targetList = append(targetList,
			factoryInterfaceTarget(
				externalVersionOutputDir, externalVersionOutputPkg,
				boilerplate, args.VersionedClientSetPackage))
		targetList = append(targetList,
			factoryTarget(
				externalVersionOutputDir, externalVersionOutputPkg,
				boilerplate, groupGoNames, genutil.PluralExceptionListToMapOrDie(args.PluralExceptions),
				externalGroupVersions, args.VersionedClientSetPackage, typesForGroupVersion))
		for _, gvs := range externalGroupVersions {
			targetList = append(targetList,
				groupTarget(externalVersionOutputDir, externalVersionOutputPkg, gvs, boilerplate))
		}
	}

	if len(internalGroupVersions) != 0 {
		targetList = append(targetList,
			factoryInterfaceTarget(internalVersionOutputDir, internalVersionOutputPkg, boilerplate, args.InternalClientSetPackage))
		targetList = append(targetList,
			factoryTarget(
				internalVersionOutputDir, internalVersionOutputPkg,
				boilerplate, groupGoNames, genutil.PluralExceptionListToMapOrDie(args.PluralExceptions),
				internalGroupVersions, args.InternalClientSetPackage, typesForGroupVersion))
		for _, gvs := range internalGroupVersions {
			targetList = append(targetList,
				groupTarget(internalVersionOutputDir, internalVersionOutputPkg, gvs, boilerplate))
		}
	}

	return targetList
}

// factoryTarget 生成 SharedInformerFactory 的两个文件：
// factory.go（工厂结构体与版本访问器）和 generic.go（ForResource 等通用接口）。
func factoryTarget(outputDirBase, outputPkgBase string, boilerplate []byte, groupGoNames, pluralExceptions map[string]string, groupVersions map[string]clientgentypes.GroupVersions, clientSetPackage string,
	typesForGroupVersion map[clientgentypes.GroupVersion][]*types.Type) generator.Target {
	return &generator.SimpleTarget{
		PkgName:       path.Base(outputDirBase),
		PkgPath:       outputPkgBase,
		PkgDir:        outputDirBase,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &factoryGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "factory.go",
				},
				outputPackage:             outputPkgBase,
				imports:                   generator.NewImportTrackerForPackage(outputPkgBase),
				groupVersions:             groupVersions,
				clientSetPackage:          clientSetPackage,
				internalInterfacesPackage: path.Join(outputPkgBase, subdirForInternalInterfaces),
				gvGoNames:                 groupGoNames,
			})

			generators = append(generators, &genericGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "generic.go",
				},
				outputPackage:        outputPkgBase,
				imports:              generator.NewImportTrackerForPackage(outputPkgBase),
				groupVersions:        groupVersions,
				pluralExceptions:     pluralExceptions,
				typesForGroupVersion: typesForGroupVersion,
				groupGoNames:         groupGoNames,
			})

			return generators
		},
	}
}

// factoryInterfaceTarget 生成 internalinterfaces 包中的 SharedInformerFactory 接口，
// 供 factory.go 与 generic.go 引用而无需依赖具体版本包。
func factoryInterfaceTarget(outputDirBase, outputPkgBase string, boilerplate []byte, clientSetPackage string) generator.Target {
	outputDir := filepath.Join(outputDirBase, subdirForInternalInterfaces)
	outputPkg := path.Join(outputPkgBase, subdirForInternalInterfaces)

	return &generator.SimpleTarget{
		PkgName:       path.Base(outputDir),
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &factoryInterfaceGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "factory_interfaces.go",
				},
				outputPackage:    outputPkg,
				imports:          generator.NewImportTrackerForPackage(outputPkg),
				clientSetPackage: clientSetPackage,
			})

			return generators
		},
	}
}

// groupTarget 生成某个 group 的 interface.go，提供按版本获取 informer 的访问器。
func groupTarget(outputDirBase, outputPackageBase string, groupVersions clientgentypes.GroupVersions, boilerplate []byte) generator.Target {
	outputDir := filepath.Join(outputDirBase, groupVersions.PackageName)
	outputPkg := path.Join(outputPackageBase, groupVersions.PackageName)
	// group 名可能带域名后缀（如 apps.example.com），包名只取第一段。
	groupPkgName := strings.Split(string(groupVersions.PackageName), ".")[0]

	return &generator.SimpleTarget{
		PkgName:       groupPkgName,
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &groupInterfaceGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "interface.go",
				},
				outputPackage:             outputPkg,
				groupVersions:             groupVersions,
				imports:                   generator.NewImportTrackerForPackage(outputPkg),
				internalInterfacesPackage: path.Join(outputPackageBase, subdirForInternalInterfaces),
			})
			return generators
		},
		FilterFunc: func(c *generator.Context, t *types.Type) bool {
			tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
			return tags.GenerateClient && tags.HasVerb("list") && tags.HasVerb("watch")
		},
	}
}

// versionTarget 生成某个 group/version 的 informer：interface.go 提供类型化
// informer 访问器，并为每个类型生成一个独立文件（文件名取小写类型名）。
func versionTarget(outputDirBase, outputPkgBase string, groupPkgName string, gv clientgentypes.GroupVersion, groupGoName string, boilerplate []byte, typesToGenerate []*types.Type, clientSetPackage, listersPackage string) generator.Target {
	// 目录与包名统一使用小写版本号，与 client-go 的既有目录结构保持一致。
	subdir := []string{groupPkgName, strings.ToLower(gv.Version.NonEmpty())}
	outputDir := filepath.Join(outputDirBase, filepath.Join(subdir...))
	outputPkg := path.Join(outputPkgBase, path.Join(subdir...))

	return &generator.SimpleTarget{
		PkgName:       strings.ToLower(gv.Version.NonEmpty()),
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &versionInterfaceGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "interface.go",
				},
				outputPackage:             outputPkg,
				imports:                   generator.NewImportTrackerForPackage(outputPkg),
				types:                     typesToGenerate,
				internalInterfacesPackage: path.Join(outputPkgBase, subdirForInternalInterfaces),
			})

			for _, t := range typesToGenerate {
				generators = append(generators, &informerGenerator{
					GoGenerator: generator.GoGenerator{
						OutputFilename: strings.ToLower(t.Name.Name) + ".go",
					},
					outputPackage:             outputPkg,
					groupPkgName:              groupPkgName,
					groupVersion:              gv,
					groupGoName:               groupGoName,
					typeToGenerate:            t,
					imports:                   generator.NewImportTrackerForPackage(outputPkg),
					clientSetPackage:          clientSetPackage,
					listersPackage:            listersPackage,
					internalInterfacesPackage: path.Join(outputPkgBase, subdirForInternalInterfaces),
				})
			}
			return generators
		},
		FilterFunc: func(c *generator.Context, t *types.Type) bool {
			tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
			return tags.GenerateClient && tags.HasVerb("list") && tags.HasVerb("watch")
		},
	}
}
