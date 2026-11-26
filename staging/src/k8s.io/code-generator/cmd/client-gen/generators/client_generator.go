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

// Package generators 实现 client-gen 的各个代码生成器。
package generators

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"k8s.io/code-generator/cmd/client-gen/args"
	"k8s.io/code-generator/cmd/client-gen/generators/fake"
	"k8s.io/code-generator/cmd/client-gen/generators/scheme"
	"k8s.io/code-generator/cmd/client-gen/generators/util"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/pkg/apidefinitions"
	codegennamer "k8s.io/code-generator/pkg/namer"
	genutil "k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"

	"k8s.io/klog/v2"
)

// NameSystems 返回本包生成器使用的命名系统。
//
// 命名系统决定了同一 Go 类型在不同上下文中的拼写（导出名、私有名、复数名等）。
// 生成的代码依赖这些名字互相引用，因此这里必须与 gengo 的默认行为保持一致，
// 只有确实需要去重（例如资源名与 group 名冲突）时才通过 ExceptionNamer 覆盖。
func NameSystems(pluralExceptions map[string]string) namer.NameSystems {
	lowercaseNamer := namer.NewAllLowercasePluralNamer(pluralExceptions)

	publicNamer := &ExceptionNamer{
		Exceptions: map[string]string{
			// 这里列出避免生成代码命名冲突的例外。
			// 键用全限定包名，值为不与 group 名冲突的替代名。
			// 例如 "k8s.io/apis/events/v1beta1.Event": "EventResource"
		},
		KeyFunc: func(t *types.Type) string {
			return t.Name.Package + "." + t.Name.Name
		},
		Delegate: namer.NewPublicNamer(0),
	}
	privateNamer := &ExceptionNamer{
		Exceptions: map[string]string{
			// 这里列出避免生成代码命名冲突的例外。
			// 键用全限定包名，值为不与 group 名冲突的替代名。
			// 例如 "k8s.io/apis/events/v1beta1.Event": "eventResource"
		},
		KeyFunc: func(t *types.Type) string {
			return t.Name.Package + "." + t.Name.Name
		},
		Delegate: namer.NewPrivateNamer(0),
	}
	publicPluralNamer := &ExceptionNamer{
		Exceptions: map[string]string{
			// 这里列出避免生成代码命名冲突的例外。
			// 键用全限定包名，值为不与 group 名冲突的替代名。
			// 例如 "k8s.io/apis/events/v1beta1.Event": "EventResource"
		},
		KeyFunc: func(t *types.Type) string {
			return t.Name.Package + "." + t.Name.Name
		},
		Delegate: namer.NewPublicPluralNamer(pluralExceptions),
	}
	privatePluralNamer := &ExceptionNamer{
		Exceptions: map[string]string{
			// 键用全限定包名，值为不与 group 名冲突的替代名。
			// 例如 "k8s.io/apis/events/v1beta1.Event": "eventResource"。
			// 这些例外用于避免生成代码中的命名冲突。
			"k8s.io/apis/events/v1beta1.Event":        "eventResources",
			"k8s.io/kubernetes/pkg/apis/events.Event": "eventResources",
		},
		KeyFunc: func(t *types.Type) string {
			return t.Name.Package + "." + t.Name.Name
		},
		Delegate: namer.NewPrivatePluralNamer(pluralExceptions),
	}

	return namer.NameSystems{
		// singularKind 用于单数 Kind 形式，主要供 scheme 注册使用。
		"singularKind":  namer.NewPublicNamer(0),
		"public":        publicNamer,
		"private":       privateNamer,
		"raw":           namer.NewRawNamer("", nil),
		"publicPlural":  publicPluralNamer,
		"privatePlural": privatePluralNamer,
		// allLowercasePlural 生成全小写复数，即 REST 资源名（如 "deployments"）。
		"allLowercasePlural": lowercaseNamer,
		// resource 允许通过 +resourceName 标签覆盖资源名，默认退化为全小写复数。
		"resource": codegennamer.NewTagOverrideNamer("resourceName", lowercaseNamer),
	}
}

// ExceptionNamer 允许为精确匹配的类型指定替代名，
// 用于处理 group 名与资源名相撞之类的命名冲突。
type ExceptionNamer struct {
	Exceptions map[string]string
	KeyFunc    func(*types.Type) string

	Delegate namer.Namer
}

// Name 返回类型对应的名字，命中例外表则直接返回替代名。
func (n *ExceptionNamer) Name(t *types.Type) string {
	key := n.KeyFunc(t)
	if exception, ok := n.Exceptions[key]; ok {
		return exception
	}
	return n.Delegate.Name(t)
}

// DefaultNameSystem 返回本包生成器排序类型时默认使用的命名系统。
func DefaultNameSystem() string {
	return "public"
}

// targetForGroup 为单个 group/version 生成一个 typed clientset 目标：
// 每个类型一个文件、一个 group client 文件，外加 generated_expansion.go。
//
// 文件按 private 名命名而非 Kind 名，是为了让生成结果与类型声明顺序无关，
// 同时避免大小写在不同文件系统上的差异。
func targetForGroup(gv clientgentypes.GroupVersion, typeList []*types.Type, clientsetDir, clientsetPkg string, groupPkgName string, groupGoName string, apiPath string, inputPkg string, applyBuilderPkg string, boilerplate []byte, prefersProtobuf bool) generator.Target {
	// 目录布局固定为 typed/<group>/<version>，其中 group 与 version 都小写。
	subdir := []string{"typed", strings.ToLower(groupPkgName), strings.ToLower(gv.Version.NonEmpty())}
	gvDir := filepath.Join(clientsetDir, filepath.Join(subdir...))
	gvPkg := path.Join(clientsetPkg, path.Join(subdir...))

	return &generator.SimpleTarget{
		PkgName:       strings.ToLower(gv.Version.NonEmpty()),
		PkgPath:       gvPkg,
		PkgDir:        gvDir,
		HeaderComment: boilerplate,
		PkgDocComment: []byte("// This package has the automatically generated typed clients.\n"),
		// GeneratorsFunc 返回生成器列表，每个生成器只负责产出一个文件。
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = []generator.Generator{
				// 每个目标包都生成 doc.go，保证包有稳定的文档入口。
				generator.GoGenerator{OutputFilename: "doc.go"},
			}
			// 每个类型单独生成一个文件，因此需要逐个类型创建生成器。
			for _, t := range typeList {
				generators = append(generators, &genClientForType{
					GoGenerator: generator.GoGenerator{
						OutputFilename: strings.ToLower(c.Namers["private"].Name(t)) + ".go",
					},
					outputPackage:             gvPkg,
					inputPackage:              inputPkg,
					clientsetPackage:          clientsetPkg,
					applyConfigurationPackage: applyBuilderPkg,
					group:                     gv.Group.NonEmpty(),
					version:                   gv.Version.String(),
					groupGoName:               groupGoName,
					prefersProtobuf:           prefersProtobuf,
					typeToMatch:               t,
					imports:                   generator.NewImportTrackerForPackage(gvPkg),
				})
			}

			generators = append(generators, &genGroup{
				GoGenerator: generator.GoGenerator{
					OutputFilename: groupPkgName + "_client.go",
				},
				outputPackage:    gvPkg,
				inputPackage:     inputPkg,
				clientsetPackage: clientsetPkg,
				group:            gv.Group.NonEmpty(),
				version:          gv.Version.String(),
				groupGoName:      groupGoName,
				apiPath:          apiPath,
				types:            typeList,
				imports:          generator.NewImportTrackerForPackage(gvPkg),
			})

			// expansion 文件用于手写扩展方法，文件名固定，生成器不会清空其内容。
			expansionFileName := "generated_expansion.go"
			generators = append(generators, &genExpansion{
				groupPackagePath: gvDir,
				GoGenerator: generator.GoGenerator{
					OutputFilename: expansionFileName,
				},
				types: typeList,
			})

			return generators
		},
		// 只处理带 +genclient 的类型，其余类型属于该包的普通 API 对象。
		FilterFunc: func(c *generator.Context, t *types.Type) bool {
			return util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...)).GenerateClient
		},
	}
}

// targetForClientset 生成顶层 clientset.go，即 Clientset 接口与实现，
// 以及按 group 划分的访问方法。
func targetForClientset(args *args.Args, clientsetDir, clientsetPkg string, groupGoNames map[clientgentypes.GroupVersion]string, boilerplate []byte) generator.Target {
	return &generator.SimpleTarget{
		PkgName:       args.ClientsetName,
		PkgPath:       clientsetPkg,
		PkgDir:        clientsetDir,
		HeaderComment: boilerplate,
		// GeneratorsFunc 返回生成器列表，每个生成器只负责产出一个文件。
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = []generator.Generator{
				&genClientset{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "clientset.go",
					},
					groups:           args.Groups,
					groupGoNames:     groupGoNames,
					clientsetPackage: clientsetPkg,
					imports:          generator.NewImportTrackerForPackage(clientsetPkg),
				},
			}
			return generators
		},
	}
}

// targetForScheme 生成 scheme/register.go，把每个 group-version 注册进运行时 scheme。
//
// 内部版本（version 为空串）的 client 无法通过公开的 versioned scheme 完成
// 编解码，因此需要额外创建 runtime.Registry，由 CreateRegistry 开关控制。
func targetForScheme(args *args.Args, clientsetDir, clientsetPkg string, groupGoNames map[clientgentypes.GroupVersion]string, boilerplate []byte) generator.Target {
	schemeDir := filepath.Join(clientsetDir, "scheme")
	schemePkg := path.Join(clientsetPkg, "scheme")

	// internal client 需要识别 group/version，因此额外创建 runtime.Registry。
	internalClient := false
NextGroup:
	for _, group := range args.Groups {
		for _, v := range group.Versions {
			if v.String() == "" {
				internalClient = true
				break NextGroup
			}
		}
	}

	return &generator.SimpleTarget{
		PkgName:       "scheme",
		PkgPath:       schemePkg,
		PkgDir:        schemeDir,
		HeaderComment: boilerplate,
		PkgDocComment: []byte("// This package contains the scheme of the automatically generated clientset.\n"),
		// GeneratorsFunc 返回生成器列表，每个生成器只负责产出一个文件。
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = []generator.Generator{
				// 每个目标包都生成 doc.go，保证包有稳定的文档入口。
				generator.GoGenerator{OutputFilename: "doc.go"},

				&scheme.GenScheme{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "register.go",
					},
					InputPackages:  args.GroupVersionPackages(),
					OutputPkg:      schemePkg,
					OutputPath:     schemeDir,
					Groups:         args.Groups,
					GroupGoNames:   groupGoNames,
					ImportTracker:  generator.NewImportTrackerForPackage(schemePkg),
					CreateRegistry: internalClient,
				},
			}
			return generators
		},
	}
}

// applyGroupOverrides 按包注释中的 +groupName 标签改写 group 名：
// 取 "// +groupName=somegroup.foo.bar.io" 的第一段 somegroup 作为 Go 代码中的 group 名，
// 例如 clientset 中的方法名。
//
// 先在 universe 中扫描出 old GV -> new GV 的映射，再据此改写 args.Groups，
// 保证后续所有生成器看到的 group 名与源码注释一致。
func applyGroupOverrides(universe types.Universe, args *args.Args) error {
	// 先建立旧 GV 到新 GV 的映射，再统一改写，避免边遍历边修改。
	changes := make(map[clientgentypes.GroupVersion]clientgentypes.GroupVersion)
	for gv, inputDir := range args.GroupVersionPackages() {
		p := universe.Package(inputDir)
		override, ok, err := apidefinitions.GroupNameForPackage(p.Comments)
		if err != nil {
			return err
		}
		if ok {
			newGV := clientgentypes.GroupVersion{
				Group:   clientgentypes.Group(override),
				Version: gv.Version,
			}
			changes[gv] = newGV
		}
	}

	// 按映射表重写 args.Groups。
	newGroups := make([]clientgentypes.GroupVersions, 0, len(args.Groups))
	for _, gvs := range args.Groups {
		gv := clientgentypes.GroupVersion{
			Group:   gvs.Group,
			Version: gvs.Versions[0].Version, // we only need a version, and the first will do
		}
		if newGV, ok := changes[gv]; ok {
			// 有覆盖规则，采用新 group 名。
			newGVS := clientgentypes.GroupVersions{
				PackageName: gvs.PackageName,
				Group:       newGV.Group,
				Versions:    gvs.Versions,
			}
			newGroups = append(newGroups, newGVS)
		} else {
			// 无覆盖规则，保持原值。
			newGroups = append(newGroups, gvs)
		}
	}
	args.Groups = newGroups
	return nil
}

// 输入路径由 input-base 与 group-version 参数拼成，可能是文件系统路径。
// 这里把它们统一改写成规范的 Go import-path。
//
// TODO: 像其他 gengo 工具一样直接接收 Go "patterns"，再从中提取 GV。
//
// 只有 universe 中尚未加载（Name 为空）的包才会触发 FindPackages；
// 已加载的包保持原样，避免重复解析。
func sanitizePackagePaths(context *generator.Context, args *args.Args) error {
	for i := range args.Groups {
		pkg := &args.Groups[i]
		for j := range pkg.Versions {
			ver := &pkg.Versions[j]
			input := ver.Package
			p := context.Universe[input]
			if p == nil || p.Name == "" {
				pkgs, err := context.FindPackages(input)
				if err != nil {
					return fmt.Errorf("can't find input package %q: %w", input, err)
				}
				p = context.Universe[pkgs[0]]
				if p == nil {
					return fmt.Errorf("can't find input package %q in universe", input)
				}
				ver.Package = p.Path
			}
		}
	}
	return nil
}

// GetTargets 汇总顶层 clientset、scheme、(fake) 与各 group/version 的生成目标。
//
// 生成顺序固定为：clientset -> scheme -> (fake) -> 各 group/version。
// 注意 ClientsetOnly 会提前返回，此时只重写聚合层，不动 typed client。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, "", gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	includedTypesOverrides := args.IncludedTypesOverrides

	if err := sanitizePackagePaths(context, args); err != nil {
		klog.Fatalf("cannot sanitize inputs: %v", err)
	}
	if err := applyGroupOverrides(context.Universe, args); err != nil {
		klog.Fatalf("cannot apply group overrides: %v", err)
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	// gvToTypes 收集每个 group-version 需要生成 client 的类型，
	// groupGoNames 记录其在 Go 代码中的 group 标识符（可能被 +groupGoName 覆盖）。
	gvToTypes := map[clientgentypes.GroupVersion][]*types.Type{}
	groupGoNames := make(map[clientgentypes.GroupVersion]string)
	for gv, inputDir := range args.GroupVersionPackages() {
		p := context.Universe.Package(inputDir)

		info, err := apidefinitions.Identify(p, apidefinitions.Client, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			continue
		}

		// 包注释中若有 "// +groupGoName=SomeUniqueShortName"，就用它作为 Go 代码中的
		// group 标识符（CamelCase）；默认取 group 首段。
		groupGoNames[gv] = namer.IC(strings.Split(gv.Group.NonEmpty(), ".")[0])
		override, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{"groupGoName"}, p.Comments)
		if err != nil {
			klog.Fatalf("cannot extract groupGoName tags: %v", err)
		}
		if override["groupGoName"] != nil {
			groupGoNames[gv] = namer.IC(override["groupGoName"][0])
		}

		for n, t := range p.Types {
			// 跳过未出现在用户指定覆盖列表中的类型。
			typesOverride, ok := includedTypesOverrides[gv]
			if ok {
				found := false
				for _, typeStr := range typesOverride {
					if typeStr == n {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			} else {
				// 该 group/version 没有覆盖列表，按默认规则只保留带 genclient 的类型。
				if tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...)); !tags.GenerateClient {
					continue
				}
			}
			if _, found := gvToTypes[gv]; !found {
				gvToTypes[gv] = []*types.Type{}
			}
			gvToTypes[gv] = append(gvToTypes[gv], t)
		}
	}

	clientsetDir := filepath.Join(args.OutputDir, args.ClientsetName)
	clientsetPkg := path.Join(args.OutputPkg, args.ClientsetName)

	var targetList []generator.Target

	targetList = append(targetList,
		targetForClientset(args, clientsetDir, clientsetPkg, groupGoNames, boilerplate))
	targetList = append(targetList,
		targetForScheme(args, clientsetDir, clientsetPkg, groupGoNames, boilerplate))
	if args.FakeClient {
		targetList = append(targetList,
			fake.TargetForClientset(args, clientsetDir, clientsetPkg, args.ApplyConfigurationPackage, groupGoNames, boilerplate))
	}

	// --clientset-only 为真时只生成聚合层，跳过各类型的 typed client。
	if args.ClientsetOnly {
		return []generator.Target(targetList)
	}

	// 按 private 名排序类型，保证同一 group-version 内的文件与接口方法顺序稳定。
	orderer := namer.Orderer{Namer: namer.NewPrivateNamer(0)}
	gvPackages := args.GroupVersionPackages()
	for _, group := range args.Groups {
		for _, version := range group.Versions {
			gv := clientgentypes.GroupVersion{Group: group.Group, Version: version.Version}
			types := gvToTypes[gv]
			inputPath := gvPackages[gv]
			targetList = append(targetList,
				targetForGroup(
					gv, orderer.OrderTypes(types), clientsetDir, clientsetPkg,
					group.PackageName, groupGoNames[gv], args.ClientsetAPIPath,
					inputPath, args.ApplyConfigurationPackage, boilerplate, args.PrefersProtobuf))
			if args.FakeClient {
				targetList = append(targetList,
					fake.TargetForGroup(gv, orderer.OrderTypes(types), clientsetDir, clientsetPkg, group.PackageName, groupGoNames[gv], inputPath, args.ApplyConfigurationPackage, boilerplate))
			}
		}
	}

	return targetList
}
