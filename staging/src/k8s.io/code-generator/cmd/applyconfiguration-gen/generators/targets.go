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
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"

	"k8s.io/code-generator/cmd/applyconfiguration-gen/args"
	"k8s.io/code-generator/cmd/client-gen/generators/util"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/pkg/apidefinitions"
	genutil "k8s.io/code-generator/pkg/util"
)

const (
	// ApplyConfigurationTypeSuffix 是生成的 apply configuration 类型名后缀，
	// 如 FooApplyConfiguration。生成与查找（ForKind/Extract）都依赖该后缀，
	// 因此不能随意改动。
	ApplyConfigurationTypeSuffix = "ApplyConfiguration"
)

// NameSystems 返回本包生成器使用的命名系统。
// public/private 用于 Go 标识符，raw 用于生成带 import 限定的类型引用。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public":  namer.NewPublicNamer(0),
		"private": namer.NewPrivateNamer(0),
		"raw":     namer.NewRawNamer("", nil),
	}
}

// DefaultNameSystem 指定遍历类型时使用 public 命名系统。
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 为 applyconfiguration-gen 构建所有输出目标：
// 每个 API 包一个 apply configuration 包、一个 utils 包（ForKind 等）
// 以及一个 internal 包（内嵌 OpenAPI schema）。输出目录布局与 client-gen
// 保持一致，以便二者共存于同一目录树。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, "", gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	// lint 规则只在显式指定时启用，避免默认行为突变。
	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	// 先确定哪些输入包需要生成，再计算可达类型图；外部 apply configuration
	// 作为额外根节点参与可达性计算，保证跨包引用能解析到正确目标。
	pkgTypes := packageTypesForInputs(context, args.OutputPkg, idOpts)
	initialTypes := args.ExternalApplyConfigurations
	refs := refGraphForReachableTypes(context.Universe, pkgTypes, initialTypes)
	typeModels, err := newTypeModels(args.OpenAPISchemaFilePath, pkgTypes)
	if err != nil {
		klog.Fatalf("Failed build type models from typeModels %s: %v", args.OpenAPISchemaFilePath, err)
	}

	groupVersions := make(map[string]clientgentypes.GroupVersions)
	groupGoNames := make(map[string]string)
	applyConfigsForGroupVersion := make(map[clientgentypes.GroupVersion][]applyConfig)

	var targetList []generator.Target
	for pkg, p := range pkgTypes {
		gv, err := groupVersion(p)
		if err != nil {
			klog.Fatalf("Failed to parse comments of package %s: %v", p.Name, err)
		}

		var toGenerate []applyConfig
		for _, t := range p.Types {
			// If we don't have an ObjectMeta field, we lack the information required to make the Apply or ApplyStatus call
			// to the kube-apiserver, so we don't need to generate the type at all
			clientTags := genclientTags(t)
			if clientTags.GenerateClient && !hasObjectMetaField(t) {
				klog.V(5).Infof("skipping type %v because does not have ObjectMeta", t)
				continue
			}
			gvk := gv.WithKind(clientgentypes.Kind(t.Name.Name))
			openAPIName := typeModels.gvkToOpenAPIType[gvk]

			// 只为可达类型生成：不可达类型既不会被 root 引用，也不会被
			// 外部 apply configuration 引用。
			if typePkg, ok := refs[t.Name]; ok {
				toGenerate = append(toGenerate, applyConfig{
					Type:               t,
					ApplyConfiguration: types.Ref(typePkg, t.Name.Name+ApplyConfigurationTypeSuffix),
					OpenAPIName:        openAPIName,
				})
			}
		}
		if len(toGenerate) == 0 {
			continue // Don't generate empty packages
		}
		// 排序保证生成结果稳定，避免 map 迭代顺序导致 diff 抖动。
		sort.Sort(applyConfigSort(toGenerate))

		// Apparently we allow the groupName to be overridden in a way that it
		// no longer maps to a Go package by name.  So we have to figure out
		// the offset of this particular output package (pkg) from the base
		// output package (args.OutputPkg).
		pkgSubdir := strings.TrimPrefix(pkg, args.OutputPkg+"/")

		// generate the apply configurations
		targetList = append(targetList,
			targetForApplyConfigurationsPackage(
				args.OutputDir, args.OutputPkg, pkgSubdir,
				boilerplate, gv, toGenerate, refs, typeModels))

		// group all the generated apply configurations by gv so ForKind() can be generated
		groupPackageName := gv.Group.NonEmpty()
		groupVersionsEntry, ok := groupVersions[groupPackageName]
		if !ok {
			groupVersionsEntry = clientgentypes.GroupVersions{
				PackageName: groupPackageName,
				Group:       gv.Group,
			}
		}
		groupVersionsEntry.Versions = append(groupVersionsEntry.Versions, clientgentypes.PackageVersion{
			Version: gv.Version,
			Package: path.Clean(p.Path),
		})

		groupGoNames[groupPackageName], err = goName(gv, p)
		if err != nil {
			klog.Fatalf("Failed to parse comments of group package %s: %v", groupPackageName, err)
		}
		applyConfigsForGroupVersion[gv] = toGenerate
		groupVersions[groupPackageName] = groupVersionsEntry
	}

	// generate ForKind() utility function
	targetList = append(targetList,
		targetForUtils(args.OutputDir, args.OutputPkg,
			boilerplate, groupVersions, applyConfigsForGroupVersion, groupGoNames, typeModels))
	// generate internal embedded schema, required for generated Extract functions
	targetList = append(targetList,
		targetForInternal(args.OutputDir, args.OutputPkg,
			boilerplate, typeModels))

	return targetList
}

// typeName 返回便于错误信息展示的 "package.Type" 形式名称。
func typeName(t *types.Type) string {
	typePackage := t.Name.Package
	return fmt.Sprintf("%s.%s", typePackage, t.Name.Name)
}

// targetForApplyConfigurationsPackage 为一个 API 版本包创建目标。
// 每个源类型对应一个输出文件（文件名取类型名小写），保持与上游
// generated apply configuration 布局一致。
func targetForApplyConfigurationsPackage(outputDirBase, outputPkgBase, pkgSubdir string, boilerplate []byte, gv clientgentypes.GroupVersion, typesToGenerate []applyConfig, refs refGraph, models *typeModels) generator.Target {
	outputDir := filepath.Join(outputDirBase, pkgSubdir)
	outputPkg := path.Join(outputPkgBase, pkgSubdir)

	return &generator.SimpleTarget{
		PkgName:       gv.Version.PackageName(),
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			for _, toGenerate := range typesToGenerate {
				var openAPIType *string
				gvk := gv.WithKind(clientgentypes.Kind(toGenerate.Type.Name.Name))
				// OpenAPI 名称是运行期 Extract 的依据，缺失时保持 nil，
				// 由生成器决定回退行为。
				if v, ok := models.gvkToOpenAPIType[gvk]; ok {
					openAPIType = &v
				}

				generators = append(generators, &applyConfigurationGenerator{
					GoGenerator: generator.GoGenerator{
						OutputFilename: strings.ToLower(toGenerate.Type.Name.Name) + ".go",
					},
					outPkgBase:   outputPkgBase,
					localPkg:     outputPkg,
					groupVersion: gv,
					applyConfig:  toGenerate,
					imports:      generator.NewImportTrackerForPackage(outputPkg),
					refGraph:     refs,
					openAPIType:  openAPIType,
				})
			}
			return generators
		},
	}
}

// targetForUtils 创建输出 utils.go 的目标：为所有版本生成 ForKind 与
// NewTypeConverter，使调用方可按 GVK 构造对应的 apply configuration。
func targetForUtils(outputDirBase, outputPkgBase string, boilerplate []byte, groupVersions map[string]clientgentypes.GroupVersions,
	applyConfigsForGroupVersion map[clientgentypes.GroupVersion][]applyConfig, groupGoNames map[string]string, models *typeModels) generator.Target {
	return &generator.SimpleTarget{
		PkgName:       path.Base(outputPkgBase),
		PkgPath:       outputPkgBase,
		PkgDir:        outputDirBase,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &utilGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "utils.go",
				},
				outputPackage:        outputPkgBase,
				imports:              generator.NewImportTrackerForPackage(outputPkgBase),
				groupVersions:        groupVersions,
				typesForGroupVersion: applyConfigsForGroupVersion,
				groupGoNames:         groupGoNames,
				typeModels:           models,
			})
			return generators
		},
	}
}

// targetForInternal 创建 internal 子包目标，用于承载内嵌的 OpenAPI schema。
// 单独成包是为了让 utils 包可以引用 schema 解析器而不产生循环依赖。
func targetForInternal(outputDirBase, outputPkgBase string, boilerplate []byte, models *typeModels) generator.Target {
	outputDir := filepath.Join(outputDirBase, "internal")
	outputPkg := path.Join(outputPkgBase, "internal")
	return &generator.SimpleTarget{
		PkgName:       path.Base(outputPkg),
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = append(generators, &internalGenerator{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "internal.go",
				},
				outputPackage: outputPkgBase,
				imports:       generator.NewImportTrackerForPackage(outputPkg),
				typeModels:    models,
			})
			return generators
		},
	}
}

// goName 计算群组的 Go 包前缀，默认取 group 首段并首字母大写；
// 注释标签 groupGoName 可覆盖，用于 group 名称不合 Go 命名习惯的场景。
func goName(gv clientgentypes.GroupVersion, p *types.Package) (string, error) {
	goName := namer.IC(strings.Split(gv.Group.NonEmpty(), ".")[0])
	override, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{"groupGoName"}, p.Comments)

	if err != nil {
		return goName, err
	}
	if values, ok := override["groupGoName"]; ok {
		goName = namer.IC(values[0])
	}

	return goName, nil
}

// packageTypesForInputs 过滤输入包并计算其在输出树中的位置。
// 内部（internal）版本包没有 json tag，无法用于 apply configuration，直接跳过。
func packageTypesForInputs(context *generator.Context, outPkgBase string, idOpts []apidefinitions.Option) map[string]*types.Package {
	pkgTypes := map[string]*types.Package{}
	for _, inputDir := range context.Inputs {
		p := context.Universe.Package(inputDir)
		info, err := apidefinitions.Identify(p, apidefinitions.ApplyConfiguration, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			continue
		}
		internal := isInternalPackage(p)
		if internal {
			klog.Warningf("Skipping internal package: %s", p.Path)
			continue
		}
		// This is how the client generator finds the package we are creating. It uses the API package name, not the group name.
		// This matches the approach of the client-gen, so the two generator can work together.
		// For example, if openshift/api/cloudnetwork/v1 contains an apigroup cloud.network.openshift.io, the client-gen
		// builds a package called cloudnetwork/v1 to contain it. This change makes the applyconfiguration-gen use the same.
		_, gvPackageString := util.ParsePathGroupVersion(p.Path)
		pkg := path.Join(outPkgBase, strings.ToLower(gvPackageString))
		pkgTypes[pkg] = p
	}
	return pkgTypes
}

// groupVersion 从包路径末两段推导 group/version，并允许 +groupName 注释覆盖
// group。版本段保持原样（如 v1、v1beta1），参与 GVK 与包目录命名。
func groupVersion(p *types.Package) (gv clientgentypes.GroupVersion, err error) {
	parts := strings.Split(p.Path, "/")
	gv.Group = clientgentypes.Group(parts[len(parts)-2])
	gv.Version = clientgentypes.Version(parts[len(parts)-1])

	// If there's a comment of the form "// +groupName=somegroup" or
	// "// +groupName=somegroup.foo.bar.io", use the first field (somegroup) as the name of the
	// group when generating.
	override, ok, err := apidefinitions.GroupNameForPackage(p.Comments)
	if err != nil {
		return gv, err
	}
	if ok {
		gv.Group = clientgentypes.Group(override)
	}

	return gv, nil
}

// isInternalPackage 通过 ObjectMeta 成员是否带 json tag 判断 internal 包。
// 内部类型没有 json tag，不能直接映射为 apply configuration。
func isInternalPackage(p *types.Package) bool {
	for _, t := range p.Types {
		for _, member := range t.Members {
			if member.Name == "ObjectMeta" {
				return isInternal(member)
			}
		}
	}
	return false
}

// isInternal 判断成员是否缺少 json tag；无 json tag 即视为 internal 类型。
func isInternal(m types.Member) bool {
	_, ok := lookupJSONTags(m)
	return !ok
}

// hasObjectMetaField 判断类型是否内嵌 ObjectMeta。apply configuration 依赖
// ObjectMeta 提供 name/namespace 等信息来构造 Apply 请求。
func hasObjectMetaField(t *types.Type) bool {
	for _, member := range t.Members {
		if objectMeta.Name == member.Type.Name && member.Embedded {
			return true
		}
	}
	return false
}
