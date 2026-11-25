/*
Copyright 2018 The Kubernetes Authors.

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
	"os"
	"path"
	"strings"

	"k8s.io/klog/v2"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/cmd/register-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// NameSystems returns the name system used by the generators in this package.
func NameSystems() namer.NameSystems {
	return namer.NameSystems{}
}

// DefaultNameSystem returns the default name system for ordering the types to be
// processed by the generators in this package.
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 构造要生成的目标列表。对每个输入包做三重筛选：是否 opt-in、是否为 internal 包、是否已有手写 register.go；
// 任一条件不满足即跳过，避免覆盖人工维护的注册文件。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, gengo.StdBuildTag, gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	targetList := []generator.Target{}
	for _, input := range context.Inputs {
		pkg := context.Universe.Package(input)
		if !isRegisterGenTarget(pkg, idOpts) {
			continue
		}
		internal, err := isInternal(pkg)
		if err != nil {
			klog.V(5).Infof("skipping the generation of %s file, due to err %v", args.OutputFile, err)
			continue
		}
		// internal 类型没有 json tag，不能用于线缆序列化，因此不生成对外注册代码。
		if internal {
			klog.V(5).Infof("skipping the generation of %s file because %s package contains internal types, note that internal types don't have \"json\" tags", args.OutputFile, pkg.Name)
			continue
		}
		registerFileName := "register.go"
		searchPath := path.Join(pkg.Dir, registerFileName)
		// 已存在 register.go 说明该包使用手写注册，生成器必须让路。
		if _, err := os.Stat(path.Join(searchPath)); err == nil {
			klog.V(5).Infof("skipping the generation of %s file because %s already exists in the path %s", args.OutputFile, registerFileName, searchPath)
			continue
		} else if err != nil && !os.IsNotExist(err) {
			klog.Fatalf("an error %v has occurred while checking if %s exists", err, registerFileName)
		}

		gv := clientgentypes.GroupVersion{}
		{
			// 约定：包路径的倒数第二段是 group，最后一段是 version。
			pathParts := strings.Split(pkg.Path, "/")
			if len(pathParts) < 2 {
				klog.Errorf("the path of the package must contain the group name and the version, path = %s", pkg.Path)
				continue
			}
			gv.Group = clientgentypes.Group(pathParts[len(pathParts)-2])
			gv.Version = clientgentypes.Version(pathParts[len(pathParts)-1])

			// 若存在形如 `// +groupName=somegroup` 或 `// +groupName=somegroup.foo.bar.io` 的注释，
			// 则从中提取全限定的 API group 名，覆盖从包路径推断出的 group。
			// 目录名与真实 API group 并不总是一致（例如 example3.io），因此允许注释覆盖。
			override, ok, err := apidefinitions.GroupNameForPackage(pkg.Comments)
			if err != nil {
				klog.Fatalf("error resolving group name: %v", err)
			}
			if ok {
				klog.V(5).Infof("overriding the group name with = %s", override)
				gv.Group = clientgentypes.Group(override)
			}
		}

		typesToRegister := []*types.Type{}
		for _, t := range pkg.Types {
			klog.V(5).Infof("considering type = %s", t.Name.String())
			// 只有内嵌 TypeMeta 的顶层类型才是可注册的 API 对象。
			for _, typeMember := range t.Members {
				if typeMember.Name == "TypeMeta" && typeMember.Embedded {
					typesToRegister = append(typesToRegister, t)
				}
			}
		}

		targetList = append(targetList,
			&generator.SimpleTarget{
				PkgName:       pkg.Name,
				PkgPath:       pkg.Path, // output to same pkg as input
				PkgDir:        pkg.Dir,  // output to same pkg as input
				HeaderComment: boilerplate,
				GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
					return []generator.Generator{
						&registerExternalGenerator{
							GoGenerator: generator.GoGenerator{
								OutputFilename: args.OutputFile,
							},
							gv:              gv,
							typesToGenerate: typesToRegister,
							outputPackage:   pkg.Path,
							imports:         generator.NewImportTrackerForPackage(pkg.Path),
						},
					}
				},
			})
	}

	return targetList
}

// isRegisterGenTarget reports whether pkg has opted in to register-gen.
// Activation rules are encoded in apidefinitions.Register's Spec
// (Boolean ActivationTag with a +groupName= fallback).
func isRegisterGenTarget(pkg *types.Package, idOpts []apidefinitions.Option) bool {
	info, err := apidefinitions.Identify(pkg, apidefinitions.Register, idOpts...)
	if err != nil {
		klog.Fatal(err)
	}
	return info.ShouldGenerate()
}

// isInternal 判定给定包是否包含 internal 类型。
// 判定依据是 TypeMeta 成员是否带 json tag：internal 类型不加 tag。
func isInternal(p *types.Package) (bool, error) {
	for _, t := range p.Types {
		for _, member := range t.Members {
			if member.Name == "TypeMeta" {
				return !strings.Contains(member.Tags, "json"), nil
			}
		}
	}
	return false, fmt.Errorf("unable to find TypeMeta for any types in package %s", p.Path)
}
