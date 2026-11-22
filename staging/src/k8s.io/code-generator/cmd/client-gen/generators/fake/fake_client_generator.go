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
	"path"
	"path/filepath"
	"strings"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"

	"k8s.io/code-generator/cmd/client-gen/args"
	scheme "k8s.io/code-generator/cmd/client-gen/generators/scheme"
	"k8s.io/code-generator/cmd/client-gen/generators/util"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
)

// TargetForGroup 返回某个 group/version 的 fake typed client 生成目标。
// 输出目录必须与真实 typed client 的目录结构镜像（typed/<group>/<version>/fake），
// 否则 fake 包无法以相同导入路径替换真实实现。
func TargetForGroup(gv clientgentypes.GroupVersion, typeList []*types.Type, clientsetDir, clientsetPkg string, groupPkgName string, groupGoName string, inputPkg string, applyBuilderPackage string, boilerplate []byte) generator.Target {
	// TODO: should make this a function, called by here and in client-generator.go
	subdir := []string{"typed", strings.ToLower(groupPkgName), strings.ToLower(gv.Version.NonEmpty())}
	outputDir := filepath.Join(clientsetDir, filepath.Join(subdir...), "fake")
	outputPkg := path.Join(clientsetPkg, path.Join(subdir...), "fake")
	realClientPkg := path.Join(clientsetPkg, path.Join(subdir...))

	return &generator.SimpleTarget{
		PkgName:       "fake",
		PkgPath:       outputPkg,
		PkgDir:        outputDir,
		HeaderComment: boilerplate,
		PkgDocComment: []byte("// Package fake has the automatically generated clients.\n"),
		// GeneratorsFunc returns a list of generators. Each generator makes a
		// single file.
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = []generator.Generator{
				// Always generate a "doc.go" file.
				generator.GoGenerator{OutputFilename: "doc.go"},
			}
			// Since we want a file per type that we generate a client for, we
			// have to provide a function for this.
			// 每个类型一个文件，文件名取 private 命名（首字母小写），便于按类型定位。
			for _, t := range typeList {
				generators = append(generators, &genFakeForType{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "fake_" + strings.ToLower(c.Namers["private"].Name(t)) + ".go",
					},
					outputPackage:             outputPkg,
					realClientPackage:         realClientPkg,
					inputPackage:              inputPkg,
					version:                   gv.Version.String(),
					groupGoName:               groupGoName,
					typeToMatch:               t,
					imports:                   generator.NewImportTrackerForPackage(outputPkg),
					applyConfigurationPackage: applyBuilderPackage,
				})
			}

			// 组级 fake client 汇总该组下所有类型的 fake 实现。
			generators = append(generators, &genFakeForGroup{
				GoGenerator: generator.GoGenerator{
					OutputFilename: "fake_" + groupPkgName + "_client.go",
				},
				outputPackage:     outputPkg,
				realClientPackage: realClientPkg,
				version:           gv.Version.String(),
				groupGoName:       groupGoName,
				types:             typeList,
				imports:           generator.NewImportTrackerForPackage(outputPkg),
			})
			return generators
		},
		FilterFunc: func(c *generator.Context, t *types.Type) bool {
			return util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...)).GenerateClient
		},
	}
}

// TargetForClientset 返回 fake clientset 聚合包的生成目标。
// 与真实 clientset 不同，fake 包只需要 clientset 与 register 两个文件：
// 类型级 fake 已由 TargetForGroup 生成在各自的子包中。
func TargetForClientset(args *args.Args, clientsetDir, clientsetPkg string, applyConfigurationPkg string, groupGoNames map[clientgentypes.GroupVersion]string, boilerplate []byte) generator.Target {
	return &generator.SimpleTarget{
		// TODO: we'll generate fake clientset for different release in the future.
		// Package name and path are hard coded for now.
		PkgName:       "fake",
		PkgPath:       path.Join(clientsetPkg, "fake"),
		PkgDir:        filepath.Join(clientsetDir, "fake"),
		HeaderComment: boilerplate,
		PkgDocComment: []byte("// This package has the automatically generated fake clientset.\n"),
		// GeneratorsFunc returns a list of generators. Each generator generates a
		// single file.
		GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
			generators = []generator.Generator{
				// Always generate a "doc.go" file.
				generator.GoGenerator{OutputFilename: "doc.go"},

				&genClientset{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "clientset_generated.go",
					},
					groups:                    args.Groups,
					groupGoNames:              groupGoNames,
					fakeClientsetPackage:      clientsetPkg,
					imports:                   generator.NewImportTrackerForPackage(clientsetPkg),
					realClientsetPackage:      clientsetPkg,
					applyConfigurationPackage: applyConfigurationPkg,
				},
				&scheme.GenScheme{
					GoGenerator: generator.GoGenerator{
						OutputFilename: "register.go",
					},
					InputPackages: args.GroupVersionPackages(),
					OutputPkg:     clientsetPkg,
					Groups:        args.Groups,
					GroupGoNames:  groupGoNames,
					ImportTracker: generator.NewImportTrackerForPackage(clientsetPkg),
					PrivateScheme: true,
				},
			}
			return generators
		},
	}
}
