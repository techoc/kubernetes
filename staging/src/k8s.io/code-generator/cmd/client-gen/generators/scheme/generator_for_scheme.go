/*
Copyright 2017 The Kubernetes Authors.

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

// Package scheme 生成 clientset 的 scheme 包：注册各 GroupVersion 的类型，
// 并导出 scheme、codecs（以及公开 scheme 的 parameter codec）。
package scheme

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// // GenScheme 为 clientset 生成包含 scheme、codecs 与 parameter codec 的包。
//
// 该生成器只运行一次（见 Filter），为整个 clientset 输出单个 scheme 包。
// PrivateScheme 用于 internal clientset：全局变量改为小写，不导出 ParameterCodec；
// CreateRegistry 用于需要 Install(registry) 而非 AddToScheme 的旧式注册路径。
type GenScheme struct {
	generator.GoGenerator
	OutputPkg       string // 必须是 Go import path
	OutputPath      string // 可选
	Groups          []clientgentypes.GroupVersions
	GroupGoNames    map[clientgentypes.GroupVersion]string
	InputPackages   map[clientgentypes.GroupVersion]string
	ImportTracker   namer.ImportTracker
	PrivateScheme   bool
	CreateRegistry  bool
	schemeGenerated bool
}

// Namers 只注册 raw namer，所有类型以完整限定名输出。
func (g *GenScheme) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.OutputPkg, g.ImportTracker),
	}
}

// // Filter 保证 scheme 包只生成一次，与类型数量无关。
func (g *GenScheme) Filter(c *generator.Context, t *types.Type) bool {
	ret := !g.schemeGenerated
	g.schemeGenerated = true
	return ret
}

// Imports 按注册方式选择 import 目标：registry 模式导入每个版本的 install 包，
// 简单模式导入版本包自身；别名统一为小写的 group+version，避免与 Go 关键字冲突。
func (g *GenScheme) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.ImportTracker.ImportLines()...)
	for _, group := range g.Groups {
		for _, version := range group.Versions {
			packagePath := g.InputPackages[clientgentypes.GroupVersion{Group: group.Group, Version: version.Version}]
			groupAlias := strings.ToLower(g.GroupGoNames[clientgentypes.GroupVersion{Group: group.Group, Version: version.Version}])
			if g.CreateRegistry {
				// // 内部 clientset 走 install 包注册，而非带 register.go 的类型包；
				// 版本目录的 install 包负责把该版本类型注册到 scheme，因此上溯一层再拼 install。
				if version.Version != "" {
					packagePath = path.Dir(packagePath)
				}
				packagePath = path.Join(packagePath, "install")

				imports = append(imports, fmt.Sprintf("%s \"%s\"", groupAlias, packagePath))
				break
			} else {
				imports = append(imports, fmt.Sprintf("%s%s \"%s\"", groupAlias, strings.ToLower(version.Version.NonEmpty()), packagePath))
			}
		}
	}
	return
}

// GenerateType 输出 scheme/codecs 全局变量与注册逻辑。
func (g *GenScheme) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	// 分组信息预先计算成可直接 range 的结构，模板中不重复做类型转换。
	allGroupVersions := clientgentypes.ToGroupVersionInfo(g.Groups, g.GroupGoNames)
	allInstallGroups := clientgentypes.ToGroupInstallPackages(g.Groups, g.GroupGoNames)

	m := map[string]interface{}{
		"publicScheme":              !g.PrivateScheme,
		"allGroupVersions":          allGroupVersions,
		"allInstallGroups":          allInstallGroups,
		"customRegister":            false,
		"runtimeNewParameterCodec":  c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/runtime", Name: "NewParameterCodec"}),
		"runtimeNewScheme":          c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/runtime", Name: "NewScheme"}),
		"serializerNewCodecFactory": c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/runtime/serializer", Name: "NewCodecFactory"}),
		"runtimeScheme":             c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/runtime", Name: "Scheme"}),
		"runtimeSchemeBuilder":      c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/runtime", Name: "SchemeBuilder"}),
		"runtimeUtilMust":           c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/util/runtime", Name: "Must"}),
		"schemaGroupVersion":        c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/runtime/schema", Name: "GroupVersion"}),
		"metav1AddToGroupVersion":   c.Universe.Function(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "AddToGroupVersion"}),
	}
	// 私有 scheme 的变量首字母小写，避免 internal clientset 把可变全局暴露给用户。
	globals := map[string]string{
		"Scheme":         "Scheme",
		"Codecs":         "Codecs",
		"ParameterCodec": "ParameterCodec",
		"Registry":       "Registry",
	}
	for k, v := range globals {
		if g.PrivateScheme {
			m[k] = strings.ToLower(v[0:1]) + v[1:]
		} else {
			m[k] = v
		}
	}

	sw.Do(globalsTemplate, m)

	// 若调用方提供了 register_custom.go，则额外调用其 Install/ExtraAddToScheme，
	// 以支持手工注册无法自动推导的类型。
	if g.OutputPath != "" {
		if _, err := os.Stat(filepath.Join(g.OutputPath, strings.ToLower("register_custom.go"))); err == nil {
			m["customRegister"] = true
		}
	}

	if g.CreateRegistry {
		sw.Do(registryRegistration, m)
	} else {
		sw.Do(simpleRegistration, m)
	}

	return sw.Error()
}

// globalsTemplate 声明 scheme、codecs，以及（仅公开 scheme）parameter codec。
var globalsTemplate = `
var $.Scheme$ = $.runtimeNewScheme|raw$()
var $.Codecs$ = $.serializerNewCodecFactory|raw$($.Scheme$)
$if .publicScheme$var $.ParameterCodec$ = $.runtimeNewParameterCodec|raw$($.Scheme$)$end -$`

// registryRegistration 生成 registry 风格：init 中直接 Install(scheme)，
// 每个 install 包自行把类型加入 scheme。
var registryRegistration = `

func init() {
	$.metav1AddToGroupVersion|raw$($.Scheme$, $.schemaGroupVersion|raw${Version: "v1"})
	Install($.Scheme$)
}

// Install registers the API group and adds types to a scheme
func Install(scheme *$.runtimeScheme|raw$) {
	$- range .allInstallGroups$
	$.InstallPackageAlias$.Install(scheme)
	$- end$
	$if .customRegister$
	ExtraInstall(scheme)
	$end -$
}
`

// simpleRegistration 用 SchemeBuilder 汇总各版本的 AddToScheme，
// 导出 AddToScheme 以便其他 clientset 组合复用（见模板中的示例注释）。
var simpleRegistration = `
var localSchemeBuilder = $.runtimeSchemeBuilder|raw${
	$- range .allGroupVersions$
	$.PackageAlias$.AddToScheme,
	$- end$
	$if .customRegister$
	ExtraAddToScheme,
	$end -$
}

// AddToScheme adds all types of this clientset into the given scheme. This allows composition
// of clientsets, like in:
//
//   import (
//     "k8s.io/client-go/kubernetes"
//     clientsetscheme "k8s.io/client-go/kubernetes/scheme"
//     aggregatorclientsetscheme "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/scheme"
//   )
//
//   kclientset, _ := kubernetes.NewForConfig(c)
//   _ = aggregatorclientsetscheme.AddToScheme(clientsetscheme.Scheme)
//
// After this, RawExtensions in Kubernetes types will serialize kube-aggregator types
// correctly.
var AddToScheme = localSchemeBuilder.AddToScheme

func init() {
	$.metav1AddToGroupVersion|raw$($.Scheme$, $.schemaGroupVersion|raw${Version: "v1"})
	$.runtimeUtilMust|raw$(AddToScheme($.Scheme$))
}
`
