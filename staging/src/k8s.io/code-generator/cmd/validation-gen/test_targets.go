/*
Copyright The Kubernetes Authors.

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

package main

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"sigs.k8s.io/yaml"
)

// 生成的测试夹具里引用到的包级符号；与 validation.go 共用的 fmtPkgSymbols 覆盖 Fprintln。
// 提前解析成符号集合，是为了在模板里通过 raw namer 输出时能正确登记导入。
var (
	schemaPkg               = "k8s.io/apimachinery/pkg/runtime/schema"
	schemaPkgSymbols        = mkPkgNames(schemaPkg, "GroupVersionKind")
	osPkgSymbols            = mkPkgNames("os", "Stderr", "Exit")
	testingSymbols          = mkPkgNames("testing", "M")
	runtimeTestPkg          = "k8s.io/apimachinery/pkg/test/coverage"
	runtimeRegisterSymbols  = mkPkgNames(runtimeTestPkg, "RegisterDeclaredRules", "FieldRules")
	runtimeAssertCovSymbols = mkPkgNames(runtimeTestPkg, "AssertDeclarativeCoverage")
)

// apiVersionRe 匹配 Kubernetes 风格的版本号：v1、v1alpha1、v2beta3。
// 用于判断输入包是否为版本化 API 包
// （辅助包或 main 包会被跳过）。
var apiVersionRe = regexp.MustCompile(`^v\d+(alpha\d+|beta\d+)?$`)

// rule 表示一条已声明的字段校验错误。
// ErrorType 对应 field.ErrorType；Origin 为空表示未标注来源。
type rule struct {
	ErrorType string
	Origin    string
}

// fieldRules 把字段路径映射到单个 Kind 声明的规则。
type fieldRules map[string][]rule

// report 保存一个 GVK 的声明规则，作为生成测试 fixture 时的
// 内存中间结构，不序列化。
type report struct {
	Group   string
	Version string
	Kind    string
	Rules   fieldRules
}

// allowlistEntry 从覆盖率 fixture 中过滤掉声明的规则。
// 所有字段必填：apiVersion 按字面匹配（形如
// "<group>/<version>"，core 组只有 "<version>"）；kind/path/
// errorType/origin 按字面匹配，或用 "*" 通配。空值在加载时
// 即被拒绝，避免条目悄悄过滤掉过多内容。
type allowlistEntry struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	ErrorType  string `json:"errorType"`
	Origin     string `json:"origin"`
	Reason     string `json:"reason"`
}

// groupVersion 把 e.APIVersion 拆成 (group, version)："v1" -> ("", "v1")；
// "apps/v1" -> ("apps", "v1")。
func (e *allowlistEntry) groupVersion() (string, string) {
	if i := strings.IndexByte(e.APIVersion, '/'); i >= 0 {
		return e.APIVersion[:i], e.APIVersion[i+1:]
	}
	return "", e.APIVersion
}

// matches 判断该条目是否过滤掉给定规则。apiVersion
// 必须精确匹配；其余字段按字面值或通配符 "*" 匹配。
// wildcard with "*".
func (e *allowlistEntry) matches(group, version, kind, path string, rule rule) bool {
	g, v := e.groupVersion()
	return g == group &&
		v == version &&
		(e.Kind == "*" || e.Kind == kind) &&
		(e.Path == "*" || e.Path == path) &&
		(e.ErrorType == "*" || e.ErrorType == rule.ErrorType) &&
		(e.Origin == "*" || e.Origin == rule.Origin)
}

// filterReports 返回移除 allowlist 规则后的 report；
// 规则被清空的 report 会丢弃。allowlist 为空时原样返回。
func filterReports(reports []*report, allowlist []allowlistEntry) []*report {
	if len(allowlist) == 0 {
		return reports
	}
	out := make([]*report, 0, len(reports))
	for _, r := range reports {
		filtered := fieldRules{}
		for path, rs := range r.Rules {
			for _, rule := range rs {
				if slices.ContainsFunc(allowlist, func(e allowlistEntry) bool {
					return e.matches(r.Group, r.Version, r.Kind, path, rule)
				}) {
					continue
				}
				filtered[path] = append(filtered[path], rule)
			}
		}
		if len(filtered) == 0 {
			continue
		}
		out = append(out, &report{
			Group: r.Group, Version: r.Version, Kind: r.Kind, Rules: filtered,
		})
	}
	return out
}

// loadAllowlist 从指定路径读取 YAML；路径为空时返回 nil。
// 读取/解析失败，或条目缺少必填 reason 时报错。
func loadAllowlist(path string) ([]allowlistEntry, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading allowlist %q: %w", path, err)
	}
	var entries []allowlistEntry
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing allowlist %q: %w", path, err)
	}
	for i, e := range entries {
		for _, f := range []struct{ name, val string }{
			{"apiVersion", e.APIVersion},
			{"kind", e.Kind},
			{"path", e.Path},
			{"errorType", e.ErrorType},
			{"origin", e.Origin},
			{"reason", e.Reason},
		} {
			if strings.TrimSpace(f.val) == "" {
				return nil, fmt.Errorf("allowlist %q entry %d: %s is required (use %q to wildcard)", path, i, f.name, "*")
			}
		}
	}
	return entries, nil
}

// coverageTestGen 为 validation-gen 的覆盖率生成测试文件。
// 它可以输出规则注册（init 函数）、apiVersions 变量以及 TestMain。
// 同一包会生成两类文件：按版本注册规则的 *_test.go，以及汇总断言的 main_test.go。
type coverageTestGen struct {
	generator.GoGenerator
	outputPackage string
	imports       namer.ImportTracker
	reports       []*report
	emitMain      bool // true 时输出 TestMain 与 apiVersions
	versions      []string
}

// newCoverageTestGen 构造一个生成器；filename 由调用方按版本或 main 命名。
func newCoverageTestGen(outputPackage, filename string, reports []*report, emitMain bool, versions []string) generator.Generator {
	return &coverageTestGen{
		GoGenerator: generator.GoGenerator{
			OutputFilename: filename,
		},
		outputPackage: outputPackage,
		imports:       generator.NewImportTrackerForPackage(outputPackage),
		reports:       reports,
		emitMain:      emitMain,
		versions:      versions,
	}
}

// Namers 注册 raw 命名系统。
func (g *coverageTestGen) Namers(*generator.Context) namer.NameSystems {
	return namer.NameSystems{"raw": namer.NewRawNamer(g.outputPackage, g.imports)}
}

// Imports 返回生成文件所需的导入行。
func (g *coverageTestGen) Imports(*generator.Context) []string { return g.imports.ImportLines() }

// Filter 恒为 false：coverage 生成器不使用按类型的生成流程。
func (g *coverageTestGen) Filter(*generator.Context, *types.Type) bool { return false }

// emitRegisterDeclaredRules 生成 init()，把每个 GVK 的声明式规则注册到运行时测试注册表。
// 路径与规则都排序，保证生成文件稳定、可复现。
func emitRegisterDeclaredRules(sw *generator.SnippetWriter, c *generator.Context, reports []*report) {
	for _, r := range reports {
		args := generator.Args{
			"schema":  mkSymbolArgs(c, schemaPkgSymbols),
			"runtime": mkSymbolArgs(c, runtimeRegisterSymbols),
			"group":   strconv.Quote(r.Group),
			"version": strconv.Quote(r.Version),
			"kind":    strconv.Quote(r.Kind),
		}
		sw.Do("func init() {\n", nil)
		sw.Do("    $.runtime.RegisterDeclaredRules|raw$(\n", args)
		sw.Do("        $.schema.GroupVersionKind|raw${Group: $.group$, Version: $.version$, Kind: $.kind$},\n", args)
		sw.Do("        $.runtime.FieldRules|raw${\n", args)

		paths := make([]string, 0, len(r.Rules))
		for p := range r.Rules {
			paths = append(paths, p)
		}
		slices.Sort(paths)
		for _, path := range paths {
			rules := slices.Clone(r.Rules[path])
			slices.SortFunc(rules, func(a, b rule) int {
				if c := cmp.Compare(a.ErrorType, b.ErrorType); c != 0 {
					return c
				}
				return cmp.Compare(a.Origin, b.Origin)
			})
			sw.Do("            $.path$: {\n", generator.Args{"path": strconv.Quote(path)})
			for _, r := range rules {
				ruleArgs := generator.Args{"errorType": strconv.Quote(r.ErrorType)}
				if r.Origin != "" {
					ruleArgs["origin"] = strconv.Quote(r.Origin)
					sw.Do("                {ErrorType: $.errorType$, Origin: $.origin$},\n", ruleArgs)
				} else {
					sw.Do("                {ErrorType: $.errorType$},\n", ruleArgs)
				}
			}
			sw.Do("            },\n", nil)
		}
		sw.Do("        },\n    )\n}\n\n", nil)
	}
}

// emitTestMain 生成 TestMain：跑完测试后再断言声明式覆盖是否完整，
// 覆盖缺口会以退出码非零的形式让 CI 失败。
func emitTestMain(sw *generator.SnippetWriter, c *generator.Context) {
	args := generator.Args{
		"testing": mkSymbolArgs(c, testingSymbols),
		"fmt":     mkSymbolArgs(c, fmtPkgSymbols),
		"os":      mkSymbolArgs(c, osPkgSymbols),
		"runtime": mkSymbolArgs(c, runtimeAssertCovSymbols),
	}
	sw.Do("func TestMain(m *$.testing.M|raw$) {\n", args)
	sw.Do("    code := m.Run()\n", nil)
	sw.Do("    if err := $.runtime.AssertDeclarativeCoverage|raw$(); err != nil {\n", args)
	sw.Do("        $.fmt.Fprintln|raw$($.os.Stderr|raw$, err)\n", args)
	sw.Do("        if code == 0 {\n            code = 1\n        }\n    }\n", nil)
	sw.Do("    $.os.Exit|raw$(code)\n}\n", args)
}

// Init 写出整个文件：先注册规则，再（可选）写 apiVersions 与 TestMain。
func (g *coverageTestGen) Init(c *generator.Context, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	if len(g.reports) > 0 {
		emitRegisterDeclaredRules(sw, c, g.reports)
	}

	if g.emitMain {
		if len(g.versions) > 0 {
			versions := slices.Clone(g.versions)
			slices.Sort(versions)
			sw.Do("var apiVersions = []string{", nil)
			for i, v := range versions {
				if i > 0 {
					sw.Do(", ", nil)
				}
				sw.Do("$.v$", generator.Args{"v": strconv.Quote(v)})
			}
			sw.Do("}\n\n", nil)
		}
		emitTestMain(sw, c)
	}
	return sw.Error()
}

// collectReports 为 pkg 中每个至少有一条声明规则的 Kind
// 向 groupKindReports 追加一个 report；记录所有包，
// testTargets 在输出阶段过滤掉非 API 包（Version 为空）。
func collectReports(pkg *types.Package, rootTypes []*types.Type, td *typeDiscoverer, groupKindReports map[schema.GroupKind][]*report) {
	// 从输入包推导 (group, version)：
	// group：若定义了 GroupName 常量（既有 API 约定）则取它，
	// 否则回退到包路径。
	// version：包名看起来像 API 版本（vN[alphaM|betaM]）时取包名，
	// 否则为空——testTargets 据此
	// 跳过非 API 包。
	var group, version string
	if c, ok := pkg.Constants["GroupName"]; ok && c.ConstValue != nil {
		group = *c.ConstValue
	} else {
		group = pkg.Path
	}
	if apiVersionRe.MatchString(pkg.Name) {
		version = pkg.Name
	}

	// 只有声明了规则的根类型才需要测试夹具；规则数由类型树遍历得出。
	for _, t := range rootTypes {
		rules := collectRules(td.typeNodes[t])
		if len(rules) == 0 {
			continue
		}
		kind := t.Name.Name
		gk := schema.GroupKind{Group: group, Kind: kind}
		groupKindReports[gk] = append(groupKindReports[gk], &report{
			Group:   group,
			Version: version,
			Kind:    kind,
			Rules:   rules,
		})
	}
}

// testTargets 为 groupKindReports 中每个 Kind 返回一个 SimpleTarget。
// 输出目录为 <testOutputRoot>/<short-group>/<lowercase(kind)>/，
// 每个版本一个 <prefix><version>_test.go，
// 外加共享的 <prefix>main_test.go。跳过 Version 为空的 Kind
// （非 API 包）以及所有规则都被 allowlist 过滤的 Kind。
func testTargets(testOutputRoot, filePrefix string, groupKindReports map[schema.GroupKind][]*report, allowlist []allowlistEntry, boilerplate []byte) []generator.Target {
	if testOutputRoot == "" || len(groupKindReports) == 0 {
		return nil
	}
	out := make([]generator.Target, 0, len(groupKindReports))
	for _, reports := range groupKindReports {
		// collectReports 保证 reports 非空。
		first := reports[0]
		if first.Version == "" {
			continue // not a real API package
		}
		reports = filterReports(reports, allowlist)
		if len(reports) == 0 {
			continue // every rule was allowlisted away
		}
		lowerKind := strings.ToLower(first.Kind)
		// 用短 group 名（首段 DNS 标签）作目录；空的旧 group 用 "core"，
		// 与 client-gen / informer-gen 的约定一致。
		group := first.Group
		if group == "" {
			group = "core"
		}
		pkgDir := filepath.Join(testOutputRoot, strings.Split(group, ".")[0], lowerKind)

		out = append(out, &generator.SimpleTarget{
			PkgName:       lowerKind,
			PkgPath:       pkgDir, // informational; gengo writes to PkgDir
			PkgDir:        pkgDir,
			HeaderComment: boilerplate,
			GeneratorsFunc: func(*generator.Context) []generator.Generator {
				gens := make([]generator.Generator, 0, len(reports)+1) // +1 for main_test.go
				versions := make([]string, 0, len(reports))
				for _, r := range reports {
					gens = append(gens, newCoverageTestGen(pkgDir, filePrefix+r.Version+"_test.go", []*report{r}, false, nil))
					versions = append(versions, r.Version)
				}
				gens = append(gens, newCoverageTestGen(pkgDir, filePrefix+"main_test.go", nil, true, versions))
				return gens
			}})
	}
	return out
}

// collectRules 遍历 node 的类型树，返回其中声明的 FieldRules。
// seen 用 defer delete 维护“当前路径”的访问集：同一类型出现在兄弟分支是允许的，
// 只有递归回环才应剪枝。
func collectRules(node *typeNode) fieldRules {
	if node == nil {
		return nil
	}
	rules := fieldRules{}
	seen := map[*typeNode]bool{}

	record := func(path string, fns []validators.FunctionGen) {
		for _, fn := range fns {
			recordRules(rules, path, fn, "")
		}
	}

	var walkNode func(*typeNode, string, bool, bool)
	var walkChild func(*childNode, string, bool, bool)
	walkNode = func(n *typeNode, path string, skipElem, skipKey bool) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		defer delete(seen, n)

		record(path, n.typeValidations.Functions)
		if n.valueType == nil {
			return
		}

		if n.typeValidations.OpaqueType {
			return
		}

		skipElem = skipElem || n.typeValidations.OpaqueValType
		skipKey = skipKey || n.typeValidations.OpaqueKeyType

		record(joinPath(path, "[*]"), n.typeValIterations.Functions)
		record(path, n.typeKeyIterations.Functions) // keys validate at parent path

		for _, fld := range n.fields {
			walkChild(fld, joinPath(path, fld.jsonName), false, false)
		}
		if n.elem != nil && !skipElem {
			walkChild(n.elem, joinPath(path, "[*]"), false, false)
		}
		if n.key != nil && !skipKey {
			walkChild(n.key, path, false, false)
		}
		if n.underlying != nil {
			walkChild(n.underlying, path, skipElem, skipKey)
		}
	}
	walkChild = func(c *childNode, path string, skipElem, skipKey bool) {
		if c == nil {
			return
		}
		record(path, c.fieldValidations.Functions)
		record(joinPath(path, "[*]"), c.fieldValIterations.Functions)
		record(path, c.fieldKeyIterations.Functions)

		if c.fieldValidations.OpaqueType {
			return
		}

		skipElem = skipElem || c.fieldValidations.OpaqueValType
		skipKey = skipKey || c.fieldValidations.OpaqueKeyType

		walkNode(c.node, path, skipElem, skipKey)
	}
	walkNode(node, "", false, false)
	return rules
}

// recordRules 递归遍历 fg，把 Wrapper/MultiWrapperFunction 的
// PathFragment 累积到 suffix，在每个产出叶子处记录 (basePath+suffix, Rule)。
// 同一次调用若运行时在不同类型或路径片段上报多个错误，
// FunctionGen 可声明多个 Emission。
// 例如带 NoAddItem 和 NoRemoveItem 的 ValSliceUpdate。
func recordRules(rules fieldRules, basePath string, fg validators.FunctionGen, suffix string) {
	if len(fg.Emits) > 0 {
		for _, e := range fg.Emits {
			path := basePath + suffix + e.PathFragment
			rules[path] = append(rules[path], rule{
				ErrorType: string(e.Type),
				Origin:    e.Origin,
			})
		}
		return
	}
	for _, arg := range fg.Args {
		switch a := arg.(type) {
		case validators.WrapperFunction:
			recordRules(rules, basePath, a.Function, suffix+a.PathFragment)
		case validators.MultiWrapperFunction:
			for _, child := range a.Functions {
				recordRules(rules, basePath, child, suffix+a.PathFragment)
			}
		}
	}
}

// joinPath 把 seg 拼接到 base 之后；seg 为空时什么也不做，
// 因此内嵌的 struct 字段（jsonName 为 ""）保持透明。
// 以 "[" 开头的段（如 "[*]"）直接拼接，不再加点号，以匹配 field.Path 的渲染形式。
func joinPath(base, seg string) string {
	if seg == "" {
		return base
	}
	if base == "" {
		return seg
	}
	if strings.HasPrefix(seg, "[") {
		return base + seg
	}
	return base + "." + seg
}
