/*
Copyright 2024 The Kubernetes Authors.

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
	"reflect"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// These are the comment tags that carry parameters for validation generation.
const (
	// Defines which types to generate validation for.  There are two places
	// this can be used:
	//   Per-package:
	//     * "*": generate validation for all types in this package
	//	   * "TypesWithField=FooBar": generate validation for all types with a
	//	     field named "FooBar"
	//     * "TypesWithSuffix=FooBar": generate validation for all types whose
	//       name ends with "FooBar"
	//   Per-type:
	//	   * "true": generate validation for this type
	//	   * "false": do not generate validation for this type
	mainTagName = "k8s:validation-gen"
	// Defines the type of the scheme used to register validations. Defaults to
	// "k8s.io/apimachinery/pkg.runtime.Scheme", but can be set to another type
	// (e.g. in tests), or set to "nil" to disable scheme registration for this
	// package.
	schemeRegistryTagName = "k8s:validation-gen-scheme-registry"
	// If set, generate go test files for test fixtures.  Supported values: "validateFalse".
	testFixtureTagName = "k8s:validation-gen-test-fixture"

	// name of the subresource that this type represents and can validate declaratively.
	isSubresourceTagName = "k8s:isSubresource"

	// name of a subresource that this type can validate declaratively, tag may be
	// repeated to support multiple subresources.
	supportsSubresourceTagName = "k8s:supportsSubresource"

	// if set on a package, generates declarative coverage test targets even if it's not a versioned API package.
	generateTestTargetsTagName = "k8s:validation-gen-test-targets"
)

var (
	runtimePkg   = "k8s.io/apimachinery/pkg/runtime"
	schemeType   = types.Name{Package: runtimePkg, Name: "Scheme"}
	metav1Pkg    = "k8s.io/apimachinery/pkg/apis/meta/v1"
	listMetaType = types.Name{Package: metav1Pkg, Name: "ListMeta"}
)

// extractAndParseTag extracts all the values for a given tag, according to the
// tag grammar.
//
// codetags.Extract 返回 map，顺序不确定；本函数只筛选指定 tag，
// 因此调用方拿到的顺序与源码出现顺序一致（ParseAll 保持行序）。
func extractAndParseTag(tagName string, comments []string) ([]codetags.Tag, error) {
	extracted := codetags.Extract("+", comments)
	var tags []codetags.Tag
	for key, lines := range extracted {
		if key != tagName {
			continue
		}
		t, err := codetags.ParseAll(lines)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tags: %w: %s", err, lines)
		}
		tags = append(tags, t...)
	}
	return tags, nil
}

// validationTypeMatch returns the +k8s:validation-gen tag values for pkg,
// or false if validation-gen should not run.
//
// 是否生成由 apidefinitions 统一判定，以便和 client-gen 等工具共享
// 同一套 API 识别规则。
func validationTypeMatch(pkg *types.Package, idOpts []apidefinitions.Option) ([]string, bool) {
	info, err := apidefinitions.Identify(pkg, apidefinitions.Validation, idOpts...)
	if err != nil {
		klog.Fatal(err)
	}
	if !info.ShouldGenerate() {
		return nil, false
	}
	return info.TypeFilters(), true
}

// checkMainTag 判断注释中是否存在指定取值的 +k8s:validation-gen 标签。
//
// require 为空时匹配无值形式（// +k8s:validation-gen），
// 有值时要求标签值序列与 require 完全一致。
// TODO: this can just accept a single bool
func checkMainTag(comments []string, require ...string) bool {
	// TODO: convert to extractAndParseTag() and update all callers to use quoted values
	tags, err := gengo.ExtractFunctionStyleCommentTags("+", []string{mainTagName}, comments)
	if err != nil {
		klog.Fatalf("Failed to extract tags: %v", err)
	}
	values, found := tags[mainTagName]
	if !found {
		return false
	}

	if len(require) == 0 {
		return len(values) == 1 && values[0].Value == ""
	}

	valueStrings := make([]string, len(values))
	for i, tag := range values {
		valueStrings[i] = tag.Value
	}

	return reflect.DeepEqual(valueStrings, require)
}

// schemeRegistryTag 解析包级 scheme 注册配置，返回 scheme 类型及是否注册。
//
// "nil" 表示该包不注册（用于同一 input 被生成到多个包时避免重复注册），
// 未标注则默认使用 apimachinery 的 runtime.Scheme。
func schemeRegistryTag(pkg *types.Package) (types.Name, bool) {
	// TODO: convert to extractAndParseTag() and update all callers to use quoted values
	tags, err := gengo.ExtractFunctionStyleCommentTags("+", []string{schemeRegistryTagName}, pkg.Comments)
	if err != nil {
		klog.Fatalf("Failed to extract scheme registry tags: %v", err)
	}
	values, found := tags[schemeRegistryTagName]
	if !found || len(values) == 0 {
		return schemeType, true // default
	}
	if len(values) > 1 {
		panic(fmt.Sprintf("Package %q contains more than one usage of %q", pkg.Path, schemeRegistryTagName))
	}
	val := values[0].Value
	if val == "nil" {
		// no registration wanted for this package
		return types.Name{}, false
	}
	return types.ParseFullyQualifiedName(val), true
}

// registerScheme reports whether pkg registers its validations with a scheme.
func registerScheme(pkg *types.Package) bool {
	_, ok := schemeRegistryTag(pkg)
	return ok
}

// isSubresourceTag 返回类型声明的 subresource 名（+k8s:isSubresource）。
//
// 每个类型至多声明一个主 subresource，重复声明直接 panic。
func isSubresourceTag(t *types.Type) (string, bool) {
	var comments []string
	comments = append(comments, t.SecondClosestCommentLines...)
	comments = append(comments, t.CommentLines...)
	tags, err := extractAndParseTag(isSubresourceTagName, comments)
	if err != nil {
		klog.Fatalf("Failed to extract isSubresource tags: %v", err)
	}
	if len(tags) == 0 {
		return "", false
	}
	if len(tags) > 1 {
		panic(fmt.Sprintf("Type %q contains more than one usage of %q", t.Name.String(), isSubresourceTagName))
	}
	return tags[0].Value, true
}

// supportedSubresourceTags 收集类型可通过 +k8s:supportsSubresource 声明的
// 全部 subresource 名，标签可重复出现。
func supportedSubresourceTags(t *types.Type) sets.Set[string] {
	var comments []string
	comments = append(comments, t.SecondClosestCommentLines...)
	comments = append(comments, t.CommentLines...)
	tags, err := extractAndParseTag(supportsSubresourceTagName, comments)
	if err != nil {
		klog.Fatalf("Failed to extract supportedSubresource tags: %v", err)
	}
	if len(tags) == 0 {
		return sets.New[string]()
	}
	subresources := sets.New[string]()
	for _, tag := range tags {
		subresources.Insert(tag.Value)
	}
	return subresources
}

// testFixtureTagValues 是 +k8s:validation-gen-test-fixture 当前支持的全部取值。
var testFixtureTagValues = sets.New("validateFalse")

// testFixtureTag 返回包声明的测试夹具种类；非法取值直接 panic，
// 避免生成出无人认识的测试文件。
func testFixtureTag(pkg *types.Package) sets.Set[string] {
	result := sets.New[string]()
	// TODO: convert to extractAndParseTag() and update all callers to use quoted values
	tags, err := gengo.ExtractFunctionStyleCommentTags("+", []string{testFixtureTagName}, pkg.Comments)
	if err != nil {
		klog.Fatalf("Failed to extract test fixture tags: %v", err)
	}
	values, found := tags[testFixtureTagName]
	if !found {
		return result
	}

	for _, tag := range values {
		if !testFixtureTagValues.Has(tag.Value) {
			panic(fmt.Sprintf("Package %q: %s must be one of '%s', but got: %s", pkg.Path, testFixtureTagName, testFixtureTagValues.UnsortedList(), tag.Value))
		}
		result.Insert(tag.Value)
	}
	return result
}

// generateTestTargetsTag 判断包是否要求为声明式校验生成覆盖率测试 target。
func generateTestTargetsTag(pkg *types.Package) bool {
	tags, err := gengo.ExtractFunctionStyleCommentTags("+", []string{generateTestTargetsTagName}, pkg.Comments)
	if err != nil {
		klog.Fatalf("Failed to extract %s tags: %v", generateTestTargetsTagName, err)
	}
	_, found := tags[generateTestTargetsTagName]
	return found
}

// NameSystems returns the name system used by the generators in this package.
//
// objectvalidationfn 是校验函数专用的命名系统，生成的函数名形如
// Validate_<Type>，必须全局可预测，便于测试和注册代码引用。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public":             namer.NewPublicNamer(1),
		"raw":                namer.NewRawNamer("", nil),
		"objectvalidationfn": validationFnNamer(),
		"private":            namer.NewPrivateNamer(0),
		"name":               namer.NewPublicNamer(0),
	}
}

// validationFnNamer 定义校验函数命名策略：前缀 Validate_，各级名字用 "_" 连接。
func validationFnNamer() *namer.NameStrategy {
	return &namer.NameStrategy{
		Prefix: "Validate_",
		Join: func(pre string, in []string, post string) string {
			return pre + strings.Join(in, "_") + post
		},
	}
}

// DefaultNameSystem returns the default name system for ordering the types to be
// processed by the generators in this package.
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 构造 validation-gen 的全部生成目标。
//
// 流程：先识别所有输入包及其类型来源（+k8s:validation-gen-input 可让类型与
// 输出包分离），再为每个输入包发现类型、收集规则、生成校验代码，最后统一
// 校验自定义校验函数是否都有标签。顺序敏感性体现在 canonical 包的选取：
// 跨包引用必须指向唯一一个注册 scheme 的包。
func GetTargets(context *generator.Context, args *Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, gengo.StdBuildTag, gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	var targetList []generator.Target

	// First load other "input" packages.  We do this as a single call because
	// it is MUCH faster.
	//
	// 一次性批量加载，避免逐包解析带来的重复类型检查开销。
	inputPkgs := make([]string, 0, len(context.Inputs))
	pkgToInput := map[string]string{}
	inputToCanonicalPkg := map[string]string{} // types package -> the output package cross-package references resolve to
	for _, input := range context.Inputs {
		klog.V(4).Infof("considering pkg %q", input)
		pkg := context.Universe[input]

		info, err := apidefinitions.Identify(pkg, apidefinitions.Validation, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			continue
		}

		// +k8s:validation-gen-input may direct the generator at types in
		// a different package than the one where validators will be emitted.
		inputPath := info.ExternalTypes()
		pkgToInput[input] = inputPath
		if inputPath != pkg.Path {
			klog.V(4).Infof("  input pkg %v", inputPath)
			inputPkgs = append(inputPkgs, inputPath)
		}
		// An input's validation may be generated into more than one package. One
		// is canonical -- the package cross-package references resolve to. The
		// registering package is canonical; if none registers, the first one seen
		// wins. At most one package may register.
		//
		// 如果两个包都注册 scheme，跨包引用会产生歧义，直接失败。
		if prev, ok := inputToCanonicalPkg[inputPath]; !ok {
			inputToCanonicalPkg[inputPath] = input
		} else if registerScheme(pkg) {
			if registerScheme(context.Universe[prev]) {
				klog.Fatalf("input %q is generated into two registering packages (%q, %q); mark one +k8s:validation-gen-scheme-registry=nil", inputPath, prev, input)
			}
			inputToCanonicalPkg[inputPath] = input // a registering package displaces a non-registering one
		}
	}

	// Make sure explicit extra-packages are added.
	var readOnlyPkgs []string
	for _, pkg := range args.ReadOnlyPkgs {
		// In case someone specifies an extra as a path into vendor, convert
		// it to its "real" package path.
		if i := strings.Index(pkg, "/vendor/"); i != -1 {
			pkg = pkg[i+len("/vendor/"):]
		}
		readOnlyPkgs = append(readOnlyPkgs, pkg)
	}
	if expanded, err := context.FindPackages(readOnlyPkgs...); err != nil {
		klog.Fatalf("cannot find extra packages: %v", err)
	} else {
		readOnlyPkgs = expanded // now in fully canonical form
	}
	for _, extra := range readOnlyPkgs {
		inputPkgs = append(inputPkgs, extra)
		pkgToInput[extra] = extra
		// Don't let a read-only package override a generation mapping.
		//
		// 只读包只用于解析类型，不能替换已有的生成映射。
		if _, ok := inputToCanonicalPkg[extra]; !ok {
			inputToCanonicalPkg[extra] = extra
		}
	}

	if len(inputPkgs) > 0 {
		if _, err := context.LoadPackages(inputPkgs...); err != nil {
			klog.Fatalf("cannot load packages: %v", err)
		}
	}
	// update context.Order to the latest context.Universe
	orderer := namer.Orderer{Namer: namer.NewPublicNamer(1)}
	context.Order = orderer.OrderUniverse(context.Universe)

	// Initialize all validator plugins exactly once.
	//
	// 插件注册有全局副作用，必须只初始化一次，否则标签会被重复注册。
	validator := validators.InitGlobalValidator(context, inputToCanonicalPkg)

	// Create a type discoverer for all types of all inputs.
	td := NewTypeDiscoverer(validator, inputToCanonicalPkg)
	if err := td.Init(context); err != nil {
		klog.Fatalf("Error discovering constants: %v", err)
	}

	// Create a linter to collect errors as we go.
	linter := newLinter(lintRules(validator)...)

	// groupKindReports accumulates Reports across every input, keyed by
	// GroupKind so testTargets emits exactly one SimpleTarget per Kind.
	groupKindReports := map[schema.GroupKind][]*report{}

	// Build a cache of type->callNode for every type we need.
	//
	// 逐包处理：识别 → 发现类型 → lint → 生成 target。
	for _, input := range context.Inputs {
		klog.V(2).InfoS("processing", "pkg", input)

		pkg := context.Universe[input]

		schemeRegistry, registerThisPkg := schemeRegistryTag(pkg)

		criteria, found := validationTypeMatch(pkg, idOpts)
		if !found {
			klog.V(2).InfoS("  did not find required tag", "tag", mainTagName)
			continue
		}
		// 包级标签必须有值；这里提前拒绝空值，避免后面静默生成空文件。
		if len(criteria) == 1 && criteria[0] == "" {
			klog.Fatalf("%s: found package tag %q with no value", input, mainTagName)
		}
		// 校验标签取值只允许 *、TypesWithField=、TypesWithSuffix=。
		for _, crit := range criteria {
			if crit == "*" {
				continue
			}
			if val, found := strings.CutPrefix(crit, "TypesWithField="); found {
				if val == "" {
					klog.Fatalf("%s: found package tag \"%s=%s\" with empty value", input, mainTagName, crit)
				}
				continue
			}
			if val, found := strings.CutPrefix(crit, "TypesWithSuffix="); found {
				if val == "" {
					klog.Fatalf("%s: found package tag \"%s=%s\" with empty value", input, mainTagName, crit)
				}
				continue
			}
			klog.Fatalf("%s: unknown value for package tag %q: %q", input, mainTagName, crit)
		}
		// shouldCreateObjectValidationFn 决定某个类型是否作为根类型生成校验。
		//
		// 优先看类型级 opt-in/opt-out，其次排除 List 类型，最后才用包级
		// criteria 匹配。顺序保证类型级标签总能覆盖包级规则。
		shouldCreateObjectValidationFn := func(t *types.Type) bool {
			// Never generate validation for unexported types.
			if namer.IsPrivateGoName(t.Name.Name) {
				return false
			}
			// opt-out
			if checkMainTag(t.CommentLines, "false") {
				return false
			}
			if checkMainTag(t.SecondClosestCommentLines, "false") {
				return false
			}
			// opt-in
			if checkMainTag(t.CommentLines, "true") {
				return true
			}
			if checkMainTag(t.SecondClosestCommentLines, "true") {
				return true
			}

			// skip types that embed metav1.ListMeta
			//
			// List 类型的校验无实际意义，且会引入额外的跨包依赖。
			if t.Kind == types.Struct {
				for _, member := range t.Members {
					if member.Embedded && member.Type.Name == listMetaType {
						return false
					}
				}
			}

			// all types
			for _, v := range criteria {
				if v == "*" {
					return true
				}
				if field, found := strings.CutPrefix(v, "TypesWithField="); found {
					if isTypeWithField(t, field) {
						return true
					}
				}
				if field, found := strings.CutPrefix(v, "TypesWithSuffix="); found {
					if isTypeWithSuffix(t, field) {
						return true
					}
				}
			}
			return false
		}

		// Find the right input pkg, which might not be this one.
		inputPath := pkgToInput[input]
		// typesPkg is where the types that need validation are defined.
		// Sometimes it is different from pkg. For example, kubernetes core/v1
		// types are defined in k8s.io/api/core/v1, while the pkg which holds
		// defaulter code is at k/k/pkg/api/v1.
		typesPkg := context.Universe[inputPath]

		// Figure out which types we should be considering further.
		var rootTypes []*types.Type
		for _, t := range typesPkg.Types {
			if shouldCreateObjectValidationFn(t) {
				rootTypes = append(rootTypes, t)
			} else {
				klog.V(6).InfoS("skipping type", "type", t)
			}
		}
		// Deterministic ordering helps in logs and debugging.
		//
		// 排序也让生成的代码和注册顺序在不同机器上完全一致。
		slices.SortFunc(rootTypes, func(a, b *types.Type) int {
			return cmp.Compare(a.Name.String(), b.Name.String())
		})

		for _, t := range rootTypes {
			klog.V(3).InfoS("pre-processing", "type", t)
			if err := td.DiscoverType(t); err != nil {
				klog.Fatalf("failed to generate validations: %v", err)
			}
		}

		// 包级 nolint 允许测试夹具跳过风格检查。
		extracted := codetags.Extract("+", pkg.Comments)
		if _, ok := extracted["k8s:validation-gen-nolint"]; !ok {
			for _, t := range rootTypes {
				klog.V(3).InfoS("linting root-type", "type", t)
				if err := linter.lintType(t); err != nil {
					klog.Fatalf("failed to lint type %q: %v", t.Name, err)
				}
			}
		}

		targetList = append(targetList,
			&generator.SimpleTarget{
				PkgName:       pkg.Name,
				PkgPath:       pkg.Path,
				PkgDir:        pkg.Dir, // output pkg is the same as the input
				HeaderComment: boilerplate,

				// 只把属于 typesPkg 的类型交给生成器，避免把输出包里的
				// 其他类型误当成待校验类型。
				FilterFunc: func(c *generator.Context, t *types.Type) bool {
					return t.Name.Package == typesPkg.Path
				},

				GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
					generators = []generator.Generator{
						NewGenValidations(args.OutputFile, pkg.Path, typesPkg.Path, rootTypes, td, inputToCanonicalPkg, schemeRegistry, registerThisPkg),
					}
					// 测试夹具：为 validateFalse 生成对应的表驱动测试文件。
					testFixtureTags := testFixtureTag(pkg)
					if testFixtureTags.Len() > 0 {
						if !strings.HasSuffix(args.OutputFile, ".go") {
							panic(fmt.Sprintf("%s requires that output file have .go suffix", testFixtureTagName))
						}
						filename := args.OutputFile[0:len(args.OutputFile)-3] + "_test.go"
						generators = append(generators, FixtureTests(filename, testFixtureTags))
					}
					// 覆盖率测试 target：把每个 Kind 的规则导出成测试数据。
					if generateTestTargetsTag(pkg) {
						var reports []*report
						for _, t := range rootTypes {
							rules := collectRules(td.typeNodes[t])
							if len(rules) == 0 {
								continue
							}
							reports = append(reports, &report{
								Group:   pkg.Path,
								Version: pkg.Name,
								Kind:    t.Name.Name,
								Rules:   rules,
							})
						}
						if len(reports) > 0 {
							filename := args.OutputFile[0:len(args.OutputFile)-3] + "_coverage_test.go"
							generators = append(generators, newCoverageTestGen(pkg.Path, filename, reports, true, nil))
						}
					}
					return generators
				},
			})

		// Accumulate per-Kind rules; testTargets emits after the loop.
		// Only the registering package contributes coverage; a non-registering
		// package generated from the same input has identical rules, so counting
		// it too would double-list the version for the Kind.
		if args.TestOutputRoot != "" && registerThisPkg {
			collectReports(typesPkg, rootTypes, td, groupKindReports)
		}
	}

	// All inputs processed: fail if a ValidateCustom_* function lacks a tag.
	if err := validators.VerifyCustomValidationsHaveTags(); err != nil {
		klog.Fatalf("%v", err)
	}

	// Emit per-Kind coverage test targets. No-op when --test-output-root is empty.
	allowlist, err := loadAllowlist(args.TestAllowlist)
	if err != nil {
		klog.Fatalf("loading allowlist: %v", err)
	}
	targetList = append(targetList, testTargets(args.TestOutputRoot, args.TestOutputFilePrefix, groupKindReports, allowlist, boilerplate)...)

	// lint 错误在这里一次性汇总输出，避免中途 Fatal 丢掉其余诊断。
	if len(linter.lintErrors) > 0 {
		buf := strings.Builder{}

		for t, errs := range linter.lintErrors {
			buf.WriteString(fmt.Sprintf("  type %v:\n", t))
			for _, err := range errs {
				buf.WriteString(fmt.Sprintf("    %s\n", err.Error()))
			}
		}
		klog.Fatalf("lint failed:\n%s", buf.String())
	}
	return targetList
}

// isTypeWithField 判断结构体是否含有指定名字的字段（TypesWithField 规则）。
func isTypeWithField(t *types.Type, fieldName string) bool {
	if t.Kind == types.Struct {
		for _, field := range t.Members {
			if field.Name == fieldName {
				return true
			}
		}
	}
	return false
}

// isTypeWithSuffix 判断类型名是否以指定后缀结尾（TypesWithSuffix 规则）。
func isTypeWithSuffix(t *types.Type, suffix string) bool {
	return strings.HasSuffix(t.Name.Name, suffix)
}
