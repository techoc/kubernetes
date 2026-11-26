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

// Package generators 实现 conversion-gen：为 internal 与 versioned 类型之间
// 生成 Convert_<in>_To_<out> 函数以及 autoConvert_* 辅助函数。
//
// 生成器会扫描 peer 包中已有（手写）的转换函数，避免重复生成；对内存布局
// 完全一致的类型使用 unsafe 直接拷贝；其余情况逐字段生成转换代码，
// 无法自动处理的字段会以 WARNING 注释标注并要求人工实现。
package generators

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"reflect"
	"sort"
	"strings"

	"k8s.io/code-generator/cmd/conversion-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// 以下常量是承载转换生成参数的注释标签名。
const (
	// 例如 doc.go 中写 "+k8s:conversion-gen=<peer-pkg>"，<peer-pkg> 是 peer 类型
	// 所在包的 import path；在类型注释上写 "+k8s:conversion-gen=false" 则跳过该类型。
	tagName = "k8s:conversion-gen"
	// 类型注释中写 "+k8s:conversion-gen:explicit-from=net/url.Values"，
	// 会生成从 net/url.Values 到该类型的转换。
	explicitFromTagName = "k8s:conversion-gen:explicit-from"
)

// extractTagValues 解析注释中的函数式 tag（形如 +tag=value），返回所有值。
// 不存在该 tag 时返回 nil 切片而不是错误，便于调用方区分“未声明”与“解析失败”。
func extractTagValues(tagName string, comments []string) ([]string, error) {
	tags, err := gengo.ExtractFunctionStyleCommentTags("+", []string{tagName}, comments)
	if err != nil {
		return nil, err
	}
	tagList, exists := tags[tagName]
	if !exists {
		return nil, nil
	}
	values := make([]string, len(tagList))
	for i, v := range tagList {
		values[i] = v.Value
	}
	return values, nil
}

// extractTag 读取 +k8s:conversion-gen 标签的值（例如 false 表示跳过该类型）。
func extractTag(comments []string) ([]string, error) {
	return extractTagValues(tagName, comments)
}

// extractExplicitFromTag 读取 +k8s:conversion-gen:explicit-from 标签，
// 用于声明从外部类型（如 net/url.Values）到本类型的转换。
func extractExplicitFromTag(comments []string) ([]string, error) {
	return extractTagValues(explicitFromTagName, comments)
}

// isCopyOnly 判断转换函数是否被标记为 copy-only：这类函数等价于内存赋值，
// 只用于反射路径，不应阻止 unsafe 内存拷贝优化。
func isCopyOnly(comments []string) (bool, error) {
	values, err := extractTagValues("k8s:conversion-fn", comments)
	if err != nil {
		return false, err
	}
	return len(values) == 1 && values[0] == "copy-only", nil
}

// isDrop 判断转换函数是否被标记为 drop：生成字段转换时跳过该字段。
func isDrop(comments []string) (bool, error) {
	values, err := extractTagValues("k8s:conversion-fn", comments)
	if err != nil {
		return false, err
	}
	return len(values) == 1 && values[0] == "drop", nil
}

// conversionNamer 是转换函数名的命名策略：只保留最后一级包名做前缀，
// 段之间用 "_" 连接，从而得到 Convert_v1_Foo_To_internal_Foo 这类名字。
// TODO: 此命名策略只是为了减少当次 PR 的改动量而保留，应改用 PublicNamer。
func conversionNamer() *namer.NameStrategy {
	return &namer.NameStrategy{
		Join: func(pre string, in []string, post string) string {
			return strings.Join(in, "_")
		},
		PrependPackageNames: 1,
	}
}

// defaultFnNamer 为默认函数生成 SetDefaults_<Type> 形式的名称。
func defaultFnNamer() *namer.NameStrategy {
	return &namer.NameStrategy{
		Prefix: "SetDefaults_",
		Join: func(pre string, in []string, post string) string {
			return pre + strings.Join(in, "_") + post
		},
	}
}

// NameSystems 返回本包生成器使用的命名系统。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public":    conversionNamer(),
		"raw":       namer.NewRawNamer("", nil),
		"defaultfn": defaultFnNamer(),
	}
}

// DefaultNameSystem 返回本包生成器排序类型时默认使用的命名系统。
func DefaultNameSystem() string {
	return "public"
}

// getPeerTypeFor 在候选 peer 包中查找与 t 同名的类型。
// 按传入顺序返回第一个命中者，顺序决定同名类型时优先使用哪个 peer。
func getPeerTypeFor(context *generator.Context, t *types.Type, potenialPeerPkgs []string) *types.Type {
	for _, ppp := range potenialPeerPkgs {
		p := context.Universe.Package(ppp)
		if p == nil {
			continue
		}
		if p.Has(t.Name.Name) {
			return p.Type(t.Name.Name)
		}
	}
	return nil
}

// conversionPair 表示一个有向转换：从 inType 转到 outType。
type conversionPair struct {
	inType  *types.Type
	outType *types.Type
}

// conversionFuncMap 以转换对为键索引手写转换函数。
// 之所以用类型对而非函数名做键，是因为生成时需要按 in/out 类型去重与查找。
// map 中的值都是 DeclarationOf，底层为 Func。
type conversionFuncMap map[conversionPair]*types.Type

// getManualConversionFunctions 扫描包中所有手写转换函数，收集到 manualMap。
// 识别依据是固定签名：func Convert_inType_To_outType(in, out *T, s Scope) error。
// 名字不符合约定的 Convert_ 函数会记录错误，提示用户修正签名。
func getManualConversionFunctions(context *generator.Context, pkg *types.Package, manualMap conversionFuncMap) {
	if pkg == nil {
		klog.Warning("Skipping nil package passed to getManualConversionFunctions")
		return
	}
	klog.V(3).Infof("Scanning for conversion functions in %v", pkg.Path)

	scopeName := types.Ref(conversionPackagePath, "Scope").Name
	errorName := types.Ref("", "error").Name
	buffer := &bytes.Buffer{}
	sw := generator.NewSnippetWriter(buffer, context, "$", "$")

	for _, f := range pkg.Functions {
		// 必须是普通函数，排除声明与变量等非 Func 形态。
		if f.Underlying == nil || f.Underlying.Kind != types.Func {
			klog.Errorf("Malformed function: %#v", f)
			continue
		}
		if f.Underlying.Signature == nil {
			klog.Errorf("Function without signature: %#v", f)
			continue
		}
		klog.V(6).Infof("Considering function %s", f.Name)
		signature := f.Underlying.Signature
		// 检查是否为转换函数：约定签名是
		// func Convert_inType_To_outType(inType, outType, conversion.Scope) error
		// 检查是否为转换函数：约定签名是
		// func Convert_inType_To_outType(inType, outType, conversion.Scope) error
		if signature.Receiver != nil {
			klog.V(6).Infof("%s has a receiver", f.Name)
			continue
		}
		// 第 3 个参数必须是 conversion.Scope，前两个是 in/out 指针。
		if len(signature.Parameters) != 3 || signature.Parameters[2].Type.Name != scopeName {
			klog.V(6).Infof("%s has wrong parameters", f.Name)
			continue
		}
		// 返回值必须只有一个 error。
		if len(signature.Results) != 1 || signature.Results[0].Type.Name != errorName {
			klog.V(6).Infof("%s has wrong results", f.Name)
			continue
		}
		inType := signature.Parameters[0].Type
		outType := signature.Parameters[1].Type
		if inType.Kind != types.Pointer || outType.Kind != types.Pointer {
			klog.V(6).Infof("%s has wrong parameter types", f.Name)
			continue
		}
		// 现在检查函数名是否符合约定。
		// TODO: 这里应直接调用 Namer。
		args := argsFromType(inType.Elem, outType.Elem)
		sw.Do("Convert_$.inType|public$_To_$.outType|public$", args)
		if f.Name.Name == buffer.String() {
			klog.V(2).Infof("Found conversion function %s", f.Name)
			key := conversionPair{inType.Elem, outType.Elem}
			// 同一个包可能被扫描两次，这是允许的；但不同包重复定义同一转换则报错。
			// 同一包被扫描两次也无妨，后面会按转换对去重。
			if v, ok := manualMap[key]; ok && v != nil && v.Name.Package != pkg.Path {
				panic(fmt.Sprintf("duplicate static conversion defined: %s -> %s from:\n%s.%s\n%s.%s", key.inType, key.outType, v.Name.Package, v.Name.Name, f.Name.Package, f.Name.Name))
			}
			manualMap[key] = f
		} else {
			// 用户写了 Convert_ 前缀但签名不匹配时给出可操作的提示。
			// 提前拦截签名写错的情况，避免生成不可用的转换函数。
			if strings.HasPrefix(f.Name.Name, "Convert_") {
				klog.Errorf("Rename function %s %s -> %s to match expected conversion signature", f.Name.Package, f.Name.Name, buffer.String())
			}
			klog.V(3).Infof("%s has wrong name", f.Name)
		}
		// 复用 buffer 前必须清空，否则上一轮生成的名字会影响本轮比较。
		buffer.Reset()
	}
}

// GetTargets 为每个通过校验的 input 包构造一个生成目标（每个目标一个输出文件）。
// 流程：识别标签 → 收集 peer 包 → 加载 peer 类型 → 汇总手写转换函数 →
// 生成转换代码并计算哪些类型不能走 unsafe 拷贝。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, args.GeneratedBuildTag, gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	// lint 规则用于校验生成结果；未配置时不传该选项。
	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	targetList := []generator.Target{}

	// 先累积已有的手写转换函数，后续生成时据此避免重复。
	// 收集已有的转换函数。
	// TODO: 这里过于临时，需要更系统的做法。
	manualConversions := conversionFuncMap{}

	// 记录内存等价类型：布局相同且没有嵌套手写转换的类型可直接用 unsafe 拷贝。
	// 记录内存布局等价的类型：布局相同且嵌套类型没有手写转换。
	// TODO: 将来若能证明足够多的类型内存相同但嵌套转换确有必要，可放宽这一限制。
	memoryEquivalentTypes := equalMemoryTypes{}

	// 一次性加载所有 peer 包，比逐个加载快得多。
	// 所有 "input" 包一次性加载，批量解析远快于逐个加载。
	filteredInputs := make([]string, 0, len(context.Inputs))
	otherPkgs := make([]string, 0, len(context.Inputs))
	pkgToPeers := map[string][]string{}
	pkgToExternal := map[string]string{}

	for _, i := range context.Inputs {
		klog.V(3).Infof("considering pkg %q", i)
		pkg := context.Universe[i]

		// Identify 解析包级标记，判断是否需要生成以及 peer 包有哪些。
		info, err := apidefinitions.Identify(pkg, apidefinitions.Conversion, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			klog.V(3).Infof("  no tag")
			continue
		}
		filteredInputs = append(filteredInputs, i)

		// 仅有 +k8s:conversion-gen=false 时只保留手写转换，
		// 不生成由 peer 驱动的标准转换。
		// 仅声明 +k8s:conversion-gen=false 时，只输出包内手写转换，
		// 不生成 peer 驱动的标准转换。
		if !info.IsExplicitOnly() {
			peerPkgs := info.PeerPackages()
			klog.V(3).Infof("  peers: %q", peerPkgs)
			pkgToPeers[i] = peerPkgs
			otherPkgs = append(otherPkgs, peerPkgs...)
		}

		// 类型实际定义所在的包可能不同于 input 包（例如 k8s.io/api/core/v1）。
		externalTypes := info.ExternalTypes()
		if externalTypes != i {
			klog.V(3).Infof("  external types: %q", externalTypes)
			otherPkgs = append(otherPkgs, externalTypes)
		}
		pkgToExternal[i] = externalTypes
	}

	// 确保显式指定的 peer 包也被加载，并将它们附加到每个 input 的 peer 列表。
	// 确保显式指定的 peer 包也加入待处理列表。
	peers := args.BasePeerDirs
	peers = append(peers, args.ExtraPeerDirs...)
	if expanded, err := context.FindPackages(peers...); err != nil {
		klog.Fatalf("cannot find peer packages: %v", err)
	} else {
		otherPkgs = append(otherPkgs, expanded...)
		// 每个包都补上这些额外依赖。
		for k := range pkgToPeers {
			pkgToPeers[k] = append(pkgToPeers[k], expanded...)
		}
	}

	if len(otherPkgs) > 0 {
		if _, err := context.LoadPackages(otherPkgs...); err != nil {
			klog.Fatalf("cannot load packages: %v", err)
		}
	}
	// 加载新包后必须重排 Order，保证类型处理顺序稳定。
	// 用最新的 Universe 刷新排序顺序。
	orderer := namer.Orderer{Namer: namer.NewPublicNamer(1)}
	context.Order = orderer.OrderUniverse(context.Universe)

	// 在 peer 包中查找手写转换函数。
	// 扫描 peer 包中已有的转换函数。
	for _, pp := range otherPkgs {
		p := context.Universe[pp]
		if p == nil {
			klog.Fatalf("failed to find pkg: %s", pp)
		}
		getManualConversionFunctions(context, p, manualConversions)
	}

	// 只为显式作为 InputDir 传入的包生成转换。
	// 只为显式作为 InputDir 传入的包生成转换。
	for _, i := range filteredInputs {
		klog.V(3).Infof("considering pkg %q", i)
		pkg := context.Universe[i]

		// 把本包内的手写转换也加入汇总。
		// 收集转换函数与默认值函数。
		getManualConversionFunctions(context, pkg, manualConversions)

		// 找到真正的类型定义包，它可能不是当前 input 包。
		// 找到正确的输入包，它未必是当前包。
		externalTypes := pkgToExternal[i]

		// typesPkg 是 versioned 类型所在的包。有时它与 pkg 不同，例如
		// kubernetes core/v1 类型定义在 k8s.io/api/core/v1，而 pkg 是 pkg/api/v1。
		// typesPkg 是版本化类型的定义包，可能与 pkg 不同：例如 core/v1 类型
		// 定义在 k8s.io/api/core/v1，而 pkg 是 pkg/api/v1。
		typesPkg := context.Universe[externalTypes]

		// SkipUnsafe 时禁用 unsafe 拷贝，一律走逐字段转换。
		unsafeEquality := TypesEqual(memoryEquivalentTypes)
		if args.SkipUnsafe {
			unsafeEquality = noEquality{}
		}

		// 输出的包目录与 input 相同，但只用同包类型（t.Name.Package == typesPkg.Path）。
		targetList = append(targetList,
			&generator.SimpleTarget{
				PkgName:       path.Base(pkg.Path),
				PkgPath:       pkg.Path,
				PkgDir:        pkg.Dir, // output pkg is the same as the input
				HeaderComment: boilerplate,
				FilterFunc: func(c *generator.Context, t *types.Type) bool {
					return t.Name.Package == typesPkg.Path
				},
				GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
					return []generator.Generator{
						NewGenConversion(args.OutputFile, typesPkg.Path, pkg.Path, manualConversions, pkgToPeers[pkg.Path], unsafeEquality),
					}
				},
			})
	}

	// 若两个类型之间存在手写转换，则它们不能作为 unsafe 拷贝的候选，
	// 否则会跳过必须执行的转换逻辑。copy-only 转换例外。
	// 两类型间若有手写转换，就不应再作为 unsafe 内存拷贝的候选。
	for k, v := range manualConversions {
		copyOnly, err := isCopyOnly(v.CommentLines)
		if err != nil {
			klog.Errorf("error extracting tags: %v", err)
		} else if copyOnly {
			klog.V(4).Infof("Conversion function %s will not block memory copy because it is copy-only", v.Name)
			continue
		}
		// 该类型必须调用转换函数，因此不参与任何等价性判断。
		memoryEquivalentTypes.Skip(k.inType, k.outType)
	}

	return targetList
}

// equalMemoryTypes 缓存类型对的“内存布局是否等价”结论。
// 键为有向类型对，查找时两个方向都会命中；值为 false 表示明确不等价。
type equalMemoryTypes map[conversionPair]bool

// Skip 显式把 a 与 b（双向）标记为不等价，使它们永远不会被当作 unsafe 拷贝候选。
func (e equalMemoryTypes) Skip(a, b *types.Type) {
	e[conversionPair{a, b}] = false
	e[conversionPair{b, a}] = false
}

// Equal 返回两个类型是否内存等价；结果会按需缓存。
func (e equalMemoryTypes) Equal(a, b *types.Type) bool {
	equal, _ := e.cachingEqual(a, b, nil)
	return equal
}

// cachingEqual 递归比较 a、b 的内存等价性，并缓存可确定的结果。
// alreadyVisitedStack 用于检测递归类型中的环。
// 返回的 cacheable 表示该结论是否可靠可缓存；若为 false，说明结论只是为打破
// 环而做的临时假设，不能写入缓存。
// cachingEqual 递归比较 a、b 的内存布局是否相同，并复用已算出的结果。
// alreadyVisitedStack 用于检测递归中的环。
// 返回的 cacheable 表示结果是否确定、可安全缓存；为假时只是为打破递归成环
// 而做的临时假设。
func (e equalMemoryTypes) cachingEqual(a, b *types.Type, alreadyVisitedStack []*types.Type) (equal, cacheable bool) {
	if a == b {
		return true, true
	}
	// 命中缓存（任一方向）即可直接返回。
	if equal, ok := e[conversionPair{a, b}]; ok {
		return equal, true
	}
	if equal, ok := e[conversionPair{b, a}]; ok {
		return equal, true
	}
	result, cacheable := e.equal(a, b, alreadyVisitedStack)
	// 只缓存确定结论；临时假设若被缓存，后续调用可能永远得到错误答案。
	if cacheable {
		e[conversionPair{a, b}] = result
		e[conversionPair{b, a}] = result
	}
	return result, cacheable
}

// equal 递归比较 a、b 的内存等价性（不查缓存）。
// alreadyVisitedStack 用于检测递归类型中的环。
// 返回的 cacheable 含义与 cachingEqual 相同。
// equal 递归比较 a、b 的内存布局是否相同。
// alreadyVisitedStack 用于检测递归中的环。
// 返回的 cacheable 表示结果是否确定、可安全缓存；为假时只是为打破递归成环
// 而做的临时假设。
func (e equalMemoryTypes) equal(a, b *types.Type, alreadyVisitedStack []*types.Type) (equal, cacheable bool) {
	// 比较底层类型，别名只是命名差异，不影响内存布局。
	in, out := unwrapAlias(a), unwrapAlias(b)
	switch {
	case in == out:
		return true, true
	case in.Kind == out.Kind:
		for _, v := range alreadyVisitedStack {
			if v == in {
				// 已在本调用栈中出现过，说明存在环；返回假定相等以终止递归，
				// 但结论不可缓存。
				// 类型已在当前递归栈中，直接返回以避免无限递归；此结果不可缓存。
				return true, false
			}
		}
		alreadyVisitedStack = append(alreadyVisitedStack, in)

		switch in.Kind {
		case types.Struct:
			// 结构体要求成员数量与逐个成员类型都等价。
			if len(in.Members) != len(out.Members) {
				return false, true
			}
			cacheable = true
			for i, inMember := range in.Members {
				outMember := out.Members[i]
				memberEqual, memberCacheable := e.cachingEqual(inMember.Type, outMember.Type, alreadyVisitedStack)
				if !memberEqual {
					return false, true
				}
				// 任一成员结论不可缓存，整体结论也不可缓存。
				if !memberCacheable {
					cacheable = false
				}
			}
			return true, cacheable
		case types.Pointer:
			return e.cachingEqual(in.Elem, out.Elem, alreadyVisitedStack)
		case types.Map:
			// map 的 key 与 value 必须同时等价。
			keyEqual, keyCacheable := e.cachingEqual(in.Key, out.Key, alreadyVisitedStack)
			valueEqual, valueCacheable := e.cachingEqual(in.Elem, out.Elem, alreadyVisitedStack)
			return keyEqual && valueEqual, keyCacheable && valueCacheable
		case types.Slice:
			return e.cachingEqual(in.Elem, out.Elem, alreadyVisitedStack)
		case types.Interface:
			// 接口的运行时布局无法静态确定，保守地视为不等价。
			// TODO: 判断接口是否真正等价；目前只接受完全相同的接口类型。
			return false, true
		case types.Builtin:
			return in.Name.Name == out.Name.Name, true
		}
	}
	return false, true
}

// findMember 在结构体类型中按字段名查找成员。
func findMember(t *types.Type, name string) (types.Member, bool) {
	if t.Kind != types.Struct {
		return types.Member{}, false
	}
	for _, member := range t.Members {
		if member.Name == name {
			return member, true
		}
	}
	return types.Member{}, false
}

// unwrapAlias 递归剥离别名，返回最终的底层类型。
// unwrapAlias 逐层剥开别名，找到最底层类型。
func unwrapAlias(in *types.Type) *types.Type {
	for in.Kind == types.Alias {
		in = in.Underlying
	}
	return in
}

const (
	runtimePackagePath    = "k8s.io/apimachinery/pkg/runtime"
	conversionPackagePath = "k8s.io/apimachinery/pkg/conversion"
)

// noEquality 是永不认为类型等价的实现，用于 --skip-unsafe 时禁用 unsafe 拷贝。
type noEquality struct{}

func (noEquality) Equal(_, _ *types.Type) bool { return false }

// TypesEqual 抽象“两个类型能否直接内存拷贝”的判定，便于测试替换实现。
type TypesEqual interface {
	Equal(a, b *types.Type) bool
}

// genConversion 生成一个文件的自动转换函数。
// genConversion 生成包含自动转换代码的文件。
type genConversion struct {
	generator.GoGenerator
	// 转换函数所针对的类型定义所在包。
	// 转换函数所针对的类型所在的包。
	typesPackage string
	// 转换函数输出到的包。
	// 转换代码输出到的包。
	outputPackage string
	// 与 typesPackage 中类型配对的 peer 包列表。
	// 包含 typesPackage 中类型的 peer 的包。
	peerPackages        []string
	manualConversions   conversionFuncMap
	imports             namer.ImportTracker
	types               []*types.Type
	explicitConversions []conversionPair
	skippedFields       map[*types.Type][]string
	useUnsafe           TypesEqual
}

// NewGenConversion 构造一个转换生成器。
// outputFilename 只是文件名，实际目录由外层 Target 决定。
func NewGenConversion(outputFilename, typesPackage, outputPackage string, manualConversions conversionFuncMap, peerPkgs []string, useUnsafe TypesEqual) generator.Generator {
	return &genConversion{
		GoGenerator: generator.GoGenerator{
			OutputFilename: outputFilename,
		},
		typesPackage:        typesPackage,
		outputPackage:       outputPackage,
		peerPackages:        peerPkgs,
		manualConversions:   manualConversions,
		imports:             generator.NewImportTrackerForPackage(outputPackage),
		types:               []*types.Type{},
		explicitConversions: []conversionPair{},
		skippedFields:       map[*types.Type][]string{},
		useUnsafe:           useUnsafe,
	}
}

// Namers 注册 raw 与 publicIT 两种命名系统；publicIT 在命名时把用到的类型
// 登记进 import 追踪器，保证生成文件里的导入完整。
func (g *genConversion) Namers(c *generator.Context) namer.NameSystems {
	// 让本文件的 raw namer 负责登记 import。
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
		"publicIT": &namerPlusImportTracking{
			delegate: conversionNamer(),
			tracker:  g.imports,
		},
	}
}

// namerPlusImportTracking 在委托命名的同时记录类型，用于生成 import 语句。
type namerPlusImportTracking struct {
	delegate namer.Namer
	tracker  namer.ImportTracker
}

func (n *namerPlusImportTracking) Name(t *types.Type) string {
	n.tracker.AddType(t)
	return n.delegate.Name(t)
}

// convertibleOnlyWithinPackage 判断 inType→outType 是否属于“可在本包自动生成的转换”：
// 二者之一必须在 typesPackage 中，且不是结构体以外的类型、不是私有类型、
// 未通过 +k8s:conversion-gen=false 显式关闭。
func (g *genConversion) convertibleOnlyWithinPackage(inType, outType *types.Type) bool {
	var t *types.Type
	var other *types.Type
	if inType.Name.Package == g.typesPackage {
		t, other = inType, outType
	} else {
		t, other = outType, inType
	}

	if t.Name.Package != g.typesPackage {
		return false
	}
	// 类型显式退出生成时直接跳过。
	tagvals, err := extractTag(t.CommentLines)
	if err != nil {
		klog.Errorf("Type %v: error extracting tags: %v", t, err)
		return false
	}

	if tagvals != nil {
		if tagvals[0] != "false" {
			klog.Fatalf("Type %v: unsupported %s value: %q", t, tagName, tagvals[0])
		}
		klog.V(2).Infof("type %v requests no conversion generation, skipping", t)
		return false
	}
	// TODO: 考虑也为其他类型种类生成转换函数。
	if t.Kind != types.Struct {
		return false
	}
	// 同时过滤掉非导出类型。
	if namer.IsPrivateGoName(other.Name.Name) {
		return false
	}
	return true
}

// getExplicitFromTypes 解析类型上的 explicit-from 标签，返回来源类型列表。
// 目前只支持 net/url.Values，因为该来源能通过 json tag 映射到目标结构体字段。
func getExplicitFromTypes(t *types.Type) []types.Name {
	comments := t.SecondClosestCommentLines
	comments = append(comments, t.CommentLines...)
	result := []types.Name{}
	paths, err := extractExplicitFromTag(comments)
	if err != nil {
		klog.Errorf("Error extracting explicit-from tag for %v: %v", t.Name, err)
		return result
	}
	for _, path := range paths {
		items := strings.Split(path, ".")
		if len(items) != 2 {
			klog.Errorf("Unexpected k8s:conversion-gen:explicit-from tag: %s", path)
			continue
		}
		switch {
		case items[0] == "net/url" && items[1] == "Values":
		default:
			klog.Fatalf("Not supported k8s:conversion-gen:explicit-from tag: %s", path)
		}
		result = append(result, types.Name{Package: items[0], Name: items[1]})
	}
	return result
}

// Filter 决定哪些类型需要生成转换，并顺带收集类型与显式转换对。
// 两类来源：存在 peer 类型且可自动转换的类型，以及声明了 explicit-from 的类型。
func (g *genConversion) Filter(c *generator.Context, t *types.Type) bool {
	// 尝试按 peer 类型生成标准双向转换。
	convertibleWithPeer := func() bool {
		peerType := getPeerTypeFor(c, t, g.peerPackages)
		if peerType == nil {
			return false
		}
		if !g.convertibleOnlyWithinPackage(t, peerType) {
			return false
		}
		g.types = append(g.types, t)
		return true
	}()

	// 若显式声明了来源类型（如 net/url.Values），登记这些转换对。
	explicitlyConvertible := func() bool {
		inTypes := getExplicitFromTypes(t)
		if len(inTypes) == 0 {
			return false
		}
		for i := range inTypes {
			pair := conversionPair{
				inType:  &types.Type{Name: inTypes[i]},
				outType: t,
			}
			g.explicitConversions = append(g.explicitConversions, pair)
		}
		return true
	}()

	return convertibleWithPeer || explicitlyConvertible
}

// isOtherPackage 判断某条 import 行是否指向输出包自身；
// 自身导入必须过滤掉，否则生成代码会出现自引用。
func (g *genConversion) isOtherPackage(pkg string) bool {
	if pkg == g.outputPackage {
		return false
	}
	if strings.HasSuffix(pkg, `"`+g.outputPackage+`"`) {
		return false
	}
	return true
}

// Imports 返回去除自引用后的导入行。
func (g *genConversion) Imports(c *generator.Context) (imports []string) {
	var importLines []string
	for _, singleImport := range g.imports.ImportLines() {
		if g.isOtherPackage(singleImport) {
			importLines = append(importLines, singleImport)
		}
	}
	return importLines
}

// argsFromType 构造模板参数中的 inType/outType。
func argsFromType(inType, outType *types.Type) generator.Args {
	return generator.Args{
		"inType":  inType,
		"outType": outType,
	}
}

// nameTmpl 是转换函数名模板；auto 前缀的版本由 generateConversion 生成。
const nameTmpl = "Convert_$.inType|publicIT$_To_$.outType|publicIT$"

// preexists 查询是否已存在 inType→outType 的手写转换函数。
func (g *genConversion) preexists(inType, outType *types.Type) (*types.Type, bool) {
	function, ok := g.manualConversions[conversionPair{inType, outType}]
	return function, ok
}

// preexistsPointers 是 preexists 的指针版本：仅当两端都是指针时，
// 检查其元素类型之间是否存在手写转换。
func (g *genConversion) preexistsPointers(inType, outType *types.Type) (*types.Type, bool) {
	if inType.Kind != types.Pointer {
		return nil, false
	}
	if outType.Kind != types.Pointer {
		return nil, false
	}
	return g.preexists(inType.Elem, outType.Elem)
}

// Init 输出文件头（init 与 RegisterConversions），把所有转换函数注册到 scheme。
// 只有生成函数（而非手写函数）需要注册；手写函数已由用户自行注册。
func (g *genConversion) Init(c *generator.Context, w io.Writer) error {
	// 高日志级别时打印所有不能走内存拷贝的类型，便于排查 unsafe 优化为何未生效。
	klogV := klog.V(6)
	if klogV.Enabled() {
		if m, ok := g.useUnsafe.(equalMemoryTypes); ok {
			var result []string
			klogV.Info("All objects without identical memory layout:")
			for k, v := range m {
				if v {
					continue
				}
				result = append(result, fmt.Sprintf("  %s -> %s = %t", k.inType, k.outType, v))
			}
			sort.Strings(result)
			for _, s := range result {
				klogV.Info(s)
			}
		}
	}
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	// 注册到 scheme 的 localSchemeBuilder，由生成的 zz_generated.* 提供。
	sw.Do("func init() {\n", nil)
	sw.Do("localSchemeBuilder.Register(RegisterConversions)\n", nil)
	sw.Do("}\n", nil)

	scheme := c.Universe.Type(types.Name{Package: runtimePackagePath, Name: "Scheme"})
	schemePtr := &types.Type{
		Kind: types.Pointer,
		Elem: scheme,
	}
	sw.Do("// RegisterConversions adds conversion functions to the given scheme.\n", nil)
	sw.Do("// Public to allow building arbitrary schemes.\n", nil)
	sw.Do("func RegisterConversions(s $.|raw$) error {\n", schemePtr)
	for _, t := range g.types {
		peerType := getPeerTypeFor(c, t, g.peerPackages)
		// 双向注册；每个方向已有手写函数时跳过，避免覆盖用户实现。
		if _, found := g.preexists(t, peerType); !found {
			args := argsFromType(t, peerType).With("Scope", types.Ref(conversionPackagePath, "Scope"))
			sw.Do("if err := s.AddGeneratedConversionFunc((*$.inType|raw$)(nil), (*$.outType|raw$)(nil), func(a, b interface{}, scope $.Scope|raw$) error { return "+nameTmpl+"(a.(*$.inType|raw$), b.(*$.outType|raw$), scope) }); err != nil { return err }\n", args)
		}
		if _, found := g.preexists(peerType, t); !found {
			args := argsFromType(peerType, t).With("Scope", types.Ref(conversionPackagePath, "Scope"))
			sw.Do("if err := s.AddGeneratedConversionFunc((*$.inType|raw$)(nil), (*$.outType|raw$)(nil), func(a, b interface{}, scope $.Scope|raw$) error { return "+nameTmpl+"(a.(*$.inType|raw$), b.(*$.outType|raw$), scope) }); err != nil { return err }\n", args)
		}
	}

	// 显式来源转换（如 net/url.Values → T）同样注册。
	for i := range g.explicitConversions {
		args := argsFromType(g.explicitConversions[i].inType, g.explicitConversions[i].outType).With("Scope", types.Ref(conversionPackagePath, "Scope"))
		sw.Do("if err := s.AddGeneratedConversionFunc((*$.inType|raw$)(nil), (*$.outType|raw$)(nil), func(a, b interface{}, scope $.Scope|raw$) error { return "+nameTmpl+"(a.(*$.inType|raw$), b.(*$.outType|raw$), scope) }); err != nil { return err }\n", args)
	}

	// 本包内定义的手写转换需要用 AddConversionFunc 注册（优先级高于 generated）。
	var pairs []conversionPair
	for pair, t := range g.manualConversions {
		if t.Name.Package != g.outputPackage {
			continue
		}
		pairs = append(pairs, pair)
	}
	// 按函数名排序，保证多次生成的输出顺序稳定。
	// 按转换函数名排序，保证输出稳定。
	sort.Slice(pairs, func(i, j int) bool {
		return g.manualConversions[pairs[i]].Name.Name < g.manualConversions[pairs[j]].Name.Name
	})
	for _, pair := range pairs {
		args := argsFromType(pair.inType, pair.outType).With("Scope", types.Ref(conversionPackagePath, "Scope")).With("fn", g.manualConversions[pair])
		sw.Do("if err := s.AddConversionFunc((*$.inType|raw$)(nil), (*$.outType|raw$)(nil), func(a, b interface{}, scope $.Scope|raw$) error { return $.fn|raw$(a.(*$.inType|raw$), b.(*$.outType|raw$), scope) }); err != nil { return err }\n", args)
	}

	sw.Do("return nil\n", nil)
	sw.Do("}\n\n", nil)
	return sw.Error()
}

// GenerateType 为单个类型输出转换函数：
// 有 peer 时生成双向转换，有 explicit-from 时生成来源转换。
func (g *genConversion) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	klog.V(5).Infof("generating for type %v", t)
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	if peerType := getPeerTypeFor(c, t, g.peerPackages); peerType != nil {
		// 两个方向都要生成，缺一不可。
		g.generateConversion(t, peerType, sw)
		g.generateConversion(peerType, t, sw)
	}

	for _, inTypeName := range getExplicitFromTypes(t) {
		inPkg, ok := c.Universe[inTypeName.Package]
		if !ok {
			klog.Errorf("Unrecognized package: %s", inTypeName.Package)
			continue
		}
		inType, ok := inPkg.Types[inTypeName.Name]
		if !ok {
			klog.Errorf("Unrecognized type in package %s: %s", inTypeName.Package, inTypeName.Name)
			continue
		}
		switch {
		case inType.Name.Package == "net/url" && inType.Name.Name == "Values":
			g.generateFromURLValues(inType, t, sw)
		default:
			klog.Errorf("Not supported input type: %#v", inType.Name)
		}
	}

	return sw.Error()
}

// generateConversion 生成一对转换函数：总是生成 autoConvert_*，
// 再根据是否存在手写实现决定是否包裹出公开的 Convert_* 函数。
func (g *genConversion) generateConversion(inType, outType *types.Type, sw *generator.SnippetWriter) {
	args := argsFromType(inType, outType).
		With("Scope", types.Ref(conversionPackagePath, "Scope"))

	// 先输出内部实现 autoConvert_*，手写函数也可复用。
	sw.Do("func auto"+nameTmpl+"(in *$.inType|raw$, out *$.outType|raw$, s $.Scope|raw$) error {\n", args)
	g.generateFor(inType, outType, sw)
	sw.Do("return nil\n", nil)
	sw.Do("}\n\n", nil)

	if _, found := g.preexists(inType, outType); found {
		// 已有公开手写转换函数，直接复用，不再生成同名函数。
		// 存在公开的手写转换方法时直接复用。
	} else if skipped := g.skippedFields[inType]; len(skipped) != 0 {
		// inType 有字段无法自动转换，因此不生成公开函数，避免掩盖未处理字段。
		// inType 中有无法自动生成的字段，需手写转换。
		klog.Errorf("Warning: could not find nor generate a final Conversion function for %v -> %v", inType, outType)
		klog.Errorf("  the following fields need manual conversion:")
		for _, f := range skipped {
			klog.Errorf("      - %v", f)
		}
	} else {
		// 无手写实现且无跳过字段时，生成公开的 Convert_* 包裹 autoConvert_*。
		// 输出公开的转换函数。
		sw.Do("// "+nameTmpl+" is an autogenerated conversion function.\n", args)
		sw.Do("func "+nameTmpl+"(in *$.inType|raw$, out *$.outType|raw$, s $.Scope|raw$) error {\n", args)
		sw.Do("return auto"+nameTmpl+"(in, out, s)\n", args)
		sw.Do("}\n\n", nil)
	}
}

// 通过遮蔽 in/out 变量，使同一段代码可嵌套到任意层级而无需改名。
// 模板用层层遮蔽的 in/out 变量，使同一段代码在任意嵌套层级都成立；
// 这样生成逻辑更易读，编译器也不在意。
func (g *genConversion) generateFor(inType, outType *types.Type, sw *generator.SnippetWriter) {
	klog.V(4).Infof("generating %v -> %v", inType, outType)
	var f func(*types.Type, *types.Type, *generator.SnippetWriter)

	// 按输入类型的 Kind 分派到对应的生成逻辑。
	switch inType.Kind {
	case types.Builtin:
		f = g.doBuiltin
	case types.Map:
		f = g.doMap
	case types.Slice:
		f = g.doSlice
	case types.Struct:
		f = g.doStruct
	case types.Pointer:
		f = g.doPointer
	case types.Alias:
		f = g.doAlias
	default:
		f = g.doUnknown
	}

	f(inType, outType, sw)
}

// doBuiltin 处理内建类型：同类型直接赋值，否则显式转换。
func (g *genConversion) doBuiltin(inType, outType *types.Type, sw *generator.SnippetWriter) {
	if inType == outType {
		sw.Do("*out = *in\n", nil)
	} else {
		sw.Do("*out = $.|raw$(*in)\n", outType)
	}
}

// doMap 生成 map 转换：先分配等长的新 map，再逐 key 处理。
// key/value 可分三种情况：直接赋值、调用手写/生成转换函数、无法转换时报 FIXME。
func (g *genConversion) doMap(inType, outType *types.Type, sw *generator.SnippetWriter) {
	sw.Do("*out = make($.|raw$, len(*in))\n", outType)
	if isDirectlyAssignable(inType.Key, outType.Key) {
		sw.Do("for key, val := range *in {\n", nil)
		if isDirectlyAssignable(inType.Elem, outType.Elem) {
			if inType.Key == outType.Key {
				sw.Do("(*out)[key] = ", nil)
			} else {
				sw.Do("(*out)[$.|raw$(key)] = ", outType.Key)
			}
			if inType.Elem == outType.Elem {
				sw.Do("val\n", nil)
			} else {
				sw.Do("$.|raw$(val)\n", outType.Elem)
			}
		} else {
			conversionExists := true
			conditionalConversionExists := false
			if function, ok := g.preexists(inType.Elem, outType.Elem); ok {
				sw.Do("newVal := new($.|raw$)\n", outType.Elem)
				sw.Do("if err := $.|raw$(&val, newVal, s); err != nil {\n", function)
			} else if function, ok := g.preexistsPointers(inType.Elem, outType.Elem); ok {
				sw.Do("newVal := new($.|raw$)\n", outType.Elem)
				sw.Do("if val != nil {\n", nil)
				sw.Do("*newVal = new($.|raw$)\n", outType.Elem.Elem)
				sw.Do("if err := $.|raw$(val, *newVal, s); err != nil {\n", function)
				conditionalConversionExists = true
			} else if g.convertibleOnlyWithinPackage(inType.Elem, outType.Elem) {
				sw.Do("newVal := new($.|raw$)\n", outType.Elem)
				sw.Do("if err := "+nameTmpl+"(&val, newVal, s); err != nil {\n", argsFromType(inType.Elem, outType.Elem))
			} else {
				args := argsFromType(inType.Elem, outType.Elem)
				sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
				sw.Do("compileErrorOnMissingConversion()\n", nil)
				conversionExists = false
			}
			if conversionExists {
				sw.Do("return err\n", nil)
				sw.Do("}\n", nil)
				if conditionalConversionExists {
					sw.Do("}\n", nil)
				}
				if inType.Key == outType.Key {
					sw.Do("(*out)[key] = *newVal\n", nil)
				} else {
					sw.Do("(*out)[$.|raw$(key)] = *newVal\n", outType.Key)
				}
			}
		}
	} else {
		// 当前不支持不可赋值的 key 类型，遇到时输出 FIXME 提示。
		// TODO: 有实际需求时再实现。
		sw.Do("for range *in {\n", nil)
		sw.Do("// FIXME: Converting unassignable keys unsupported $.|raw$\n", inType.Key)
	}
	sw.Do("}\n", nil)
}

// doSlice 生成 slice 转换：先按等长分配，元素可分直接赋值与函数转换两类。
// 对同元素类型的内建切片使用 copy 以提升性能。
func (g *genConversion) doSlice(inType, outType *types.Type, sw *generator.SnippetWriter) {
	sw.Do("*out = make($.|raw$, len(*in))\n", outType)
	if inType.Elem == outType.Elem && inType.Elem.Kind == types.Builtin {
		sw.Do("copy(*out, *in)\n", nil)
	} else {
		sw.Do("for i := range *in {\n", nil)
		if isDirectlyAssignable(inType.Elem, outType.Elem) {
			if inType.Elem == outType.Elem {
				sw.Do("(*out)[i] = (*in)[i]\n", nil)
			} else {
				sw.Do("(*out)[i] = $.|raw$((*in)[i])\n", outType.Elem)
			}
		} else {
			conversionExists := true
			conditionalConversionExists := false
			if function, ok := g.preexists(inType.Elem, outType.Elem); ok {
				sw.Do("if err := $.|raw$(&(*in)[i], &(*out)[i], s); err != nil {\n", function)
			} else if function, ok := g.preexistsPointers(inType.Elem, outType.Elem); ok {
				sw.Do("if (*in)[i] != nil {\n", nil)
				sw.Do("(*out)[i] = new($.|raw$)\n", outType.Elem.Elem)
				sw.Do("if err := $.|raw$((*in)[i], (*out)[i], s); err != nil {\n", function)
				conditionalConversionExists = true
			} else if g.convertibleOnlyWithinPackage(inType.Elem, outType.Elem) {
				sw.Do("if err := "+nameTmpl+"(&(*in)[i], &(*out)[i], s); err != nil {\n", argsFromType(inType.Elem, outType.Elem))
			} else {
				args := argsFromType(inType.Elem, outType.Elem)
				sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
				sw.Do("compileErrorOnMissingConversion()\n", nil)
				conversionExists = false
			}
			if conversionExists {
				sw.Do("return err\n", nil)
				sw.Do("}\n", nil)
				if conditionalConversionExists {
					sw.Do("}\n", nil)
				}
			}
		}
		sw.Do("}\n", nil)
	}
}

// doStruct 生成结构体转换。若两个结构体内存布局等价且无需手写转换，
// 直接用一次 unsafe 内存拷贝；否则逐字段生成。
func (g *genConversion) doStruct(inType, outType *types.Type, sw *generator.SnippetWriter) {
	ok, err := g.canUseMemoryCopyConversion(inType, outType)
	if err != nil {
		klog.Errorf("Type %v: error checking for direct-copy conversion: %v", inType, err)
	}
	if ok {
		args := argsFromType(inType, outType).
			With("Pointer", types.Ref("unsafe", "Pointer"))
		sw.Do("*out = *(*$.outType|raw$)($.Pointer|raw$(in))\n", args)
		return
	}
	for _, inMember := range inType.Members {
		tagvals, err := extractTag(inMember.CommentLines)
		if err != nil {
			klog.Errorf("Member %v.%v: error extracting tags: %v", inType, inMember.Name, err)
		}
		if tagvals != nil && tagvals[0] == "false" {
			// 字段显式声明跳过转换。
			// 该字段不参与转换。
			sw.Do("// INFO: in."+inMember.Name+" opted out of conversion generation\n", nil)
			continue
		}
		outMember, found := findMember(outType, inMember.Name)
		if !found {
			// peer 类型没有同名字段，无法自动转换，记录跳过字段并提示人工处理。
			// peer 类型中没有该字段。
			sw.Do("// WARNING: in."+inMember.Name+" requires manual conversion: does not exist in peer-type\n", nil)
			g.skippedFields[inType] = append(g.skippedFields[inType], inMember.Name)
			continue
		}

		// 跨包时无法访问私有字段，只能跳过并要求人工处理。
		if namer.IsPrivateGoName(inMember.Name) && g.outputPackage != inType.Name.Package {
			sw.Do("// WARNING: in."+inMember.Name+" is not exported and cannot be read\n", nil)
			g.skippedFields[inType] = append(g.skippedFields[inType], inMember.Name)
			continue
		}
		if namer.IsPrivateGoName(outMember.Name) && g.outputPackage != outType.Name.Package {
			sw.Do("// WARNING: out."+inMember.Name+" is not exported and cannot be set\n", nil)
			g.skippedFields[inType] = append(g.skippedFields[inType], inMember.Name)
			continue
		}

		inMemberType, outMemberType := inMember.Type, outMember.Type
		// 为两端创建底层类型的副本，但保留顶层别名（别名可赋值），
		// 这样后续既能识别别名，又能比较实际布局。
		// 复制两个底层类型但保留顶层别名：别名之间可直接赋值。
		if underlying := unwrapAlias(inMemberType); underlying != inMemberType {
			copied := *underlying
			copied.Name = inMemberType.Name
			inMemberType = &copied
		}
		if underlying := unwrapAlias(outMemberType); underlying != outMemberType {
			copied := *underlying
			copied.Name = outMemberType.Name
			outMemberType = &copied
		}

		args := argsFromType(inMemberType, outMemberType).With("name", inMember.Name)

		// 对值完全等价的类型尝试直接内存拷贝（指针/map/slice）。
		// 值完全等价时尝试直接内存拷贝。
		if g.useUnsafe.Equal(inMemberType, outMemberType) {
			args = args.
				With("Pointer", types.Ref("unsafe", "Pointer")).
				With("SliceHeader", types.Ref("reflect", "SliceHeader"))
			switch inMemberType.Kind {
			case types.Pointer:
				sw.Do("out.$.name$ = ($.outType|raw$)($.Pointer|raw$(in.$.name$))\n", args)
				continue
			case types.Map:
				sw.Do("out.$.name$ = *(*$.outType|raw$)($.Pointer|raw$(&in.$.name$))\n", args)
				continue
			case types.Slice:
				sw.Do("out.$.name$ = *(*$.outType|raw$)($.Pointer|raw$(&in.$.name$))\n", args)
				continue
			}
		}

		// 按顶层类型名判断是否存在手写函数，而不是按底层名。
		// 按顶层类型名判断，而不是底层类型名。
		if function, ok := g.preexists(inMember.Type, outMember.Type); ok {
			dropFn, err := isDrop(function.CommentLines)
			if err != nil {
				klog.Errorf("Error extracting drop tag for function %s: %v", function.Name, err)
			} else if dropFn {
				continue
			}
			// copy-only 且可直接赋值的函数可以内联为赋值，而不调用函数。
			// 例如让带私有字段的类型正确拷贝的转换函数，等价于内存赋值：
			// 反射路径需要它，但它不应阻止内存级转换。
			// 仅做直接赋值的 copy-only 函数可以内联而非调用。
			// 例如某些含私有字段的类型必须靠转换函数正确拷贝；这些函数等价于内存赋值，
			// 反射路径需要它们，但不应因此阻塞内存拷贝。
			// Convert_unversioned_Time_to_unversioned_Time 即是一例。
			copyOnly, copyErr := isCopyOnly(function.CommentLines)
			if copyErr != nil {
				klog.Errorf("Error extracting copy-only tag for function %s: %v", function.Name, copyErr)
				copyOnly = false
			}
			if !copyOnly || !g.isFastConversion(inMemberType, outMemberType) {
				args["function"] = function
				sw.Do("if err := $.function|raw$(&in.$.name$, &out.$.name$, s); err != nil {\n", args)
				sw.Do("return err\n", nil)
				sw.Do("}\n", nil)
				continue
			}
			klog.V(2).Infof("Skipped function %s because it is copy-only and we can use direct assignment", function.Name)
		}

		// 无法自动转换时，在输出任何代码前先退出，避免生成不完整的转换逻辑。
		// 无法自动转换时直接放弃，不输出半成品代码。
		if inMemberType.Kind != outMemberType.Kind {
			sw.Do("// WARNING: in."+inMember.Name+" requires manual conversion: inconvertible types ("+
				inMemberType.String()+" vs "+outMemberType.String()+")\n", nil)
			g.skippedFields[inType] = append(g.skippedFields[inType], inMember.Name)
			continue
		}

		switch inMemberType.Kind {
		case types.Builtin:
			if inMemberType == outMemberType {
				sw.Do("out.$.name$ = in.$.name$\n", args)
			} else {
				sw.Do("out.$.name$ = $.outType|raw$(in.$.name$)\n", args)
			}
		case types.Map, types.Slice, types.Pointer:
			if g.isDirectlyAssignable(inMemberType, outMemberType) {
				sw.Do("out.$.name$ = in.$.name$\n", args)
				continue
			}

			sw.Do("if in.$.name$ != nil {\n", args)
			sw.Do("in, out := &in.$.name$, &out.$.name$\n", args)
			g.generateFor(inMemberType, outMemberType, sw)
			sw.Do("} else {\n", nil)
			sw.Do("out.$.name$ = nil\n", args)
			sw.Do("}\n", nil)
		case types.Struct:
			if g.isDirectlyAssignable(inMemberType, outMemberType) {
				sw.Do("out.$.name$ = in.$.name$\n", args)
				continue
			}
			conversionExists := true
			if g.convertibleOnlyWithinPackage(inMemberType, outMemberType) {
				sw.Do("if err := "+nameTmpl+"(&in.$.name$, &out.$.name$, s); err != nil {\n", args)
			} else {
				args := argsFromType(inMemberType, outMemberType)
				sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
				sw.Do("compileErrorOnMissingConversion()\n", nil)
				conversionExists = false
			}
			if conversionExists {
				sw.Do("return err\n", nil)
				sw.Do("}\n", nil)
			}
		case types.Alias:
			if isDirectlyAssignable(inMemberType, outMemberType) {
				if inMemberType == outMemberType {
					sw.Do("out.$.name$ = in.$.name$\n", args)
				} else {
					sw.Do("out.$.name$ = $.outType|raw$(in.$.name$)\n", args)
				}
			} else {
				conversionExists := true
				if g.convertibleOnlyWithinPackage(inMemberType, outMemberType) {
					sw.Do("if err := "+nameTmpl+"(&in.$.name$, &out.$.name$, s); err != nil {\n", args)
				} else {
					args := argsFromType(inMemberType, outMemberType)
					sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
					sw.Do("compileErrorOnMissingConversion()\n", nil)
					conversionExists = false
				}
				if conversionExists {
					sw.Do("return err\n", nil)
					sw.Do("}\n", nil)
				}
			}
		default:
			conversionExists := true
			if g.convertibleOnlyWithinPackage(inMemberType, outMemberType) {
				sw.Do("if err := "+nameTmpl+"(&in.$.name$, &out.$.name$, s); err != nil {\n", args)
			} else {
				args := argsFromType(inMemberType, outMemberType)
				sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
				sw.Do("compileErrorOnMissingConversion()\n", nil)
				conversionExists = false
			}
			if conversionExists {
				sw.Do("return err\n", nil)
				sw.Do("}\n", nil)
			}
		}
	}
}

// canUseMemoryCopyConversion 判断两个结构体能否用一次 unsafe 内存拷贝完成转换。
// 仅当布局完全一致，且不存在手写转换（copy-only 除外）时返回 true，
// 同时排除被 conversion-gen 关闭、私有或缺失的字段。
// canUseMemoryCopyConversion 报告两个结构体能否用一次 unsafe 内存拷贝替代逐字段转换。
// 仅当内存布局完全一致、且没有手写转换（copy-only 除外）时才返回 true。
func (g *genConversion) canUseMemoryCopyConversion(inType, outType *types.Type) (bool, error) {
	if !g.useUnsafe.Equal(inType, outType) {
		return false, nil
	}
	for _, inMember := range inType.Members {
		tagvals, err := extractTag(inMember.CommentLines)
		if err != nil {
			return false, err
		}
		if len(tagvals) > 0 && tagvals[0] == "false" {
			return false, nil // opted out of conversion-gen
		}
		outMember, found := findMember(outType, inMember.Name)
		if !found {
			return false, nil
		}

		if namer.IsPrivateGoName(inMember.Name) && g.outputPackage != inType.Name.Package {
			return false, nil
		}
		if namer.IsPrivateGoName(outMember.Name) && g.outputPackage != outType.Name.Package {
			return false, nil
		}
		// 存在 copy-only 之外的手写转换时放弃内存拷贝。
		if function, ok := g.preexists(inMember.Type, outMember.Type); ok {
			copyOnly, err := isCopyOnly(function.CommentLines)
			if err != nil || !copyOnly {
				return false, err
			}
		}
	}
	return true, nil
}

// isFastConversion 判断 copy-only 函数能否被等价的内联赋值替代。
// 内建类型总是可以；其余类型要求底层类型一致（可赋值）。
func (g *genConversion) isFastConversion(inType, outType *types.Type) bool {
	switch inType.Kind {
	case types.Builtin:
		return true
	case types.Map, types.Slice, types.Pointer, types.Struct, types.Alias:
		return g.isDirectlyAssignable(inType, outType)
	default:
		return false
	}
}

// isDirectlyAssignable 是方法版的可赋值判定：比较解别名后的类型是否相同。
func (g *genConversion) isDirectlyAssignable(inType, outType *types.Type) bool {
	return unwrapAlias(inType) == unwrapAlias(outType)
}

// doPointer 生成指针转换：先分配新对象，再对元素做直接赋值或函数转换。
func (g *genConversion) doPointer(inType, outType *types.Type, sw *generator.SnippetWriter) {
	sw.Do("*out = new($.Elem|raw$)\n", outType)
	if isDirectlyAssignable(inType.Elem, outType.Elem) {
		if inType.Elem == outType.Elem {
			sw.Do("**out = **in\n", nil)
		} else {
			sw.Do("**out = $.|raw$(**in)\n", outType.Elem)
		}
	} else {
		conversionExists := true
		if function, ok := g.preexists(inType.Elem, outType.Elem); ok {
			sw.Do("if err := $.|raw$(*in, *out, s); err != nil {\n", function)
		} else if g.convertibleOnlyWithinPackage(inType.Elem, outType.Elem) {
			sw.Do("if err := "+nameTmpl+"(*in, *out, s); err != nil {\n", argsFromType(inType.Elem, outType.Elem))
		} else {
			args := argsFromType(inType.Elem, outType.Elem)
			sw.Do("// FIXME: Provide conversion function to convert $.inType|raw$ to $.outType|raw$\n", args)
			sw.Do("compileErrorOnMissingConversion()\n", nil)
			conversionExists = false
		}
		if conversionExists {
			sw.Do("return err\n", nil)
			sw.Do("}\n", nil)
		}
	}
}

// doAlias 暂不支持别名，退化为输出 FIXME。
func (g *genConversion) doAlias(inType, outType *types.Type, sw *generator.SnippetWriter) {
	// TODO: 增加对别名的支持。
	g.doUnknown(inType, outType, sw)
}

// doUnknown 对无法识别的类型输出 FIXME 注释，提示需要手写转换。
func (g *genConversion) doUnknown(inType, outType *types.Type, sw *generator.SnippetWriter) {
	sw.Do("// FIXME: Type $.|raw$ is unsupported.\n", inType)
}

// generateFromURLValues 生成从 net/url.Values 到结构体的转换。
// 借助字段的 json tag 作为 key 取第一个值；缺省时写入零值。
func (g *genConversion) generateFromURLValues(inType, outType *types.Type, sw *generator.SnippetWriter) {
	args := generator.Args{
		"inType":  inType,
		"outType": outType,
		"Scope":   types.Ref(conversionPackagePath, "Scope"),
	}
	sw.Do("func auto"+nameTmpl+"(in *$.inType|raw$, out *$.outType|raw$, s $.Scope|raw$) error {\n", args)
	for _, outMember := range outType.Members {
		tagvals, err := extractTag(outMember.CommentLines)
		if err != nil {
			klog.Errorf("Member %v.%v: error extracting tags: %v", outType, outMember.Name, err)
		}
		if tagvals != nil && tagvals[0] == "false" {
			// 该字段不参与转换。
			sw.Do("// INFO: in."+outMember.Name+" opted out of conversion generation\n", nil)
			continue
		}
		jsonTag := reflect.StructTag(outMember.Tags).Get("json")
		// 只取 json tag 的 name 部分（逗号前的部分）。
		index := strings.Index(jsonTag, ",")
		if index == -1 {
			index = len(jsonTag)
		}
		// 没有 json tag 的字段无法从 url.Values 映射，跳过并提示。
		if index == 0 {
			memberArgs := generator.Args{
				"name": outMember.Name,
			}
			sw.Do("// WARNING: Field $.name$ does not have json tag, skipping.\n\n", memberArgs)
			continue
		}
		memberArgs := generator.Args{
			"name": outMember.Name,
			"tag":  jsonTag[:index],
		}
		sw.Do("if values, ok := map[string][]string(*in)[\"$.tag$\"]; ok && len(values) > 0 {\n", memberArgs)
		g.fromValuesEntry(inType.Underlying.Elem, outMember, sw)
		sw.Do("} else {\n", nil)
		g.setZeroValue(outMember, sw)
		sw.Do("}\n", nil)
	}
	sw.Do("return nil\n", nil)
	sw.Do("}\n\n", nil)

	if _, found := g.preexists(inType, outType); found {
		// 存在公开的手写转换方法时直接复用。
	} else {
		// 输出公开的转换函数。
		sw.Do("// "+nameTmpl+" is an autogenerated conversion function.\n", args)
		sw.Do("func "+nameTmpl+"(in *$.inType|raw$, out *$.outType|raw$, s $.Scope|raw$) error {\n", args)
		sw.Do("return auto"+nameTmpl+"(in, out, s)\n", args)
		sw.Do("}\n\n", nil)
	}
}

// fromValuesEntry 为一个目标字段生成从 url.Values 取值的代码：
// 优先手写函数，其次直接赋值（字符串）或内存拷贝（等价类型）。
func (g *genConversion) fromValuesEntry(inType *types.Type, outMember types.Member, sw *generator.SnippetWriter) {
	memberArgs := generator.Args{
		"name": outMember.Name,
		"type": outMember.Type,
	}
	if function, ok := g.preexists(inType, outMember.Type); ok {
		args := memberArgs.With("function", function)
		sw.Do("if err := $.function|raw$(&values, &out.$.name$, s); err != nil {\n", args)
		sw.Do("return err\n", nil)
		sw.Do("}\n", nil)
		return
	}
	switch {
	case outMember.Type == types.String:
		sw.Do("out.$.name$ = values[0]\n", memberArgs)
	case g.useUnsafe.Equal(inType, outMember.Type):
		args := memberArgs.With("Pointer", types.Ref("unsafe", "Pointer"))
		switch inType.Kind {
		case types.Pointer:
			sw.Do("out.$.name$ = ($.type|raw$)($.Pointer|raw$(&values))\n", args)
		case types.Map, types.Slice:
			sw.Do("out.$.name$ = *(*$.type|raw$)($.Pointer|raw$(&values))\n", args)
		default:
			// TODO: 支持更多类型以扩大自动转换的覆盖面。
			sw.Do("// FIXME: out.$.name$ is of not yet supported type and requires manual conversion\n", memberArgs)
		}
	default:
		// TODO: 支持更多类型以扩大自动转换的覆盖面。
		sw.Do("// FIXME: out.$.name$ is of not yet supported type and requires manual conversion\n", memberArgs)
	}
}

// setZeroValue 为 url.Values 中缺失的字段生成零值赋值。
// 先解别名，再按底层 Kind 输出合法的零值字面量。
func (g *genConversion) setZeroValue(outMember types.Member, sw *generator.SnippetWriter) {
	outMemberType := unwrapAlias(outMember.Type)
	memberArgs := generator.Args{
		"name":  outMember.Name,
		"alias": outMember.Type,
		"type":  outMemberType,
	}

	switch outMemberType.Kind {
	case types.Builtin:
		switch outMemberType {
		case types.String:
			sw.Do("out.$.name$ = \"\"\n", memberArgs)
		case types.Int64, types.Int32, types.Int16, types.Int, types.Uint64, types.Uint32, types.Uint16, types.Uint:
			sw.Do("out.$.name$ = 0\n", memberArgs)
		case types.Uintptr, types.Byte:
			sw.Do("out.$.name$ = 0\n", memberArgs)
		case types.Float64, types.Float32, types.Float:
			sw.Do("out.$.name$ = 0\n", memberArgs)
		case types.Bool:
			sw.Do("out.$.name$ = false\n", memberArgs)
		default:
			sw.Do("// FIXME: out.$.name$ is of unsupported type and requires manual conversion\n", memberArgs)
		}
	case types.Struct:
		if outMemberType == outMember.Type {
			sw.Do("out.$.name$ = $.type|raw${}\n", memberArgs)
		} else {
			sw.Do("out.$.name$ = $.alias|raw$($.type|raw${})\n", memberArgs)
		}
	case types.Map, types.Slice, types.Pointer:
		sw.Do("out.$.name$ = nil\n", memberArgs)
	case types.Alias:
		// outMemberType 早已剥去别名，理论上不会走到这里。
		sw.Do("// FIXME: unexpected error for out.$.name$\n", memberArgs)
	case types.Interface, types.Array:
		sw.Do("out.$.name$ = nil\n", memberArgs)
	default:
		sw.Do("// FIXME: out.$.name$ is of unsupported type and requires manual conversion\n", memberArgs)
	}
}

// isDirectlyAssignable 判断 inType 能否直接赋值给 outType：
// 内建类型只要可赋值即可；非内建类型还要求同包（不同包的同名类型在 Go 中不可赋值）。
func isDirectlyAssignable(inType, outType *types.Type) bool {
	// TODO: 这里应真正判断两类型是否可赋值，而不是只看当前用法下
	// 恰好能指示可赋值性的表面特征。
	return inType.IsAssignable() && (inType.IsPrimitive() || isSamePackage(inType, outType))
}

// isSamePackage 判断两个类型是否定义在同一个包中。
func isSamePackage(inType, outType *types.Type) bool {
	return inType.Name.Package == outType.Name.Package
}
