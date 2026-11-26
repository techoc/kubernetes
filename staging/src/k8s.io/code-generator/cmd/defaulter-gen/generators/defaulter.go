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

// Package generators 实现 defaulter-gen 的代码生成：为标注了
// 指令 +k8s:defaulter-gen 的类型生成 SetDefaults_/SetObjectDefaults_ 函数与
// RegisterDefaults 注册函数。
//
// 生成策略的关键约束：
//   - 每个类型的默认值由手写或生成的 SetDefaults_<Type> 提供，Object 级函数
//     会递归调用子类型的 defaulter；
//   - 递归调用按“访问路径”建模（字段 / 指针解引用 / 切片索引 / map 键），
//     因此同一 defaulter 可被嵌套结构体、切片元素、map 值复用；
//   - 生成结果必须与手写 defaulter 共存，手写的优先且不会被覆盖。
package generators

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/code-generator/cmd/defaulter-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	genutil "k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// typeZeroValue 给出各类型在 JSON 层面的零值。
// 生成器用它判断 +default 标注是否等于零值：等于零值的标注无需生成代码，
// 从而避免产生大量无意义的 if 分支。数值统一用 float64（JSON 数字的解码类型），
// time.Time 与 []byte 在 JSON 中是字符串。
var typeZeroValue = map[string]interface{}{
	"uint":        0.,
	"uint8":       0.,
	"uint16":      0.,
	"uint32":      0.,
	"uint64":      0.,
	"int":         0.,
	"int8":        0.,
	"int16":       0.,
	"int32":       0.,
	"int64":       0.,
	"byte":        0.,
	"float64":     0.,
	"float32":     0.,
	"bool":        false,
	"time.Time":   "",
	"string":      "",
	"integer":     0.,
	"number":      0.,
	"boolean":     false,
	"[]byte":      "", // base64 encoded characters
	"interface{}": interface{}(nil),
}

// 下面两个常量是承载 defaulter 生成参数的注释标签。
const tagName = "k8s:defaulter-gen"
const defaultTagName = "default"

// extractDefaultTag 从注释中提取 +default=... 的值。
func extractDefaultTag(comments []string) ([]string, error) {
	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{defaultTagName}, comments)
	if err != nil {
		return nil, err
	}
	return tags[defaultTagName], nil
}

// extractTag 提取 +k8s:defaulter-gen 标签的值，并返回是否存在该标签。
// 标签解析失败属于生成器内部错误，直接 fatal，因为继续生成会产生错误代码。
func extractTag(comments []string) ([]string, bool) {
	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{tagName}, comments)
	if err != nil {
		klog.Fatalf("Error extracting %s tag: %v", tagName, err)
	}

	values, found := tags[tagName]
	if !found || len(values) == 0 {
		return nil, false
	}

	return values, true
}

// defaulterMatchType 返回 pkg 中需要生成 defaulter 的字段名过滤列表；
// 若该包不应运行 defaulter-gen 则返回 false。
func defaulterMatchType(pkg *types.Package, idOpts []apidefinitions.Option) ([]string, bool) {
	info, err := apidefinitions.Identify(pkg, apidefinitions.Defaulter, idOpts...)
	if err != nil {
		klog.Fatal(err)
	}
	if !info.ShouldGenerate() {
		return nil, false
	}
	return info.TypeFilters(), true
}

// checkTag 判断标签值是否与 require 完全一致。
// require 为空时表示要求标签存在但无参数（即值为空字符串）。
func checkTag(comments []string, require ...string) (bool, error) {
	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{tagName}, comments)
	if err != nil {
		return false, err
	}

	if len(require) == 0 {
		return len(tags[tagName]) == 1 && tags[tagName][0] == "", nil
	}
	return reflect.DeepEqual(tags[tagName], require), nil
}

// defaultFnNamer 生成 SetDefaults_<Type> 形式的函数名。
// 与手写函数共用同一命名规则，才能在已有函数映射中正确匹配。
func defaultFnNamer() *namer.NameStrategy {
	return &namer.NameStrategy{
		Prefix: "SetDefaults_",
		Join: func(pre string, in []string, post string) string {
			return pre + strings.Join(in, "_") + post
		},
	}
}

// objectDefaultFnNamer 生成 SetObjectDefaults_<Type> 形式的函数名。
func objectDefaultFnNamer() *namer.NameStrategy {
	return &namer.NameStrategy{
		Prefix: "SetObjectDefaults_",
		Join: func(pre string, in []string, post string) string {
			return pre + strings.Join(in, "_") + post
		},
	}
}

// NameSystems 返回本包生成器使用的命名系统。
// defaultfn/objectdefaultfn 两个系统只用于拼接函数名，不参与类型名渲染。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public":          namer.NewPublicNamer(1),
		"raw":             namer.NewRawNamer("", nil),
		"defaultfn":       defaultFnNamer(),
		"objectdefaultfn": objectDefaultFnNamer(),
	}
}

// DefaultNameSystem 返回按类型排序处理时使用的命名系统。
func DefaultNameSystem() string {
	return "public"
}

// defaults 汇总某个类型已声明的全部默认化函数（它们都应为单参数 func）。
type defaults struct {
	// object 是顶层类型的 defaulter（通常带有 TypeMeta），会调用所有子 defaulter。
	// 若对象级 defaulter 尚未生成，则可能为 nil。
	object *types.Type
	// base 是 SetDefaults_Pod 这类只处理自身字段、不调用子 defaulter 的函数；
	// 仅有 base 不足以完成整个类型的默认化。
	base *types.Type
	// additional 是零个或多个 SetDefaults_Pod_XXXX 形式的函数，
	// 可被包含进 object defaulter 中。
	additional []*types.Type
}

// defaulterFuncMap 中的类型都是 Kind 为 Func 的 DeclarationOf。
type defaulterFuncMap map[*types.Type]defaults

// getManualDefaultingFunctions 收集包内所有手写的默认化函数，写入 manualMap。
// 手写函数必须被识别出来，否则生成器会重复生成同名函数或忽略用户意图。
func getManualDefaultingFunctions(context *generator.Context, pkg *types.Package, manualMap defaulterFuncMap) {
	buffer := &bytes.Buffer{}
	sw := generator.NewSnippetWriter(buffer, context, "$", "$")

	for _, f := range pkg.Functions {
		// 防御畸形类型信息：没有底层 Func 或签名的函数无法判断用途，直接跳过并告警。
		if f.Underlying == nil || f.Underlying.Kind != types.Func {
			klog.Errorf("Malformed function: %#v", f)
			continue
		}
		if f.Underlying.Signature == nil {
			klog.Errorf("Function without signature: %#v", f)
			continue
		}
		signature := f.Underlying.Signature
		// 判断是否为默认化函数。它们都应具有以下签名：
		// object: func SetObjectDefaults_inType(*inType)
		// base: func SetDefaults_inType(*inType)
		// additional: func SetDefaults_inType_Qualifier(*inType)
		// 因此必须是：无接收者、单参数、无返回值、参数为指针。
		if signature.Receiver != nil {
			continue
		}
		if len(signature.Parameters) != 1 {
			continue
		}
		if len(signature.Results) != 0 {
			continue
		}
		inType := signature.Parameters[0].Type
		if inType.Kind != types.Pointer {
			continue
		}
		// 判断是否为主 defaulter（SetDefaults_<Type>）。
		args := defaultingArgsFromType(inType.Elem)
		sw.Do("$.inType|defaultfn$", args)
		switch {
		case f.Name.Name == buffer.String():
			key := inType.Elem
			// 同一包可能被扫描多次，这是允许的；但不同包定义同名 defaulter 属于冲突。
			v, ok := manualMap[key]
			if ok && v.base != nil && v.base.Name.Package != pkg.Path {
				panic(fmt.Sprintf("duplicate static defaulter defined: %#v", key))
			}
			v.base = f
			manualMap[key] = v
			klog.V(6).Infof("found base defaulter function for %s from %s", key.Name, f.Name)
		// 判断是否为附加 defaulter（SetDefaults_<Type>_<Qualifier>）。
		case strings.HasPrefix(f.Name.Name, buffer.String()+"_"):
			key := inType.Elem
			v, ok := manualMap[key]
			if ok {
				// 去重：同一函数可能因多次扫描而重复出现。
				exists := false
				for _, existing := range v.additional {
					if existing.Name == f.Name {
						exists = true
						break
					}
				}
				if exists {
					continue
				}
			}
			v.additional = append(v.additional, f)
			manualMap[key] = v
			klog.V(6).Infof("found additional defaulter function for %s from %s", key.Name, f.Name)
		}
		buffer.Reset()
		// 判断是否为对象级 defaulter（SetObjectDefaults_<Type>）。
		sw.Do("$.inType|objectdefaultfn$", args)
		if f.Name.Name == buffer.String() {
			key := inType.Elem
			// 同上，重复扫描同一包是允许的。
			v, ok := manualMap[key]
			if ok && v.base != nil && v.base.Name.Package != pkg.Path {
				panic(fmt.Sprintf("duplicate static defaulter defined: %#v", key))
			}
			v.object = f
			manualMap[key] = v
			klog.V(6).Infof("found object defaulter function for %s from %s", key.Name, f.Name)
		}
		buffer.Reset()
	}
}

// GetTargets 为每个需要生成 defaulter 的输入包构造生成目标。
// 关键步骤：先收集手写函数与输入包（含 +k8s:defaulter-gen-input 指向的外部类型包），
// 再决定哪些类型值得生成对象级 defaulter，最后为每个包产出一个 SimpleTarget。
func GetTargets(context *generator.Context, args *args.Args) []generator.Target {
	boilerplate, err := gengo.GoBoilerplate(args.GoHeaderFile, args.GeneratedBuildTag, gengo.StdGeneratedBy)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	var idOpts []apidefinitions.Option
	if len(args.LintRules) > 0 {
		idOpts = append(idOpts, apidefinitions.WithLintRules(args.LintRules...))
	}

	targetList := []generator.Target{}

	// 累积已存在的默认化函数。
	// TODO: 这里过于临时，需要更系统的做法。
	existingDefaulters := defaulterFuncMap{}

	buffer := &bytes.Buffer{}
	sw := generator.NewSnippetWriter(buffer, context, "$", "$")

	// 先一次性加载其他“输入”包：批量加载比逐包加载快得多。
	inputPkgs := make([]string, 0, len(context.Inputs))
	pkgToInput := map[string]string{}
	for _, i := range context.Inputs {
		klog.V(5).Infof("considering pkg %q", i)
		pkg := context.Universe[i]

		info, err := apidefinitions.Identify(pkg, apidefinitions.Defaulter, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			continue
		}

		// 关键步骤：先收集手写函数与输入包（含 +k8s:defaulter-gen-input 指向的外部类型包），
		inputPath := info.ExternalTypes()
		if inputPath != pkg.Path {
			klog.V(5).Infof("  input pkg %v", inputPath)
			inputPkgs = append(inputPkgs, inputPath)
			pkgToInput[i] = inputPath
		} else {
			pkgToInput[i] = i
		}
	}

	// 确保显式指定的 peer 包被加载。
	var peerPkgs []string
	for _, pkg := range args.ExtraPeerDirs {
		// 若 peer 被指定为 vendor 下的路径，转换为真实包路径。
		if i := strings.Index(pkg, "/vendor/"); i != -1 {
			pkg = pkg[i+len("/vendor/"):]
		}
		peerPkgs = append(peerPkgs, pkg)
	}
	if expanded, err := context.FindPackages(peerPkgs...); err != nil {
		klog.Fatalf("cannot find peer packages: %v", err)
	} else {
		peerPkgs = expanded // 现在是完全规范化形式
	}
	inputPkgs = append(inputPkgs, peerPkgs...)

	if len(inputPkgs) > 0 {
		if _, err := context.LoadPackages(inputPkgs...); err != nil {
			klog.Fatalf("cannot load packages: %v", err)
		}
	}
	// 把 context.Order 更新到最新的 context.Universe。
	// 顺序决定生成顺序与类型处理次序，未刷新会遗漏刚加载的包。
	orderer := namer.Orderer{Namer: namer.NewPublicNamer(1)}
	context.Order = orderer.OrderUniverse(context.Universe)

	for _, i := range context.Inputs {
		pkg := context.Universe[i]

		// typesPkg 是“需要 defaulter 的类型”所在的包。
		// 它有时与 pkg 不同：例如 kubernetes 的 core/v1 类型定义在 k8s.io/api/core/v1，
		// 而 defaulter 代码位于 k/k/pkg/api/v1。
		typesPkg := pkg

		// 收集手写默认化函数。
		getManualDefaultingFunctions(context, pkg, existingDefaulters)

		// 同样收集 peer 包中的手写函数。
		for _, pp := range peerPkgs {
			getManualDefaultingFunctions(context, context.Universe[pp], existingDefaulters)
		}

		typesWith, found := defaulterMatchType(pkg, idOpts)
		if !found {
			klog.V(2).InfoS("  did not find required tag", "tag", tagName)
			continue
		}
		shouldCreateObjectDefaulterFn := func(t *types.Type) bool {
			if defaults, ok := existingDefaulters[t]; ok && defaults.object != nil {
				// 已存在手写对象级 defaulter，生成会与之重复。
				baseTypeName := "<unknown>"
				if defaults.base != nil {
					baseTypeName = defaults.base.Name.String()
				}
				klog.V(5).Infof("  an object defaulter already exists as %s", baseTypeName)
				return false
			}
			// 退出生成。
			optOut, err := checkTag(t.SecondClosestCommentLines, "false")
			if err != nil {
				klog.Fatalf("Error extracting %s tags: %v", tagName, err)
			}
			if optOut {
				return false
			}
			// 显式要求生成。
			optIn, err := checkTag(t.SecondClosestCommentLines, "true")
			if err != nil {
				klog.Fatalf("Error extracting %s tags: %v", tagName, err)
			}
			if optIn {
				return true
			}
			// 包级 k8s:defaulter-gen 标签的每个值被解释为字段名（如 TypeMeta、
			// ListMeta、ObjectMeta）；凡含有这些字段的类型都需要生成 defaulter。
			// 这比全局开关更实用：通常只有带 TypeMeta 的对象才需要默认化。
			if t.Kind == types.Struct && len(typesWith) > 0 {
				for _, field := range t.Members {
					for _, s := range typesWith {
						if field.Name == s {
							return true
						}
					}
				}
			}
			return false
		}

		// 找到正确的输入包，它可能不是当前包。
		inputPath := pkgToInput[i]
		typesPkg = context.Universe[inputPath]

		newDefaulters := defaulterFuncMap{}
		for _, t := range typesPkg.Types {
			if !shouldCreateObjectDefaulterFn(t) {
				continue
			}
			if namer.IsPrivateGoName(t.Name.Name) {
				// 私有类型无法被外部包引用，生成的方法也无法被调用。
				klog.V(5).Infof("  found a type %v, but it is a private name", t)
				continue
			}

			// 创建占位类型，稍后填充 object defaulter。
			newDefaulters[t] = defaults{}
		}

		// 只为确实存在子 defaulter 的对象生成 defaulter，避免注册空函数。
		// 循环反复推进：某类型生成后可能使其父类型也具备生成条件，直到不再变化。
		for {
			promoted := 0
			for t, d := range newDefaulters {
				if d.object != nil {
					continue
				}
				if newCallTreeForType(existingDefaulters, newDefaulters).build(t, true) != nil {
					args := defaultingArgsFromType(t)
					sw.Do("$.inType|objectdefaultfn$", args)
					newDefaulters[t] = defaults{
						object: &types.Type{
							Name: types.Name{
								Package: pkg.Path,
								Name:    buffer.String(),
							},
							Kind: types.Func,
						},
					}
					buffer.Reset()
					promoted++
				}
			}
			if promoted != 0 {
				continue
			}

			// 清理最终没有任何子 defaulter 的类型。
			for t, d := range newDefaulters {
				if d.object == nil {
					klog.V(6).Infof("did not generate defaulter for %s because no child defaulters were registered", t.Name)
					delete(newDefaulters, t)
				}
			}
			break
		}

		if len(newDefaulters) == 0 {
			klog.V(5).Infof("no defaulters in package %s", pkg.Name)
			// 包本身没有 defaulter 标签时，无需生成文件（否则会产出空文件）。
			if _, hasTag := extractTag(pkg.Comments); !hasTag {
				continue
			}
		}

		targetList = append(targetList,
			&generator.SimpleTarget{
				PkgName:       path.Base(pkg.Path),
				PkgPath:       pkg.Path,
				PkgDir:        pkg.Dir, // 输出包与输入包相同
				HeaderComment: boilerplate,

				// 只处理类型定义所在的包，避免把 peer 包的类型也写进本包。
				FilterFunc: func(c *generator.Context, t *types.Type) bool {
					return t.Name.Package == typesPkg.Path
				},

				GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
					return []generator.Generator{
						NewGenDefaulter(args.OutputFile, typesPkg.Path, pkg.Path, existingDefaulters, newDefaulters, peerPkgs),
					}
				},
			})
	}
	return targetList
}

// callTreeForType 持有构建类型调用树所需的上下文。
type callTreeForType struct {
	existingDefaulters     defaulterFuncMap
	newDefaulters          defaulterFuncMap
	currentlyBuildingTypes map[*types.Type]bool
}

// newCallTreeForType 构造调用树构建器。
// currentlyBuildingTypes 用于检测递归类型，防止无限递归。
func newCallTreeForType(existingDefaulters, newDefaulters defaulterFuncMap) *callTreeForType {
	return &callTreeForType{
		existingDefaulters:     existingDefaulters,
		newDefaulters:          newDefaulters,
		currentlyBuildingTypes: make(map[*types.Type]bool),
	}
}

// resolveTypeAndDepth 沿指针与别名逐层展开 t，直到到达第一个非指针类型；
// 同时返回经过的指针层数，供生成代码决定需要多少层取地址。
func resolveTypeAndDepth(t *types.Type) (*types.Type, int) {
	var prev *types.Type
	depth := 0
	for prev != t {
		prev = t
		if t.Kind == types.Alias {
			t = t.Underlying
		} else if t.Kind == types.Pointer {
			t = t.Elem
			depth += 1
		}
	}
	return t, depth
}

// getPointerElementPath 沿指针与别名展开，返回从 t 到其基础值类型路径上的
// 所有指针元素。
//
// 示例：
//
//	type MyString string
//	type MyStringPointer *MyString
//	type MyStringPointerPointer *MyStringPointer
//	type MyStringAlias MyStringPointer
//	type MyStringAliasPointer *MyStringAlias
//	type MyStringAliasDoublePointer **MyStringAlias
//
//	t		  				   | defaultPointerElementPath(t)
//	---------------------------|----------------------------------------
//	MyString                   | []
//	MyStringPointer            | [MyString]
//	MyStringPointerPointer     | [MyStringPointer, MyString]
//	MyStringAlias              | [MyStringPointer, MyString]
//	MyStringAliasPointer       | [MyStringAlias, MyStringPointer, MyString]
//	MyStringAliasDoublePointer | [*MyStringAlias, MyStringAlias, MyStringPointer, MyString]
func getPointerElementPath(t *types.Type) []*types.Type {
	var path []*types.Type
	for t != nil {
		switch t.Kind {
		case types.Alias:
			t = t.Underlying
		case types.Pointer:
			t = t.Elem
			path = append(path, t)
		default:
			t = nil
		}
	}
	return path
}

// getNestedDefault 在解析别名类型时返回遇到的第一个默认值。
// 别名链上不同层级都可能带 +default，最外层优先。
func getNestedDefault(t *types.Type) (string, error) {
	var prev *types.Type
	for prev != t {
		prev = t
		defaultMap, err := extractDefaultTag(t.CommentLines)
		if err != nil {
			return "", err
		}
		if len(defaultMap) == 1 && defaultMap[0] != "" {
			return defaultMap[0], nil
		}
		if t.Kind == types.Alias {
			t = t.Underlying
		} else if t.Kind == types.Pointer {
			t = t.Elem
		}
	}
	return "", nil
}

var refRE = regexp.MustCompile(`^ref\((?P<reference>[^"]+)\)$`)
var refREIdentIndex = refRE.SubexpIndex("reference")

// parseSymbolReference 匹配以下形式的字符串：
//   - ref(Ident)
//   - ref(pkgpath.Ident)
//
// 若匹配，返回（可选的）pkgpath、Ident 以及 true；否则返回空值与 false。
// ref() 让 +default 能引用常量或变量，而非只支持字面量。
func parseSymbolReference(s, sourcePackage string) (types.Name, bool) {
	matches := refRE.FindStringSubmatch(s)
	if len(matches) < refREIdentIndex || matches[refREIdentIndex] == "" {
		return types.Name{}, false
	}

	contents := matches[refREIdentIndex]
	name := types.ParseFullyQualifiedName(contents)
	// 未写包路径时，符号默认属于注释所在包。
	if len(name.Package) == 0 {
		name.Package = sourcePackage
	}
	return name, true
}

// populateDefaultValue 把 +default 标注解析到 node 上。
// 若 node 为 nil（如标量字段本不产生 callNode），则按需新建仅承载默认值的节点。
func populateDefaultValue(node *callNode, t *types.Type, tags string, commentLines []string, commentPackage string) *callNode {
	defaultMap, err := extractDefaultTag(commentLines)
	if err != nil {
		klog.Fatalf("Error extracting default tag: %v", err)
	}

	var defaultString string
	if len(defaultMap) == 1 {
		defaultString = defaultMap[0]
	} else if len(defaultMap) > 1 {
		klog.Fatalf("Found more than one default tag for %v", t.Kind)
	}

	baseT, depth := resolveTypeAndDepth(t)
	// 指针类型本身通常不带标注，默认值往往写在其指向的元素类型上，需向内查找。
	if depth > 0 && defaultString == "" {
		defaultString, err = getNestedDefault(t)
		if err != nil {
			klog.Fatalf("Error extracting nested default tag: %v", err)
		}
	}

	if len(defaultString) == 0 {
		return node
	}
	var symbolReference types.Name
	var defaultValue interface{}
	if id, ok := parseSymbolReference(defaultString, commentPackage); ok {
		// ref(...) 形式：不是字面量，生成代码直接引用符号。
		symbolReference = id
		defaultString = ""
	} else if err := json.Unmarshal([]byte(defaultString), &defaultValue); err != nil {
		klog.Fatalf("Failed to unmarshal default: %v", err)
	}

	if defaultValue != nil {
		zero := typeZeroValue[t.String()]
		if reflect.DeepEqual(defaultValue, zero) {
			// 若标注的默认值等于该类型的零值，则无需生成任何默认化代码。
			return node
		}
	}

	// 基本类型不会自动生成 callNode；若默认值需要写入，则创建仅含默认值的节点。
	if node == nil {
		node = &callNode{}
		node.markerOnly = true
	}

	node.defaultIsPrimitive = baseT.IsPrimitive()
	node.defaultType = baseT
	node.defaultTopLevelType = t
	node.defaultValue.InlineConstant = defaultString
	node.defaultValue.SymbolReference = symbolReference
	return node
}

// build 构建“字段访问路径 -> 应调用函数”的树。路径按 Go 中的访问方式描述
// （指针、元素、切片、键），对树做中序遍历即可生成在正确类型上调用嵌套函数的
// Go 代码。若该类型无需调用任何函数，或类型为基本类型（defaulter 目前只能作用于
// 结构体），返回值可能为 nil。
//
// root 为 true 时不会使用 newDefaulter。existingDefaulters 应包含代码中已定义的
// 全部默认化函数；newDefaulters 应包含所有可能生成或将会生成的对象级 defaulter。
// 若 newDefaulters 中存在某类型但 object 字段为 nil，本函数会跳过该 defaulter，
// 这样可避免为“会调用空 defaulter 的列表类型”生成对象级函数。
func (c *callTreeForType) build(t *types.Type, root bool) *callNode {
	parent := &callNode{}

	if root {
		// 根节点永远是（被当作）指针。
		parent.elem = true
	}

	defaults := c.existingDefaulters[t]
	newDefaults, generated := c.newDefaulters[t]
	switch {
	case !root && generated && newDefaults.object != nil:
		parent.call = append(parent.call, newDefaults.object)
		// 将要生成的 defaulter 按定义是覆盖式的，因此停止递归。
		klog.V(6).Infof("the defaulter %s will be generated as an object defaulter", t.Name)
		return parent

	case defaults.object != nil:
		// 对象级 defaulter 总是覆盖式的。
		parent.call = append(parent.call, defaults.object)
		return parent

	case defaults.base != nil:
		parent.call = append(parent.call, defaults.base)
		// 若 base 函数声明自己“覆盖”了子 defaulter，可停止递归。
		isCovers, err := checkTag(defaults.base.CommentLines, "covers")
		if err != nil {
			klog.Fatalf("error extracting %s tag: %v", tagName, err)
		}
		if isCovers {
			klog.V(6).Infof("the defaulter %s indicates it covers all sub generators", t.Name)
			return parent
		}
	}

	// base 已加入，现在加入为该对象定义的所有附加 defaulter。
	parent.call = append(parent.call, defaults.additional...)

	// 若类型正在构建中，说明遇到了嵌套递归类型：不再展开，也不生成任何内容。
	// 这是防止递归类型导致无限递归的关键。
	if c.currentlyBuildingTypes[t] {
		return nil
	}
	// 标记该类型正在构建。
	c.currentlyBuildingTypes[t] = true

	defer func() {
		// 该类型本次已作为父节点处理完毕，可以安全地再次构建其子树。
		c.currentlyBuildingTypes[t] = false
	}()

	switch t.Kind {
	case types.Pointer:
		if child := c.build(t.Elem, false); child != nil {
			child.elem = true
			parent.children = append(parent.children, *child)
		}
	case types.Slice, types.Array:
		if child := c.build(t.Elem, false); child != nil {
			child.index = true
			if t.Elem.Kind == types.Pointer {
				child.elem = true
			}
			parent.children = append(parent.children, *child)
		} else if member := populateDefaultValue(nil, t.Elem, "", t.Elem.CommentLines, t.Elem.Name.Package); member != nil {
			// 元素类型自身没有 defaulter，但带默认值标注时仍需要遍历切片写入默认值。
			member.index = true
			parent.children = append(parent.children, *member)
		}
	case types.Map:
		if child := c.build(t.Elem, false); child != nil {
			child.key = true
			parent.children = append(parent.children, *child)
		} else if member := populateDefaultValue(nil, t.Elem, "", t.Elem.CommentLines, t.Elem.Name.Package); member != nil {
			member.key = true
			parent.children = append(parent.children, *member)
		}

	case types.Struct:
		for _, field := range t.Members {
			name := field.Name
			// 内嵌字段没有名字，用其类型名代替，以便生成 `x.FieldName` 形式的访问路径。
			if len(name) == 0 {
				if field.Type.Kind == types.Pointer {
					name = field.Type.Elem.Name.Name
				} else {
					name = field.Type.Name.Name
				}
			}
			if child := c.build(field.Type, false); child != nil {
				child.field = name
				populateDefaultValue(child, field.Type, field.Tags, field.CommentLines, field.Type.Name.Package)
				parent.children = append(parent.children, *child)
			} else if member := populateDefaultValue(nil, field.Type, field.Tags, field.CommentLines, t.Name.Package); member != nil {
				member.field = name
				parent.children = append(parent.children, *member)
			}
		}
	case types.Alias:
		if child := c.build(t.Underlying, false); child != nil {
			parent.children = append(parent.children, *child)
		}
	}
	if len(parent.children) == 0 && len(parent.call) == 0 {
		return nil
	}
	return parent
}

const (
	runtimePackagePath    = "k8s.io/apimachinery/pkg/runtime"
	conversionPackagePath = "k8s.io/apimachinery/pkg/conversion"
)

// genDefaulter 生成一个包含自动生成默认化代码的文件。
type genDefaulter struct {
	generator.GoGenerator
	typesPackage       string
	outputPackage      string
	peerPackages       []string
	newDefaulters      defaulterFuncMap
	existingDefaulters defaulterFuncMap
	imports            namer.ImportTracker
	typesForInit       []*types.Type
}

// NewGenDefaulter 构造默认化代码生成器。
// outputFilename 为生成文件名（如 zz_generated.defaults.go），
// typesPackage 为类型定义所在包，outputPackage 为输出包。
func NewGenDefaulter(outputFilename, typesPackage, outputPackage string, existingDefaulters, newDefaulters defaulterFuncMap, peerPkgs []string) generator.Generator {
	return &genDefaulter{
		GoGenerator: generator.GoGenerator{
			OutputFilename: outputFilename,
		},
		typesPackage:       typesPackage,
		outputPackage:      outputPackage,
		peerPackages:       peerPkgs,
		newDefaulters:      newDefaulters,
		existingDefaulters: existingDefaulters,
		imports:            generator.NewImportTrackerForPackage(outputPackage),
		typesForInit:       make([]*types.Type, 0),
	}
}

// Namers 让 raw 命名系统记录生成代码引用的导入，以便输出正确的 import 块。
func (g *genDefaulter) Namers(c *generator.Context) namer.NameSystems {
	// 让本文件的 raw namer 跟踪其导入。
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// isOtherPackage 判断导入是否指向输出包之外的包（外部包才需要写入 import 块）。
func (g *genDefaulter) isOtherPackage(pkg string) bool {
	if pkg == g.outputPackage {
		return false
	}
	if strings.HasSuffix(pkg, `"`+g.outputPackage+`"`) {
		return false
	}
	return true
}

// Filter 只保留已生成对象级 defaulter 的类型，并记录到 typesForInit，
// 供 Init 生成 RegisterDefaults 注册代码。
func (g *genDefaulter) Filter(c *generator.Context, t *types.Type) bool {
	defaults, ok := g.newDefaulters[t]
	if !ok || defaults.object == nil {
		return false
	}
	g.typesForInit = append(g.typesForInit, t)
	return true
}

// Imports 返回需要写入文件的 import 列表，过滤掉指向输出包自身的导入。
func (g *genDefaulter) Imports(c *generator.Context) (imports []string) {
	var importLines []string
	for _, singleImport := range g.imports.ImportLines() {
		if g.isOtherPackage(singleImport) {
			importLines = append(importLines, singleImport)
		}
	}
	return importLines
}

// Init 生成 RegisterDefaults：把每个对象级 defaulter 注册进 runtime.Scheme。
// 注册顺序跟随 Filter 的遍历顺序，保持输出稳定。
func (g *genDefaulter) Init(c *generator.Context, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	scheme := c.Universe.Type(types.Name{Package: runtimePackagePath, Name: "Scheme"})
	schemePtr := &types.Type{
		Kind: types.Pointer,
		Elem: scheme,
	}
	sw.Do("// RegisterDefaults adds defaulters functions to the given scheme.\n", nil)
	sw.Do("// Public to allow building arbitrary schemes.\n", nil)
	sw.Do("// All generated defaulters are covering - they call all nested defaulters.\n", nil)
	sw.Do("func RegisterDefaults(scheme $.|raw$) error {\n", schemePtr)
	for _, t := range g.typesForInit {
		args := defaultingArgsFromType(t)
		// 注册的函数把 interface{} 断言回具体类型，再调用对象级 defaulter。
		sw.Do("scheme.AddTypeDefaultingFunc(&$.inType|raw${}, func(obj interface{}) { $.inType|objectdefaultfn$(obj.(*$.inType|raw$)) })\n", args)
	}
	sw.Do("return nil\n", nil)
	sw.Do("}\n\n", nil)
	return sw.Error()
}

// GenerateType 为单个类型生成 SetObjectDefaults_ 函数。
func (g *genDefaulter) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	if _, ok := g.newDefaulters[t]; !ok {
		return nil
	}

	klog.V(5).Infof("generating for type %v", t)

	callTree := newCallTreeForType(g.existingDefaulters, g.newDefaulters).build(t, true)
	if callTree == nil {
		klog.V(5).Infof("  no defaulters defined")
		return nil
	}
	i := 0
	callTree.VisitInOrder(func(ancestors []*callNode, current *callNode) {
		if ref := &current.defaultValue.SymbolReference; len(ref.Name) > 0 {
			// 确保符号所在包被导入到生成文件中。
			g.imports.AddSymbol(*ref)

			// 用导入解析出的本地包名重写全限定名，避免包名与路径不一致导致编译失败。
			ref.Package = g.imports.LocalNameOf(ref.Package)
		}

		if len(current.call) == 0 {
			return
		}
		path := callPath(append(ancestors, current))
		klog.V(5).Infof("  %d: %s", i, path)
		i++
	})

	sw := generator.NewSnippetWriter(w, c, "$", "$")
	g.generateDefaulter(c, t, callTree, sw)
	return sw.Error()
}

// defaultingArgsFromType 构造模板参数，供 defaultfn/objectdefaultfn 命名系统渲染。
func defaultingArgsFromType(inType *types.Type) generator.Args {
	return generator.Args{
		"inType": inType,
	}
}

// generateDefaulter 输出 SetObjectDefaults_ 函数体，函数体由 callTree 遍历生成。
func (g *genDefaulter) generateDefaulter(c *generator.Context, inType *types.Type, callTree *callNode, sw *generator.SnippetWriter) {
	sw.Do("func $.inType|objectdefaultfn$(in *$.inType|raw$) {\n", defaultingArgsFromType(inType))
	callTree.WriteMethod(c, "in", 0, nil, sw)
	sw.Do("}\n\n", nil)
}

// callNode 表示 Go 类型访问树中的一个节点：从根到叶的路径对应 Go 代码中的
// 一次访问。例如，若某个容器生命周期钩子存在默认化函数，要正确调用它需要：
//
//	for i := range pod.Spec.Containers {
//	  o := &pod.Spec.Containers[i]
//	  if o.LifecycleHook != nil {
//	    SetDefaults_LifecycleHook(o.LifecycleHook)
//	  }
//	}
//
// 这会被表示为如下的调用树：
//
//	callNode
//	  field: "Spec"
//	  children:
//	  - field: "Containers"
//	    children:
//	    - index: true
//	      children:
//	      - field: "LifecycleHook"
//	        elem: true
//	        call:
//	        - SetDefaults_LifecycleHook
//
// 遍历该树即可生成上述 Go 结构（先取 Spec 字段，再取 Containers，然后 range，
// 接着判空 LifecycleHook，最后对该字段的指针调用 SetDefaults_LifecycleHook）。
type callNode struct {
	// field 是要访问的 Go 成员名。
	field string
	// key 为 true 表示这是 map，需要 range 其键值。
	key bool
	// index 为 true 表示这是切片，需要 range 其元素。
	index bool
	// elem 为 true 表示前序元素指向一个指针（通常仅 field 为 true）。
	elem bool

	// call 是必须在该节点上调用的全部函数，按顺序执行。
	call []*types.Type
	// children 是还需要继续遍历的子调用节点。
	children []callNode

	// defaultValue 是该节点的默认值。
	// 只有基本类型与指针类型才可能带默认值。
	defaultValue defaultValue

	// defaultIsPrimitive 决定默认值的赋值方式：
	// 基本类型直接赋值，复杂类型走 JSON 反序列化。
	defaultIsPrimitive bool

	// markerOnly 为 true 表示该节点仅用于写入默认值。
	markerOnly bool

	// defaultType 是节点的传递性底层/元素类型。
	// 提供的默认值字面量或引用应可转换为该类型。
	//
	// 例如：
	//	node type = *string 			-> 	defaultType = string
	//	node type = StringPointerAlias 	-> 	defaultType = string
	// 仅在 defaultIsPrimitive 为 true 时填充。
	defaultType *types.Type

	// defaultTopLevelType 是值最终应解析到的类型。
	// 与 defaultType 不同，它不展开别名与指针。
	defaultTopLevelType *types.Type
}

// defaultValue 描述一个默认值的来源：内联字面量或符号引用，二者互斥。
type defaultValue struct {
	// 值直接写在标记注释中，并按 JSON 解析。
	InlineConstant string
	// 符号名相对于解析出的包路径，例如来自其他包时为
	// k8s.io/pkg.apis.v1.Foo，同包时仅为 `Foo`。
	SymbolReference types.Name
}

// IsEmpty 判断是否没有默认值。
func (d defaultValue) IsEmpty() bool {
	resolved := d.Resolved()
	return resolved == ""
}

// Resolved 返回可直接写入生成代码的值表达式：优先内联字面量，否则为符号引用。
func (d defaultValue) Resolved() string {
	if len(d.InlineConstant) > 0 {
		return d.InlineConstant
	}
	return d.SymbolReference.String()
}

// CallNodeVisitorFunc 是访问调用树的回调。ancestors 是当前节点到根的全部父节点，
// 根节点处为空。
type CallNodeVisitorFunc func(ancestors []*callNode, node *callNode)

// VisitInOrder 按中序（先父后子）遍历调用树。
func (n *callNode) VisitInOrder(fn CallNodeVisitorFunc) {
	n.visitInOrder(nil, fn)
}

// visitInOrder 是 VisitInOrder 的递归实现。
func (n *callNode) visitInOrder(ancestors []*callNode, fn CallNodeVisitorFunc) {
	fn(ancestors, n)
	ancestors = append(ancestors, n)
	for i := range n.children {
		n.children[i].visitInOrder(ancestors, fn)
	}
}

var (
	indexVariables = "ijklmnop"
	localVariables = "abcdefgh"
)

// varsForDepth 生成在该函数嵌套深度下词法作用域内保证唯一的临时变量名。
// 前 7 层使用惯用的 Go 循环变量名，更深的层级退化为带数字前缀的名字。
func varsForDepth(depth int) (index, local string) {
	if depth > len(indexVariables) {
		index = fmt.Sprintf("i%d", depth)
	} else {
		index = indexVariables[depth : depth+1]
	}
	if depth > len(localVariables) {
		local = fmt.Sprintf("local%d", depth)
	} else {
		local = localVariables[depth : depth+1]
	}
	return
}

// writeCalls 根据节点的 call 字段，为给定变量名生成函数调用列表。
// 若变量不是指针，调用时自动取地址，因为 defaulter 的签名要求指针参数。
func (n *callNode) writeCalls(varName string, isVarPointer bool, sw *generator.SnippetWriter) {
	accessor := varName
	if !isVarPointer {
		accessor = "&" + accessor
	}
	for _, fn := range n.call {
		sw.Do("$.fn|raw$($.var$)\n", generator.Args{
			"fn":  fn,
			"var": accessor,
		})
	}
}

// getTypeZeroValue 返回类型的零值，供生成 `if x == <zero>` 判断。
// 字符串零值必须加引号，否则生成的是空表达式。
func getTypeZeroValue(t string) (interface{}, error) {
	defaultZero, ok := typeZeroValue[t]
	if !ok {
		return nil, fmt.Errorf("cannot find zero value for type %v in typeZeroValue", t)
	}

	// 空字符串的零值在生成代码中必须带引号。
	if defaultZero == "" {
		defaultZero = strconv.Quote(defaultZero.(string))
	}
	return defaultZero, nil
}

// writeDefaulter 生成写入默认值的代码。策略按类型分三类：
// 指针链（逐层取地址）、基本类型（与零值比较后赋值）、复杂类型（JSON 反序列化）。
func (n *callNode) writeDefaulter(c *generator.Context, varName string, index string, isVarPointer bool, sw *generator.SnippetWriter) {
	if n.defaultValue.IsEmpty() {
		return
	}

	jsonUnmarshalType := c.Universe.Type(types.Name{Package: "encoding/json", Name: "Unmarshal"})

	args := generator.Args{
		"defaultValue":  n.defaultValue.Resolved(),
		"varName":       varName,
		"index":         index,
		"varTopType":    n.defaultTopLevelType,
		"jsonUnmarshal": jsonUnmarshalType,
	}

	variablePlaceholder := ""

	if n.index {
		// 数组元素默认化。
		variablePlaceholder = "$.varName$[$.index$]"
	} else if n.key {
		// map 值默认化。map 值不可寻址，需要临时变量中转。
		variablePlaceholder = "$.varName$[$.index$]"
		mapDefaultVar := args["index"].(string) + "_default"
		args["mapDefaultVar"] = mapDefaultVar
	} else {
		// 基本类型默认化。
		variablePlaceholder = "$.varName$"
	}

	// defaultIsPrimitive 为 true 表示该类型或其（数组/map 中的）底层类型是基本类型，
	// 或是指向基本类型的指针（如 int、map[string]*string、[]int）。
	if n.defaultIsPrimitive {
		// 若赋值目标是指针而默认值是基本类型，需要沿指针路径逐层取地址，
		// 直到类型匹配。
		if pointerPath := getPointerElementPath(n.defaultTopLevelType); len(pointerPath) > 0 {
			// 若目标是指针，pointerPath 的最后一项即最底层指针的元素类型，
			// 也就是默认值的基础类型。
			destElemType := pointerPath[len(pointerPath)-1]
			pointerArgs := args.WithArgs(generator.Args{
				"varDepth":     len(pointerPath),
				"baseElemType": destElemType,
			})

			sw.Do(fmt.Sprintf("if %s == nil {\n", variablePlaceholder), pointerArgs)
			if len(n.defaultValue.InlineConstant) > 0 {
				// 默认值是字面量，可用 var 语句赋值。
				sw.Do("var ptrVar$.varDepth$ $.baseElemType|raw$ = $.defaultValue$\n", pointerArgs)
			} else {
				// 默认值不是字面量，可能需要转换到目标指针的基础类型。
				sw.Do("ptrVar$.varDepth$ := $.baseElemType|raw$($.defaultValue$)\n", pointerArgs)
			}

			// 自内向外逐层取地址：最后一层写入目标变量，其余层创建中间指针。
			for i := len(pointerPath); i >= 1; i-- {
				dest := fmt.Sprintf("ptrVar%d", i-1)
				assignment := ":="
				if i == 1 {
					// 最后一次赋值写入存储目标。
					dest = variablePlaceholder
					assignment = "="
				}

				sourceType := "*" + destElemType.String()
				if i == len(pointerPath) {
					// 初始值不是指针。
					sourceType = destElemType.String()
				}
				destElemType = pointerPath[i-1]

				// dest 不能放进 args：它的值可能是模板占位符而非普通值。
				elementArgs := pointerArgs.WithArgs(generator.Args{
					"assignment":   assignment,
					"source":       fmt.Sprintf("ptrVar%d", i),
					"destElemType": destElemType,
				})

				// 类型完全一致时跳过转换。
				if destElemType.String() == sourceType {
					sw.Do(fmt.Sprintf("%v $.assignment$ &$.source$\n", dest), elementArgs)
				} else {
					sw.Do(fmt.Sprintf("%v $.assignment$ (*$.destElemType|raw$)(&$.source$)\n", dest), elementArgs)
				}
			}
		} else {
			// 基本类型不能用 nil 判断，必须与类型的零值比较。
			defaultZero, err := getTypeZeroValue(n.defaultType.String())
			if err != nil {
				klog.Error(err)
			}
			args["defaultZero"] = defaultZero

			sw.Do(fmt.Sprintf("if %s == $.defaultZero$ {\n", variablePlaceholder), args)

			if len(n.defaultValue.InlineConstant) > 0 {
				sw.Do(fmt.Sprintf("%s = $.defaultValue$", variablePlaceholder), args)
			} else {
				// 符号引用需要显式转换到顶层类型（如命名别名类型）。
				sw.Do(fmt.Sprintf("%s = $.varTopType|raw$($.defaultValue$)", variablePlaceholder), args)
			}
		}
	} else {
		sw.Do(fmt.Sprintf("if %s == nil {\n", variablePlaceholder), args)
		// map 值不可直接取址，需要临时变量承载 JSON 反序列化结果。
		// 这适用于值非基本类型的 map（如 map[string]SubStruct）。
		if n.key {
			sw.Do("$.mapDefaultVar$ := $.varName$[$.index$]\n", args)
			sw.Do("if err := $.jsonUnmarshal|raw$([]byte(`$.defaultValue$`), &$.mapDefaultVar$); err != nil {\n", args)
		} else {
			variablePointer := variablePlaceholder
			if !isVarPointer {
				variablePointer = "&" + variablePointer
			}
			sw.Do(fmt.Sprintf("if err := $.jsonUnmarshal|raw$([]byte(`$.defaultValue$`), %s); err != nil {\n", variablePointer), args)
		}
		// 默认值在编译期已知，反序列化失败说明标注写错了，直接 panic 暴露问题。
		sw.Do("panic(err)\n", nil)
		sw.Do("}\n", nil)
		if n.key {
			sw.Do("$.varName$[$.index$] = $.mapDefaultVar$\n", args)
		}
	}
	sw.Do("}\n", nil)
}

// WriteMethod 对调用树做中序遍历，按需生成循环与 if 块，把调用树正确转换为
// 在全部子节点上调用相应函数的方法体。depth 用于在正确层级生成局部变量名。
func (n *callNode) WriteMethod(c *generator.Context, varName string, depth int, ancestors []*callNode, sw *generator.SnippetWriter) {
	if len(n.field) > 0 {
		varName = varName + "." + n.field
	}

	index, local := varsForDepth(depth)
	vars := generator.Args{
		"index": index,
		"local": local,
		"var":   varName,
	}

	// 切片元素本身已是指针时（n.index）不需要额外判空，避免生成 `if x[i] != nil`
	// 后又对其取地址的冗余判断。
	isPointer := n.elem && !n.index
	if isPointer && len(ancestors) > 0 {
		sw.Do("if $.var$ != nil {\n", vars)
	}

	switch {
	case n.index:
		sw.Do("for $.index$ := range $.var$ {\n", vars)
		if !n.markerOnly {
			if n.elem {
				// 元素已是指针，直接使用。
				sw.Do("$.local$ := $.var$[$.index$]\n", vars)
			} else {
				// 元素是值类型，取其地址传给 defaulter。
				sw.Do("$.local$ := &$.var$[$.index$]\n", vars)
			}
		}

		n.writeDefaulter(c, varName, index, isPointer, sw)
		n.writeCalls(local, true, sw)
		for i := range n.children {
			n.children[i].WriteMethod(c, local, depth+1, append(ancestors, n), sw)
		}
		sw.Do("}\n", nil)
	case n.key:
		if !n.defaultValue.IsEmpty() {
			// map 键有类型，不能与其他数组/map 共用同一个索引变量名，
			// 否则嵌套 map 会因变量遮蔽而取错键。
			index = index + "_" + ancestors[len(ancestors)-1].field
			vars["index"] = index
			sw.Do("for $.index$ := range $.var$ {\n", vars)
			n.writeDefaulter(c, varName, index, isPointer, sw)
			sw.Do("}\n", nil)
		}
	default:
		n.writeDefaulter(c, varName, index, isPointer, sw)
		n.writeCalls(varName, isPointer, sw)
		for i := range n.children {
			n.children[i].WriteMethod(c, varName, depth, append(ancestors, n), sw)
		}
	}

	if isPointer && len(ancestors) > 0 {
		sw.Do("}\n", nil)
	}
}

// callPath 是从根到某个节点的访问路径，仅用于调试输出。
type callPath []*callNode

// String 返回 callPath 的字符串表示，大致接近 Go 访问器写法，仅供调试。
func (path callPath) String() string {
	if len(path) == 0 {
		return "<none>"
	}
	var parts []string
	for _, p := range path {
		last := len(parts) - 1
		switch {
		case p.elem:
			if len(parts) > 0 {
				parts[last] = "*" + parts[last]
			} else {
				parts = append(parts, "*")
			}
		case p.index:
			if len(parts) > 0 {
				parts[last] += "[i]"
			} else {
				parts = append(parts, "[i]")
			}
		case p.key:
			if len(parts) > 0 {
				parts[last] += "[key]"
			} else {
				parts = append(parts, "[key]")
			}
		default:
			if len(p.field) > 0 {
				parts = append(parts, p.field)
			} else {
				parts = append(parts, "<root>")
			}
		}
	}
	var calls []string
	for _, fn := range path[len(path)-1].call {
		calls = append(calls, fn.Name.String())
	}
	if len(calls) == 0 {
		calls = append(calls, "<none>")
	}

	return strings.Join(parts, ".") + " calls " + strings.Join(calls, ", ")
}
