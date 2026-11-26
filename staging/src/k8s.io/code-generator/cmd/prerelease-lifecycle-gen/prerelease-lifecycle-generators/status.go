/*
Copyright 2020 The Kubernetes Authors.

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

// Package prereleaselifecyclegenerators 生成 API 类型的生命周期方法
// （APILifecycleIntroduced/Deprecated/Removed/Replacement）。
// 这些方法供 apiserver 在运行期判定 API 的引入、弃用与移除版本。
package prereleaselifecyclegenerators

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/code-generator/cmd/prerelease-lifecycle-gen/args"
	"k8s.io/code-generator/pkg/apidefinitions"
	genutil "k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"

	"k8s.io/klog/v2"
)

// 以下标签承载 API 生命周期生成参数。由于发布节奏固定，API 引入时即可较准确地推断生命周期版本。
const (
	tagEnabledName    = "k8s:prerelease-lifecycle-gen"
	introducedTagName = tagEnabledName + ":introduced"
	deprecatedTagName = tagEnabledName + ":deprecated"
	removedTagName    = tagEnabledName + ":removed"

	replacementTagName = tagEnabledName + ":replacement"
)

// tagValue 保存 tagName 标签中的参数，即逗号前的主值。
type tagValue struct {
	value string
}

// extractEnabledTypeTag 提取 +k8s:prerelease-lifecycle-gen 标签。
// 类型注释分为 Closest/SecondClosest 两段，需合并后再查找。
func extractEnabledTypeTag(t *types.Type) *tagValue {
	comments := append(append([]string{}, t.SecondClosestCommentLines...), t.CommentLines...)
	return extractTag(tagEnabledName, comments)
}

// tagExists 判断类型上是否存在指定标签，不校验其取值。
func tagExists(tagName string, t *types.Type) bool {
	comments := append(append([]string{}, t.SecondClosestCommentLines...), t.CommentLines...)
	rawTag := extractTag(tagName, comments)
	return rawTag != nil
}

// extractKubeVersionTag 解析形如 "major.minor" 的版本标签。
// 缺少标签或格式错误都返回错误，由调用方决定是 fatal 还是跳过。
func extractKubeVersionTag(tagName string, t *types.Type) (*tagValue, int, int, error) {
	comments := append(append([]string{}, t.SecondClosestCommentLines...), t.CommentLines...)
	rawTag := extractTag(tagName, comments)
	if rawTag == nil || len(rawTag.value) == 0 {
		return nil, -1, -1, fmt.Errorf("%v missing %v=Version tag", t, tagName)
	}

	splitValue := strings.Split(rawTag.value, ".")
	if len(splitValue) != 2 || len(splitValue[0]) == 0 || len(splitValue[1]) == 0 {
		return nil, -1, -1, fmt.Errorf("%v format must match %v=xx.yy tag", t, tagName)
	}
	major, err := strconv.ParseInt(splitValue[0], 10, 32)
	if err != nil {
		return nil, -1, -1, fmt.Errorf("%v format must match %v=xx.yy : %w", t, tagName, err)
	}
	minor, err := strconv.ParseInt(splitValue[1], 10, 32)
	if err != nil {
		return nil, -1, -1, fmt.Errorf("%v format must match %v=xx.yy : %w", t, tagName, err)
	}

	return rawTag, int(major), int(minor), nil
}

// extractIntroducedTag 提取 +k8s:prerelease-lifecycle-gen:introduced。
func extractIntroducedTag(t *types.Type) (*tagValue, int, int, error) {
	return extractKubeVersionTag(introducedTagName, t)
}

// extractDeprecatedTag 提取 +k8s:prerelease-lifecycle-gen:deprecated。
func extractDeprecatedTag(t *types.Type) (*tagValue, int, int, error) {
	return extractKubeVersionTag(deprecatedTagName, t)
}

// extractRemovedTag 提取 +k8s:prerelease-lifecycle-gen:removed。
func extractRemovedTag(t *types.Type) (*tagValue, int, int, error) {
	return extractKubeVersionTag(removedTagName, t)
}

// extractReplacementTag 解析 "<group>,<version>,<kind>" 形式的替换标签。
// 校验命名规范（group 小写、version 以 v 开头且小写、kind 首字母大写），
// 以便生成代码中的 GVK 字面量合法。
func extractReplacementTag(t *types.Type) (group, version, kind string, hasReplacement bool, err error) {
	comments := append(append([]string{}, t.SecondClosestCommentLines...), t.CommentLines...)

	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{replacementTagName}, comments)
	if err != nil {
		return "", "", "", false, fmt.Errorf("failed to parse comments: %w", err)
	}
	tagVals := tags[replacementTagName]
	if len(tagVals) == 0 {
		// 标签不存在。
		return "", "", "", false, nil
	}
	// 多个取值属于配置错误，直接报错。
	if len(tagVals) > 1 {
		return "", "", "", false, fmt.Errorf("found %d %s tags: %q", len(tagVals), replacementTagName, tagVals)
	}
	tagValue := tagVals[0]
	parts := strings.Split(tagValue, ",")
	if len(parts) != 3 {
		return "", "", "", false, fmt.Errorf(`%s value must be "<group>,<version>,<kind>", got %q`, replacementTagName, tagValue)
	}
	group, version, kind = parts[0], parts[1], parts[2]
	if len(version) == 0 || len(kind) == 0 {
		return "", "", "", false, fmt.Errorf(`%s value must be "<group>,<version>,<kind>", got %q`, replacementTagName, tagValue)
	}
	// 校验 group 是否全小写。
	if strings.ToLower(group) != group {
		return "", "", "", false, fmt.Errorf(`replacement group must be all lower-case, got %q`, group)
	}
	// 校验 version 是否以 v 开头且全小写。
	if !strings.HasPrefix(version, "v") || strings.ToLower(version) != version {
		return "", "", "", false, fmt.Errorf(`replacement version must start with "v" and be all lower-case, got %q`, version)
	}
	// 校验 kind 首字母是否大写。
	if strings.ToUpper(kind[:1]) != kind[:1] {
		return "", "", "", false, fmt.Errorf(`replacement kind must start with uppercase-letter, got %q`, kind)
	}
	return group, version, kind, true, nil
}

// extractTag 是标签解析的公共入口：要求至多一个值，且除主值外不允许
// 额外参数，避免标签语法歧义。
func extractTag(tagName string, comments []string) *tagValue {
	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{tagName}, comments)
	if err != nil {
		klog.Fatalf("Error extracting %s tags: %v", tagName, err)
	}
	if tags[tagName] == nil {
		// 标签不存在。
		return nil
	}
	// 多个取值属于配置错误，直接报错。
	if len(tags[tagName]) > 1 {
		klog.Fatalf("Found %d %s tags: %q", len(tags[tagName]), tagName, tags[tagName])
	}

	// 走到这里说明已解析出有效值。
	tag := &tagValue{}

	// 第一个值是主值。
	parts := strings.Split(tags[tagName][0], ",")
	if len(parts) >= 1 {
		tag.value = parts[0]
	}

	// 其余值是附加参数。
	parts = parts[1:]
	for i := range parts {
		kv := strings.SplitN(parts[i], "=", 2)
		k := kv[0]
		if k != "" {
			klog.Fatalf("Unsupported %s param: %q", tagName, parts[i])
		}
	}
	return tag
}

// NameSystems 返回生成器使用的命名系统；public 使用 1 级缩写以匹配
// 本项目生成的类型命名习惯。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public": namer.NewPublicNamer(1),
		"raw":    namer.NewRawNamer("", nil),
	}
}

// DefaultNameSystem 返回本包生成器排序类型时默认使用的命名系统。
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 构造输出目标定义。
// 为每个启用生成的输入包创建一个同目录输出目标；输出文件名由参数指定，
// 便于与其他生成器产物区分。
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

	for _, i := range context.Inputs {
		klog.V(5).Infof("considering pkg %q", i)
		pkg := context.Universe[i]

		info, err := apidefinitions.Identify(pkg, apidefinitions.PrereleaseLifecycle, idOpts...)
		if err != nil {
			klog.Fatal(err)
		}
		if !info.ShouldGenerate() {
			klog.V(5).Infof("  not enabled")
			continue
		}
		klog.V(3).Infof("generating package %q", pkg.Path)

		targetList = append(targetList,
			&generator.SimpleTarget{
				PkgName:       strings.Split(path.Base(pkg.Path), ".")[0],
				PkgPath:       pkg.Path,
				PkgDir:        pkg.Dir, // output pkg is the same as the input
				HeaderComment: boilerplate,
				FilterFunc: func(c *generator.Context, t *types.Type) bool {
					return t.Name.Package == pkg.Path
				},
				GeneratorsFunc: func(c *generator.Context) (generators []generator.Generator) {
					return []generator.Generator{
						NewPrereleaseLifecycleGen(args.OutputFile, pkg.Path),
					}
				},
			})
	}
	return targetList
}

// genPreleaseLifecycle 收集需要生成生命周期方法的类型并输出代码。
type genPreleaseLifecycle struct {
	generator.GoGenerator
	targetPackage string
	imports       namer.ImportTracker
	typesForInit  []*types.Type
}

// NewPrereleaseLifecycleGen 创建生命周期代码生成器。
func NewPrereleaseLifecycleGen(outputFilename, targetPackage string) generator.Generator {
	return &genPreleaseLifecycle{
		GoGenerator: generator.GoGenerator{
			OutputFilename: outputFilename,
		},
		targetPackage: targetPackage,
		imports:       generator.NewImportTracker(),
		typesForInit:  make([]*types.Type, 0),
	}
}

// Namers 为生成代码提供 public/intrapackage/raw 命名系统；
// intrapackage 不缩写包内类型名，避免与外部引用混淆。
func (g *genPreleaseLifecycle) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"public":       namer.NewPublicNamer(1),
		"intrapackage": namer.NewPublicNamer(0),
		"raw":          namer.NewRawNamer("", nil),
	}
}

// Filter 只保留可作为 API 服务的类型（含 TypeMeta），并记录待生成列表。
func (g *genPreleaseLifecycle) Filter(c *generator.Context, t *types.Type) bool {
	// 过滤掉不参与生成或无法在包内安全引用的类型。
	if !isAPIType(t) {
		klog.V(2).Infof("Type %v is not a valid target for status", t)
		return false
	}
	g.typesForInit = append(g.typesForInit, t)
	return true
}

// versionMethod 校验类型是否定义了签名正确的 <methodName>() 方法。
// Introduced() 可让类型作者提供更高效的深拷贝实现；要求签名是
//
//	func (t *T) <methodName>() string
//
// 注意：当前实现要求两个返回值 (major, minor int)，与上面的历史注释不一致，
// 以实际签名检查为准。
func versionMethod(methodName string, t *types.Type) (*types.Signature, error) {
	f, found := t.Methods[methodName]
	if !found {
		return nil, nil
	}
	if len(f.Signature.Parameters) != 0 {
		return nil, fmt.Errorf("type %v: invalid  %v signature, expected no parameters", t, methodName)
	}
	if len(f.Signature.Results) != 2 {
		return nil, fmt.Errorf("type %v: invalid  %v signature, expected exactly two result types", t, methodName)
	}

	ptrRcvr := f.Signature.Receiver != nil && f.Signature.Receiver.Kind == types.Pointer && f.Signature.Receiver.Elem.Name == t.Name
	nonPtrRcvr := f.Signature.Receiver != nil && f.Signature.Receiver.Name == t.Name

	if !ptrRcvr && !nonPtrRcvr {
		// 理论上不会走到这里。
		return nil, fmt.Errorf("type %v: invalid %v signature, expected a receiver of type %s or *%s", t, methodName, t.Name.Name, t.Name.Name)
	}

	return f.Signature, nil
}

// versionedMethodOrDie 返回 <methodName>() 方法的签名，找不到则返回 nil；
// 若类型不对则调用 klog.Fatalf。
// “OrDie” 语义：类型作者显式实现的方法若签名错误，生成应立即失败。
func versionedMethodOrDie(methodName string, t *types.Type) *types.Signature {
	ret, err := versionMethod(methodName, t)
	if err != nil {
		klog.Fatal(err)
	}
	return ret
}

// isAPIType 表示某个类型是否可能被用来对外提供 API，也就是“它是否带 TypeMeta”。
// 这并不意味着该类型一定在被提供服务，但我们会处理所有带 TypeMeta 的类型。
// 私有类型与非结构体一律排除；别名会递归检查底层类型。
func isAPIType(t *types.Type) bool {
	// 过滤非导出类型。
	if namer.IsPrivateGoName(t.Name.Name) {
		return false
	}

	if t.Kind != types.Struct {
		return false
	}

	for _, currMember := range t.Members {
		if currMember.Embedded && currMember.Name == "TypeMeta" {
			return true
		}
	}

	if t.Kind == types.Alias {
		return isAPIType(t.Underlying)
	}

	return false
}

// isOtherPackage 判断 import 行是否来自目标包之外；import 行可能带有引号，
// 因此同时比较裸路径与带引号形式。
func (g *genPreleaseLifecycle) isOtherPackage(pkg string) bool {
	if pkg == g.targetPackage {
		return false
	}
	if strings.HasSuffix(pkg, "\""+g.targetPackage+"\"") {
		return false
	}
	return true
}

// Imports 过滤掉目标包自身的 import，避免生成文件自引用。
func (g *genPreleaseLifecycle) Imports(c *generator.Context) (imports []string) {
	importLines := []string{}
	for _, singleImport := range g.imports.ImportLines() {
		if g.isOtherPackage(singleImport) {
			importLines = append(importLines, singleImport)
		}
	}
	return importLines
}

// isGAVersionRegex 匹配 GA 版本段（v1、v2...），用于决定是否生成
// 弃用/移除方法：GA 版本没有默认的弃用节奏。
var (
	isGAVersionRegex = regexp.MustCompile(`^v\d+$`)
)

// argsFromType 计算生成模板所需的版本参数。
// 默认策略：弃用版本 = 引入版本 + 3 个 minor，移除版本 = 弃用版本 + 3 个
// minor；显式标签可覆盖。GA 版本只有显式声明时才输出对应方法。
func (g *genPreleaseLifecycle) argsFromType(c *generator.Context, t *types.Type) (generator.Args, error) {
	a := generator.Args{
		"type": t,
	}
	_, introducedMajor, introducedMinor, err := extractIntroducedTag(t)
	if err != nil {
		return nil, err
	}

	// 版本取自包路径最后一段。这里用启发式规则区分 GA 与预发布：
	// GA 版本形如 vN，N 为纯数字。
	version := path.Base(t.Name.Package)
	isGAVersion := isGAVersionRegex.MatchString(version)

	a = a.
		With("introducedMajor", introducedMajor).
		With("introducedMinor", introducedMinor)

	// 按发布策略推算版本。
	hasDeprecated := tagExists(deprecatedTagName, t)
	hasRemoved := tagExists(removedTagName, t)

	deprecatedMajor := introducedMajor
	deprecatedMinor := introducedMinor + 3
	// 允许通过标签显式覆盖弃用版本。
	if hasDeprecated {
		_, deprecatedMajor, deprecatedMinor, err = extractDeprecatedTag(t)
		if err != nil {
			return nil, err
		}
	}

	if !isGAVersion || hasDeprecated {
		a = a.
			With("deprecatedMajor", deprecatedMajor).
			With("deprecatedMinor", deprecatedMinor)
	}

	// 按发布策略推算版本。
	removedMajor := deprecatedMajor
	removedMinor := deprecatedMinor + 3
	// 允许通过标签显式覆盖移除版本。
	if hasRemoved {
		_, removedMajor, removedMinor, err = extractRemovedTag(t)
		if err != nil {
			return nil, err
		}
	}

	if !isGAVersion || hasRemoved {
		a = a.
			With("removedMajor", removedMajor).
			With("removedMinor", removedMinor)
	}

	replacementGroup, replacementVersion, replacementKind, hasReplacement, err := extractReplacementTag(t)
	if err != nil {
		return nil, err
	}
	if hasReplacement {
		gvkType := c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/runtime/schema", Name: "GroupVersionKind"})
		g.imports.AddType(gvkType)
		a = a.
			With("replacementGroup", replacementGroup).
			With("replacementVersion", replacementVersion).
			With("replacementKind", replacementKind).
			With("GroupVersionKind", gvkType)
	}

	return a, nil
}

// Init 无文件级输出，生成内容全部来自 GenerateType。
func (g *genPreleaseLifecycle) Init(c *generator.Context, w io.Writer) error {
	return nil
}

// GenerateType 为单个类型输出生命周期方法；若类型作者已手写同名方法，
// 则跳过生成以避免重复定义。
func (g *genPreleaseLifecycle) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	klog.V(3).Infof("Generating prerelease-lifecycle for type %v", t)

	sw := generator.NewSnippetWriter(w, c, "$", "$")
	args, err := g.argsFromType(c, t)
	if err != nil {
		return err
	}

	if versionedMethodOrDie("APILifecycleIntroduced", t) == nil {
		sw.Do("// APILifecycleIntroduced is an autogenerated function, returning the release in which the API struct was introduced as int versions of major and minor for comparison.\n", args)
		sw.Do("// It is controlled by \""+introducedTagName+"\" tags in types.go.\n", args)
		sw.Do("func (in *$.type|intrapackage$) APILifecycleIntroduced() (major, minor int) {\n", args)
		sw.Do("    return $.introducedMajor$, $.introducedMinor$\n", args)
		sw.Do("}\n\n", nil)
	}

	if _, hasDeprecated := args["deprecatedMajor"]; hasDeprecated {
		if versionedMethodOrDie("APILifecycleDeprecated", t) == nil {
			sw.Do("// APILifecycleDeprecated is an autogenerated function, returning the release in which the API struct was or will be deprecated as int versions of major and minor for comparison.\n", args)
			sw.Do("// It is controlled by \""+deprecatedTagName+"\" tags in types.go or  \""+introducedTagName+"\" plus three minor.\n", args)
			sw.Do("func (in *$.type|intrapackage$) APILifecycleDeprecated() (major, minor int) {\n", args)
			sw.Do("    return $.deprecatedMajor$, $.deprecatedMinor$\n", args)
			sw.Do("}\n\n", nil)
		}
	}

	if _, hasReplacement := args["replacementKind"]; hasReplacement {
		if versionedMethodOrDie("APILifecycleReplacement", t) == nil {
			sw.Do("// APILifecycleReplacement is an autogenerated function, returning the group, version, and kind that should be used instead of this deprecated type.\n", args)
			sw.Do("// It is controlled by \""+replacementTagName+"=<group>,<version>,<kind>\" tags in types.go.\n", args)
			sw.Do("func (in *$.type|intrapackage$) APILifecycleReplacement() ($.GroupVersionKind|raw$) {\n", args)
			sw.Do("    return $.GroupVersionKind|raw${Group:\"$.replacementGroup$\", Version:\"$.replacementVersion$\", Kind:\"$.replacementKind$\"}\n", args)
			sw.Do("}\n\n", nil)
		}
	}

	if _, hasRemoved := args["removedMajor"]; hasRemoved {
		if versionedMethodOrDie("APILifecycleRemoved", t) == nil {
			sw.Do("// APILifecycleRemoved is an autogenerated function, returning the release in which the API is no longer served as int versions of major and minor for comparison.\n", args)
			sw.Do("// It is controlled by \""+removedTagName+"\" tags in types.go or  \""+deprecatedTagName+"\" plus three minor.\n", args)
			sw.Do("func (in *$.type|intrapackage$) APILifecycleRemoved() (major, minor int) {\n", args)
			sw.Do("    return $.removedMajor$, $.removedMinor$\n", args)
			sw.Do("}\n\n", nil)
		}
	}

	return sw.Error()
}
