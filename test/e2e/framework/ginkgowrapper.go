/*
Copyright 2022 The Kubernetes Authors.

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

package framework

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

// Feature 是被测集群必须具备的某项特性的名称。
// 这类特性不同于 feature gate。
type Feature string

// Environment 是测试可运行环境的名称，例如
// "Linux" 或 "Windows"。
type Environment string

type Valid[T comparable] struct {
	items  sets.Set[T]
	frozen bool
}

// Add 注册一个新的有效条目名称。预期用法是
//
// var SomeFeature = framework.ValidFeatures.Add("Some")
//
// 在 E2E suite 的 init 阶段调用。单个测试不应注册
// 自己的条目，以免新条目失控地增殖。E2E suite 可以
// 但不必通过冻结有效名称集合来强制这一点。
func (v *Valid[T]) Add(item T) T {
	if v.frozen {
		RecordBug(NewBug(fmt.Sprintf(`registry %T is already frozen, "%v" must not be added anymore`, *v, item), 1))
	}
	if v.items == nil {
		v.items = sets.New[T]()
	}
	if v.items.Has(item) {
		RecordBug(NewBug(fmt.Sprintf(`registry %T already contains "%v", it must not be added again`, *v, item), 1))
	}
	v.items.Insert(item)
	return item
}

func (v *Valid[T]) Freeze() {
	v.frozen = true
}

// 这些变量包含 [WithFeature] 和 [WithEnvironment] 接受的参数。
// 框架本身没有预定义的
// 常量。测试套件和测试可以自定义常量，并在调用这些 With 函数前
// 把它们加进来。
var (
	ValidFeatures     Valid[Feature]
	ValidEnvironments Valid[Environment]
)

var errInterface = reflect.TypeOf((*error)(nil)).Elem()

// IgnoreNotFound 可用于把任意函数包装进
// [ginkgo.DeferCleanup] 调用。当被包装函数返回的错误被
// `apierrors.IsNotFound` 视为"未找到"时，忽略该错误，
// 而不是在清理阶段让测试失败。适用于只需确保某对象
// 已不存在的清理代码。
func IgnoreNotFound(in any) any {
	inType := reflect.TypeOf(in)
	inValue := reflect.ValueOf(in)
	return reflect.MakeFunc(inType, func(args []reflect.Value) []reflect.Value {
		var out []reflect.Value
		if inType.IsVariadic() {
			out = inValue.CallSlice(args)
		} else {
			out = inValue.Call(args)
		}
		if len(out) > 0 {
			lastValue := out[len(out)-1]
			last := lastValue.Interface()
			if last != nil && lastValue.Type().Implements(errInterface) && apierrors.IsNotFound(last.(error)) {
				out[len(out)-1] = reflect.Zero(errInterface)
			}
		}
		return out
	}).Interface()
}

// AnnotatedLocation 可用于提供更详细的源代码
// 位置信息，方法是把结果作为额外参数传给
// BeforeEach/AfterEach/DeferCleanup/It 等。
func AnnotatedLocation(annotation string) types.CodeLocation {
	return AnnotatedLocationWithOffset(annotation, 1)
}

// AnnotatedLocationWithOffset 会跳过额外的调用栈层级。offset 为 0 时
// 与 [AnnotatedLocation] 相同。
func AnnotatedLocationWithOffset(annotation string, offset int) types.CodeLocation {
	codeLocation := types.NewCodeLocation(offset + 1)
	codeLocation.FileName = path.Base(codeLocation.FileName)
	codeLocation = types.NewCustomCodeLocation(annotation + " | " + codeLocation.String())
	return codeLocation
}

// SIGDescribe 返回 ginkgo.Describe 的包装函数，把
// SIG 名称作为注解注入。参数应为小写、
// 无空格且不带 sig- 或 SIG- 前缀。
func SIGDescribe(sig string) func(...interface{}) bool {
	ginkgo.GinkgoHelper()
	if !sigRE.MatchString(sig) || strings.HasPrefix(sig, "sig-") {
		RecordBug(NewBug(fmt.Sprintf("SIG label must be lowercase, no spaces and no sig- prefix, got instead: %q", sig), 0))
	}
	return func(args ...interface{}) bool {
		ginkgo.GinkgoHelper()
		args = append([]interface{}{WithLabel("sig-" + sig)}, args...)
		return registerInSuite(ginkgo.Describe, args)
	}
}

var sigRE = regexp.MustCompile(`^[a-z]+(-[a-z]+)*$`)

// ConformanceIt 是 ginkgo It 的包装函数。添加 "[Conformance]" 标签并简化静态分析。
func ConformanceIt(args ...interface{}) bool {
	args = append(args, ginkgo.Offset(1), WithConformance())
	return It(args...)
}

// It 是 [ginkgo.It] 的包装器，取消了参数
// 必须以文本字符串开头的要求。
// 文本和参数可以混用。最终文本是文本参数与 With 函数中特殊标签
// 的拼接。
func It(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.It, args)
}

// It 是对应包函数的简写。
func (f *Framework) It(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.It, args)
}

// Describe 是 [ginkgo.Describe] 的包装器，取消了参数
// 必须以文本字符串开头的要求。
// 文本和参数可以混用。最终文本是文本参数与 With 函数中特殊标签
// 的拼接。
func Describe(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.Describe, args)
}

// Describe 是对应包函数的简写。
func (f *Framework) Describe(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.Describe, args)
}

// Context 是 [ginkgo.Context] 的包装器，取消了参数
// 必须以文本字符串开头的要求。
// 文本和参数可以混用。最终文本是文本参数与 With 函数中特殊标签
// 的拼接。
func Context(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.Context, args)
}

// Context 是对应包函数的简写。
func (f *Framework) Context(args ...interface{}) bool {
	ginkgo.GinkgoHelper()
	return registerInSuite(ginkgo.Context, args)
}

// registerInSuite 是所有包装函数的通用实现。它
// 预期通过一个中间包装器调用。
func registerInSuite(ginkgoCall func(string, ...interface{}) bool, args []interface{}) bool {
	var offset ginkgo.Offset
	for arg := range allArgs(args) {
		if o, ok := arg.(ginkgo.Offset); ok {
			offset = o
		}
	}
	offset += 2 // This function and the top-level wrapper.
	return ginkgoCall("", args, offset)
}

// allArgs 生成一个迭代器，处理嵌套而不展开切片。
func allArgs(args []any) func(yield func(arg any) bool) {
	return func(yield func(arg any) bool) {
		iterArgs(args, yield)
	}
}

// iterArgs 递归下降到 []any 中，对其他所有参数调用 yield。
func iterArgs(args []any, yield func(arg any) bool) bool {
	for _, arg := range args {
		switch arg := arg.(type) {
		case []any:
			if !iterArgs(arg, yield) {
				return false
			}
		default:
			if !yield(arg) {
				return false
			}
		}
	}
	return true
}

// 如果使用了 framework，用户可能也会使用我们的特殊测试
// 参数。为确保测试注册正常，我们把代码注入
// 测试树构建过程。因此用 ginkgo.It 还是 framework.It
// 都无关紧要了。
//
// 代码流程如下：
//   - init 注册 transformGinkgoNodeArgs。
//     从此以后，所有 ginkgo.Describe/Context/It 调用都会调用
//     transformGinkgoNodeArgs，它把我们的特殊参数替换为
//     Ginkgo 能处理的内容。
//   - 顶层 Describe 调用注册含更多测试定义的回调。
//     此时回调尚未被调用。
//   - 参数解析。
//   - Ginkgo 递归调用回调，得到完整的测试树。
//   - Ginkgo 运行这些测试。
//
// 这个 init 保证在我们的特殊参数函数
// 被调用之前发生，因为它就是这些包的 init 代码。
// 不使用 framework 的代码可能在 transformGinkgoNodeArgs 注册前
// 就调用 ginkgo.Describe。这没问题，因为那样参数都不可能是特殊的。
func init() {
	ginkgo.AddTreeConstructionNodeArgsTransformer(transformGinkgoNodeArgs)
}

func transformGinkgoNodeArgs(nodeType types.NodeType, offset ginkgo.Offset, text string, args []any) (string, []any, []error) {
	text, args = expandGinkgoArgs(nodeType == types.NodeTypeIt, offset+1, text, args)
	return text, args, nil
}

// leafNodeLabels 包含这样的标签：
// - 构建 spec 树时可能被多次添加
// - 只需显示一次
// - 很可能不被视为完整测试名字符串的一部分
//
// 如果把它们多次直接插入触发添加位置的文本中，它们只会作为
// 标签（不可见）添加，文本则根据之前添加的标签
// 在叶节点文本（即 ginkgo.It 名称）末尾
// 添加一次。
//
// 初始集合包含 E2E framework 定义的标签。Feature
// gate 稳定性文本（Alpha、Beta 等）和 feature gate 依赖
// 在遇到时添加，
// 以免对这些字符串的含义做假设，或因为它们
// 根本无法预先得知。
var leafNodeLabels = sets.New[string](
	"Conformance",
	"Disruptive",
	"Feature:OffByDefault",
	"Flaky",
	"NodeConformance",
	"Serial",
	"Slow",
)

// expandGinkgoArgs 拼接所有字符串，并把我们的自定义
// 参数转换为 Ginkgo 能处理的内容。它经由
// ginkgo.AddTreeConstructionNodeArgsTransformer 调用。
func expandGinkgoArgs(leafNode bool, offset ginkgo.Offset, text string, args []any) (string, []any) {
	var ginkgoArgs []interface{}
	var texts []string

	if text != "" {
		texts = append(texts, text)
	}
	previousText := ""

	// 叶节点的所有标签，包括来自父节点的和本次调用添加的。
	allLabels := sets.New[string]()
	if leafNode {
		// 只能在树构建期间调用，即不能用于顶层节点，
		// 所以要小心。
		report := ginkgo.CurrentTreeConstructionNodeReport()
		previousText = report.FullText()
		allLabels = sets.New[string](report.Labels()...)
	}
	addLabel := func(label string) {
		ginkgoArgs = append(ginkgoArgs, ginkgo.Label(label))
		allLabels.Insert(label)
	}

	featureDependencies := utilfeature.DefaultMutableFeatureGate.Dependencies()

	// addFeatureGate 处理 feature gate：
	// - 直接命名的 feature gate 的 [Feature:<name>] 文本
	//   就地插入，因为测试名称可能依赖它。
	// - 依赖项的文本添加在末尾（附加信息）。
	// - [Beta] 之类的元数据总是添加在末尾。
	var addFeatureGate func(name featuregate.Feature, spec featuregate.FeatureSpec, direct bool)
	addFeatureGate = func(name featuregate.Feature, spec featuregate.FeatureSpec, direct bool) {
		fullLabel := "FeatureGate:" + string(name)
		addLabel(fullLabel)
		if direct {
			texts = append(texts, fmt.Sprintf("[%s]", fullLabel))
		} else {
			leafNodeLabels.Insert(fullLabel)
		}

		// 我们用混合大小写（即 Beta 而非 BETA）。GA feature gate 没有级别字符串。
		var level string
		if spec.PreRelease != "" {
			level = string(spec.PreRelease)
			level = strings.ToUpper(level[0:1]) + strings.ToLower(level[1:])
			addLabel(level)
			leafNodeLabels.Insert(level)
		}
		if !spec.Default {
			addLabel("Feature:OffByDefault")
		}
		if level == "Beta" && !spec.Default {
			// 不嵌入文本中！
			ginkgoArgs = append(ginkgoArgs, ginkgo.Label("BetaOffByDefault"))
		}

		// 也递归添加依赖项。
		for _, name := range featureDependencies[name] {
			spec := utilfeature.DefaultMutableFeatureGate.GetAll()[name]
			addFeatureGate(name, spec, false)
		}
	}

	var providerChecks [][]string
	skipUnlessProviderIs := func() {
		for _, supportedProviders := range providerChecks {
			if !ProviderIs(supportedProviders...) {
				ginkgo.Skip(fmt.Sprintf("Only supported for providers %v (not %s)", supportedProviders, TestContext.Provider))
			}
		}
	}
	injectSkipUnlessProviderIs := func() {
		if leafNode {
			// 直接在 It 中检查。
			skipUnlessProviderIs()
		} else {
			// 插入带相同检查的 BeforeEach。
			ginkgo.BeforeEach(skipUnlessProviderIs)
		}
	}

	haveEmptyStrings := false
	for _, arg := range args {
		switch arg := arg.(type) {
		case featureGate:
			addFeatureGate(arg.name, arg.spec, true)
		case label:
			fullLabel := arg.String()
			if arg.parts[0] == "Provider" {
				providerChecks = append(providerChecks, strings.Split(arg.parts[1], ","))
				// 只添加文本，不作为标签：Ginkgo 标签不能包含逗号。
				// 可以拆成多个标签，但语义会不明确：
				// [Provider:gce,aws] 表示"provider 必须是这两者之一"，
				// 但 [Provider:gce] [Provider:aws] 可能是那个意思，也可能是"必须同时是
				// gce 和 aws"，即测试总是需要跳过（例如
				// 因为不同地方有独立的 WithProvider 调用）。
				texts = append(texts, fmt.Sprintf("[%s]", fullLabel))
				continue
			}
			addLabel(fullLabel)
			if !leafNodeLabels.Has(fullLabel) {
				texts = append(texts, fmt.Sprintf("[%s]", fullLabel))
			}
			if arg.parts[0] == "KubeletMinVersion" {
				ginkgoArgs = append(ginkgoArgs, ginkgo.ComponentSemVerConstraint("kubelet", ">="+arg.parts[1]))
			}
			switch fullLabel {
			case "Serial":
				ginkgoArgs = append(ginkgoArgs, ginkgo.Serial)
			case "Slow":
				// 先启动慢测试。这样可避免它们
				// 在一轮运行的末尾才启动，
				// 从而拖长整体运行时间。
				ginkgoArgs = append(ginkgoArgs, ginkgo.SpecPriority(1))
			}
		case string:
			if arg == "" {
				haveEmptyStrings = true
			}
			texts = append(texts, arg)
		case func(context.Context):
			// 包装会出现在栈回溯中。尽量避免。
			if len(providerChecks) > 0 {
				body := arg
				arg = func(ctx context.Context) {
					injectSkipUnlessProviderIs()
					body(ctx)
				}
			}
			ginkgoArgs = append(ginkgoArgs, arg)
		case func(ginkgo.SpecContext):
			if len(providerChecks) > 0 {
				body := arg
				arg = func(ctx ginkgo.SpecContext) {
					injectSkipUnlessProviderIs()
					body(ctx)
				}
			}
			ginkgoArgs = append(ginkgoArgs, arg)
		case func():
			if len(providerChecks) > 0 {
				body := arg
				arg = func() {
					injectSkipUnlessProviderIs()
					body()
				}
			}
			ginkgoArgs = append(ginkgoArgs, arg)
		default:
			ginkgoArgs = append(ginkgoArgs, arg)
		}
	}

	if haveEmptyStrings {
		RecordBug(NewBug("empty strings as separators are unnecessary and need to be removed", int(offset)))
	}

	// 强制文本片段不以空格开头或结尾，因为
	// 下面的拼接会产生双空格。
	for _, text := range texts {
		if strings.HasPrefix(text, " ") || strings.HasSuffix(text, " ") {
			RecordBug(NewBug(fmt.Sprintf("trailing or leading spaces are unnecessary and need to be removed: %q", text), int(offset)))
		}
	}

	// 确保每个叶节点文本包含目前收集到的所有附加标签。
	// 这些标签来自与容器节点关联的标签集合。
	if leafNode {
		var delayedLabels []string

		for label := range allLabels {
			if leafNodeLabels.Has(label) {
				delayedLabels = append(delayedLabels, label)
			}
		}

		slices.Sort(delayedLabels)
		for _, label := range delayedLabels {
			text := fmt.Sprintf("[%s]", label)
			if strings.Contains(previousText, text) ||
				slices.Contains(texts, text) {
				// 末尾不要重复前面已包含的文本。
				// 实践中 FeatureGate 可能既是显式提及又是依赖项，
				// 这时就会发生重复。
				continue
			}
			texts = append(texts, text)
			// 这样 validateText 就满意了。
			ginkgoArgs = append(ginkgoArgs, ginkgo.Label(label))
		}
	}

	text = strings.Join(texts, " ")
	return text, ginkgoArgs
}

var (
	tagRe                 = regexp.MustCompile(`\[.*?\]`)
	deprecatedTags        = sets.New("Conformance", "Flaky", "NodeConformance", "Disruptive", "Serial", "Slow")
	deprecatedTagPrefixes = sets.New("Environment", "Feature", "NodeFeature", "FeatureGate")
	deprecatedStability   = sets.New("Alpha", "Beta")
)

// validateSpecs 检查测试 spec 是否按预期注册。
func validateSpecs(specs types.SpecReports) {
	checked := sets.New[call]()

	// 每个完整测试名只应使用一次。
	specNames := make(map[string][]types.SpecReport)

	for _, spec := range specs {
		for i, text := range spec.ContainerHierarchyTexts {
			c := call{
				text:     text,
				location: spec.ContainerHierarchyLocations[i],
			}
			if checked.Has(c) {
				// 同一个容器不需要检查多次。
				continue
			}
			checked.Insert(c)
			validateText(c.location, text, spec.ContainerHierarchyLabels[i])
		}
		c := call{
			text:     spec.LeafNodeText,
			location: spec.LeafNodeLocation,
		}
		if !checked.Has(c) {
			validateText(spec.LeafNodeLocation, spec.LeafNodeText, spec.LeafNodeLabels)
			checked.Insert(c)
		}

		// 记录同名被用于什么。空名在特殊节点
		// （如 ReportAfterSuite）上会被多次使用。
		fullText := spec.FullText()
		if fullText != "" {
			specNames[fullText] = append(specNames[fullText], spec)
		}
	}

	for fullText, specs := range specNames {
		if len(specs) > 1 {
			// 完全相同的 It 调用可能出现两次，此时
			// 两个不同 spec 的完整文本和位置相同。我们显示为
			// "<location> (2x)"
			locationCounts := make(map[string]int)
			for _, spec := range specs {
				locationCounts[spec.LeafNodeLocation.String()]++
			}
			var locationTexts []string
			for locationText, count := range locationCounts {
				text := locationText
				if count > 1 {
					text += fmt.Sprintf(" (%dx)", count)
				}
				locationTexts = append(locationTexts, text)
			}
			recordTextBug(specs[0].LeafNodeLocation, fmt.Sprintf("full test name is not unique: %q (%s)", fullText, strings.Join(locationTexts, ", ")))
		}
	}
}

// call 作为 Describe 或 Context 等容器节点调用的（近似）唯一标识符。
// 它并不完美，因为理论上同一行可能有多个文本相同的调用，
// 但实践中这不是问题。
type call struct {
	text     string
	location types.CodeLocation
}

// validateText 检查一些不应再通过
// 纯文本字符串添加的已知标签。最终所有这些标签都应替换
// 为新 API。
func validateText(location types.CodeLocation, text string, labels []string) {
	for _, tag := range tagRe.FindAllString(text, -1) {
		if tag == "[]" {
			recordTextBug(location, "[] in plain text is invalid")
			continue
		}
		// 去掉方括号。
		tag = tag[1 : len(tag)-1]
		if slices.Contains(labels, tag) {
			// 好的，也被设为标签了。
			continue
		}
		// TODO: 目前我们只把它设为文本值
		// 也许应该反映到标签中，但那可能破坏一些
		// 现有 job，而且我们仍按精确计划设置
		if tag == "Feature:OffByDefault" {
			continue
		}
		if deprecatedTags.Has(tag) {
			recordTextBug(location, fmt.Sprintf("[%s] in plain text is deprecated and must be added through With%s instead", tag, tag))
		}
		if deprecatedStability.Has(tag) {
			if slices.Contains(labels, "Feature:"+tag) {
				// 好的，也被设为标签了。
				continue
			}
			recordTextBug(location, fmt.Sprintf("[%s] in plain text is deprecated and must be added by defining the feature gate through WithFeatureGate instead", tag))
		}
		if index := strings.Index(tag, ":"); index > 0 {
			prefix := tag[:index]
			if deprecatedTagPrefixes.Has(prefix) {
				recordTextBug(location, fmt.Sprintf("[%s] in plain text is deprecated and must be added through With%s(%s) instead", tag, prefix, tag[index+1:]))
			}
		}
	}
}

func recordTextBug(location types.CodeLocation, message string) {
	RecordBug(Bug{FileName: location.FileName, LineNumber: location.LineNumber, Message: message})
}

// WithFeature 表示某个测试或一组测试只有在
// 某项特性可用时才有效。返回值可作为额外
// 参数传给 framework 包装器和直接用 Ginkgo 函数。
//
// 该特性必须列在 ValidFeatures 中。
func WithFeature(name Feature) interface{} {
	return withFeature(name)
}

// WithFeature 是对应包函数的简写。
func (f *Framework) WithFeature(name Feature) interface{} {
	return withFeature(name)
}

func withFeature(name Feature) interface{} {
	if !ValidFeatures.items.Has(name) {
		RecordBug(NewBug(fmt.Sprintf("WithFeature: unknown feature %q", name), 2))
	}
	return newLabel("Feature", string(name))
}

// WithFeatureGate 表示某个测试或一组测试依赖于某个
// feature gate 及对应的 API group（如果存在）
// 被启用。返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
//
// feature gate 必须列在
// [k8s.io/apiserver/pkg/util/feature.DefaultMutableFeatureGate] 中。一旦
// feature gate 从中移除，使用它的 WithFeatureGate 调用
// 也需要移除。
//
// 根据特性当前的稳定级别，[Alpha] 或 [Beta] 会自动加到测试名中，
// 以模拟历史上
// 这些标签的用法。
//
// 对标签过滤，Alpha 或 Beta 会被加入 Ginkgo 标签。
//
// 添加 [Feature:OffByDefault] 是为了支持在使用传统
// \[Feature:.*\] 跳过正则表达式的 job 中，跳过依赖
// alpha 或 beta feature gate 的测试。
//
// Feature:OffByDefault 也可用于标签过滤。
//
// feature gate 是默认关闭的 beta 特性时，还会*仅作为标签*添加
// BetaOffByDefault。这可用于按 beta 加默认状态
// 来包含或排除测试。Alpha 没有等价标签，因为所有 alpha 都是
// 默认关闭的。
//
// 如果测试能在任何启用了 alpha 或 beta 特性的集群上
// 运行（相应 API group 也已启用），只用 WithFeatureGate 标注
// 就足够。否则必须用 WithFeature 定义额外的
// 要求。
func WithFeatureGate(featureGate featuregate.Feature) interface{} {
	return withFeatureGate(featureGate)
}

// WithFeatureGate 是对应包函数的简写。
func (f *Framework) WithFeatureGate(featureGate featuregate.Feature) interface{} {
	return withFeatureGate(featureGate)
}

func withFeatureGate(name featuregate.Feature) interface{} {
	spec, ok := utilfeature.DefaultMutableFeatureGate.GetAll()[name]
	if !ok {
		RecordBug(NewBug(fmt.Sprintf("WithFeatureGate: the feature gate %q is unknown", name), 2))
	}
	return featureGate{name, spec}
}

type featureGate struct {
	name featuregate.Feature
	spec featuregate.FeatureSpec
}

// WithEnvironment 表示某个测试或一组测试只能在
// 特定环境中运行。返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
//
// 环境必须列在 ValidEnvironments 中。
func WithEnvironment(name Environment) interface{} {
	return withEnvironment(name)
}

// WithEnvironment 是对应包函数的简写。
func (f *Framework) WithEnvironment(name Environment) interface{} {
	return withEnvironment(name)
}

func withEnvironment(name Environment) interface{} {
	if !ValidEnvironments.items.Has(name) {
		RecordBug(NewBug(fmt.Sprintf("WithEnvironment: unknown environment %q", name), 2))
	}
	return newLabel("Environment", string(name))
}

// WithProvider 表示某个测试或一组测试依赖于
// 仅在所列 cluster provider 之一上可用的特性。
// 它把 [Provider: <provider1>,<provider2>, ...] 标签插入测试名
// 并在函数开头注入运行时跳过检查。
//
// 按 provider 过滤是语义不明确的遗留机制：哪些
// provider 支持哪些特性并未定义。更推荐定义特性并
// 改用 WithFeature。
//
// 注意在 Describe/Context/It 调用中它必须放在函数之前！
func WithProvider(providers ...string) interface{} {
	return withProvider(providers)
}

// WithProvider 是对应包函数的简写。
func (f *Framework) WithProvider(providers ...string) interface{} {
	return withProvider(providers)
}

func withProvider(providers []string) interface{} {
	return newLabel("Provider", strings.Join(providers, ","))
}

// WithConformance 表示某个测试或一组测试必须在
// 所有符合规范的 Kubernetes 集群中通过。返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
func WithConformance() interface{} {
	return withConformance()
}

// WithConformance 是对应包函数的简写。
func (f *Framework) WithConformance() interface{} {
	return withConformance()
}

func withConformance() interface{} {
	return newLabel("Conformance")
}

// WithNodeConformance 表示某个 node 功能测试或一组测试
// 不依赖特定运行时或 Kubernetes 发行版的
// 行为。返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
func WithNodeConformance() interface{} {
	return withNodeConformance()
}

// WithNodeConformance 是对应包函数的简写。
func (f *Framework) WithNodeConformance() interface{} {
	return withNodeConformance()
}

func withNodeConformance() interface{} {
	return newLabel("NodeConformance")
}

// WithDisruptive 表示某个测试或一组测试会暂时
// 影响 Kubernetes 集群的功能。返回值必须
// 作为额外参数传给 [framework.It]、[framework.Describe]、
// [framework.Context]。
func WithDisruptive() interface{} {
	return withDisruptive()
}

// WithDisruptive 是对应包函数的简写。
func (f *Framework) WithDisruptive() interface{} {
	return withDisruptive()
}

func withDisruptive() interface{} {
	return newLabel("Disruptive")
}

// WithSerial 表示某个测试或一组测试不得与
// 其他测试并行运行。返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
//
// 从 ginkgo v2 开始，串行和并行测试可以在
// 同一次调用中执行。Ginkgo 自身会确保串行测试
// 顺序执行。
func WithSerial() interface{} {
	return withSerial()
}

// WithSerial 是对应包函数的简写。
func (f *Framework) WithSerial() interface{} {
	return withSerial()
}

func withSerial() interface{} {
	return newLabel("Serial")
}

// WithSlow 表示某个测试或一组测试中的每个测试
// 都很慢（在 CI 中运行预计超过 5 分钟）。
// 返回值可作为额外参数
// 传给 framework 包装器和直接用 Ginkgo 函数。
func WithSlow() interface{} {
	return withSlow()
}

// WithSlow 是对应包函数的简写。
func (f *Framework) WithSlow() interface{} {
	return WithSlow()
}

func withSlow() interface{} {
	return newLabel("Slow")
}

// WithLabel 是 [ginkgo.Label] 的包装器。除了给测试添加任意
// 标签外，它还把标签以方括号形式注入测试
// 名称。
func WithLabel(label string) interface{} {
	return withLabel(label)
}

// WithLabel 是对应包函数的简写。
func (f *Framework) WithLabel(label string) interface{} {
	return withLabel(label)
}

func withLabel(label string) interface{} {
	return newLabel(label)
}

// WithFlaky 表示某个测试或一组测试会随机失败。
// 这些测试通常会被过滤出来，与其他测试分开运行。
func WithFlaky() interface{} {
	return withFlaky()
}

// WithFlaky 是对应包函数的简写。
func (f *Framework) WithFlaky() interface{} {
	return withFlaky()
}

func withFlaky() interface{} {
	return newLabel("Flaky")
}

// WithKubeletMinVersion 表示某个测试或一组测试需要
// kubelet 版本 >= 给定版本字符串。把最低版本写成
// `<major>.<minor>` 就够，patch 版本可以
// 加上但不是必需的。
//
// 这会添加
// - 文本中的 `[KubeletMinVersion:<version>]` 标签
// - `KubeletMinVersion:<version>` 标签
// - 以及 "kubelet" 组件的 Ginkgo semver 约束。
//
// 过滤测试最简单的方式是用 `ginkgo
// --sem-ver-filter="kubelet=1.35"`，它会过滤掉需要更新
// kubelet 的测试。
func WithKubeletMinVersion(version string) interface{} {
	return withKubeletMinVersion(version)
}

// WithKubeletMinVersion 是对应包函数的简写。
func (f *Framework) WithKubeletMinVersion(version string) interface{} {
	return withKubeletMinVersion(version)
}

func withKubeletMinVersion(version string) interface{} {
	return newLabel("KubeletMinVersion", version)
}

// label 必须可比较，有代码用它检测 feature.Windows。
type label struct {
	// 各部分用 ":" 连接构成完整标签。
	parts [2]string
	len   int
}

func (l label) String() string {
	if l.len == 2 {
		return l.parts[0] + ":" + l.parts[1]
	}
	return l.parts[0]
}

func newLabel(parts ...string) label {
	switch len(parts) {
	case 1:
		return label{parts: [2]string{parts[0], ""}, len: 1}
	case 2:
		return label{parts: [2]string{parts[0], parts[1]}, len: 2}
	default:
		panic(fmt.Sprintf("invalid number of part strings: %v", parts))
	}
}
