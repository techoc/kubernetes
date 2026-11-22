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

// Package validators 实现 validation-gen 的标记校验器框架与各类声明式校验。
//
// 每个校验器（TagValidator）负责一种标记（如 +k8s:required、+k8s:neq），
// 在 Init 时拿到 gengo 上下文，之后按 Scope（类型、字段、列表元素、map 键/值）
// 把标记翻译成可在生成代码中调用的校验函数。
//
// 注册发生在包 init 阶段，必须早于 InitGlobalValidator；后者一旦调用，
// 注册表即冻结，防止运行期出现非确定性的校验器集合。
package validators

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/generator"
)

// 这是标记校验器的全局注册表。为简单起见与实现放在同一包中，
// 但不应被直接使用，应通过包级函数访问。
var globalRegistry = &registry{
	tagValidators: map[string]TagValidator{},
}

// registry 保存已注册的标记及其索引。
type registry struct {
	lock        sync.Mutex
	initialized atomic.Bool // init() 是否已调用

	tagValidators map[string]TagValidator // 以标记名为键
	tagIndex      []string                // 全部标记名
}

// addTagValidator 注册一个校验器。
// 初始化之后禁止注册，否则生成结果的校验器集合会随调用时机变化。
func (reg *registry) addTagValidator(tv TagValidator) {
	if reg.initialized.Load() {
		panic("registry was modified after init")
	}

	reg.lock.Lock()
	defer reg.lock.Unlock()

	name := tv.TagName()
	if _, exists := globalRegistry.tagValidators[name]; exists {
		panic(fmt.Sprintf("tag %q was registered twice", name))
	}
	// 稳定性等级决定该标记能否在正式 API 中使用，必须显式声明。
	switch level := tv.Docs().StabilityLevel; level {
	case TagStabilityLevelAlpha, TagStabilityLevelBeta, TagStabilityLevelStable:
		// 合法
	case "":
		panic(fmt.Sprintf("tag %q is missing stability level", name))
	default:
		panic(fmt.Sprintf("tag %q has invalid stability level %q", name, level))
	}
	globalRegistry.tagValidators[name] = tv
}

// init 用生成上下文初始化全部校验器，并冻结注册表。
func (reg *registry) init(c *generator.Context, inputToCanonicalPkg map[string]string) {
	if reg.initialized.Load() {
		panic("registry.init() was called twice")
	}

	reg.lock.Lock()
	defer reg.lock.Unlock()

	cfg := Config{
		GengoContext:        c,
		TagValidator:        reg,
		InputToCanonicalPkg: inputToCanonicalPkg,
	}

	for _, tv := range globalRegistry.tagValidators {
		reg.tagIndex = append(reg.tagIndex, tv.TagName())
		tv.Init(cfg)
	}
	// 排序后 tagIndex 才能用于稳定的文档与遍历顺序。
	sort.Strings(reg.tagIndex)

	reg.initialized.Store(true)
}

// ExtractTags 从注释中提取已注册的校验标记，忽略其他标记。
func (reg *registry) ExtractTags(_ Context, comments []string) ([]codetags.Tag, error) {
	extracted := codetags.Extract("+", comments)
	var tags []codetags.Tag
	for tagName, lines := range extracted {
		// 只关心本包注册的标记，其他 +k8s:... 标记（如 +optional）不属于校验范畴。
		if !slices.Contains(reg.tagIndex, tagName) {
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

// ExtractValidations 根据给定上下文（如类型定义）求值已注册的校验器，
// 包括对所有类型生效的类型校验器，以及仅当注释块中出现特定标记时才运行的
// 标记校验器。匹配的校验器产生零个或多个校验项，稍后由代码生成逻辑渲染。
func (reg *registry) ExtractValidations(context Context, tags ...codetags.Tag) (Validations, error) {
	if !reg.initialized.Load() {
		panic("registry.init() was not called")
	}

	validations, err := reg.ExtractTagValidations(context, tags...)
	if err != nil {
		return Validations{}, err
	}

	return validations, nil
}

// ExtractTagValidations 逐个标记求值校验器，并合并结果。
func (reg *registry) ExtractTagValidations(context Context, tags ...codetags.Tag) (Validations, error) {
	if !reg.initialized.Load() {
		panic("registry.init() was not called")
	}
	accumulatedValidations := Validations{}
	tags = reg.sortTags(tags)
	for _, tag := range tags {
		tv := reg.tagValidators[tag.Name]
		// 此处可确定 tv 存在且非 nil，因为注册表在初始化时已校验过。
		// 作用域检查必须在此完成：把 +k8s:required 用在类型上是常见错误，
		// 越晚发现生成的代码越难排查。
		if scopes := tv.ValidScopes(); !scopes.Has(context.Scope) {
			return Validations{}, fmt.Errorf("tag %q cannot be specified on %s", tv.TagName(), context.Scope)
		}
		if err := typeCheck(tag, tv.Docs()); err != nil {
			return Validations{}, fmt.Errorf("tag %q: %w", tv.TagName(), err)
		}
		if theseValidations, err := tv.GetValidations(context, tag); err != nil {
			return Validations{}, fmt.Errorf("tag %q: %w", tv.TagName(), err)
		} else {
			accumulatedValidations.Add(theseValidations)
		}
	}
	return accumulatedValidations, nil
}

// sortTags 按标记名排序，保证生成结果确定。
// 必须先排序再生成校验：
//
// 部分标记是包装其他标记的“元标记”，例如：
//
//	// +k8s:validateFalse="111"
//	// +k8s:validateFalse="222"
//	// +k8s:ifEnabled(Foo)=+k8s:validateFalse="333"
//
// 标记提取会按标记名分组。前两个是 "k8s:validateFalse" 的实例，
// 第三个是 "k8s:ifEnabled" 的实例。
//
// 不排序时（map 迭代顺序随机），标记校验器的调用顺序不确定，
// 会导致生成的校验项顺序不确定。先按标记名排序可保证 "k8s:ifEnabled"
// 相对 "k8s:validateFalse" 的处理次序一致，使同一标记名的实例保持连续；
// 同名标记之间按出现顺序处理，因此相对顺序仍被保留。
func (reg *registry) sortTags(tags []codetags.Tag) []codetags.Tag {
	sortedTags := make([]codetags.Tag, len(tags))
	copy(sortedTags, tags)

	slices.SortFunc(sortedTags, func(a, b codetags.Tag) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return sortedTags
}

// Docs 返回注册表中每个标记的文档。
func (reg *registry) Docs() []TagDoc {
	var result []TagDoc
	for _, k := range reg.tagIndex {
		v := reg.tagValidators[k]
		result = append(result, v.Docs())
	}
	return result
}

// RegisterTagValidator 必须被调用，TagValidator 才会被 validation-gen 使用。
// 详见 TagValidator。
func RegisterTagValidator(tv TagValidator) {
	globalRegistry.addTagValidator(tv)
}

// TagValidationExtractor 表示校验器插件的聚合。
type TagValidationExtractor interface {
	// ExtractTagValidations 提取给定标记关联的全部校验。
	// 某些标记校验器可能更新内部状态并返回延迟生成的校验。
	ExtractTagValidations(context Context, Tags ...codetags.Tag) (Validations, error)
}

// ValidationExtractor 表示校验器插件的聚合（含标记提取与文档）。
type ValidationExtractor interface {

	// ExtractTags 从注释行中提取标记。
	ExtractTags(context Context, comments []string) ([]codetags.Tag, error)

	// ExtractValidations 根据给定上下文（如类型定义）求值已注册的校验器，
	// 包括对所有类型生效的类型校验器，以及仅当注释块中出现特定标记时才运行的
	// 标记校验器。匹配的校验器产生零个或多个校验项，稍后由代码生成逻辑渲染。
	ExtractValidations(context Context, Tags ...codetags.Tag) (Validations, error)

	// ValidationExtractor 同时也是 TagValidationExtractor。
	TagValidationExtractor

	// Docs 返回每个已知标记的文档。
	Docs() []TagDoc

	// Stability 返回给定标记的稳定性等级。
	Stability(tag string) (TagStabilityLevel, error)

	// IsKnownTag 判断标记是否为已注册的校验标记。
	IsKnownTag(tag string) bool
}

// Stability 返回给定标记的稳定性等级。
func (reg *registry) Stability(tag string) (TagStabilityLevel, error) {
	// 调用方可能带也可能不带前导 '+'，统一去掉后再查表。
	tagName := strings.TrimPrefix(tag, "+")
	tv, ok := reg.tagValidators[tagName]
	if !ok {
		return "", fmt.Errorf("tag %q doesn't have stability level", tag)
	}

	if tv.Docs().StabilityLevel == "" {
		return "", fmt.Errorf("tag %q doesn't have stability level", tag)
	}
	return tv.Docs().StabilityLevel, nil
}

// GetStability 从全局注册表返回给定标记的稳定性等级。
func GetStability(tag string) (TagStabilityLevel, error) {
	return globalRegistry.Stability(tag)
}

// IsKnownTag 判断标记是否已注册为校验标记。
func (reg *registry) IsKnownTag(tag string) bool {
	_, err := reg.Stability(tag)
	return err == nil
}

// IsKnownTag 判断给定标记是否为已注册的校验标记。
func IsKnownTag(tag string) bool {
	return globalRegistry.IsKnownTag(tag)
}

// InitGlobalValidator 必须由主程序恰好调用一次，以初始化并安全访问全局标记
// 注册表。调用之后不得再注册任何校验器。
func InitGlobalValidator(c *generator.Context, inputToCanonicalPkg map[string]string) ValidationExtractor {
	globalRegistry.init(c, inputToCanonicalPkg)
	return globalRegistry
}
