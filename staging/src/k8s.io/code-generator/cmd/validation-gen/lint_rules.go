/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

// checkAlphaBetaUsage 递归校验 +k8s:alpha/+k8s:beta 只能出现在标签链的最外层：
// 它们修饰的是其 ValueTag 指向的验证标签，嵌套使用会让稳定性语义含糊。
// isRoot 为 false 表示已处在 ValueTag 内部。
func checkAlphaBetaUsage(tag codetags.Tag, isRoot bool) (string, error) {
	if tag.Name == "k8s:alpha" || tag.Name == "k8s:beta" {
		if !isRoot {
			return fmt.Sprintf("tag %q can't be used in between", tag.Name), nil
		}
		// 稳定性前缀必须携带被修饰的验证标签，否则没有任何作用对象。
		if tag.ValueTag == nil {
			return fmt.Sprintf("tag %q requires a validation tag as its value payload", tag.Name), nil
		}
	}

	if tag.ValueTag != nil {
		return checkAlphaBetaUsage(*tag.ValueTag, false)
	}
	return "", nil
}

// alphaBetaPrefix 是另一条规则：+k8s:alpha 与 +k8s:beta 必须位于标签链最外层，
// 作为内部验证标签的前缀。
func alphaBetaPrefix() lintRule {
	return func(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
		for _, tag := range tags {
			// 只检查 alpha/beta 标签或校验标签。
			if msg, err := checkAlphaBetaUsage(tag, true); err != nil || msg != "" {
				return msg, err
			}
		}
		return "", nil
	}
}

// checkTagStability 递归检查某个标签及其嵌套标签是否满足当前上下文的稳定性要求。
// 未知标签（非声明式验证标签）没有稳定性元数据，直接放行交给其他规则处理。
func checkTagStability(tag codetags.Tag, contextLevel validators.TagStabilityLevel) (string, error) {
	tagStability, err := validators.GetStability(tag.Name)
	// 所有 DV（声明式校验）标签都带稳定性，没有稳定性取值的不是合法 DV 标签。
	if err != nil {
		return "", nil
	}
	cmpOrder, err := tagStability.Compare(contextLevel)
	if err != nil {
		return "", err
	}
	// 标签稳定性低于上下文允许的最低级别时拒绝，例如在 Stable 上下文中使用 Alpha 标签。
	if cmpOrder < 0 {
		return fmt.Sprintf("tag %q with stability level %q cannot be used in %s validation", tag.Name, tagStability, contextLevel), nil
	}
	if tag.ValueTag != nil {
		return checkTagStability(*tag.ValueTag, contextLevel)
	}
	return "", nil
}

// validationStability 对标签施加稳定性级别约束。
func validationStability() lintRule {
	return func(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
		pkgPath := t.Name.Package
		if container != nil {
			pkgPath = container.Name.Package
		}

		// 不带前缀的验证标签通常要求是 Stable 级。
		// 在 Alpha 包里允许不带前缀使用 Alpha 级和 Beta 级验证标签；
		// 在 Beta 包里允许不带前缀使用 Beta 级验证标签。
		// 包路径末段（如 v1alpha1）决定了默认的稳定性上下文。
		defaultContextLevel := validators.TagStabilityLevelStable
		// APIVersion 取自包路径的最后一段。
		apiVersion := path.Base(pkgPath)
		if strings.Contains(apiVersion, "alpha") {
			defaultContextLevel = validators.TagStabilityLevelAlpha
		} else if strings.Contains(apiVersion, "beta") {
			defaultContextLevel = validators.TagStabilityLevelBeta
		}

		for _, tag := range tags {
			contextLevel := defaultContextLevel
			tagToCheck := tag

			// 稳定性级别标签要为内层校验设置稳定性上下文，覆盖包级默认值。
			if tag.Name == "k8s:alpha" || tag.Name == "k8s:beta" {
				if tag.Name == "k8s:alpha" {
					contextLevel = validators.TagStabilityLevelAlpha
				} else {
					contextLevel = validators.TagStabilityLevelBeta
				}
				if tag.ValueTag == nil {
					continue
				}
				tagToCheck = *tag.ValueTag
			}

			// 对特性门控标签，允许开发者在其中嵌套 beta 级验证标签，而不必强迫验证作者
			// 额外手写代码（那会绕过声明式验证的等价性检查）：当前上下文为 Stable 时，
			// 自动把稳定性上下文放宽为 Beta，特性门控内的 beta 验证因此不必手写等价实现。
			if tagToCheck.Name == "k8s:ifEnabled" || tagToCheck.Name == "k8s:ifDisabled" {
				if contextLevel == validators.TagStabilityLevelStable {
					contextLevel = validators.TagStabilityLevelBeta
				}
			}

			msg, err := checkTagStability(tagToCheck, contextLevel)
			if err != nil {
				return "", err
			}
			if msg != "" {
				return msg, nil
			}
		}
		return "", nil
	}
}

// hasTag 递归检查标签树中是否存在给定名称的标签。
// 条件标签（如 ifEnabled）的载荷里可能还有验证标签，必须一并检查。
func hasTag(tags []codetags.Tag, name string) bool {
	for _, tag := range tags {
		if tag.Name == name {
			return true
		}
		// 同时检查条件标签的取值。
		if tag.ValueTag != nil && hasTag([]codetags.Tag{*tag.ValueTag}, name) {
			return true
		}
	}
	return false
}

// hasRequirednessTag 报告标签中是否含 +k8s:optional、+k8s:required 或 +k8s:forbidden。
func hasRequirednessTag(tags []codetags.Tag) bool {
	return hasTag(tags, "k8s:optional") || hasTag(tags, "k8s:required") || hasTag(tags, "k8s:forbidden")
}

// tags 中包含任何已注册且不是 opaqueType 的验证标签时，hasNonOpaqueValidationTag 返回 true。
// chainTags 中的标签会展开其 ValueTag 继续检查；其余已知验证标签即视为存在验证。
func hasNonOpaqueValidationTag(extractor validators.ValidationExtractor, chainTags sets.Set[string], tags []codetags.Tag) bool {
	for _, tag := range tags {
		if tag.Name == "k8s:optional" || tag.Name == "k8s:opaqueType" {
			continue
		}
		if chainTags.Has(tag.Name) {
			if tag.ValueTag != nil && hasNonOpaqueValidationTag(extractor, chainTags, []codetags.Tag{*tag.ValueTag}) {
				return true
			}
			continue
		}
		// 检查是否为已知的校验标签。
		if extractor.IsKnownTag(tag.Name) {
			return true
		}
	}
	return false
}

// requiredAndOptional 检查带校验（直接或传递）的指针、slice、map、数组字段
// 是否显式声明了 +k8s:optional 或 +k8s:required。
//
// 规则动机：声明式验证生成器对“有验证但未声明可选性”的字段无法判断零值语义，
// 因此要求作者显式表态。判断“有验证”时必须穿透别名、容器元素和嵌套结构体，
// 因为验证可能只存在于元素的类型上。
func requiredAndOptional(extractor validators.ValidationExtractor) lintRule {
	// 这些标签的载荷是验证标签（如 alpha/beta/ifEnabled），需要展开而非直接比对。
	chainTags := sets.New[string]()
	for _, doc := range extractor.Docs() {
		if doc.PayloadsType == codetags.ValueTypeTag {
			chainTags.Insert(doc.Tag)
		}
	}

	// opacity 记录沿途遇到的 opaqueType/opaqueKeyType/opaqueValType：
	// 一旦某层被声明为不透明，就不再深入该层（其内部字段不属于本 API 的契约）。
	type opacity struct {
		typ, key, val bool
	}

	filterTags := func(tags []codetags.Tag) []codetags.Tag {
		var filtered []codetags.Tag
		for _, tag := range tags {
			if extractor.IsKnownTag(tag.Name) {
				filtered = append(filtered, tag)
			}
		}
		return filtered
	}

	// 缓存以“类型 + 不透明状态”为键；nil 值表示正在访问，用于检测递归类型形成的环。
	type cacheKey struct {
		t  *types.Type
		op opacity
	}
	hasValidation := make(map[cacheKey]*bool)

	// 返回 hasValidation、hasCycle 与 error。
	var hasTransitiveValidation func(t *types.Type, op opacity) (bool, bool, error)
	hasTransitiveValidation = func(t *types.Type, op opacity) (bool, bool, error) {
		if op.typ {
			return false, false, nil
		}

		ck := cacheKey{t, op}
		visitedVal, visited := hasValidation[ck]
		if visited {
			if visitedVal == nil {
				return false, true, nil // cycle detected
			}
			return *visitedVal, false, nil
		}
		hasValidation[ck] = nil

		tTags, err := extractor.ExtractTags(validators.Context{Scope: validators.ScopeType, Type: t}, t.CommentLines)
		if err != nil {
			return false, false, err
		}

		typeVals, err := extractor.ExtractValidations(
			validators.Context{Scope: validators.ScopeType, Type: t},
			filterTags(tTags)...,
		)
		if err != nil {
			return false, false, err
		}

		op.typ = op.typ || typeVals.OpaqueType
		op.key = op.key || typeVals.OpaqueKeyType
		op.val = op.val || typeVals.OpaqueValType

		if op.typ {
			hasVal := false
			hasValidation[ck] = &hasVal
			return false, false, nil
		}

		if typeVals.HasEmitable() {
			hasVal := true
			hasValidation[ck] = &hasVal
			return true, false, nil
		}

		// 各容器形态分别把不透明状态传递到对应的位置：
		// slice/array 传给元素；map 分别传给 key 与 value。
		var hasVal, cycleBroken bool
		switch t.Kind {
		case types.Alias:
			hasVal, cycleBroken, err = hasTransitiveValidation(t.Underlying, op)
		case types.Slice, types.Array:
			hasVal, cycleBroken, err = hasTransitiveValidation(t.Elem, opacity{typ: op.val})
		case types.Map:
			kVal, cbKey, err := hasTransitiveValidation(t.Key, opacity{typ: op.key})
			if err != nil {
				return false, false, err
			}
			eVal, cbVal, err := hasTransitiveValidation(t.Elem, opacity{typ: op.val})
			if err != nil {
				return false, false, err
			}
			hasVal = kVal || eVal
			cycleBroken = cbKey || cbVal
		case types.Pointer:
			hasVal, cycleBroken, err = hasTransitiveValidation(t.Elem, op)
		case types.Struct:
			for _, m := range t.Members {
				mTags, err := extractor.ExtractTags(validators.Context{Scope: validators.ScopeField, Type: m.Type}, m.CommentLines)
				if err != nil {
					return false, false, err
				}
				if hasNonOpaqueValidationTag(extractor, chainTags, mTags) {
					hasVal = true
					break
				}
				fieldVals, err := extractor.ExtractValidations(
					validators.Context{Scope: validators.ScopeField, Type: m.Type},
					filterTags(mTags)...,
				)
				if err != nil {
					return false, false, err
				}
				mOp := opacity{
					typ: op.typ || fieldVals.OpaqueType,
					key: op.key || fieldVals.OpaqueKeyType,
					val: op.val || fieldVals.OpaqueValType,
				}
				hv, cb, err := hasTransitiveValidation(m.Type, mOp)
				if err != nil {
					return false, false, err
				}
				cycleBroken = cycleBroken || cb
				if hv {
					hasVal = true
					break
				}
			}
		}

		if err != nil {
			return false, false, err
		}

		// 含环的结论可能依赖于进入环的位置，不缓存以免得到错误答案。
		if !cycleBroken {
			hasValidation[ck] = &hasVal
		} else {
			delete(hasValidation, ck)
		}
		return hasVal, cycleBroken, nil
	}

	return func(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
		// 只关心结构体中的字段；检查结构体本身时跳过。
		if container == nil || container.Kind != types.Struct || container == t {
			return "", nil
		}

		// 非指针结构体（及其别名）不支持必选性标签，直接跳过。
		underlying := t
		for underlying.Kind == types.Alias {
			underlying = underlying.Underlying
		}
		if underlying.Kind == types.Struct {
			return "", nil
		}

		// 检查是否已有必选性标签。
		if hasRequirednessTag(tags) {
			return "", nil
		}

		// 检查是否存在校验（直接或有效传递）。
		if hasNonOpaqueValidationTag(extractor, chainTags, tags) {
			return "field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden", nil
		}

		fieldVals, err := extractor.ExtractValidations(
			validators.Context{Scope: validators.ScopeField, Type: t},
			filterTags(tags)...,
		)
		if err != nil {
			return fmt.Sprintf("invalid validation tags: %v", err), nil
		}

		topOp := opacity{
			typ: fieldVals.OpaqueType,
			key: fieldVals.OpaqueKeyType,
			val: fieldVals.OpaqueValType,
		}

		hasTransitiveVal, _, err := hasTransitiveValidation(t, topOp)
		if err != nil {
			return fmt.Sprintf("invalid validation tags: %v", err), nil
		}

		if hasTransitiveVal {
			return "field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden", nil
		}

		return "", nil
	}
}

// lintRules 返回按顺序执行的 lint 规则集合。
// 顺序影响报错优先级：先检查标签用法（alpha/beta 前缀），再检查稳定性，最后检查可选性声明。
func lintRules(extractor validators.ValidationExtractor) []lintRule {
	return []lintRule{
		alphaBetaPrefix(),
		validationStability(),
		requiredAndOptional(extractor),
	}
}
