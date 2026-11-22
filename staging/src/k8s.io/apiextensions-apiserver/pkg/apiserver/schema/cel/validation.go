/*
Copyright 2021 The Kubernetes Authors.

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

package cel

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	celgo "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	"k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/metrics"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// Validator 与 schema.Structural 的结构平行，并为每个 schema 节点的
// x-kubernetes-validations 保存已编译的 CEL 程序。
type Validator struct {
	Items                *Validator
	Properties           map[string]Validator
	AllOfValidators      []*Validator
	AdditionalProperties *Validator

	Schema *schema.Structural

	uncompiledRules []apiextensions.ValidationRule
	compiledRules   []CompilationResult

	// 程序的编译在 CRD 创建/更新时已预检，按理不会失败；若此处重新编译后仍然失败，
	// 说明存在内部 bug。但真遇到编译错误时，记录下来并作为校验错误上报。
	compilationErr error

	// 当该校验节点对应资源根时 isResourceRoot 为 true。资源根要么是正在被校验的
	// 自定义资源的根，要么是某个 XEmbeddedResource 对象的根。
	isResourceRoot bool

	// celActivationFactory 生成 Activations，把标识符（例如 self 和 oldSelf）
	// 解析为 CEL 值。oldSelf 可选与不可选两种情况各需生成一个 activation。
	celActivationFactory func(sts *schema.Structural, obj, oldObj interface{}) (activation interpreter.Activation, optionalOldSelfActivation interpreter.Activation)
}

// NewValidator 编译 Structural schema 的 x-kubernetes-validations 扩展中定义的
// 全部 CEL 程序，返回一个自定义资源校验器；其内部为所有传递性包含校验规则的
// items、properties 和 additionalProperties 保存嵌套校验器。
// 若 Structural schema 中没有校验规则则返回 nil。可能返回一个只含错误的校验器。
// perCallLimit 仅作为测试输入参数。调用方应始终传入 k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 PerCallLimit。
func NewValidator(s *schema.Structural, isResourceRoot bool, perCallLimit uint64) *Validator {
	if !hasXValidations(s) {
		return nil
	}
	return validator(s, s, isResourceRoot, model.SchemaDeclType(s, isResourceRoot), perCallLimit)
}

// validator 为给定 schema 及其下层所有 x-kubernetes-validations 创建 Validator；
// 若 schema 中存在 x-kubernetes-validations 则返回 Validator，否则返回 nil。
// declType 应是对应结构 schema 的 CEL DeclType。
// perCallLimit 仅供测试使用。调用方应始终传入 k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 PerCallLimit。
func validator(validationSchema, nodeSchema *schema.Structural, isResourceRoot bool, declType *cel.DeclType, perCallLimit uint64) *Validator {
	compilationSchema := *nodeSchema
	compilationSchema.XValidations = validationSchema.XValidations
	compiledRules, err := Compile(&compilationSchema, declType, perCallLimit, environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()), StoredExpressionsEnvLoader())

	var itemsValidator, additionalPropertiesValidator *Validator
	var propertiesValidators map[string]Validator
	var allOfValidators []*Validator
	var elemType *cel.DeclType
	if declType != nil {
		elemType = declType.ElemType
	} else {
		elemType = declType
	}

	if validationSchema.Items != nil && nodeSchema.Items != nil {
		itemsValidator = validator(validationSchema.Items, nodeSchema.Items, nodeSchema.Items.XEmbeddedResource, elemType, perCallLimit)
	}

	if len(validationSchema.Properties) > 0 {
		propertiesValidators = make(map[string]Validator, len(validationSchema.Properties))
		for k, validationProperty := range validationSchema.Properties {
			nodeProperty, ok := nodeSchema.Properties[k]
			if !ok {
				// 只能为位于 schema 结构主干的字段添加值校验。
				continue
			}

			var fieldType *cel.DeclType
			if escapedPropName, ok := cel.Escape(k); ok {
				if declType == nil {
					continue
				}
				if f, ok := declType.Fields[escapedPropName]; ok {
					fieldType = f.Type
				} else {
					// 未知类型的字段直接跳过 CEL 校验
					continue
				}
			} else {
				// 属性名无法转义时，declType 中可能没有该字段，此时应把字段值类型转换为 DeclType。
				fieldType = model.SchemaDeclType(&nodeProperty, nodeProperty.XEmbeddedResource)
				if fieldType == nil {
					continue
				}
			}
			if p := validator(&validationProperty, &nodeProperty, nodeProperty.XEmbeddedResource, fieldType, perCallLimit); p != nil {
				propertiesValidators[k] = *p
			}
		}
	}
	if validationSchema.AdditionalProperties != nil && validationSchema.AdditionalProperties.Structural != nil &&
		nodeSchema.AdditionalProperties != nil && nodeSchema.AdditionalProperties.Structural != nil {
		additionalPropertiesValidator = validator(validationSchema.AdditionalProperties.Structural, nodeSchema.AdditionalProperties.Structural, nodeSchema.AdditionalProperties.Structural.XEmbeddedResource, elemType, perCallLimit)
	}

	if validationSchema.ValueValidation != nil && len(validationSchema.ValueValidation.AllOf) > 0 {
		allOfValidators = make([]*Validator, 0, len(validationSchema.ValueValidation.AllOf))
		for _, allOf := range validationSchema.ValueValidation.AllOf {
			allOfValidator := validator(nestedToStructural(&allOf), nodeSchema, isResourceRoot, declType, perCallLimit)
			if allOfValidator != nil {
				allOfValidators = append(allOfValidators, allOfValidator)
			}
		}
	}

	if len(compiledRules) > 0 || err != nil || itemsValidator != nil || additionalPropertiesValidator != nil || len(propertiesValidators) > 0 || len(allOfValidators) > 0 {
		activationFactory := validationActivationWithoutOldSelf
		for _, rule := range compiledRules {
			if rule.UsesOldSelf {
				activationFactory = validationActivationWithOldSelf
				break
			}
		}

		return &Validator{
			compiledRules:        compiledRules,
			uncompiledRules:      validationSchema.XValidations,
			compilationErr:       err,
			isResourceRoot:       isResourceRoot,
			Items:                itemsValidator,
			AdditionalProperties: additionalPropertiesValidator,
			Properties:           propertiesValidators,
			AllOfValidators:      allOfValidators,
			celActivationFactory: activationFactory,
			Schema:               nodeSchema,
		}
	}

	return nil
}

// options 保存单次校验的可选行为。
type options struct {
	ratchetingOptions
}

// Option 是校验的可选配置。
type Option func(*options)

// WithRatcheting 启用 ratcheting，并传入当前的 old/new 关联对象。
func WithRatcheting(correlation *common.CorrelatedObject) Option {
	return func(o *options) {
		o.currentCorrelation = correlation
	}
}

// Validate 用 Validator 中的全部 x-kubernetes-validations 规则校验 obj，返回错误。
// 若校验规则超出 costBudget，后续求值会被跳过，返回的 errs 非空，且返回的
// remainingBudget 为负。
// 除非还要再调用一次 validate，大多数调用方可以忽略返回的 remainingBudget。
// 传入 context 是为了在 CEL 校验期间支持取消。
func (s *Validator) Validate(ctx context.Context, fldPath *field.Path, _ *schema.Structural, obj, oldObj interface{}, costBudget int64, opts ...Option) (errs field.ErrorList, remainingBudget int64) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}

	return s.validate(ctx, fldPath, obj, oldObj, opt.ratchetingOptions, costBudget)
}

// ratchetingOptions 保存当前关联对象以及最近的可关联父节点。
// 保存父节点是为了在抛出错误的位置用简单逻辑判断是否应 ratchet。
// 仍按常规方式用 Key 和 Index 下探到下一个节点。
type ratchetingOptions struct {
	// 当前关联对象。为 nil 表示该节点位于 schema 中不可关联的部分。
	currentCorrelation *common.CorrelatedObject

	// currentCorrelation 为 nil 时，这是离该节点最近的、可关联的父节点。
	// 若父节点与旧值 deepequal，则该节点抛出的错误会被 ratchet。
	nearestParentCorrelation *common.CorrelatedObject
}

// shouldRatchetError 返回当前节点产生的错误是否应被 ratchet。
//
// 满足以下任一条件时，当前节点的错误应被 ratchet：
//  1. 当前节点可关联，且与旧值相等
//  2. 当前节点有可关联的祖先，且该祖先与旧值相等
func (r ratchetingOptions) shouldRatchetError() bool {
	if r.currentCorrelation != nil {
		return r.currentCorrelation.CachedDeepEqual()
	}

	return r.nearestParentCorrelation.CachedDeepEqual()
}

// 沿树按字段名找到下一个节点并返回对应 options。找不到时保留最近可关联祖先的引用，
// 供 ratchet 使用。
func (r ratchetingOptions) key(field string) ratchetingOptions {
	if r.currentCorrelation == nil {
		return r
	} else if r.nearestParentCorrelation == nil && (field == "apiVersion" || field == "kind") {
		// apiVersion 和 kind 字段的变化无法 ratchet，因为它们不可见
		// （新旧值都被转换成同一类型）。
		//
		return ratchetingOptions{}
	}

	// 除根节点外 nearestParentCorrelation 始终非 nil。
	// 下面这行保证下一个 nearestParentCorrelation 被设为非 nil 的 r.currentCorrelation。
	return ratchetingOptions{currentCorrelation: r.currentCorrelation.Key(field), nearestParentCorrelation: r.currentCorrelation}
}

// 沿树按下标找到下一个节点并返回对应 options。找不到时保留最近可关联祖先的引用，
// 供 ratchet 使用。
func (r ratchetingOptions) index(idx int) ratchetingOptions {
	if r.currentCorrelation == nil {
		return r
	}

	return ratchetingOptions{currentCorrelation: r.currentCorrelation.Index(idx), nearestParentCorrelation: r.currentCorrelation}
}

// nestedToStructural 把 NestedValueValidation 转换为对应的 Structural，
// 用于处理 allOf 中的校验规则。
func nestedToStructural(nested *schema.NestedValueValidation) *schema.Structural {
	if nested == nil {
		return nil
	}

	structuralConversion := &schema.Structural{
		ValueValidation:      &nested.ValueValidation,
		ValidationExtensions: nested.ValidationExtensions,
		Generic:              nested.ForbiddenGenerics,
		Extensions:           nested.ForbiddenExtensions,
		Items:                nestedToStructural(nested.Items),
	}

	if len(nested.Properties) > 0 {
		structuralConversion.Properties = make(map[string]schema.Structural, len(nested.Properties))
		for k, v := range nested.Properties {
			structuralConversion.Properties[k] = *nestedToStructural(&v)
		}
	}

	if nested.AdditionalProperties != nil {
		structuralConversion.AdditionalProperties = &schema.StructuralOrBool{
			Structural: nestedToStructural(nested.AdditionalProperties),
		}
	}

	return structuralConversion
}

// validate 在节点上执行表达式校验，并按对象类型递归到数组/map 子节点。
func (s *Validator) validate(ctx context.Context, fldPath *field.Path, obj, oldObj interface{}, correlation ratchetingOptions, costBudget int64) (errs field.ErrorList, remainingBudget int64) {
	t := time.Now()
	defer func() {
		metrics.Metrics.ObserveEvaluation(time.Since(t))
	}()
	remainingBudget = costBudget
	if s == nil || obj == nil {
		return nil, remainingBudget
	}

	errs, remainingBudget = s.validateExpressions(ctx, fldPath, obj, oldObj, correlation, remainingBudget)

	if remainingBudget < 0 {
		return errs, remainingBudget
	}

	// 若 schema 有 allOf，则递归进去检查是否有需要执行的校验规则。
	for _, allOfValidator := range s.AllOfValidators {
		var allOfErrs field.ErrorList
		// 传入 currentCorrelation 为 nil 的 options，以镜像 schema ratcheting 的行为：
		// allOf 中的错误不做 ratchet。将来可能会调整 allOf 的该行为。
		allOfErrs, remainingBudget = allOfValidator.validate(ctx, fldPath, obj, oldObj, ratchetingOptions{nearestParentCorrelation: correlation.nearestParentCorrelation}, remainingBudget)
		errs = append(errs, allOfErrs...)
		if remainingBudget < 0 {
			return errs, remainingBudget
		}
	}

	switch obj := obj.(type) {
	case []interface{}:
		oldArray, _ := oldObj.([]interface{})
		var arrayErrs field.ErrorList
		arrayErrs, remainingBudget = s.validateArray(ctx, fldPath, obj, oldArray, correlation, remainingBudget)
		errs = append(errs, arrayErrs...)
		return errs, remainingBudget
	case map[string]interface{}:
		oldMap, _ := oldObj.(map[string]interface{})
		var mapErrs field.ErrorList
		mapErrs, remainingBudget = s.validateMap(ctx, fldPath, obj, oldMap, correlation, remainingBudget)
		errs = append(errs, mapErrs...)
		return errs, remainingBudget
	}

	return errs, remainingBudget
}

// validateExpressions 只执行当前节点自身的 CEL 规则，不递归。
func (s *Validator) validateExpressions(ctx context.Context, fldPath *field.Path, obj, oldObj interface{}, correlation ratchetingOptions, costBudget int64) (errs field.ErrorList, remainingBudget int64) {
	sts := s.Schema

	// 防止 oldObj 是“非 nil 接口、nil 值”的情况。
	if oldObj != nil {
		v := reflect.ValueOf(oldObj)
		switch v.Kind() {
		case reflect.Map, reflect.Pointer, reflect.Interface, reflect.Slice:
			if v.IsNil() {
				oldObj = nil // +k8s:verify-mutation:reason=clone
			}
		}
	}

	remainingBudget = costBudget
	if obj == nil {
		// 只校验非 null 值。需要检查可空值状态或可选字段是否存在的规则，必须在
		// 其外层 schema 上表达。例如数组的 items 是可空字符串时，数组 schema 上的规则
		// 可以检查 items 是否为 null，但可空字符串 schema 上的规则只会校验非 null 的字符串。
		return nil, remainingBudget
	}
	if s.compilationErr != nil {
		errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("rule compiler initialization error: %v", s.compilationErr)))
		return errs, remainingBudget
	}
	if len(s.compiledRules) == 0 {
		return nil, remainingBudget // 没有需要执行的内容
	}
	if remainingBudget <= 0 {
		errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("validation failed due to running out of cost budget, no further validation rules will be run")))
		return errs, -1
	}
	if s.isResourceRoot {
		sts = model.WithTypeAndObjectMeta(sts)
	}
	activation, optionalOldSelfActivation := s.celActivationFactory(sts, obj, oldObj)
	for i, compiled := range s.compiledRules {
		rule := s.uncompiledRules[i]
		if compiled.Error != nil {
			errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("rule compile error: %v", compiled.Error)))
			continue
		}
		if compiled.Program == nil {
			// 规则为空
			continue
		}

		// 启用 ratcheting 时，即使 optionalOldSelf 为 true，也允许带 oldSelf 的规则求值。
		optionalOldSelfRule := ptr.Deref(rule.OptionalOldSelf, false)
		if compiled.UsesOldSelf && oldObj == nil {
			// 转换类规则只在存在可比较的旧值时才求值；
			// 但若规则使用 optional oldSelf 且该特性门控已启用，则允许求值。
			if !utilfeature.DefaultFeatureGate.Enabled(features.CRDValidationRatcheting) {
				continue
			}

			if !optionalOldSelfRule {
				continue
			}
		}

		ruleActivation := activation
		if optionalOldSelfRule {
			ruleActivation = optionalOldSelfActivation
		}

		evalResult, evalDetails, err := compiled.Program.ContextEval(ctx, ruleActivation)
		if evalDetails == nil {
			errs = append(errs, field.InternalError(fldPath, fmt.Errorf("runtime cost could not be calculated for validation rule: %v, no further validation rules will be run", ruleErrorString(rule))))
			return errs, -1
		} else {
			rtCost := evalDetails.ActualCost()
			if rtCost == nil {
				errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("runtime cost could not be calculated for validation rule: %v, no further validation rules will be run", ruleErrorString(rule))))
				return errs, -1
			} else {
				if *rtCost > math.MaxInt64 || int64(*rtCost) > remainingBudget {
					errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("validation failed due to running out of cost budget, no further validation rules will be run")))
					return errs, -1
				}
				remainingBudget -= int64(*rtCost)
			}
		}
		if err != nil {
			// 参见 types.Err 中定义的错误类型列表
			if strings.HasPrefix(err.Error(), "no such overload") {
				// 大多数 overload 错误会被编译器捕获，并给出规则中出错的具体位置。
				// 这里是在运行时出现 overload 错误、没有任何细节，因此追加更明确的
				// 错误消息。只有绕过静态类型检查时才会出现这种情况。int-or-string
				// 被标记为 dynamic，因此绕过了编译器的类型检查。
				errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("'%v': call arguments did not match a supported operator, function or macro signature for rule: %v", err, ruleErrorString(rule))))
			} else if strings.HasPrefix(err.Error(), "operation cancelled: actual cost limit exceeded") {
				errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("'%v': no further validation rules will be run due to call cost exceeds limit for rule: %v", err, ruleErrorString(rule))))
				return errs, -1
			} else {
				// 例如：no such key: {key}、index out of bounds: {index}、整数溢出、除以零等
				errs = append(errs, field.Invalid(fldPath, sts.Type, fmt.Sprintf("%v evaluating rule: %v", err, ruleErrorString(rule))))
			}
			continue
		}

		if evalResult == types.True {
			continue
		}

		// 准备一个 field error，说明表达式为何求值为 False。
		// 其 detail 可能来自另一个可能求值失败或超预算的表达式。

		currentFldPath := fldPath
		if len(compiled.NormalizedRuleFieldPath) > 0 {
			currentFldPath = currentFldPath.Child(compiled.NormalizedRuleFieldPath)
		}

		addErr := func(e *field.Error) {
			if !compiled.UsesOldSelf && correlation.shouldRatchetError() {
				warning.AddWarning(ctx, "", e.Error())
			} else {
				errs = append(errs, e)
			}
		}

		detail, ok := "", false
		if compiled.MessageExpression != nil {
			messageExpression, newRemainingBudget, msgErr := evalMessageExpression(ctx, compiled.MessageExpression, rule.MessageExpression, activation, remainingBudget)
			if msgErr == nil {
				detail, ok = messageExpression, true
				remainingBudget = newRemainingBudget
			} else if msgErr.Type == cel.ErrorTypeInternal {
				addErr(field.InternalError(currentFldPath, msgErr))
				return errs, -1
			} else if msgErr.Type == cel.ErrorTypeInvalid {
				addErr(field.Invalid(currentFldPath, sts.Type, msgErr.Error()))
				return errs, -1
			} else {
				klog.V(2).ErrorS(msgErr, "messageExpression evaluation failed")
				remainingBudget = newRemainingBudget
			}
		}
		if !ok {
			detail = ruleMessageOrDefault(rule)
		}

		value := obj
		if sts.Type == "object" || sts.Type == "array" {
			value = field.OmitValueType{}
		}

		addErr(fieldErrorForReason(currentFldPath, value, detail, rule.Reason))
	}
	return errs, remainingBudget
}

var unescapeMatcher = regexp.MustCompile(`\\.`)

// unescapeSingleQuote 处理字符串字面量中的反斜杠转义。
func unescapeSingleQuote(s string) (string, error) {
	var err error
	unescaped := unescapeMatcher.ReplaceAllStringFunc(s, func(matchStr string) string {
		directive := matchStr[1]
		switch directive {
		case 'a':
			return "\a"
		case 'b':
			return "\b"
		case 'f':
			return "\f"
		case 'n':
			return "\n"
		case 'r':
			return "\r"
		case 't':
			return "\t"
		case 'v':
			return "\v"
		case '\'':
			return "'"
		case '\\':
			return "\\"
		default:
			err = fmt.Errorf("invalid escape char %s", matchStr)
			return ""
		}
	})
	return unescaped, err
}

// validFieldPathOptions 控制 ValidFieldPath 的解析行为。
type validFieldPathOptions struct {
	allowArrayNotation bool
}

// ValidFieldPathOption 为 ValidFieldPath 提供可变参数选项。
type ValidFieldPathOption func(*validFieldPathOptions)

// WithFieldPathAllowArrayNotation 设置字段路径中是否允许数组记法
// （'[<index or map key>]'）。
// 默认允许。
func WithFieldPathAllowArrayNotation(allow bool) ValidFieldPathOption {
	return func(options *validFieldPathOptions) {
		options.allowArrayNotation = allow
	}
}

// ValidFieldPath 校验 jsonPath 是合法的 JSON Path，且只包含对给定 schema 有效的
// 字段和 map 访问器；返回校验后 jsonPath 的 field.Path 表示或错误。
func ValidFieldPath(jsonPath string, schema *schema.Structural, options ...ValidFieldPathOption) (validFieldPath *field.Path, foundSchema *schema.Structural, err error) {
	opts := &validFieldPathOptions{allowArrayNotation: true}
	for _, opt := range options {
		opt(opts)
	}

	appendToPath := func(name string, isNamed bool) error {
		if !isNamed {
			validFieldPath = validFieldPath.Key(name)
			schema = schema.AdditionalProperties.Structural
		} else {
			validFieldPath = validFieldPath.Child(name)
			val, ok := schema.Properties[name]
			if !ok {
				return fmt.Errorf("does not refer to a valid field")
			}
			schema = &val
		}
		return nil
	}

	validFieldPath = nil

	scanner := bufio.NewScanner(strings.NewReader(jsonPath))

	// 配置扫描器把字符串切成 token。
	// 三个分隔符（'.'、'['、']'）作为单字符 token 返回。
	// 分隔符之间的其他文本作为字符串 token 返回。
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if len(data) > 0 {
			for i := 0; i < len(data); i++ {
				// 若处于单引号字符串中，寻找字符串结尾，
				// 期间忽略分隔符。
				if data[0] == '\'' {
					if i > 0 && data[i] == '\'' && data[i-1] != '\\' {
						// 返回带引号的字符串
						return i + 1, data[:i+1], nil
					}
					continue
				}
				switch data[i] {
				case '.', '[', ']': // 分隔符
					if i == 0 {
						// 返回该分隔符。
						return 1, data[:1], nil
					} else {
						// 返回分隔符之前的标识符。
						// 下一次 split 调用会返回该分隔符。
						return i, data[:i], nil
					}
				}
			}
			if atEOF {
				// 返回字符串。
				return len(data), data, nil
			}
		}
		return 0, nil, nil
	})

	var tok string
	var isNamed bool
	for scanner.Scan() {
		tok = scanner.Text()
		switch tok {
		case "[":
			if !opts.allowArrayNotation {
				return nil, nil, fmt.Errorf("array notation is not allowed")
			}
			if !scanner.Scan() {
				return nil, nil, fmt.Errorf("unexpected end of JSON path")
			}
			tok = scanner.Text()
			if len(tok) < 2 || tok[0] != '\'' || tok[len(tok)-1] != '\'' {
				return nil, nil, fmt.Errorf("expected single quoted string but got %s", tok)
			}
			unescaped, err := unescapeSingleQuote(tok[1 : len(tok)-1])
			if err != nil {
				return nil, nil, fmt.Errorf("invalid string literal: %w", err)
			}

			if schema.Properties != nil {
				isNamed = true
			} else if schema.AdditionalProperties != nil {
				isNamed = false
			} else {
				return nil, nil, fmt.Errorf("does not refer to a valid field")
			}
			if err := appendToPath(unescaped, isNamed); err != nil {
				return nil, nil, err
			}
			if !scanner.Scan() {
				return nil, nil, fmt.Errorf("unexpected end of JSON path")
			}
			tok = scanner.Text()
			if tok != "]" {
				return nil, nil, fmt.Errorf("expected ] but got %s", tok)
			}
		case ".":
			if !scanner.Scan() {
				return nil, nil, fmt.Errorf("unexpected end of JSON path")
			}
			tok = scanner.Text()
			if schema.Properties != nil {
				isNamed = true
			} else if schema.AdditionalProperties != nil {
				isNamed = false
			} else {
				return nil, nil, fmt.Errorf("does not refer to a valid field")
			}
			if err := appendToPath(tok, isNamed); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("expected [ or . but got: %s", tok)
		}
	}

	return validFieldPath, schema, nil
}

// fieldErrorForReason 按规则声明的 reason 构造对应类型的 field error。
func fieldErrorForReason(fldPath *field.Path, value interface{}, detail string, reason *apiextensions.FieldValueErrorReason) *field.Error {
	if reason == nil {
		return field.Invalid(fldPath, value, detail)
	}
	switch *reason {
	case apiextensions.FieldValueForbidden:
		return field.Forbidden(fldPath, detail)
	case apiextensions.FieldValueRequired:
		return field.Required(fldPath, detail)
	case apiextensions.FieldValueDuplicate:
		return field.Duplicate(fldPath, value)
	default:
		return field.Invalid(fldPath, value, detail)
	}
}

// evalMessageExpression 对给定的 message expression 求值，返回求值后的字符串形式
// 和剩余预算；求值过程中出错则返回错误。
func evalMessageExpression(ctx context.Context, expr celgo.Program, exprSrc string, activation interpreter.Activation, remainingBudget int64) (string, int64, *cel.Error) {
	evalResult, evalDetails, err := expr.ContextEval(ctx, activation)
	if evalDetails == nil {
		return "", -1, &cel.Error{
			Type:   cel.ErrorTypeInternal,
			Detail: fmt.Sprintf("runtime cost could not be calculated for messageExpression: %q", exprSrc),
		}
	}
	rtCost := evalDetails.ActualCost()
	if rtCost == nil {
		return "", -1, &cel.Error{
			Type:   cel.ErrorTypeInternal,
			Detail: fmt.Sprintf("runtime cost could not be calculated for messageExpression: %q", exprSrc),
		}
	} else if *rtCost > math.MaxInt64 || int64(*rtCost) > remainingBudget {
		return "", -1, &cel.Error{
			Type:   cel.ErrorTypeInvalid,
			Detail: "messageExpression evaluation failed due to running out of cost budget, no further validation rules will be run",
		}
	}
	if err != nil {
		if strings.HasPrefix(err.Error(), "operation cancelled: actual cost limit exceeded") {
			return "", -1, &cel.Error{
				Type:   cel.ErrorTypeInvalid,
				Detail: fmt.Sprintf("no further validation rules will be run due to call cost exceeds limit for messageExpression: %q", exprSrc),
			}
		}
		return "", remainingBudget - int64(*rtCost), &cel.Error{
			Detail: fmt.Sprintf("messageExpression evaluation failed due to: %v", err.Error()),
		}
	}
	messageStr, ok := evalResult.Value().(string)
	if !ok {
		return "", remainingBudget - int64(*rtCost), &cel.Error{
			Detail: "messageExpression failed to convert to string",
		}
	}
	trimmedMsgStr := strings.TrimSpace(messageStr)
	if len(trimmedMsgStr) > celconfig.MaxEvaluatedMessageExpressionSizeBytes {
		return "", remainingBudget - int64(*rtCost), &cel.Error{
			Detail: fmt.Sprintf("messageExpression beyond allowable length of %d", celconfig.MaxEvaluatedMessageExpressionSizeBytes),
		}
	} else if hasNewlines(trimmedMsgStr) {
		return "", remainingBudget - int64(*rtCost), &cel.Error{
			Detail: "messageExpression should not contain line breaks",
		}
	} else if len(trimmedMsgStr) == 0 {
		return "", remainingBudget - int64(*rtCost), &cel.Error{
			Detail: "messageExpression should evaluate to a non-empty string",
		}
	}
	return trimmedMsgStr, remainingBudget - int64(*rtCost), nil
}

var newlineMatcher = regexp.MustCompile(`[\n]+`)

// hasNewlines 判断字符串是否包含换行。
func hasNewlines(s string) bool {
	return newlineMatcher.MatchString(s)
}

// ruleMessageOrDefault 返回规则的自定义消息，没有时生成默认的失败消息。
func ruleMessageOrDefault(rule apiextensions.ValidationRule) string {
	if len(rule.Message) == 0 {
		return fmt.Sprintf("failed rule: %s", ruleErrorString(rule))
	} else {
		return strings.TrimSpace(rule.Message)
	}
}

// ruleErrorString 返回用于错误消息的规则描述：优先使用 message，否则使用规则本身。
func ruleErrorString(rule apiextensions.ValidationRule) string {
	if len(rule.Message) > 0 {
		return strings.TrimSpace(rule.Message)
	}
	return strings.TrimSpace(rule.Rule)
}

// validationActivation 把 self/oldSelf 解析为 CEL 值。
type validationActivation struct {
	self, oldSelf ref.Val
	hasOldSelf    bool
}

// validationActivationWithOldSelf 生成根据 new/old 对象绑定 self 和 oldSelf 的
// activation，同时生成 oldSelf 为 optional 的版本。
func validationActivationWithOldSelf(sts *schema.Structural, obj, oldObj interface{}) (activation interpreter.Activation, optionalOldSelfActivation interpreter.Activation) {
	va := &validationActivation{
		self: UnstructuredToVal(obj, sts),
	}
	optionalVA := &validationActivation{
		self:       va.self,
		hasOldSelf: true, // 表示 oldSelf 变量对 CEL 可见，并不表示它已有值
		oldSelf:    types.OptionalNone,
	}

	if oldObj != nil {
		va.oldSelf = UnstructuredToVal(oldObj, sts) // +k8s:verify-mutation:reason=clone
		va.hasOldSelf = true

		optionalVA.oldSelf = types.OptionalOf(va.oldSelf) // +k8s:verify-mutation:reason=clone
	}

	return va, optionalVA
}

// validationActivationWithoutOldSelf 生成仅绑定 self 的 activation；optional 版本
// 与之相同，因为该场景下 oldSelf 永远不会被引用。
func validationActivationWithoutOldSelf(sts *schema.Structural, obj, _ interface{}) (interpreter.Activation, interpreter.Activation) {
	res := &validationActivation{
		self: UnstructuredToVal(obj, sts),
	}
	return res, res
}

// ResolveName 把 CEL 标识符解析为对应的值。
func (a *validationActivation) ResolveName(name string) (interface{}, bool) {
	switch name {
	case ScopedVarName:
		return a.self, true
	case OldScopedVarName:
		return a.oldSelf, a.hasOldSelf
	default:
		return nil, false
	}
}

// Parent 返回父 activation，这里没有父级。
func (a *validationActivation) Parent() interpreter.Activation {
	return nil
}

// validateMap 递归校验 map 的 additionalProperties 和 properties。
func (s *Validator) validateMap(ctx context.Context, fldPath *field.Path, obj, oldObj map[string]interface{}, correlation ratchetingOptions, costBudget int64) (errs field.ErrorList, remainingBudget int64) {
	remainingBudget = costBudget
	if remainingBudget < 0 {
		return errs, remainingBudget
	}
	if s == nil || obj == nil {
		return nil, remainingBudget
	}

	correlatable := MapIsCorrelatable(s.Schema.XMapType)

	if s.AdditionalProperties != nil {
		for k, v := range obj {
			var oldV interface{}
			if correlatable {
				oldV = oldObj[k] // +k8s:verify-mutation:reason=clone
			}

			var err field.ErrorList
			err, remainingBudget = s.AdditionalProperties.validate(ctx, fldPath.Key(k), v, oldV, correlation.key(k), remainingBudget)
			errs = append(errs, err...)
			if remainingBudget < 0 {
				return errs, remainingBudget
			}
		}
	}
	if s.Properties != nil {
		for k, v := range obj {
			sub, ok := s.Properties[k]
			if ok {
				var oldV interface{}
				if correlatable {
					oldV = oldObj[k] // +k8s:verify-mutation:reason=clone
				}

				var err field.ErrorList
				err, remainingBudget = sub.validate(ctx, fldPath.Child(k), v, oldV, correlation.key(k), remainingBudget)
				errs = append(errs, err...)
				if remainingBudget < 0 {
					return errs, remainingBudget
				}
			}
		}
	}

	return errs, remainingBudget
}

// validateArray 递归校验数组的元素。
func (s *Validator) validateArray(ctx context.Context, fldPath *field.Path, obj, oldObj []interface{}, correlation ratchetingOptions, costBudget int64) (errs field.ErrorList, remainingBudget int64) {
	remainingBudget = costBudget
	if remainingBudget < 0 {
		return errs, remainingBudget
	}

	if s.Items != nil {
		// 只有 map 类型的 list 支持 CEL 规则的 self-oldSelf 关联。若不是 map 类型 list，
		// makeMapList 返回的实现总是返回 nil。
		correlatableOldItems := makeMapList(s.Schema, oldObj)
		for i := range obj {
			var err field.ErrorList
			err, remainingBudget = s.Items.validate(ctx, fldPath.Index(i), obj[i], correlatableOldItems.Get(obj[i]), correlation.index(i), remainingBudget)
			errs = append(errs, err...)
			if remainingBudget < 0 {
				return errs, remainingBudget
			}
		}
	}

	return errs, remainingBudget
}

// MapIsCorrelatable 返回 mapType 是否可用于在 update 后把 map 的数据元素与更新前的
// 数据元素关联起来。
func MapIsCorrelatable(mapType *string) bool {
	// 若将来引入第三种 map 类型，默认视为不可关联。未指定时默认是 granular。
	return mapType == nil || *mapType == "granular" || *mapType == "atomic"
}

// hasXValidations 递归判断 schema 及其子节点中是否存在 x-kubernetes-validations。
func hasXValidations(s *schema.Structural) bool {
	if s == nil {
		return false
	}
	if len(s.XValidations) > 0 {
		return true
	}
	if hasXValidations(s.Items) {
		return true
	}
	if s.AdditionalProperties != nil && hasXValidations(s.AdditionalProperties.Structural) {
		return true
	}
	if s.Properties != nil {
		for _, prop := range s.Properties {
			if hasXValidations(&prop) {
				return true
			}
		}
	}
	return false
}
