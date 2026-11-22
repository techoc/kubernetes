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

package validators

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"
)

const (
	customValidationTagName = "k8s:customValidation"

	// 手写验证函数的命名约定：ValidateCustom_<Type>[_<Field>]。
	customValidationFuncPrefix = "ValidateCustom_"
)

// Fixed types in the custom-validation signature; value/oldValue types vary.
// 这些是签名中位置固定的参数类型，只有 value/oldValue 随被验证类型变化。
var (
	contextType   = types.Name{Package: "context", Name: "Context"}
	operationType = types.Name{Package: "k8s.io/apimachinery/pkg/api/operation", Name: "Operation"}
	fieldPathType = types.Name{Package: "k8s.io/apimachinery/pkg/util/validation/field", Name: "Path"}
	errorListType = types.Name{Package: "k8s.io/apimachinery/pkg/util/validation/field", Name: "ErrorList"}
)

var customValidationValidator = &customValidationTagValidator{}

func init() {
	RegisterTagValidator(customValidationValidator)
}

// customValidationTagValidator wires a hand-written ValidateCustom_* function
// (in the generated package, found by naming convention) into the generated
// traversal, so it inherits traversal, ratcheting and short-circuiting.
type customValidationTagValidator struct {
	gengoContext        *generator.Context
	inputToCanonicalPkg map[string]string
	// claimed records every function a tag wired (directly or via a composing
	// tag such as ifEnabled), so verifyCustomFunctions can spot ValidateCustom_*
	// functions defined without a tag.
	claimed map[types.Name]bool
}

// Init 从生成器配置中取得 universe 与输入/输出包映射，供后续函数查找使用。
func (v *customValidationTagValidator) Init(cfg Config) {
	v.gengoContext = cfg.GengoContext
	v.inputToCanonicalPkg = cfg.InputToCanonicalPkg
	v.claimed = map[types.Name]bool{}
}

// TagName 返回该验证器负责的标签名。
func (customValidationTagValidator) TagName() string {
	return customValidationTagName
}

// customValidationTagValidScopes 自定义验证既可作用于整个类型，也可作用于单个字段。
var customValidationTagValidScopes = sets.New(ScopeType, ScopeField)

// ValidScopes 返回允许的作用域。
func (customValidationTagValidator) ValidScopes() sets.Set[Scope] {
	return customValidationTagValidScopes
}

// GetValidations 按命名约定定位手写函数并校验其签名，然后把它登记为一个函数调用。
func (v *customValidationTagValidator) GetValidations(context Context, _ codetags.Tag) (Validations, error) {
	// Resolve the output package for the type that owns the call site (the type
	// itself, or the containing struct for a field), as done for Validate_<Type>.
	// 字段作用域的函数定义在包含它的结构体所属的生成包中，而非字段类型的包。
	definingType := context.Type
	if context.Scope == ScopeField {
		definingType = context.ParentType
	}
	outPkg, ok := v.inputToCanonicalPkg[definingType.Name.Package]
	if !ok {
		return Validations{}, fmt.Errorf("cannot resolve generated package for %s (is it being processed by validation-gen?)", definingType.Name.Package)
	}

	// Resolve the function name from the naming convention.
	// 字段级函数名附加字段名，避免同一类型多个字段冲突。
	funcName := customValidationFuncPrefix + definingType.Name.Name
	if context.Scope == ScopeField {
		funcName += "_" + context.Member.Name
	}
	fn := types.Name{Package: outPkg, Name: funcName}

	// value and oldValue are the nilable form of the validated value, matching
	// what the generated traversal passes to the function.
	// 指针化后调用方可以区分“未设置”与零值，这也是 ratcheting 比较所需的形式。
	valueType := context.Type
	if !util.IsNilableType(valueType) {
		valueType = types.PointerTo(valueType)
	}

	// Fail with a clear error if the function is missing or mis-typed, rather
	// than silently skipping validation or emitting code that won't compile.
	if err := v.checkFunction(fn, valueType); err != nil {
		return Validations{}, err
	}
	v.claimed[fn] = true

	// Declare no Emission: the generator can't know a hand-written function's
	// error type or path, so the declarative-validation test generator must not
	// synthesize a coverage case for it. The runtime call is unaffected.
	var result Validations
	result.AddFunction(Function(customValidationTagName, DefaultFlags, fn).WithComment("custom validation"))
	return result, nil
}

// VerifyCustomValidationsHaveTags errors for any ValidateCustom_* function that
// is untagged, or defined in an input package instead of the generated one.
// Call it after all inputs have been processed.
func VerifyCustomValidationsHaveTags() error {
	return customValidationValidator.verifyCustomFunctions()
}

// verifyCustomFunctions 扫描所有相关包，收集命名不规范或未被任何标签引用的
// ValidateCustom_* 函数。问题汇总后排序输出，保证报错稳定、便于批量修复。
func (v *customValidationTagValidator) verifyCustomFunctions() error {
	if v.gengoContext == nil {
		return nil
	}
	var issues []string
	scanned := map[string]bool{}
	for inPkg, outPkg := range v.inputToCanonicalPkg {
		// A ValidateCustom_* function in the input package is misplaced; it must
		// live in the generated package. (inPkg == outPkg for self-contained
		// packages, e.g. test fixtures.)
		if inPkg != outPkg {
			if pkg := v.gengoContext.Universe[inPkg]; pkg != nil {
				for name := range pkg.Functions {
					if strings.HasPrefix(name, customValidationFuncPrefix) {
						issues = append(issues, fmt.Sprintf("%s.%s: move to the generated package %s", inPkg, name, outPkg))
					}
				}
			}
		}
		// A ValidateCustom_* function in the generated package with no tag.
		// 多个输入包可能映射到同一输出包，用 scanned 去重避免重复报告。
		if scanned[outPkg] {
			continue
		}
		scanned[outPkg] = true
		if pkg := v.gengoContext.Universe[outPkg]; pkg != nil {
			for name := range pkg.Functions {
				if strings.HasPrefix(name, customValidationFuncPrefix) && !v.claimed[types.Name{Package: outPkg, Name: name}] {
					issues = append(issues, fmt.Sprintf("%s.%s: no matching tag (add +%s, or rename if not a custom validation)", outPkg, name, customValidationTagName))
				}
			}
		}
	}
	if len(issues) > 0 {
		sort.Strings(issues)
		return fmt.Errorf("+%s: %s", customValidationTagName, strings.Join(issues, "; "))
	}
	return nil
}

// checkFunction verifies that the referenced hand-written function exists in the
// universe and has the canonical validator signature for the given value type.
func (v customValidationTagValidator) checkFunction(fn types.Name, valueType *types.Type) error {
	if v.gengoContext == nil {
		return nil // docs/lint path: nothing to verify against.
	}
	pkg := v.gengoContext.Universe[fn.Package]
	if pkg == nil {
		return fmt.Errorf("+%s: cannot find package %q to verify function %q", customValidationTagName, fn.Package, fn.Name)
	}
	f := pkg.Functions[fn.Name]
	if f == nil {
		return fmt.Errorf("+%s: expected hand-written function %s.%s was not found", customValidationTagName, fn.Package, fn.Name)
	}
	if sig := f.Underlying.Signature; sig == nil || !signatureMatches(sig, valueType) {
		return fmt.Errorf("+%s: %s.%s must have signature "+
			"func(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue %s) field.ErrorList",
			customValidationTagName, fn.Package, fn.Name, renderType(valueType))
	}
	return nil
}

// signatureMatches reports whether sig is the canonical custom-validation
// signature for a value of the given type.
func signatureMatches(sig *types.Signature, valueType *types.Type) bool {
	if sig.Variadic || len(sig.Parameters) != 5 || len(sig.Results) != 1 {
		return false
	}
	return isType(sig.Parameters[0].Type, contextType) &&
		isType(sig.Parameters[1].Type, operationType) &&
		isPointerTo(sig.Parameters[2].Type, fieldPathType) &&
		sameType(sig.Parameters[3].Type, valueType) &&
		sameType(sig.Parameters[4].Type, valueType) &&
		isType(sig.Results[0].Type, errorListType)
}

// isType 判断类型是否恰好是给定命名类型。
func isType(t *types.Type, name types.Name) bool {
	return t != nil && t.Name == name
}

// isPointerTo 判断类型是否为指向给定命名类型的指针。
func isPointerTo(t *types.Type, name types.Name) bool {
	return t != nil && t.Kind == types.Pointer && t.Elem != nil && t.Elem.Name == name
}

// sameType structurally compares two types by kind and (for leaves) name.
func sameType(a, b *types.Type) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case types.Pointer, types.Slice, types.Array:
		return sameType(a.Elem, b.Elem)
	case types.Map:
		return sameType(a.Key, b.Key) && sameType(a.Elem, b.Elem)
	default:
		return a.Name == b.Name
	}
}

// renderType formats a type for an error message (e.g. "*Struct", "[]StringType").
func renderType(t *types.Type) string {
	if t == nil {
		return "?"
	}
	switch t.Kind {
	case types.Pointer:
		return "*" + renderType(t.Elem)
	case types.Slice, types.Array:
		return "[]" + renderType(t.Elem)
	case types.Map:
		return "map[" + renderType(t.Key) + "]" + renderType(t.Elem)
	default:
		return t.Name.Name
	}
}

// Docs 描述标签用法。函数名的 <Type>/<Field> 使用 Go 标识符而非 JSON 名，
// 因为函数定义在生成代码里，必须与 Go 命名一致。
func (v customValidationTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            customValidationTagName,
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(customValidationTagValidScopes),
		Description:    "Calls a hand-written validation function from the generated traversal code.",
		Docs: "The function lives in the generated package, with signature " +
			"func(context.Context, operation.Operation, *field.Path, value, oldValue <ValueType>) field.ErrorList, " +
			"where <ValueType> is the validated value's type made nilable: a pointer (e.g. *string), or the " +
			"type itself if already nilable (slice, map, pointer). In the function name, <Type> and <Field> " +
			"are Go identifiers (e.g. Replicas, not the JSON name replicas). " +
			"Field scope (ValidateCustom_<Type>_<Field>) validates one field; on update the framework skips " +
			"the call when that field is unchanged. Type scope (ValidateCustom_<Type>) is for checks across " +
			"multiple fields; the framework does not skip it on update, so an expensive check should return " +
			"early when value and oldValue are equal. " +
			"For per-element checks, put the tag on the element type, or use field scope and loop inside the " +
			"function.",
	}
}
