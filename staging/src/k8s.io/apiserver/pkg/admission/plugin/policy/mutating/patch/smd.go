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

package patch

import (
	"context"
	"errors"
	"fmt"
	celgo "github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"strings"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/schema"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
	"sigs.k8s.io/structured-merge-diff/v6/value"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/managedfields"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/cel/mutation/dynamic"
)

// ApplyConfigurationCondition 包含编译并求值返回 apply configuration 的
// CEL 表达式所需的输入。
//
// 表达式必须返回 Object 初始值（apply configuration），而不是普通 map。
type ApplyConfigurationCondition struct {
	Expression string
}

// 编译期断言：该条件必须满足 CEL 表达式访问器接口。
var _ plugincel.ExpressionAccessor = &ApplyConfigurationCondition{}

// GetExpression 返回 apply configuration 表达式的 CEL 源码。
func (v *ApplyConfigurationCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 限定表达式返回 Object 类型，以便编译器做静态类型检查。
func (v *ApplyConfigurationCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{applyConfigObjectType}
}

// applyConfigObjectType 是 Object 初始化器的 CEL 类型，名称需与 dynamic.ObjectVal 的声明一致。
var applyConfigObjectType = celtypes.NewObjectType("Object")

// NewApplyConfigurationPatcher 创建一个执行 applyConfiguration 变更的 patcher。
func NewApplyConfigurationPatcher(expressionEvaluator plugincel.MutatingEvaluator) Patcher {
	return &applyConfigPatcher{expressionEvaluator: expressionEvaluator}
}

// applyConfigPatcher 通过求值 apply configuration 表达式，把结果以 SSA 合并方式应用到对象上。
type applyConfigPatcher struct {
	expressionEvaluator plugincel.MutatingEvaluator
}

// Patch 求值表达式得到 apply configuration，再与当前对象做结构化合并。
// 返回新的对象副本，不修改入参，便于 reinvocation 阶段比较前后差异。
func (e *applyConfigPatcher) Patch(ctx context.Context, r Request, runtimeCELCostBudget int64) (runtime.Object, error) {
	admissionRequest := plugincel.CreateAdmissionRequest(
		r.VersionedAttributes.Attributes,
		metav1.GroupVersionResource(r.MatchedResource),
		metav1.GroupVersionKind(r.VersionedAttributes.VersionedKind))

	// 编译错误优先返回：表达式本身不合法时继续求值没有意义。
	compileErrors := e.expressionEvaluator.CompilationErrors()
	if len(compileErrors) > 0 {
		return nil, errors.Join(compileErrors...)
	}
	eval, _, err := e.expressionEvaluator.ForInput(ctx, r.VersionedAttributes, admissionRequest, r.OptionalVariables, r.Namespace, runtimeCELCostBudget)
	if err != nil {
		return nil, err
	}
	if eval.Error != nil {
		return nil, eval.Error
	}
	v := eval.EvalResult

	// 编译器保证返回类型是类型名为 "Object" 的 ObjectVal。
	// 运行时仍做断言，因为动态类型系统无法完全保证，误用时应给出明确错误而非 panic。
	objVal, ok := v.(*dynamic.ObjectVal)
	if !ok {
		// 编译器已对返回类型做过类型检查，这不应发生。
		return nil, fmt.Errorf("unsupported return type from ApplyConfiguration expression: %v", v.Type())
	}
	// TODO: Object 初始化器的类型检查不够充分。
	// 作为过渡措施，我们用这个合理性检查来检测字段名与 Object 初始化器之间的
	// 类型不匹配，例如 "Object.spec{ selector: Object.spec.wrong{}}" 会被判定为不匹配。
	// 进入 beta 之前，需要为 Object 初始化器以及 "object" 和 "oldObject" 变量
	// 附上完整类型信息，从而让 CEL 能够进行全面的运行时类型检查。
	//
	// 这是对类型检查缺口的补偿：字段路径必须与对象初始化器类型名一致，
	// 否则用户会静默写入一个不存在的结构，直到后续更新才暴露。
	err = objVal.CheckTypeNamesMatchFieldPathNames()
	if err != nil {
		return nil, fmt.Errorf("type mismatch: %w", err)
	}

	value, ok := objVal.Value().(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid return type: %T", v)
	}

	patchObject := unstructured.Unstructured{Object: value}
	// 补上 GVK：表达式产出的 map 本身不带类型信息，而合并前必须校验 GVK 一致。
	patchObject.SetGroupVersionKind(r.VersionedAttributes.VersionedObject.Object().GetObjectKind().GroupVersionKind())
	patched, err := ApplyStructuredMergeDiff(r.TypeConverter, r.VersionedAttributes.VersionedObject.Object(), &patchObject)
	if err != nil {
		return nil, fmt.Errorf("error applying patch: %w", err)
	}
	return patched, nil
}

// ApplyStructuredMergeDiff 对对象应用结构化合并 diff，并返回应用补丁后的
// 对象副本。
//
// 使用 SSA 语义合并：patch 中被显式设置的字段生效，未出现的字段保持原值。
func ApplyStructuredMergeDiff(
	typeConverter managedfields.TypeConverter,
	originalObject runtime.Object,
	patch *unstructured.Unstructured,
) (runtime.Object, error) {
	// GVK 不一致说明调用方把 patch 应用到了错误的对象上，属于编程错误，直接拒绝。
	if patch.GroupVersionKind() != originalObject.GetObjectKind().GroupVersionKind() {
		return nil, fmt.Errorf("patch (%v) and original object (%v) are not of the same gvk", patch.GroupVersionKind().String(), originalObject.GetObjectKind().GroupVersionKind().String())
	} else if typeConverter == nil {
		return nil, fmt.Errorf("type converter must not be nil")
	}

	patchObjTyped, err := typeConverter.ObjectToTyped(patch)
	if err != nil {
		return nil, fmt.Errorf("failed to convert patch object to typed object: %w", err)
	}

	// 先校验再合并：原子字段一旦被部分覆盖会丢失未声明的子字段，必须提前拦截。
	err = validatePatch(patchObjTyped)
	if err != nil {
		return nil, fmt.Errorf("invalid ApplyConfiguration: %w", err)
	}

	// 允许重复项：现有对象可能因历史原因含重复列表元素，不应让读取失败。
	liveObjTyped, err := typeConverter.ObjectToTyped(originalObject, typed.AllowDuplicates)
	if err != nil {
		return nil, fmt.Errorf("failed to convert original object to typed object: %w", err)
	}

	newObjTyped, err := liveObjTyped.Merge(patchObjTyped)
	if err != nil {
		return nil, fmt.Errorf("failed to merge patch: %w", err)
	}

	// 我们的变更型准入策略会设置字段，但不跟踪所有权。
	// 补丁中新引入的字段不会被任何字段管理器跟踪
	//（因此若原对象再次被更新、而变更策略未启用，这些字段将被丢弃）。
	// 这必然意味着：变更型策略对对象的修改，只有在修改发生时策略处于
	// 启用状态才会被保留。（若策略未启用，修改可能被丢弃。）
	//
	// 这是有意的权衡：不引入字段管理器可避免污染 managedFields，
	// 代价是策略下线后这些字段可能在下一次更新中被丢弃。

	newObj, err := typeConverter.TypedToObject(newObjTyped)
	if err != nil {
		return nil, fmt.Errorf("failed to convert typed object to object: %w", err)
	}

	return newObj, nil
}

// validatePatch 在 apply configuration 中搜索处于原子（atomic）模式的数组、
// 映射或结构体元素，一旦发现即返回错误。
// 这可以防止意外删除字段：用户可能想修改原子类型中的部分字段，
// 却没有意识到原子新值中未显式设置的所有字段都会被删除。
func validatePatch(v *typed.TypedValue) error {
	atomics := findAtomics(nil, v.Schema(), v.TypeRef(), v.AsValue())
	if len(atomics) > 0 {
		return fmt.Errorf("may not mutate atomic arrays, maps or structs: %v", strings.Join(atomics, ", "))
	}
	return nil
}

// findAtomics 返回遍历给定值时发现的原子模式数组、映射或结构体的字段路径。
//
// 递归遍历 schema 与值；遇到原子容器即记录路径并继续深入，以便一次性报告所有违规位置。
func findAtomics(path []fieldpath.PathElement, s *schema.Schema, tr schema.TypeRef, v value.Value) (atomics []string) {
	if a, ok := s.Resolve(tr); ok { // 校验阶段在此之前已完成，并已确认所有 schema 都能解析
		if v.IsMap() && a.Map != nil {
			if a.Map.ElementRelationship == schema.Atomic {
				atomics = append(atomics, pathString(path))
			}
			v.AsMap().Iterate(func(key string, val value.Value) bool {
				pe := fieldpath.PathElement{FieldName: &key}
				if sf, ok := a.Map.FindField(key); ok {
					tr = sf.Type
					atomics = append(atomics, findAtomics(append(path, pe), s, tr, val)...)
				}
				return true
			})
		}
		if v.IsList() && a.List != nil {
			if a.List.ElementRelationship == schema.Atomic {
				atomics = append(atomics, pathString(path))
			}
			list := v.AsList()
			for i := 0; i < list.Length(); i++ {
				pe := fieldpath.PathElement{Index: &i}
				atomics = append(atomics, findAtomics(append(path, pe), s, a.List.ElementType, list.At(i))...)
			}
		}
	}
	return atomics
}

// pathString 把路径元素拼接成可读字符串，用于错误消息。
func pathString(path []fieldpath.PathElement) string {
	sb := strings.Builder{}
	for _, p := range path {
		sb.WriteString(p.String())
	}
	return sb.String()
}
