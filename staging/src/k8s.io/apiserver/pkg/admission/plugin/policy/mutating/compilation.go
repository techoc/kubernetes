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

package mutating

import (
	"fmt"

	"k8s.io/api/admissionregistration/v1"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// compilePolicy compiles the policy into a PolicyEvaluator
// any error is stored and delayed until invocation.
//
// Each individual mutation is compiled into MutationEvaluationFunc and
// returned is a PolicyEvaluator in the same order as the mutations appeared in the policy.
//
// compilePolicy 编译失败时不返回 error，而是把错误塞进 PolicyEvaluator.Error 延迟到
// 请求时暴露。这是刻意的：策略对象一旦被接受就必须始终存在，编译错误不能阻止其入库，
// 否则用户将无法通过更新策略来修复自身错误。
func compilePolicy(policy *v1.MutatingAdmissionPolicy) PolicyEvaluator {
	// HasParams 决定 CEL 环境是否注入 params 变量；authorizer 对 mutating 策略总是可用。
	opts := plugincel.OptionalVariableDeclarations{HasParams: policy.Spec.ParamKind != nil, HasAuthorizer: true}
	compiler, err := plugincel.NewCompositedCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	if err != nil {
		return PolicyEvaluator{Error: &apiservercel.Error{
			Type:   apiservercel.ErrorTypeInternal,
			Detail: fmt.Sprintf("failed to initialize CEL compiler: %v", err),
		}}
	}

	// Compile and store variables
	// 变量先编译并存入编译器，后续 matchConditions 与 mutations 才能引用它们。
	compiler.CompileAndStoreVariables(convertV1Variables(policy.Spec.Variables), opts, environment.StoredExpressions)

	// Compile matchers
	// 没有 matchConditions 时保持 nil，调用方可据此跳过匹配阶段。
	var matcher matchconditions.Matcher = nil
	matchConditions := policy.Spec.MatchConditions
	if len(matchConditions) > 0 {
		matchExpressionAccessors := make([]plugincel.ExpressionAccessor, len(matchConditions))
		for i := range matchConditions {
			matchExpressionAccessors[i] = (*matchconditions.MatchCondition)(&matchConditions[i])
		}
		matcher = matchconditions.NewMatcher(compiler.CompileCondition(matchExpressionAccessors, opts, environment.StoredExpressions), policy.Spec.FailurePolicy, "policy", "validate", policy.Name)
	}

	// Compiler patchers
	// 每个 mutation 独立编译为 Patcher，顺序严格保持与策略中声明一致，
	// 因为后一个 patch 可能依赖前一个 patch 的结果。
	var patchers []patch.Patcher
	patchOptions := opts
	patchOptions.HasPatchTypes = true
	for _, m := range policy.Spec.Mutations {
		switch m.PatchType {
		case v1.PatchTypeJSONPatch:
			if m.JSONPatch != nil {
				accessor := &patch.JSONPatchCondition{Expression: m.JSONPatch.Expression}
				compileResult := compiler.CompileMutatingEvaluator(accessor, patchOptions, environment.StoredExpressions)
				patchers = append(patchers, patch.NewJSONPatcher(compileResult))
			}
		case v1.PatchTypeApplyConfiguration:
			if m.ApplyConfiguration != nil {
				accessor := &patch.ApplyConfigurationCondition{Expression: m.ApplyConfiguration.Expression}
				compileResult := compiler.CompileMutatingEvaluator(accessor, patchOptions, environment.StoredExpressions)
				patchers = append(patchers, patch.NewApplyConfigurationPatcher(compileResult))
			}
		}
	}

	return PolicyEvaluator{Matcher: matcher, Mutators: patchers, CompositedCompiler: compiler}
}

// convertV1Variables 把 v1.Variable 适配成编译器接受的 NamedExpressionAccessor。
// 复用 validating.Variable 而非定义新类型，保证两种策略共享同一套 CEL 变量语义。
func convertV1Variables(variables []v1.Variable) []plugincel.NamedExpressionAccessor {
	if variables == nil {
		return nil
	}
	res := make([]plugincel.NamedExpressionAccessor, len(variables))
	for i, v := range variables {
		res[i] = &validating.Variable{
			Name:       v.Name,
			Expression: v.Expression,
		}
	}
	return res
}
